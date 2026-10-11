//! A node's statics, set back when it stops (ADR-061 §3). One process may start a node, stop it and
//! start another, on another lake (`pondra.open`), so every static that holds a node's or a lake's
//! state is declared with `per_node!`, and `Node::stop` calls `reset()` last. A static declared that
//! way registers itself the first time it is used, so one never used costs nothing, and none can be
//! left out of the reset by forgetting to add it to a list. `tools/statics_check.py` fails a static
//! that is neither `per_node!` nor known to outlive a node.
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, RwLock};

/// What a per-node static is set back to when its node stops: its state as a node starts.
pub trait Clear {
    fn clear(&self);
}

impl<T: Default> Clear for Mutex<T> {
    fn clear(&self) {
        // (taken out under the lock, dropped after it: dropping may take other locks)
        let old = std::mem::take(&mut *self.lock().unwrap_or_else(|e| e.into_inner()));
        drop(old);
    }
}

impl<T: Default> Clear for RwLock<T> {
    fn clear(&self) {
        let old = std::mem::take(&mut *self.write().unwrap_or_else(|e| e.into_inner()));
        drop(old);
    }
}

macro_rules! atomics {
    ($($t:ty = $zero:expr),*) => {$(
        impl Clear for $t {
            fn clear(&self) { self.store($zero, Ordering::SeqCst) }
        }
    )*};
}
atomics!(AtomicBool = false, AtomicU32 = 0, AtomicU64 = 0, AtomicUsize = 0, AtomicI64 = 0);

#[cfg(feature = "server")]
impl<T: Default> Clear for tokio::sync::watch::Sender<T> {
    fn clear(&self) {
        drop(self.send_replace(T::default()));
    }
}

/// A static that belongs to the node running in this process (ADR-061 §3), set back by `reset()` when
/// the node stops. Declared with `per_node!`; used as the value it holds (`ACCESS.lock()`).
pub struct PerNode<T: 'static> {
    value: LazyLock<T>,
    clear: fn(),
    known: AtomicBool,
}

impl<T> PerNode<T> {
    /// `init` makes the value as a node starts; `clear` sets it back (`per_node!` writes both).
    pub const fn new(init: fn() -> T, clear: fn()) -> Self {
        PerNode { value: LazyLock::new(init), clear, known: AtomicBool::new(false) }
    }
}

impl<T> std::ops::Deref for PerNode<T> {
    type Target = T;
    fn deref(&self) -> &T {
        if !self.known.load(Ordering::Acquire) && !self.known.swap(true, Ordering::AcqRel) {
            USED.lock().unwrap_or_else(|e| e.into_inner()).push(self.clear);
        }
        &self.value
    }
}

/// The per-node statics used since the process started: how to set each back.
static USED: Mutex<Vec<fn()>> = Mutex::new(Vec::new());

/// Every per-node static used since the process started, set back: the last step of `Node::stop`,
/// once the node's work has ended (its writers, its Python workers, its tasks).
pub fn reset() {
    let all = USED.lock().unwrap_or_else(|e| e.into_inner()).clone(); // (a clear may use another static)
    for clear in all {
        clear();
    }
}

/// A value a node sets once and reads after, set back to `None` when it stops: what a `OnceLock`
/// was, before a process could hold a node after another (`per_node!(static X: RwLock<Option<T>> = RwLock::new(None))`).
pub trait Once<T> {
    /// Sets it if it isn't set yet: false if it was.
    fn set_once(&self, v: T) -> bool;
    /// Its value, made by `make` if it isn't set yet (as `OnceLock::get_or_init`).
    fn get_or(&self, make: impl FnOnce() -> T) -> T
    where
        T: Clone;
    /// Its value, if it is set.
    fn current(&self) -> Option<T>
    where
        T: Clone;
}

impl<T> Once<T> for RwLock<Option<T>> {
    fn set_once(&self, v: T) -> bool {
        let mut slot = self.write().unwrap_or_else(|e| e.into_inner());
        slot.is_none() && slot.replace(v).is_none()
    }

    fn get_or(&self, make: impl FnOnce() -> T) -> T
    where
        T: Clone,
    {
        if let Some(v) = self.current() {
            return v;
        }
        self.write().unwrap_or_else(|e| e.into_inner()).get_or_insert_with(make).clone()
    }

    fn current(&self) -> Option<T>
    where
        T: Clone,
    {
        self.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Declares a static that belongs to the node running in this process (ADR-061 §3), set back when it
/// stops: `per_node!(static ACCESS: Mutex<HashMap<String, Access>> = Mutex::default());`. Its type
/// says how (`Clear`: a lock's value taken back to its default, an atomic to 0 or false); a type
/// with no default says it with `clear = |value| …;` after the value.
#[macro_export]
macro_rules! per_node {
    ($(#[$m:meta])* $vis:vis static $name:ident: $ty:ty = $init:expr; clear = $clear:expr $(;)?) => {
        $(#[$m])* $vis static $name: $crate::reset::PerNode<$ty> =
            $crate::reset::PerNode::new(|| $init, || {
                let set_back: fn(&$ty) = $clear;
                set_back(&$name)
            });
    };
    ($(#[$m:meta])* $vis:vis static $name:ident: $ty:ty = $init:expr $(;)?) => {
        $(#[$m])* $vis static $name: $crate::reset::PerNode<$ty> =
            $crate::reset::PerNode::new(|| $init, || <$ty as $crate::reset::Clear>::clear(&$name));
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    crate::per_node!(static NAMES: Mutex<HashMap<String, u32>> = Mutex::new(HashMap::new()));
    crate::per_node!(static COUNT: AtomicU64 = AtomicU64::new(0));
    crate::per_node!(static MADE: RwLock<Option<String>> = RwLock::new(None));
    crate::per_node!(static NEVER: AtomicU64 = AtomicU64::new(0));
    crate::per_node!(static CUSTOM: Mutex<Vec<u8>> = Mutex::new(vec![1]); clear = |v| *v.lock().unwrap() = vec![1]);

    // (each static's own clear, as `reset` calls it: `reset` itself would set back the statics of
    // the other tests running beside this one)
    #[test]
    fn a_node_leaves_nothing_of_itself() {
        NAMES.lock().unwrap().insert("ann".into(), 1);
        COUNT.fetch_add(5, Ordering::SeqCst);
        assert!(MADE.set_once("a".into()));
        assert!(!MADE.set_once("b".into()));
        assert_eq!(MADE.get_or(|| "c".into()), "a");
        CUSTOM.lock().unwrap().push(2);
        let used = USED.lock().unwrap().clone();
        for clear in [NAMES.clear, COUNT.clear, MADE.clear, CUSTOM.clear] {
            assert!(used.iter().any(|f| std::ptr::fn_addr_eq(*f, clear)), "registered when first used");
            clear();
        }
        assert!(NAMES.lock().unwrap().is_empty());
        assert_eq!(COUNT.load(Ordering::SeqCst), 0);
        assert_eq!(MADE.current(), None);
        assert_eq!(MADE.get_or(|| "c".into()), "c");
        assert_eq!(*CUSTOM.lock().unwrap(), vec![1]);
        assert!(!NEVER.known.load(Ordering::SeqCst), "one never used is never made, nor registered");
    }
}
