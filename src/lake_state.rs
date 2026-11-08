//! A lake's state by module (invariant 301): what an engine or an extension module keeps for one lake, such as its
//! functions' answers (`pyfn`) or the answers kept for their pages (`pages`). The lake is the catalog's and can't name
//! those modules (the floors), so it keeps each module's state by its type (`States`, `Lake::state`), and the module that
//! owns it says what it is. (Not an object's parts, `objects::Part`: those are what `pondra.parts` lists.)
//!
//! The rules, so a lake's state stays listed and never becomes a bag of anything:
//! 1. Each state is a registry entry (`Registered`): its name and what it holds. One nobody registered is refused, so the
//!    section lists every lake's state in one place.
//! 2. A state is made once a lake, the first time it is asked for, and never shared with another lake. It never holds
//!    who is asking: that goes with the session.
//! 3. When the lake goes, its states stop (`LakeState::stop`), the last made first.
//! 4. A state that is a cache is charged to the lake's line as it is made, and gives memory back when the line needs it
//!    (`LakeState::cache`).
use crate::ledger::{Charged, Ledger};
use crate::registry::{Registry, Section};
use std::any::{Any, TypeId};
use std::sync::{Arc, Mutex, PoisonError};

/// What a module keeps for each lake: one, made on its first use there (`States::get`).
pub trait LakeState: Any + Send + Sync + Sized {
    /// Its name in the registry's section, and on the lake's line when it is a cache (`/metrics`' `cache_bytes`).
    const NAME: &'static str;
    /// What it holds, for the section's list.
    const ABOUT: &'static str;
    /// A new one, for a lake that asks for it the first time.
    fn make() -> Self;
    /// Its lake goes: anything it must end ends here. Nothing, by default.
    fn stop(&self) {}
    /// It as a cache the lake's line charges, when it is one: the line counts what it holds and takes memory back from it.
    fn cache(self: Arc<Self>) -> Option<Arc<dyn Charged>> {
        None
    }
}

/// A lake's state as the registry lists it.
pub struct Entry {
    pub name: &'static str,
    #[cfg_attr(not(test), allow(dead_code))] // (step 12's generated docs list every state with it)
    pub about: &'static str,
    id: TypeId,
}

/// The registry's section of a lake's states: every one a lake may keep, as its extensions add them (`add`).
#[derive(Default)]
pub struct Registered(Vec<Entry>);

impl Registered {
    /// A state a lake may keep.
    pub fn add<P: LakeState>(&mut self) {
        self.0.push(Entry { name: P::NAME, about: P::ABOUT, id: TypeId::of::<P>() });
    }

    /// Every state, as it was added.
    #[cfg_attr(not(test), allow(dead_code))] // (step 12's generated docs read it)
    pub fn all(&self) -> &[Entry] {
        &self.0
    }

    fn has<P: LakeState>(&self) -> bool {
        self.0.iter().any(|k| k.id == TypeId::of::<P>())
    }
}

impl Section for Registered {
    const NAME: &'static str = "lake_state";

    fn check(&self, _: &Registry) -> Result<(), String> {
        for (i, k) in self.0.iter().enumerate() {
            if let Some(o) = self.0[..i].iter().find(|o| o.name == k.name || o.id == k.id) {
                return Err(format!("two lake states are {}: {} and {}", if o.id == k.id { "one type" } else { "named alike" }, o.name, k.name));
            }
        }
        Ok(())
    }
}

/// A lake's states, by type, in the order they were made (`Lake::state`).
#[derive(Default)]
pub struct States(Mutex<Vec<Made>>);

struct Made {
    id: TypeId,
    state: Arc<dyn Any + Send + Sync>,
    stop: fn(&(dyn Any + Send + Sync)),
}

impl States {
    /// The lake's `P`, made the first time and charged to `line` when it is a cache. Refused when no extension of this
    /// build registered `P`.
    pub fn get<P: LakeState>(&self, line: Option<&Arc<Ledger>>) -> anyhow::Result<Arc<P>> {
        self.get_in(crate::registry::get(), line)
    }

    fn get_in<P: LakeState>(&self, r: &Registry, line: Option<&Arc<Ledger>>) -> anyhow::Result<Arc<P>> {
        let mut made = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(m) = made.iter().find(|m| m.id == TypeId::of::<P>()) {
            return Ok(m.state.clone().downcast::<P>().expect("a state is kept under its own type"));
        }
        anyhow::ensure!(r.find::<Registered>().is_some_and(Registered::has::<P>), "{} is no state a lake keeps: the extension that owns it adds it to the registry's lake_state section (`lake_state::Registered::add`)", P::NAME);
        let state = Arc::new(P::make());
        if let (Some(line), Some(cache)) = (line, state.clone().cache()) {
            line.charge(P::NAME, Arc::downgrade(&cache)); // (weak: it goes with its lake)
        }
        made.push(Made { id: TypeId::of::<P>(), state: state.clone(), stop: |p| p.downcast_ref::<P>().expect("its own type").stop() });
        Ok(state)
    }
}

impl Drop for States {
    fn drop(&mut self) {
        let made = std::mem::take(self.0.get_mut().unwrap_or_else(PoisonError::into_inner));
        for m in made.into_iter().rev() {
            (m.stop)(&*m.state); // (the last made first: one made later may lean on one made before it)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::BuiltIn;

    static STOPPED: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

    #[derive(Default)]
    struct Held(Mutex<usize>);
    impl LakeState for Held {
        const NAME: &'static str = "held";
        const ABOUT: &'static str = "bytes held, for a test";
        fn make() -> Self {
            Held::default()
        }
        fn stop(&self) {
            STOPPED.lock().unwrap().push(Self::NAME);
        }
        fn cache(self: Arc<Self>) -> Option<Arc<dyn Charged>> {
            Some(self)
        }
    }
    impl Charged for Held {
        fn held(&self) -> usize {
            *self.0.lock().unwrap()
        }
        fn give_back(&self, bytes: usize) -> usize {
            let mut h = self.0.lock().unwrap();
            let given = bytes.min(*h);
            *h -= given;
            given
        }
        fn gives_back(&self) -> bool {
            true
        }
    }

    struct Other;
    impl LakeState for Other {
        const NAME: &'static str = "other";
        const ABOUT: &'static str = "nothing, for a test";
        fn make() -> Self {
            Other
        }
        fn stop(&self) {
            STOPPED.lock().unwrap().push(Self::NAME);
        }
    }

    struct Unlisted;
    impl LakeState for Unlisted {
        const NAME: &'static str = "unlisted";
        const ABOUT: &'static str = "a state no extension added";
        fn make() -> Self {
            Unlisted
        }
    }

    /// A lake's state is made once, on its first use, and is that lake's alone; a cache is charged to its line, which takes
    /// memory back from it; a state nobody registered is refused by name; and the states stop as their lake goes, the last
    /// made first.
    #[test]
    fn a_lakes_states_are_its_own_listed_charged_and_stopped_last_first() {
        let r = Registry::build(&[&BuiltIn { name: "test", requires: &[], register: |r| {
            r.section_mut::<Registered>().add::<Held>();
            r.section_mut::<Registered>().add::<Other>();
        } }]);
        let line = Ledger::new(1 << 20, 1 << 20, false);
        let (a, b) = (States::default(), States::default());
        let held = a.get_in::<Held>(&r, Some(&line)).unwrap();
        assert!(Arc::ptr_eq(&held, &a.get_in::<Held>(&r, Some(&line)).unwrap()), "made once a lake");
        assert!(!Arc::ptr_eq(&held, &b.get_in::<Held>(&r, None).unwrap()), "never another lake's");
        *held.0.lock().unwrap() = 4096;
        assert_eq!(line.held(), 4096, "a cache is charged to its lake's line");
        let _ = a.get_in::<Other>(&r, Some(&line)).unwrap();
        let refused = a.get_in::<Unlisted>(&r, None).err().map(|e| e.to_string()).unwrap_or_default();
        assert!(refused.contains("unlisted is no state a lake keeps"), "{refused}");
        assert_eq!(r.section::<Registered>().all().iter().map(|k| (k.name, k.about.is_empty())).collect::<Vec<_>>(), [("held", false), ("other", false)], "listed in one place");
        STOPPED.lock().unwrap().clear();
        drop((held, a));
        assert_eq!(*STOPPED.lock().unwrap(), ["other", "held"], "the last made stops first");
        assert_eq!(line.held(), 0, "a lake's cache goes with it");
    }

    /// Two states of one type, or of one name, are refused as the registry is built.
    #[test]
    fn two_lake_states_alike_are_refused() {
        let mut k = Registered::default();
        k.add::<Held>();
        k.add::<Held>();
        assert!(k.check(&Registry::default()).unwrap_err().contains("one type"));
    }
}
