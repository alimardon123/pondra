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
//!    (`LakeState::cache`). A cache every query reads (the lookups' row groups, the tables' tails) is made as its lake
//!    opens instead (`Registered::add_at_open`, `States::open`), so the lake charges its caches in one order: the order
//!    they give memory back in (`Lake::caches`).
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

    /// A new one, made as its lake opens (`Registered::add_at_open`), sized by what the lake says of itself: `make`'s, by
    /// default.
    fn make_as_it_opens(opening: &Opening) -> Self {
        let _ = opening;
        Self::make()
    }

    /// Does it keep what files hold, so the lake tells it of files a merge let go of (`files_gone`)? No, by default.
    fn keeps_files(&self) -> bool {
        false
    }

    /// The lake's merges let go of these files (path, bytes): what it keeps of them goes. Nothing, by default.
    fn files_gone(&self, files: &[(String, u64)]) {
        let _ = files;
    }
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
    /// Made as its lake opens (`add_at_open`): how.
    at_open: Option<Maker>,
}

/// A new state as its lake opens, and it as a cache when it is one (`made_as_it_opens`).
type Maker = fn(&Opening) -> (Arc<dyn Kept>, Option<Arc<dyn Charged>>);

/// What a lake says of itself as it opens, to the states it makes then (`LakeState::make_as_it_opens`): its caches are
/// sized by it.
pub struct Opening {
    /// A lake held in this process's memory (`memory://`).
    pub memory: bool,
    /// Its node's machine's memory, when it is known (`Machine::ram`).
    pub ram: Option<usize>,
    /// Its node's memory for queries (`store::node_memory`), which a cache goes by where the machine's memory isn't known.
    pub queries: usize,
}

/// The registry's section of a lake's states: every one a lake may keep, as its extensions add them (`add`).
#[derive(Default)]
pub struct Registered(Vec<Entry>);

impl Registered {
    /// A state a lake may keep.
    pub fn add<P: LakeState>(&mut self) {
        self.push(Entry { name: P::NAME, about: P::ABOUT, id: TypeId::of::<P>(), at_open: None });
    }

    /// A state each lake makes as it opens (`States::open`), in the order they are added: a cache every query reads,
    /// which the lake charges among its own caches, in their order (`Lake::caches`).
    pub fn add_at_open<P: LakeState>(&mut self) {
        self.push(Entry { name: P::NAME, about: P::ABOUT, id: TypeId::of::<P>(), at_open: Some(made_as_it_opens::<P>) });
    }

    #[inline(never)] // (one copy for every state, as `States::get_any`)
    fn push(&mut self, entry: Entry) {
        self.0.push(entry);
    }

    /// Every state, as it was added.
    #[cfg_attr(not(test), allow(dead_code))] // (step 12's generated docs read it)
    pub fn all(&self) -> &[Entry] {
        &self.0
    }

    fn has(&self, id: TypeId) -> bool {
        self.0.iter().any(|k| k.id == id)
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
    name: &'static str,
    state: Arc<dyn Kept>,
    cache: Option<Arc<dyn Charged>>,
}

/// A state as `States` keeps it: one `stop` (and one each of the files' hooks) for every state's type, through its vtable.
trait Kept: Any + Send + Sync {
    fn stop_kept(&self);
    fn keeps_files_kept(&self) -> bool;
    fn files_gone_kept(&self, files: &[(String, u64)]);
}

impl<P: LakeState> Kept for P {
    fn stop_kept(&self) {
        self.stop()
    }

    fn keeps_files_kept(&self) -> bool {
        self.keeps_files()
    }

    fn files_gone_kept(&self, files: &[(String, u64)]) {
        self.files_gone(files)
    }
}

impl States {
    /// The lake's `P`, made the first time and charged to `line` when it is a cache. Refused when no extension of this
    /// build registered `P`.
    #[inline(always)] // (one copy a state: `get_in`'s)
    pub fn get<P: LakeState>(&self, line: Option<&Arc<Ledger>>) -> anyhow::Result<Arc<P>> {
        self.get_in(crate::registry::get(), line)
    }

    /// Each state's own code is this and its `made`, and all the rest is `get_any`'s, one copy for every state: the
    /// page downloads each copy, and every module's statics come to be states (invariant 301).
    fn get_in<P: LakeState>(&self, r: &Registry, line: Option<&Arc<Ledger>>) -> anyhow::Result<Arc<P>> {
        let state: Arc<dyn Any + Send + Sync> = self.get_any(r, TypeId::of::<P>(), P::NAME, line, made::<P>)?;
        Ok(state.downcast::<P>().unwrap_or_else(|_| unreachable!("a state is kept under its own type")))
    }

    fn get_any(&self, r: &Registry, id: TypeId, name: &'static str, line: Option<&Arc<Ledger>>, made: fn() -> (Arc<dyn Kept>, Option<Arc<dyn Charged>>)) -> anyhow::Result<Arc<dyn Kept>> {
        let mut all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(m) = all.iter().find(|m| m.id == id) {
            return Ok(m.state.clone());
        }
        anyhow::ensure!(r.find::<Registered>().is_some_and(|k| k.has(id)), "{name} is no state a lake keeps: the extension that owns it adds it to the registry's lake_state section (`lake_state::Registered::add`)");
        let (state, cache) = made();
        if let (Some(line), Some(cache)) = (line, &cache) {
            line.charge(name, Arc::downgrade(cache)); // (weak: it goes with its lake)
        }
        all.push(Made { id, name, state: state.clone(), cache });
        Ok(state)
    }

    /// Makes the states each lake makes as it opens (`Registered::add_at_open`), in the section's order. They aren't
    /// charged here: the lake charges them among its own caches, in its order (`Lake::caches`).
    pub fn open(&self, r: &Registry, opening: &Opening) {
        let mut all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for k in r.find::<Registered>().map_or(&[][..], |k| &k.0) {
            if let (Some(made), false) = (k.at_open, all.iter().any(|m| m.id == k.id)) {
                let (state, cache) = made(opening);
                all.push(Made { id: k.id, name: k.name, state, cache });
            }
        }
    }

    /// Does any of the states keep what files hold (`LakeState::keeps_files`)?
    pub fn keep_files(&self) -> bool {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|m| m.state.keeps_files_kept())
    }

    /// The lake's merges let go of these files: each state that keeps what files hold forgets them (`LakeState::files_gone`).
    pub fn files_gone(&self, files: &[(String, u64)]) {
        let all: Vec<Arc<dyn Kept>> = self.0.lock().unwrap_or_else(PoisonError::into_inner).iter().filter(|m| m.state.keeps_files_kept()).map(|m| m.state.clone()).collect();
        for state in all {
            state.files_gone_kept(files); // (outside the lock: a state's own lock is taken there)
        }
    }

    /// The states that are caches, by name, in the order they were made: those made as the lake opened first.
    pub fn caches(&self) -> Vec<(&'static str, Arc<dyn Charged>)> {
        let all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        all.iter().filter_map(|m| Some((m.name, m.cache.clone()?))).collect()
    }
}

/// A new `P` for `States`, and it as a cache when it is one.
fn made<P: LakeState>() -> (Arc<dyn Kept>, Option<Arc<dyn Charged>>) {
    let state = Arc::new(P::make());
    (state.clone(), state.cache())
}

/// `made`, as the lake opens (`LakeState::make_as_it_opens`).
fn made_as_it_opens<P: LakeState>(opening: &Opening) -> (Arc<dyn Kept>, Option<Arc<dyn Charged>>) {
    let state = Arc::new(P::make_as_it_opens(opening));
    (state.clone(), state.cache())
}

impl Drop for States {
    fn drop(&mut self) {
        let made = std::mem::take(self.0.get_mut().unwrap_or_else(PoisonError::into_inner));
        for m in made.into_iter().rev() {
            m.state.stop_kept(); // (the last made first: one made later may lean on one made before it)
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
        let line = Ledger::new(1 << 20, 1 << 20);
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

    #[derive(Default)]
    struct Opened(Mutex<usize>);
    impl LakeState for Opened {
        const NAME: &'static str = "opened";
        const ABOUT: &'static str = "bytes held, made as its lake opens, for a test";
        fn make() -> Self {
            Opened::default()
        }
        fn cache(self: Arc<Self>) -> Option<Arc<dyn Charged>> {
            Some(self)
        }
    }
    impl Charged for Opened {
        fn held(&self) -> usize {
            *self.0.lock().unwrap()
        }
        fn give_back(&self, _: usize) -> usize {
            0
        }
    }

    /// A state added to be made as its lake opens is made by `open` and listed among the lake's caches, but not charged
    /// there (the lake charges its caches in its own order), and is made once.
    #[test]
    fn a_state_made_as_its_lake_opens_is_listed_among_its_caches() {
        let r = Registry::build(&[&BuiltIn { name: "test", requires: &[], register: |r| r.section_mut::<Registered>().add_at_open::<Opened>() }]);
        let line = Ledger::new(1 << 20, 1 << 20);
        let a = States::default();
        assert!(a.caches().is_empty(), "nothing made yet");
        let opening = Opening { memory: false, ram: None, queries: 1 << 30 };
        a.open(&r, &opening);
        assert_eq!(a.caches().iter().map(|(name, _)| *name).collect::<Vec<_>>(), ["opened"], "made as the lake opens");
        let opened = a.get_in::<Opened>(&r, Some(&line)).unwrap();
        *opened.0.lock().unwrap() = 4096;
        assert_eq!(line.held(), 0, "not charged as it is made: the lake charges it");
        a.open(&r, &opening);
        assert!(Arc::ptr_eq(&opened, &a.get_in::<Opened>(&r, None).unwrap()) && a.caches().len() == 1, "made once");
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
