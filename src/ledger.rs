//! The memory ledger (designs/redesign/redesign.md §3, step 1e): what this process holds in memory, kept in one place.
//!
//! - `ByteLru`, the one cache bounded by bytes. Each of the node's caches of that shape (a map, a running byte total, and a
//!   loop that drops the least recently used past a bound) was written by hand about ten times, each with its own total to
//!   get right; they all use this one now, so a bound is set, read and later reclaimed in one way.
//! - `Ledger`, the one line for query memory, and `LakePool`, a lake's share of it as DataFusion's `MemoryPool`.
//! - `Charged`, a cache the ledger counts: what it holds, and memory it gives back when the node needs it. The lakes on a
//!   line charge their caches to it, so the node's memory watch takes memory back from all of them in one way.

use datafusion::common::human_readable_size;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError, TryLockError, Weak};

/// A map that keeps its most recently used entries within a byte bound, and within a count bound when one is set. Each
/// entry is charged the bytes its `put` says, fixed for as long as it is kept, so the total never drifts from what the
/// entries were charged. Past either bound the least recently used entries leave. An entry charged more than the whole
/// byte bound is not kept at all, and it does not push the others out.
///
/// What leaves is handed back to the caller, so it can let go of what an entry stood for (a file on disk) and drop big
/// values after letting go of its lock.
pub struct ByteLru<K: Hash + Eq, V> {
    map: lru::LruCache<K, (V, usize)>,
    bytes: usize,
    most: usize,
    count: usize,
}

impl<K: Hash + Eq, V> ByteLru<K, V> {
    /// A cache holding at most `most` bytes, however many entries that is.
    pub fn new(most: usize) -> Self { ByteLru { map: lru::LruCache::unbounded(), bytes: 0, most, count: usize::MAX } }

    /// The same cache, holding at most `count` entries as well.
    pub fn with_count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    /// Keeps `value` under `key`, charged `bytes`, in place of what the key held, and returns what left to make room: the
    /// least recently used first. If `bytes` alone is past the bound, nothing else leaves: the key's old value is dropped,
    /// and the new entry is handed back as the one that left.
    pub fn put(&mut self, key: K, value: V, bytes: usize) -> Vec<(K, V)> {
        if bytes > self.most || self.count == 0 {
            if let Some((_, (_, old))) = self.map.pop_entry(&key) {
                self.bytes -= old;
            }
            return vec![(key, value)];
        }
        if let Some((_, (_, old))) = self.map.push(key, (value, bytes)) {
            self.bytes -= old; // (the key's value before: replaced)
        }
        self.bytes += bytes;
        self.trim()
    }

    /// Sets the byte bound to `most` and returns what left to come within it. (A bound read from a setting is set again
    /// before each `put`, so a setting changed while the node runs applies to the next one.)
    pub fn set_most(&mut self, most: usize) -> Vec<(K, V)> {
        self.most = most;
        self.trim()
    }

    /// Lets go of the least recently used entries until `bytes` have left, `entries` of them have, or none is left, and
    /// returns them: memory the node needs back (`Charged::give_back`). The bound stays, so the cache fills again as it is
    /// read.
    pub fn shed(&mut self, bytes: usize, entries: usize) -> Vec<(K, V)> {
        let (mut freed, mut left) = (0, vec![]);
        while freed < bytes && left.len() < entries {
            let Some((k, (v, b))) = self.map.pop_lru() else { break };
            (self.bytes, freed) = (self.bytes - b, freed + b);
            left.push((k, v));
        }
        left
    }

    fn trim(&mut self) -> Vec<(K, V)> {
        let mut left = vec![];
        while self.bytes > self.most || self.map.len() > self.count {
            let Some((k, (v, b))) = self.map.pop_lru() else { break };
            self.bytes -= b;
            left.push((k, v));
        }
        left
    }

    /// The value kept under `key`, now the most recently used.
    pub fn get<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        self.map.get(key).map(|(v, _)| v)
    }

    /// The value kept under `key`, now the most recently used, to change in place. Its charge stays what `put` said, so
    /// change only what the charge doesn't count (a time, a flag); a value that grows is `put` again.
    pub fn get_mut<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        self.map.get_mut(key).map(|(v, _)| v)
    }

    /// The value kept under `key`, leaving the order as it is.
    pub fn peek<Q: Hash + Eq + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        self.map.peek(key).map(|(v, _)| v)
    }

    /// Whether `key` is kept (leaving the order as it is).
    pub fn contains<Q: Hash + Eq + ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.map.contains(key)
    }

    /// Takes the value kept under `key` out of the cache.
    pub fn pop<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        let (v, b) = self.map.pop(key)?;
        self.bytes -= b;
        Some(v)
    }

    /// Takes out every entry `keep` says no to, and returns them.
    pub fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) -> Vec<(K, V)>
    where
        K: Clone,
    {
        let gone: Vec<K> = self.map.iter().filter(|(k, (v, _))| !keep(k, v)).map(|(k, _)| k.clone()).collect();
        gone.into_iter().filter_map(|k| self.pop(&k).map(|v| (k, v))).collect()
    }

    /// Lets go of every entry.
    pub fn clear(&mut self) {
        self.map.clear();
        self.bytes = 0;
    }

    /// The entries, the most recently used first, leaving the order as it is.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> { self.map.iter().map(|(k, (v, _))| (k, v)) }

    /// The bytes the entries are charged together.
    pub fn bytes(&self) -> usize { self.bytes }

    /// The byte bound.
    pub fn most(&self) -> usize { self.most }

    pub fn len(&self) -> usize { self.map.len() }

    pub fn is_empty(&self) -> bool { self.map.is_empty() }
}

impl<K: Hash + Eq, V> std::fmt::Debug for ByteLru<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ByteLru({} entries, {} of {} bytes)", self.map.len(), self.bytes, self.most)
    }
}

// ---------------------------------------------------------------- what the caches hold

/// A cache the ledger counts (`Ledger::charge`): what it holds, and memory it gives back when the node needs it. A
/// `ByteLru` cache gives back its least recently used entries. A cache with rules of its own (the log's decoded rows,
/// SlateDB's blocks, DataFusion's footers and statistics) says what it holds and keeps it: `give_back`'s default.
pub trait Charged: Send + Sync {
    /// The bytes it holds now.
    fn held(&self) -> usize;

    /// Lets go of about `bytes`, its least useful entries first, and says how many bytes went.
    fn give_back(&self, bytes: usize) -> usize {
        let _ = bytes;
        0
    }
}

impl<K: Hash + Eq + Send, V: Send> Charged for Mutex<ByteLru<K, V>> {
    fn held(&self) -> usize { self.lock().unwrap_or_else(PoisonError::into_inner).bytes() }

    fn give_back(&self, bytes: usize) -> usize { give_back_from(self, |c| c, bytes) }
}

/// The entries `give_back_from` takes at once: a reader that comes meanwhile waits for this many at most.
const AT_ONCE: usize = 64;

/// Takes about `bytes` back from the `ByteLru` that `lru` finds in what `cache` guards, and says how many bytes went. It
/// takes [`AT_ONCE`] entries at a time, each time only while no reader holds the cache, and lets go of them after letting
/// go of the lock: a lookup never waits for the values to be freed, nor for a whole cache to be emptied. (It runs on the
/// memory watch's turn, `oom.rs`, never on a reader's path; a cache a reader holds now is asked again at its next look.)
pub fn give_back_from<T, K: Hash + Eq, V>(cache: &Mutex<T>, lru: impl Fn(&mut T) -> &mut ByteLru<K, V>, bytes: usize) -> usize {
    let mut freed = 0;
    while freed < bytes {
        let gone = {
            let mut guard = match cache.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                Err(TryLockError::WouldBlock) => break,
            };
            let c = lru(&mut guard);
            let before = c.bytes;
            let gone = c.shed(bytes - freed, AT_ONCE);
            freed += before - c.bytes;
            gone
        };
        if gone.is_empty() {
            break;
        }
        drop(gone); // (big values are let go of after the lock is)
    }
    freed
}

/// Takes about `bytes` back from `caches`, each in turn, and says how many bytes came back.
pub fn take_back<'a>(caches: impl IntoIterator<Item = &'a dyn Charged>, bytes: usize) -> usize {
    let mut freed = 0;
    for c in caches {
        if freed >= bytes {
            break;
        }
        freed += c.give_back(bytes - freed);
    }
    freed
}

// ---------------------------------------------------------------- the line

/// The node's one line for query memory, which every lake it opens shares. Each lake's queries run on a `LakePool`: DataFusion's
/// `FairSpillPool` over the lake's share of the line, which is the whole line while one lake runs queries and half of it
/// while two do. So a node with one lake runs as it did, and several lakes in one process (attached and memory databases,
/// `pondra.connect` twice) no longer each take a whole line of their own, beside the hot columns and the caches.
///
/// As within one `FairSpillPool`, a share is held to at the next grow. When a second lake starts, the first lake's
/// spilling operators are refused their next grow past its new share and spill, and the second lake's are not refused
/// for what the first still holds until then. Memory that can't be spilled (a hash join's build side) is held to the
/// lake's share at each grow, so a lake can't take the other's either. The totals are atomic: a lake's grow locks only
/// that lake's own counts, as it did on a pool of its own.
#[derive(Debug)]
pub struct Ledger {
    line: usize,
    reserved: AtomicUsize, // every lake's reservations together
    busy: AtomicUsize,     // lakes with a memory consumer registered now
    caches: Mutex<Vec<(&'static str, Weak<dyn Charged>)>>, // what the node keeps on this line, by name (`charge`), in the order they give back
}

impl Ledger {
    pub fn new(line: usize) -> Arc<Ledger> { Arc::new(Ledger { line, reserved: AtomicUsize::new(0), busy: AtomicUsize::new(0), caches: Default::default() }) }

    /// A new lake's pool on this line.
    pub fn pool(self: &Arc<Self>) -> Arc<LakePool> { Arc::new(LakePool { ledger: self.clone(), part: Default::default() }) }

    pub fn line(&self) -> usize { self.line }

    /// What every lake's queries hold now.
    pub fn reserved(&self) -> usize { self.reserved.load(Relaxed) }

    /// A lake's share now: the line over the lakes running queries, at least one.
    pub fn share(&self) -> usize { self.line / self.busy.load(Relaxed).max(1) }

    /// Counts `cache` on this line, under `name`, for as long as it lives: `/metrics`' `cache_bytes` reads it by that name,
    /// and the node's memory watch takes memory back from it before it stops a query (`oom.rs`). Caches give back in the
    /// order they were charged, so a lake's go in the order it lists them. A cache is charged once, though several lakes
    /// keep it (the ranges of a bucket two lakes are in, the node's planning facts).
    pub fn charge(&self, name: &'static str, cache: Weak<dyn Charged>) {
        let mut caches = self.caches.lock().unwrap_or_else(PoisonError::into_inner);
        caches.retain(|(_, c)| c.strong_count() > 0);
        if !caches.iter().any(|(_, c)| Weak::ptr_eq(c, &cache)) {
            caches.push((name, cache));
        }
    }

    /// The caches still alive, for the caller to hold a moment (a closed lake's go here).
    fn charged(&self) -> Vec<(&'static str, Arc<dyn Charged>)> {
        let mut caches = self.caches.lock().unwrap_or_else(PoisonError::into_inner);
        caches.retain(|(_, c)| c.strong_count() > 0);
        caches.iter().filter_map(|(name, c)| Some((*name, c.upgrade()?))).collect()
    }

    /// What the caches on this line hold now.
    pub fn held(&self) -> usize { self.charged().iter().map(|(_, c)| c.held()).sum() }

    /// What the caches on this line hold now by name, each name once (two lakes' tails are one line), in the order the
    /// names were first charged.
    pub fn held_by_name(&self) -> Vec<(&'static str, usize)> {
        let mut by_name: Vec<(&'static str, usize)> = vec![];
        for (name, c) in self.charged() {
            match by_name.iter_mut().find(|(n, _)| *n == name) {
                Some((_, held)) => *held += c.held(),
                None => by_name.push((name, c.held())),
            }
        }
        by_name
    }

    /// Takes about `bytes` back from the caches on this line, in the order they were charged, and says how many came back.
    pub fn give_back(&self, bytes: usize) -> usize { take_back(self.charged().iter().map(|(_, c)| &**c), bytes) }
}

/// One lake's pool of query memory: `FairSpillPool`'s rules over the lake's share of its ledger's line (`Ledger`).
#[derive(Debug)]
pub struct LakePool {
    ledger: Arc<Ledger>,
    part: Mutex<Part>,
}

/// A lake's consumers and what they hold.
#[derive(Debug, Default)]
struct Part {
    consumers: usize,
    spillers: usize,
    spillable: usize,
    unspillable: usize,
}

impl LakePool {
    pub fn ledger(&self) -> &Arc<Ledger> { &self.ledger }

    fn part(&self) -> std::sync::MutexGuard<'_, Part> { self.part.lock().unwrap_or_else(PoisonError::into_inner) }
}

impl Part {
    fn of(&mut self, r: &MemoryReservation) -> &mut usize {
        match r.consumer().can_spill() {
            true => &mut self.spillable,
            false => &mut self.unspillable,
        }
    }
}

impl std::fmt::Display for LakePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ledger(share: {} of {})", human_readable_size(self.ledger.share()), human_readable_size(self.ledger.line))
    }
}

impl MemoryPool for LakePool {
    fn name(&self) -> &str { "ledger" }

    fn register(&self, c: &MemoryConsumer) {
        let mut p = self.part();
        if p.consumers == 0 {
            self.ledger.busy.fetch_add(1, Relaxed);
        }
        p.consumers += 1;
        p.spillers += usize::from(c.can_spill());
    }

    fn unregister(&self, c: &MemoryConsumer) {
        let mut p = self.part();
        p.consumers = p.consumers.saturating_sub(1);
        p.spillers = p.spillers.saturating_sub(usize::from(c.can_spill()));
        if p.consumers == 0 {
            self.ledger.busy.fetch_sub(1, Relaxed);
        }
    }

    fn grow(&self, r: &MemoryReservation, additional: usize) {
        *self.part().of(r) += additional;
        self.ledger.reserved.fetch_add(additional, Relaxed);
    }

    fn shrink(&self, r: &MemoryReservation, shrink: usize) {
        let mut p = self.part();
        let held = p.of(r);
        *held = held.saturating_sub(shrink);
        self.ledger.reserved.fetch_sub(shrink, Relaxed);
    }

    fn try_grow(&self, r: &MemoryReservation, additional: usize) -> Result<()> {
        let mut p = self.part();
        let share = self.ledger.share();
        let available = match r.consumer().can_spill() {
            // (no spiller past its part of what the lake's unspillable memory leaves of its share)
            true => share.saturating_sub(p.unspillable).checked_div(p.spillers).unwrap_or(share).saturating_sub(r.size()),
            false => share.saturating_sub(p.unspillable + p.spillable),
        };
        if additional > available {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {self}",
                human_readable_size(additional),
                r.consumer().name(),
                human_readable_size(r.size()),
                human_readable_size(available),
            )));
        }
        *p.of(r) += additional;
        self.ledger.reserved.fetch_add(additional, Relaxed);
        Ok(())
    }

    /// What this lake's queries hold (`Ledger::reserved` is every lake's).
    fn reserved(&self) -> usize {
        let p = self.part();
        p.spillable + p.unspillable
    }

    fn memory_limit(&self) -> MemoryLimit { MemoryLimit::Finite(self.ledger.share()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(c: &ByteLru<&'static str, u32>) -> Vec<&'static str> { c.iter().map(|(k, _)| *k).collect() }

    #[test]
    fn the_least_recently_used_leave_past_the_byte_bound() {
        let mut c = ByteLru::new(100);
        assert!(c.put("a", 1, 40).is_empty());
        assert!(c.put("b", 2, 40).is_empty());
        assert_eq!(c.get("a"), Some(&1)); // (a is now the most recently used)
        assert_eq!(c.put("c", 3, 40), vec![("b", 2)], "b, the least recently used, leaves");
        assert_eq!((keys(&c), c.bytes()), (vec!["c", "a"], 80));
        assert_eq!(c.peek("a"), Some(&1));
        assert_eq!(c.put("d", 4, 60), vec![("a", 1)], "peek leaves the order: a is still the least recently used");
        assert_eq!((keys(&c), c.bytes()), (vec!["d", "c"], 100), "a bound reached exactly holds");
    }

    #[test]
    fn a_key_put_again_is_charged_its_new_bytes() {
        let mut c = ByteLru::new(100);
        let _ = c.put("a", 1, 30);
        let _ = c.put("b", 2, 30);
        assert!(c.put("a", 9, 70).is_empty(), "a's 30 bytes are given back before its 70 are charged");
        assert_eq!((keys(&c), c.bytes(), c.peek("a")), (vec!["a", "b"], 100, Some(&9)));
        assert!(c.put("a", 8, 10).is_empty());
        assert_eq!(c.bytes(), 40, "a value that shrinks gives its bytes back");
    }

    #[test]
    fn an_entry_past_the_whole_bound_is_not_kept_and_pushes_nothing_out() {
        let mut c = ByteLru::new(100);
        let _ = c.put("a", 1, 50);
        let _ = c.put("b", 2, 50);
        assert_eq!(c.put("big", 3, 101), vec![("big", 3)], "the entry itself is handed back");
        assert_eq!((keys(&c), c.bytes()), (vec!["b", "a"], 100), "the others stay");
        assert_eq!(c.put("a", 4, 101), vec![("a", 4)]);
        assert_eq!((keys(&c), c.bytes()), (vec!["b"], 50), "a key's old value goes when its new one is past the bound");
    }

    #[test]
    fn a_count_bound_holds_beside_the_byte_bound() {
        let mut c = ByteLru::new(1000).with_count(2);
        let _ = c.put("a", 1, 1);
        let _ = c.put("b", 2, 1);
        assert_eq!(c.put("c", 3, 1), vec![("a", 1)]);
        assert_eq!((c.len(), c.bytes()), (2, 2));
        let mut none = ByteLru::new(1000).with_count(0);
        assert_eq!(none.put("a", 1, 1), vec![("a", 1)], "a count of none keeps nothing");
        assert!(none.is_empty());
    }

    #[test]
    fn pop_retain_clear_and_a_lower_bound_give_their_bytes_back() {
        let mut c = ByteLru::new(100);
        for (k, v) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)] {
            let _ = c.put(k, v, 10 * v as usize);
        }
        assert_eq!(c.pop("b"), Some(2));
        assert_eq!((c.pop("b"), c.bytes()), (None, 80));
        let mut gone = c.retain(|_, v| v % 2 == 0);
        gone.sort();
        assert_eq!((gone, keys(&c), c.bytes()), (vec![("a", 1), ("c", 3)], vec!["d"], 40));
        let _ = c.put("e", 5, 50);
        assert_eq!(c.set_most(60), vec![("d", 4)], "a lower bound lets the least recently used go");
        assert_eq!((c.most(), c.bytes(), c.contains("e"), c.contains("d")), (60, 50, true, false));
        c.clear();
        assert_eq!((c.len(), c.bytes()), (0, 0));
    }

    #[test]
    fn a_value_changed_in_place_keeps_its_charge_and_becomes_the_most_recent() {
        let mut c = ByteLru::new(100);
        let _ = c.put("a", 1, 50);
        let _ = c.put("b", 2, 50);
        *c.get_mut("a").unwrap() = 7;
        assert_eq!((keys(&c), c.bytes()), (vec!["a", "b"], 100));
        assert_eq!(c.put("c", 3, 50), vec![("b", 2)]);
    }

    #[test]
    fn keys_are_looked_up_by_what_they_borrow_as() {
        let mut c: ByteLru<String, u32> = ByteLru::new(100);
        let _ = c.put("a".to_string(), 1, 1);
        assert_eq!(c.get("a"), Some(&1));
        assert_eq!((c.peek("a"), c.contains("a")), (Some(&1), true));
        assert_eq!(c.pop("a"), Some(1));
    }

    fn limit(pool: &LakePool) -> Option<usize> {
        match pool.memory_limit() {
            MemoryLimit::Finite(n) => Some(n),
            _ => None,
        }
    }

    fn consumer(pool: &Arc<LakePool>, name: &str, spills: bool) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = pool.clone();
        MemoryConsumer::new(name).with_can_spill(spills).register(&pool)
    }

    #[test]
    fn one_lake_has_the_whole_line_as_on_a_pool_of_its_own() {
        let ledger = Ledger::new(100);
        let a = ledger.pool();
        let (s1, s2) = (consumer(&a, "s1", true), consumer(&a, "s2", true));
        assert!(s1.try_grow(50).is_ok() && s2.try_grow(50).is_ok(), "two spillers, half the line each");
        assert!(s1.try_grow(1).is_err(), "no spiller past its half");
        let held = consumer(&a, "held", false);
        assert!(held.try_grow(1).is_err(), "nothing that can't spill past the line");
        s2.shrink(30);
        assert!(held.try_grow(30).is_ok() && held.try_grow(1).is_err());
        assert_eq!((a.reserved(), ledger.reserved(), limit(&a)), (100, 100, Some(100)));
        drop((s1, s2, held));
        assert_eq!((a.reserved(), ledger.reserved(), ledger.busy.load(Relaxed)), (0, 0, 0), "all given back");
    }

    #[test]
    fn two_lakes_in_one_process_each_keep_their_share() {
        let ledger = Ledger::new(100);
        let (a, b) = (ledger.pool(), ledger.pool());
        let spillers: Vec<_> = (0..4).map(|i| consumer(&a, &format!("a{i}"), true)).collect();
        for s in &spillers {
            s.try_grow(25).expect("alone, a lake has the whole line: a quarter for each of its four spillers");
        }
        assert_eq!(ledger.reserved(), 100);
        // B starts while A holds the whole line: its spiller gets half the line now, not what A leaves.
        let b1 = consumer(&b, "b1", true);
        assert_eq!(limit(&b), Some(50));
        b1.try_grow(50).expect("B's share, though A still holds the line");
        assert!(b1.try_grow(1).is_err(), "and no more");
        // A's spillers are held to A's half at their next grow, and spill.
        assert!(spillers[0].try_grow(1).is_err(), "a spiller of A past its part of A's half");
        for s in &spillers {
            s.shrink(25 - 12); // (what each spilled)
        }
        assert!(spillers[0].try_grow(1).is_err() && spillers[0].try_grow(0).is_ok());
        // Memory that can't be spilled is held to each lake's share too.
        let (a_held, b_held) = (consumer(&a, "a held", false), consumer(&b, "b held", false));
        assert!(a_held.try_grow(3).is_err(), "A's 48 and 3 more pass A's half");
        assert!(a_held.try_grow(2).is_ok());
        assert!(b_held.try_grow(1).is_err(), "B's spiller holds B's half");
        b1.shrink(20);
        assert!(b_held.try_grow(20).is_ok());
        assert_eq!((a.reserved(), b.reserved(), ledger.reserved()), (50, 50, 100));
        // When B is done, A has the whole line again.
        drop((b1, b_held));
        assert_eq!((ledger.busy.load(Relaxed), limit(&a)), (1, Some(100)));
        assert!(a_held.try_grow(50).is_ok());
    }

    #[test]
    fn shed_lets_the_least_recently_used_go_until_enough_has_and_keeps_the_bound() {
        let mut c = ByteLru::new(100);
        for (k, b) in [("a", 10), ("b", 20), ("c", 30)] {
            assert!(c.put(k, 0u32, b).is_empty());
        }
        let _ = c.get("a");
        let gone: Vec<&str> = c.shed(25, 10).into_iter().map(|(k, _)| k).collect();
        assert_eq!(gone, ["b", "c"], "the least recently used first, until 25 bytes have gone");
        assert_eq!((keys(&c), c.bytes(), c.most()), (vec!["a"], 10, 100));
        assert!(c.put("b", 0, 20).is_empty());
        assert_eq!(c.shed(1000, 1).len(), 1, "and no more entries than it is asked for");
        assert_eq!((keys(&c), c.bytes()), (vec!["b"], 20));
        assert!(c.shed(1000, 10).len() == 1 && c.is_empty() && c.bytes() == 0);
        assert!(c.put("d", 0, 100).is_empty(), "it fills again to its bound");
    }

    #[test]
    fn a_line_takes_memory_back_from_its_caches_in_turn_and_forgets_the_dropped_ones() {
        let ledger = Ledger::new(1000);
        let cache = |bytes: &[usize]| {
            let c = Arc::new(Mutex::new(ByteLru::new(1000)));
            for (i, &b) in bytes.iter().enumerate() {
                let _ = c.lock().unwrap().put(i, (), b);
            }
            c
        };
        let (first, second, third) = (cache(&[100, 100]), cache(&[50, 50]), cache(&[70]));
        for (name, c) in [("first", &first), ("second", &second), ("first", &third)] {
            let c: Arc<dyn Charged> = c.clone();
            ledger.charge(name, Arc::downgrade(&c));
            ledger.charge(name, Arc::downgrade(&c)); // (charged once, however many lakes keep it)
        }
        assert_eq!((ledger.held(), ledger.held_by_name()), (370, vec![("first", 270), ("second", 100)]));
        assert_eq!(ledger.give_back(250), 250, "the first cache's entries, the least recently used first, then the second's");
        assert_eq!((first.held(), second.held(), third.held()), (0, 50, 70));
        let held = third.lock().unwrap();
        assert_eq!(ledger.give_back(1000), 50, "a cache a reader holds now is skipped, and asked again at the next look");
        drop(held);
        drop(second);
        assert_eq!(ledger.held(), 70, "a dropped cache is let go of");
        assert_eq!(ledger.give_back(1000), 70);
        assert_eq!((ledger.held(), ledger.caches.lock().unwrap().len()), (0, 2));
    }
}
