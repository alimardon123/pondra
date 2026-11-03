//! The memory ledger (designs/redesign/redesign.md §3, step 1e): what this process holds in memory, kept in one place.
//!
//! - `ByteLru`, the one cache bounded by bytes. Each of the node's caches of that shape (a map, a running byte total, and a
//!   loop that drops the least recently used past a bound) was written by hand about ten times, each with its own total to
//!   get right; they all use this one now, so a bound is set, read and later reclaimed in one way.
//! - `Ledger`, the one line for query memory, and `LakePool`, a lake's share of it as DataFusion's `MemoryPool`.

use datafusion::common::human_readable_size;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};

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
}

impl Ledger {
    pub fn new(line: usize) -> Arc<Ledger> { Arc::new(Ledger { line, reserved: AtomicUsize::new(0), busy: AtomicUsize::new(0) }) }

    /// A new lake's pool on this line.
    pub fn pool(self: &Arc<Self>) -> Arc<LakePool> { Arc::new(LakePool { ledger: self.clone(), part: Default::default() }) }

    pub fn line(&self) -> usize { self.line }

    /// What every lake's queries hold now.
    pub fn reserved(&self) -> usize { self.reserved.load(Relaxed) }

    /// A lake's share now: the line over the lakes running queries, at least one.
    pub fn share(&self) -> usize { self.line / self.busy.load(Relaxed).max(1) }
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
}
