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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
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

    /// Does it hold its bytes inside the whole line, with the queries (`Ledger`)? Yes, unless the user gave it a budget of
    /// its own (`PONDRA_HOT_GB`, `PONDRA_CACHE_MB`): then it keeps that budget beside the line, and only the node's memory
    /// watch takes memory back from it.
    fn inside(&self) -> bool { true }
}

impl<K: Hash + Eq + Send, V: Send> Charged for Mutex<ByteLru<K, V>> {
    fn held(&self) -> usize { self.lock().unwrap_or_else(PoisonError::into_inner).bytes() }

    fn give_back(&self, bytes: usize) -> usize { give_back_from(self, |c| c, bytes) }
}

/// About the memory a parsed JSON value holds, to charge a cache of them (`ByteLru::put`): each value's own 32 bytes, a
/// string's text, and each field's name with a map's overhead for it. Neither exact nor costly: a walk over what was parsed.
pub fn json_bytes(v: &serde_json::Value) -> usize {
    32 + match v {
        serde_json::Value::String(s) => s.len(),
        serde_json::Value::Array(a) => a.iter().map(json_bytes).sum(),
        serde_json::Value::Object(o) => o.iter().map(|(k, v)| 48 + k.len() + json_bytes(v)).sum(),
        _ => 0,
    }
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

// ---------------------------------------------------------------- the line

/// The node's one line for query memory, which every lake it opens shares. Each lake's queries run on a `LakePool`: DataFusion's
/// `FairSpillPool` over the lake's share of the line, which is the whole line while one lake runs queries and half of it
/// while two do. So a node with one lake runs as it did, and several lakes in one process (attached and memory databases,
/// `pondra.connect` twice) no longer each take a whole line of their own, beside the hot columns and the caches.
///
/// The queries and the caches charged to the ledger also fit one whole line together (`whole`): a grow that would pass it
/// takes cache bytes back first, and spills only if they don't come back. Every cache is inside it, except one the user
/// gave a budget of its own (`Charged::inside`), which keeps that budget beside it with the memory watch behind it. With
/// `PONDRA_MEMORY_GB` set it is the whole line, queries and caches together, as DuckDB's `memory_limit` is (`hard`): a
/// spilling operator the caches didn't make room for spills at once. Unset, the queries keep their own line (a third of the
/// machine's memory, which leaves room for what their reservations don't count, `array_agg` and the like) inside a whole
/// line at the memory watch's trim line (`oom::trim_line`), and a grow past it takes what the caches give back and goes on
/// within its share, as before: spilling sooner than today would change what a query does by default. So the budgets no
/// longer add up past the machine's memory, and a query grows into what the caches hold rather than the watch stopping it
/// there. What the caches hold is read at the watch's looks (`note`) and lowered by what each grow takes back, so a grow
/// reads one number and locks no cache to read it; it takes bytes back only from a cache it finds free
/// (`Charged::give_back`), and never waits on a reader. Memory that can't spill is held to its share alone on either line.
///
/// As within one `FairSpillPool`, a share is held to at the next grow. When a second lake starts, the first lake's
/// spilling operators are refused their next grow past its new share and spill, and the second lake's are not refused
/// for what the first still holds until then. Memory that can't be spilled (a hash join's build side) is held to the
/// lake's share at each grow, so a lake can't take the other's either. The totals are atomic: a lake's grow locks only
/// that lake's own counts, as it did on a pool of its own.
#[derive(Debug)]
pub struct Ledger {
    line: usize,           // the queries' line: what their reservations hold together, at most
    whole: usize,          // the whole line: the queries' reservations and the caches together (`usize::MAX`: none known)
    hard: bool,            // the whole line is the user's (`PONDRA_MEMORY_GB`): a spiller it has no room for spills
    reserved: AtomicUsize, // every lake's reservations together
    busy: AtomicUsize,     // lakes with a memory consumer registered now
    cached: AtomicUsize,   // what the caches held at the last look (`note`), less what grows took back since
    dry: AtomicBool,       // a take-back since the last look came back short: grows don't ask the caches again until `note`
    caches: Mutex<Vec<(&'static str, Weak<dyn Charged>)>>, // what the node keeps on this line, by name (`charge`), in the order they give back
}

impl Ledger {
    /// A ledger whose queries hold `line` at most, inside `whole` with the caches (`line` itself when the setting is the whole
    /// line). A `hard` whole line refuses a spiller's grow the caches don't make room for; a soft one only takes cache bytes back.
    pub fn new(line: usize, whole: usize, hard: bool) -> Arc<Ledger> {
        Arc::new(Ledger { line, whole, hard, reserved: AtomicUsize::new(0), busy: AtomicUsize::new(0), cached: AtomicUsize::new(0), dry: AtomicBool::new(false), caches: Default::default() })
    }

    /// A new lake's pool on this line.
    pub fn pool(self: &Arc<Self>) -> Arc<LakePool> { Arc::new(LakePool { ledger: self.clone(), part: Default::default() }) }

    pub fn line(&self) -> usize { self.line }

    /// The whole line, for the queries and the caches together.
    pub fn whole(&self) -> usize { self.whole }

    /// What the caches held at the last look, less what grows took back since: what a grow goes by.
    pub fn cached(&self) -> usize { self.cached.load(Relaxed) }

    /// Reads what the caches inside the whole line hold now (`Charged::inside`), for the grows to go by until the next look,
    /// and says it. The node's memory watch reads it at each of its looks (`oom.rs`), so a grow never locks a cache to read it.
    pub fn note(&self) -> usize {
        let held = self.charged().iter().filter(|(_, c)| c.inside()).map(|(_, c)| c.held()).sum();
        self.cached.store(held, Relaxed);
        self.dry.store(false, Relaxed);
        held
    }

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

    /// Takes about `bytes` back from the caches on this line, in the order they were charged, and says how many came back:
    /// the node's memory watch, from every cache.
    pub fn give_back(&self, bytes: usize) -> usize { self.take(bytes, false) }

    /// Takes about `bytes` back from the caches inside the whole line alone: a query's grow past it.
    fn make_room(&self, bytes: usize) -> usize { self.take(bytes, true) }

    fn take(&self, bytes: usize, inside_only: bool) -> usize {
        let (mut freed, mut inside) = (0, 0);
        for (_, c) in self.charged() {
            if freed >= bytes {
                break;
            }
            let counted = c.inside();
            if inside_only && !counted {
                continue;
            }
            let got = c.give_back(bytes - freed);
            freed += got;
            inside += if counted { got } else { 0 };
        }
        let _ = self.cached.fetch_update(Relaxed, Relaxed, |held| Some(held.saturating_sub(inside)));
        freed
    }

    /// How far the queries holding `additional` bytes more would pass the whole line, beside what the caches hold: what the
    /// caches give back first, from those a reader isn't holding (`give_back`).
    fn over(&self, additional: usize) -> usize {
        self.reserved().saturating_add(additional).saturating_add(self.cached()).saturating_sub(self.whole)
    }
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

    /// What `r` may grow by within the lake's share, by `FairSpillPool`'s rules.
    fn available(&self, p: &Part, r: &MemoryReservation) -> usize {
        let share = self.ledger.share();
        match r.consumer().can_spill() {
            // (no spiller past its part of what the lake's unspillable memory leaves of its share)
            true => share.saturating_sub(p.unspillable).checked_div(p.spillers).unwrap_or(share).saturating_sub(r.size()),
            false => share.saturating_sub(p.unspillable + p.spillable),
        }
    }

    /// The refusal of a grow, which a spilling operator answers by spilling and any other query by stopping.
    fn refused(&self, r: &MemoryReservation, additional: usize, available: usize) -> DataFusionError {
        DataFusionError::ResourcesExhausted(format!(
            "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {self}",
            human_readable_size(additional),
            r.consumer().name(),
            human_readable_size(r.size()),
            human_readable_size(available),
        ))
    }
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
        let l = &self.ledger;
        write!(f, "ledger(share: {} of {}", human_readable_size(l.share()), human_readable_size(l.line))?;
        match l.whole {
            usize::MAX => write!(f, ")"),
            whole => write!(f, ", caches {} of {} with queries)", human_readable_size(l.cached()), human_readable_size(whole)),
        }
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
        let available = self.available(&p, r);
        if additional > available {
            return Err(self.refused(r, additional, available));
        }
        let over = self.ledger.over(additional);
        if over > 0 {
            // A take-back that came back short (a reader in a cache, or bytes a cache can't give back: a table's tail, the
            // footers) isn't asked for again until the memory watch's next look, so a query growing near the line doesn't
            // walk the caches at every grow for bytes that aren't there.
            let mut short = self.ledger.dry.load(Relaxed);
            if !short {
                // (the cache bytes go outside the lake's lock, so its other grows don't wait on them; then its share is read again)
                drop(p);
                short = self.ledger.make_room(over) < over;
                if short {
                    self.ledger.dry.store(true, Relaxed);
                }
                p = self.part();
                let available = self.available(&p, r);
                if additional > available {
                    return Err(self.refused(r, additional, available));
                }
            }
            if short && self.ledger.hard && r.consumer().can_spill() && r.size() > 0 {
                // On the user's line a spiller spills at once rather than wait on a reader. One that holds nothing yet has
                // nothing to spill (a sort's first batch would fail its query: "Not enough memory to continue external
                // sort"), and what can't spill (a hash join's build side) would fail its query for a reader's moment in a
                // cache, so both are held to their share alone, as before, with the memory watch behind them, and take back
                // what the caches give. On the default line every grow does that: it spills where it did before, and the
                // caches it found free gave their bytes back.
                return Err(self.refused(r, additional, 0));
            }
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
        let ledger = Ledger::new(100, usize::MAX, false);
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
        let ledger = Ledger::new(100, usize::MAX, false);
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
        let ledger = Ledger::new(1000, usize::MAX, false);
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

    /// A cache of `entries` entries of `each` bytes, charged to `ledger` (the caller keeps it alive).
    fn charged_cache(ledger: &Ledger, entries: usize, each: usize) -> Arc<Mutex<ByteLru<usize, ()>>> {
        let c = Arc::new(Mutex::new(ByteLru::new(usize::MAX)));
        for i in 0..entries {
            let _ = c.lock().unwrap().put(i, (), each);
        }
        let charged: Arc<dyn Charged> = c.clone();
        ledger.charge("cache", Arc::downgrade(&charged));
        c
    }

    #[test]
    fn a_grow_past_the_whole_line_takes_cache_bytes_back_rather_than_spill() {
        // Queries and caches inside one line of 100: the caches hold 80, which a grow sees from the last look alone.
        let ledger = Ledger::new(100, 100, true);
        let cache = charged_cache(&ledger, 8, 10);
        assert_eq!((ledger.note(), ledger.cached()), (80, 80));
        let a = ledger.pool();
        let (s, held) = (consumer(&a, "s", true), consumer(&a, "held", false));
        s.try_grow(20).expect("what the caches leave of the line");
        s.try_grow(25).expect("past it, the caches give back the 25 it takes, the least recently used first");
        assert_eq!((cache.held(), ledger.cached(), ledger.reserved()), (50, 50, 45));
        held.try_grow(50).expect("memory that can't spill takes cache bytes back too");
        assert_eq!((cache.held(), ledger.cached()), (0, 0));
        assert!(s.try_grow(10).is_err(), "past the lake's share, a grow spills as before");
        assert_eq!(ledger.reserved(), 95);
        // A cache that grew since the last look is seen at the next.
        let _ = cache.lock().unwrap().put(100, (), 5);
        assert_eq!(ledger.note(), 5);
        assert!(s.try_grow(5).is_ok() && cache.held() == 0, "and taken back at the next grow past the line");
        assert!(s.try_grow(1).is_err(), "the line is full");
        drop((s, held));
        // With no whole line (a machine whose memory isn't read), the caches stay beside the queries, as before.
        let beside = Ledger::new(100, usize::MAX, false);
        let kept = charged_cache(&beside, 8, 10);
        beside.note();
        let q = consumer(&beside.pool(), "q", true);
        assert!(q.try_grow(100).is_ok() && q.try_grow(1).is_err());
        assert_eq!(kept.held(), 80);
    }

    #[test]
    fn on_the_default_line_a_grow_takes_what_the_caches_give_back_and_spills_only_where_it_did_before() {
        // The whole line is the memory watch's trim line (`PONDRA_MEMORY_GB` unset): the queries keep their share.
        let ledger = Ledger::new(100, 120, false);
        let cache = charged_cache(&ledger, 8, 10);
        ledger.note();
        let a = ledger.pool();
        let s = consumer(&a, "s", true);
        s.try_grow(60).expect("past the whole line, the caches give back 20");
        assert_eq!((cache.held(), ledger.cached()), (60, 60));
        let reader = cache.lock().unwrap();
        s.try_grow(40).expect("a reader holds the cache: the grow goes on within its share and never waits");
        assert_eq!(reader.bytes(), 60);
        drop(reader);
        assert!(s.try_grow(1).is_err(), "past the share it spills, as before");
    }

    /// A cache inside the line that gives nothing back (a table's tail, the footers).
    struct Kept(usize);

    impl Charged for Kept {
        fn held(&self) -> usize { self.0 }
    }

    #[test]
    fn a_spiller_that_holds_nothing_yet_is_never_refused_for_bytes_the_caches_keep() {
        let ledger = Ledger::new(100, 100, true);
        let kept: Arc<dyn Charged> = Arc::new(Kept(90));
        ledger.charge("kept", Arc::downgrade(&kept));
        ledger.note();
        let a = ledger.pool();
        let sort = consumer(&a, "sort", true);
        sort.try_grow(15).expect("past the line, a sort's first batch has nothing to spill, so it grows within its share");
        assert!(sort.try_grow(10).is_err(), "holding bytes past the line, it spills them");
        sort.free();
        sort.try_grow(20).expect("and grows again from nothing");
        assert_eq!(ledger.reserved(), 20);
    }

    /// A cache the user gave a budget of its own (`Charged::inside`).
    struct Own(Mutex<ByteLru<usize, ()>>);

    impl Charged for Own {
        fn held(&self) -> usize { self.0.held() }

        fn give_back(&self, bytes: usize) -> usize { self.0.give_back(bytes) }

        fn inside(&self) -> bool { false }
    }

    #[test]
    fn a_cache_with_a_budget_of_its_own_keeps_it_beside_the_line_and_only_the_watch_takes_it_back() {
        let ledger = Ledger::new(100, 100, true);
        let own = Arc::new(Own(Mutex::new(ByteLru::new(usize::MAX))));
        for i in 0..8 {
            let _ = own.0.lock().unwrap().put(i, (), 10);
        }
        let charged: Arc<dyn Charged> = own.clone();
        ledger.charge("own", Arc::downgrade(&charged));
        let inside = charged_cache(&ledger, 2, 10);
        assert_eq!((ledger.note(), ledger.held()), (20, 100), "the line counts the caches inside it; `/metrics` all of them");
        let s = consumer(&ledger.pool(), "s", true);
        s.try_grow(90).expect("a grow takes back from the caches inside the line alone");
        assert_eq!((own.held(), inside.held(), ledger.cached()), (80, 10, 10));
        assert_eq!(ledger.give_back(30), 30, "the watch takes memory back from every cache, in the order they were charged");
        assert_eq!((own.held(), ledger.cached()), (50, 10), "and the line still counts only those inside it");
    }

    #[test]
    fn a_grow_that_finds_every_cache_held_by_a_reader_spills_at_once_and_never_waits() {
        let ledger = Ledger::new(100, 100, true);
        let cache = charged_cache(&ledger, 8, 10);
        ledger.note();
        let a = ledger.pool();
        let reader = cache.lock().unwrap(); // (a reader in the cache now)
        let (tx, rx) = std::sync::mpsc::channel();
        let grow = {
            let a = a.clone();
            std::thread::spawn(move || {
                let s = consumer(&a, "s", true);
                s.try_grow(10).expect("within the line, beside the caches");
                let refused = s.try_grow(50).is_err();
                tx.send(refused).unwrap();
                s
            })
        };
        let refused = rx.recv_timeout(std::time::Duration::from_secs(10)).expect("the grow answered while the reader held the cache");
        assert!(refused, "nothing came back, so the grow is refused, and the operator spills what it holds");
        assert_eq!(reader.bytes(), 80, "and the cache kept what it held");
        let held = consumer(&a, "held", false);
        held.try_grow(30).expect("what can't spill is held to its share alone, not failed for a reader's moment");
        drop(reader);
        let s = grow.join().unwrap();
        assert!(s.try_grow(50).is_err(), "until the watch looks again, a grow doesn't walk the caches for bytes it was refused");
        ledger.note();
        s.try_grow(50).expect("once the reader is done, the grow after the next look takes the cache bytes back");
        assert_eq!((cache.held(), ledger.cached(), ledger.reserved()), (10, 10, 90));
    }
}
