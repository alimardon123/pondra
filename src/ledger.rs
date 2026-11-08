//! The memory ledger (designs/redesign/redesign.md §3, step 1e): what this process holds in memory, kept in one place.
//!
//! - `ByteLru`, the one cache bounded by bytes. Each of the node's caches of that shape (a map, a running byte total, and a
//!   loop that drops the least recently used past a bound) was written by hand about ten times, each with its own total to
//!   get right; they all use this one now, so a bound is set, read and later reclaimed in one way.
//! - `Root`, the process's memory, and `Ledger`, a node's budget on it: its line for query memory, and `LakePool`, a lake's
//!   share of it as DataFusion's `MemoryPool`.
//! - `Charged`, a cache the ledger counts: what it holds, and memory it gives back when the node needs it. The lakes on a
//!   line charge their caches to it, so the node's memory watch takes memory back from all of them in one way.

use datafusion::common::human_readable_size;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use std::borrow::Borrow;
use std::hash::Hash;
use datafusion::common::instant::Instant;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
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

    /// Does it give memory back (`give_back`)? A cache that keeps what it holds, `give_back`'s default, says no: an idle
    /// node's watch looks every 2 s only while a cache holds what it could give back (`oom.rs`).
    fn gives_back(&self) -> bool { false }

    /// Does it hold its bytes inside the whole line, with the queries (`Ledger`)? Yes, unless the user gave it a budget of
    /// its own (`PONDRA_HOT_GB`, `PONDRA_CACHE_MB`): then it keeps that budget beside the line, and only the node's memory
    /// watch takes memory back from it.
    fn inside(&self) -> bool { true }
}

impl<K: Hash + Eq + Send, V: Send> Charged for Mutex<ByteLru<K, V>> {
    fn held(&self) -> usize { self.lock().unwrap_or_else(PoisonError::into_inner).bytes() }

    fn give_back(&self, bytes: usize) -> usize { give_back_from(self, |c| c, bytes) }

    fn gives_back(&self) -> bool { true }
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

/// How old the caches' count may be at a query's grow, in ms (`Ledger::fresh`).
const STALE_MS: u64 = 1000;
const NEVER: u64 = u64::MAX;

/// Each name once, in the order first given.
fn kinds_of(names: impl Iterator<Item = &'static str>) -> Vec<&'static str> {
    let mut kinds = vec![];
    for name in names {
        if !kinds.contains(&name) {
            kinds.push(name);
        }
    }
    kinds
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
/// spilling operator the caches didn't make room for spills at once. Unset, the queries' line is the whole line, at the
/// memory watch's trim line (`oom::trim_line`, `store::memory_limit`), and a grow past what the caches leave takes what they
/// give back and goes on within its share: the caches give way to the queries, and what the reservations don't count
/// (`array_agg` and the like) has the room between the trim line and the stop line. So the budgets no longer add up past
/// the machine's memory, and a query grows into what the caches hold rather than spilling beside them or the watch
/// stopping it there. What the caches hold is read at the watch's looks (`note`) and lowered by what each grow takes back, so a grow
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
    born: Instant,         // (what `noted` counts from)
    noted: AtomicU64,      // the last look (`note`), in ms since `born`: `NEVER` before the first
    caches: Mutex<Vec<(&'static str, Weak<dyn Charged>)>>, // what the node keeps on this line, by name (`charge`), in the order they give back
    root: Arc<Root>,       // the process's memory, which this node's budget is on (`Root`)
}

impl Ledger {
    /// A ledger whose queries hold `line` at most, inside `whole` with the caches (`line` itself when the setting is the whole
    /// line), alone on a root of its own sizes: a test's. A `hard` whole line refuses a spiller's grow the caches don't make
    /// room for; a soft one only takes cache bytes back.
    #[cfg(test)]
    pub fn new(line: usize, whole: usize, hard: bool) -> Arc<Ledger> { Ledger::on(&Root::new(line, whole), line, whole, hard) }

    /// A node's budget on `root`, as `new` takes its sizes: the node's lakes share it, and the root holds it to what the
    /// process's other nodes leave (`line_now`, `over`).
    pub fn on(root: &Arc<Root>, line: usize, whole: usize, hard: bool) -> Arc<Ledger> {
        let made = Arc::new(Ledger { line, whole, hard, reserved: AtomicUsize::new(0), busy: AtomicUsize::new(0), cached: AtomicUsize::new(0), dry: AtomicBool::new(false), born: Instant::now(), noted: AtomicU64::new(NEVER), caches: Default::default(), root: root.clone() });
        let mut nodes = root.nodes.lock().unwrap_or_else(PoisonError::into_inner);
        nodes.retain(|n| n.strong_count() > 0);
        nodes.push(Arc::downgrade(&made));
        drop(nodes);
        made
    }

    /// The root this budget is on: the process's memory.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn root(&self) -> &Arc<Root> { &self.root }

    /// A new lake's pool on this line.
    pub fn pool(self: &Arc<Self>) -> Arc<LakePool> { Arc::new(LakePool { ledger: self.clone(), part: Default::default() }) }

    pub fn line(&self) -> usize { self.line }

    /// The whole line, for the queries and the caches together.
    pub fn whole(&self) -> usize { self.whole }

    /// What the caches held at the last look, less what grows took back since: what a grow goes by.
    pub fn cached(&self) -> usize { self.cached.load(Relaxed) }

    /// A cache inside the whole line holds `bytes` more since the last look (`note`): a hot load's decode, while it runs.
    pub fn cache_grew(&self, bytes: usize) { self.cached_to(|held| held.saturating_add(bytes)); }

    /// A cache inside the whole line holds `bytes` less since the last look, of what it said it grew (`cache_grew`).
    pub fn cache_shrank(&self, bytes: usize) { self.cached_to(|held| held.saturating_sub(bytes)); }

    /// Sets what the caches are counted as holding (`cached`) to `to` of it, and the root's by as much, so the root's is
    /// every node's together.
    fn cached_to(&self, to: impl Fn(usize) -> usize) {
        let was = self.cached.fetch_update(Relaxed, Relaxed, |held| Some(to(held))).unwrap_or_else(|held| held);
        self.root.moved(&self.root.cached, was, to(was));
    }

    /// Adds `bytes` to what the queries hold, here and on the root.
    fn reserve(&self, bytes: usize) {
        self.reserved.fetch_add(bytes, Relaxed);
        self.root.reserved.fetch_add(bytes, Relaxed);
    }

    /// Takes `bytes` off what the queries hold, here and on the root.
    fn release(&self, bytes: usize) {
        self.reserved.fetch_sub(bytes, Relaxed);
        self.root.reserved.fetch_sub(bytes, Relaxed);
    }

    /// Reads what the caches inside the whole line hold now (`Charged::inside`), for the grows to go by until the next look,
    /// and says it. The node's memory watch reads it at each of its looks (`oom.rs`), so a grow never locks a cache to read it.
    pub fn note(&self) -> usize {
        self.noted.store(self.now(), Relaxed);
        let held = self.charged().iter().filter(|(_, c)| c.inside()).map(|(_, c)| c.held()).sum();
        self.cached_to(|_| held);
        self.dry.store(false, Relaxed);
        held
    }

    /// Notes the caches (`note`) when nothing has for [`STALE_MS`]: where no memory watch runs (`pondra sql`, a page), a
    /// query's grow is what notes them, so the whole line holds the caches there too (11-07 audit M4). Where the watch runs it
    /// looks at each request's start, and on a node's line at least every second while one runs, so there this seldom notes.
    fn fresh(&self) {
        let (now, noted) = (self.now(), self.noted.load(Relaxed));
        if (noted == NEVER || now.saturating_sub(noted) >= STALE_MS) && self.noted.compare_exchange(noted, now, Relaxed, Relaxed).is_ok() {
            self.note(); // (one grow notes: the others go by the last look meanwhile)
        }
    }

    fn now(&self) -> u64 { self.born.elapsed().as_millis() as u64 }

    /// What every lake's queries hold now.
    pub fn reserved(&self) -> usize { self.reserved.load(Relaxed) }

    /// A lake's share now: the line over the lakes running queries, at least one.
    pub fn share(&self) -> usize { self.line_now(true) / self.busy.load(Relaxed).max(1) }

    /// The node's line now: its own, less what the process's other nodes hold past the root's line. A spiller's is at
    /// least the node's even share of the root while other nodes run queries too, as a lake's share is within its node, so
    /// it spills there rather than being refused for what another node holds, and the other node's spillers are held to
    /// theirs at their next grow. A node alone on its root has its own line, whatever the root's: one node in a process,
    /// every node until several run in one, holds what it held before the root.
    fn line_now(&self, spills: bool) -> usize {
        let others = self.root.reserved().saturating_sub(self.reserved());
        if others == 0 {
            return self.line;
        }
        let free = self.root.line.saturating_sub(others);
        self.line.min(if spills { free.max(self.root.share()) } else { free })
    }

    /// Counts `cache` on this line, under `name`, for as long as it lives: `/metrics`' `cache_bytes` reads it by that name,
    /// and the node's memory watch takes memory back from it before it stops a query (`oom.rs`). Caches give back by kind,
    /// in the order the names were first charged, so a lake's kinds go in the order it lists them. A cache is charged once, though several lakes
    /// keep it (the ranges of a bucket two lakes are in, the node's planning facts).
    pub fn charge(&self, name: &'static str, cache: Weak<dyn Charged>) {
        let mut caches = self.caches.lock().unwrap_or_else(PoisonError::into_inner);
        caches.retain(|(_, c)| c.strong_count() > 0);
        if !caches.iter().any(|(_, c)| Weak::ptr_eq(c, &cache)) {
            caches.push((name, cache));
        }
    }

    /// The caches still alive, for the caller to hold a moment (a closed lake's go here), by kind: each name's together, in
    /// the order the names were first charged, so every lake's hot columns come before any lake's ranges (11-06 audit L7).
    /// The order they give memory back in.
    fn charged(&self) -> Vec<(&'static str, Arc<dyn Charged>)> {
        let mut caches = self.caches.lock().unwrap_or_else(PoisonError::into_inner);
        caches.retain(|(_, c)| c.strong_count() > 0);
        let kinds = kinds_of(caches.iter().map(|(name, _)| *name));
        let mut alive: Vec<_> = caches.iter().filter_map(|(name, c)| Some((*name, c.upgrade()?))).collect();
        alive.sort_by_key(|(name, _)| kinds.iter().position(|kind| kind == name)); // (stable: a name's in the order charged)
        alive
    }

    /// The names of the caches on this line, each once, in the order they were first charged: the order kinds give memory
    /// back in, on this line and across the node's lines (`oom.rs`).
    pub fn kinds(&self) -> Vec<&'static str> {
        let caches = self.caches.lock().unwrap_or_else(PoisonError::into_inner);
        kinds_of(caches.iter().filter(|(_, c)| c.strong_count() > 0).map(|(name, _)| *name))
    }

    /// Do the caches on this line hold anything they could give back (`Charged::gives_back`)?
    pub fn givable(&self) -> bool { self.charged().iter().any(|(_, c)| c.gives_back() && c.held() > 0) }

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

    /// Takes about `bytes` back from the caches on this line, a kind at a time (`charged`), and says how many came back:
    /// the node's memory watch, from every cache.
    pub fn give_back(&self, bytes: usize) -> usize { self.take(bytes, false, None) }

    /// Takes about `bytes` back from this line's caches named `kind` alone: the node's memory watch, a kind at a time
    /// across the node's lines (`oom.rs`).
    pub fn give_back_kind(&self, kind: &str, bytes: usize) -> usize { self.take(bytes, false, Some(kind)) }

    /// Takes cache bytes back for a query's grow past a whole line, from the caches inside the lines alone, and says whether
    /// that came back short: `own` past this node's line, from its own caches; `root` past the root's, from its own caches
    /// first, then from the other nodes' by what they hold, the most first, so a busy node doesn't empty a quiet
    /// neighbour's caches while its own still hold bytes.
    fn make_room(&self, own: usize, root: usize) -> bool {
        let mut freed = self.take(own.max(root), true, None);
        let short = freed < own;
        if freed < root {
            let mut others: Vec<Arc<Ledger>> = self.root.nodes().into_iter().filter(|n| !std::ptr::eq(n.as_ref(), self)).collect();
            others.sort_by_key(|n| std::cmp::Reverse(n.cached()));
            for other in others {
                if freed >= root {
                    break;
                }
                freed += other.take(root - freed, true, None);
            }
        }
        short || freed < root
    }

    fn take(&self, bytes: usize, inside_only: bool, kind: Option<&str>) -> usize {
        let (mut freed, mut inside) = (0, 0);
        for (name, c) in self.charged() {
            if freed >= bytes {
                break;
            }
            let counted = c.inside();
            if (inside_only && !counted) || kind.is_some_and(|kind| kind != name) {
                continue;
            }
            let got = c.give_back(bytes - freed);
            freed += got;
            inside += if counted { got } else { 0 };
        }
        self.cached_to(|held| held.saturating_sub(inside));
        freed
    }

    /// How far the queries holding `additional` bytes more would pass the node's whole line, beside what its caches hold,
    /// and how far past the root's, beside every node's: what the caches give back first, from those a reader isn't
    /// holding (`make_room`). A node alone on its root goes by its own line alone, as `line_now` does.
    fn over(&self, additional: usize) -> (usize, usize) {
        let past = |reserved: usize, cached: usize, whole: usize| reserved.saturating_add(additional).saturating_add(cached).saturating_sub(whole);
        let own = past(self.reserved(), self.cached(), self.whole);
        let (reserved, cached) = (self.root.reserved(), self.root.cached());
        let others = reserved.saturating_sub(self.reserved()).saturating_add(cached.saturating_sub(self.cached()));
        (own, if others == 0 { 0 } else { past(reserved, cached, self.root.whole) })
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        // (a node's budget gone: what it counted is no longer the root's)
        self.root.moved(&self.root.cached, self.cached.load(Relaxed), 0);
        self.root.moved(&self.root.reserved, self.reserved.load(Relaxed), 0);
    }
}

// ---------------------------------------------------------------- the process's memory

/// The process's memory (redesign step 2, review change 4): its line for every node's queries together, and its whole
/// line for them and every node's caches. Each node's budget (`Ledger`) is on it, as a child reservation is on its pool, so
/// two nodes in one process together hold no more than the process's line (`Ledger::line_now`), and past its whole line a
/// grow takes cache bytes back from any node's caches before it spills (`Ledger::make_room`). Its sizes are the machine's
/// and `PONDRA_MEMORY_MAX_GB`'s (`store::root_line`, `store::root_whole`); a node's own `PONDRA_MEMORY_GB` is its budget.
#[derive(Debug)]
pub struct Root {
    line: usize,           // every node's queries together, at most
    whole: usize,          // every node's queries and caches together (`usize::MAX`: none known)
    reserved: AtomicUsize, // every node's queries' reservations together
    cached: AtomicUsize,   // what every node's caches are counted as holding (`Ledger::cached`), together
    busy: AtomicUsize,     // nodes with a lake running queries now
    nodes: Mutex<Vec<Weak<Ledger>>>, // the nodes' budgets on it (`Ledger::on`)
}

impl Root {
    /// A root of `line` for the queries and `whole` for them and the caches.
    pub fn new(line: usize, whole: usize) -> Arc<Root> {
        Arc::new(Root { line, whole, reserved: AtomicUsize::new(0), cached: AtomicUsize::new(0), busy: AtomicUsize::new(0), nodes: Default::default() })
    }

    pub fn line(&self) -> usize { self.line }

    pub fn whole(&self) -> usize { self.whole }

    /// Every node's queries' reservations now.
    pub fn reserved(&self) -> usize { self.reserved.load(Relaxed) }

    /// What every node's caches were counted as holding at their last looks, less what grows took back since.
    pub fn cached(&self) -> usize { self.cached.load(Relaxed) }

    /// A node's even share of the line while several run queries.
    fn share(&self) -> usize { self.line / self.busy.load(Relaxed).max(1) }

    /// The nodes' budgets alive now: the memory watch's walk (`oom.rs`) and a grow's past the whole line (`make_room`).
    pub fn nodes(&self) -> Vec<Arc<Ledger>> {
        self.nodes.lock().unwrap_or_else(PoisonError::into_inner).iter().filter_map(Weak::upgrade).collect()
    }

    /// Moves `count` by what one node's count moved, from `was` to `now`.
    fn moved(&self, count: &AtomicUsize, was: usize, now: usize) {
        match now.cmp(&was) {
            std::cmp::Ordering::Greater => count.fetch_add(now - was, Relaxed),
            std::cmp::Ordering::Less => count.fetch_sub(was - now, Relaxed),
            std::cmp::Ordering::Equal => 0,
        };
    }
}

// (the process's memory: one root for the sizes its settings and machine gave at its start, so one in any process but a
// test's, whose lakes opened under other settings get a root of their own)
static ROOTS: Mutex<Vec<Weak<Root>>> = Mutex::new(Vec::new());

/// The process's root of `line` and `whole` (`store::lake_pool`): the one there is, else a new one.
pub fn root(line: usize, whole: usize) -> Arc<Root> {
    let mut roots = ROOTS.lock().unwrap_or_else(PoisonError::into_inner);
    roots.retain(|r| r.strong_count() > 0);
    if let Some(root) = roots.iter().filter_map(Weak::upgrade).find(|r| (r.line, r.whole) == (line, whole)) {
        return root;
    }
    let made = Root::new(line, whole);
    roots.push(Arc::downgrade(&made));
    made
}

/// Every node's budget in the process now, on every root: what the memory watch takes memory back from (`oom.rs`).
pub fn budgets() -> Vec<Arc<Ledger>> {
    let roots: Vec<Arc<Root>> = ROOTS.lock().unwrap_or_else(PoisonError::into_inner).iter().filter_map(Weak::upgrade).collect();
    roots.iter().flat_map(|r| r.nodes()).collect()
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

    /// What `r` may grow by within the lake's room (`room`), by `FairSpillPool`'s rules, and the room.
    fn available(&self, p: &Part, r: &MemoryReservation) -> (usize, usize) {
        let spills = r.consumer().can_spill();
        let room = self.room(p, spills);
        let available = match spills {
            // (no spiller past its part of what the lake's unspillable memory leaves of its room)
            true => room.saturating_sub(p.unspillable).checked_div(p.spillers).unwrap_or(room).saturating_sub(r.size()),
            false => room.saturating_sub(p.unspillable + p.spillable),
        };
        (available, room)
    }

    /// What the lake's queries may hold now: the line less what the other lakes' queries hold, so a lake running one small
    /// SELECT beside it doesn't halve its room (11-06 audit M10). A spiller has at least the lake's even share of the line
    /// (`Ledger::share`) while others hold the rest, as between pools of their own: the other lakes' spillers are held to
    /// theirs at their next grow, and spill.
    fn room(&self, p: &Part, spills: bool) -> usize {
        let l = &self.ledger;
        let line = l.line_now(spills); // (the node's line, less what other nodes hold past the root's)
        let free = line.saturating_sub(l.reserved().saturating_sub(p.spillable + p.unspillable));
        if spills { free.max(line / l.busy.load(Relaxed).max(1)) } else { free }
    }

    /// The refusal of a grow, which a spilling operator answers by spilling and any other query by stopping.
    /// (`room`: the lake's, which the error names, `LakePool::line`)
    fn refused(&self, r: &MemoryReservation, additional: usize, available: usize, room: usize) -> DataFusionError {
        DataFusionError::ResourcesExhausted(format!(
            "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}",
            human_readable_size(additional),
            r.consumer().name(),
            human_readable_size(r.size()),
            human_readable_size(available),
            self.line(room),
        ))
    }

    /// The line as an error says it: the lake's room in it, and the caches beside the queries.
    fn line(&self, room: usize) -> String {
        let l = &self.ledger;
        match l.whole {
            usize::MAX => format!("ledger(this database's room: {} of {})", human_readable_size(room), human_readable_size(l.line)),
            whole => format!("ledger(this database's room: {} of {}, caches {} of {} with queries)", human_readable_size(room), human_readable_size(l.line), human_readable_size(l.cached()), human_readable_size(whole)),
        }
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
        if p.consumers == 0 && self.ledger.busy.fetch_add(1, Relaxed) == 0 {
            self.ledger.root.busy.fetch_add(1, Relaxed); // (the node's first lake running queries)
        }
        p.consumers += 1;
        p.spillers += usize::from(c.can_spill());
    }

    fn unregister(&self, c: &MemoryConsumer) {
        let mut p = self.part();
        p.consumers = p.consumers.saturating_sub(1);
        p.spillers = p.spillers.saturating_sub(usize::from(c.can_spill()));
        if p.consumers == 0 && self.ledger.busy.fetch_sub(1, Relaxed) == 1 {
            self.ledger.root.busy.fetch_sub(1, Relaxed); // (its last)
        }
    }

    fn grow(&self, r: &MemoryReservation, additional: usize) {
        *self.part().of(r) += additional;
        self.ledger.reserve(additional);
    }

    fn shrink(&self, r: &MemoryReservation, shrink: usize) {
        let mut p = self.part();
        let held = p.of(r);
        *held = held.saturating_sub(shrink);
        self.ledger.release(shrink);
    }

    fn try_grow(&self, r: &MemoryReservation, additional: usize) -> Result<()> {
        self.ledger.fresh(); // (before the lake's lock: a note locks each cache a moment)
        let mut p = self.part();
        let (available, room) = self.available(&p, r);
        if additional > available {
            return Err(self.refused(r, additional, available, room));
        }
        let (own, root) = self.ledger.over(additional);
        if own.max(root) > 0 {
            // A take-back that came back short (a reader in a cache, or bytes a cache can't give back: a table's tail, the
            // footers) isn't asked for again until the memory watch's next look, so a query growing near the line doesn't
            // walk the caches at every grow for bytes that aren't there.
            let mut short = self.ledger.dry.load(Relaxed);
            if !short {
                // (the cache bytes go outside the lake's lock, so its other grows don't wait on them; then its share is read again)
                drop(p);
                short = self.ledger.make_room(own, root);
                if short {
                    self.ledger.dry.store(true, Relaxed);
                }
                p = self.part();
                let (available, room) = self.available(&p, r);
                if additional > available {
                    return Err(self.refused(r, additional, available, room));
                }
            }
            if short && self.ledger.hard && r.consumer().can_spill() && r.size() > 0 {
                // On the user's line a spiller spills at once rather than wait on a reader. One that holds nothing yet has
                // nothing to spill (a sort's first batch would fail its query: "Not enough memory to continue external
                // sort"), and what can't spill (a hash join's build side) would fail its query for a reader's moment in a
                // cache, so both are held to their share alone, as before, with the memory watch behind them, and take back
                // what the caches give. On the default line every grow does that: it spills where it did before, and the
                // caches it found free gave their bytes back.
                return Err(self.refused(r, additional, 0, self.room(&p, true)));
            }
        }
        *p.of(r) += additional;
        self.ledger.reserve(additional);
        Ok(())
    }

    /// What this lake's queries hold (`Ledger::reserved` is every lake's).
    fn reserved(&self) -> usize {
        let p = self.part();
        p.spillable + p.unspillable
    }

    fn memory_limit(&self) -> MemoryLimit { MemoryLimit::Finite(self.room(&self.part(), true)) }
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

    /// A lake's room is the line less what the other lakes' queries hold (11-06 audit M10): B's one small statement doesn't
    /// halve A's, so A's hash join that needs 60% of the line fits, and its refusal names A's room. A spiller still has its
    /// lake's even share while the other lake holds the rest.
    #[test]
    fn a_lakes_room_is_the_line_less_what_the_others_hold() {
        let ledger = Ledger::new(100, usize::MAX, false);
        let (a, b) = (ledger.pool(), ledger.pool());
        let small = consumer(&b, "b's small SELECT", false);
        small.try_grow(1).unwrap();
        let join = consumer(&a, "a's hash join", false);
        assert!(join.try_grow(60).is_ok(), "60 of the 99 B leaves, not of half the line");
        assert!(join.try_grow(39).is_ok());
        let e = join.try_grow(1).unwrap_err().to_string();
        assert!(e.contains("0.0 B remain available for the total memory pool: ledger(this database's room: 99.0 B of 100.0 B)"), "{e}");
        let sorter = consumer(&b, "b's sort", true);
        assert_eq!(limit(&b), Some(50));
        assert!(sorter.try_grow(49).is_ok() && sorter.try_grow(1).is_err(), "B's half, less its small SELECT's 1");
        drop(join);
        assert!(sorter.try_grow(50).is_ok(), "A done: the line less B's own");
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
    fn a_line_takes_memory_back_from_its_caches_by_kind_and_forgets_the_dropped_ones() {
        let ledger = Ledger::new(1000, usize::MAX, false);
        let cache = |bytes: &[usize]| {
            let c = Arc::new(Mutex::new(ByteLru::new(1000)));
            for (i, &b) in bytes.iter().enumerate() {
                let _ = c.lock().unwrap().put(i, (), b);
            }
            c
        };
        let (first, second, third) = (cache(&[100, 100]), cache(&[50, 50]), cache(&[50, 20]));
        for (name, c) in [("first", &first), ("second", &second), ("first", &third)] {
            let c: Arc<dyn Charged> = c.clone();
            ledger.charge(name, Arc::downgrade(&c));
            ledger.charge(name, Arc::downgrade(&c)); // (charged once, however many lakes keep it)
        }
        assert_eq!((ledger.held(), ledger.held_by_name(), ledger.kinds()), (370, vec![("first", 270), ("second", 100)], vec!["first", "second"]));
        // (as two lakes' hot columns and one's ranges: the second lake's hot columns go before the first's ranges, 11-06 audit L7)
        assert_eq!(ledger.give_back(250), 250, "the first kind's caches, each in the order charged and its least recently used first");
        assert_eq!((first.held(), second.held(), third.held()), (0, 100, 20));
        assert_eq!(ledger.give_back_kind("second", 50), 50, "a kind alone");
        assert_eq!((first.held(), second.held(), third.held()), (0, 50, 20));
        let held = third.lock().unwrap();
        assert_eq!(ledger.give_back(1000), 50, "a cache a reader holds now is skipped, and asked again at the next look");
        drop(held);
        drop(second);
        assert_eq!((ledger.held(), ledger.kinds()), (20, vec!["first"]), "a dropped cache is let go of");
        assert!(ledger.givable());
        assert_eq!(ledger.give_back(1000), 20);
        assert_eq!((ledger.held(), ledger.caches.lock().unwrap().len()), (0, 2));
        assert!(!ledger.givable(), "nothing left to give back");
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
        assert!(!ledger.givable(), "what a cache keeps isn't what it could give back (11-06 audit L8)");
        ledger.note();
        let a = ledger.pool();
        let sort = consumer(&a, "sort", true);
        sort.try_grow(15).expect("past the line, a sort's first batch has nothing to spill, so it grows within its share");
        assert!(sort.try_grow(10).is_err(), "holding bytes past the line, it spills them");
        sort.free();
        sort.try_grow(20).expect("and grows again from nothing");
        assert_eq!(ledger.reserved(), 20);
    }

    /// A hot load's decode holds its estimate on the line while it runs (`Hot::load`, 11-07 audit L6): a grow meanwhile
    /// has only what it leaves.
    #[test]
    fn a_decode_running_holds_its_estimate_on_the_line() {
        let ledger = Ledger::new(100, 100, true);
        let a = ledger.pool();
        let sort = consumer(&a, "sort", true);
        ledger.note(); // (the watch's look: the grows go by it, and the decode's estimate on it, for a second)
        ledger.cache_grew(60);
        sort.try_grow(40).expect("beside the decode's 60");
        assert!(sort.try_grow(1).is_err(), "nothing past it");
        ledger.cache_shrank(60);
        sort.try_grow(60).expect("the decode is over");
        assert_eq!(ledger.cached(), 0);
    }

    /// Where no memory watch looks (`pondra sql`, a page), a query's grow notes the caches itself (11-07 audit M4): the whole
    /// line holds them there too.
    #[test]
    fn where_no_watch_looks_a_grow_notes_the_caches() {
        let ledger = Ledger::new(100, 100, true);
        let kept: Arc<dyn Charged> = Arc::new(Kept(70));
        ledger.charge("kept", Arc::downgrade(&kept));
        assert_eq!(ledger.cached(), 0, "no look yet");
        let a = ledger.pool();
        let sort = consumer(&a, "sort", true);
        sort.try_grow(30).expect("beside the 70 the cache holds");
        assert_eq!(ledger.cached(), 70, "the grow looked");
        assert!(sort.try_grow(1).is_err(), "and nothing past them");
    }

    /// A cache the user gave a budget of its own (`Charged::inside`).
    struct Own(Mutex<ByteLru<usize, ()>>);

    impl Charged for Own {
        fn held(&self) -> usize { self.0.held() }

        fn give_back(&self, bytes: usize) -> usize { self.0.give_back(bytes) }

        fn gives_back(&self) -> bool { true }

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

    /// Two nodes in one process together hold no more than its line (review change 4): each has a budget of 100 on a root of
    /// 150, so the second's memory that can't spill is held to what the first leaves of the root, though its own budget
    /// has room; a spiller there has its node's even share of the root and spills past it; and a node alone on its root has
    /// its own budget, whatever the root's, as before the root.
    #[test]
    fn two_nodes_on_one_root_hold_no_more_than_its_line() {
        let root = Root::new(150, usize::MAX);
        let (a, b) = (Ledger::on(&root, 100, usize::MAX, false), Ledger::on(&root, 100, usize::MAX, false));
        let (pa, pb) = (a.pool(), b.pool());
        let held_a = consumer(&pa, "a", false);
        held_a.try_grow(100).expect("a node alone on its root has its whole budget");
        let held_b = consumer(&pb, "b", false);
        assert!(held_b.try_grow(51).is_err(), "what a holds leaves b 50 of the root");
        held_b.try_grow(50).expect("b's 50");
        assert_eq!((root.reserved(), a.reserved(), b.reserved()), (150, 100, 50));
        drop(held_b);
        let spills = consumer(&pb, "b's sort", true);
        spills.try_grow(75).expect("a spiller has its node's even share of the root, 150 over two nodes");
        assert!(spills.try_grow(1).is_err(), "and spills past it");
        drop((held_a, spills));
        assert_eq!(root.reserved(), 0, "all given back, on the root too");
        let alone = Ledger::on(&Root::new(50, usize::MAX), 100, usize::MAX, false);
        consumer(&alone.pool(), "alone", false).try_grow(100).expect("a node alone on its root holds its budget past the root's line");
    }

    /// Past the root's whole line, a grow takes cache bytes back from its own node's caches first, then from the other
    /// nodes' by what they hold, the most first, instead of every node's caches growing past it (review change 4; the
    /// redesign thread's order, 11-08: a busy node doesn't empty a quiet neighbour's caches while its own hold bytes).
    #[test]
    fn past_the_roots_whole_line_a_grow_takes_its_own_nodes_caches_first_then_the_fullest_others() {
        let root = Root::new(1000, 300);
        let [a, b, c] = [0; 3].map(|_| Ledger::on(&root, 1000, usize::MAX, false));
        let (ca, cb, cc) = (charged_cache(&a, 10, 10), charged_cache(&b, 5, 10), charged_cache(&c, 12, 10));
        for node in [&a, &b, &c] {
            node.note();
        }
        assert_eq!(root.cached(), 270, "the root counts every node's caches");
        let q = consumer(&a.pool(), "a's query", false);
        q.try_grow(100).expect("100 past what the caches leave of 300: a's own caches give the 70");
        assert_eq!((ca.held(), cb.held(), cc.held()), (30, 50, 120), "a's own first, the others untouched");
        q.try_grow(130).expect("130 more: a's last 30, then 100 of the fullest other node's");
        assert_eq!((ca.held(), cb.held(), cc.held()), (0, 50, 20), "c held the most, b keeps its 50");
        assert_eq!((root.cached(), root.reserved()), (70, 230));
        drop(q);
        drop(c);
        assert_eq!(root.cached(), 50, "a node's budget gone, its caches no longer count on the root");
    }
}
