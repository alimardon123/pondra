//! A node stops the query that grows it past its memory line, rather than be killed for memory.
//!
//! Some operators grow without asking the query memory pool: `array_agg`, `median` and `string_agg`
//! over huge groups, and the last steps that build their answers. The pool can't refuse what it
//! isn't asked for, so on a machine or in a container with too little memory the OS killed the whole
//! node: `array_agg` of 30 M values in 3 groups peaked at 1,941 MB resident. A cap that looked only
//! when something asked the pool found 1.8–1.9 GB already resident when it refused, no better than
//! none. So a watch reads the process's resident memory on a timer, whatever asked for it, and when
//! it passes the node's line it stops the request that grew it: its future is dropped, so its memory
//! is let go, and it answers 53200. The node stays up and answers the rest. Which request grew it is
//! read from the allocator (`Meter`): each request is charged what its own polls allocated, less what
//! they freed, so a long cheap request started early is not stopped for a query that grew the node.
//! The one watch lets the hot columns (`hot.rs`) go first, past seven tenths of the line, since a column
//! is decoded again when it is read; it stops a query only past the line itself.
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use datafusion::common::instant::Instant;
use std::time::Duration;

/// What a stopped request answers (SQLSTATE 53200: `codes::by_words` reads "out of memory").
pub const OOM: &str = "out of memory: the node was nearly out of memory and stopped this query, the one that grew it (PONDRA_MEMORY_MAX_GB); a write in it may or may not have been applied";

/// What a query the page's watch stopped answers (SQLSTATE 53200: `codes::of` reads "out of memory"): the page's words, as `OOM` is the node's.
pub(crate) const PAGE_OOM: &str = "out of memory: the page was nearly out of memory and stopped this query, the one that grew it (memory_max_gb)";

/// How long a stop holds the watch: the node's allocator gives freed pages back over about ten
/// seconds (`main.rs`), so resident memory falls after a stop, and a second stop would be for memory
/// the first one already let go.
const HOLD: Duration = Duration::from_secs(10);

/// How long a request must have run to be stopped: one that has only just started did not grow the
/// memory the watch sees, and its own cost is still arriving. (A second let `array_agg` of 30M values
/// reach 1.75 GB on a 1 GB line on CI's runner, where it grows faster.)
const FIRST: Duration = Duration::from_millis(200);

/// How fast the watch takes a query to grow, at most: it looks again before memory growing at this
/// rate could pass the line, every 20 ms near it and every second at most far below it.
const RATE: f64 = 8.0 * (1u64 << 30) as f64;

crate::per_node!(static SEEN: AtomicUsize = AtomicUsize::new(0));
// (the watch is waiting: a request that starts then has no reading until the watch's next look gives it one)
crate::per_node!(static IDLE: AtomicBool = AtomicBool::new(false));
// (the caches the watch may give memory back from: a lake opened in this node registers its hot columns)
crate::per_node!(static GIVERS: std::sync::Mutex<Vec<Weak<crate::hot::Hot>>> = Default::default());
// (a wake-up holding no data: a request started, or the hot columns grew, so the watch looks again now)
crate::per_node!(static WAKE: tokio::sync::Notify = tokio::sync::Notify::new(); clear = |_| ());

/// The node's memory line in bytes: `PONDRA_MEMORY_MAX_GB`, else 85% of the machine's or container's
/// memory. None when it is 0 (off), or in a host's process, where the memory is the host's.
pub fn line() -> Option<usize> {
    // (a page's line is its device's, and it has one only where its memory is read: `store::resident`)
    if cfg!(target_arch = "wasm32") {
        return crate::store::resident().and_then(|_| line_on(crate::env::device_line()));
    }
    if crate::node::hosted() {
        return None;
    }
    crate::env::memory_line(crate::store::ram().map(|ram| (ram as f64 * 0.85) as usize))
}

/// The memory line on a page's device, which has `device` bytes (`Options::line`, or the one its member was opened with): the
/// device's, else `env::PAGE_LINE`, through `PONDRA_MEMORY_MAX_GB` as `env::memory_line` takes it.
pub(crate) fn line_on(device: Option<usize>) -> Option<usize> {
    crate::env::memory_line(Some(device.unwrap_or(crate::env::PAGE_LINE)))
}

/// A request watched at each of its polls, where no watch loop can run beside it (a page's one thread): before each poll,
/// past `line` by `reading` and grown by its own polls by a tenth of the line (`pick`'s margin), it is stopped: its
/// future dropped, its memory let go, and `Err(())` answered (the caller says why).
#[cfg(target_arch = "wasm32")] // (its one caller, `panics::work`, is a page's: a node has a watch loop)
pub(crate) async fn polled<T>(reading: impl Fn() -> usize, line: usize, f: impl Future<Output = T>) -> Result<T, ()> {
    let margin = line / 10; // (`pick`'s margin, a node's)
    let mut inner = Some(Box::pin(f));
    let mut grown = 0usize; // (what this query's polls grew the page by, less what they let go)
    std::future::poll_fn(move |cx| {
        let now = reading();
        if now >= line && grown >= margin {
            inner = None; // (the query's memory goes with its future, before anything else is read)
            return Poll::Ready(Err(()));
        }
        let Some(future) = inner.as_mut() else { return Poll::Ready(Err(())) }; // (never polled once it has answered: an answer is the stop's)
        let out = future.as_mut().poll(cx);
        let after = reading();
        grown = grown.saturating_add(after.saturating_sub(now)).saturating_sub(now.saturating_sub(after));
        out.map(Ok)
    })
    .await
}

/// The resident memory the watch read last (0 before its first look). While the watch waits it is
/// `usize::MAX`, not read yet: a request that starts then takes the watch's next reading as its own
/// (`panics::note`), and `pick` passes it over until then.
pub fn seen() -> usize {
    if IDLE.load(Relaxed) {
        usize::MAX
    } else {
        SEEN.load(Relaxed)
    }
}

/// The resident memory past which the hot columns are given back: seven tenths of the line, so the
/// cache goes before a query is stopped. With no line (a host's process, or `PONDRA_MEMORY_MAX_GB=0`),
/// three fifths of the machine's memory, as the cache had it before this watch: the host's memory isn't
/// ours to stop queries for, but our own cache is ours to drop.
pub fn trim_line() -> Option<usize> {
    line().map(of_line).or_else(|| Some(crate::store::ram()? / 5 * 3))
}

/// Seven tenths of a line: 85% of RAM by default, which is 60% of RAM, where the hot columns were
/// trimmed before this watch took them over.
fn of_line(line: usize) -> usize {
    line / 10 * 7
}

/// Lets `hot`'s decoded columns be given back when the node needs memory: the watch (`start`) trims
/// them past [`trim_line`], before it stops any query. A lake opened outside a node (`pondra sql`,
/// `pondra catalog`, `pondra backup`) is held to the cache's own budget alone, since no watch runs in
/// its process.
pub fn gives_back(hot: &Arc<crate::hot::Hot>) {
    if hot.on() {
        GIVERS.lock().unwrap_or_else(|e| e.into_inner()).push(Arc::downgrade(hot));
        wake();
    }
}

/// Wakes the watch, so it looks again now. `notify_one` keeps a wake sent between a look and the wait
/// after it, so none is lost.
pub fn wake() {
    WAKE.notify_one()
}

/// The caches still alive, upgraded for the caller to hold a moment (dead entries are dropped here: a
/// closed lake's columns go with its last `Arc`, and the caller must not keep one over a wait).
fn givers() -> Vec<Arc<crate::hot::Hot>> {
    let mut list = GIVERS.lock().unwrap_or_else(|e| e.into_inner());
    list.retain(|w| w.strong_count() > 0);
    list.iter().filter_map(Weak::upgrade).collect()
}

/// Starts the watch, when the process can say how much memory it holds and the hot columns can be given
/// back (`trim_line`). The line is checked only where there is one (a node's own process). It runs on a thread of
/// its own (`panics::alone`), which a node stopped ends, as its other loops end.
pub fn start() {
    // (a page's one thread can't run this loop beside a query: a page's requests are watched at each poll instead, `panics::work`)
    if cfg!(target_arch = "wasm32") {
        return;
    }
    let Some(trim) = trim_line() else { return };
    if crate::store::resident().is_none() {
        return;
    }
    let line = line(); // (None in a host's process: there the watch only gives the cache back)
    if line.is_some() {
        // DataFusion's spawned work charges the request that spawned it, as the request's own polls do (`charge`).
        // (A tracer a program embedding the library set first is kept: its error is ignored.)
        let _ = datafusion::common::runtime::set_join_set_tracer(&TRACER);
    }
    crate::panics::alone(async move {
        let mut last_stop = None; // when the last stop was made: it holds the watch (`pending`)
        let mut said = false; // the line for this stretch over the line, said once
        loop {
            let resident = crate::store::resident().unwrap_or(0);
            SEEN.store(resident, Relaxed); // (what the OS says: `noted` and `pick` compare it)
            crate::panics::note(resident); // (a request that started unread takes this look's reading, before any stop)
            let now = Instant::now();
            let mut felt = resident;
            if felt > trim {
                // The cache goes first: a column is decoded again when it is read, a query can't be.
                let need = felt - trim;
                let holders = givers();
                let mut freed = 0;
                for hot in &holders {
                    if freed >= need {
                        break;
                    }
                    freed += hot.trim(need - freed);
                }
                drop(holders); // (held over a wait, a closed lake's columns would stay in memory)
                if freed > 0 {
                    // The pages just freed would stay resident for seconds (`purge`), so they are handed back now:
                    // the reading after it is what the process holds, and the watch goes by that alone.
                    purge();
                    felt = crate::store::resident().unwrap_or(felt);
                    SEEN.store(felt, Relaxed);
                    crate::panics::note(felt);
                }
            }
            if let Some(line) = line {
                if felt < line {
                    said = false;
                }
                if due(last_stop, felt, line, now) {
                    let mb = |bytes: usize| bytes >> 20;
                    if crate::panics::stop_one(now, line / 10) {
                        crate::metrics::add(&crate::metrics::STOPPED_FOR_MEMORY, 1);
                        eprintln!("stopped a query: the node held {} MB, its line {} MB (PONDRA_MEMORY_MAX_GB)", mb(felt), mb(line));
                        last_stop = Some(now);
                    } else if !std::mem::replace(&mut said, true) {
                        // (nothing to stop yet: a request old enough that grew the node is stopped at a later look,
                        // so no hold starts here; the line is said once per stretch over the line)
                        eprintln!("the node holds {} MB, past its line {} MB, and no query grew it", mb(felt), mb(line));
                    }
                }
            }
            // The wait: as often as the trim and the stop need while a request runs (a plain sleep, since a wake on every
            // request would look on every statement); otherwise until a request starts, the cache changes, or
            // the timer ends: 2 s while the cache holds columns (Flight, Kafka and tiering grow the node outside
            // `work`, and the cache is what can answer them), else 30 s.
            match line {
                Some(line) if crate::panics::running() => tokio::time::sleep(every(felt, if felt < trim { trim } else { line })).await,
                _ => {
                    let timer = if givers().iter().any(|h| h.usage().0 > 0) { Duration::from_secs(2) } else { Duration::from_secs(30) };
                    IDLE.store(true, Relaxed);
                    let _ = tokio::time::timeout(timer, WAKE.notified()).await;
                    IDLE.store(false, Relaxed);
                }
            }
        }
    });
}

/// When to look again: before memory growing at [`RATE`] could pass the next mark from where it is now, the trim
/// line while under it (the cache must go before the line comes), else the line. (Looking every second while under
/// half the line, a query that grew from there in that second passed a 1 GB line by 0.75 GB before the watch saw it.)
fn every(resident: usize, mark: usize) -> Duration {
    Duration::from_secs_f64(mark.saturating_sub(resident) as f64 / RATE).clamp(Duration::from_millis(20), Duration::from_secs(1))
}

/// Is a request to be stopped now: over the line, and no stop still holding.
fn due(last_stop: Option<Instant>, resident: usize, line: usize, now: Instant) -> bool {
    resident >= line && !pending(last_stop, resident, line, now)
}

/// Is a stop still holding: the node is over its line, and the last stop was less than [`HOLD`] ago. (A
/// look that finds nothing to stop holds nothing: a request old enough by the next look is stopped then.)
fn pending(last_stop: Option<Instant>, resident: usize, line: usize, now: Instant) -> bool {
    resident >= line && last_stop.is_some_and(|at| now.saturating_duration_since(at) < HOLD)
}

// The request's memory. Each thread of jemalloc keeps two counters, the bytes it has allocated and the
// bytes it has freed (`thread.allocatedp`, `thread.deallocatedp`: no stats build needed). A poll of a future
// runs on one thread, so the difference of the two counters across the poll is what that poll did, and a
// request charged each poll's difference is charged its own memory, less what it gave back. jemalloc
// is the node's allocator on Linux with glibc (`main.rs`); a program with another allocator, and every
// other OS, has no counters: `thread_net` is then 0, and every request counts 0, so `pick` goes by age.
#[cfg(all(feature = "doors", target_os = "linux", target_env = "gnu"))]
mod counters {
    use std::cell::Cell;

    thread_local! {
        // This thread's two counters' addresses, read on its first poll (null where jemalloc has none).
        static POINTERS: Cell<Option<(*const u64, *const u64)>> = const { Cell::new(None) };
    }

    // The address jemalloc keeps for the counter `name` (null where it keeps none).
    fn address(name: &std::ffi::CStr) -> *const u64 {
        crate::jemalloc::read(name).unwrap_or(std::ptr::null())
    }

    pub fn net() -> i64 {
        let (allocated, freed) = POINTERS.with(|c| match c.get() {
            Some(p) => p,
            None => {
                let p = (address(c"thread.allocatedp"), address(c"thread.deallocatedp"));
                c.set(Some(p));
                p
            }
        });
        if allocated.is_null() || freed.is_null() {
            return 0;
        }
        // SAFETY: both are this thread's own counters, which jemalloc keeps for the thread's life.
        unsafe { (*allocated as i64).wrapping_sub(*freed as i64) }
    }

    // Every arena's free pages go back to the OS (4096 is `MALLCTL_ARENAS_ALL`); the command takes no value, and a
    // failed purge leaves the pages where they were, so its return code is ignored.
    pub fn purge() {
        crate::jemalloc::write(c"arena.4096.purge", ());
    }
}

/// Bytes this thread has allocated less the bytes it has freed (see above); 0 where there are no counters.
#[cfg(all(feature = "doors", target_os = "linux", target_env = "gnu"))]
fn thread_net() -> i64 {
    counters::net()
}

/// Bytes this thread has allocated less the bytes it has freed; 0 where there are no counters.
#[cfg(not(all(feature = "doors", target_os = "linux", target_env = "gnu")))]
fn thread_net() -> i64 {
    0
}

/// Hands the allocator's free pages back to the OS now. jemalloc keeps freed pages about ten seconds (`main.rs`):
/// purged, resident memory read after this says what the process holds, and the watch goes by that. Where jemalloc
/// isn't the allocator (another OS, or a program embedding the library), there is nothing to purge, and a big free
/// goes back at once.
#[cfg(all(feature = "doors", target_os = "linux", target_env = "gnu"))]
fn purge() {
    counters::purge()
}

// (no jemalloc to purge here: see the version above)
#[cfg(not(all(feature = "doors", target_os = "linux", target_env = "gnu")))]
fn purge() {}

// The tests run on jemalloc too, as the node does, so the meter has counters to read there.
#[cfg(all(test, feature = "doors", target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static TEST_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// A future that charges what its polls allocate, less what they free, to `used`. The poll reads the
/// thread's counters before and after the inner poll, so what the inner future does on its thread is
/// what counts; work it only starts (a spawned task, a blocking closure) is charged where it runs, through
/// [`charge`] and [`TRACER`]. The inner future is boxed, so the meter is `Unpin` and needs no `unsafe`.
pub struct Meter<F> {
    inner: Pin<Box<F>>,
    used: Arc<AtomicI64>,
}

impl<F: Future> Meter<F> {
    pub fn new(inner: F, used: Arc<AtomicI64>) -> Self {
        Meter { inner: Box::pin(inner), used }
    }
}

impl<F: Future> Future for Meter<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        let before = thread_net();
        let out = this.inner.as_mut().poll(cx);
        this.used.fetch_add(thread_net().wrapping_sub(before), Relaxed); // (one relaxed add per poll)
        out
    }
}

tokio::task_local! {
    /// The memory counter of the request the current task is part of, set by [`charge`]. Work it spawns
    /// (DataFusion's tasks and blocking closures, through [`TRACER`]) charges the same counter.
    static REQUEST: Arc<AtomicI64>;
}

/// Runs `f` as a request whose memory is `used`: its polls are metered, and the work it spawns is too.
/// Work that Pondra spawns itself is not: the hot columns' decoding (`hot.rs`), which the cache owns, and a
/// write's Parquet encoding (`tier.rs`), which is bounded.
pub fn charge<F: Future>(used: Arc<AtomicI64>, f: F) -> impl Future<Output = F::Output> {
    REQUEST.scope(used.clone(), Meter::new(f, used))
}

/// DataFusion's tracer (`datafusion::common::runtime`): a task or a blocking closure it spawns while a
/// request runs is charged to that request, wherever it runs. Outside a request nothing changes.
struct Tracer;

static TRACER: Tracer = Tracer;

impl datafusion::common::runtime::JoinSetTracer for Tracer {
    fn trace_future(
        &self,
        fut: futures::future::BoxFuture<'static, Box<dyn std::any::Any + Send>>,
    ) -> futures::future::BoxFuture<'static, Box<dyn std::any::Any + Send>> {
        use futures::FutureExt;
        match REQUEST.try_with(Arc::clone) {
            Ok(used) => charge(used, fut).boxed(),
            Err(_) => fut,
        }
    }

    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn std::any::Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn std::any::Any + Send> + Send> {
        match REQUEST.try_with(Arc::clone) {
            Ok(used) => Box::new(move || {
                let before = thread_net();
                let out = f();
                used.fetch_add(thread_net().wrapping_sub(before), Relaxed);
                out
            }),
            Err(_) => f,
        }
    }
}

/// A request running through `panics::work`, as the choice of which to stop sees it (`panics::RUNNING`,
/// by id).
pub struct Running {
    /// When it started.
    pub since: Instant,
    /// The resident memory the watch read when it started.
    pub noted: usize,
    /// How to stop it, when it may be stopped (the node has a line: `panics::work`).
    pub stop: Option<futures::future::AbortHandle>,
    pub stopped: bool,
    /// What the request's polls have allocated, less what they freed (its [`Meter`]'s counter): set only for
    /// a request the node may stop, and 0 for the rest.
    pub used: Option<Arc<AtomicI64>>,
}

impl Running {
    fn used(&self) -> i64 {
        self.used.as_ref().map_or(0, |u| u.load(Relaxed))
    }
}

/// The request to stop, of those that may be stopped, have run for [`FIRST`], and started at least `margin`
/// bytes below the memory now (so they may have grown it). Of those, the one whose own allocations are the
/// most, if that is at least `margin`: the request that grew the node. Otherwise the oldest, as before: the
/// growth was elsewhere (an operator that doesn't count what it holds, or no counters at all), and age is
/// then all there is to go on. None when no request qualifies. Of requests that tie, the lower id (the
/// map's first) is the older, and is taken.
pub fn pick(running: &std::collections::BTreeMap<u64, Running>, now: Instant, resident: usize, margin: usize) -> Option<u64> {
    let fit: Vec<(&u64, &Running)> = running
        .iter()
        .filter(|(_, r)| r.stop.is_some() && !r.stopped)
        .filter(|(_, r)| now.saturating_duration_since(r.since) >= FIRST)
        .filter(|(_, r)| r.noted.saturating_add(margin) <= resident)
        .collect();
    let grew = fit.iter().min_by_key(|(_, r)| std::cmp::Reverse(r.used())).filter(|(_, r)| r.used() >= margin as i64);
    grew.or_else(|| fit.iter().min_by_key(|(_, r)| r.since)).map(|(id, _)| **id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const LINE: usize = 1 << 30;
    const MARGIN: usize = LINE / 10;
    const MB: usize = 1 << 20;

    fn at(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    fn req(since: Instant, noted: usize) -> Running {
        Running { since, noted, stop: Some(futures::future::AbortHandle::new_pair().0), stopped: false, used: None }
    }

    // A request that has allocated `used` bytes so far (its meter's counter).
    fn grew(since: Instant, noted: usize, used: usize) -> Running {
        Running { used: Some(Arc::new(AtomicI64::new(used as i64))), ..req(since, noted) }
    }

    fn runs(list: Vec<(u64, Running)>) -> BTreeMap<u64, Running> {
        list.into_iter().collect()
    }

    #[test]
    fn the_watch_looks_sooner_the_nearer_the_line() {
        assert_eq!(every(0, 64 << 30), Duration::from_secs(1), "far below a big line: a look a second");
        assert_eq!(every(LINE - (1 << 20), LINE), Duration::from_millis(20), "near it: every 20 ms");
        assert_eq!(every(2 * LINE, LINE), Duration::from_millis(20), "past it too");
        let half = every(LINE / 2, LINE);
        assert!(half > Duration::from_millis(50) && half < Duration::from_millis(70), "half a 1 GB line: before 8 GB/s could pass it ({half:?})");
    }

    #[test]
    fn under_the_line_nothing_is_stopped() {
        let now = at(60);
        assert!(!due(None, LINE - 1, LINE, now));
        assert!(due(None, LINE, LINE, now)); // (at the line: over it)
        assert!(!due(Some(now), LINE - 1, LINE, now));
    }

    #[test]
    fn over_the_line_the_oldest_grown_request_is_stopped() {
        let start = Instant::now();
        let running = runs(vec![(7, req(start + Duration::from_secs(2), 0)), (3, req(start, 0)), (9, req(start, 0))]);
        // (3 and 9 started together: the lower id is the older in the map, and both qualify; 3 comes first)
        assert_eq!(pick(&running, start + Duration::from_secs(5), LINE, MARGIN), Some(3));
    }

    #[test]
    fn the_request_that_grew_the_node_is_stopped_not_the_oldest() {
        let start = Instant::now();
        let now = start + Duration::from_secs(5);
        let margin = 100 * MB;
        // An old request that has allocated 1 MB, and a young one that has allocated 500 MB: the young one grew the node.
        let old = grew(start, 0, MB);
        let young = grew(start + Duration::from_secs(1), 0, 500 * MB);
        assert_eq!(pick(&runs(vec![(1, old), (2, young)]), now, LINE, margin), Some(2));
    }

    #[test]
    fn below_the_margin_no_request_grew_the_node_and_the_oldest_is_stopped() {
        let start = Instant::now();
        let now = start + Duration::from_secs(5);
        let margin = 100 * MB;
        // Both allocated less than the margin: the growth was elsewhere, so age is all there is to go on.
        let old = grew(start, 0, 10 * MB);
        let young = grew(start + Duration::from_secs(1), 0, 20 * MB);
        assert_eq!(pick(&runs(vec![(1, old), (2, young)]), now, LINE, margin), Some(1));
    }

    #[test]
    fn younger_stopped_unstoppable_and_fresh_requests_are_passed_over() {
        let start = Instant::now();
        let now = start + Duration::from_secs(5);
        let young = req(now - Duration::from_millis(150), 0);
        assert_eq!(pick(&runs(vec![(1, young)]), now, LINE, MARGIN), None, "just started");
        let stopped = Running { stopped: true, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(2, stopped)]), now, LINE, MARGIN), None, "already stopped");
        let not = Running { stop: None, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(3, not)]), now, LINE, MARGIN), None, "not stoppable");
        let near = || (4, req(start, LINE - MARGIN / 2));
        assert_eq!(pick(&runs(vec![near()]), now, LINE, MARGIN), None, "started near today's memory: it didn't grow it");
        let grown = (5, req(start, LINE - 2 * MARGIN));
        assert_eq!(pick(&runs(vec![near(), grown]), now, LINE, MARGIN), Some(5), "the one that grew is chosen past the others");
        let unread = Running { noted: usize::MAX, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(6, unread)]), now, LINE, MARGIN), None, "no reading yet (it started while the watch waited): passed over");
    }

    #[test]
    fn a_stop_holds_for_ten_seconds_or_until_the_memory_is_under_the_line() {
        let now = at(100);
        let half_a_second_ago = now - Duration::from_millis(500);
        let ten_and_a_half_ago = now - Duration::from_millis(10_500);
        assert!(pending(Some(half_a_second_ago), LINE, LINE, now));
        assert!(!pending(Some(ten_and_a_half_ago), LINE, LINE, now));
        assert!(!pending(Some(half_a_second_ago), LINE - 1, LINE, now), "under the line: nothing holds");
        assert!(!pending(None, LINE, LINE, now));
        assert!(!due(Some(half_a_second_ago), LINE, LINE, now), "held: not again");
        assert!(due(Some(ten_and_a_half_ago), LINE, LINE, now), "held past ten seconds: again");
    }

    #[test]
    fn the_watch_gives_the_cache_seven_tenths_of_the_line() {
        assert_eq!(of_line(10 << 30), 7 << 30);
        assert_eq!(of_line(LINE), LINE / 10 * 7);
    }

    // (only where the counters are: jemalloc's, on Linux with glibc; elsewhere `thread_net` is 0)
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn a_meter_charges_what_its_polls_keep_and_not_what_they_free() {
        let used = Arc::new(AtomicI64::new(0));
        let kept = futures::executor::block_on(Meter::new(async { vec![1u8; 64 * MB] }, used.clone())); // (allocated in its poll, and returned)
        let after_kept = used.load(Relaxed);
        assert!(after_kept >= (64 * MB) as i64, "a 64 MB Vec kept is charged at least 64 MB ({after_kept} bytes)");
        futures::executor::block_on(Meter::new(
            async {
                let scratch = vec![2u8; 64 * MB];
                std::hint::black_box(scratch.len()) // (allocated and freed within the poll)
            },
            used.clone(),
        ));
        let freed = used.load(Relaxed) - after_kept;
        assert!(freed.abs() < MB as i64, "a 64 MB Vec allocated and freed is charged under 1 MB ({freed} bytes)");
        drop(kept);
    }
}
