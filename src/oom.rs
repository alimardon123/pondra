//! A node stops the query that grows it past its memory line, rather than be killed for memory.
//!
//! Some operators grow without asking the query memory pool: `array_agg`, `median` and `string_agg`
//! over huge groups, and the last steps that build their answers. The pool can't refuse what it
//! isn't asked for, so on a machine or in a container with too little memory the OS killed the whole
//! node: `array_agg` of 30 M values in 3 groups peaked at 1,941 MB resident. A cap that looked only
//! when something asked the pool found 1.8–1.9 GB already resident when it refused, no better than
//! none. So a watch reads the process's resident memory on a timer, whatever asked for it, and when
//! it passes the node's line it stops the request that grew it: its future is dropped, so its memory
//! is let go, and it answers 53200. The node stays up and answers the rest. A query in its first try, which a
//! frugal session can try again (`query::frugally`), has that try stopped alone, and runs again there holding
//! less (a median from a sort's middle rows, `frugal.rs`); a second stop takes the request. Which request grew it is
//! read from the allocator (`Meter`): each request is charged what its own polls allocated, less what
//! they freed, so a long cheap request started early is not stopped for a query that grew the node.
//! The one watch lets the node's caches go first, under the line (`of_line`), since what a cache held is read
//! again when it is needed: the caches charged to the node's lines (`ledger::Charged`), each lake's in the order
//! `Lake::caches` lists them, its hot columns first (`hot.rs`). It stops a query only past the line itself. At each
//! look it notes what the caches hold, which a query's grow past the whole line goes by to take cache bytes back
//! before it spills (`ledger::Ledger`): the trim line is that whole line, unless `PONDRA_MEMORY_GB` sets it.
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{Context, Poll};
use datafusion::common::instant::Instant;
use std::time::Duration;

/// What a stopped request answers (SQLSTATE 53200: `codes::by_words` reads "out of memory").
pub const OOM: &str = "out of memory: the node was nearly out of memory and stopped this query, the one that grew it (PONDRA_MEMORY_MAX_GB); a write in it may or may not have been applied";

/// What a query the page's watch stopped answers (SQLSTATE 53200: `codes::of` reads "out of memory"): the page's words, as `OOM` is the node's.
pub(crate) const PAGE_OOM: &str = "out of memory: the page was nearly out of memory and stopped this query, the one that grew it (memory_max_gb)";

/// How long a stop holds the watch while the node stays over its line: the node's allocator gives freed
/// pages back over about ten seconds (`main.rs`), so resident memory falls after a stop, and a second stop
/// would be for memory the first one already let go. Once under the line, nothing holds.
pub(crate) const HOLD: Duration = Duration::from_secs(10);

/// How long a request must have run to be stopped: one that has only just started did not grow the
/// memory the watch sees, and its own cost is still arriving. (A second let `array_agg` of 30M values
/// reach 1.75 GB on a 1 GB line on CI's runner, where it grows faster.)
const FIRST: Duration = Duration::from_millis(200);

/// How fast the watch takes a query to grow, at most: it looks again before memory growing at this
/// rate could pass the line, every 5 ms near it (20 ms while a stop it made settles) and every second at most far
/// below it.
const RATE: f64 = 8.0 * (1u64 << 30) as f64;

/// The process's memory watch (redesign step 2): one a process, as the memory it reads is and the line it holds the
/// process to (`ledger::Root`). It starts at the first node's start (`start`) and lives as long as the process, so
/// a node that stops ends nothing another node in the process goes by. (Not in a root: a test's lake gets a root of
/// its own sizes, and the process still has one watch.)
struct Watch {
    /// The resident memory read last (0 before the first look).
    seen: AtomicUsize,
    /// The watch is waiting: a request that starts then has no reading until the watch's next look gives it one.
    idle: AtomicBool,
    /// The watch runs in this process: the hot columns may then sit beside the query memory (`Hot::room_for`).
    watching: AtomicBool,
    /// A wake-up holding no data: a request started, or the hot columns grew, so the watch looks again now.
    wake: tokio::sync::Notify,
    /// The loop started (`start`): once a process.
    started: std::sync::Once,
}

static WATCH: Watch = Watch {
    seen: AtomicUsize::new(0),
    idle: AtomicBool::new(false),
    watching: AtomicBool::new(false),
    wake: tokio::sync::Notify::const_new(),
    started: std::sync::Once::new(),
};

/// The node's memory line in bytes: `PONDRA_MEMORY_MAX_GB`, else 85% of the machine's or container's
/// memory. None when it is 0 (off), or in a host's process, where the memory is the host's.
pub fn line() -> Option<usize> {
    // (a page's line is its device's, and it has one only where its memory is read: `Machine::resident`, a page's `Device`)
    if cfg!(target_arch = "wasm32") {
        let device = crate::machine::process();
        return device.resident().and_then(|_| line_on(device.line()));
    }
    if crate::machine::hosted() {
        return None;
    }
    crate::env::memory_line(crate::machine::process().ram().map(|ram| (ram as f64 * 0.85) as usize))
}

/// The memory line on a page's device, which gives the module `device` bytes (its machine's line, `Machine::line`): the
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
    if WATCH.idle.load(Relaxed) {
        usize::MAX
    } else {
        WATCH.seen.load(Relaxed)
    }
}

/// Waits, at most [`HOLD`], until the watch reads the node back near `noted`, what it held when the request started:
/// within a tenth of its line (the margin `pick` stops by), and a tenth under the line. After a first try it stopped
/// (`panics::first_try`), whose partitions' tasks may still be in a poll that holds the try's memory, and whose freed
/// pages the watch is handing back (`purge`), so the second try doesn't start in that memory and get stopped for it.
/// (B50's median under 2 GB: started once the node was a tenth under its line, at 1,560 MB, the second try was stopped
/// at 1,742 MB on a 1,740 MB line, 0.6 s later.) True if it came back.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn come_back(noted: usize) -> bool {
    let Some(line) = line() else { return true };
    let near = noted.saturating_add(line / 10).min(line - line / 10);
    let since = Instant::now();
    while seen() >= near {
        if since.elapsed() >= HOLD {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

/// A page stops no first try (`panics::first_try` runs it as it is there).
#[cfg(target_arch = "wasm32")]
pub(crate) async fn come_back(_: usize) -> bool {
    true
}

/// The resident memory past which the hot columns are given back, before a query is stopped (`of_line`). With no line (a host's process, or `PONDRA_MEMORY_MAX_GB=0`), 85% of the
/// line the machine would have (85% of its memory): the host's memory isn't ours to stop queries for,
/// but our own cache is ours to drop.
pub fn trim_line() -> Option<usize> {
    line().or_else(|| Some(crate::machine::process().ram()? / 20 * 17)).map(of_line)
}

/// 85% of a line, or 320 MB under it where that is lower, and never under 70% of it. With the default line
/// (85% of RAM) on a machine of 3 GB or more, 72% of RAM: the hot columns take up to half of RAM (`Hot::new`),
/// so queries have a fifth of RAM beside them before any column is given back, and an eighth more before the
/// line, where a query is stopped. (At seven tenths of the line, 60% of RAM, the columns were given back as
/// soon as a query took a tenth: TPC-H's and ClickBench's columns came and went.) Under a small line, 15% of
/// it was too near: a query growing as fast as the watch looks for (`RATE`, 320 MB in 40 ms) passed a 1 GB line
/// by 11% while the columns were being given back (`harness hot`, on a GitHub runner); 320 MB under it is
/// what it grows in the 40 ms the trim and its purge take.
fn of_line(line: usize) -> usize {
    line.saturating_sub(320 << 20).clamp(line / 10 * 7, line / 20 * 17)
}

/// Does the watch run in this process (`start`)? Where it doesn't, nothing gives the hot columns back.
pub fn watching() -> bool {
    WATCH.watching.load(Relaxed)
}

/// Wakes the watch, so it looks again now. `notify_one` keeps a wake sent between a look and the wait
/// after it, so none is lost.
pub fn wake() {
    WATCH.wake.notify_one()
}

/// Takes about `bytes` back from the caches of every node in the process (`ledger::budgets`, `Ledger::give_back_kind`), and says how many bytes came back. The
/// watch gives them back past [`trim_line`], before it stops any query. A lake opened outside a node (`pondra sql`,
/// `pondra catalog`, `pondra backup`) holds its caches to their own budgets alone, since no watch runs in its process.
/// A kind at a time across the nodes' budgets, so a budget's ranges never go before another's hot columns (11-06 audit L7).
fn give_back(bytes: usize) -> usize {
    let ledgers = crate::store::ledgers();
    let mut kinds: Vec<&'static str> = vec![];
    for kind in ledgers.iter().flat_map(|ledger| ledger.kinds()) {
        if !kinds.contains(&kind) {
            kinds.push(kind);
        }
    }
    let mut freed = 0;
    for kind in kinds {
        for ledger in &ledgers {
            if freed >= bytes {
                return freed;
            }
            freed += ledger.give_back_kind(kind, bytes - freed);
        }
    }
    freed
}

/// Starts the process's watch (`Watch`), when the process can say how much memory it holds and the caches can be given
/// back (`trim_line`): at the first node's start, once a process. The line is checked only where there is one (a node's
/// own process). It runs on a thread of its own (`panics::alone`) for as long as the process: a node that stops ends its
/// own loops, not the process's watch, which then reads the caches and requests of the nodes still running (none: it
/// waits, and looks every 30 s).
pub fn start() {
    // (a page's one thread can't run this loop beside a query: a page's requests are watched at each poll instead, `panics::work`)
    if cfg!(target_arch = "wasm32") {
        return;
    }
    // DataFusion's spawned work runs as the request that spawned it, and charges it where there is a line, as the
    // request's own polls do (`charge`: with no line, no request is charged). (A tracer a program embedding the library
    // set first is kept: its error is ignored.)
    let _ = datafusion::common::runtime::set_join_set_tracer(&TRACER);
    WATCH.started.call_once(watch);
}

/// The watch's loop (`start`).
fn watch() {
    let Some(trim) = trim_line() else { return };
    if crate::machine::process().resident().is_none() {
        return;
    }
    let line = line(); // (None in a host's process: there the watch only gives the cache back)
    WATCH.watching.store(true, Relaxed);
    crate::panics::alone(async move {
        let mut last_stop = None; // when the last stop was made: it holds the watch (`pending`)
        let mut said = false; // the line for this stretch over the line, said once
        loop {
            let mut settling = false; // (a stop this watch made still holds, or its request is still ending)
            let resident = crate::machine::process().resident().unwrap_or(0);
            WATCH.seen.store(resident, Relaxed); // (what the OS says: `noted` and `pick` compare it)
            for ledger in crate::store::ledgers() {
                ledger.note(); // (what the caches hold, which a query's grow goes by until the next look: `ledger::Ledger`)
            }
            crate::panics::note(resident); // (a request that started unread takes this look's reading, before any stop)
            let now = Instant::now();
            let mut felt = resident;
            if felt > trim {
                // The caches go first: what they held is read again when it is needed, a query can't be.
                let freed = give_back(felt - trim);
                if freed > 0 {
                    // The pages just freed would stay resident for seconds (`purge`), so they are handed back now:
                    // the reading after it is what the process holds, and the watch goes by that alone.
                    purge();
                    felt = crate::machine::process().resident().unwrap_or(felt);
                    WATCH.seen.store(felt, Relaxed);
                    crate::panics::note(felt);
                }
            }
            if let Some(line) = line {
                // (a request stopped and not gone yet is still letting its memory go: no other is stopped for that memory)
                let winding = crate::panics::stopping();
                if pending(last_stop, felt, line, now) || winding {
                    // A stopped query's memory goes back to the allocator as its tasks drop, and from there to the system
                    // over ten seconds: handed back now, the node is under its line again before the next query comes
                    // (stopped median, then string_agg at once: killed with the stop's memory still resident, B50 10-31).
                    purge();
                    felt = crate::machine::process().resident().unwrap_or(felt);
                    WATCH.seen.store(felt, Relaxed);
                    crate::panics::note(felt);
                }
                if felt < line {
                    said = false;
                    // (under the line, the stop's memory has gone: a query that passes it now grew it, and is stopped,
                    // though the stop was under ten seconds ago; held, the next heavy query was killed, B50 10-31)
                    last_stop = None;
                }
                if due(last_stop, felt, line, now) {
                    let mb = |bytes: usize| bytes >> 20;
                    if let Some(first) = crate::panics::stop_one(now, line / 10) {
                        let again = if first { "'s first try (it runs again frugally)" } else { "" };
                        eprintln!("stopped a query{again}: the node held {} MB, its line {} MB (PONDRA_MEMORY_MAX_GB)", mb(felt), mb(line));
                        last_stop = Some(now);
                    } else if !winding && !std::mem::replace(&mut said, true) {
                        // (nothing to stop yet: a request old enough that grew the node is stopped at a later look,
                        // so no hold starts here; the line is said once per stretch over the line)
                        eprintln!("the node holds {} MB, past its line {} MB, and no query grew it", mb(felt), mb(line));
                    }
                }
                settling = winding || last_stop.is_some();
            }
            // The wait: as often as the trim and the stop need while a request runs (a plain sleep, since a wake on every
            // request would look on every statement); otherwise until a request starts, a lake opens, the hot columns
            // grow, or the timer ends: 2 s while the caches hold what they could give back (Flight, Kafka and tiering grow
            // the node outside `work`, and the caches are what can answer them), else 30 s. (The log's rows and the
            // catalog's blocks are never empty, and keep what they hold: counted, an idle node looked every 2 s.)
            match line {
                Some(line) if crate::panics::running() => tokio::time::sleep(every(felt, trim, line, settling)).await,
                _ => {
                    let timer = if crate::store::ledgers().iter().any(|l| l.givable()) { Duration::from_secs(2) } else { Duration::from_secs(30) };
                    WATCH.idle.store(true, Relaxed);
                    let _ = tokio::time::timeout(timer, WATCH.wake.notified()).await;
                    WATCH.idle.store(false, Relaxed);
                }
            }
        }
    });
}

/// When to look again: before memory growing at [`RATE`] could pass the line from where it is now, and while under the
/// trim line before it could pass that too (the cache must go before the line comes), though no sooner than half the
/// time to the line: a full cache holds the node just under its trim line (`Hot::room_for`), and looks every 5 ms there
/// read `/proc` and take the requests' lock 200 times a second (11-03 audit L12). (Looking every second while under
/// half the line, a query that grew from there in that second passed a 1 GB line by 0.75 GB before the watch saw it.)
/// No sooner than 5 ms: near the line a GROUP BY's `array_agg` grows the node 1-6 GB/s, and at 20 ms at least the watch
/// stopped one up to 17 MB past a 1 GB line (its peak 1,031-1,041 MB, harness `hot`), at 5 ms 3 MB (1,029-1,033 MB:
/// `logs/round36/memory-line-check.json`). While a stop it made settles (`settling`), 20 ms: the stop's memory is going,
/// and each look then hands freed pages back (`purge`).
fn every(resident: usize, trim: usize, line: usize, settling: bool) -> Duration {
    let soonest = Duration::from_millis(if settling { 20 } else { 5 });
    let until = |mark: usize| mark.saturating_sub(resident) as f64 / RATE;
    let secs = match resident < trim {
        true => until(line).min(until(trim).max(until(line) / 2.0)),
        false => until(line),
    };
    Duration::from_secs_f64(secs).clamp(soonest, Duration::from_secs(1))
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
/// request runs is charged to that request, wherever it runs, and a task runs as its caller (`auth::carried`).
struct Tracer;

static TRACER: Tracer = Tracer;

impl datafusion::common::runtime::JoinSetTracer for Tracer {
    fn trace_future(
        &self,
        fut: futures::future::BoxFuture<'static, Box<dyn std::any::Any + Send>>,
    ) -> futures::future::BoxFuture<'static, Box<dyn std::any::Any + Send>> {
        use futures::FutureExt;
        // (and its caller, as a task it spawns would lose it: a function that asks back mid-query, and an error's words, read
        // what the query's caller may, `rights::scope`; the node's own query stays the node's: `auth::carried`)
        let fut = crate::auth::carried(fut).boxed();
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
    /// Stopped by the watch, and its memory still its own until its stopped work has ended: no stop is made meanwhile.
    pub stopped: bool,
    /// Between its first try and its second, waiting for the first try's memory to go (`panics::first_try`): the watch
    /// doesn't stop it, and may stop another.
    pub resting: bool,
    /// What the request's polls have allocated, less what they freed (its [`Meter`]'s counter): set only for
    /// a request the node may stop, and 0 for the rest. The request's polls and each task or blocking closure
    /// it spawned hold it ([`charge`], [`TRACER`]), so this entry's is the last one once they have all ended.
    pub used: Option<Arc<AtomicI64>>,
    /// When a stopped request's own future ended while work it spawned was still running: the entry stays,
    /// stopped, until that work has ended too (`panics::stopping`).
    pub left: Option<Instant>,
    /// How to stop the first try it is in (`panics::first_try`), which a frugal session can try again: the watch
    /// stops that rather than the request.
    pub first_try: Option<futures::future::AbortHandle>,
    /// Its node's counters, which count a stop of it (`panics::stop_one`, `panics::stopped_for_memory`).
    pub metrics: Arc<crate::metrics::Metrics>,
}

impl Running {
    fn used(&self) -> i64 {
        self.used.as_ref().map_or(0, |u| u.load(Relaxed))
    }

    /// Is work the request spawned still running: a task or a blocking closure holds its counter.
    pub(crate) fn spawned(&self) -> bool {
        self.holders() > 1
    }

    /// How many hold the request's counter: this entry, the request's future while it runs, and each task or blocking
    /// closure it spawned that hasn't ended.
    pub(crate) fn holders(&self) -> usize {
        self.used.as_ref().map_or(0, Arc::strong_count)
    }
}

/// The memory counter of the request the current task is part of (a test's stand-in for a task it spawned).
#[cfg(test)]
pub(crate) fn counter() -> Option<Arc<AtomicI64>> {
    REQUEST.try_with(Arc::clone).ok()
}

/// The request to stop, of those that may be stopped, have run for [`FIRST`], and started at least `margin`
/// bytes below the memory now (so they may have grown it). Of those, the one whose own allocations are the
/// most, if that is at least `margin`: the request that grew the node. Otherwise the oldest, as before: the
/// growth was elsewhere (an operator that doesn't count what it holds, or no counters at all), and age is
/// then all there is to go on. None when no request qualifies. Of requests that tie, the lower id (the
/// map's first) is the older, and is taken. None too while a request stopped before is still running, or the work
/// it spawned is (`panics::stopping`): it goes at its next poll, and until then it may still be growing the node
/// (its answer's last steps run in one poll), so the memory over the line is still its own. (Under the line for a moment after its stop and over it again
/// before it had gone, the watch stopped the older cheap request beside it, by age: CI, 10-31.)
pub fn pick(running: &std::collections::BTreeMap<u64, Running>, now: Instant, resident: usize, margin: usize) -> Option<u64> {
    if running.values().any(|r| r.stopped) {
        return None;
    }
    let fit: Vec<(&u64, &Running)> = running
        .iter()
        .filter(|(_, r)| r.stop.is_some() && !r.stopped && !r.resting)
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
        Running { since, noted, stop: Some(futures::future::AbortHandle::new_pair().0), stopped: false, resting: false, used: None, left: None, first_try: None, metrics: Default::default() }
    }

    // A request that has allocated `used` bytes so far (its meter's counter).
    fn grew(since: Instant, noted: usize, used: usize) -> Running {
        Running { used: Some(Arc::new(AtomicI64::new(used as i64))), ..req(since, noted) }
    }

    fn runs(list: Vec<(u64, Running)>) -> BTreeMap<u64, Running> {
        list.into_iter().collect()
    }

    /// A task DataFusion spawns runs as its query's caller (a function that asks back, an error's words: `rights::scope`),
    /// where a task spawned with nothing lost it; and the node's own query stays the node's.
    #[tokio::test]
    async fn a_task_datafusion_spawns_keeps_its_caller() {
        use datafusion::common::runtime::JoinSetTracer;
        use futures::FutureExt;
        let whole = || async { Box::new(crate::rights::scope().whole()) as Box<dyn std::any::Any + Send> }.boxed();
        let ran = |traced: futures::future::BoxFuture<'static, Box<dyn std::any::Any + Send>>| async { *tokio::spawn(traced).await.unwrap().downcast::<bool>().unwrap() };
        let token = crate::auth::Principal::token(crate::auth::Role::Read);
        assert!(!ran(crate::auth::WHO.scope(token.clone(), async { whole() }).await).await, "spawned as is: no caller, nothing");
        assert!(ran(crate::auth::WHO.scope(token, async { TRACER.trace_future(whole()) }).await).await, "traced: its caller's");
        assert!(ran(crate::auth::as_node(async { TRACER.trace_future(whole()) }).await).await, "the node's own");
        assert!(!ran(TRACER.trace_future(whole())).await, "no caller: none");
    }

    #[test]
    fn the_watch_looks_sooner_the_nearer_the_line() {
        let trim = of_line(LINE);
        assert_eq!(every(0, of_line(64 << 30), 64 << 30, false), Duration::from_secs(1), "far below a big line: a look a second");
        assert_eq!(every(LINE - (1 << 20), trim, LINE, false), Duration::from_millis(5), "near it: every 5 ms");
        assert_eq!(every(2 * LINE, trim, LINE, false), Duration::from_millis(5), "past it too");
        assert_eq!(every(2 * LINE, trim, LINE, true), Duration::from_millis(20), "past it, a stop settling: every 20 ms");
        let half = every(LINE / 2, trim, LINE, false);
        assert!(half > Duration::from_millis(20) && half < Duration::from_millis(35), "half a 1 GB line: before 8 GB/s could pass its trim line ({half:?})");
        // Just under the trim line, where a full cache holds the node: half the time to the line, not 5 ms (11-03 audit L12).
        let parked = every(trim - (1 << 20), trim, LINE, false);
        let to_line = (LINE - trim + (1 << 20)) as f64 / RATE;
        assert!(parked >= Duration::from_secs_f64(to_line / 2.0 - 1e-6) && parked < Duration::from_secs_f64(to_line), "{parked:?}");
        assert!(parked > Duration::from_millis(15), "a 1 GB line's trim line is 300 MB under it: ~18 ms ({parked:?})");
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
        let stopped = || Running { stopped: true, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(2, stopped())]), now, LINE, MARGIN), None, "already stopped");
        let cheap = req(start, 0); // (the oldest, which age alone would pick)
        assert_eq!(pick(&runs(vec![(1, cheap), (2, stopped())]), now, LINE, MARGIN), None, "one stopped is still going: its memory, not another's");
        let not = Running { stop: None, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(3, not)]), now, LINE, MARGIN), None, "not stoppable");
        let near = || (4, req(start, LINE - MARGIN / 2));
        assert_eq!(pick(&runs(vec![near()]), now, LINE, MARGIN), None, "started near today's memory: it didn't grow it");
        let grown = (5, req(start, LINE - 2 * MARGIN));
        assert_eq!(pick(&runs(vec![near(), grown]), now, LINE, MARGIN), Some(5), "the one that grew is chosen past the others");
        let unread = Running { noted: usize::MAX, ..req(start, 0) };
        assert_eq!(pick(&runs(vec![(6, unread)]), now, LINE, MARGIN), None, "no reading yet (it started while the watch waited): passed over");
        // (audit 11-01 H5: a request between its tries, its first try's memory going, isn't stopped, and holds no stop)
        let resting = || (7, Running { resting: true, used: Some(Arc::new(AtomicI64::new(LINE as i64))), ..req(start, 0) });
        assert_eq!(pick(&runs(vec![resting()]), now, LINE, MARGIN), None, "resting between its tries");
        assert_eq!(pick(&runs(vec![resting(), (8, grew(start + Duration::from_secs(1), 0, MARGIN))]), now, LINE, MARGIN), Some(8), "the one beside it that grew is stopped");
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
    fn the_watch_gives_the_cache_back_past_85_hundredths_of_the_line_or_320_mb_under_a_small_one() {
        assert_eq!(of_line(20 << 30), 17 << 30);
        assert_eq!(of_line(2 << 30), (2 << 30) - (320 << 20), "320 MB under a 2 GB line: 84%");
        assert_eq!(of_line(1 << 30), (1 << 30) / 10 * 7, "never under seven tenths of the line");
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
