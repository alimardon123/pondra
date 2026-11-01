//! A panic in a request is answered as an error, and the node stays up; a panic in the work the
//! node can't do without (committing, tiering, following the leader, the run log…) still stops it,
//! as before: a crashed writer restarts and reloads its state from the catalog.
//!
//! - **Doors** run each request in [`door`]: HTTP (`server::guard`), each Postgres statement, and
//!   each Kafka and Flight connection (its task: a panic ends that connection only); a query's
//!   request in [`work`], on the queries' runtime when another has run a while.
//! - **The node's own loops** are started with [`spawn`]: a panic in one aborts the process.
//! - DataFusion's own tasks (a query's partitions) hand a panic back to the request that ran them.
use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use datafusion::common::instant::Instant;

/// Run a request: `Err` (the panic's message) if it panicked.
pub async fn door<T>(f: impl Future<Output = T>) -> Result<T, String> {
    let _busy = crate::drain::Busy::new(); // (a node stopping waits for it: `drain.rs`)
    AssertUnwindSafe(f).catch_unwind().await.map_err(|p| said(&*p))
}

/// Run a request on the runtime queries run on, as [`door`] does. Queries and the node's own work
/// (appends, commits, heartbeats, the commit stream) then never wait behind each other's tasks:
/// the OS shares the cores between two runtimes, where one ran a woken append only after every
/// query task ahead of it (a writer beside 400 dashboard clients landed 2,860 rows a second of
/// 22,000). A request runs where it came in while no other has run for [`LONG`]: the hop wakes a
/// parked thread each way, which cost pgbench's four clients, whose statements take a millisecond
/// or two, a sixth of their transactions. Dropped, as when its client goes, it stops the work.
///
/// A `stoppable` request is one the node may stop for memory (`oom.rs`), where the node has a line to
/// stop it at: then it answers [`crate::oom::OOM`] and its future is dropped, its memory let go, and its
/// polls' allocations are counted to it (`oom::charge`), so the watch stops the one that grew the node. A page (wasm32) has no
/// watch loop, so it watches each poll of one instead (`oom::polled`), and answers its own [`crate::oom::PAGE_OOM`].
/// Any other request is not wrapped, and runs where it came in or on the queries' runtime, as the others do.
pub async fn work<T: Send + 'static>(stoppable: bool, f: impl Future<Output = T> + Send + 'static) -> Result<T, String> {
    use futures::future::{Aborted, Either};
    use std::sync::atomic::Ordering::Relaxed;
    // (a page's one thread runs no watch loop beside a request, so each poll of a stoppable one is watched: `oom::polled`)
    #[cfg(target_arch = "wasm32")]
    {
        if let Some(line) = stoppable.then(crate::oom::line).flatten() {
            return door(crate::oom::polled(|| crate::store::resident().unwrap_or(0), line, f)).await?.map_err(|()| crate::oom::PAGE_OOM.to_string());
        }
    }
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    struct Stop(u64, Option<tokio::task::AbortHandle>);
    impl Drop for Stop {
        fn drop(&mut self) {
            let mut running = RUNNING.lock().unwrap_or_else(|e| e.into_inner());
            match running.get_mut(&self.0) {
                // (stopped, its tasks end at their next poll, and until then they may still grow the node: their
                // last polls took the node over its line again after the stop, and the watch stopped the cheap
                // query beside it, by age: CI, 11-01)
                Some(r) if r.stopped && r.spawned() => r.left = Some(Instant::now()),
                _ => {
                    running.remove(&self.0);
                }
            }
            drop(running);
            if let Some(t) = self.1.take() {
                t.abort();
            }
        }
    }
    let id = NEXT.fetch_add(1, Relaxed);
    let stoppable = stoppable && crate::oom::line().is_some(); // (nothing could stop it otherwise)
    let f = REQUEST.scope(id, f); // (a first try in it finds its entry: `first_try`)
    // A request the node may stop is charged the memory its polls allocate (`oom::charge`): the watch stops the one that grew it.
    let used = stoppable.then(|| std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)));
    let (f, handle) = match &used {
        Some(used) => {
            let (f, handle) = futures::future::abortable(crate::oom::charge(used.clone(), f));
            (Either::Left(f), Some(handle))
        }
        None => (Either::Right(f.map(Ok::<T, Aborted>)), None),
    };
    let long = {
        let mut running = RUNNING.lock().unwrap_or_else(|e| e.into_inner());
        let long = running.first_key_value().is_some_and(|(_, r)| r.since.elapsed() >= LONG);
        running.insert(id, crate::oom::Running { since: Instant::now(), noted: crate::oom::seen(), stop: handle, stopped: false, used, left: None, first_try: None });
        long
    };
    crate::oom::wake(); // (the watch looks now, once this request is in RUNNING: it may be one to stop)
    let mut stop = Stop(id, None);
    let out = if !long {
        door(f).await
    } else {
        apart(door(f), &mut stop.1).await
    };
    match out? {
        Ok(v) => Ok(v),
        Err(_stopped) => Err(crate::oom::OOM.to_string()), // (the node stopped it for memory: `stop_one`)
    }
}

/// Runs a request that has run a while (`work`) as `door` does, on the queries' runtime, with its abort handle in `stop`
/// before it runs, so the node can stop it there. A page has no such runtime: its one loop runs the request in place.
#[cfg(not(target_arch = "wasm32"))]
async fn apart<T: Send + 'static>(f: impl Future<Output = Result<T, String>> + Send + 'static, stop: &mut Option<tokio::task::AbortHandle>) -> Result<T, String> {
    let task = queries().spawn(f);
    *stop = Some(task.abort_handle());
    task.await.unwrap_or_else(|e| Err(format!("internal error: {e}")))
}

#[cfg(target_arch = "wasm32")]
async fn apart<T>(f: impl Future<Output = Result<T, String>>, _: &mut Option<tokio::task::AbortHandle>) -> Result<T, String> {
    f.await
}

// The requests running through [`work`], by id (ids grow with time, so the first is the oldest).
crate::per_node!(static RUNNING: std::sync::Mutex<std::collections::BTreeMap<u64, crate::oom::Running>> = Default::default());

/// Is a request running through [`work`]: the watch has something to look at (`oom::start` waits on it).
pub(crate) fn running() -> bool {
    !RUNNING.lock().unwrap_or_else(|e| e.into_inner()).is_empty()
}

/// Is a request the watch stopped still running, or work it spawned (a DataFusion task ends at its next poll): until
/// it has all ended, the memory is still its own (`oom::pick` stops no other meanwhile). A stopped request whose own
/// future and spawned work have ended leaves here, as one does whose spawned work runs past [`crate::oom::HOLD`] (a
/// blocking closure runs to its end). Only the watch's loop calls it, at each look.
pub(crate) fn stopping() -> bool {
    let mut running = RUNNING.lock().unwrap_or_else(|e| e.into_inner());
    running.retain(|_, r| r.left.is_none_or(|at| r.spawned() && at.elapsed() < crate::oom::HOLD));
    running.values().any(|r| r.stopped)
}

tokio::task_local! {
    /// The request running through [`work`] that this task's work is part of, by its id in `RUNNING`.
    static REQUEST: u64;
}

/// Runs `f`, the first try of work that can be tried again in a frugal session (`query::frugally`), so that when the
/// memory watch picks its request to stop, it stops this try alone: the try's future is dropped and its memory let
/// go, and the request runs on to its second try, which the watch stops whole. What the pool refuses
/// comes back as an error and is tried again there; what grows the node without asking the pool (a median's copies of
/// its groups' values) is stopped by the watch, and was answered 53200 with no second try (B50 under 2 GB, 10-31).
/// Outside a request the node may stop, and inside another first try, `f` runs as it is. Stopped, it returns once the
/// node's memory has come back under its line (`oom::come_back`): [`Stopped::Again`], and the request may be stopped
/// again; [`Stopped::Over`] if the memory stayed over it, and the request ends out of memory. A try that ended in a
/// way the caller tries again (`again`: the pool refused it) waits the same way, the request held as stopped meanwhile,
/// and is answered as it ended whether or not the memory came back (it was tried again before the watch could stop it).
/// (Refused by the pool at the line, the median's first try ended on its own, and the watch stopped the request whole
/// for that try's memory as its second try began: harness scale, 11-01.)
pub async fn first_try<T>(f: impl Future<Output = T>, again: impl Fn(&T) -> bool) -> Result<T, Stopped> {
    let Ok(id) = REQUEST.try_with(|id| *id) else { return Ok(f.await) };
    let (f, handle) = futures::future::abortable(f);
    let mine = match RUNNING.lock().unwrap_or_else(|e| e.into_inner()).get_mut(&id) {
        Some(r) if r.stop.is_some() && !r.stopped && r.first_try.is_none() => {
            r.first_try = Some(handle);
            true
        }
        _ => false,
    };
    let out = f.await; // (stopped, the try's future is dropped here, before the request goes on)
    if !mine {
        return out.map_err(|_| Stopped::Over); // (never stopped: only a registered try is)
    }
    let tried_again = out.as_ref().map_or(true, &again);
    let noted = match RUNNING.lock().unwrap_or_else(|e| e.into_inner()).get_mut(&id) {
        Some(r) => {
            r.first_try = None;
            r.stopped |= tried_again; // (its memory is going: the watch stops nothing for it, this request included)
            r.noted
        }
        None => usize::MAX,
    };
    let back = !tried_again || crate::oom::come_back(noted).await;
    if let Some(r) = RUNNING.lock().unwrap_or_else(|e| e.into_inner()).get_mut(&id) {
        r.stopped &= !(back || out.is_ok()); // (its try and the try's memory gone: the request may be stopped again, whole)
    }
    match out {
        Ok(v) => Ok(v),
        Err(_) if back => Err(Stopped::Again),
        Err(_) => Err(Stopped::Over),
    }
}

/// How a [`first_try`] the memory watch stopped ended.
#[derive(Debug, PartialEq)]
pub enum Stopped {
    /// The node's memory came back under its line: the work may be tried again.
    Again,
    /// It stayed over it: the request is out of memory.
    Over,
}

/// Gives each request that started while the watch waited (no reading: `oom::seen` was `usize::MAX`) the
/// reading of the watch's look after it started, which the watch calls before any stop decision. The
/// others keep theirs. Only the watch's loop calls it: a request's `noted` is one reading, never a file read.
pub(crate) fn note(resident: usize) {
    for r in RUNNING.lock().unwrap_or_else(|e| e.into_inner()).values_mut() {
        if r.noted == usize::MAX {
            r.noted = resident;
        }
    }
}

/// Stops the request the watch chose (`oom::pick`), if one qualifies: its first try, if it is in one ([`first_try`]),
/// which it then runs again frugally; else the request, which is aborted, so its future is dropped and answers
/// [`crate::oom::OOM`]. `None` if none qualified, else whether a first try was what stopped.
pub(crate) fn stop_one(now: Instant, margin: usize) -> Option<bool> {
    let mut running = RUNNING.lock().unwrap_or_else(|e| e.into_inner());
    let id = crate::oom::pick(&running, now, crate::oom::seen(), margin)?;
    let r = running.get_mut(&id)?;
    r.stopped = true; // (until the stopped future is gone: `first_try` or the request's end)
    if let Some(first) = r.first_try.take() {
        first.abort(); // (as below, at its next poll)
        return Some(true);
    }
    if let Some(stop) = &r.stop {
        stop.abort(); // (its next poll finds it aborted: the future is dropped there)
    }
    Some(false)
}

/// Start work of the leader's own that reads and writes as queries do (a view running its query
/// again) on the queries' runtime: appends and commits never wait behind it (invariant 231). A panic
/// in it is that work's alone, as a request's is.
pub fn aside<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    #[cfg(not(target_arch = "wasm32"))]
    let task = queries().spawn(f);
    #[cfg(target_arch = "wasm32")]
    let task = tokio::task::spawn(f); // (a page has one loop, and no queries' runtime: the work runs on it)
    tracked(task)
}

/// Start a loop that must keep its pace whatever the queries do (the memory watch) on a thread of its own: on the
/// node's runtime, a query's partitions busy on every core held its timer back past the moment it had to look (the
/// hot memory-line check passed its line now and then). Ended with the node, as [`task`]'s are.
pub fn alone<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    #[cfg(not(target_arch = "wasm32"))]
    let task = {
        static R: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        let watch = R.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).thread_name("pondra-watch").enable_all().build().expect("a runtime")
        });
        watch.spawn(f)
    };
    #[cfg(target_arch = "wasm32")]
    let task = tokio::task::spawn(f); // (a page has one loop, and the watch runs on it)
    tracked(task)
}

/// How long a request runs before the ones after it go to the queries' runtime: under load a
/// query's time grows as the cores are shared, so dashboards beside writers pass it at once.
const LONG: std::time::Duration = std::time::Duration::from_millis(10);

/// The queries' runtime: made when first asked for, with a thread a core, as [`runtime`] makes them.
#[cfg(not(target_arch = "wasm32"))]
fn queries() -> &'static tokio::runtime::Runtime {
    static R: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    R.get_or_init(|| {
        runtime().thread_name("pondra-query").build().expect("a runtime")
    })
}

/// A runtime as the node's are made (`main.rs`'s and the queries'): Linux's main stack on every
/// thread, 8 MB (invariant 195), and few blocking threads, each gone after a second idle. On a
/// local lake every file read and write is a blocking task; under small steady commits they come
/// often enough that no thread of the pool ever sat idle a second, so it stayed at the most it had
/// ever needed at once (about 90 on 4 cores), and each thread's heap kept what its work had used: a
/// node grew 2–5 MB a minute (the 24-hour soak) while what it held stayed at 80–90 MB. With four a
/// core, 16 at least, it levels off, and TPC-H reads files as fast (`harness.py memory`). Windows
/// keeps tokio's 512: there a child's pipes are read on blocking threads (each Python worker
/// holds one), and a cap could leave none for files.
#[cfg(not(target_arch = "wasm32"))]
pub fn runtime() -> tokio::runtime::Builder {
    let cores = crate::env::cores();
    let most = crate::env::or("PONDRA_BLOCKING_THREADS", if cfg!(windows) { 512 } else { 16.max(4 * cores) });
    let mut b = tokio::runtime::Builder::new_multi_thread();
    b.enable_all().thread_stack_size(8 << 20).thread_keep_alive(std::time::Duration::from_secs(1)).max_blocking_threads(most);
    b
}

/// Start a loop the node can't do without: if it panics, the node stops.
pub fn spawn<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    tracked(tokio::spawn(async move {
        match AssertUnwindSafe(f).catch_unwind().await {
            Ok(v) => v,
            Err(p) => {
                // (a library's node restarting in place unwinds the task that asked for it, and its supervisor has been told
                // already: that is no stop. Otherwise the node stops; in a library `stop` returns and the panic unwinds on)
                if p.downcast_ref::<crate::node::Restarting>().is_none() {
                    crate::node::stop(format!("a panic in the node's own work: {}", said(&*p)));
                }
                std::panic::resume_unwind(p)
            }
        }
    }))
}

/// Start a task of the node's whose panic is its own (a connection's, work beside a request): what
/// `tokio::spawn` was, but ended with the node (`tools/per_node_check.py` fails a `tokio::spawn`).
pub fn task<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    tracked(tokio::spawn(f))
}

/// Start a task the node's stop must not end (`abort_all`), which ends by itself: a catalog's close, or a guest's open that
/// closes after (`node.rs`). Cut midway, a close leaves its flush half done and an open its reader's poller running for
/// ever. Anything else starts with [`spawn`] or [`task`].
pub fn untracked<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    tokio::spawn(f)
}

// What this node started (`spawn`, `task`, `aside`), so a node stopped in a process ends it all
// (ADR-061 §4): the runtime outlives the node, and a loop left on it would go on writing its lake.
crate::per_node!(static STARTED: std::sync::Mutex<Started> = std::sync::Mutex::new(Started { tasks: Vec::new(), prune_at: 0 }));

#[derive(Default)]
struct Started {
    tasks: Vec<tokio::task::AbortHandle>,
    /// How many before the finished ones are let go: twice what was left last time, so a task costs
    /// O(1) on average however many stay running (1,000 connections open).
    prune_at: usize,
}

fn tracked<T>(t: tokio::task::JoinHandle<T>) -> tokio::task::JoinHandle<T> {
    let mut started = STARTED.lock().unwrap_or_else(|e| e.into_inner());
    if started.tasks.len() >= started.prune_at {
        started.tasks.retain(|h| !h.is_finished());
        started.prune_at = 64.max(2 * started.tasks.len());
    }
    started.tasks.push(t.abort_handle());
    t
}

/// Ends every task this node started (`Node::stop`, before its catalog closes and its term is given
/// back: a keep-alive left running would mark it alive again), but the one asking.
/// (On a page a task has no id, and the one asking is the page's own call, not a task: every one ends.)
pub fn abort_all() {
    #[cfg(not(target_arch = "wasm32"))]
    let me = tokio::task::try_id();
    for t in std::mem::take(&mut STARTED.lock().unwrap_or_else(|e| e.into_inner()).tasks) {
        #[cfg(not(target_arch = "wasm32"))]
        if Some(t.id()) == me {
            continue;
        }
        t.abort();
    }
}

/// A panic's message.
pub fn said(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(r) = p.downcast_ref::<crate::node::Restarting>() {
        return r.to_string(); // (57P03: a request that met a node restarting in place asks again)
    }
    let m = p.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| p.downcast_ref::<String>().cloned());
    format!("internal error: {}", m.unwrap_or_else(|| "a panic".into()))
}

/// `pondra_panic()`: a SQL function that panics, for the tests (`PONDRA_TEST_PANICS=1` only).
pub fn test_function(ctx: &datafusion::prelude::SessionContext) {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    if crate::env::flag("PONDRA_TEST_PANICS") {
        let f = |_: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> { panic!("pondra_panic() was called") };
        ctx.register_udf(create_udf("pondra_panic", vec![], DataType::Int64, Volatility::Volatile, std::sync::Arc::new(f)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // (a request that started while the watch waited has no reading: `note` gives it the watch's next one, and
    // keeps the others'; the entries are this test's own, with no stop, so no other test can stop them)
    #[test]
    fn a_request_that_started_unread_takes_the_watchs_next_reading() {
        let unread = crate::oom::Running { since: Instant::now(), noted: usize::MAX, stop: None, stopped: false, used: None, left: None, first_try: None };
        let read = crate::oom::Running { since: Instant::now(), noted: 5, stop: None, stopped: false, used: None, left: None, first_try: None };
        RUNNING.lock().unwrap().insert(u64::MAX - 1, unread);
        RUNNING.lock().unwrap().insert(u64::MAX, read);
        note(7 << 20);
        {
            let running = RUNNING.lock().unwrap();
            assert_eq!(running.get(&(u64::MAX - 1)).expect("the unread one").noted, 7 << 20, "the unread one takes the reading");
            assert_eq!(running.get(&u64::MAX).expect("the read one").noted, 5, "a read one keeps its own");
        }
        RUNNING.lock().unwrap().retain(|id, _| *id < u64::MAX - 1);
    }

    // (one test, not two: the running requests are the process's, and a parallel test's stop would be
    // this one's to see)
    #[test]
    fn a_stop_takes_a_stoppable_request_and_answers_out_of_memory() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
        rt.block_on(async {
            let later = || Instant::now() + std::time::Duration::from_secs(2); // (a request that has run a second or more)
            let asked = tokio::spawn(work(false, std::future::pending::<()>()));
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(stop_one(later(), 0), None, "a request that may not be stopped is passed over");
            asked.abort();
            let stoppable = tokio::spawn(work(true, std::future::pending::<()>()));
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(stop_one(later(), 0), Some(false), "a stoppable request is stopped");
            assert_eq!(stoppable.await.expect("the task ends"), Err(crate::oom::OOM.to_string()));
            assert_eq!(stop_one(later(), 0), None, "a stopped request is not stopped again");
            assert_eq!(work(true, async { 5 }).await, Ok(5), "a request that ends on its own answers");
            // A stopped request whose spawned work still runs holds the stop until that work has ended: the cheap
            // request beside it is not stopped meanwhile, and is once the work is done (and the memory still over).
            let cheap = tokio::spawn(work(true, std::future::pending::<()>()));
            let (done, held) = tokio::sync::oneshot::channel::<()>();
            let grower = tokio::spawn(work(true, async move {
                let used = crate::oom::counter().expect("a stoppable request's counter");
                tokio::spawn(async move {
                    let _used = used; // (as a DataFusion task holds it: `oom::TRACER`)
                    let _ = held.await;
                });
                std::future::pending::<()>().await
            }));
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            {
                let mut running = RUNNING.lock().unwrap();
                let first = running.values_mut().find(|r| r.stop.is_some()).expect("the cheap one");
                first.used.as_ref().expect("its counter").store(-(1 << 40), std::sync::atomic::Ordering::Relaxed); // (it freed, not grew)
                let last = running.values_mut().last().expect("the grower");
                last.used.as_ref().expect("its counter").store(1 << 40, std::sync::atomic::Ordering::Relaxed);
            }
            assert_eq!(stop_one(later(), 0), Some(false), "the grower is stopped");
            assert_eq!(grower.await.expect("the task ends"), Err(crate::oom::OOM.to_string()));
            assert!(stopping(), "its spawned work still runs: the stop holds");
            assert_eq!(stop_one(later(), 0), None, "the cheap one is not stopped for the grower's memory");
            drop(done);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(!stopping(), "its spawned work has ended: the grower has left");
            assert_eq!(stop_one(later(), 0), Some(false), "a stop now takes the cheap one, the one left");
            assert_eq!(cheap.await.expect("the task ends"), Err(crate::oom::OOM.to_string()));
            // A request in a first try: the stop takes the try, the request goes on to its second, and a stop then
            // takes the request. A first try inside another isn't one of its own, and outside a request it just runs.
            let (tried, mut told) = tokio::sync::watch::channel(0);
            let twice = tokio::spawn(work(true, async move {
                let first = first_try(async {
                    assert_eq!(first_try(async { 1 }, |_| false).await, Ok(1), "a try inside a first try runs as it is");
                    let _ = tried.send(1);
                    std::future::pending::<()>().await
                }, |_| false).await;
                let _ = tried.send(2);
                std::future::pending::<()>().await;
                first
            }));
            told.wait_for(|n| *n == 1).await.expect("the first try runs");
            assert_eq!(stop_one(later(), 0), Some(true), "a request in its first try: the try is stopped");
            told.wait_for(|n| *n == 2).await.expect("the request goes on after its first try");
            assert_eq!(stop_one(later(), 0), Some(false), "past its first try, the request is stopped");
            assert_eq!(twice.await.expect("the task ends"), Err(crate::oom::OOM.to_string()));
            assert_eq!(first_try(async { 7 }, |_| false).await, Ok(7), "outside a request, a first try just runs");
            // A first try that ends in a way its caller tries again (the pool refused it) is answered as it ended, once its
            // memory has gone, and its request may then be stopped whole, as after a stopped try.
            let (tried, mut told) = tokio::sync::watch::channel(0);
            let refused = tokio::spawn(work(true, async move {
                let first = first_try(async { 8 }, |n| *n == 8).await;
                let _ = tried.send(1);
                std::future::pending::<()>().await;
                first
            }));
            told.wait_for(|n| *n == 1).await.expect("the first try ends");
            assert_eq!(stop_one(later(), 0), Some(false), "after a try tried again, the request is stopped whole");
            assert_eq!(refused.await.expect("the task ends"), Err(crate::oom::OOM.to_string()));
        });
    }
}
