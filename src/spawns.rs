//! What the node starts, and what ends with it (redesign step 5, piece 7): the node's own loops ([`spawn`]: a panic in one
//! stops the node), its tasks whose panic is their own ([`task`]: a connection's, work beside a request), work on the
//! queries' runtime ([`aside`]), the memory watch's thread ([`alone`]), and the runtimes they run on ([`runtime`]). Every
//! floor starts its tasks here, so it is the base's: a node's stop ends what it started ([`abort_all`], ADR-061 §4). The
//! requests' half (a door's panic answered as an error, the memory watch's stops) is `panics.rs`'s, in the engine.
use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;

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
/// hot memory-line check passed its line now and then). It is the process's, not a node's (`oom::Watch`): no node's
/// stop ends it, as [`task`]'s are ended.
pub fn alone<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        static R: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        let watch = R.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).thread_name("pondra-watch").enable_all().build().expect("a runtime")
        });
        watch.spawn(f)
    }
    #[cfg(target_arch = "wasm32")]
    tokio::task::spawn(f) // (a page has one loop, and the watch runs on it)
}

/// The queries' runtime: made when first asked for, with a thread a core, as [`runtime`] makes them.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn queries() -> &'static tokio::runtime::Runtime {
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
    let most = crate::env::value("PONDRA_BLOCKING_THREADS");
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
                if p.downcast_ref::<crate::lifecycle::Restarting>().is_none() {
                    crate::stop_node(format!("a panic in the node's own work: {}", said(&*p)));
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
    if let Some(r) = p.downcast_ref::<crate::lifecycle::Restarting>() {
        return r.to_string(); // (57P03: a request that met a node restarting in place asks again)
    }
    let m = p.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| p.downcast_ref::<String>().cloned());
    format!("internal error: {}", m.unwrap_or_else(|| "a panic".into()))
}
