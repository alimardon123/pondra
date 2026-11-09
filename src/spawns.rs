//! What the node starts, and what ends with it (redesign step 5, piece 7). A node's tasks are its [`Tasks`]
//! (`NodeState::tasks`), started two ways, each saying whose work it is: the node's own ([`Tasks::spawn_node`], vital or
//! not: [`Vital`]) or a request's ([`Tasks::spawn_as`], as a [`Carry`]: its caller's, or a door's connection), and on the
//! queries' runtime the same ([`Tasks::aside`]). A node's stop ends them all ([`Tasks::abort_all`], ADR-061 §4). Also here:
//! the memory watch's thread ([`alone`]), a close the stop must not cut ([`untracked`]) and the runtimes ([`runtime`]).
//! Every floor starts its tasks here, so it is the base's. The requests' half (a door's panic answered as an error, the
//! memory watch's stops) is `panics.rs`'s, in the engine. A loop that serves the node's statics, with no node in hand, is
//! theirs ([`for_statics`]): it ends as they are set back.
use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;

/// Start a loop that must keep its pace whatever the queries do (the memory watch) on a thread of its own: on the
/// node's runtime, a query's partitions busy on every core held its timer back past the moment it had to look (the
/// hot memory-line check passed its line now and then). It is the process's, not a node's (`oom::Watch`): no node's
/// stop ends it, as a node's [`Tasks`] are ended.
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

/// Start a task the node's stop must not end (its list's `abort_all`), which ends by itself: a catalog's close, or a guest's
/// open that closes after (`node.rs`). Cut midway, a close leaves its flush half done and an open its reader's poller
/// running for ever.
pub fn untracked<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    tokio::spawn(f)
}

/// Start a loop that serves the node's statics (`per_node!`: its sessions' reaper, its Python pools', its sign-in keys'
/// refresh), whose callers have no node in hand: it is the statics' own, and ends as they are
/// set back (`reset::reset`, a node's stop's last step). It runs as nobody (no principal, not the node's: it reads no
/// table), and its panic is its own.
pub fn for_statics<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    let t = tokio::spawn(f);
    STATICS.lock().unwrap_or_else(|e| e.into_inner()).track(t.abort_handle());
    t
}

crate::per_node!(static STATICS: std::sync::Mutex<Started> = Default::default(); clear = |s| s.lock().unwrap_or_else(|e| e.into_inner()).end());

/// What a list started and hasn't seen end (a node's [`Tasks`], the statics' loops).
#[derive(Default)]
struct Started {
    tasks: Vec<tokio::task::AbortHandle>,
    /// How many before the finished ones are let go: twice what was left last time, so a task costs
    /// O(1) on average however many stay running (1,000 connections open).
    prune_at: usize,
}

impl Started {
    fn track(&mut self, t: tokio::task::AbortHandle) {
        if self.tasks.len() >= self.prune_at {
            self.tasks.retain(|h| !h.is_finished());
            self.prune_at = 64.max(2 * self.tasks.len());
        }
        self.tasks.push(t);
    }

    /// Ends every one but the task asking. (On a page a task has no id, and the one asking is the page's own call, not a
    /// task: every one ends.)
    fn end(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        let me = tokio::task::try_id();
        for t in std::mem::take(&mut self.tasks) {
            #[cfg(not(target_arch = "wasm32"))]
            if Some(t.id()) == me {
                continue;
            }
            t.abort();
        }
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

tokio::task_local! {
    /// Set around the node's own work, which no request asked for ([`as_node`]: a loop of the node's, a task's tick).
    static NODE: ();
}

/// `f` as the node's own work: with no principal set, rights take it for the node's (`auth::Principal::node`), and work
/// with neither is refused and sees nothing. A spawned task keeps no task-local, so [`Tasks::spawn_node`] sets it.
pub async fn as_node<F: Future>(f: F) -> F::Output { NODE.scope((), f).await }

/// Is this the node's own work ([`as_node`])?
pub fn node_work() -> bool { NODE.try_with(|_| ()).is_ok() }

/// Whether a panic in the node's own task stops the node ([`Tasks::spawn_node`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Vital {
    /// A loop the node can't do without (committing, tiering, following the leader, an accept loop): its panic stops it.
    Stop,
    /// Work whose panic is its own (a sweep's round, a cache's fetch): the task ends, the node goes on.
    Own,
}

/// Whose work a task a request starts is, carried into it ([`Tasks::spawn_as`]): a spawned task keeps no task-local, so
/// what it would read of its caller (its principal, its session) is captured here when it is started, and set again
/// around it. `auth` makes them: the caller's (`auth::Carried`), or a door's connection (`auth::Door`), which sets each
/// request's principal itself.
pub trait Carry: Send + 'static {
    /// `f`, run as whoever this says.
    fn carry<F: Future + Send + 'static>(self, f: F) -> impl Future<Output = F::Output> + Send + 'static
    where
        F::Output: Send;
}

/// What a node started, so its stop ends it all (ADR-061 §4: the runtime outlives the node, and a loop left on it would go
/// on writing its lake). Its two ways to start a task say whose work it is, and the compiler checks every site says one.
/// A clone is the same list (a loop that starts tasks of its own keeps one).
#[derive(Clone)]
pub struct Tasks {
    life: crate::lifecycle::Life, // (what a vital panic stops: the node's host's, or a page's member)
    started: std::sync::Arc<std::sync::Mutex<Started>>,
}

impl Tasks {
    /// What a node with this lifecycle starts: nothing yet.
    pub fn new(life: crate::lifecycle::Life) -> Tasks { Tasks { life, started: Default::default() } }

    /// Start the node's own work (no request asked for it), as [`as_node`]. A panic in it stops the node (its lifecycle's
    /// `stop`) when `vital` is [`Vital::Stop`], and ends only the task otherwise.
    pub fn spawn_node<T: Send + 'static>(&self, vital: Vital, f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
        let (f, life) = (as_node(f), self.life.clone());
        self.track(tokio::spawn(async move {
            match AssertUnwindSafe(f).catch_unwind().await {
                Ok(v) => v,
                Err(p) => {
                    // (a library's node restarting in place unwinds the task that asked for it, and its supervisor has been
                    // told already: that is no stop. Otherwise the node stops; in a library `stop` returns and the panic
                    // unwinds on)
                    if vital == Vital::Stop && p.downcast_ref::<crate::lifecycle::Restarting>().is_none() {
                        life.stop(format!("a panic in the node's own work: {}", said(&*p)));
                    }
                    std::panic::resume_unwind(p)
                }
            }
        }))
    }

    /// Start work a request asked for, as `who` (its caller, or a door's connection): its panic is its own, never the
    /// node's.
    pub fn spawn_as<T: Send + 'static>(&self, who: impl Carry, f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
        self.track(tokio::spawn(who.carry(f)))
    }

    /// Start work that reads and writes as queries do (a view running its query again) on the queries' runtime, as `who`
    /// ([`Node`], or a request's caller): appends and commits never wait behind it (invariant 231). Its panic is its own,
    /// as a request's is.
    pub fn aside<T: Send + 'static>(&self, who: impl Carry, f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
        let f = who.carry(f);
        #[cfg(not(target_arch = "wasm32"))]
        let task = queries().spawn(f);
        #[cfg(target_arch = "wasm32")]
        let task = tokio::task::spawn(f); // (a page has one loop, and no queries' runtime: the work runs on it)
        self.track(task)
    }

    fn track<T>(&self, t: tokio::task::JoinHandle<T>) -> tokio::task::JoinHandle<T> {
        self.started.lock().unwrap_or_else(|e| e.into_inner()).track(t.abort_handle());
        t
    }

    /// Ends every task this node started, but the one asking (`Node::stop`, before its catalog closes and its term is
    /// given back: a keep-alive left running would mark it alive again).
    pub fn abort_all(&self) { self.started.lock().unwrap_or_else(|e| e.into_inner()).end() }
}

/// The node's own work, as a [`Carry`] ([`Tasks::aside`] of a view's run): [`as_node`].
pub struct Node;

impl Carry for Node {
    fn carry<F: Future + Send + 'static>(self, f: F) -> impl Future<Output = F::Output> + Send + 'static
    where
        F::Output: Send,
    {
        as_node(f)
    }
}
