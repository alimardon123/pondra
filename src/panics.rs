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
pub async fn work<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> Result<T, String> {
    use std::{collections::BTreeMap, sync::Mutex, time::Instant};
    // The requests running, by when each started: ids grow with time, so the first is the oldest.
    static RUNNING: Mutex<BTreeMap<u64, Instant>> = Mutex::new(BTreeMap::new());
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    struct Stop(u64, Option<tokio::task::AbortHandle>);
    impl Drop for Stop {
        fn drop(&mut self) {
            RUNNING.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.0);
            if let Some(t) = self.1.take() {
                t.abort();
            }
        }
    }
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let long = {
        let mut running = RUNNING.lock().unwrap_or_else(|e| e.into_inner());
        let long = running.first_key_value().is_some_and(|(_, t)| t.elapsed() >= LONG);
        running.insert(id, Instant::now());
        long
    };
    let mut stop = Stop(id, None);
    if !long {
        return door(f).await;
    }
    let task = queries().spawn(door(f));
    stop.1 = Some(task.abort_handle());
    task.await.unwrap_or_else(|e| Err(format!("internal error: {e}")))
}

/// How long a request runs before the ones after it go to the queries' runtime: under load a
/// query's time grows as the cores are shared, so dashboards beside writers pass it at once.
const LONG: std::time::Duration = std::time::Duration::from_millis(10);

/// The queries' runtime: made when first asked for, with a thread a core, as [`runtime`] makes them.
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
pub fn runtime() -> tokio::runtime::Builder {
    let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
    let most = std::env::var("PONDRA_BLOCKING_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or(if cfg!(windows) { 512 } else { 16.max(4 * cores) });
    let mut b = tokio::runtime::Builder::new_multi_thread();
    b.enable_all().thread_stack_size(8 << 20).thread_keep_alive(std::time::Duration::from_secs(1)).max_blocking_threads(most);
    b
}

/// Start a loop the node can't do without: if it panics, the node stops.
pub fn spawn<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> tokio::task::JoinHandle<T> {
    tokio::spawn(async move {
        match AssertUnwindSafe(f).catch_unwind().await {
            Ok(v) => v,
            Err(p) => {
                eprintln!("stopping: a panic in the node's own work: {}", said(&*p));
                std::process::abort()
            }
        }
    })
}

/// A panic's message.
pub fn said(p: &(dyn std::any::Any + Send)) -> String {
    let m = p.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| p.downcast_ref::<String>().cloned());
    format!("internal error: {}", m.unwrap_or_else(|| "a panic".into()))
}

/// `pondra_panic()`: a SQL function that panics, for the tests (`PONDRA_TEST_PANICS=1` only).
pub fn test_function(ctx: &datafusion::prelude::SessionContext) {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    if std::env::var("PONDRA_TEST_PANICS").is_ok_and(|v| v == "1") {
        let f = |_: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> { panic!("pondra_panic() was called") };
        ctx.register_udf(create_udf("pondra_panic", vec![], DataType::Int64, Volatility::Volatile, std::sync::Arc::new(f)));
    }
}
