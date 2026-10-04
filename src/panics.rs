//! A panic in a request is answered as an error, and the node stays up; a panic in the work the
//! node can't do without (committing, tiering, following the leader, the run log…) still stops it,
//! as before: a crashed writer restarts and reloads its state from the catalog.
//!
//! - **Doors** run each request in [`door`]: HTTP (`server::guard`), each Postgres statement, and
//!   each Kafka and Flight connection (its task: a panic ends that connection only); a query's
//!   request in [`work`], on the queries' runtime when another is running.
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
/// 22,000). A request that comes alone runs where it came in: the runtime is its own then. Dropped,
/// as when its client goes, it stops the work.
pub async fn work<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> Result<T, String> {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    static RUNNING: AtomicUsize = AtomicUsize::new(0);
    struct Stop(Option<tokio::task::AbortHandle>);
    impl Drop for Stop {
        fn drop(&mut self) {
            RUNNING.fetch_sub(1, SeqCst);
            if let Some(t) = self.0.take() {
                t.abort();
            }
        }
    }
    let others = RUNNING.fetch_add(1, SeqCst);
    let mut stop = Stop(None);
    if others == 0 {
        return door(f).await; // (alone: the hop would cost more than it saves, a parked thread woken each way)
    }
    let task = queries().spawn(door(f));
    stop.0 = Some(task.abort_handle());
    task.await.unwrap_or_else(|e| Err(format!("internal error: {e}")))
}

/// The queries' runtime: made when first asked for, with a thread a core and the node's stacks.
fn queries() -> &'static tokio::runtime::Runtime {
    static R: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    R.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread().thread_name("pondra-query").thread_stack_size(8 << 20).enable_all().build().expect("a runtime")
    })
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
