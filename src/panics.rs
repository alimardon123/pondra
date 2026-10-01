//! A panic in a request is answered as an error, and the node stays up; a panic in the work the
//! node can't do without (committing, tiering, following the leader, the run log…) still stops it,
//! as before: a crashed writer restarts and reloads its state from the catalog.
//!
//! - **Doors** run each request in [`door`]: HTTP (`server::guard`), each Postgres statement, and
//!   each Kafka and Flight connection (its task: a panic ends that connection only).
//! - **The node's own loops** are started with [`spawn`]: a panic in one aborts the process.
//! - DataFusion's own tasks (a query's partitions) hand a panic back to the request that ran them.
use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;

/// Run a request: `Err` (the panic's message) if it panicked.
pub async fn door<T>(f: impl Future<Output = T>) -> Result<T, String> {
    AssertUnwindSafe(f).catch_unwind().await.map_err(|p| said(&*p))
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
