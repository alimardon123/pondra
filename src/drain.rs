//! Stopping without dropping anything (ADR-039). A node told to stop (SIGTERM from a scheduler or
//! `pondra` scaling down, Ctrl-C, the program that started it ending) drains first:
//!
//! 1. it says it isn't ready (`GET /ready` answers 503; `GET /healthz` stays 200 while the process
//!    runs), so a load balancer stops sending it work, and waits `PONDRA_DRAIN_GRACE_SECS` (0) for
//!    one to notice;
//! 2. new requests are turned away with an answer a client retries elsewhere (HTTP 503 with
//!    `Retry-After`; a new Postgres connection is refused, a new statement on an open one answered
//!    57P01), except the cluster's own calls, which a leader's followers and a query's other nodes
//!    still need;
//! 3. requests in flight at every door that has them (HTTP, each Postgres statement) finish, for up
//!    to `PONDRA_DRAIN_SECS` (30). Only those that came before the signal are waited for: a leader's
//!    followers keep sending it flushes until it steps down, and on a bucket one is always in flight,
//!    so waiting for none held a leader the whole 30 s (and a scheduler's kill came first);
//! 4. then the leader checkpoints its catalog and gives up its term, so the next leader takes over
//!    at once (`main.rs`). Everything acknowledged was durable already (`--ack durable`) or is held
//!    by the followers (`--ack replicated`), so nothing waits on the bucket here.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, RwLock};
use std::time::Duration;

crate::per_node!(static DRAINING: AtomicBool = AtomicBool::new(false));
// (each request holds its own node's count: one that outlives its node's stop counts down that
// node's, never the next one's in the process)
crate::per_node!(static IN_FLIGHT: RwLock<Arc<AtomicUsize>> = RwLock::default());

/// This node's requests in flight.
fn in_flight() -> Arc<AtomicUsize> { IN_FLIGHT.read().unwrap_or_else(|e| e.into_inner()).clone() }

/// Is this node stopping?
pub fn draining() -> bool { DRAINING.load(SeqCst) }

/// Held by a request's work while it runs (`panics::door`): counted if it came before the drain.
pub struct Busy(Option<Arc<AtomicUsize>>);

impl Busy {
    pub fn new() -> Busy {
        // (counted, then the flag read: with `drain` storing the flag, then reading the count, one of
        // them sees the other, so no request that came before the drain goes uncounted)
        let count = in_flight();
        count.fetch_add(1, SeqCst);
        if draining() {
            count.fetch_sub(1, SeqCst);
            return Busy(None);
        }
        Busy(Some(count))
    }
}

impl Drop for Busy {
    fn drop(&mut self) {
        if let Some(count) = &self.0 {
            count.fetch_sub(1, SeqCst);
        }
    }
}

/// Requests a draining node still takes: the cluster's own (a leader's followers, a query's other
/// nodes) and the health checks.
pub fn still_taken(path: &str) -> bool { path.starts_with("/cluster/") || matches!(path, "/healthz" | "/ready" | "/stats" | "/metrics") }

/// Drain: stop being ready, wait out the grace, then for the requests in flight (bounded).
pub async fn drain() {
    DRAINING.store(true, SeqCst);
    tokio::time::sleep(crate::env::secs("PONDRA_DRAIN_GRACE_SECS", 0.0)).await;
    let (start, most, count) = (tokio::time::Instant::now(), crate::env::secs("PONDRA_DRAIN_SECS", 30.0), in_flight());
    while count.load(SeqCst) > 0 && start.elapsed() < most {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let left = count.load(SeqCst);
    if left > 0 {
        eprintln!("stopping with {left} request{} still running after {:?} (PONDRA_DRAIN_SECS)", if left == 1 { "" } else { "s" }, most);
    }
}
