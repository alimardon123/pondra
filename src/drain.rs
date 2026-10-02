//! Stopping without dropping anything (ADR-039). A node told to stop (SIGTERM from a scheduler or
//! `pondra` scaling down, Ctrl-C, the program that started it ending) drains first:
//!
//! 1. it says it isn't ready (`GET /ready` answers 503; `GET /healthz` stays 200 while the process
//!    runs), so a load balancer stops sending it work, and waits `PONDRA_DRAIN_GRACE_SECS` (0) for
//!    one to notice;
//! 2. new requests are turned away with an answer a client retries elsewhere (HTTP 503 with
//!    `Retry-After`; a new Postgres connection is refused), except the cluster's own calls, which a
//!    leader's followers and a query's other nodes still need;
//! 3. requests in flight at every door that has them (HTTP, each Postgres statement) finish, for up
//!    to `PONDRA_DRAIN_SECS` (30);
//! 4. then the leader checkpoints its catalog and gives up its term, so the next leader takes over
//!    at once (`main.rs`). Everything acknowledged was durable already (`--ack durable`) or is held
//!    by the followers (`--ack replicated`), so nothing waits on the bucket here.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

static DRAINING: AtomicBool = AtomicBool::new(false);
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Is this node stopping?
pub fn draining() -> bool { DRAINING.load(SeqCst) }

/// Held by a request's work while it runs (`panics::door`).
pub struct Busy;

impl Busy {
    pub fn new() -> Busy {
        IN_FLIGHT.fetch_add(1, SeqCst);
        Busy
    }
}

impl Drop for Busy {
    fn drop(&mut self) { IN_FLIGHT.fetch_sub(1, SeqCst); }
}

/// Requests a draining node still takes: the cluster's own (a leader's followers, a query's other
/// nodes) and the health checks.
pub fn still_taken(path: &str) -> bool { path.starts_with("/cluster/") || matches!(path, "/healthz" | "/ready" | "/stats" | "/metrics") }

fn secs(var: &str, default: f64) -> Duration { Duration::from_secs_f64(std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default)) }

/// Drain: stop being ready, wait out the grace, then for the requests in flight (bounded).
pub async fn drain() {
    DRAINING.store(true, SeqCst);
    tokio::time::sleep(secs("PONDRA_DRAIN_GRACE_SECS", 0.0)).await;
    let (start, most) = (tokio::time::Instant::now(), secs("PONDRA_DRAIN_SECS", 30.0));
    while IN_FLIGHT.load(SeqCst) > 0 && start.elapsed() < most {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let left = IN_FLIGHT.load(SeqCst);
    if left > 0 {
        eprintln!("stopping with {left} request{} still running after {:?} (PONDRA_DRAIN_SECS)", if left == 1 { "" } else { "s" }, most);
    }
}
