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
//! 4. then the leader checkpoints its catalog. Everything acknowledged was durable already
//!    (`--ack durable`) or is held by the followers (`--ack replicated`), so nothing waits on the
//!    bucket here;
//! 5. then the HTTP door closes (`close_door`, from `Held::stop`): an answer still being written goes
//!    out whole, for at most what is left of `PONDRA_DRAIN_SECS` (and at least 250 ms, for the last
//!    bytes hyper holds), and the streams that never end by themselves (a commit feed, a watch, a live
//!    query) end there, their clients reconnecting to another node. Last, a leader gives up its term,
//!    so the next leader takes over at once.
//!
//! A connection is served on a task of its own, which a node's stop doesn't end: in a process that
//! goes on after the node stops (a library, a lake moved), one still writing past the bound stays up
//! on the old router until its client goes; a program's exit ends it, as before.
use futures::{Stream, StreamExt};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

crate::per_node!(static DRAINING: AtomicBool = AtomicBool::new(false));
// (each request holds its own node's count: one that outlives its node's stop counts down that
// node's, never the next one's in the process)
crate::per_node!(static IN_FLIGHT: RwLock<Arc<AtomicUsize>> = RwLock::default());
// The HTTP door's state, one per node as `IN_FLIGHT` is; when this node's drain began (its `PONDRA_DRAIN_SECS`
// are counted from it); and whether a second signal came.
crate::per_node!(static DOOR: tokio::sync::watch::Sender<Door> = tokio::sync::watch::channel(Door::Unopened).0);
crate::per_node!(static DRAIN_START: Mutex<Option<tokio::time::Instant>> = Mutex::new(None));
crate::per_node!(static HURRIED: AtomicBool = AtomicBool::new(false));

/// The HTTP door's state, in order: not opened, serving, closing (its streams end and its answers go out), closed.
#[derive(Clone, Copy, Default, PartialEq, PartialOrd)]
enum Door {
    #[default]
    Unopened,
    Open,
    Closing,
    Closed,
}

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

    /// The request's count, held again by the body of an answer sent in pieces, which goes on after its response: counted
    /// whatever the drain says, so it is taken only while the request's own `Busy` (`panics::door`) is held.
    pub fn carried() -> Busy { let count = in_flight(); count.fetch_add(1, SeqCst); Busy(Some(count)) }
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
    *DRAIN_START.lock().unwrap_or_else(|e| e.into_inner()) = Some(start); // (the door's wait is what is left of `most`: `door_bound`)
    while count.load(SeqCst) > 0 && start.elapsed() < most {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let left = count.load(SeqCst);
    if left > 0 {
        eprintln!("stopping with {left} request{} still running after {:?} (PONDRA_DRAIN_SECS)", if left == 1 { "" } else { "s" }, most);
    }
    crate::locks::release_all(); // (the transactions' row locks go to their leaders now: other waits end at once, not at the lease)
}

/// A second signal while draining: a node stopping now doesn't wait for its door (`close_door`).
pub fn hurry() { HURRIED.store(true, SeqCst); }

/// The HTTP door is serving (its task, when it starts): `close_door` waits for it to close.
pub(crate) fn door_open() { DOOR.send_replace(Door::Open); }

/// The HTTP door's serve has ended (its task, when it ends).
pub(crate) fn door_closed() { DOOR.send_replace(Door::Closed); }

/// Resolves once this node's door starts closing. Takes this node's receiver when it is called.
pub fn closing() -> impl Future<Output = ()> + Send + 'static {
    let mut door = DOOR.subscribe();
    async move { drop(door.wait_for(|d| *d >= Door::Closing).await) } // (its guard isn't Send: dropped here)
}

/// The stream, ended once the door starts closing. A stream that never ends by itself (a commit feed, a watch, a live
/// query's lines) would hold a graceful close for its whole bound; its client reconnects, as it does when a node goes.
pub fn endless<S: Stream + Send + 'static>(s: S) -> impl Stream<Item = S::Item> + Send { s.take_until(closing()) }

/// How long `close_door` waits: what is left of the drain's `PONDRA_DRAIN_SECS`, a second when no drain ran (a
/// library's stop after a failure), nothing after a second signal.
fn door_bound() -> Duration {
    if HURRIED.load(SeqCst) {
        return Duration::ZERO;
    }
    match *DRAIN_START.lock().unwrap_or_else(|e| e.into_inner()) {
        Some(start) => crate::env::secs("PONDRA_DRAIN_SECS", 30.0).saturating_sub(start.elapsed()),
        None => Duration::from_secs(1),
    }
}

/// Closes the HTTP door (`Held::stop`, once the leader has checkpointed): the streams that never end by themselves end
/// (`endless`), and the answers being written go out whole. Returns once the door has closed or `door_bound` has passed,
/// and at least 250 ms after it began, which lets the last bytes hyper holds reach the client.
pub async fn close_door() {
    let opened = *DOOR.borrow() != Door::Unopened;
    if !opened {
        return; // (no door was opened: a node that failed to start, or a program with no HTTP door)
    }
    let started = tokio::time::Instant::now();
    DOOR.send_if_modified(|d| {
        let serving = *d == Door::Open;
        if serving {
            *d = Door::Closing;
        }
        serving
    });
    let bound = door_bound();
    let mut door = DOOR.subscribe();
    let shut = tokio::time::timeout(bound, async { drop(door.wait_for(|d| *d == Door::Closed).await) }).await.is_ok();
    if !HURRIED.load(SeqCst) {
        tokio::time::sleep_until(started + Duration::from_millis(250)).await;
    }
    if !shut {
        eprintln!("stopping with the HTTP door still open after {:?}", started.elapsed());
    }
}
