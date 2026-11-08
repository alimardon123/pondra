//! Stopping without dropping anything (ADR-039). A node told to stop (SIGTERM from a scheduler or
//! `pondra` scaling down, Ctrl-C, the program that started it ending) drains first:
//!
//! 1. it says it isn't ready (`GET /ready` answers 503; `GET /healthz` stays 200 while the process
//!    runs), so a load balancer stops sending it work, and waits `PONDRA_DRAIN_GRACE_SECS` (0) for
//!    one to notice;
//! 2. new requests are turned away with an answer a client retries elsewhere (HTTP 503 with
//!    `Retry-After`; a new Postgres connection is refused, a new statement on an open one answered
//!    57P01; a new Kafka connection is closed at once, and an open one before its next request),
//!    except the cluster's own calls, which a leader's followers and a query's other nodes still need;
//! 3. requests in flight at every door that has them (HTTP, each Postgres statement, each Kafka produce
//!    and offset commit until its answer is written) finish, for up to `PONDRA_DRAIN_SECS` (30). Only those that came before the signal are waited for: a leader's
//!    followers keep sending it flushes until it steps down, and on a bucket one is always in flight,
//!    so waiting for none held a leader the whole 30 s (and a scheduler's kill came first);
//! 4. then the leader checkpoints its catalog. Everything acknowledged was durable already
//!    (`--ack durable`) or is held by the followers (`--ack replicated`), so nothing waits on the
//!    bucket here;
//! 5. then the HTTP and Flight doors close (`close_door`, from `Held::stop`): an answer still being written goes
//!    out whole, for at most what is left of `PONDRA_DRAIN_SECS` (and at least 250 ms, for the last
//!    bytes hyper holds, once it took a connection), and the streams that never end by themselves (a
//!    commit feed, a watch, a live query) end there, their clients reconnecting to another node.
//!    Last, a leader gives up its term, so the next leader takes over at once.
//!
//! A connection is served on a task of its own, which a node's stop doesn't end: in a process that
//! goes on after the node stops (a library, a lake moved), one still writing past the bound stays up
//! on the old router until its client goes; a program's exit ends it, as before.
use crate::node_state::{Door, NodeState};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

/// Drain: stop being ready, wait out the grace, then for the requests in flight (bounded). The node's state says what
/// is in flight (`node_state::Drain`): a door asks it, never this module.
pub async fn drain(node: &NodeState) {
    let d = &node.drain;
    d.begin();
    tokio::time::sleep(crate::env::secs("PONDRA_DRAIN_GRACE_SECS")).await;
    let (start, most) = (tokio::time::Instant::now(), crate::env::secs("PONDRA_DRAIN_SECS"));
    *d.started.lock().unwrap_or_else(|e| e.into_inner()) = Some(start); // (the door's wait is what is left of `most`: `door_bound`)
    while d.in_flight() > 0 && start.elapsed() < most {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let left = d.in_flight();
    if left > 0 {
        eprintln!("stopping with {left} request{} still running after {:?} (PONDRA_DRAIN_SECS)", if left == 1 { "" } else { "s" }, most);
    }
    crate::locks::release_all(); // (the transactions' row locks go to their leaders now: other waits end at once, not at the lease)
}

/// How long `close_door` waits: what is left of the drain's `PONDRA_DRAIN_SECS`, a second when no drain ran (a
/// library's stop after a failure), nothing after a second signal.
fn door_bound(node: &NodeState) -> Duration {
    if node.drain.hurried.load(SeqCst) {
        return Duration::ZERO;
    }
    match *node.drain.started.lock().unwrap_or_else(|e| e.into_inner()) {
        Some(start) => crate::env::secs("PONDRA_DRAIN_SECS").saturating_sub(start.elapsed()),
        None => Duration::from_secs(1),
    }
}

/// Closes the HTTP door (`Held::stop`, once the leader has checkpointed): the streams that never end by themselves end
/// (`Drain::endless`), and the answers being written go out whole. Returns once the door has closed or `door_bound` has
/// passed, and at least 250 ms after it began, which lets the last bytes hyper holds reach the client, when the door took
/// a connection (a library's, asked in its process, mostly takes none: its close took 0.26 s for this wait alone).
pub async fn close_door(node: &NodeState) {
    let d = &node.drain;
    let opened = *d.door.borrow() != Door::Unopened;
    if !opened {
        return; // (no door was opened: a node that failed to start, or a program with no HTTP door)
    }
    let started = tokio::time::Instant::now();
    d.door.send_if_modified(|door| {
        let serving = *door == Door::Open;
        if serving {
            *door = Door::Closing;
        }
        serving
    });
    let bound = door_bound(node);
    let mut door = d.door.subscribe();
    let others = d.others.clone();
    let shut = tokio::time::timeout(bound, async {
        drop(door.wait_for(|d| *d == Door::Closed).await);
        while others.load(SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await; // (Flight's door: its connections closing, `Drain::serving`)
        }
    })
    .await
    .is_ok();
    if !d.hurried.load(SeqCst) && d.took.load(SeqCst) {
        tokio::time::sleep_until(started + Duration::from_millis(250)).await;
    }
    if !shut {
        eprintln!("stopping with a door still open after {:?}", started.elapsed());
    }
}
