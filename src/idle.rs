//! `pondra serve --idle-exit SECS` (`designs/serverless-lake.md`): a node that ends itself once nobody has used it for
//! SECS and it has nothing left to do, so a platform that starts it on the next request (Cloud Run, Fly, Knative,
//! systemd's socket) bills only while there is work. It stops as on SIGTERM: drained (`drain.rs`), and a leader gives its
//! term back at once, so the next start leads straight away.
//!
//! What counts as use, each held as a [`Use`](crate::node_state::Use) while it lasts: an HTTP request until its answer has been sent (a live
//! query's or a watch's for as long as it streams, and a joined page's follow of the commit stream), but not the cluster's
//! own calls or the health checks (`node_state::still_taken`), which never end while a cluster runs; a Postgres, Kafka or
//! Flight connection while it is open; a run of a procedure or task. What is left to do, on the node that leads: rows in
//! the log not yet tiered, and a task on a schedule that isn't suspended, which runs only while a node does (said once,
//! and the node stays up); on a follower, under `--ack replicated`, the copies of the leader's commits it holds that
//! aren't in the bucket yet (`replica.rs`: a start-on-request platform's disk may not outlive the process).
use crate::node_state::NodeState;
use crate::replica::ReplicaLog;
use crate::store::Lake;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Weak};
use std::time::Duration;

/// What the node that serves holds, to see what it has left to do: its lake, whether it leads, and a follower's copies
/// of the leader's commits (`--ack replicated`). Weak, so a node stopped lets them go.
#[derive(Clone)]
pub struct Work {
    lake: Weak<Lake>,
    leads: bool,
    replica: Option<Weak<ReplicaLog>>,
}

impl Work {
    /// The work of the node whose App this is.
    pub fn of(app: &crate::app::App) -> Work { Work { lake: Arc::downgrade(&app.lake), leads: app.seq.is_some(), replica: app.replica.as_ref().map(Arc::downgrade) } }
}

/// Resolves once the node has had no use for `secs` and has nothing left to do; never when `secs` is 0. Counted from the
/// call (the node's start) or the last use, whichever came later. The uses are the node's own (`node_state::Idle`).
pub async fn ended(secs: f64, node: Arc<NodeState>, work: Work) {
    if secs <= 0.0 {
        return std::future::pending().await;
    }
    let (since, quiet) = (node.idle.now(), (secs * 1000.0) as u64);
    let tick = Duration::from_secs_f64((secs / 4.0).clamp(0.05, 5.0));
    loop {
        tokio::time::sleep(tick).await;
        let last = node.idle.last().max(since);
        if node.idle.using() > 0 || node.idle.now().saturating_sub(last) < quiet {
            continue;
        }
        if !left(&node, &work).await {
            return;
        }
    }
}

/// Has the node that serves something left to do (see the module's note)? A node whose lake is gone has: it starts again.
async fn left(node: &NodeState, work: &Work) -> bool {
    let Some(lake) = work.lake.upgrade() else { return true };
    if !work.leads {
        return work.replica.as_ref().and_then(Weak::upgrade).is_some_and(|r| r.holds()); // (pruned once the leader's flush is in the bucket)
    }
    if lake.backlog.load(SeqCst) > 0 {
        return true; // (tiered first: another look next time)
    }
    match crate::runs::tasks(&lake).await {
        Ok(tasks) => {
            let scheduled = tasks.iter().find(|(_, t)| !t.schedule.is_empty() && !t.suspended);
            if let Some((name, _)) = scheduled {
                if !node.idle.said.swap(true, SeqCst) {
                    eprintln!("--idle-exit: this node stays up while the lake has tasks on a schedule (task {name}), which run only while a node does: suspend them (ALTER TASK … SUSPEND), or keep a node running");
                }
            }
            scheduled.is_some()
        }
        Err(_) => true, // (the catalog didn't answer: another look next time)
    }
}
