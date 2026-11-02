//! `pondra serve --idle-exit SECS` (`designs/serverless-lake.md`): a node that ends itself once nobody has used it for
//! SECS and it has nothing left to do, so a platform that starts it on the next request (Cloud Run, Fly, Knative,
//! systemd's socket) bills only while there is work. It stops as on SIGTERM: drained (`drain.rs`), and a leader gives its
//! term back at once, so the next start leads straight away.
//!
//! What counts as use, each held as a [`Use`] while it lasts: an HTTP request until its answer has been sent (a live
//! query's or a watch's for as long as it streams), but not the cluster's own calls or the health checks
//! (`drain::still_taken`), which never end while a cluster runs; a Postgres, Kafka or Flight connection while it is
//! open; a run of a procedure or task. What is left to do, on the node that leads: rows in the log not yet tiered, and a
//! task on a schedule that isn't suspended, which runs only while a node does (said once, and the node stays up).
use crate::store::Lake;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

// (each use holds its own node's count, as `drain::Busy` does: one that outlives its node's stop counts down that
// node's, never the next one's in the process)
crate::per_node!(static USING: RwLock<Arc<AtomicUsize>> = RwLock::default());
// When the last use began or ended (`log::now_ms`); 0 until the first.
crate::per_node!(static LAST: AtomicU64 = AtomicU64::new(0));
// What the node that serves holds, to see what it has left to do: its lake, and whether it leads.
crate::per_node!(static NODE: Mutex<Option<(Weak<Lake>, bool)>> = Mutex::new(None));
// Whether the wait for scheduled tasks was said (once a node).
crate::per_node!(static SAID: AtomicBool = AtomicBool::new(false));

/// Held while a use lasts (see the module's note).
pub struct Use(Arc<AtomicUsize>);

impl Use {
    pub fn new() -> Use {
        let count = USING.read().unwrap_or_else(|e| e.into_inner()).clone();
        count.fetch_add(1, SeqCst);
        LAST.store(crate::log::now_ms(), SeqCst);
        Use(count)
    }
}

impl Drop for Use {
    fn drop(&mut self) {
        LAST.store(crate::log::now_ms(), SeqCst);
        self.0.fetch_sub(1, SeqCst);
    }
}

/// A body that keeps its request a use until it has been sent, or dropped (a live query's lines, a Flight stream).
#[cfg(feature = "doors")]
pub fn kept<B: axum::body::HttpBody>(body: B, used: Use) -> impl axum::body::HttpBody<Data = B::Data, Error = B::Error> {
    http_body_util::BodyExt::map_frame(body, move |frame| {
        let _ = &used;
        frame
    })
}

/// The requests that aren't a use: the ones a cluster sends all the time (a follower's beat, its feed of the
/// leader's commits) and the health checks, which never stop while a node runs.
pub fn background(path: &str) -> bool { matches!(path, "/cluster/beat" | "/cluster/log" | "/healthz" | "/ready" | "/stats" | "/metrics") }

/// The node serving now (`node::keep`): what [`ended`] looks at for work left.
pub fn serving(lake: &Arc<Lake>, leads: bool) { *NODE.lock().unwrap_or_else(|e| e.into_inner()) = Some((Arc::downgrade(lake), leads)); }

/// Resolves once the node has had no use for `secs` and has nothing left to do; never when `secs` is 0. Counted from the
/// call (the process's start) or the last use, whichever came later.
pub async fn ended(secs: f64) {
    if secs <= 0.0 {
        return std::future::pending().await;
    }
    let (since, quiet) = (crate::log::now_ms(), (secs * 1000.0) as u64);
    let tick = Duration::from_secs_f64((secs / 4.0).clamp(0.05, 5.0));
    loop {
        tokio::time::sleep(tick).await;
        let using = USING.read().unwrap_or_else(|e| e.into_inner()).load(SeqCst);
        let last = LAST.load(SeqCst).max(since);
        if using > 0 || crate::log::now_ms().saturating_sub(last) < quiet {
            continue;
        }
        if !left().await {
            return;
        }
    }
}

/// Has the node that serves something left to do (see the module's note)? A node not yet serving has: it starts.
async fn left() -> bool {
    let node = NODE.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let Some((lake, leads)) = node else { return true };
    let Some(lake) = lake.upgrade() else { return true };
    if !leads {
        return false;
    }
    if lake.backlog.load(SeqCst) > 0 {
        return true; // (tiered first: another look next time)
    }
    match crate::runs::tasks(&lake).await {
        Ok(tasks) => {
            let scheduled = tasks.iter().find(|(_, t)| !t.schedule.is_empty() && !t.suspended);
            if let Some((name, _)) = scheduled {
                if !SAID.swap(true, SeqCst) {
                    eprintln!("--idle-exit: this node stays up while the lake has tasks on a schedule (task {name}), which run only while a node does: suspend them (ALTER TASK … SUSPEND), or keep a node running");
                }
            }
            scheduled.is_some()
        }
        Err(_) => true, // (the catalog didn't answer: another look next time)
    }
}
