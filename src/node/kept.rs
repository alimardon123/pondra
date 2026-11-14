//! A node its host keeps in its process across restarts, until the host closes it (ADR-061 §5, §9): a library's
//! (`library::Opened`), and each database of a folder of lakes (`dbserver.rs`), several in one process. The node never
//! takes the process down: a restart halts it and starts it again in place, and a failure stops it and says why to every
//! request after, until it is closed. Its host reaches it through its doors while it runs (`Running`).
use super::{Event, Held, Node, Options, Restarting};
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// What a kept node is doing now.
#[derive(Clone, Debug, PartialEq)]
pub enum State {
    Starting,
    Running,
    /// Halted, about to start again: why.
    Restarting(String),
    /// Stopped, until opened again: why.
    Failed(String),
    Closed,
}

/// What a host reaches a kept node through while it runs: its router (`Node::router`), and its Postgres door when its host
/// hands it the connections (`Node::pg`).
#[derive(Clone)]
pub(crate) struct Running {
    pub(crate) router: axum::Router,
    pub(crate) pg: Option<Arc<crate::pg::Door>>,
}

/// A kept node's doors, there while it runs.
pub(crate) type Slot = Arc<Mutex<Option<Running>>>;

/// Pauses between tries to start again; five restarts within a minute of each other is giving up.
const PAUSES: [f64; 5] = [0.5, 1.0, 2.0, 4.0, 8.0];

/// The node in place: what exec does to a process. Its tasks end, its state (its term's lock file,
/// whose file says "restarting", its Python workers) is let go, and its lake with it; nothing waits
/// on the bucket, and the term isn't given back: a fenced or cut-off node has nothing to make durable.
pub(crate) fn halt(held: &Held, node: Option<Node>) {
    held.abort_tasks();
    drop(node);
}

/// Keeps the node `options` start until it is closed (`Event::Close`, or its host let go of), saying what it does in
/// `state` and holding its doors in `running` while it runs. The first start's outcome goes to `first`. A lake that moves
/// (a library's, whose lake may be in memory) is settled at its new place and the node started there. The node's own work
/// (`auth::as_node`), whichever host keeps it: a request is its caller's.
pub(crate) async fn supervise(options: Options, state: Arc<Mutex<State>>, running: Slot, told: super::Told, first: std::sync::mpsc::Sender<Result<(), String>>) {
    crate::auth::as_node(supervising(options, state, running, told, first)).await
}

async fn supervising(mut options: Options, state: Arc<Mutex<State>>, running: Slot, told: super::Told, first: std::sync::mpsc::Sender<Result<(), String>>) {
    let super::Told { mut events, mut moves } = told; // (its nodes': their failures, restarts and the close; their lakes' moves)
    // (its doors are there exactly while the node runs: a request at any other time is told why)
    let set = |s: State| {
        let mut r = running.lock().unwrap_or_else(PoisonError::into_inner);
        if s != State::Running {
            r.take();
        }
        *state.lock().unwrap_or_else(PoisonError::into_inner) = s;
    };
    let mut first = Some(first);
    let mut restarts: Vec<Instant> = vec![];
    loop {
        set(State::Starting);
        // (a start can be told to restart by a task it started, or unwind with `Restarting` itself)
        let held = Arc::new(Held::default()); // (this start's: a start dropped midway still ends what it started, `halt`)
        let start = AssertUnwindSafe(Node::open_in(options.clone(), held.clone())).catch_unwind();
        let why = tokio::select! {
            started = start => match started {
                Ok(Ok(node)) => {
                    *running.lock().unwrap_or_else(PoisonError::into_inner) = Some(Running { router: node.router(), pg: node.pg() });
                    set(State::Running);
                    if let Some(f) = first.take() {
                        let _ = f.send(Ok(()));
                    }
                    let since = Instant::now();
                    let next = tokio::select! {
                        e = events.recv() => e,
                        Some(m) = moves.recv() => {
                            // The lake moves: its requests answered, the node stopped, its objects settled on the
                            // target, and the node started there. A move is not a restart, so it is not counted.
                            node.drain().await;
                            set(State::Restarting(format!("moving the lake to {}", m.to)));
                            node.stop().await;
                            if let Err(e) = crate::memory::settle(&m).await {
                                // (the lake stays in memory: the node starts again on it, and says why in `/stats`)
                                options.move_failed = Some(crate::memory::move_failed(&m.to, &e));
                                continue;
                            }
                            (options.dir, options.move_failed) = (m.to, None);
                            continue;
                        }
                    };
                    match next {
                        Some(Event::Restart(why)) => {
                            running.lock().unwrap_or_else(PoisonError::into_inner).take(); // (no request reaches a node being halted)
                            halt(&held, Some(node));
                            if since.elapsed() >= Duration::from_secs(60) {
                                restarts.clear(); // (a node that served a minute starts the count again)
                            }
                            why
                        }
                        Some(Event::Failed(why)) => {
                            set(State::Failed(why.clone())); // (said at once: the stop may wait on the bucket)
                            if tokio::time::timeout(Duration::from_secs(30), node.stop()).await.is_err() {
                                halt(&held, None);
                            }
                            return failed_until_closed(&set, why, &mut events, first.take()).await;
                        }
                        Some(Event::Close) | None => {
                            let t0 = std::time::Instant::now();
                            node.drain().await; // (its requests in flight answered: the router stays till then)
                            if crate::env::is_set("PONDRA_TRACE_START") {
                                eprintln!("stop: drained {:.2} s", t0.elapsed().as_secs_f64());
                            }
                            running.lock().unwrap_or_else(PoisonError::into_inner).take();
                            node.stop().await;
                            set(State::Closed);
                            return;
                        }
                    }
                }
                Ok(Err(e)) if e.downcast_ref::<crate::format::Newer>().is_some() || crate::memory_lake::torn(&e) => {
                    return failed_until_closed(&set, format!("{e:#}"), &mut events, first.take()).await; // (no try will open it: nor a place a move didn't finish)
                }
                Ok(Err(e)) => format!("{e:#}"), // (`Node::open` gave back what it took)
                Err(p) => {
                    halt(&held, None);
                    match p.downcast::<Restarting>() {
                        Ok(r) => r.0,
                        Err(p) => return failed_until_closed(&set, crate::spawns::said(&*p), &mut events, first.take()).await,
                    }
                }
            },
            e = events.recv() => match e {
                Some(Event::Restart(why)) => {
                    halt(&held, None); // (the start's future is dropped: what it took goes with its statics)
                    why
                }
                Some(Event::Failed(why)) => {
                    halt(&held, None);
                    return failed_until_closed(&set, why, &mut events, first.take()).await;
                }
                Some(Event::Close) | None => {
                    halt(&held, None);
                    set(State::Closed);
                    return;
                }
            },
        };
        while let Ok(e) = events.try_recv() {
            // (the same trouble said by several tasks at once: one restart; a close still closes)
            if matches!(e, Event::Close) {
                set(State::Closed);
                return;
            }
        }
        restarts.retain(|t| t.elapsed() < Duration::from_secs(60));
        let Some(pause) = PAUSES.get(restarts.len()) else {
            return failed_until_closed(&set, format!("restarted {} times within a minute; the last time: {why}", PAUSES.len()), &mut events, first.take()).await;
        };
        restarts.push(Instant::now());
        eprintln!("restarting the node in place: {why}");
        set(State::Restarting(why));
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs_f64(*pause)) => {}
            e = events.recv() => if matches!(e, Some(Event::Close) | None) {
                set(State::Closed);
                return;
            },
        }
    }
}

/// Failed: said to whoever opened it, then kept until it is closed.
async fn failed_until_closed(set: &impl Fn(State), why: String, events: &mut mpsc::UnboundedReceiver<Event>, first: Option<std::sync::mpsc::Sender<Result<(), String>>>) {
    set(State::Failed(why.clone()));
    if let Some(f) = first {
        let _ = f.send(Err(why));
    }
    while let Some(e) = events.recv().await {
        if matches!(e, Event::Close) {
            break;
        }
    }
    set(State::Closed);
}
