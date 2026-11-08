//! The node as the code below the hosts sees it (redesign §8, step 2): what its host does when it must stop or start
//! again ([`Lifecycle`](crate::lifecycle::Lifecycle)), and what its stop waits for and refuses: whether it drains, the
//! requests and uses its doors hold, and its HTTP door's state. Every lake the node opens is handed it (`Lake::open`), so
//! a door reaches it through its App's lake (`app.lake.node`). It is never a static: two nodes in one process drain and
//! restart apart. What a stop does with it is the hosts' (`drain::drain`, `drain::close_door`, `idle::ended`).
use crate::lifecycle::Life;
use futures::{Stream, StreamExt};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

/// A node's lifecycle and its doors' state, shared by every lake it opens.
pub struct NodeState {
    /// What its host does when it must stop or start again.
    pub life: Life,
    /// The machine it runs on: its process's (`machine::install`), or, for the sim's members, one of its own.
    pub machine: crate::machine::Mach,
    /// The clients its calls out are made with, by whom each goes to: its process's host's (`net::install`), or its own.
    #[cfg(feature = "lake")]
    pub net: std::sync::Arc<dyn crate::net::Net>,
    /// The space it spills into past its memory: its process's host's (`scratch::install`), or this machine's folders.
    #[cfg(feature = "lake")]
    pub scratch: std::sync::Arc<dyn crate::scratch::Scratch>,
    /// Its settings, read once as it started (`settings::Values`).
    pub settings: crate::settings::Values,
    /// What its drain refuses and waits for (`drain.rs`).
    pub drain: Drain,
    /// What counts as a use of it, for `--idle-exit` (`idle.rs`).
    pub idle: Idle,
    /// Its budget of the process's memory (`ledger::Ledger` on the process's `ledger::Root`): the line its lakes' queries
    /// and caches share, made as its first lake opens (`store::lake_pool`).
    pub(crate) line: std::sync::OnceLock<Arc<crate::ledger::Ledger>>,
    /// When its start began: what `PONDRA_TRACE_START`'s times count from (`trace`).
    began: datafusion::common::instant::Instant,
}

impl NodeState {
    /// A node's state, before its doors open: not draining, nothing in flight, on its process's machine, with its process's
    /// settings as they are now.
    pub fn new(life: Life) -> Arc<NodeState> { NodeState::starting(life, crate::settings::Values::now()) }

    /// A node's state with the settings it starts with (`node::Options::settings`).
    pub fn starting(life: Life, settings: crate::settings::Values) -> Arc<NodeState> { NodeState::make(life, crate::machine::installed(), settings) }

    /// A node's state on a machine of its own (the sim's member: one with little memory, say).
    pub fn on(life: Life, machine: crate::machine::Mach) -> Arc<NodeState> { NodeState::make(life, machine, crate::settings::Values::now()) }

    fn make(life: Life, machine: crate::machine::Mach, settings: crate::settings::Values) -> Arc<NodeState> {
        Arc::new(NodeState { life, machine, #[cfg(feature = "lake")] net: crate::net::installed(), #[cfg(feature = "lake")] scratch: crate::scratch::installed(), settings, drain: Drain::default(), idle: Idle::default(), line: Default::default(), began: datafusion::common::instant::Instant::now() })
    }

    /// The state of a lake a test opens, with no node around it (`lifecycle::Unhosted`).
    pub fn unhosted() -> Arc<NodeState> { NodeState::new(crate::lifecycle::unhosted()) }

    /// A cold start's step and its time since this node's start began, with `PONDRA_TRACE_START` (`tools/cold_trace.sh`,
    /// C5). The catalog's open times its own steps (`store::trace_since`).
    pub fn trace(&self, what: &str) {
        if self.settings.is_set("PONDRA_TRACE_START") {
            eprintln!("start: {what} {:.2} s", self.began.elapsed().as_secs_f64());
        }
    }
}

/// The HTTP door's state, in order: not opened, serving, closing (its streams end and its answers go out), closed.
#[derive(Clone, Copy, Default, PartialEq, PartialOrd, Debug)]
pub enum Door {
    #[default]
    Unopened,
    Open,
    Closing,
    Closed,
}

/// A node's drain (`drain.rs`): whether it began, the requests in flight that came before it, the HTTP door's state,
/// when the drain began (its `PONDRA_DRAIN_SECS` are counted from it), and whether a second signal came.
pub struct Drain {
    draining: AtomicBool,
    // (each request holds its own node's count: one that outlives its node's stop counts down that node's)
    in_flight: Arc<AtomicUsize>,
    pub(crate) door: tokio::sync::watch::Sender<Door>,
    #[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the doors' drain and idle exit read it: a page has neither)
    pub(crate) started: Mutex<Option<tokio::time::Instant>>,
    // (whether the HTTP door took a connection: only then may a client still be reading an answer's last bytes)
    pub(crate) took: AtomicBool,
    pub(crate) hurried: AtomicBool,
    // (the doors besides HTTP's that close with it, Flight's, counted while each serves)
    pub(crate) others: Arc<AtomicUsize>,
}

impl Default for Drain {
    fn default() -> Drain {
        Drain { draining: AtomicBool::new(false), in_flight: Default::default(), door: tokio::sync::watch::channel(Door::Unopened).0, started: Mutex::new(None), took: AtomicBool::new(false), hurried: AtomicBool::new(false), others: Default::default() }
    }
}

impl Drain {
    /// Is this node stopping?
    pub fn draining(&self) -> bool { self.draining.load(SeqCst) }

    /// The drain begins (`drain::drain`): no request counted from here on.
    #[cfg_attr(not(feature = "doors"), allow(dead_code))]
    pub(crate) fn begin(&self) { self.draining.store(true, SeqCst) }

    /// The requests in flight now that came before the drain.
    #[cfg_attr(not(feature = "doors"), allow(dead_code))]
    pub(crate) fn in_flight(&self) -> usize { self.in_flight.load(SeqCst) }

    /// Held by a request's work while it runs (`panics::door`): counted if it came before the drain.
    pub fn busy(&self) -> Busy {
        // (counted, then the flag read: with `drain` storing the flag, then reading the count, one of
        // them sees the other, so no request that came before the drain goes uncounted)
        self.in_flight.fetch_add(1, SeqCst);
        if self.draining() {
            self.in_flight.fetch_sub(1, SeqCst);
            return Busy(None);
        }
        Busy(Some(self.in_flight.clone()))
    }

    /// The request's count, held again by the body of an answer sent in pieces, which goes on after its response: counted
    /// whatever the drain says, so it is taken only while the request's own `Busy` (`panics::door`) is held.
    pub fn carried(&self) -> Busy {
        self.in_flight.fetch_add(1, SeqCst);
        Busy(Some(self.in_flight.clone()))
    }

    /// A second signal while draining: a node stopping now doesn't wait for its door (`drain::close_door`).
    pub fn hurry(&self) { self.hurried.store(true, SeqCst) }

    /// The HTTP door is serving (its task, when it starts): `close_door` waits for it to close.
    pub fn door_open(&self) { self.door.send_replace(Door::Open); }

    /// The HTTP door took a connection (`tls::Doors`): its close waits the 250 ms that let an answer's last bytes out.
    pub fn took(&self) { self.took.store(true, SeqCst) }

    /// The HTTP door's serve has ended (its task, when it ends).
    pub fn door_closed(&self) { self.door.send_replace(Door::Closed); }

    /// Held by a door besides HTTP's while it serves (Flight's): `close_door` waits for it to be dropped, as it waits for
    /// the HTTP door to close. The door closes gracefully on `closing`, and its serve returns once its connections have
    /// closed: an answer whose last messages a slow client hasn't read yet is still in its connection when the answer's
    /// own `Busy` is dropped.
    pub fn serving(&self) -> Serving {
        self.others.fetch_add(1, SeqCst);
        Serving(self.others.clone())
    }

    /// Resolves once this node's door starts closing.
    pub fn closing(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut door = self.door.subscribe();
        async move { drop(door.wait_for(|d| *d >= Door::Closing).await) } // (its guard isn't Send: dropped here)
    }

    /// The stream, ended once the door starts closing. A stream that never ends by itself (a commit feed, a watch, a live
    /// query's lines) would hold a graceful close for its whole bound; its client reconnects, as it does when a node goes.
    pub fn endless<S: Stream + Send + 'static>(&self, s: S) -> impl Stream<Item = S::Item> + Send { s.take_until(self.closing()) }
}

/// A request's place in its node's count of requests in flight (`Drain::busy`): None for one that came once the drain
/// began, which a door then doesn't take (Kafka's: `kafka::conn`).
pub struct Busy(Option<Arc<AtomicUsize>>);

impl Busy {
    /// Whether it is counted.
    pub fn counted(&self) -> bool { self.0.is_some() }
}

impl Drop for Busy {
    fn drop(&mut self) {
        if let Some(count) = &self.0 {
            count.fetch_sub(1, SeqCst);
        }
    }
}

/// A door besides HTTP's serving (`Drain::serving`).
pub struct Serving(Arc<AtomicUsize>);

impl Drop for Serving {
    fn drop(&mut self) { self.0.fetch_sub(1, SeqCst); }
}

/// Requests a draining node still takes: the cluster's own (a leader's followers, a query's other
/// nodes) and the health checks.
pub fn still_taken(path: &str) -> bool { path.starts_with("/cluster/") || matches!(path, "/healthz" | "/ready" | "/stats" | "/metrics") }

/// What counts as a use of a node, for `--idle-exit` (`idle.rs` says what does): the uses open now, and when the last
/// one began or ended.
pub struct Idle {
    // (each use holds its own node's count, as `Busy` does)
    uses: Arc<Uses>,
    /// Whether the wait for scheduled tasks was said (once a node).
    #[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the doors' drain and idle exit read it: a page has neither)
    pub(crate) said: AtomicBool,
}

/// The uses open now, and when the last one began or ended: milliseconds since `born`, counted from 1 (0: none yet).
struct Uses {
    open: AtomicUsize,
    last: AtomicU64,
    born: datafusion::common::instant::Instant,
}

impl Uses {
    fn now(&self) -> u64 { self.born.elapsed().as_millis() as u64 + 1 }
}

impl Default for Idle {
    fn default() -> Idle {
        Idle { uses: Arc::new(Uses { open: AtomicUsize::new(0), last: AtomicU64::new(0), born: datafusion::common::instant::Instant::now() }), said: AtomicBool::new(false) }
    }
}

impl Idle {
    /// A use begins: held while it lasts.
    pub fn begin(&self) -> Use {
        self.uses.open.fetch_add(1, SeqCst);
        self.uses.last.store(self.uses.now(), SeqCst);
        Use(self.uses.clone())
    }

    /// The uses open now.
    #[cfg_attr(not(feature = "doors"), allow(dead_code))]
    pub(crate) fn using(&self) -> usize { self.uses.open.load(SeqCst) }

    /// Milliseconds since the node's state was made, counted from 1: what [`Idle::last`] counts in.
    #[cfg_attr(not(feature = "doors"), allow(dead_code))]
    pub(crate) fn now(&self) -> u64 { self.uses.now() }

    /// When the last use began or ended ([`Idle::now`]'s count); 0 until the first.
    #[cfg_attr(not(feature = "doors"), allow(dead_code))]
    pub(crate) fn last(&self) -> u64 { self.uses.last.load(SeqCst) }
}

/// Held while a use lasts (`Idle::begin`).
pub struct Use(Arc<Uses>);

impl Drop for Use {
    fn drop(&mut self) {
        self.0.last.store(self.0.now(), SeqCst);
        self.0.open.fetch_sub(1, SeqCst);
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
/// leader's commits, which a joined page's follow of it is, though: `server::feed`) and the health checks, which never
/// stop while a node runs.
pub fn background(path: &str) -> bool { matches!(path, "/cluster/beat" | "/cluster/log" | "/healthz" | "/ready" | "/stats" | "/metrics") }

#[cfg(test)]
mod tests {
    use super::*;

    /// Two nodes in one process drain apart (step 2's gate): one's drain refuses its own new requests and waits for its
    /// own in flight, and the other's go on as before.
    #[test]
    fn two_nodes_drain_apart() {
        let (a, b) = (NodeState::unhosted(), NodeState::unhosted());
        let (held, other) = (a.drain.busy(), b.drain.busy());
        a.drain.begin();
        assert!(a.drain.draining() && !b.drain.draining());
        assert!(!a.drain.busy().counted(), "a new request of the draining node is refused");
        assert!(b.drain.busy().counted(), "the other node takes its new requests");
        assert_eq!((a.drain.in_flight(), b.drain.in_flight()), (1, 1), "each counts its own");
        drop(held);
        assert_eq!(a.drain.in_flight(), 0);
        drop(other);
        let used = a.idle.begin();
        assert_eq!((a.idle.using(), b.idle.using()), (1, 0));
        drop(used);
        assert!(a.idle.last() > 0 && b.idle.last() == 0);
    }
}
