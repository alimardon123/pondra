//! A node: one lake served, from its start to its stop (ADR-061). The program (`cli.rs`) starts one
//! with its signals and standard input around it; a library will start and stop one in its own process.
//! A guest (`Options::guest`) is a node a program starts for its own statements: it answers them and ends.
//!
//! This file is the lake half: what a node is made of that a page has too (`stop`, `say`, the guest's and the moves'
//! state), and a host's state (`hosted`, `host`, the supervisor's events). The doors half is `node/run.rs`.
use std::sync::{Mutex, PoisonError, atomic::{AtomicBool, Ordering::SeqCst}};
use tokio::sync::mpsc;

#[cfg(feature = "doors")] mod run;
#[cfg(feature = "doors")] pub use run::{Node, Options, Unreached};
#[cfg(feature = "doors")] pub(crate) use run::{attach, request, Held};

/// What a guest is for. A guest that reads claims nothing; one that writes may record, so it leads a lake nobody leads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Guest {
    /// It only reads: it follows a leader that answers, or reads beside one that doesn't, or nobody's, and claims nothing.
    Reads,
    /// It may record writes, so it leads a lake nobody leads (`Cluster::guest`).
    Writes,
}

crate::per_node!(
    /// Is the node open in this process a guest? Set at its start, set back when it stops: `cluster::restart_with`
    /// keeps a guest's job, and a guest's lock file counts as gone at once.
    static GUEST: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false));

/// Is the node open in this process a guest (`Options::guest`)?
pub(crate) fn is_guest() -> bool { GUEST.load(std::sync::atomic::Ordering::SeqCst) }

/// Marks the node being started a guest or not (`Node::start`).
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (only the doors half starts a node)
pub(crate) fn set_guest(on: bool) { GUEST.store(on, std::sync::atomic::Ordering::SeqCst) }

/// The node stops, for good, with this reason, when its own work can't go on (a vital loop's panic, the catalog's
/// compactor, a lake a newer Pondra wrote). In a library the node stops and the host's process stays: `failed`
/// says why, and this returns. In the program the process ends with status 1, as a supervisor expects of a stop and
/// with no core: what it made goes first (an exit runs no destructor).
pub(crate) fn stop(why: String) {
    if hosted() {
        return failed(why);
    }
    eprintln!("stopping: {why}");
    crate::memory::leave();
    std::process::exit(1)
}

/// A line the node says on stderr when all goes well (a node is up, a lake's format moved on). A library's host and a
/// `pondra sql` guest's shell show only trouble, so there it is not said.
pub(crate) fn say(line: impl std::fmt::Display) {
    if !hosted() && !is_guest() {
        eprintln!("{line}");
    }
}

/// A lake moving to another place (`memory::set_location`): the host stops the node, settles the lake's
/// objects there (`memory::settle`) and starts the node again on `to`. `copied` is what the first pass copied.
pub struct Move {
    pub from: String,
    pub to: String,
    pub copied: crate::memory::Copied,
}

/// Where a lake's moves go: the host of the node open now, which took them with `mover`. Not `per_node!`:
/// a host outlives each node it starts (the program's, a library's supervisor's).
static MOVER: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<Move>>> = std::sync::Mutex::new(None);

/// The host's end of the moves: taken once, before it starts its node.
pub fn mover() -> tokio::sync::mpsc::UnboundedReceiver<Move> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    *MOVER.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(tx);
    rx
}

/// Can this node's lake be moved: a host took `mover` and is still there?
pub fn movable() -> bool { MOVER.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().is_some_and(|s| !s.is_closed()) }

/// Hands a move to the host, which answers for it once the node has begun to stop.
pub fn move_lake(m: Move) -> anyhow::Result<()> {
    let sent = MOVER.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().map(|s| s.send(m).is_ok()).unwrap_or(false);
    anyhow::ensure!(sent, "this node can't move its lake: it was started without a host that restarts it");
    Ok(())
}

/// Whether this process runs its node as a library: set by the first library's open (`host`), never by
/// the program. What the process is, so it outlives each node.
static HOSTED: AtomicBool = AtomicBool::new(false);

/// Where a node's restarts and failures are sent: the supervisor of the node open now, if any. One node is open at a
/// time in a process.
static SUPERVISOR: Mutex<Option<mpsc::UnboundedSender<Event>>> = Mutex::new(None);

/// Is this process a library's (`host`), not the program's?
pub fn hosted() -> bool { HOSTED.load(SeqCst) }

/// What a node's supervisor is told: a restart or a failure from the node's own work, or the close of the node
/// (a library's `Opened`).
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the library's supervisor is the doors half's)
pub(crate) enum Event {
    /// Halt the node and start it again (`cluster::restart_with`).
    Restart(String),
    /// Stop the node and say why until it is opened again.
    Failed(String),
    /// `Opened::close`.
    Close,
}

pub(crate) fn send(e: Event) {
    if let Some(s) = SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).as_ref() {
        let _ = s.send(e);
    }
}

/// In a library, the node stops with this reason (a vital loop's panic, the compactor, a lake that moved past this
/// build), and every request is answered with it until the node is opened again.
pub fn failed(why: String) {
    eprintln!("stopping: {why}");
    send(Event::Failed(why));
}

/// Puts a library's node under `tx`, the supervisor's end of its events, and says `hosted` from now on. Refused while a
/// node is open in this process.
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the library's open calls it)
pub(crate) fn host(tx: mpsc::UnboundedSender<Event>) -> anyhow::Result<()> {
    {
        let mut s = SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner);
        anyhow::ensure!(s.is_none(), "a node is open in this process already: close it first (one at a time)");
        *s = Some(tx);
    }
    HOSTED.store(true, SeqCst);
    Ok(())
}

/// Takes the supervisor back: the library's node is closed, or its open failed.
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the library's close calls it)
pub(crate) fn unhost() { SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).take(); }

/// Is a node open in this process as a library's? (`host` refuses a second one.)
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the library's open of a lake from JSON asks it first)
pub(crate) fn hosting() -> bool { SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).is_some() }
