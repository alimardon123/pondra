//! A node: one lake served, from its start to its stop (ADR-061). The program (`cli.rs`) starts one
//! with its signals and standard input around it; a library will start and stop one in its own process.
//! A guest (`Options::guest`) is a node a program starts for its own statements: it answers them and ends.
//!
//! This file is the lake half: what a node is made of that a page has too (`stop`, `say`, the guest's and the moves'
//! state), a host's state (`hosted`, `host`, the supervisor's events), and what a node does to start again
//! (`restart`, `restart_with`), the program's own path (`exe`) and the lead of a lake nobody leads (`lead_once`).
//! The doors half is `node/run.rs`.
use crate::{app, backup, branch, ddl, ext, feeds, format, learned, log, panics, python, replica, runs, spmd, store, terms, users, views};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError, atomic::{AtomicBool, Ordering::SeqCst}};
use std::time::Duration;
use tokio::sync::mpsc;
use crate::members::Members;

#[cfg(feature = "doors")] mod run;
#[cfg(feature = "doors")] pub use run::{Node, Options};
#[cfg(feature = "doors")] pub(crate) use run::{request, Held};

/// What a guest is for. A guest that reads claims nothing; one that writes may record, so it leads a lake nobody leads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Guest {
    /// It only reads: it follows a leader that answers, or reads beside one that doesn't, or nobody's, and claims nothing.
    Reads,
    /// It may record writes, so it leads a lake nobody leads (`Cluster::guest`).
    Writes,
}

crate::per_node!(
    /// Is the node open in this process a guest? Set at its start, set back when it stops: `restart_with`
    /// keeps a guest's job, and a guest's lock file counts as gone at once.
    static GUEST: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false));

/// Is the node open in this process a guest (`Options::guest`)?
pub(crate) fn is_guest() -> bool { GUEST.load(std::sync::atomic::Ordering::SeqCst) }

/// Marks the node being started a guest or not (`Node::start`).
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (only the doors half starts a node)
pub(crate) fn set_guest(on: bool) { GUEST.store(on, std::sync::atomic::Ordering::SeqCst) }

/// What a member's start takes besides its lake: `pondra serve`'s flags for the lake (the node's `Options` holds one),
/// a library's, a guest's.
#[derive(Clone)]
pub struct Start {
    pub(crate) reader: bool,
    pub(crate) flush_ms: u64,
    pub(crate) tier_secs: f64,
    pub(crate) task_ms: u64,
    pub(crate) retain_secs: u64,
    pub(crate) backlog: u64,
    pub(crate) python: Option<String>,
    pub(crate) auth: std::sync::Arc<crate::auth::Auth>,
}

impl Default for Start {
    /// `pondra serve`'s defaults: its flags read them (`cli.rs`).
    fn default() -> Start {
        Start { reader: false, flush_ms: 0, tier_secs: 10.0, task_ms: 1000, retain_secs: 60, backlog: 10_000_000, python: None, auth: Arc::new(crate::auth::Auth::new(None, None, None)) }
    }
}

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
    /// Halt the node and start it again (`restart_with`).
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

/// What a node restarting in place unwinds with ([`restart_with`]): `panics::spawn` lets it go, and
/// `panics::door` answers the request 57P03, as a node catching up does.
#[derive(Debug)]
pub struct Restarting(pub String);

impl std::fmt::Display for Restarting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the node is restarting to rejoin the cluster ({}), catching up with its leader: try again, or ask another node", self.0)
    }
}

/// The node must start again, to rejoin its cluster: `restart_with`, with nothing carried.
pub fn restart(why: &str) -> ! {
    restart_with(why, &[])
}

/// `restart`, with settings of the command line's own for the next run (`pondra sql`'s job and its
/// tries: `env.rs`), on top of what this process was given. A guest sent back five times gives up: its statement
/// fails, saying why. In a library the node is halted and started again in place: the supervisor is told, and the
/// task that asked unwinds, past the panic hook (a host's error reporting sees no panic for a takeover). In the
/// program the same binary runs again (`run::exec_again`). On a page there is no program to run again, so a restart
/// is a stop until the page decides otherwise (L2's answer in the design).
pub fn restart_with(why: &str, carried: &[(&str, String)]) -> ! {
    let guest = is_guest();
    if guest && crate::write::tries() >= 5 {
        eprintln!("gave up after 5 restarts: {why}"); // (a guest that is sent back five times is a statement that fails: it says why)
        crate::memory::leave();
        std::process::exit(1);
    }
    eprintln!("restarting to rejoin the cluster: {why}");
    // (our claims' lock files say we're coming back: an exec, or a new process on Windows, lets go of
    // them for a moment, and nobody here may take that for our end: `gone_here`. A guest's don't: its
    // lock is let go by the exec, and counts as gone at once, so the next writer doesn't wait on it)
    if !guest {
        if let Ok(held) = crate::terms::HELD.lock() {
            held.values().for_each(|(_, f)| crate::terms::say(f, &format!("restarting {}", crate::log::now_ms())));
        }
    }
    if hosted() {
        send(Event::Restart(why.to_string())); // (in place: a library's host keeps its process)
        std::panic::resume_unwind(Box::new(Restarting(why.to_string())))
    }
    #[cfg(feature = "doors")]
    run::exec_again(carried);
    #[cfg(not(feature = "doors"))]
    {
        stop(format!("{why}")); // (a page: a restart is a stop, until the page decides otherwise)
        unreachable!("stop ends an unhosted process")
    }
}

/// The program a node starts helpers with (`pondra lead`, Python workers looked for beside it): this
/// executable in the program; in a library, the one the packages ship beside it (`PONDRA_LAUNCHER`,
/// which `pondra.open` gives), since the host's executable is Python or Node.
pub fn exe() -> anyhow::Result<PathBuf> {
    if cfg!(target_arch = "wasm32") {
        anyhow::bail!("a page has no pondra program to start"); // (`std::env::current_exe` isn't there)
    }
    match hosted() {
        false => Ok(std::env::current_exe()?),
        true => crate::env::get("PONDRA_LAUNCHER").map(PathBuf::from).ok_or_else(|| {
            anyhow::anyhow!("this needs the pondra program, and the library was opened without it: give PONDRA_LAUNCHER, the path of the pondra binary (pondra.open gives the one its package ships)")
        }),
    }
}

/// Leads the lake at `dir` for a moment, making it if nothing is there yet, and answers its inbox, in a `pondra lead`
/// process of its own (`run::lead_once`). For a lake nobody leads, and for a writer whose leader is gone (`inbox::send`).
/// A node never leads another lake itself: that lake's catalog writer would outlive the moment in the node, and a
/// later leader of it would fence it out.
#[cfg(feature = "doors")]
pub async fn lead_once(dir: &str) -> anyhow::Result<()> {
    // (another process would lead a lake of its own memory, gone when it ends)
    anyhow::ensure!(!crate::memory::in_memory(dir), "{dir} is in a process's memory, where a lake is made by opening it (pondra.open(), pondra :memory:) and led by that process alone: attach or make a database in a folder or a bucket");
    run::lead_once(dir).await
}

/// A page starts no process, so it leads no lake but its own.
#[cfg(not(feature = "doors"))]
pub async fn lead_once(_dir: &str) -> anyhow::Result<()> {
    Err(crate::codes::coded("0A000", "a page leads no lake: a browser starts no processes (a node leads one in a process of its own)"))
}

/// The member's App over its opened lake (`Node::start`, and a page's open): its SQL attachments synced, the
/// followers' changes a leader inherits recovered, its sequencer and log started, its first user made. `replica` is
/// what this follower holds for its leader (a node's folder; None elsewhere).
pub async fn start_app(lake: Arc<crate::store::Lake>, members: Arc<dyn crate::members::Members>, replica: Option<Arc<crate::replica::ReplicaLog>>, s: &Start) -> anyhow::Result<crate::app::App> {
    let lives = !is_guest();
    let leader = members.is_leader();
    let term = members.leader().n;
    let reader = s.reader || members.reader();
    let (addr, dir) = (members.me(), lake.url.as_str()); // (what the first user's message names: this member, and its lake)
    let l = lake.clone();
    ddl::sync(&l, lives).await?;
    crate::store::trace("attachments synced");
    if lives {
        every(Duration::from_secs(1), move || { let l = l.clone(); async move { ddl::sync(&l, true).await } });
        let l = lake.clone();
        crate::panics::task(async move { l.warm().await.map_err(|e| eprintln!("warming the SSD tier: {e:#}")) });
    }
    // Followers keep the leader's changes that aren't in the bucket yet (replicated acks);
    // a new leader first commits what they hold from the previous one.
    crate::store::trace("warming");
    if let (true, Some(own)) = (leader, &replica) {
        // (the members are asked with the nodes' key: one that just restarted holds an
        // unsigned call until it has caught up, and it catches up from this leader)
        if crate::dial::node_key().is_none() {
            if let Some(k) = users::node_key_made(&lake).await {
                crate::dial::set_node_key(k);
            }
        }
        match replica::recover(&lake, &addr, term, Some(own)).await {
            // (a node outside the members that can't reach them: it let its term go, and they lead; it follows them)
            Err(e) if e.downcast_ref::<replica::Unreached>().is_some() => {
                eprintln!("{e:#}");
                crate::terms::newer_claimed(&lake.store, term).await;
                restart("not leading past members this node can't reach")
            }
            r => r?,
        };
    } else if leader && !lives {
        // (a guest that leads recovers as `write::lead` does: no followers' changes to hold, and no key to ask for)
        replica::recover(&lake, "", term, None).await?;
    }
    crate::store::trace("followers' changes recovered");
    let lock: Arc<tokio::sync::Mutex<()>> = Default::default(); // (the lake's: everything that rewrites table metadata)
    let pace = (s.tier_secs > 0.0).then(|| log::Pace { rows: s.backlog, lock: lock.clone() }); // rows waiting to be tiered
    let seq = if leader { Some(log::Sequencer::start(lake.clone(), pace).await?) } else { None };
    let to = match &seq {
        Some(s) => log::To::Local(s.clone()),
        None => log::To::Leader(members.leader().addr.clone()),
    };
    let _ = lake.to.set(to.clone()); // (a follower's sequences' values: the leader's sequencer)
    let log = (!reader).then(|| Arc::new(log::Log::start(lake.clone(), Duration::from_millis(s.flush_ms), to)));
    crate::store::trace("the sequencer");
    python::init(s.python.clone());
    let app = app::App { lake: lake.clone(), members: members.clone(), log, seq, lock, retain_ms: s.retain_secs * 1000, results: Default::default(), replica: replica.clone(), auth: s.auth.clone() };
    crate::constraints::share_lock(&app.lake, app.lock.clone()); // a checked append of this lake takes this lock (`constraints::leader`); the Log is reachable only through the App
    learned::start(&lake); // (the history this node's planner learns from: nothing is read until a query is planned)
    // (a lake served with tokens is not one a node may make a first user of: an accidental node with none must
    // not write to it, and is refused by `server.rs` instead)
    if leader && app.auth.network() && !app.auth.on() && !users::signed(&app.lake).await {
        // (on the network with no token nobody could sign in: the lake's first user is made before
        // anyone is served, and its token said once. A failure stops the node; a restart tries again.)
        let _guard = app.lock.lock().await;
        match users::first_user(&app.lake).await {
            Ok(Some(token)) => eprintln!(
                "This lake now asks everyone to sign in: it is served on {addr} with no token.\n\
                 Its first user is pondra, a superuser. Its token (shown once, kept only as a hash):\n  {token}\n\
                 The console: http://{addr}/ (Sign in, with the token). Postgres: user pondra, the token as its password.\n\
                 Another token: pondra token --dir {dir}. Or set --admin-token on every node."),
            Ok(None) => {}
            Err(e) => {
                terms::release(&lake.store, term).await; // (another node may lead it: not one with no first user)
                return Err(e.context("a node on the network with no token makes the lake's first user before it serves, and that failed: set --admin-token, or listen on 127.0.0.1"));
            }
        }
    }
    Ok(app)
}

/// The member's own loops over its App, each as a node that lives keeps it (none for a guest): the leader's keys,
/// tasks, tiering, checkpoints and emission, a follower's catalog refresh, streaming tasks, the store's upkeep.
pub fn keep(app: &crate::app::App, s: &Start) {
    let lives = !is_guest();
    let leader = app.seq.is_some();
    let reader = s.reader || app.members.reader();
    let lake = app.lake.clone();
    if leader && lives {
        let (l, c, tokens) = (app.lake.clone(), app.members.clone(), app.auth.on()); // (beside serving, not before it: C5)
        panics::spawn(async move {
            let made = users::kept(&l).await == "none";
            match users::make_keys(&l).await {
                Err(e) => eprintln!("the lake's keys (sessions', the nodes'): {e:#}"), // (sessions' signing key, and the nodes' own: `users.rs`)
                // (into every node's view at once: a node restarted before the next checkpoint
                // couldn't check the nodes' key, and held its new leader's calls 10 s)
                Ok(()) if made => l.cat.checkpoint().await.unwrap_or_else(|e| eprintln!("checkpoint: {e:#}")),
                Ok(()) => {}
            }
            if backup::frozen(&l) {
                return; // (a backup's secrets and keys stay as its runs wrote them: ADR-070)
            }
            if tokens {
                // (once, and for good: a node started by hand without them is then never open: `users::signed`)
                if let Err(e) = users::mark_signed(&l).await {
                    eprintln!("the lake's tokens aren't remembered: {e:#}");
                }
            }
            match ext::rewrap(&l).await {
                Ok(0) => {}
                Ok(n) => eprintln!("rewrapped {n} secret{} with the master key in use now", if n == 1 { "" } else { "s" }),
                Err(e) => eprintln!("secrets not rewrapped: {e:#}"),
            }
            // (sealed once every node can read them sealed: tried each minute until then)
            let mut said = false;
            while let Err(e) = async { users::seal_keys(&l).await?; branch::seal_keys(&l).await }.await {
                if !c.is_leader() {
                    break;
                }
                if !std::mem::replace(&mut said, true) {
                    eprintln!("the lake's keys stay as they are for now: {e:#}");
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
        let (l, c) = (app.lake.clone(), app.members.clone());
        panics::spawn(async move {
            // (a branch's pins in bases on other servers, renewed while it is used here: `branch::lapse`)
            while c.is_leader() {
                branch::renew(&l).await;
                tokio::time::sleep(branch::renew_every()).await;
            }
        });
    }
    if lives {
        let l = app.lake.clone();
        panics::spawn(async move {
            // (a follower's catalog shows the leader's keys once the leader has flushed them)
            let mut said = false;
            for _ in 0..600 {
                match users::node_key(&l).await {
                    Ok(k) => {
                        crate::dial::set_node_key(k); // (nodes call each other with it when no admin token is set: cluster::http)
                        return;
                    }
                    Err(e) if !said && format!("{e:#}").contains("master key") => {
                        said = true; // (sealed by a master key this node doesn't have: said at once, since waiting won't help)
                        eprintln!("{e:#}");
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            eprintln!("the lake's key for its nodes isn't readable here: with users and no --admin-token, this node can't call the others");
        });
    }
    if app.members.is_leader() && !app.members.reader() && lives {
        runs::schedule(app.clone()); // (tasks: the leader runs their ticks)
    }
    if app.members.is_leader() && !app.members.reader() && lives && !backup::frozen(&lake) {
        // (a lake opened somewhere it didn't publish from — moved by SET LOCATION, or copied — publishes its
        // Iceberg tables here at once: `iceberg::Published::location`. Only then: the pass loads every
        // published table's manifests, and a lake that stayed where it was has nothing new for it)
        let (a, l) = (app.clone(), lake.clone());
        panics::task(async move {
            if !crate::iceberg::elsewhere(&l).await {
                return;
            }
            let _lock = a.lock.lock().await; // (the lock tiering publishes under: `app::App::tier_all`)
            if let Err(e) = crate::delta::publish_all(&l).await {
                eprintln!("publishing at start: {e:#}");
            }
        });
    }
    if lives && app.log.is_some() {
        feeds::start(app.clone()); // (other clusters' topics feeding views: none, no work)
    }
    if let (true, Some(seq), Some(log)) = (lives, &app.seq, &app.log) {
        let (lake, log, seq, lock) = (lake.clone(), log.clone(), seq.clone(), app.lock.clone()); // windows, sessions and matches past the watermark, emitted once
        panics::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if let Err(e) = views::emit_all(&lake, &log, &seq, &lock).await {
                    eprintln!("emission: {e:#}");
                }
            }
        });
    }
    // Finished shuffles are forgotten here, and what they spilled deleted.
    if lives {
        every(Duration::from_secs(30), || async { spmd::gc(); Ok(()) });
    }
    if leader && lives {
        // The leader's SSD tier learns of objects other nodes wrote from its own commits (and its hot
        // columns of the files they replaced).
        let l = lake.clone();
        panics::spawn(async move {
            let (_, mut commits) = l.cat.subscribe();
            loop {
                match commits.recv().await {
                    Ok(store::Frame::Change(d)) => l.arrived(&d),
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue, // (just fewer prefetches)
                    Err(_) => break,
                }
            }
        });
        // Tier as soon as new rows commit — other engines see them in the Delta log a moment
        // later — but at most once every `tier_secs`, and at least every 5 × that (bulk
        // inserts, compaction, retention). Only a loud commit wakes it (`store::quiet`): the
        // statements' history commits every second while anyone queries, and the round each
        // one woke set a pace that held the lake's next write back up to `tier_secs`
        // (freshness.py: Delta and Iceberg 1 s after the ack instead of 30 ms). So did the
        // round's own commit, so what it waits past is the version the round left.
        if s.tier_secs > 0.0 {
            let (a, mut hwm, period) = (app.clone(), lake.hwm.subscribe(), Duration::from_secs_f64(s.tier_secs));
            panics::spawn(async move {
                loop {
                    let (start, before) = (tokio::time::Instant::now(), *hwm.borrow_and_update());
                    let failed = a.tier_all(0).await.inspect_err(|e| eprintln!("background job failed: {e:#}")).is_err();
                    // (rows that came while it ran may be loud ones it didn't take: they go at
                    // the round's pace, as any commit did)
                    let seen = (*hwm.borrow() == before).then(|| a.lake.cat.version()).flatten();
                    tokio::time::sleep_until(start + period).await;
                    // (right away if rows came meanwhile; a round that failed, say on a full
                    // disk, is tried again then too, not only once more rows come)
                    if !failed {
                        let loud = async { while hwm.changed().await.is_ok() && seen.is_some() && a.lake.cat.version() == seen {} };
                        let _ = tokio::time::timeout(period * 4, loud).await;
                    }
                }
            });
        }
        // …and any table as soon as a million rows wait in the log (bounds memory and read cost).
        let a = app.clone();
        every(if s.tier_secs == 0.0 { Duration::ZERO } else { Duration::from_secs(1) }, move || { let a = a.clone(); async move { a.tier_all(1_000_000).await.map(|_| ()) } });
        // Persist the catalog's memtable every few seconds when something changed, so a
        // restart (or a new leader, reader or `pondra sql`) replays only a few seconds of
        // catalog WAL. Not more often: each is a level-0 file that SlateDB's compactor
        // has to keep up with, and writes stall when too many pile up.
        let l = lake.clone();
        every(Duration::from_secs(5), move || { let l = l.clone(); async move { l.cat.checkpoint().await } });
        if lake.cat.replicas > 1 {
            replica::members(lake.clone(), app.members.clone());
        }
        format::raise(lake.clone(), app.members.clone()); // (once every node knows this build's format: ADR-039)
    }
    if !leader {
        if lives {
            format::watch(lake.clone());
        }
        // How far our own catalog view is: all a reader has, and a follower's fallback.
        if lives {
            let l = lake.clone();
            every(Duration::from_millis(250), move || { let l = l.clone(); async move { l.refresh().await } });
        }
    }
    if !reader && lives {
        let (a, max_wait) = (app.clone(), Duration::from_millis(s.task_ms));
        panics::spawn(async move {
            let mut commits = a.lake.hwm.subscribe();
            loop {
                let _ = tokio::time::timeout(max_wait, commits.changed()).await; // new rows, or time's up
                if let Err(e) = a.run_tasks().await {
                    eprintln!("streaming tasks failed: {e:#}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        });
    }
    if lives {
        store::serving(); // (the catalog's compactor and garbage collector start now: C5)
    }
    crate::oom::start(); // (the memory line: a query that grows the node past it is stopped, not the node: `oom.rs`)
    if lives {
        runs::mark_stopped(app.clone()); // (runs whose node stopped under them: `stopped`)
        if leader {
            let l = app.lake.clone(); // (what it inherited, replayed from the WAL, into every node's view)
            crate::panics::task(async move { l.cat.checkpoint().await.map_err(|e| eprintln!("checkpoint: {e:#}")) });
        }
    }
}

crate::per_node!(
    /// Is a page's member open in this process (`open_page`)? Set when one opens, set back when it closes.
    static PAGED: AtomicBool = AtomicBool::new(false));

/// A page's member (designs/page-lake-p2.md): a lake in the page's memory, led by the page alone (`members::Alone`),
/// started as every member is (`start_app`, `keep`). One at a time in a process, as a node is: `close` ends it.
pub struct Paged { pub app: crate::app::App, url: String, term: u64, marks: tokio::task::JoinHandle<()> }

/// Opens a page's member (`Paged`), its queries holding the page's share of its device's `line` (`env::device_line`):
/// refused by name while one is open in this process, and set back if the open fails.
pub async fn open_page(line: Option<usize>) -> anyhow::Result<Paged> {
    if PAGED.swap(true, SeqCst) {
        return Err(crate::codes::coded("55006", "a page on the lake is open in this module already: close it first"));
    }
    crate::env::set_device_line(line); // (its lake's queries hold the page's share of it: `store::memory_limit`)
    let url = crate::memory::place(":memory:");
    let store = crate::memory::store(&url);
    let (alone, marks) = match crate::members::Alone::lead(&store).await {
        Ok(led) => led,
        Err(e) => {
            crate::memory::forget_memory(&url);
            PAGED.store(false, SeqCst);
            return Err(e);
        }
    };
    let term = alone.leader().n;
    let start = Start::default();
    let mut lake = None;
    let made = async {
        let l = crate::store::Lake::open(&url, true, false).await?;
        lake = Some(l.clone());
        start_app(l, alone.clone(), None, &start).await
    }
    .await;
    match made {
        Ok(app) => {
            keep(&app, &start);
            Ok(Paged { app, url, term, marks })
        }
        Err(e) => {
            end_page(marks, lake, &store, term, &url).await; // (a failed open leaves nothing running)
            Err(e)
        }
    }
}

/// Ends what `open_page` started, as a node's stop does (`node::run::Held::stop` and `Node::stop`): its term's marks, its
/// loops, its catalog, its term, its lake in memory, and the module's per-node state, so another `open_page` starts
/// afresh. `Paged::close` and a failed open both end this way.
async fn end_page(marks: tokio::task::JoinHandle<()>, lake: Option<Arc<crate::store::Lake>>, store: &crate::store::Store, term: u64, url: &str) {
    marks.abort(); // (a dropped handle aborts nothing: its mark would land again after the release)
    crate::panics::abort_all();
    if let Some(lake) = lake {
        let _ = lake.cat.close().await;
    }
    crate::terms::release(store, term).await;
    crate::memory::forget_memory(url);
    crate::reset::reset();
    PAGED.store(false, SeqCst);
}

impl Paged {
    /// Ends the page's member (`end_page`), so another `open_page` starts afresh.
    pub async fn close(self) {
        let Paged { app, url, term, marks } = self;
        let lake = app.lake.clone();
        drop(app);
        end_page(marks, Some(lake.clone()), &lake.store, term, &url).await;
    }
}

/// Run `job` forever, starting every `period` (right away if a run took longer; a zero period
/// disables it).
fn every<F, Fut>(period: Duration, job: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send,
{
    if period.is_zero() {
        return;
    }
    panics::spawn(async move {
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            if let Err(e) = job().await {
                eprintln!("background job failed: {e:#}");
            }
        }
    });
}
