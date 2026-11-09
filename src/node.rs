//! A node: one lake served, from its start to its stop (ADR-061). The program (`cli.rs`) starts one
//! with its signals and standard input around it; a library will start and stop one in its own process.
//! A guest (`Options::guest`) is a node a program starts for its own statements: it answers them and ends.
//!
//! This file is the lake half: what a node is made of that a page has too (`stop`, the moves' state), a host's state
//! (`hosted`, `host`, the supervisor's events), and what a node does when it must stop or start again (`Native`, the
//! `Lifecycle` every lake it opens is handed), the program's own path (`exe`) and the lead of a lake nobody leads
//! (`lead_once`).
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

pub use crate::lifecycle::{Guest, Restarting};

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
/// compactor, a lake a newer Pondra wrote). Where a host supervises the node, it is told (`failed`) and this returns:
/// it stops the node as it stops it on a close, so a leader makes what it committed durable, checkpoints its catalog
/// and gives up its term, and the next node leads at once (invariant 210). A library's host then stays; `pondra serve`
/// then ends with status 1 (`cli.rs`). Elsewhere (a guest, a command of the command line's) the process ends here with
/// status 1, as a supervisor expects of a stop and with no core: what it made goes first (an exit runs no destructor).
pub(crate) fn stop(why: String) {
    if hosted() || supervised() {
        return failed(why);
    }
    eprintln!("stopping: {why}");
    crate::memory::leave();
    std::process::exit(1)
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

/// Where a node's restarts and failures are sent: the supervisor of the node open now, if any (a library's, `host`, or
/// `pondra serve`'s, `supervise`). One node is open at a time in a process.
static SUPERVISOR: Mutex<Option<mpsc::UnboundedSender<Event>>> = Mutex::new(None);

/// Is this process a library's (`host`), not the program's?
pub fn hosted() -> bool { crate::machine::hosted() }

/// What a node's supervisor is told: a restart or a failure from the node's own work, or the close of the node
/// (a library's `Opened`).
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the library's supervisor is the doors half's)
pub(crate) enum Event {
    /// Halt the node and start it again (`Lifecycle::restart`).
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

/// The node's supervisor is told it stops with this reason (a vital loop's panic, the compactor, a lake that moved past
/// this build). A library's then answers every request with it until the node is opened again; `pondra serve`'s ends.
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
    crate::machine::set_hosted(); // (what the process is, from its first library's open: it outlives each node)
    Ok(())
}

/// `pondra serve`'s end of its nodes' failures (`stop`), taken once, before it starts its first node. Not `host`: the
/// process is still the program's (`hosted` stays false), so a restart still runs the program again.
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the command line's serve takes it)
pub(crate) fn supervise() -> mpsc::UnboundedReceiver<Event> {
    let (tx, rx) = mpsc::unbounded_channel();
    *SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner) = Some(tx);
    rx
}

/// Is a supervisor told of this process's node's failures (`host`, `supervise`)?
fn supervised() -> bool { SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).as_ref().is_some_and(|s| !s.is_closed()) }

/// Takes the supervisor back: the library's node is closed, or its open failed.
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the library's close calls it)
pub(crate) fn unhost() { SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).take(); }

/// Is a node open in this process as a library's? (`host` refuses a second one.)
#[cfg_attr(not(feature = "doors"), allow(dead_code))] // (the library's open of a lake from JSON asks it first)
pub(crate) fn hosting() -> bool { SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).is_some() }

/// The lifecycle of a node in this process (`lifecycle::Lifecycle`): the program's, a library's (`hosted`) or a page's.
/// A guest's (`Options::guest`) ends with its statement. Every lake the node opens, and its member, is handed it.
pub struct Native {
    guest: bool,
    settings: crate::settings::Values, // (the node's, which the processes it starts are handed: `lead_elsewhere`)
}

/// A node's state as the code below the hosts sees it, with this host's lifecycle: a guest's when `guest`.
pub fn native(guest: bool) -> Arc<crate::node_state::NodeState> { native_with(guest, crate::settings::Values::now()) }

/// `native`, the node starting with `settings` (`Options::settings`).
pub(crate) fn native_with(guest: bool, settings: crate::settings::Values) -> Arc<crate::node_state::NodeState> {
    crate::node_state::NodeState::starting(Arc::new(Native { guest, settings: settings.clone() }), settings)
}

impl crate::lifecycle::Lifecycle for Native {
    fn stop(&self, why: String) { stop(why) }
    fn restart(&self, why: &str, carried: &[(&str, String)]) -> ! { restart_with(self.guest, why, carried) }
    /// (a library's host and a `pondra sql` guest's shell show only trouble, so there it is not said)
    fn say(&self, line: &str) {
        if !hosted() && !self.guest {
            eprintln!("{line}");
        }
    }
    fn one_off(&self) -> bool { self.guest }
    fn owns_process(&self) -> bool { !hosted() }
    fn program(&self) -> anyhow::Result<PathBuf> { exe() }
    fn lead_elsewhere<'a>(&'a self, dir: &'a str) -> futures::future::BoxFuture<'a, anyhow::Result<()>> { Box::pin(lead_once(dir, &self.settings)) }
}

/// A page's lifecycle (redesign §8): what ends is the page's member, never its worker. A stop ends the member, which
/// answers every request with why from then on (`Paged::stopped`, 57P01), until the page closes and the module's page is
/// claimed again. A restart is a stop said as one (57P03), and then the task that asked unwinds with [`Restarting`]. A page
/// can't unwind (the page's std is built to abort), so there the module traps, and the program that runs the page opens
/// it again: a kept page's tables are in its folder, and an in-memory page's are gone.
pub struct PageLife {
    stopped: Mutex<Option<(&'static str, String)>>, // (the SQLSTATE and the words every request answers once the member ended)
    say: Option<fn(&str)>,
}

impl PageLife {
    /// A page's lifecycle, saying its lines with `say` (`page::Options::say`), or nowhere.
    pub fn new(say: Option<fn(&str)>) -> Arc<PageLife> { Arc::new(PageLife { stopped: Mutex::new(None), say }) }

    /// The SQLSTATE and the words of how the member ended (`stop`, `restart`), or None while it lives.
    pub fn stopped(&self) -> Option<(&'static str, String)> { self.stopped.lock().unwrap_or_else(PoisonError::into_inner).clone() }

    fn end(&self, code: &'static str, words: String) {
        if let Some(say) = self.say {
            say(&words)
        }
        self.stopped.lock().unwrap_or_else(PoisonError::into_inner).get_or_insert((code, words)); // (the first end is the one said)
    }
}

impl crate::lifecycle::Lifecycle for PageLife {
    fn stop(&self, why: String) { self.end("57P01", format!("this page's lake stopped ({why}): close the page and open it again")) }
    fn restart(&self, why: &str, _: &[(&str, String)]) -> ! {
        self.end("57P03", Restarting(why.to_string()).to_string());
        std::panic::resume_unwind(Box::new(Restarting(why.to_string())))
    }
    fn say(&self, line: &str) {
        if let Some(say) = self.say {
            say(line)
        }
    }
    fn one_off(&self) -> bool { false }
    fn owns_process(&self) -> bool { true } // (the page's worker is its own: no host shares it)
    fn program(&self) -> anyhow::Result<PathBuf> { anyhow::bail!("a page has no pondra program to start") }
    fn lead_elsewhere<'a>(&'a self, dir: &'a str) -> futures::future::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move { anyhow::bail!("a page leads its own lake only, not {dir}") })
    }
}

/// The node starts again to rejoin its cluster (`Lifecycle::restart`), with settings of the command line's own for the
/// next run (`pondra sql`'s job and its tries: `env.rs`), on top of what this process was given. A guest sent back five times gives up: its statement
/// fails, saying why. In a library the node is halted and started again in place: the supervisor is told, and the
/// task that asked unwinds, past the panic hook (a host's error reporting sees no panic for a takeover). In the
/// program the same binary runs again (`run::exec_again`). On a page there is no program to run again, so a restart
/// is a stop until the page decides otherwise (L2's answer in the design).
fn restart_with(guest: bool, why: &str, carried: &[(&str, String)]) -> ! {
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
    run::exec_again(guest, carried);
    #[cfg(not(feature = "doors"))]
    {
        let _ = carried; // (a page carries nothing: nothing runs again)
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
pub async fn lead_once(dir: &str, values: &crate::settings::Values) -> anyhow::Result<()> {
    // (another process would lead a lake of its own memory, gone when it ends)
    anyhow::ensure!(!crate::memory_lake::in_memory(dir), "{dir} is in a process's memory, where a lake is made by opening it (pondra.open(), pondra :memory:) and led by that process alone: attach or make a database in a folder or a bucket");
    run::lead_once(dir, values).await
}

/// A page starts no process, so it leads no lake but its own.
#[cfg(not(feature = "doors"))]
pub async fn lead_once(_dir: &str, _values: &crate::settings::Values) -> anyhow::Result<()> {
    Err(crate::codes::coded("0A000", "a page leads no lake: a browser starts no processes (a node leads one in a process of its own)"))
}

/// The member's App over its opened lake (`Node::start`, and a page's open): its SQL attachments synced, the
/// followers' changes a leader inherits recovered, its sequencer and log started, its first user made. `replica` is
/// what this follower holds for its leader (a node's folder; None elsewhere).
pub async fn start_app(lake: Arc<crate::store::Lake>, members: Arc<dyn crate::members::Members>, replica: Option<Arc<crate::replica::ReplicaLog>>, s: &Start) -> anyhow::Result<crate::app::App> {
    if let Some(key) = s.auth.owner_key() {
        let _ = lake.owner_key.set(key.to_string()); // (whose tasks this process runs with its owner's files: `runs::signed`)
    }
    let lives = !lake.node.life.one_off();
    let leader = members.is_leader();
    let term = members.leader().n;
    let reader = s.reader || members.reader();
    let (addr, dir) = (members.me(), lake.url.as_str()); // (what the first user's message names: this member, and its lake)
    let l = lake.clone();
    ddl::sync(&l, lives).await?;
    lake.node.trace("attachments synced");
    if lives {
        every(Duration::from_secs(1), move || { let l = l.clone(); async move { ddl::sync(&l, true).await } });
        let l = lake.clone();
        crate::panics::task(async move { l.warm().await.map_err(|e| eprintln!("warming the SSD tier: {e:#}")) });
    }
    // Followers keep the leader's changes that aren't in the bucket yet (replicated acks);
    // a new leader first commits what they hold from the previous one.
    lake.node.trace("warming");
    let recovered = match (leader, &replica) {
        (true, Some(own)) => {
            // (the members are asked with the nodes' key: one that just restarted holds an
            // unsigned call until it has caught up, and it catches up from this leader)
            if crate::dial::node_key().is_none() {
                if let Some(k) = users::node_key_made(&lake).await {
                    crate::dial::set_node_key(k);
                }
            }
            Some(replica::recover(&lake, &addr, term, Some(own)).await)
        }
        // (a guest that leads recovers as `write::lead` does: no followers' changes to hold, and no key to ask for)
        (true, None) if !lives => Some(replica::recover(&lake, "", term, None).await),
        _ => None,
    };
    match recovered {
        // (a node outside the members that can't reach them: it let its term go, and they lead; it follows them, and a
        // guest starts over through them, as `write::deliver` does: audit 10-31 L11)
        Some(Err(e)) if e.downcast_ref::<replica::Unreached>().is_some() => {
            eprintln!("{e:#}");
            crate::terms::newer_claimed(&lake.store, term).await;
            lake.node.life.restart("not leading past members this node can't reach", &[])
        }
        Some(r) => {
            r?;
        }
        None => {}
    }
    lake.node.trace("followers' changes recovered");
    let lock: Arc<tokio::sync::Mutex<()>> = Default::default(); // (the lake's: everything that rewrites table metadata)
    let pace = (s.tier_secs > 0.0).then(|| log::Pace { rows: s.backlog, lock: lock.clone() }); // rows waiting to be tiered
    if leader && lives {
        lake.cat.keep_records().await?; // (its catalog hooks' records made whole, then kept by every write: a guest's statement reads past them)
    }
    let seq = if leader { Some(log::Sequencer::start(lake.clone(), pace).await?) } else { None };
    let to = match &seq {
        Some(s) => log::To::Local(s.clone()),
        None => log::To::Leader(members.leader().addr.clone()),
    };
    let _ = lake.to.set(to.clone()); // (a follower's sequences' values: the leader's sequencer)
    let log = (!reader).then(|| Arc::new(log::Log::start(lake.clone(), Duration::from_millis(s.flush_ms), to)));
    lake.node.trace("the sequencer");
    python::init(s.python.clone());
    let app = app::App { lake: lake.clone(), members: members.clone(), log, seq, lock, retain_ms: s.retain_secs * 1000, results: Default::default(), replica: replica.clone(), auth: s.auth.clone(), tables: Default::default() };
    if let Some(ledger) = lake.ledger() {
        ledger.charge("answers", Arc::downgrade(&app.results) as std::sync::Weak<dyn crate::ledger::Charged>); // (the remembered answers: `/metrics`' `cache_bytes`)
        // (the node's caches of what the lake's own floor doesn't reach, charged once whatever lakes it opens: what lookups
        // keep (the statements worked out as point queries and the log tails' keys))
        for (name, cache) in [("point_plans", crate::serve::points()), ("tail_keys", crate::serve::tail_keys())] {
            ledger.charge(name, Arc::downgrade(&cache));
        }
        // (the lake's state its doors use, made as it starts, so `/metrics` names it before its first use: the answers kept
        // for their pages, its functions' answers, the logs and manifests of the Delta and Iceberg tables it reads, and its
        // queries' planning facts; each is charged to the lake's line as it is made, `Lake::state`)
        lake.state::<crate::pages::Pages>()?;
        lake.state::<crate::pyfn::Answered>()?;
        lake.state::<crate::read_delta::Logs>()?;
        lake.state::<crate::read_iceberg::Manifests>()?;
        lake.state::<crate::query::FactsKept>()?;
    }
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
/// tasks, tiering, checkpoints and emission, a follower's catalog refresh, streaming tasks, the store's upkeep. Each is
/// the node's own work, and says so where it starts (`own`).
pub fn keep(app: &crate::app::App, s: &Start) {
    let lives = !app.lake.node.life.one_off();
    let leader = app.seq.is_some();
    let reader = s.reader || app.members.reader();
    let lake = app.lake.clone();
    if leader && lives {
        let (l, c, tokens) = (app.lake.clone(), app.members.clone(), app.auth.on()); // (beside serving, not before it: C5)
        panics::own(async move {
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
        panics::own(async move {
            // (a branch's pins in bases on other servers, renewed while it is used here: `branch::lapse`)
            while c.is_leader() {
                branch::renew(&l).await;
                tokio::time::sleep(branch::renew_every()).await;
            }
        });
    }
    if lives {
        let l = app.lake.clone();
        let closing = app.lake.node.drain.closing();
        panics::own(async move {
            // (a follower's catalog shows the leader's keys once the leader has flushed them)
            let mut said = false;
            for _ in 0..600 {
                match users::node_key(&l).await {
                    Ok(k) => {
                        crate::dial::set_node_key(k); // (nodes call each other with it when no admin token is set: cluster::http)
                        // (and a new one when a leader seals keys that were in the clear: `users::seal_keys`)
                        tokio::pin!(closing);
                        loop {
                            tokio::select! {
                                () = &mut closing => return,
                                () = tokio::time::sleep(users::NODE_KEY_LOOK) => {}
                            }
                            if let Some(k) = users::sealed_node_key(&l).await.filter(|k| crate::dial::node_key().as_deref() != Some(k)) {
                                crate::dial::set_node_key(k);
                                eprintln!("this node took the lake's new keys from the bucket");
                            }
                        }
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
        panics::task(crate::auth::as_node(async move {
            if !crate::iceberg::elsewhere(&l).await {
                return;
            }
            let _lock = a.lock.lock().await; // (the lock tiering publishes under: `app::App::tier_all`)
            if let Err(e) = crate::delta::publish_all(&l).await {
                eprintln!("publishing at start: {e:#}");
            }
        }));
    }
    if lives && app.log.is_some() {
        feeds::start(app.clone()); // (other clusters' topics feeding views: none, no work)
    }
    if let (true, Some(seq), Some(log)) = (lives, &app.seq, &app.log) {
        let (lake, log, seq, lock) = (lake.clone(), log.clone(), seq.clone(), app.lock.clone()); // windows, sessions and matches past the watermark, emitted once
        panics::own(async move {
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
        panics::own(async move {
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
            panics::own(async move {
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
        panics::own(async move {
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

/// Is a page's member open in this process (`open_page`)? Set when one opens, and set back by `end_page` as its last step,
/// after `reset()`: a waiting `open_page` takes it only once every other static is set back. Not `per_node!`, since `reset()`
/// would set it back before the rest are, and a page opened in that gap would have them cleared under it.
static PAGED: AtomicBool = AtomicBool::new(false);

/// Is a live `page::Page` holding this module's page (`claim_page`)? Set by the claim, set back by `unclaim_page`. Not
/// `per_node!`: a freed page's member closes in the background, and its `reset()` would set back the claim of the page made
/// after it.
static CLAIMED: AtomicBool = AtomicBool::new(false);

/// Claims this module's page for a `page::Page` (`Page::claim`), refused 55006 while a live page holds it.
pub fn claim_page() -> anyhow::Result<()> {
    if CLAIMED.swap(true, SeqCst) {
        return Err(crate::codes::coded("55006", "a page is open in this module already: close it (close() or free()) first, or open the other in a Worker"));
    }
    Ok(())
}

/// Gives the module's page back (`claim_page`): its page is closed, or dropped.
pub fn unclaim_page() {
    CLAIMED.store(false, SeqCst);
}

/// A page's member (designs/page-lake-p2.md): a lake in the page's memory, led by the page alone (`members::Alone`),
/// started as every member is (`start_app`, `keep`). One at a time in a process, as a node is: `close` ends it. `term` is the
/// term it leads, 0 for a joined page, which claims none (`open_joined`); `marks` keeps that term alive (a finished task for one).
/// `ended` is the sign-in a joined page follows its node with has ended (`ended`); an own page's never does. `follow` is the
/// task that follows a joined page's node's stream (`open_joined`), aborted at the page's end (`end_page`).
pub struct Paged { pub app: crate::app::App, url: String, term: u64, marks: tokio::task::JoinHandle<()>, follow: Option<tokio::task::JoinHandle<()>>, ended: Arc<AtomicBool>, superuser: AtomicBool, pub(crate) life: Arc<PageLife> }

/// Takes the module's page (`PAGED`) for a page's member. A freed page's member may still be closing in the background,
/// which sets PAGED back when it has ended: then this waits for it, 10 ms at a time, for 30 s at most, and is refused 55006
/// after that.
async fn claim_paged() -> anyhow::Result<()> {
    // (`swap` checks and sets at once, so two opens can't both take it)
    let mut sleeps = 0;
    while PAGED.swap(true, SeqCst) {
        if sleeps == 3_000 {
            return Err(crate::codes::coded("55006", "the page freed before this one is still closing: reload, or open this one in a Worker"));
        }
        sleeps += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

/// Opens a page's member (`Paged`), its lake on `store` when one is given (a kept page's folder), else in memory, and its
/// queries holding the page's share of its device's line (`store::memory_limit`). Set back if the open fails.
pub async fn open_page(store: Option<crate::store::Store>, life: Arc<PageLife>) -> anyhow::Result<Paged> {
    claim_paged().await?;
    let url = crate::memory_lake::place(":memory:");
    let owned = store.is_some(); // (a given store is a kept page's folder, under its Web Lock: no other writer, so its term is taken at once)
    if let Some(given) = store {
        crate::memory_lake::give(&url, given); // (a kept page's objects stay in its folder when the page closes: `end_page` forgets the URL only)
    }
    let node = crate::node_state::NodeState::new(life.clone());
    let store = crate::store::counted(crate::memory_lake::store(&url), &node.metrics); // (its term's claims and marks the page's: `Counted`)
    let lead = if owned { crate::members::Alone::lead_owned(&store).await } else { crate::members::Alone::lead(&store).await };
    let (alone, marks) = match lead {
        Ok(led) => led,
        Err(e) => {
            crate::memory_lake::forget_memory(&url);
            PAGED.store(false, SeqCst);
            return Err(e);
        }
    };
    let term = alone.leader().n;
    let start = Start::default();
    let mut lake = None;
    let made = async {
        let l = crate::store::Lake::open(&url, true, false, node).await?;
        lake = Some(l.clone());
        start_app(l, alone.clone(), None, &start).await
    }
    .await;
    match made {
        Ok(app) => {
            keep(&app, &start);
            Ok(Paged { app, url, term, marks, follow: None, ended: Default::default(), superuser: AtomicBool::new(false), life })
        }
        Err(e) => {
            end_page(marks, None, lake, &store, term, &url).await; // (a failed open leaves nothing running)
            Err(e)
        }
    }
}

/// Ends what `open_page` started, as a node's stop does (`node::run::Held::stop` and `Node::stop`): its term's marks, its
/// loops, its catalog, its term, its lake in memory, and the module's per-node state, so another `open_page` starts
/// afresh. `Paged::close` and a failed open both end this way.
async fn end_page(marks: tokio::task::JoinHandle<()>, follow: Option<tokio::task::JoinHandle<()>>, lake: Option<Arc<crate::store::Lake>>, store: &crate::store::Store, term: u64, url: &str) {
    marks.abort(); // (a dropped handle aborts nothing: its mark would land again after the release)
    if let Some(follow) = follow {
        follow.abort(); // (the node's stream ends as the page closes, not at the node's next commit)
    }
    crate::panics::abort_all();
    if let Some(lake) = lake {
        let _ = lake.cat.close().await;
    }
    if term > 0 {
        crate::terms::release(store, term).await; // (a joined page claimed no term: `open_joined`'s is 0, and its store takes no write)
    }
    crate::memory_lake::forget_memory(url);
    crate::reset::reset();
    PAGED.store(false, SeqCst); // (last: the next `open_page` starts once the statics are set back)
}

/// Opens a joined page's member (`Paged`): a read-only follower of the lake the cluster at `node` leads (designs/
/// l2-page-joins-cluster.md, J5). Its catalog is the node's (`kv::remote`, under the sign-in `auth`), its files are read from
/// `store` (the node's lake files through its door), and its commit stream is followed (`members::follow`), with a refresh every
/// 3 s (a node's is 250 ms, but each of a page's is a request). It claims no term and writes nothing to the cluster, so closing
/// it changes nothing there. The sign-in's end stops both loops and sets `ended` (a 401 or 403 from the node, or the refresh's
/// closed error).
pub async fn open_joined(node: &str, auth: &str, store: crate::store::Store, life: Arc<PageLife>) -> anyhow::Result<Paged> {
    claim_paged().await?;
    let url = crate::memory_lake::place(":memory:");
    crate::memory_lake::give(&url, store);
    let store = crate::memory_lake::store(&url);
    let node = node.trim_end_matches('/').to_string();
    let members: Arc<dyn Members> = Arc::new(crate::members::Joined::new(&node, auth)); // (the page's calls go to this node, with this sign-in: `dial::Joined`)
    let kv: Arc<dyn crate::kv::Kv> = Arc::new(crate::kv::remote::Remote::new(&node, auth));
    let marks = crate::panics::task(async {}); // (a joined page claims no term, so it runs no mark: this task is finished)
    let start = Start::default();
    let mut lake = None;
    let made = async {
        let l = crate::store::Lake::open_on(&url, true, kv, crate::node_state::NodeState::new(life.clone())).await?;
        lake = Some(l.clone());
        start_app(l, members, None, &start).await
    }
    .await;
    match made {
        Ok(app) => {
            let (superuser, slug) = whoami(&node, auth).await; // (the sign-in's: a superuser's joins stage their rows, `page::joined_part`)
            if let (Some(slug), Some(joined)) = (slug, app.members.joined()) {
                joined.set_slug(slug);
            }
            let (following, refreshing) = (Arc::downgrade(&app.lake), Arc::downgrade(&app.lake));
            let (http, auth) = (app.lake.node.net.client(crate::net::Policy::ENDPOINT), auth.to_string()); // (the join's sign-in alone: no token of ours)
            let ended = Arc::new(AtomicBool::new(false)); // (the sign-in ended: neither loop asks again)
            let (said, refreshed) = (ended.clone(), ended.clone());
            let open = move || -> futures::future::BoxFuture<'static, Result<crate::fetch::Response, crate::members::Open>> {
                let (http, url, auth, said) = (http.clone(), format!("{node}/cluster/log"), auth.clone(), said.clone());
                Box::pin(async move {
                    match crate::fetch::send(http.get(url).header(reqwest::header::AUTHORIZATION, auth)).await {
                        Ok(r) if r.status().is_success() => Ok(r),
                        // (the sign-in ended: the stream is not followed again)
                        Ok(r) if matches!(r.status(), reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN) => {
                            eprintln!("this page's sign-in at its node ended ({}): it no longer follows the lake", r.status());
                            said.store(true, SeqCst);
                            Err(crate::members::Open::Stop)
                        }
                        _ => Err(crate::members::Open::Retry),
                    }
                })
            };
            let follow = panics::spawn(crate::members::follow(following, open, |_, _| {}, || {}, false)); // (a page relays nothing: its node's feed is its stream)
            panics::spawn(crate::page_records::run(&app)); // (the page's own statements go to this node each second: `abort_all` ends it)
            panics::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    let Some(live) = refreshing.upgrade().filter(|_| !refreshed.load(SeqCst)) else { return };
                    if let Err(e) = live.refresh().await {
                        // (the Remote's closed error: the node refused the sign-in, so the catalog is no longer followed)
                        if e.downcast_ref::<crate::kv::Error>().is_some_and(|k| k.closed()) {
                            refreshed.store(true, SeqCst);
                        }
                        eprintln!("refreshing the joined lake's catalog: {e:#}");
                    }
                }
            });
            Ok(Paged { app, url, term: 0, marks, follow: Some(follow), ended, superuser: AtomicBool::new(superuser), life })
        }
        Err(e) => {
            end_page(marks, None, lake, &store, 0, &url).await; // (a failed open leaves nothing running)
            Err(e)
        }
    }
}

/// What the sign-in a joined page follows its node with is, as the node's `GET /whoami` says it (asked once as `open_joined` opens
/// the member, and again after a refusal of a staged write: `Paged::ask_superuser_again`): whether it is a superuser's, which
/// decides whether the page's INSERTs are its own to stage (`page::joined_part`), and its slug, the folder it stages them in
/// (`auth::slug`). Not known (no answer in 10 s, or a sign-in refused): neither.
async fn whoami(node: &str, auth: &str) -> (bool, Option<String>) {
    let asked = crate::fetch::send(crate::net::process().client(crate::net::Policy::ENDPOINT).get(format!("{node}/whoami")).header(reqwest::header::AUTHORIZATION, auth));
    let answer = async {
        let r = asked.await.ok().filter(|r| r.status().is_success())?;
        let who: serde_json::Value = r.json().await.ok()?;
        Some((who["superuser"].as_bool()?, who["slug"].as_str().map(str::to_string)))
    };
    tokio::time::timeout(Duration::from_secs(10), answer).await.ok().flatten().unwrap_or((false, None))
}

impl Paged {
    /// Whether the sign-in a joined page follows its node with is a superuser's (`whoami`). Always false for a page of its own,
    /// which has no node to write through.
    pub fn superuser(&self) -> bool {
        self.superuser.load(SeqCst)
    }

    /// Asks the node again whether the sign-in is a superuser's, after it refused a staged write with a 403 (`page::own_answer`):
    /// a user demoted since the join takes the page's own INSERTs no more, and they go whole to the node.
    pub async fn ask_superuser_again(&self) {
        if let Some(joined) = self.app.members.joined() {
            self.superuser.store(whoami(joined.node(), joined.auth()).await.0, SeqCst);
        }
    }

    /// Whether the sign-in a joined page follows its node with has ended (`open_joined`): its requests are then refused.
    pub fn ended(&self) -> bool {
        self.ended.load(SeqCst)
    }

    /// The SQLSTATE and the words of how the member's lake ended (its lifecycle's stop or restart: `PageLife`), or None while
    /// it lives. Every request is answered with them from then on.
    pub fn stopped(&self) -> Option<(&'static str, String)> {
        self.life.stopped()
    }

    /// Says the sign-in has ended, as a node refusing it does (`page::Page::ask`): the page answers 28000 from then on.
    pub fn set_ended(&self) {
        self.ended.store(true, SeqCst)
    }

    /// Ends the page's member (`end_page`), so another `open_page` starts afresh.
    pub async fn close(self) {
        let Paged { app, url, term, marks, follow, .. } = self;
        let _ = tokio::time::timeout(Duration::from_secs(2), crate::page_records::flush(&app)).await; // (what the page's statements wait to go to its node: once, within 2 s)
        let lake = app.lake.clone();
        drop(app);
        end_page(marks, follow, Some(lake.clone()), &lake.store, term, &url).await;
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
    panics::own(async move {
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

#[cfg(test)]
mod tests {
    /// A lake of its own, with its own node, led alone and run as a guest's (`pondra sql`'s) is: its statements go through
    /// `on_node`, as the leader's doors send them, and no loop of the leader's runs beside them (a tiering round's minute
    /// sweep would read every table inside a statement's count). The returned mark is aborted by the caller.
    async fn lake_app() -> (crate::app::App, tokio::task::JoinHandle<()>) {
        let url = format!("memory://catalog-counts-{}", uuid::Uuid::new_v4().simple());
        let (alone, marks) = crate::members::Alone::lead(&crate::memory_lake::store(&url)).await.unwrap();
        let lake = crate::store::Lake::open(&url, true, false, super::native(true)).await.unwrap(); // (a guest's: `node::start_app` starts no loops)
        let app = super::start_app(lake, alone, None, &super::Start::default()).await.unwrap();
        (app, marks)
    }

    /// The write-set rule (designs/redesign/redesign.md §4, step 5 piece 10e): a statement reads and writes the catalog
    /// records of what it names, not of every table. Among 2,000 tables, each reads and writes fewer than 50. (Here, with
    /// the node: a test in `write` may not start one, a floor below.)
    #[tokio::test]
    async fn a_statement_reads_and_writes_only_its_write_set() {
        crate::auth::as_node(async {
            let (app, marks) = lake_app().await;
            let (lake, counts) = (app.lake.clone(), app.lake.node.metrics.clone()); // (this lake's own counters: its node's)
            let spec = r#"{"columns": [["id", "Int64"], ["a", "Int64"]]}"#;
            for i in 0..2000 {
                crate::write::create_table(&lake, &format!("t{i}"), spec).await.unwrap();
            }
            let scope = crate::auth::Principal::node().scope;
            // (as a living leader does at its start: the catalog's records of the folders tables use made whole, then kept
            // by each write, so a new table's folder check reads its folder's alone: piece 10c)
            lake.cat.keep_records().await.unwrap();
            let within = 50;
            let statements = [
                ("INSERT", "INSERT INTO t0 VALUES (1, 1)", within),
                ("UPDATE", "UPDATE t0 SET a = 2 WHERE id = 1", within),
                ("DELETE", "DELETE FROM t0 WHERE id = 1", within),
                ("CREATE VIEW", "CREATE VIEW v0 AS SELECT * FROM t0", within),
                ("DROP TABLE", "DROP TABLE t1", within),
                ("ALTER TABLE RENAME TO", "ALTER TABLE t2 RENAME TO t2b", within),
                ("CREATE TABLE", "CREATE TABLE n0 (a BIGINT)", within),
                ("CREATE TABLE AS SELECT", "CREATE TABLE c0 AS SELECT * FROM t0", within),
            ];
            for (what, sql, most) in statements {
                let stmt = crate::write::parse(sql).unwrap_or_else(|| panic!("{sql}: not a statement"));
                let (reads0, writes0) = (counts.catalog_reads.load(std::sync::atomic::Ordering::Relaxed), counts.catalog_writes.load(std::sync::atomic::Ordering::Relaxed));
                crate::write::on_node(&app, stmt, None, &scope).await.unwrap_or_else(|e| panic!("{sql}: {e:#}"));
                let reads = counts.catalog_reads.load(std::sync::atomic::Ordering::Relaxed) - reads0;
                let writes = counts.catalog_writes.load(std::sync::atomic::Ordering::Relaxed) - writes0;
                eprintln!("{what}: read {reads}, wrote {writes}"); // (the numbers, with --nocapture)
                assert!(reads < most && writes < 50, "{what} ({sql}) read {reads} and wrote {writes} catalog records of 2,000 tables; under {most} and 50");
            }
            marks.abort(); // (a dropped handle aborts nothing: its mark would land again after the lake is gone)
            crate::memory_lake::forget_memory(&lake.url);
        })
        .await
    }

    /// The folder records the catalog keeps (`ddl::folder_hook`, step 5 piece 10c) say what every table says
    /// (`ddl::folder_users`) after each statement that moves a folder's use: a rename (the folder stays), a new table under
    /// the old name (`a__2`), a drop (kept), an undrop, a clone (a share), a clone's source dropped, a purge. A leader's start
    /// makes them whole for a table made before they were kept. (Here, with the node: a rename and a drop need the leader's
    /// sequencer.)
    #[tokio::test]
    async fn the_folder_records_say_what_every_table_says() {
        use crate::ddl::{folder_used, folder_users, free_folder, Use, FOLDER_USES};
        crate::auth::as_node(async {
            let (app, marks) = lake_app().await;
            let lake = app.lake.clone();
            let by = |uses: &mut Vec<(String, Use)>| uses.sort_by_key(|(n, u)| (n.clone(), format!("{u:?}")));
            let says = |lake: std::sync::Arc<crate::store::Lake>, step: &'static str| async move {
                let mut every = folder_users(&lake).await.unwrap();
                let recorded: std::collections::BTreeSet<String> = lake.cat.scan::<String>(FOLDER_USES, "fu0").await.unwrap().into_iter()
                    .map(|(k, _)| k[FOLDER_USES.len()..].split_once('/').unwrap().0.replace("%2F", "/").replace("%25", "%")).collect();
                assert!(recorded.iter().all(|f| every.contains_key(f)), "{step}: a record of a folder no table uses: {recorded:?} {:?}", every.keys().collect::<Vec<_>>());
                for (folder, uses) in every.iter_mut() {
                    let mut kept = folder_used(&lake, folder).await.unwrap();
                    by(uses);
                    by(&mut kept);
                    assert_eq!(&kept, uses, "{step}: folder {folder}");
                }
            };
            let scope = crate::auth::Principal::node().scope;
            let run = |sql: &'static str| {
                let (app, scope) = (app.clone(), scope.clone());
                async move {
                    let stmt = crate::write::parse(sql).unwrap_or_else(|| panic!("{sql}: not a statement"));
                    crate::write::on_node(&app, stmt, None, &scope).await.unwrap_or_else(|e| panic!("{sql}: {e:#}"));
                }
            };
            run("CREATE TABLE a (x BIGINT)").await; // (before they are kept: a guest's start keeps none)
            assert!(!lake.cat.records_kept());
            lake.cat.keep_records().await.unwrap();
            assert!(lake.cat.records_kept());
            says(lake.clone(), "made whole at the start").await;
            for (step, sql) in [
                ("a second table", "CREATE TABLE b (x BIGINT)"),
                ("a rename", "ALTER TABLE a RENAME TO a2"),
                ("a new table under the old name", "CREATE TABLE a (x BIGINT)"),
                ("a drop", "DROP TABLE b"),
                ("an undrop", "UNDROP TABLE b"),
                ("a clone", "CREATE TABLE c CLONE a2"),
                ("a clone's source dropped", "DROP TABLE a2"),
                ("a purge", "DROP TABLE b PURGE"),
            ] {
                run(sql).await;
                says(lake.clone(), step).await;
            }
            let a = lake.cat.get::<crate::store::TableMeta>(&crate::store::table_key("a")).await.unwrap().unwrap();
            assert_eq!(a.folder.as_deref(), Some("a__2"), "the renamed table keeps folder a: the new one takes the next");
            assert_eq!(free_folder(&lake, "a").await.unwrap().as_deref(), Some("a__3"), "a, kept for the dropped a2, and a__2 are used");
            assert_eq!(free_folder(&lake, "d").await.unwrap(), None);
            marks.abort();
            crate::memory_lake::forget_memory(&lake.url);
        })
        .await
    }

    #[test]
    fn a_claimed_page_refuses_a_second_claim_by_55006() {
        super::claim_page().expect("the first claim is taken");
        let refused = super::claim_page().expect_err("a second claim is refused while the first holds the page");
        assert_eq!(crate::codes::of(&refused), "55006");
        super::unclaim_page();
        super::claim_page().expect("the page is claimable again once it is given back");
        super::unclaim_page();
    }

    /// A page's lifecycle keeps how its member ended: a stop's 57P01 with why and the way out, said to the page's console;
    /// a restart's 57P03, after which the task that asked unwinds with `Restarting`. The first end is the one kept. It has no
    /// program and leads no other lake. (No page is claimed or opened here: `page::tests` says why. tests/page_lake.rs step 11
    /// runs a page through it.)
    #[test]
    fn a_page_life_keeps_its_first_end_and_unwinds_on_a_restart() {
        use crate::lifecycle::Lifecycle;
        static SAID: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        fn said(line: &str) { SAID.lock().unwrap().push(line.to_string()) }
        let stopped = super::PageLife::new(Some(said));
        assert!(stopped.stopped().is_none(), "a member that lives has not ended");
        stopped.stop("a test's stop".into());
        let (code, words) = stopped.stopped().expect("a stop is kept");
        assert_eq!(code, "57P01");
        assert!(words.contains("a test's stop") && words.contains("open it again"), "{words}");
        assert_eq!(SAID.lock().unwrap().as_slice(), [words.clone()], "the stop is said to the page's console");
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stopped.restart("a later restart", &[]))).expect_err("a restart doesn't return");
        assert!(unwound.downcast_ref::<super::Restarting>().is_some(), "it unwinds with Restarting");
        assert_eq!(stopped.stopped().map(|(c, _)| c), Some("57P01"), "the first end is the one kept");
        let restarted = super::PageLife::new(None);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| restarted.restart("a test's restart", &[])));
        let (code, words) = restarted.stopped().expect("a restart is kept");
        assert_eq!(code, "57P03");
        assert!(words.contains("a test's restart"), "{words}");
        assert!(restarted.program().is_err() && !restarted.one_off() && restarted.owns_process());
        let elsewhere = futures::executor::block_on(restarted.lead_elsewhere("s3://b/l")).expect_err("a page leads its own lake only");
        assert!(elsewhere.to_string().contains("s3://b/l"), "{elsewhere}");
    }
}
