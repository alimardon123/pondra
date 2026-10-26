//! A node: one lake served, from its start to its stop (ADR-061). The program (`cli.rs`) starts one
//! with its signals and standard input around it; a library will start and stop one in its own process.
//! A guest (`Options::guest`) is a node a program starts for its own statements: it answers them and ends.
use crate::*;
use std::{future::Future, sync::{Arc, OnceLock}, time::Duration};

/// What a node is started with: `pondra serve`'s options for one lake, worked out (cli.rs).
#[derive(Clone)]
pub struct Options {
    pub(crate) dir: String,
    /// The address the HTTP door listens on (`--addr`).
    pub(crate) listen: String,
    /// How the other nodes reach this one, if not `listen` (`--advertise`).
    pub(crate) advertise: Option<String>,
    pub(crate) reader: bool,
    pub(crate) flush_ms: u64,
    pub(crate) tier_secs: f64,
    pub(crate) task_ms: u64,
    pub(crate) retain_secs: u64,
    pub(crate) backlog: u64,
    pub(crate) pg: Option<String>,
    pub(crate) kafka: Option<String>,
    pub(crate) kafka_advertise: Option<String>,
    pub(crate) flight: Option<String>,
    pub(crate) attach: Vec<String>,
    pub(crate) attach_found: Option<String>,
    pub(crate) python: Option<String>,
    pub(crate) auth: Arc<auth::Auth>,
    /// What a guest is for (`Options::guest`); None for a node that lives.
    pub(crate) guest: Option<Guest>,
}

impl Options {
    /// `pondra serve --lake dir --addr listen`, every other option as `serve`'s default: the node a
    /// library opens (ADR-061 §2).
    pub fn local(dir: &str, listen: &str) -> Options {
        Options { dir: dir.into(), listen: listen.into(), advertise: None, reader: false, flush_ms: 0, tier_secs: 10.0, task_ms: 1000, retain_secs: 60, backlog: 10_000_000,
                  pg: None, kafka: None, kafka_advertise: None, flight: None, attach: vec![], attach_found: None, python: None, auth: Arc::new(auth::Auth::new(None, None, None)), guest: None }
    }

    /// A guest for `dir`: the node a program starts for its own statements, which answers them and ends. No
    /// address and no doors, and none of the loops a node that lives keeps (`Node::start`). Its tokens are the
    /// environment's, and its own calls are signed with a key it makes, as a library's node's are.
    pub fn guest(dir: &str, attach: Vec<String>, guest: Guest) -> Options {
        let auth = auth::Auth::from_env().owned_by(Some(uuid::Uuid::new_v4().simple().to_string()));
        Options { dir: dir.into(), listen: String::new(), advertise: None, reader: false, flush_ms: 0, tier_secs: 0.0, task_ms: 1000, retain_secs: 60, backlog: 10_000_000,
                  pg: None, kafka: None, kafka_advertise: None, flight: None, attach, attach_found: None, python: None, auth: Arc::new(auth), guest: Some(guest) }
    }
}

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

/// The node stops, for good, with this reason, when its own work can't go on (a vital loop's panic, the catalog's
/// compactor, a lake a newer Pondra wrote). In a library the node stops and the host's process stays: `library::failed`
/// says why, and this returns. In the program the process ends with status 1, as a supervisor expects of a stop and
/// with no core: what it made goes first (an exit runs no destructor).
pub(crate) fn stop(why: String) {
    if crate::library::on() {
        return crate::library::failed(why);
    }
    eprintln!("stopping: {why}");
    crate::memory::leave();
    std::process::exit(1)
}

/// A line the node says on stderr when all goes well (a node is up, a lake's format moved on). A library's host and a
/// `pondra sql` guest's shell show only trouble, so there it is not said.
pub(crate) fn say(line: impl std::fmt::Display) {
    if !crate::library::on() && !is_guest() {
        eprintln!("{line}");
    }
}

/// A guest's start found its lake's leader alive but not answering from here: its write goes through the bucket's
/// inbox instead (the program downcasts it to know so).
#[derive(Debug)]
pub struct Unreached;

impl std::fmt::Display for Unreached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the lake's leader is alive but doesn't answer from here")
    }
}

impl std::error::Error for Unreached {}

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

/// What a node holds as it starts (its term when it leads, then its lake), so a stop at any point gives back what was taken (ADR-061, invariant 300).
#[derive(Default)]
pub(crate) struct Held {
    term: OnceLock<(store::Store, u64)>,
    lake: OnceLock<Arc<store::Lake>>,
    /// Set by a guest that only reads: its catalog closes in a task of its own, which the answer doesn't wait for (a
    /// reader owes nothing: no term, nothing to make durable, and the close waits on its poller's cycle in flight).
    detach: OnceLock<()>,
}

impl Held {
    /// What a node does once drained (`drain.rs`, which the program runs first, with a second signal
    /// racing it): what it committed is made durable, the catalog is checkpointed, and a leader gives up
    /// its term, so the next node leads at once instead of waiting out the lease (invariant 210). A node
    /// stopped before its lake is open gives up its term without the durable wait and the checkpoint.
    /// In between, what the node started ends (ADR-061 §4): the HTTP door closes (`drain::close_door`), then its
    /// loops, then its catalog's own work, so nothing of it writes the lake once its term is given back (a keep-alive
    /// left running would write the leader's mark again).
    pub(crate) async fn stop(&self) {
        let (term, lake) = (self.term.get(), self.lake.get());
        let t0 = std::time::Instant::now();
        let tr = |w: &str| if crate::env::is_set("PONDRA_TRACE_START") { eprintln!("stop: {w} {:.2} s", t0.elapsed().as_secs_f64()) };
        // (a lake in memory goes with its node: no other node will read what it made durable)
        if let (Some(_), Some(l)) = (term, lake.filter(|l| !crate::memory::in_memory(&l.url))) {
            // (what it committed in the bucket, with replicated acks too; the catalog's
            // memtable written out: the next leader replays no WAL, C5)
            let _ = tokio::time::timeout(Duration::from_secs(10), l.cat.wait_durable(l.cat.committed())).await;
            tr("durable");
            let _ = tokio::time::timeout(Duration::from_secs(5), l.cat.checkpoint()).await;
            tr("checkpoint");
        }
        // (after the checkpoint, as a leader's followers may still call the door until then; before the tasks end, so
        // the answers the door is writing go out whole first)
        crate::drain::close_door().await;
        tr("door");
        panics::abort_all();
        tr("tasks");
        if let Some(l) = lake {
            if self.detach.get().is_some() {
                // (untracked: the next `abort_all` of this process must not stop a close half done)
                let l = l.clone();
                panics::untracked(async move { let _ = l.cat.close().await; });
                tr("catalog (detached)");
            } else {
                let _ = tokio::time::timeout(Duration::from_secs(5), l.cat.close()).await;
                tr("catalog");
            }
        }
        if let Some((store, n)) = term {
            cluster::step_down(store, *n).await;
            tr("term");
        }
    }
}

/// A node serving one lake: started, serving, then stopped. One at a time in a process, as often as
/// wanted (ADR-061 §3).
pub struct Node {
    /// What this node holds, given back by `stop`.
    held: Arc<Held>,
    /// The HTTP door, served on a task of its own: `serving` waits on it. None for a guest, which has no door.
    served: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    /// What the door serves: a library's host calls it straight (`library::Opened::ask`).
    router: axum::Router,
    /// The key this node's own calls are signed with (`Auth::owner_key`): `ask` sends it as the owner.
    owner: Option<String>,
    /// A guest's App, for `finish`: what it drains and waits for before it stops.
    guest: Option<server::App>,
}

/// A guest that reads opens its lake beside its term look (`Node::start`): this is that open, made at the same time as the
/// look and kept if the look finds no leader to stream from. One that finds a leader lets it finish and closes it, as
/// `close_after` does: an open is never aborted midway, since a reader dropped midway runs its poller for ever.
struct Beside(Option<tokio::task::JoinHandle<anyhow::Result<Arc<store::Lake>>>>);

/// A speculative open nothing keeps: it finishes, then its catalog closes, in a task of its own (untracked, so no
/// `abort_all` cuts the close short, and the poller stops with the close).
fn close_after(open: tokio::task::JoinHandle<anyhow::Result<Arc<store::Lake>>>) {
    panics::untracked(async move {
        if let Ok(Ok(lake)) = open.await {
            let _ = lake.cat.close().await;
        }
    });
}

impl Beside {
    fn start(dir: &str, guest: Option<Guest>) -> Beside {
        Beside(matches!(guest, Some(Guest::Reads)).then(|| {
            let dir = dir.to_string();
            panics::untracked(async move { store::Lake::open_polled(&dir, false, false, store::READER_ONE_SNAPSHOT).await })
        }))
    }

    /// The open, if the start keeps it: `streamed` false is a lake read beside its leader or with nobody leading, the
    /// open's own case. None when the start opens the lake itself (streamed, or nothing was begun).
    async fn keep(&mut self, streamed: bool) -> Option<anyhow::Result<Arc<store::Lake>>> {
        let open = self.0.take()?;
        if streamed {
            close_after(open);
            return None;
        }
        Some(match open.await {
            Ok(lake) => lake,
            Err(e) => Err(anyhow::Error::new(e)),
        })
    }
}

impl Drop for Beside {
    fn drop(&mut self) {
        if let Some(open) = self.0.take() {
            close_after(open); // (a start that ends before its look does, with an error, lets the open finish, then closes it)
        }
    }
}

impl Node {
    /// Everything `pondra serve` did before it served, then the HTTP door served on a task of its own.
    /// `held` takes what this start takes (the term, then the lake), so a stop during the start gives it back.
    pub(crate) async fn start(o: Options, held: Arc<Held>) -> anyhow::Result<Node> {
        let Options { dir, listen, advertise, reader, flush_ms, tier_secs, task_ms, retain_secs, backlog, pg, kafka, kafka_advertise, flight, attach: attached, attach_found, python, auth, guest } = o;
        // (a guest answers one program's statements and ends: none of the loops and doors a node that lives keeps, so
        // every `lives` below keeps them for a node that lives only)
        let lives = guest.is_none();
        GUEST.store(!lives, std::sync::atomic::Ordering::SeqCst);
        if lives {
            // (the spill folders of processes that ended, swept beside the start and not on it: a crashed one's may be
            // gigabytes, and a cold start waits for nothing but what serving needs, invariant 182)
            std::thread::spawn(crate::memory::sweep_all);
        }
        let serve_store = lives && crate::env::flag("PONDRA_SERVE_STORE"); // (read once, here: the checks, the cluster and the store's door all take this start's answer)
        crate::served::checked(&dir, reader, auth.admin_set(), serve_store)?; // (who may serve or open a store: `served.rs`)
        crate::origins::check()?; // (a page's origins that aren't origins stop the node here, before it joins or serves: `origins.rs`)
        let owner = auth.owner_key().map(str::to_string); // (what `ask` signs with: read before `auth` moves into the App)
        let (_, store, _) = store::open_store(&dir)?;
        let t_start = std::time::Instant::now();
        let tr = |w: &str| store::trace(w, t_start);
        let addr = advertise.unwrap_or_else(|| listen.clone()); // (how others reach it: its cluster name)
        if lives {
            sharing::set_endpoint(&addr); // (where a recipient's profile points: `sharing.rs`)
        }
        if let Some(at) = dir.strip_prefix("pondra://") {
            // (a lake whose node is away, or not up yet: wait for it, as a follower waits for its leader; a guest waits 10 s at most)
            let at = at.trim_end_matches('/');
            match lives {
                true => crate::served::wait_up(at).await,
                false => tokio::time::timeout(Duration::from_secs(10), crate::served::wait_up(at)).await.map_err(|_| anyhow::anyhow!("{dir} doesn't answer: the node that serves it must be up"))?,
            }
        }
        // (a guest that writes can't lead a lake read through another server: refused by name, as `write::deliver` refuses it)
        if guest == Some(Guest::Writes) && store::reach_of(&dir).is_some_and(|r| r.read_only) {
            return Err(crate::write::read_only_refused(&dir));
        }
        // (a guest that reads begins its lake's open here, beside the term look: the look and the open are each requests to
        // the bucket, and the start waits for the later of them, not for both in turn)
        let mut beside = Beside::start(&dir, guest);
        let cluster = match guest {
            None => cluster::Cluster::join(&store, &addr, reader, !store::served(&dir), serve_store).await?,
            Some(g) => match cluster::Cluster::guest(&store, g, !store::served(&dir)).await.map_err(|e| e.context(dir.clone()))? {
                Some(c) => c,
                // (its leader is alive and answers nobody here: the program sends the write through the bucket's inbox)
                None => return Err(anyhow::Error::new(Unreached)),
            },
        };
        // (a guest that reads is a reader from here on: no log, whose history and audit lines would go to a leader it doesn't have)
        let reader = reader || cluster.reader;
        let leader = cluster.is_leader();
        // (the node that serves its store leads it: its followers' store is in this process, invariant 299)
        anyhow::ensure!(!serve_store || leader, "--serve-store: {dir} is led by the node at {}; the node that serves a lake's store leads it (stop that node first)", cluster.leader.addr);
        if leader {
            // (taken before the trace below, so a stop that follows the trace line finds it)
            let _ = held.term.set((store.clone(), cluster.leader.n)); // (given back by a stop from here on: `Held::stop`)
        }
        tr("the lease");
        if leader {
            // "Still here", in the bucket, from the start: for machines outside the cluster. (A guest marks its term
            // as `write::lead` does: its task ends with the node.)
            if lives {
                cluster.clone().keep_alive(store.clone(), &dir); // (beside the catalog's opening, not before it: C5)
            } else {
                let _ = cluster::mark_every(store.clone(), cluster.leader.n);
            }
        }
        // Read-only nodes follow the leader's commit stream too (when a live one is there to ask),
        // so their reads are as fresh as a follower's instead of waiting for catalog polls.
        // (a guest's mode said whether its leader answers: a guest that follows one streams, and asks no more)
        let streamed = !leader && !cluster.leader.addr.is_empty() && (!reader || (lives && cluster.leader_alive().await));
        // (a guest that reads one snapshot for one statement never polls the bucket for newer manifests: `READER_ONE_SNAPSHOT`)
        let poll = match guest {
            Some(Guest::Reads) if !streamed => store::READER_ONE_SNAPSHOT,
            _ => store::READER_POLL,
        };
        let opened = match beside.keep(streamed).await {
            Some(lake) => lake,
            None => store::Lake::open_polled(&dir, leader, streamed, poll).await,
        };
        let lake = match opened {
            // (a lake a newer Pondra wrote, or a place a move didn't finish: this binary must not lead or serve it,
            // nor hold the term; and a restart would find the same place again)
            Err(e) if e.downcast_ref::<format::Newer>().is_some() || crate::memory::torn(&e) => {
                if leader {
                    cluster::release(&store, cluster.leader.n).await;
                }
                return Err(e);
            }
            // (a guest answers with the error: `Node::open` gives back what it took)
            Err(e) if !lives => return Err(e),
            Err(e) if leader => {
                eprintln!("opening the lake as leader failed: {e:#}"); // e.g. a newer leader fenced us
                tokio::time::sleep(Duration::from_secs(1)).await;
                cluster::restart("opening the lake as leader failed")
            }
            // A new lake whose first leader hasn't made the catalog yet (nodes started
            // together), or never will (it died first): look again, until it has or its
            // mark goes stale and this node takes over.
            Err(e) if ["failed to find latest transactional object", store::Lake::NO_LAKE].iter().any(|m| format!("{e:#}").contains(m)) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                cluster::restart("the lake has no catalog yet: its leader is still making it")
            }
            lake => lake?,
        };
        let _ = held.lake.set(lake.clone()); // (a stop from here on makes the lake durable first: `Held::stop`)
        if guest == Some(Guest::Reads) && !streamed {
            let _ = held.detach.set(()); // (a reader owes nothing: its close is let go, `Held::stop`)
        }
        tr("the lake open");
        for spec in &attached {
            attach(&lake, spec, &addr, lives).await?;
        }
        if let Some(folder) = &attach_found {
            let found = ddl::attach_found(&lake, folder, &addr).await;
            if !found.is_empty() {
                eprintln!("attached the lakes in {folder}: {}", found.join(", "));
            }
        }
        // The lakes attached in SQL (`ATTACH … AS …`), and later ATTACHes and DETACHes.
        tr("lakes attached");
        let (l, me) = (lake.clone(), addr.clone());
        ddl::sync(&l, &me, lives).await?;
        tr("attachments synced");
        if lives {
            every(Duration::from_secs(1), move || { let (l, me) = (l.clone(), me.clone()); async move { ddl::sync(&l, &me, true).await } });
            let l = lake.clone();
            crate::panics::task(async move { l.warm().await.map_err(|e| eprintln!("warming the SSD tier: {e:#}")) });
        }
        // Followers keep the leader's changes that aren't in the bucket yet (replicated acks);
        // a new leader first commits what they hold from the previous one.
        tr("warming");
        let replica = if !lives || reader || crate::memory::in_memory(&dir) { None } else { Some(replica::ReplicaLog::open(replica::dir(&dir, &addr))?) }; // (a lake in memory has no followers to hold changes for; a guest is on no network)
        if let (true, Some(own)) = (leader, &replica) {
            // (the members are asked with the nodes' key: one that just restarted holds an
            // unsigned call until it has caught up, and it catches up from this leader)
            if cluster::node_key().is_none() {
                if let Some(k) = users::node_key_made(&lake).await {
                    cluster::set_node_key(k);
                }
            }
            replica::recover(&lake, &addr, cluster.leader.n, Some(own)).await?;
        } else if leader && !lives {
            // (a guest that leads recovers as `write::lead` does: no followers' changes to hold, and no key to ask for)
            replica::recover(&lake, "", cluster.leader.n, None).await?;
        }
        tr("followers' changes recovered");
        let lock: Arc<tokio::sync::Mutex<()>> = Default::default(); // (the lake's: everything that rewrites table metadata)
        let pace = (tier_secs > 0.0).then(|| log::Pace { rows: backlog, lock: lock.clone() }); // rows waiting to be tiered
        let seq = if leader { Some(log::Sequencer::start(lake.clone(), pace).await?) } else { None };
        let to = match &seq {
            Some(s) => log::To::Local(s.clone()),
            None => log::To::Leader(cluster.leader.addr.clone()),
        };
        let _ = lake.to.set(to.clone()); // (a follower's sequences' values: the leader's sequencer)
        let log = (!reader).then(|| Arc::new(log::Log::start(lake.clone(), Duration::from_millis(flush_ms), to)));
        tr("the sequencer");
        python::init(python);
        let app = server::App { lake: lake.clone(), cluster: cluster.clone(), log, seq, lock, retain_ms: retain_secs * 1000, results: Default::default(), replica: replica.clone(), auth };
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
                    cluster::release(&store, cluster.leader.n).await; // (another node may lead it: not one with no first user)
                    return Err(e.context("a node on the network with no token makes the lake's first user before it serves, and that failed: set --admin-token, or listen on 127.0.0.1"));
                }
            }
        }
        if leader && lives {
            let (l, c, tokens) = (app.lake.clone(), app.cluster.clone(), app.auth.on()); // (beside serving, not before it: C5)
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
            let (l, c) = (app.lake.clone(), app.cluster.clone());
            panics::spawn(async move {
                // (a branch's pins in bases on other servers, renewed while it is used here: `branch::lapse`)
                while c.is_leader() {
                    branch::renew(&l).await;
                    tokio::time::sleep(branch::renew_every()).await;
                }
            });
        }
        tr("the app");
        if lives {
            let l = app.lake.clone();
            panics::spawn(async move {
                // (a follower's catalog shows the leader's keys once the leader has flushed them)
                let mut said = false;
                for _ in 0..600 {
                    match users::node_key(&l).await {
                        Ok(k) => {
                            cluster::set_node_key(k); // (nodes call each other with it when no admin token is set: cluster::http)
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
        if cluster.is_leader() && !cluster.reader && lives {
            runs::schedule(app.clone()); // (tasks: the leader runs their ticks)
        }
        if cluster.is_leader() && !cluster.reader && lives && !backup::frozen(&lake) {
            // (a lake opened somewhere it didn't publish from — moved by SET LOCATION, or copied — publishes its
            // Iceberg tables here at once: `iceberg::Published::location`. Only then: the pass loads every
            // published table's manifests, and a lake that stayed where it was has nothing new for it)
            let (a, l) = (app.clone(), lake.clone());
            panics::task(async move {
                if !crate::iceberg::elsewhere(&l).await {
                    return;
                }
                let _lock = a.lock.lock().await; // (the lock tiering publishes under: `server::tier_all`)
                if let Err(e) = crate::delta::publish_all(&l).await {
                    eprintln!("publishing at start: {e:#}");
                }
            });
        }
        // A door whose port is taken stops the start, as HTTP's does: beside a Postgres on 5432, psql would
        // otherwise talk to that one. (Each door binds again a moment later; handing it this listener is cleaner.)
        for (flag, door) in [("pg", &pg), ("kafka", &kafka), ("flight", &flight)] {
            if let Some(a) = door {
                std::net::TcpListener::bind(a.as_str()).map_err(|e| anyhow::anyhow!("--{flag} {a}: {e}"))?;
            }
        }
        if let Some(pg_addr) = pg {
            let a = app.clone();
            panics::spawn(async move { pg::serve(a, pg_addr).await.map_err(|e| eprintln!("postgres protocol: {e:#}")) });
        }
        if let Some(kafka_addr) = kafka {
            let port = kafka_addr.rsplit_once(':').map_or("9092", |(_, p)| p).to_string();
            let advertise = kafka_advertise.unwrap_or_else(|| format!("{}:{port}", addr.rsplit_once(':').map_or("127.0.0.1", |(h, _)| h)));
            let a = app.clone();
            panics::spawn(async move { kafka::serve(a, kafka_addr, advertise).await.map_err(|e| eprintln!("kafka protocol: {e:#}")) });
        }
        if let Some(flight_addr) = flight {
            let a = app.clone();
            panics::spawn(async move { flight::serve(a, flight_addr).await.map_err(|e| eprintln!("flight: {e:#}")) });
        }
        if lives && app.log.is_some() {
            feeds::start(app.clone()); // (other clusters' topics feeding views: none, no work)
        }
        if let (true, Some(_), Some(log)) = (lives, &app.seq, &app.log) {
            let (lake, log) = (lake.clone(), log.clone()); // windows and sessions past the watermark, emitted once
            panics::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if let Err(e) = views::emit_all(&lake, &log).await {
                        eprintln!("emission: {e:#}");
                    }
                }
            });
        }
        // Finished shuffles are forgotten here, and what they spilled deleted.
        if lives {
            every(Duration::from_secs(30), || async { spmd::gc(); Ok(()) });
        }
        if let (true, Some(seq)) = (lives, &app.seq) {
            inbox::serve(lake.clone(), seq.clone(), app.lock.clone(), cluster.rings()); // writers that can't reach us
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
            if tier_secs > 0.0 {
                let (a, mut hwm, period) = (app.clone(), lake.hwm.subscribe(), Duration::from_secs_f64(tier_secs));
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
            every(if tier_secs == 0.0 { Duration::ZERO } else { Duration::from_secs(1) }, move || { let a = a.clone(); async move { a.tier_all(1_000_000).await.map(|_| ()) } });
            // Persist the catalog's memtable every few seconds when something changed, so a
            // restart (or a new leader, reader or `pondra sql`) replays only a few seconds of
            // catalog WAL. Not more often: each is a level-0 file that SlateDB's compactor
            // has to keep up with, and writes stall when too many pile up.
            let l = lake.clone();
            every(Duration::from_secs(5), move || { let l = l.clone(); async move { l.cat.checkpoint().await } });
            if lake.cat.replicas > 1 {
                replica::members(lake.clone(), cluster.clone());
            }
            format::raise(lake.clone(), cluster.clone()); // (once every node knows this build's format: ADR-039)
        }
        if !leader {
            if lives {
                format::watch(lake.clone());
            }
            if streamed {
                if !lives {
                    cluster::signed(&lake).await; // (a guest's calls to its leader are signed with the nodes' key, which it waits for, 10 s at most)
                }
                cluster::catch_up(lake.clone(), store.clone(), cluster.leader.clone()); // (answers wait until this node holds what the leader had; a view that replays the WAL has it)
            }
            if lives {
                match reader {
                    false => cluster.clone().follow(store),
                    true => cluster.clone().watch_leader(store, streamed), // a reader never votes or leads
                }
            }
            if streamed {
                cluster::mirror(lake.clone(), cluster.leader.addr.clone(), addr.clone(), replica);
            }
            // How far our own catalog view is: all a reader has, and a follower's fallback.
            if lives {
                let l = lake.clone();
                every(Duration::from_millis(250), move || { let l = l.clone(); async move { l.refresh().await } });
            }
        }
        if !reader && lives {
            let (a, max_wait) = (app.clone(), Duration::from_millis(task_ms));
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
            let role = if reader { "reader" } else if leader { "leader" } else { "follower" };
            say(format!("pondra {role} (term {}) serving {dir} on {addr}", cluster.leader.n));
        }
        // No Nagle: a small answer goes out at once, not after the client's delayed ACK (the
        // Postgres, Kafka and Flight ports do the same).
        tr("listening");
        let listener = match lives {
            true => Some(tls::Doors::bind(&listen, tls::Door::Http).await?), // (TLS too: `tls.rs`)
            false => None,
        };
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
        let guest_app = (!lives).then(|| app.clone());
        // (the HTTP door, a loop the node can't do without: a panic in it aborts, as invariant 178 says; `serving` waits on it)
        let router = server::router(app);
        let served = listener.map(|listener| {
            let door = router.clone();
            panics::spawn(async move {
                // (closing once `drain::close_door` runs: no new connections, each one's answer finished, then the serve ends)
                drain::door_open();
                let out = axum::serve(listener, door.into_make_service_with_connect_info::<tls::Peer>()).with_graceful_shutdown(drain::closing()).await.map_err(anyhow::Error::from); // (who asks: `console::save_settings`)
                drain::door_closed();
                out
            })
        });
        Ok(Node { held, served, router, owner, guest: guest_app })
    }

    /// What the HTTP door serves, for a request made in this process (`library::Opened::ask`).
    pub(crate) fn router(&self) -> axum::Router { self.router.clone() }

    /// Until the HTTP door stops. An error ends the program, as it did when `serve` ran the door inline. A guest has
    /// no door: nothing ends this, and the program ends with its statement.
    pub(crate) async fn serving(&mut self) -> anyhow::Result<()> {
        match self.served.as_mut() {
            Some(served) => served.await?,
            None => std::future::pending().await,
        }
    }

    /// One request, made in this process to the node's own router as its owner (`request`), with no port: how a guest
    /// asks its statement, as a library's host asks its node (`library::Opened::ask`).
    pub async fn ask(&self, method: &str, path: &str, body: Vec<u8>) -> anyhow::Result<http::Response<axum::body::Body>> {
        use tower::ServiceExt;
        let req = request(method, path, &[], body, self.owner.as_deref())?;
        Ok(self.router.clone().oneshot(req).await.unwrap_or_else(|never| match never {}))
    }

    /// Ends a guest once its statement is answered: a guest that leads drains the inbox (the writers that couldn't reach
    /// it) and waits for what it committed to be durable, then it stops, on every path, so its term is given back. What
    /// the drain or the wait gave is returned after the stop. A node that lives is only stopped.
    pub async fn finish(self) -> anyhow::Result<()> {
        let Some(app) = self.guest.clone() else {
            self.stop().await;
            return Ok(());
        };
        let drained = async {
            if let Some(seq) = &app.seq {
                inbox::drain(&app.lake, seq, &app.lock).await?;
                app.lake.cat.wait_durable(app.lake.cat.committed()).await; // (no cap: the answer is printed after this, and a leader's acks are durable already)
            }
            anyhow::Ok(())
        }
        .await;
        crate::locks::release_all(); // (the row locks go to their leaders now, as `drain::drain` ends)
        self.stop().await; // (on every path: an early `?` would leave the term marked, and the next open in this process waiting on it)
        drained
    }

    /// Once drained (`drain.rs`, run by the program first): gives back what the node holds (`Held::stop`).
    /// `start` for a library: what the start takes is given back if it fails.
    pub async fn open(o: Options) -> anyhow::Result<Node> {
        let held = Arc::new(Held::default());
        match Node::start(o, held.clone()).await {
            Ok(node) => Ok(node),
            Err(e) => {
                held.stop().await;
                reset::reset();
                Err(e)
            }
        }
    }

    /// Stops the node, once the program drained it (`drain.rs`): its log writers write what they
    /// were sent (through the log, so while it still leads), it gives back what it holds
    /// (`Held::stop`), and its statics are set back (`reset`), so the next node in this process
    /// starts as in a process of its own (ADR-061 §3, invariant 301).
    pub async fn stop(self) {
        history::end_writers().await;
        self.held.stop().await;
        reset::reset();
    }
}

/// One request for a node's router, as its HTTP door takes it: `method` and `path` (with its query), `headers`, and
/// `body`, made in this process as the owner `owner` (its key, which the owner's files and rights go by), with no port
/// (`library::Opened::ask`, `Node::ask`).
pub(crate) fn request(method: &str, path: &str, headers: &[(String, String)], body: Vec<u8>, owner: Option<&str>) -> anyhow::Result<http::Request<axum::body::Body>> {
    let mut req = http::Request::builder().method(method).uri(path);
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if let Some(key) = owner {
        req = req.header("x-pondra-owner", key);
    }
    let mut req = req.body(axum::body::Body::from(body))?;
    let from = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
    req.extensions_mut().insert(axum::extract::ConnectInfo(crate::tls::Peer { addr: from, tls: false, signed: false, local: true }));
    Ok(req)
}

/// `--attach name=dir`: read another lake as `name.table`. A node keeps it fresh the way a
/// read-only node does: its leader's commit stream when that answers, and its own catalog view.
pub(crate) async fn attach(home: &store::Lake, spec: &str, me: &str, follow: bool) -> anyhow::Result<()> {
    let (name, dir) = spec.split_once('=').ok_or_else(|| anyhow::anyhow!("--attach takes name=dir"))?;
    ddl::database_name(name)?; // (the names the catalog attached keep working; a new `--attach _x=` is refused)
    ddl::attach(home, name, dir, me, follow, follow).await
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
