//! The doors half of a node (`node.rs` is the lake half): the options a node is started with, its start and its stop,
//! and the requests its router answers. Declared by `node.rs` under feature `doors`, as the doors' own code is.
use crate::*;
use super::{Guest, Start};
use crate::members::Members;
use std::{sync::{Arc, OnceLock}, time::Duration};

/// What a node is started with: `pondra serve`'s options for one lake, worked out (cli.rs).
#[derive(Clone)]
pub struct Options {
    pub(crate) dir: String,
    /// The address the HTTP door listens on (`--addr`).
    pub(crate) listen: String,
    /// How the other nodes reach this one, if not `listen` (`--advertise`).
    pub(crate) advertise: Option<String>,
    pub(crate) start: crate::node::Start,
    pub(crate) pg: Option<String>,
    pub(crate) kafka: Option<String>,
    pub(crate) kafka_advertise: Option<String>,
    pub(crate) flight: Option<String>,
    pub(crate) attach: Vec<String>,
    pub(crate) attach_found: Option<String>,
    /// What a guest is for (`Options::guest`); None for a node that lives.
    pub(crate) guest: Option<Guest>,
    /// Its settings, read once: its process's as it is opened (`settings::Values::now`), after its flags are given.
    pub(crate) settings: crate::settings::Values,
    /// Why its lake's last move out of memory failed (`memory::move_failed`): its host starts it again on the lake in
    /// memory after one, and it says why (`NodeState::move_failed`). None once a move lands.
    pub(crate) move_failed: Option<String>,
    /// Its host (`node::Host`): told when the node's own work fails or must start again, and takes its lake's moves. None
    /// for a guest, a command's own node and a test's: a failure ends the process, and the lake can't move.
    pub(crate) host: Option<crate::node::Host>,
}

impl Options {
    /// `pondra serve --lake dir --addr listen`, every other option as `serve`'s default: the node a
    /// library opens (ADR-061 §2).
    pub fn local(dir: &str, listen: &str) -> Options {
        Options { dir: dir.into(), listen: listen.into(), advertise: None, start: Start::default(),
                  pg: None, kafka: None, kafka_advertise: None, flight: None, attach: vec![], attach_found: None, guest: None, settings: crate::settings::Values::now(), move_failed: None, host: None }
    }

    /// A guest for `dir`: the node a program starts for its own statements, which answers them and ends. No
    /// address and no doors, and none of the loops a node that lives keeps (`Node::start`). Its tokens are the
    /// environment's, and its own calls are signed with a key it makes, as a library's node's are.
    pub fn guest(dir: &str, attach: Vec<String>, guest: Guest) -> Options {
        // (a lake in this process's memory is new: nobody wrote it, so a guest that only reads leads it, and reads its empty
        // catalog, as `pondra sql --dir :memory: "SELECT 1"` asks; a reader would find no lake there. An empty folder is still
        // refused, which catches a mistyped path)
        let guest = if crate::memory_lake::in_memory(dir) { Guest::Writes } else { guest };
        let auth = auth::Auth::from_env().owned_by(Some(uuid::Uuid::new_v4().simple().to_string()));
        Options { dir: dir.into(), listen: String::new(), advertise: None, start: Start { tier_secs: 0.0, auth: Arc::new(auth), ..Start::default() },
                  pg: None, kafka: None, kafka_advertise: None, flight: None, attach, attach_found: None, guest: Some(guest), settings: crate::settings::Values::now(), move_failed: None, host: None }
    }
}

/// What a node holds as it starts (its term when it leads, then its lake), so a stop at any point gives back what was taken (ADR-061, invariant 300).
#[derive(Default)]
pub(crate) struct Held {
    term: OnceLock<(store::Store, u64)>,
    lake: OnceLock<Arc<store::Lake>>,
    /// Set by a guest that only reads: its catalog closes in a task of its own, which the answer doesn't wait for (a
    /// reader owes nothing: no term, nothing to make durable, and the close waits on its poller's cycle in flight).
    detach: OnceLock<()>,
    /// The node's state (`node_state.rs`), set as its start begins: what its drain and its door's close act on.
    node: OnceLock<Arc<crate::node_state::NodeState>>,
}

impl Held {
    /// Ends every task the node started but the one asking (`NodeState::tasks`). The loops of its statics end as they are
    /// set back, last (`spawns::for_statics`, `reset::reset`).
    pub(crate) fn abort_tasks(&self) {
        if let Some(node) = self.node.get() {
            node.tasks.abort_all();
        }
    }

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
        if let (Some(_), Some(l)) = (term, lake.filter(|l| !crate::memory_lake::in_memory(&l.url))) {
            // (what it committed in the bucket, with replicated acks too; the catalog's
            // memtable written out: the next leader replays no WAL, C5)
            let _ = tokio::time::timeout(Duration::from_secs(10), l.cat.wait_durable(l.cat.committed())).await;
            tr("durable");
            let _ = tokio::time::timeout(Duration::from_secs(5), l.cat.checkpoint()).await;
            tr("checkpoint");
        }
        // (after the checkpoint, as a leader's followers may still call the door until then; before the tasks end, so
        // the answers the door is writing go out whole first)
        if let Some(node) = self.node.get() {
            crate::drain::close_door(node).await;
        }
        tr("door");
        self.abort_tasks();
        tr("tasks");
        if let Some(l) = lake {
            if self.detach.get().is_some() {
                // (untracked: the next `abort_all` of this process must not stop a close half done)
                let l = l.clone();
                crate::spawns::untracked(async move { let _ = l.cat.close().await; });
                tr("catalog (detached)");
            } else {
                let _ = tokio::time::timeout(Duration::from_secs(5), l.cat.close()).await;
                tr("catalog");
            }
        }
        if let Some((store, n)) = term {
            if let Some(node) = self.node.get() {
                terms::step_down(store, *n, &node.claims).await; // (the node's claims: `Claims`)
            }
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
    guest: Option<app::App>,
    /// What `--idle-exit` looks at for work left (`idle::ended`).
    work: crate::idle::Work,
}

/// A guest that reads opens its lake beside its term look (`Node::start`): this is that open, made at the same time as the
/// look and kept if the look finds no leader to stream from. One that finds a leader lets it finish and closes it, as
/// `close_after` does: an open is never aborted midway, since a reader dropped midway runs its poller for ever.
struct Beside(Option<tokio::task::JoinHandle<anyhow::Result<Arc<store::Lake>>>>);

/// A speculative open nothing keeps: it finishes, then its catalog closes, in a task of its own (untracked, so no
/// `abort_all` cuts the close short, and the poller stops with the close).
fn close_after(open: tokio::task::JoinHandle<anyhow::Result<Arc<store::Lake>>>) {
    crate::spawns::untracked(async move {
        if let Ok(Ok(lake)) = open.await {
            let _ = lake.cat.close().await;
        }
    });
}

impl Beside {
    fn start(dir: &str, guest: Option<Guest>, here: &Arc<crate::node_state::NodeState>) -> Beside {
        Beside(matches!(guest, Some(Guest::Reads)).then(|| {
            let (dir, here) = (dir.to_string(), here.clone());
            crate::spawns::untracked(async move { store::Lake::open_polled(&dir, false, false, store::READER_ONE_SNAPSHOT, here).await })
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
        let Options { dir, listen, advertise, start, pg, kafka, kafka_advertise, flight, attach: attached, attach_found, guest, settings, move_failed, host } = o;
        // (a guest answers one program's statements and ends: none of the loops and doors a node that lives keeps, so
        // every `lives` below keeps them for a node that lives only)
        let lives = guest.is_none();
        // (the machine and the network, installed by the program's start; a library's process or a test's installs them here,
        // its first node's start, as the host it is)
        crate::machine::install(Arc::new(crate::os::Os::default()));
        crate::net::install(Arc::new(crate::os::Net::default()));
        // (what this node does when it must stop or start again, and what its stop waits for: its lakes and its member hold it)
        let here = crate::node::native_with(!lives, settings, host);
        let _ = held.node.set(here.clone()); // (a stop drains and closes this node's door: `Held::stop`)
        if let Some(why) = move_failed {
            let _ = here.move_failed.set(why); // (its lake stayed in memory: `/stats` and `pondra.databases` say why)
        }
        if lives {
            // (the spill folders of processes that ended, swept beside the start and not on it: a crashed one's may be
            // gigabytes, and a cold start waits for nothing but what serving needs, invariant 182)
            std::thread::spawn(crate::scratch::sweep_all);
        }
        let serve_store = lives && here.settings.flag("PONDRA_SERVE_STORE"); // (read once, here: the checks, the cluster and the store's door all take this start's answer)
        crate::served::checked(&dir, start.reader, start.auth.admin_set(), serve_store)?; // (who may serve or open a store: `served.rs`)
        crate::origins::check()?; // (a page's origins that aren't origins stop the node here, before it joins or serves: `origins.rs`)
        let owner = start.auth.owner_key().map(str::to_string); // (what `ask` signs with)
        let store = store::counted(store::open_store(&dir, Some(&here.reaches))?.1, &here.metrics); // (its terms, marks and members' requests the node's: `Counted`)
        let tr = |what: &str| here.trace(what); // (timed from this start: `NodeState::trace`)
        let addr = advertise.unwrap_or_else(|| listen.clone()); // (how others reach it: its cluster name)
        if let Some(at) = dir.strip_prefix("pondra://") {
            // (a lake whose node is away, or not up yet: wait for it, as a follower waits for its leader; a guest waits 10 s at most)
            let at = at.trim_end_matches('/');
            match lives {
                true => crate::served::wait_up(at).await,
                false => tokio::time::timeout(Duration::from_secs(10), crate::served::wait_up(at)).await.map_err(|_| anyhow::anyhow!("{dir} doesn't answer: the node that serves it must be up"))?,
            }
        }
        // (a guest that writes can't lead a lake read through another server: refused by name, as `write::deliver` refuses it)
        if guest == Some(Guest::Writes) && here.reaches.of(&dir).is_some_and(|r| r.read_only) {
            return Err(crate::write::read_only_refused(&dir));
        }
        // (a guest that reads begins its lake's open here, beside the term look: the look and the open are each requests to
        // the bucket, and the start waits for the later of them, not for both in turn)
        let mut beside = Beside::start(&dir, guest, &here);
        let cluster = match guest {
            None => cluster::Cluster::join(&store, &addr, start.reader, !store::served(&dir), serve_store, here.clone()).await?,
            Some(g) => cluster::Cluster::guest(&store, g, !store::served(&dir), here.clone()).await.map_err(|e| e.context(dir.clone()))?,
        };
        // (a guest that reads is a reader from here on: no log, whose history and audit lines would go to a leader it doesn't have)
        let reader = start.reader || cluster.reader;
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
                cluster.clone().keep_alive(store.clone(), &dir, &here); // (beside the catalog's opening, not before it: C5)
            } else {
                let _ = terms::mark_every(&here.tasks, store.clone(), cluster.leader.n);
            }
        }
        // Read-only nodes follow the leader's commit stream too (when a live one is there to ask),
        // so their reads are as fresh as a follower's instead of waiting for catalog polls.
        // (a guest's mode said whether its leader answers: a guest that follows one streams, and asks no more)
        let streamed = !leader && !cluster.leader.addr.is_empty() && (!reader || (lives && cluster.leader_alive().await));
        // (a guest that reads one snapshot for one statement never polls the bucket for newer manifests: `READER_ONE_SNAPSHOT`;
        // an inbox guest polls as a follower does: its `seen_there` waits for the leader's mark, which a snapshot never shows)
        // (a node that follows the commit stream answers from it, and polls less: `FOLLOWER_POLL`)
        let poll = match guest {
            Some(Guest::Reads) if !streamed => store::READER_ONE_SNAPSHOT,
            None if streamed => store::FOLLOWER_POLL,
            _ => store::READER_POLL,
        };
        let opened = match beside.keep(streamed).await {
            Some(lake) => lake,
            None => store::Lake::open_polled(&dir, leader, streamed, poll, here.clone()).await,
        };
        let lake = match opened {
            // (a lake a newer Pondra wrote, or a place a move didn't finish: this binary must not lead or serve it,
            // nor hold the term; and a restart would find the same place again)
            Err(e) if e.downcast_ref::<format::Newer>().is_some() || crate::memory_lake::torn(&e) => {
                if leader {
                    terms::release(&store, cluster.leader.n, &here.claims).await;
                }
                return Err(e);
            }
            // (a guest answers with the error: `Node::open` gives back what it took)
            Err(e) if !lives => return Err(e),
            Err(e) if leader => {
                eprintln!("opening the lake as leader failed: {e:#}"); // e.g. a newer leader fenced us
                tokio::time::sleep(Duration::from_secs(1)).await;
                here.life.restart("opening the lake as leader failed", &[])
            }
            // A new lake whose first leader hasn't made the catalog yet (nodes started
            // together), or never will (it died first): look again, until it has or its
            // mark goes stale and this node takes over.
            Err(e) if ["failed to find latest transactional object", store::Lake::NO_LAKE].iter().any(|m| format!("{e:#}").contains(m)) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                here.life.restart("the lake has no catalog yet: its leader is still making it", &[])
            }
            lake => lake?,
        };
        let _ = held.lake.set(lake.clone()); // (a stop from here on makes the lake durable first: `Held::stop`)
        if lives {
            sharing::set_endpoint(&lake, &addr); // (where a recipient's profile points: `sharing.rs`)
        }
        if (guest == Some(Guest::Reads) || cluster.by_inbox()) && !streamed {
            let _ = held.detach.set(()); // (a reader owes nothing: its close is let go, `Held::stop`; an inbox guest's writes are answered before it)
        }
        tr("the lake open");
        for spec in &attached {
            attach(&lake, spec, &addr, lives).await?;
        }
        if let Some(folder) = &attach_found {
            let found = ddl::attach_found(&lake, folder).await;
            if !found.is_empty() {
                eprintln!("attached the lakes in {folder}: {}", found.join(", "));
            }
        }
        // The lakes attached in SQL (`ATTACH … AS …`), and later ATTACHes and DETACHes.
        tr("lakes attached");
        let replica = if !lives || reader || crate::memory_lake::in_memory(&dir) { None } else { Some(replica::ReplicaLog::open(replica::dir(&dir, &addr)?)?) }; // (a lake in memory has no followers to hold changes for; a guest is on no network)
        let app = node::start_app(lake.clone(), cluster.clone(), replica.clone(), &start).await?;
        tr("the app");
        if leader && !lives {
            // (a guest that leads its lake runs no leader's loop to raise it, `format::raise`: it moves on as it opens)
            crate::format::alone(&lake).await.unwrap_or_else(|e| eprintln!("the lake's format: {e:#}"));
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
            here.tasks.spawn_node(crate::spawns::Vital::Stop, async move { pg::serve(a, pg_addr).await.map_err(|e| eprintln!("postgres protocol: {e:#}")) });
        }
        if let Some(kafka_addr) = kafka {
            let port = kafka_addr.rsplit_once(':').map_or("9092", |(_, p)| p).to_string();
            let advertise = kafka_advertise.unwrap_or_else(|| format!("{}:{port}", addr.rsplit_once(':').map_or("127.0.0.1", |(h, _)| h)));
            let a = app.clone();
            here.tasks.spawn_node(crate::spawns::Vital::Stop, async move { kafka::serve(a, kafka_addr, advertise).await.map_err(|e| eprintln!("kafka protocol: {e:#}")) });
        }
        if let Some(flight_addr) = flight {
            let a = app.clone();
            here.tasks.spawn_node(crate::spawns::Vital::Stop, async move { flight::serve(a, flight_addr).await.map_err(|e| eprintln!("flight: {e:#}")) });
        }
        if let (true, Some(seq)) = (lives, &app.seq) {
            inbox::serve(lake.clone(), seq.clone(), app.lock.clone(), cluster.rings()); // writers that can't reach us
        }
        if !leader {
            if streamed {
                if !lives {
                    crate::dial::signed(&lake).await; // (a guest's calls to its leader are signed with the nodes' key, which it waits for, 10 s at most)
                }
                cluster::catch_up(lake.clone(), store.clone(), cluster.leader.clone()); // (answers wait until this node holds what the leader had; a view that replays the WAL has it)
            }
            let down = Arc::new(std::sync::atomic::AtomicBool::new(false)); // (is the leader's commit stream down: the mirror says, a reader's watch reads)
            if lives {
                match reader {
                    false => cluster.clone().follow(store, lake.log.untaken.clone(), &here),
                    true => cluster.clone().watch_leader(store, streamed, down.clone(), &here), // a reader never votes or leads
                }
            }
            if streamed {
                cluster::mirror(lake.clone(), cluster.leader.addr.clone(), addr.clone(), replica, down.clone());
                if !lives {
                    // (a guest synced the lakes attached in SQL once, in `start_app`, from a view that skips the WAL, and
                    // syncs no more: an ATTACH its leader made in the last few seconds wasn't there yet, and `pondra sql`
                    // found no such table until the leader's next checkpoint. Once it holds what its leader had: again.)
                    let _ = lake.caught_up().await;
                    ddl::sync(&lake, false).await?;
                }
            }
        }
        if lives {
            let role = if reader { "reader" } else if leader { "leader" } else { "follower" };
            here.life.say(&format!("pondra {role} (term {}) serving {dir} on {addr}", cluster.leader.n));
        }
        // No Nagle: a small answer goes out at once, not after the client's delayed ACK (the
        // Postgres, Kafka and Flight ports do the same).
        tr("listening");
        let listener = match lives {
            true => Some(tls::Doors::bind(&listen, tls::Door::Http, &here.tasks, crate::auth::Door).await?.for_node(here.clone())), // (TLS too: `tls.rs`)
            false => None,
        };
        node::keep(&app, &start);
        let guest_app = (!lives).then(|| app.clone());
        let work = crate::idle::Work::of(&app);
        // (the HTTP door, a loop the node can't do without: a panic in it aborts, as invariant 178 says; `serving` waits on it)
        let router = server::router(app);
        let served = listener.map(|listener| {
            let (door, here) = (router.clone(), here.clone());
            let node = here.clone();
            node.tasks.spawn_node(crate::spawns::Vital::Stop, async move {
                // (closing once `drain::close_door` runs: no new connections, each one's answer finished, then the serve ends)
                here.drain.door_open();
                let out = axum::serve(listener, door.into_make_service_with_connect_info::<tls::Peer>()).with_graceful_shutdown(here.drain.closing()).await.map_err(anyhow::Error::from); // (who asks: `console::save_settings`)
                here.drain.door_closed();
                out
            })
        });
        Ok(Node { held, served, router, owner, guest: guest_app, work })
    }

    /// The node's state (`node_state.rs`): its lifecycle, its drain and its uses.
    fn here(&self) -> &Arc<crate::node_state::NodeState> { self.held.node.get().expect("a started node's state") }

    /// Drains the node before its stop (`drain::drain`): it stops being ready, turns new requests away, and waits for the
    /// ones in flight.
    pub(crate) async fn drain(&self) {
        crate::drain::drain(self.here()).await;
        // (the transactions' row locks go to their leaders now, as the drain ends: other waits end at once, not at the lease)
        if let Some(at) = self.held.lake.get().and_then(|l| crate::temp::sessions(l)) {
            crate::locks::release_all(&at);
        }
    }

    /// A second signal while draining: the stop doesn't wait for the door (`Drain::hurry`).
    pub(crate) fn hurry(&self) { self.here().drain.hurry() }

    /// Resolves once nobody has used the node for `secs` and it has nothing left to do (`idle::ended`, `--idle-exit`).
    pub(crate) fn idle(&self, secs: f64) -> impl std::future::Future<Output = ()> + Send + 'static { crate::idle::ended(secs, self.here().clone(), self.work.clone()) }

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
                inbox::drain(&app.lake, seq, &app.lock, false).await?;
                app.lake.cat.wait_durable(app.lake.cat.committed()).await; // (no cap: the answer is printed after this, and a leader's acks are durable already)
                // (what it wrote is durable: a full catalog level 0 or a long log is tidied on the way out; one that fails is left to the next)
                if let Err(e) = app.tidy_one_off().await {
                    eprintln!("the log stays untiered for now: {e:#}");
                }
            }
            anyhow::Ok(())
        }
        .await;
        if let Some(at) = crate::temp::sessions(&app.lake) {
            crate::locks::release_all(&at); // (the row locks go to their leaders now, as `drain::drain` ends)
        }
        self.stop().await; // (on every path: an early `?` would leave the term marked, and the next open in this process waiting on it)
        drained
    }

    /// Once drained (`drain.rs`, run by the program first): gives back what the node holds (`Held::stop`).
    /// `start` for a library: what the start takes is given back if it fails.
    pub async fn open(o: Options) -> anyhow::Result<Node> { Node::open_in(o, Arc::new(Held::default())).await }

    /// [`Node::open`] into `held`, which the caller keeps: a start whose future it drops (a library's supervisor told to
    /// restart or close meanwhile) still ends the tasks it started (`Held::abort_tasks`).
    pub(crate) async fn open_in(o: Options, held: Arc<Held>) -> anyhow::Result<Node> {
        match Node::start(o, held.clone()).await {
            Ok(node) => Ok(node),
            Err(e) => {
                held.stop().await;
                if let Some(node) = held.node.get() {
                    node.claims.clear(); // (its claims' locks let go as it stops: `Claims`)
                }
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
        if let Some(lake) = self.held.lake.get() {
            history::end_writers(lake).await;
        }
        self.held.stop().await;
        if let Some(node) = self.held.node.get() {
            node.claims.clear(); // (its claims' locks let go as it stops: `Claims`)
        }
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
    let leader = crate::terms::latest(&crate::store::open_store(dir, Some(&home.node.reaches))?.1).await?.map(|t| t.addr).filter(|a| !a.is_empty());
    let live = match (&leader, follow) {
        (Some(a), true) => crate::dial::call(&home.node, a, crate::dial::Call::get("/cluster/leader").timeout(Duration::from_secs(2))).await.is_ok(),
        _ => false,
    };
    let other = ddl::attach(home, name, dir, follow, live).await?;
    if let (true, Some(a)) = (live, leader) {
        cluster::mirror(other, a, me.to_string(), None, Default::default()); // (its own flag: an attached lake's stream says nothing of the home lake's leader)
    }
    Ok(())
}

/// The program's restart: the same binary runs again with the same arguments, in place (exec) on unix; elsewhere a
/// new process starts and this one leaves. `carried` goes to the new process on top of what this one was given.
pub(crate) fn exec_again(guest: bool, carried: &[(&str, String)]) -> ! {
    crate::memory::leave(); // (what this process made goes first: exec and `process::exit` run no destructor)
    let mut args = std::env::args_os();
    let mut cmd = std::process::Command::new(args.next().expect("argv[0]"));
    // (a guest's rerun keeps its job, so a write it recorded once isn't recorded again by the rerun: `write::job`)
    let mut carry = carried.to_vec();
    if guest && !carried.iter().any(|(k, _)| *k == "PONDRA_JOB") {
        carry.push(("PONDRA_JOB", crate::write::job().to_string()));
        carry.push(("PONDRA_TRIES", (crate::write::tries() + 1).to_string()));
    }
    // (its settings by its environment: an exec keeps this process's input, whose line of settings was taken, so the
    // program run again isn't asked for another, `--settings-stdin`)
    cmd.args(args.filter(|a| a != "--settings-stdin")).envs(crate::env::given()).envs(carry.iter().map(|(k, v)| (k, v)));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        panic!("restart failed: {}", cmd.exec()); // replaces this process; only returns on failure
    }
    #[cfg(not(unix))]
    if crate::env::is_set("PONDRA_SUPERVISED") {
        std::process::exit(75); // (`pondra service`'s supervisor starts it again: one node, as exec keeps it elsewhere)
    }
    #[cfg(not(unix))]
    match cmd.spawn() {
        // No exec() on Windows: start the replacement, then leave (it binds our address once we're gone).
        Ok(_) => std::process::exit(0),
        Err(e) => panic!("restart failed: {e}"),
    }
}

/// Leads the lake at `dir` in a `pondra lead` process of its own, which answers the lake's inbox and ends
/// (`node::lead_once` says why it is not the node itself). It is handed the node's `values` (`settings::Line`) and the
/// places it reaches (`reaches`).
pub(crate) async fn lead_once(dir: &str, values: &crate::settings::Values, reaches: &crate::reaches::Reaches) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut cmd = tokio::process::Command::new(crate::node::exe()?);
    cmd.args(["lead", "--dir", dir]).env_remove("PONDRA_JOB").env_remove("PONDRA_TRIES").stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped());
    let line = crate::settings::Line::of(values).with(crate::reaches::REACH_VAR, reaches.handed()).hand(cmd.as_std_mut()); // (and the keys this node reaches other places with)
    let mut child = cmd.spawn()?;
    let mut input = child.stdin.take().expect("a pipe");
    let _ = input.write_all(line.as_bytes()).await; // (one that ended at once says why on its stderr, below)
    drop(input);
    let out = child.wait_with_output().await?;
    anyhow::ensure!(out.status.success(), "couldn't lead the lake at {dir}: {}", String::from_utf8_lossy(&out.stderr).trim());
    Ok(())
}
