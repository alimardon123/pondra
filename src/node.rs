//! A node: one lake served, from its start to its stop (ADR-061). The program (`cli.rs`) starts one
//! with its signals and standard input around it; a library will start and stop one in its own process.
use crate::*;
use std::{future::Future, sync::{Arc, OnceLock}, time::Duration};

/// What a node is started with: `pondra serve`'s options for one lake, worked out (cli.rs).
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
}

impl Options {
    /// `pondra serve --lake dir --addr listen`, every other option as `serve`'s default: the node a
    /// library opens (ADR-061 §2).
    pub fn local(dir: &str, listen: &str) -> Options {
        Options { dir: dir.into(), listen: listen.into(), advertise: None, reader: false, flush_ms: 0, tier_secs: 10.0, task_ms: 1000, retain_secs: 60, backlog: 10_000_000,
                  pg: None, kafka: None, kafka_advertise: None, flight: None, attach: vec![], attach_found: None, python: None, auth: Arc::new(auth::Auth::new(None, None, None)) }
    }
}

/// What a node holds as it starts (its term when it leads, then its lake), so a stop at any point gives back what was taken (ADR-061, invariant 300).
#[derive(Default)]
pub(crate) struct Held {
    term: OnceLock<(store::Store, u64)>,
    lake: OnceLock<Arc<store::Lake>>,
}

impl Held {
    /// What a node does once drained (`drain.rs`, which the program runs first, with a second signal
    /// racing it): what it committed is made durable, the catalog is checkpointed, and a leader gives up
    /// its term, so the next node leads at once instead of waiting out the lease (invariant 210). A node
    /// stopped before its lake is open gives up its term without the durable wait and the checkpoint.
    /// In between, what the node started ends (ADR-061 §4): its loops, the HTTP door, then its
    /// catalog's own work, so nothing of it writes the lake once its term is given back (a keep-alive
    /// left running would write the leader's mark again).
    pub(crate) async fn stop(&self) {
        let (term, lake) = (self.term.get(), self.lake.get());
        if let (Some(_), Some(l)) = (term, lake) {
            // (what it committed in the bucket, with replicated acks too; the catalog's
            // memtable written out: the next leader replays no WAL, C5)
            let _ = tokio::time::timeout(Duration::from_secs(10), l.cat.wait_durable(l.cat.committed())).await;
            let _ = tokio::time::timeout(Duration::from_secs(5), l.cat.checkpoint()).await;
        }
        panics::abort_all();
        if let Some(l) = lake {
            let _ = tokio::time::timeout(Duration::from_secs(5), l.cat.close()).await;
        }
        if let Some((store, n)) = term {
            cluster::step_down(store, *n).await;
        }
    }
}

/// A node serving one lake: started, serving, then stopped. One at a time in a process, as often as
/// wanted (ADR-061 §3).
pub struct Node {
    /// What this node holds, given back by `stop`.
    held: Arc<Held>,
    /// The HTTP door, served on a task of its own: `serving` waits on it.
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Node {
    /// Everything `pondra serve` did before it served, then the HTTP door served on a task of its own.
    /// `held` takes what this start takes (the term, then the lake), so a stop during the start gives it back.
    pub(crate) async fn start(o: Options, held: Arc<Held>) -> anyhow::Result<Node> {
        let Options { dir, listen, advertise, reader, flush_ms, tier_secs, task_ms, retain_secs, backlog, pg, kafka, kafka_advertise, flight, attach: attached, attach_found, python, auth } = o;
        let (_, store, _) = store::open_store(&dir)?;
        let t_start = std::time::Instant::now();
        let tr = |w: &str| store::trace(w, t_start);
        let addr = advertise.unwrap_or_else(|| listen.clone()); // (how others reach it: its cluster name)
        sharing::set_endpoint(&addr); // (where a recipient's profile points: `sharing.rs`)
        let cluster = cluster::Cluster::join(&store, &addr, reader).await?;
        let leader = cluster.is_leader();
        if leader {
            // (taken before the trace below, so a stop that follows the trace line finds it)
            let _ = held.term.set((store.clone(), cluster.leader.n)); // (given back by a stop from here on: `Held::stop`)
        }
        tr("the lease");
        if leader {
            // "Still here", in the bucket, from the start: for machines outside the cluster.
            cluster.clone().keep_alive(store.clone(), &dir); // (beside the catalog's opening, not before it: C5)
        }
        // Read-only nodes follow the leader's commit stream too (when a live one is there to ask),
        // so their reads are as fresh as a follower's instead of waiting for catalog polls.
        let streamed = !leader && !cluster.leader.addr.is_empty() && (!reader || cluster.leader_alive().await);
        let lake = match store::Lake::open(&dir, leader, streamed).await {
            // (a lake a newer Pondra wrote: this binary must not lead or serve it, nor hold the term)
            Err(e) if e.downcast_ref::<format::Newer>().is_some() => {
                if leader {
                    cluster::release(&store, cluster.leader.n).await;
                }
                return Err(e);
            }
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
        tr("the lake open");
        for spec in &attached {
            attach(&lake, spec, &addr, true).await?;
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
        ddl::sync(&l, &me, true).await?;
        tr("attachments synced");
        every(Duration::from_secs(1), move || { let (l, me) = (l.clone(), me.clone()); async move { ddl::sync(&l, &me, true).await } });
        let l = lake.clone();
        crate::panics::task(async move { l.warm().await.map_err(|e| eprintln!("warming the SSD tier: {e:#}")) });
        // Followers keep the leader's changes that aren't in the bucket yet (replicated acks);
        // a new leader first commits what they hold from the previous one.
        tr("warming");
        let replica = if reader { None } else { Some(replica::ReplicaLog::open(replica::dir(&dir, &addr))?) };
        if let (true, Some(own)) = (leader, &replica) {
            // (the members are asked with the nodes' key: one that just restarted holds an
            // unsigned call until it has caught up, and it catches up from this leader)
            if cluster::node_key().is_none() {
                if let Some(k) = users::node_key_made(&lake).await {
                    cluster::set_node_key(k);
                }
            }
            replica::recover(&lake, &addr, cluster.leader.n, Some(own)).await?;
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
        learned::start(&lake); // (the history this node's planner learns from: nothing is read until a query is planned)
        if leader {
            let (l, c) = (app.lake.clone(), app.cluster.clone()); // (beside serving, not before it: C5)
            panics::spawn(async move {
                let made = users::kept(&l).await == "none";
                match users::make_keys(&l).await {
                    Err(e) => eprintln!("the lake's keys (sessions', the nodes'): {e:#}"), // (sessions' signing key, and the nodes' own: `users.rs`)
                    // (into every node's view at once: a node restarted before the next checkpoint
                    // couldn't check the nodes' key, and held its new leader's calls 10 s)
                    Ok(()) if made => l.cat.checkpoint().await.unwrap_or_else(|e| eprintln!("checkpoint: {e:#}")),
                    Ok(()) => {}
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
        if cluster.is_leader() && !cluster.reader {
            runs::schedule(app.clone()); // (tasks: the leader runs their ticks)
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
        if app.log.is_some() {
            feeds::start(app.clone()); // (other clusters' topics feeding views: none, no work)
        }
        if let (Some(_), Some(log)) = (&app.seq, &app.log) {
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
        every(Duration::from_secs(30), || async { spmd::gc(); Ok(()) });
        if let Some(seq) = &app.seq {
            inbox::serve(lake.clone(), seq.clone(), app.lock.clone()); // writers that can't reach us
        }
        if leader {
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
            // inserts, compaction, retention).
            if tier_secs > 0.0 {
                let (a, mut hwm, period) = (app.clone(), lake.hwm.subscribe(), Duration::from_secs_f64(tier_secs));
                panics::spawn(async move {
                    loop {
                        let start = tokio::time::Instant::now();
                        hwm.borrow_and_update();
                        let failed = a.tier_all(0).await.inspect_err(|e| eprintln!("background job failed: {e:#}")).is_err();
                        tokio::time::sleep_until(start + period).await;
                        // (right away if rows came meanwhile; a round that failed, say on a full
                        // disk, is tried again then too, not only once more rows come)
                        if !failed {
                            let _ = tokio::time::timeout(period * 4, hwm.changed()).await;
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
        } else {
            format::watch(lake.clone());
            if streamed {
                cluster::catch_up(lake.clone(), store.clone(), cluster.leader.clone()); // (answers wait until this node holds what the leader had; a view that replays the WAL has it)
            }
            match reader {
                false => cluster.clone().follow(store),
                true => cluster.clone().watch_leader(store, streamed), // a reader never votes or leads
            }
            if streamed {
                cluster::mirror(lake.clone(), cluster.leader.addr.clone(), addr.clone(), replica);
            }
            // How far our own catalog view is: all a reader has, and a follower's fallback.
            let l = lake.clone();
            every(Duration::from_millis(250), move || { let l = l.clone(); async move { l.refresh().await } });
        }
        if !reader {
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
        let role = if reader { "reader" } else if leader { "leader" } else { "follower" };
        eprintln!("pondra {role} (term {}) serving {dir} on {addr}", cluster.leader.n);
        // No Nagle: a small answer goes out at once, not after the client's delayed ACK (the
        // Postgres, Kafka and Flight ports do the same).
        tr("listening");
        let listener = tls::Doors::bind(&listen, tls::Door::Http).await?; // (TLS too: `tls.rs`)
        store::serving(); // (the catalog's compactor and garbage collector start now: C5)
        runs::mark_stopped(app.clone()); // (runs whose node stopped under them: `stopped`)
        if leader {
            let l = app.lake.clone(); // (what it inherited, replayed from the WAL, into every node's view)
            crate::panics::task(async move { l.cat.checkpoint().await.map_err(|e| eprintln!("checkpoint: {e:#}")) });
        }
        // (the HTTP door, a loop the node can't do without: a panic in it aborts, as invariant 178 says; `serving` waits on it)
        let served = panics::spawn(async move { axum::serve(listener, server::router(app).into_make_service_with_connect_info::<tls::Peer>()).await.map_err(anyhow::Error::from) }); // (who asks: `console::save_settings`)
        Ok(Node { held, served })
    }

    /// Until the HTTP door stops. An error ends the program, as it did when `serve` ran the door inline.
    pub(crate) async fn serving(&mut self) -> anyhow::Result<()> {
        (&mut self.served).await?
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

/// `--attach name=dir`: read another lake as `name.table`. A node keeps it fresh the way a
/// read-only node does: its leader's commit stream when that answers, and its own catalog view.
pub(crate) async fn attach(home: &store::Lake, spec: &str, me: &str, follow: bool) -> anyhow::Result<()> {
    let (name, dir) = spec.split_once('=').ok_or_else(|| anyhow::anyhow!("--attach takes name=dir"))?;
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
