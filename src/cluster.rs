//! Cluster mode. Every node is the same binary pointed at the same bucket; there is no
//! coordination service. The bucket and plain HTTP are enough:
//!
//! * The leader is the one catalog writer. Leadership is a term number: `cluster/term/{n}` is
//!   created put-if-absent, so exactly one node wins each term, and SlateDB fencing guarantees
//!   an older leader can no longer commit (it exits).
//! * Followers answer SQL from the bucket themselves and forward writes to the leader.
//! * Liveness is HTTP: followers heartbeat the leader every second and get back the member list,
//!   which decides who runs which streaming-task shard. If the leader stays unreachable for
//!   `LEASE` and no other member has heard from it either, a follower claims the next term and
//!   restarts itself as the leader. (The check keeps one badly connected follower from
//!   deposing a healthy leader.)
//! * The leader also marks itself alive in the bucket every 10 s (`cluster/alive/{n}`), for
//!   machines outside the cluster: a node starting on an idle lake leads at once instead of
//!   waiting out a lease, and a `pondra sql` INSERT never deposes a leader it merely can't reach.
use crate::replica::ReplicaLog;
use crate::store::{Frame, Lake, Store};
use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LEASE: Duration = Duration::from_secs(5);
const STARTUP: Duration = Duration::from_secs(30); // a new leader may need this long to open the catalog
const CUT_OFF: Duration = Duration::from_secs(15); // a leader that hasn't reached the bucket this long steps aside
pub const CUT_OFF_HEADER: &str = "x-pondra-cut-off"; // (a heartbeat's answer from a leader cut off)
pub const CUT_OFF_SAYS: &str = "this node leads but can't reach the bucket: another node will lead in a moment, ask it";

#[derive(Serialize, Deserialize, Clone)]
pub struct Term {
    pub n: u64,
    pub addr: String, // the leader of this term
}

pub struct Cluster {
    pub addr: String, // a node is known by its address
    pub reader: bool, // read-only nodes never lead, run tasks or accept writes
    pub leader: Term, // fixed for this process's lifetime: a new leader means a restart
    beats: Mutex<BTreeMap<String, Instant>>, // leader: follower -> last heartbeat
    view: Mutex<Vec<String>>,                // follower: live nodes, as told by the leader
    last_ok: Mutex<Instant>,                 // follower: last heartbeat the leader answered
    heard: std::sync::atomic::AtomicBool,    // follower: the leader has answered us at least once
    pub shard_runs: std::sync::atomic::AtomicU64, // task shards this node has run (for /stats)
    cut_off: std::sync::atomic::AtomicBool,       // leader: it can't reach the bucket (`keep_alive`)
}

impl Cluster {
    /// Follow a live leader; lead if nobody leads (or the latest term is ours: we're restarting).
    pub async fn join(store: &Store, addr: &str, reader: bool) -> Result<Arc<Cluster>> {
        let leader = loop {
            match latest(store).await? {
                Some(t) if t.addr == addr || reader => break t, // ours, or we only read anyway
                None if reader => break Term { n: 0, addr: String::new() },
                Some(t) if alive(store, &t).await && !t.addr.is_empty() => break t,
                Some(t) if alive(store, &t).await => tokio::time::sleep(Duration::from_secs(1)).await, // a `pondra sql` INSERT is recording: wait
                t => {
                    if let Some(t) = claim(store, t.map_or(1, |t| t.n + 1), addr).await? {
                        break t; // (or someone else just did: look again)
                    }
                }
            }
        };
        let view = Mutex::new(vec![]); // a follower runs no shards until the leader lists it
        let last_ok = Mutex::new(Instant::now() + STARTUP); // until we first hear from the leader
        let (beats, shard_runs, heard) = (Default::default(), Default::default(), Default::default());
        Ok(Arc::new(Cluster { addr: addr.into(), reader, leader, beats, view, last_ok, heard, shard_runs, cut_off: Default::default() }))
    }

    pub fn is_leader(&self) -> bool { self.leader.addr == self.addr }

    /// Live members, sorted: shard `s` of a task runs on `nodes[s % nodes.len()]`.
    pub fn nodes(&self) -> Vec<String> {
        if !self.is_leader() {
            return if self.leader_ok() { self.view.lock().unwrap().clone() } else { vec![] }; // cut off: run no shards
        }
        let beats = self.beats.lock().unwrap();
        let mut nodes: Vec<String> = beats.iter().filter(|(_, t)| t.elapsed() < LEASE).map(|(a, _)| a.clone()).collect();
        nodes.push(self.addr.clone());
        nodes.sort();
        nodes
    }

    pub fn runs_shard(&self, shard: u32) -> bool {
        let nodes = self.nodes();
        nodes.iter().position(|n| *n == self.addr).is_some_and(|p| shard as usize % nodes.len() == p)
    }

    /// Leader side of a heartbeat: our term and the live members, or nothing while this leader
    /// can't reach the bucket (the follower takes over: `follow`).
    pub fn beat(&self, addr: String) -> Option<(u64, Vec<String>)> {
        self.beats.lock().unwrap().insert(addr, Instant::now());
        (!self.unreached()).then(|| (self.leader.n, self.nodes()))
    }

    /// Has this node heard from the leader within the lease? Peers ask before taking over.
    pub fn leader_ok(&self) -> bool { self.is_leader() || self.last_ok.lock().unwrap().elapsed() < LEASE }

    /// What peers asking `/cluster/leader` get: our leader's term, and whether we still hear it.
    pub fn leader_status(&self) -> (u64, bool) { (self.leader.n, self.leader_ok() && !self.unreached()) }

    /// Leader: has it gone `CUT_OFF` without reaching the bucket, with another node there to take
    /// over? It turns requests away then. A leader alone serves what it can, as it always did:
    /// nobody else can lead, and its writes wait for the bucket.
    pub fn cut_off(&self) -> bool { self.unreached() && self.nodes().len() > 1 }

    fn unreached(&self) -> bool { self.cut_off.load(std::sync::atomic::Ordering::Relaxed) }

    /// Leader: "still here" in the bucket every 10 s (`cluster/alive/{n}`), for machines outside
    /// the cluster. One that hasn't reached its bucket for `CUT_OFF` (no mark written, no answer
    /// to anything else: `budget::answered`; slow is still there) can't commit, though its
    /// followers may reach it and the bucket both: it answers their heartbeats that it is cut off,
    /// and one that has reached the bucket for a few seconds takes the next term (a new leader
    /// fences it), instead of every write waiting for as long as its link to the bucket is down.
    /// Once it reaches the bucket again it follows whoever leads now, or leads on if nobody took
    /// over (the bucket was down for everyone). Meanwhile, if another node is there to lead, it
    /// turns requests away (`cut_off`: `server::guard`, `audit::statement`): it can't commit, and
    /// what it reads may be stale.
    pub fn keep_alive(self: Arc<Self>, store: Store, lake: &str) {
        let marked = Arc::new(std::sync::atomic::AtomicU64::new(crate::log::now_ms()));
        let (me, m) = (self.clone(), marked.clone());
        crate::panics::spawn(async move {
            loop {
                let ok = mark_alive(&store, me.leader.n).await.is_ok();
                if ok {
                    m.store(crate::log::now_ms(), std::sync::atomic::Ordering::Relaxed);
                    // (a newer term claimed meanwhile: cut off a moment ago, or paused for longer
                    // than a lease. SlateDB's fencing stops our commits; this stops our reads too.)
                    if store.head(&Path::from(format!("cluster/term/{:020}", me.leader.n + 1))).await.is_ok() {
                        restart(&format!("term {} has a newer leader", me.leader.n + 1));
                    }
                }
                tokio::time::sleep(Duration::from_secs(if ok { 10 } else { 1 })).await; // (C5: a key at most once a second)
            }
        });
        let bucket = crate::store::bucket_url(lake);
        crate::panics::spawn(async move {
            let mut cut = false;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let heard = marked.load(std::sync::atomic::Ordering::Relaxed).max(bucket.as_deref().map_or(0, crate::budget::answered));
                if cut != (crate::log::now_ms().saturating_sub(heard) > CUT_OFF.as_millis() as u64) {
                    cut = !cut;
                    self.cut_off.store(cut, std::sync::atomic::Ordering::Relaxed);
                    eprintln!("{}", if cut { "this leader can't reach the bucket: it tells its followers, and one that reaches it takes over" } else { "this leader reaches the bucket again, and still leads" });
                }
            }
        });
    }

    /// Follower loop: heartbeat the leader; if it's gone for `LEASE`, claim the next term.
    pub fn follow(self: Arc<Self>, store: Store) {
        crate::panics::spawn(async move {
            let mut reached: Option<Instant> = None; // (since when we reach the bucket the leader says it can't)
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let url = crate::tls::url(&format!("{}/cluster/beat?from={}", self.leader.addr, self.addr));
                let answer = http().post(url).timeout(Duration::from_secs(2)).send().await;
                if answer.as_ref().is_ok_and(|r| r.headers().contains_key(CUT_OFF_HEADER)) {
                    // The leader says it can't reach the bucket: no lease to wait out, no peer to
                    // ask. Take the next term once this node has reached the bucket for a few
                    // seconds (claiming needs it): if the bucket was down for everyone, the leader
                    // reaches it again in those seconds, and keeps leading.
                    match latest(&store).await {
                        Ok(Some(t)) if t.n > self.leader.n => restart(&format!("term {} has a newer leader", t.n)),
                        Ok(_) if reached.get_or_insert_with(Instant::now).elapsed() >= Duration::from_secs(3) => {
                            if claim(&store, self.leader.n + 1, &self.addr).await.is_ok() {
                                restart("the leader can't reach the bucket: the next term was claimed");
                            }
                        }
                        Ok(_) => {}
                        Err(_) => reached = None, // (nor can we: wait)
                    }
                    continue;
                }
                reached = None;
                match answer.and_then(|r| r.error_for_status()) {
                    Ok(r) => {
                        let (term, nodes): (u64, Vec<String>) = r.json().await.unwrap_or_default();
                        if term != self.leader.n {
                            restart("a new leader answers at the leader's address");
                        }
                        *self.view.lock().unwrap() = nodes;
                        *self.last_ok.lock().unwrap() = Instant::now();
                        self.heard.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    // A member that lost the leader takes over after the lease (if no peer still
                    // hears it). One that never reached it — say, outside the cluster's network —
                    // only once the leader's mark in the bucket is stale: it never deposes a live one.
                    Err(_) if (!self.leader_ok() && self.heard.load(std::sync::atomic::Ordering::Relaxed)) || !alive(&store, &self.leader).await => match latest(&store).await {
                        Ok(Some(t)) if t.n > self.leader.n => restart(&format!("term {} has a newer leader", t.n)), // follow it
                        Ok(_) if self.peer_sees_leader().await => {}     // only our link to the leader is down
                        Ok(_) => {
                            claim(&store, self.leader.n + 1, &self.addr).await.ok(); // we win the term, or someone else does
                            restart("the leader stopped answering (and no peer hears it): the next term was claimed");
                        }
                        Err(_) => {} // can't reach the bucket either: wait
                    },
                    Err(_) => {}
                }
            }
        });
    }

    /// Read-only nodes: no heartbeat, no vote, no takeover. They only notice when leadership
    /// moves, and restart to follow the new leader's commit stream (see `mirror`).
    pub fn watch_leader(self: Arc<Self>, store: Store, streamed: bool) {
        crate::panics::spawn(async move {
            let mut gone = 0;
            loop {
                // While the leader's commit stream is down, look for a new leader every second
                // (it is elected within the lease), not every fifteen: the reads are fresh again
                // about as soon as the followers' are. (A few bucket reads a second, only then.)
                for _ in 0..15 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if STREAM_DOWN.load(std::sync::atomic::Ordering::Relaxed) && matches!(latest(&store).await, Ok(Some(t)) if t.n != self.leader.n) {
                        restart("another leads");
                    }
                }
                // Following a leader that has been gone for two checks: reopen reading the
                // catalog's WAL as well, or what it committed in its last seconds (not yet in the
                // files our view reads) would stay invisible until a new leader appears.
                gone = if streamed && !self.leader_alive().await { gone + 1 } else { 0 };
                if gone >= 2 || matches!(latest(&store).await, Ok(Some(t)) if t.n != self.leader.n) {
                    restart("the leader is gone or another leads");
                }
            }
        });
    }

    /// Does the leader of our term answer? (A read-only node follows its commit stream only then.)
    pub async fn leader_alive(&self) -> bool {
        let r = http().get(crate::tls::url(&format!("{}/cluster/leader", self.leader.addr))).timeout(Duration::from_secs(2)).send().await;
        match r {
            Ok(r) => r.json::<(u64, bool)>().await.is_ok_and(|(term, _)| term == self.leader.n),
            Err(_) => false,
        }
    }

    /// Does another member still hear our leader (same term)?
    async fn peer_sees_leader(&self) -> bool {
        let peers = self.view.lock().unwrap().clone(); // the last member list we were given
        for peer in peers.iter().filter(|p| **p != self.addr && **p != self.leader.addr) {
            let ok = http().get(crate::tls::url(&format!("{peer}/cluster/leader"))).timeout(Duration::from_secs(1)).send().await;
            if let Ok(r) = ok {
                if r.json::<(u64, bool)>().await.is_ok_and(|(term, ok)| ok && term == self.leader.n) {
                    return true;
                }
            }
        }
        false
    }
}

/// A node that just started under a leader: what the leader has committed, then until this node
/// holds it too (`Lake::caught_up` waits on it; 10 s at most each way). Restarted after a
/// failover, its catalog view lacks what the new leader took over from the old one's WAL until the
/// new leader flushes it, a moment after it leads.
pub fn catch_up(lake: Arc<Lake>, leader: String) {
    lake.caught.send_replace(false);
    crate::panics::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let url = crate::tls::url(&format!("{leader}/cluster/visible"));
        let mut upto = None;
        while upto.is_none() && tokio::time::Instant::now() < deadline {
            upto = async { http().get(&url).timeout(Duration::from_secs(2)).send().await?.error_for_status()?.json::<u64>().await }.await.ok();
            if upto.is_none() {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        if let Some(upto) = upto {
            let mut hwm = lake.hwm.subscribe();
            let _ = tokio::time::timeout(Duration::from_secs(10), async { while lake.visible() < upto { let _ = tokio::time::timeout(Duration::from_millis(50), hwm.changed()).await; } }).await;
        }
        lake.caught.send_replace(true);
    });
}

/// Follower or read-only node: follow the leader's commit stream (see `store.rs`). If it breaks,
/// reconnect; meanwhile our own catalog view keeps us correct, just a little behind. A follower
/// (`replica`) of a leader that replicates commits also keeps every change on local disk and
/// says so: that is what lets the leader acknowledge a write before the bucket has it.
/// Is the leader's commit stream down (`mirror`)? A read-only node then looks for a new leader.
static STREAM_DOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn mirror(lake: Arc<Lake>, leader: String, me: String, replica: Option<Arc<ReplicaLog>>) {
    // Acks, coalesced: however many changes arrive meanwhile, one request says "up to here".
    let (held, mut to_ack) = tokio::sync::watch::channel((0u64, 0u64, 0u64)); // term, first, last
    let l = leader.clone();
    crate::panics::spawn(async move {
        while to_ack.changed().await.is_ok() {
            let (term, first, upto) = *to_ack.borrow_and_update();
            let url = crate::tls::url(&format!("{l}/cluster/ack?from={me}&term={term}&first={first}&upto={upto}"));
            let _ = http().post(url).timeout(Duration::from_secs(2)).send().await;
        }
    });
    crate::panics::spawn(async move {
        let mut last = 0; // the last change we got (a reconnect replays some we have)
        loop {
            if let Ok(r) = http().get(crate::tls::url(&format!("{leader}/cluster/log"))).send().await.and_then(|r| r.error_for_status()) {
                STREAM_DOWN.store(false, std::sync::atomic::Ordering::Relaxed);
                let (mut body, mut buf, mut term) = (r.bytes_stream(), bytes::BytesMut::new(), None);
                while let Some(Ok(chunk)) = body.next().await {
                    buf.extend_from_slice(&chunk);
                    while let Ok(Some(f)) = Frame::take(&mut buf) {
                        match (f, &replica) {
                            (Frame::Start { term: t, replicated }, log) => {
                                term = replicated.then_some(t);
                                log.iter().for_each(|log| log.start(t));
                            }
                            (Frame::Change(d), _) if d.id <= last => {}
                            (Frame::Change(d), log) => {
                                last = d.id;
                                if let (Some(log), Some(t)) = (log, term) {
                                    match log.hold(t, &d) {
                                        Ok(Some((first, upto))) => drop(held.send_replace((t, first, upto))),
                                        Ok(None) => {} // a newer leader exists: never ack this one again
                                        Err(e) => eprintln!("keeping a replica: {e}"),
                                    }
                                }
                                lake.hold(d);
                            }
                            (Frame::Committed(upto), _) => lake.commit_upto(upto),
                            (Frame::Durable(upto), log) => log.iter().for_each(|log| log.prune(upto)),
                        }
                    }
                }
            }
            STREAM_DOWN.store(true, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

/// An HTTP client that carries no token of its own: the process serving a folder of databases (`dbserver.rs`) passes each client's on.
pub fn http_bare() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| reqwest::Client::builder().build().unwrap_or_default())
}

/// One HTTP client (connection pool) for all node-to-node traffic. It carries this process's
/// token: a node's is the admin token, else the lake's own key for its nodes (`users::node_key`,
/// known once the lake is open: the client is made again then); a `pondra sql` writer's is
/// `PONDRA_TOKEN`.
pub fn http() -> reqwest::Client {
    static CLIENT: std::sync::Mutex<Option<(Option<String>, reqwest::Client)>> = std::sync::Mutex::new(None);
    let token = std::env::var("PONDRA_TOKEN").or_else(|_| std::env::var("PONDRA_ADMIN_TOKEN")).or_else(|_| std::env::var("PONDRA_NODE_KEY")).ok();
    let mut client = CLIENT.lock().unwrap();
    if let Some((t, c)) = client.as_ref().filter(|(t, _)| *t == token) {
        let _ = t;
        return c.clone();
    }
    let made = {
        let headers: reqwest::header::HeaderMap = token.iter().filter_map(|t| format!("Bearer {t}").parse().ok()).map(|v| (reqwest::header::AUTHORIZATION, v)).collect();
        let client = |b: reqwest::ClientBuilder| crate::tls::client(b).default_headers(headers.clone()).build(); // (HTTPS between nodes: `tls.rs`)
        client(reqwest::Client::builder()).unwrap_or_else(|e| {
            // (a minimal container image with no CA certificates: nodes talk plain HTTP anyway)
            eprintln!("HTTPS calls out will fail: {e} (install the ca-certificates package)");
            client(reqwest::Client::builder().tls_certs_only([])).expect("an HTTP client")
        })
    };
    *client = Some((token, made.clone()));
    made
}

/// The newest term, if any.
pub async fn latest(store: &Store) -> Result<Option<Term>> {
    let metas: Vec<_> = store.list(Some(&Path::from("cluster/term"))).try_collect().await?;
    let Some(m) = metas.iter().max_by_key(|m| m.location.to_string()) else { return Ok(None) };
    Ok(Some(serde_json::from_slice(&store.get(&m.location).await?.bytes().await?)?))
}

/// Try to become leader of term `n`: put-if-absent, so at most one node succeeds. (`addr` is
/// empty for a `pondra sql` INSERT recording its files: nothing to follow, just wait for it.)
pub async fn claim(store: &Store, n: u64, addr: &str) -> Result<Option<Term>> {
    let term = Term { n, addr: addr.into() };
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    match store.put_opts(&Path::from(format!("cluster/term/{n:020}")), serde_json::to_vec(&term)?.into(), opts).await {
        Ok(_) => mark_alive(store, n).await.map(|_| Some(term)),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The holder of term `n` is still here (the leader: every 10 s).
pub async fn mark_alive(store: &Store, n: u64) -> Result<()> {
    store.put(&Path::from(format!("cluster/alive/{n:020}")), Vec::<u8>::new().into()).await?;
    Ok(())
}

/// Has the holder of term `t` marked itself alive in the last 30 s?
pub async fn alive(store: &Store, t: &Term) -> bool {
    match store.head(&Path::from(format!("cluster/alive/{:020}", t.n))).await {
        Ok(m) => (crate::log::now_ms() as i64 - m.last_modified.timestamp_millis()) < 30_000,
        Err(_) => false,
    }
}

/// A one-off writer is done: whoever comes next doesn't wait for its mark to go stale.
pub async fn release(store: &Store, n: u64) {
    let _ = store.delete(&Path::from(format!("cluster/alive/{n:020}"))).await;
}

/// Re-run this same binary with the same arguments: the new process re-reads its role.
/// (By the path it was started with: if the binary was upgraded in place, the new one starts.)
pub fn restart(why: &str) -> ! {
    eprintln!("restarting to rejoin the cluster: {why}");
    let mut args = std::env::args_os();
    let mut cmd = std::process::Command::new(args.next().expect("argv[0]"));
    cmd.args(args);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        panic!("restart failed: {}", cmd.exec()); // replaces this process; only returns on failure
    }
    #[cfg(not(unix))]
    if std::env::var_os("PONDRA_SUPERVISED").is_some() {
        std::process::exit(75); // (`pondra service`'s supervisor starts it again: one node, as exec keeps it elsewhere)
    }
    #[cfg(not(unix))]
    match cmd.spawn() {
        // No exec() on Windows: start the replacement, then leave (it binds our address once we're gone).
        Ok(_) => std::process::exit(0),
        Err(e) => panic!("restart failed: {e}"),
    }
}
