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
        Ok(Arc::new(Cluster { addr: addr.into(), reader, leader, beats, view, last_ok, heard, shard_runs }))
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

    /// Leader side of a heartbeat: our term and the live members.
    pub fn beat(&self, addr: String) -> (u64, Vec<String>) {
        self.beats.lock().unwrap().insert(addr, Instant::now());
        (self.leader.n, self.nodes())
    }

    /// Has this node heard from the leader within the lease? Peers ask before taking over.
    pub fn leader_ok(&self) -> bool { self.is_leader() || self.last_ok.lock().unwrap().elapsed() < LEASE }

    /// What peers asking `/cluster/leader` get: our leader's term, and whether we still hear it.
    pub fn leader_status(&self) -> (u64, bool) { (self.leader.n, self.leader_ok()) }

    /// Follower loop: heartbeat the leader; if it's gone for `LEASE`, claim the next term.
    pub fn follow(self: Arc<Self>, store: Store) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let url = format!("http://{}/cluster/beat?from={}", self.leader.addr, self.addr);
                match http().post(url).timeout(Duration::from_secs(2)).send().await.and_then(|r| r.error_for_status()) {
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
        tokio::spawn(async move {
            let mut gone = 0;
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
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
        let r = http().get(format!("http://{}/cluster/leader", self.leader.addr)).timeout(Duration::from_secs(2)).send().await;
        match r {
            Ok(r) => r.json::<(u64, bool)>().await.is_ok_and(|(term, _)| term == self.leader.n),
            Err(_) => false,
        }
    }

    /// Does another member still hear our leader (same term)?
    async fn peer_sees_leader(&self) -> bool {
        let peers = self.view.lock().unwrap().clone(); // the last member list we were given
        for peer in peers.iter().filter(|p| **p != self.addr && **p != self.leader.addr) {
            let ok = http().get(format!("http://{peer}/cluster/leader")).timeout(Duration::from_secs(1)).send().await;
            if let Ok(r) = ok {
                if r.json::<(u64, bool)>().await.is_ok_and(|(term, ok)| ok && term == self.leader.n) {
                    return true;
                }
            }
        }
        false
    }
}

/// Follower or read-only node: follow the leader's commit stream (see `store.rs`). If it breaks,
/// reconnect; meanwhile our own catalog view keeps us correct, just a little behind. A follower
/// (`replica`) of a leader that replicates commits also keeps every change on local disk and
/// says so: that is what lets the leader acknowledge a write before the bucket has it.
pub fn mirror(lake: Arc<Lake>, leader: String, me: String, replica: Option<Arc<ReplicaLog>>) {
    // Acks, coalesced: however many changes arrive meanwhile, one request says "up to here".
    let (held, mut to_ack) = tokio::sync::watch::channel((0u64, 0u64, 0u64)); // term, first, last
    let l = leader.clone();
    tokio::spawn(async move {
        while to_ack.changed().await.is_ok() {
            let (term, first, upto) = *to_ack.borrow_and_update();
            let url = format!("http://{l}/cluster/ack?from={me}&term={term}&first={first}&upto={upto}");
            let _ = http().post(url).timeout(Duration::from_secs(2)).send().await;
        }
    });
    tokio::spawn(async move {
        let mut last = 0; // the last change we got (a reconnect replays some we have)
        loop {
            if let Ok(r) = http().get(format!("http://{leader}/cluster/log")).send().await {
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
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

/// One HTTP client (connection pool) for all node-to-node traffic. It carries this process's
/// token: a node's is the admin token, a `pondra sql` writer's is `PONDRA_TOKEN`.
pub fn http() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        let token = std::env::var("PONDRA_TOKEN").or_else(|_| std::env::var("PONDRA_ADMIN_TOKEN")).ok();
        let headers: reqwest::header::HeaderMap = token.iter().filter_map(|t| format!("Bearer {t}").parse().ok()).map(|v| (reqwest::header::AUTHORIZATION, v)).collect();
        let client = |b: reqwest::ClientBuilder| b.default_headers(headers.clone()).build();
        client(reqwest::Client::builder()).unwrap_or_else(|e| {
            // (a minimal container image with no CA certificates: nodes talk plain HTTP anyway)
            eprintln!("HTTPS calls out will fail: {e} (install the ca-certificates package)");
            client(reqwest::Client::builder().tls_certs_only([])).expect("an HTTP client")
        })
    })
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
    match cmd.spawn() {
        // No exec() on Windows: start the replacement, then leave (it binds our address once we're gone).
        Ok(_) => std::process::exit(0),
        Err(e) => panic!("restart failed: {e}"),
    }
}
