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
use crate::store::Store;
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
    pub shard_runs: std::sync::atomic::AtomicU64, // task shards this node has run (for /stats)
}

impl Cluster {
    /// Lead if nobody leads yet or the latest term is ours (we're restarting); otherwise follow.
    pub async fn join(store: &Store, addr: &str, reader: bool) -> Result<Arc<Cluster>> {
        let leader = match latest(store).await? {
            Some(t) => t, // ours (we're restarting) or someone else's
            None if reader => Term { n: 0, addr: String::new() },
            None => match claim(store, 1, addr).await? {
                Some(t) => t,
                None => latest(store).await?.expect("someone just claimed term 1"),
            },
        };
        let view = Mutex::new(vec![]); // a follower runs no shards until the leader lists it
        let last_ok = Mutex::new(Instant::now() + STARTUP); // until we first hear from the leader
        let (beats, shard_runs) = (Default::default(), Default::default());
        Ok(Arc::new(Cluster { addr: addr.into(), reader, leader, beats, view, last_ok, shard_runs }))
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
                            restart(); // a new leader came up at the same address: rejoin
                        }
                        *self.view.lock().unwrap() = nodes;
                        *self.last_ok.lock().unwrap() = Instant::now();
                    }
                    Err(_) if !self.leader_ok() => match latest(&store).await {
                        Ok(Some(t)) if t.n > self.leader.n => restart(), // there's a newer leader: follow it
                        Ok(_) if self.peer_sees_leader().await => {}     // only our link to the leader is down
                        Ok(_) => {
                            claim(&store, self.leader.n + 1, &self.addr).await.ok(); // we win the term, or someone else does
                            restart();
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
    pub fn watch_leader(self: Arc<Self>, store: Store) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                if matches!(latest(&store).await, Ok(Some(t)) if t.n != self.leader.n) {
                    restart();
                }
            }
        });
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

/// Follower: mirror the leader's catalog commits as they happen (see `store.rs`). If the stream
/// breaks, reconnect; meanwhile our own catalog view keeps us correct, just a little behind.
pub fn mirror(lake: Arc<crate::store::Lake>, leader: String) {
    tokio::spawn(async move {
        let mut last = 0; // the last commit number we got (they're consecutive, unless we missed some)
        loop {
            if let Ok(r) = http().get(format!("http://{leader}/cluster/log")).send().await {
                let (mut body, mut buf) = (r.bytes_stream(), bytes::BytesMut::new());
                while let Some(Ok(chunk)) = body.next().await {
                    buf.extend_from_slice(&chunk);
                    while let Ok(Some(d)) = crate::store::Delta::take(&mut buf) {
                        if d.id > last {
                            // A gap: commits between our view and this one may be missing.
                            let gap = d.id > last.max(lake.cat.view_c()) + 1;
                            last = d.id;
                            lake.apply(&d, gap);
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

/// One HTTP client (connection pool) for all node-to-node traffic.
pub fn http() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// The newest term, if any.
async fn latest(store: &Store) -> Result<Option<Term>> {
    let metas: Vec<_> = store.list(Some(&Path::from("cluster/term"))).try_collect().await?;
    let Some(m) = metas.iter().max_by_key(|m| m.location.to_string()) else { return Ok(None) };
    Ok(Some(serde_json::from_slice(&store.get(&m.location).await?.bytes().await?)?))
}

/// Try to become leader of term `n`: put-if-absent, so at most one node succeeds.
async fn claim(store: &Store, n: u64, addr: &str) -> Result<Option<Term>> {
    let term = Term { n, addr: addr.into() };
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    match store.put_opts(&Path::from(format!("cluster/term/{n:020}")), serde_json::to_vec(&term)?.into(), opts).await {
        Ok(_) => Ok(Some(term)),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Re-run this same binary with the same arguments: the new process re-reads its role.
/// (By the path it was started with: if the binary was upgraded in place, the new one starts.)
pub fn restart() -> ! {
    eprintln!("restarting to rejoin the cluster");
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
