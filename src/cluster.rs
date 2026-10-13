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
//!   A claimer also holds a lock file in this machine's temp folder (named in its term record), so
//!   a process on the same machine knows at once that a killed claimer is gone; another machine
//!   has no file to look at and waits for the mark. A leader killed and started again takes the
//!   next term: only its own restart takes its term back.
use crate::replica::ReplicaLog;
use crate::store::{Frame, Lake, Store};
use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
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
    /// The claim's lock file on the machine that claimed it (`HELD`), or empty (older releases, or a
    /// temp folder that couldn't take one): 32 hex characters, never a path.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub lock: String,
}

pub struct Cluster {
    pub addr: String, // a node is known by its address
    pub reader: bool, // read-only nodes never lead, run tasks or accept writes
    pub leader: Term, // fixed for this process's lifetime: a new leader means a restart
    beats: Mutex<BTreeMap<String, (Instant, String, u32)>>, // leader: follower -> last heartbeat, its release and format
    view: Mutex<Vec<String>>,                // follower: live nodes, as told by the leader
    last_ok: Mutex<Instant>,                 // follower: last heartbeat the leader answered
    heard: std::sync::atomic::AtomicBool,    // follower: the leader has answered us at least once
    pub shard_runs: std::sync::atomic::AtomicU64, // task shards this node has run (for /stats)
    cut_off: std::sync::atomic::AtomicBool,       // leader: it can't reach the bucket (`keep_alive`)
    others: std::sync::atomic::AtomicBool,        // leader cut off: another node was there meanwhile
}

impl Cluster {
    /// Follow a live leader; lead if nobody leads (or the latest term is ours, and we're coming back
    /// from our own restart: `coming_back`).
    pub async fn join(store: &Store, addr: &str, reader: bool) -> Result<Arc<Cluster>> {
        let leader = loop {
            match latest(store).await? {
                Some(t) if reader => break t, // (we only read anyway)
                // (ours. A term that stepped down is over, and taken again it would look dead to
                // everyone (`alive`): a new one, below. One we're coming back to after our own restart
                // is taken back: the exec let go of its lock)
                Some(t) if t.addr == addr && !stepped_down(store, t.n).await => {
                    if coming_back(&t) {
                        relock(&t);
                        break t;
                    }
                    if let Some(t) = claim(store, t.n + 1, addr).await? {
                        break t; // (or someone else just did: look again)
                    }
                }
                None if reader => break Term { n: 0, addr: String::new(), lock: String::new() },
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
        Ok(Arc::new(Cluster { addr: addr.into(), reader, leader, beats, view, last_ok, heard, shard_runs, cut_off: Default::default(), others: Default::default() }))
    }

    pub fn is_leader(&self) -> bool { self.leader.addr == self.addr }

    /// Live members, sorted: shard `s` of a task runs on `nodes[s % nodes.len()]`.
    pub fn nodes(&self) -> Vec<String> {
        if !self.is_leader() {
            return if self.leader_ok() { self.view.lock().unwrap().clone() } else { vec![] }; // cut off: run no shards
        }
        let beats = self.beats.lock().unwrap();
        let mut nodes: Vec<String> = beats.iter().filter(|(_, (t, ..))| t.elapsed() < LEASE).map(|(a, _)| a.clone()).collect();
        nodes.push(self.addr.clone());
        nodes.sort();
        nodes
    }

    pub fn runs_shard(&self, shard: u32) -> bool {
        let nodes = self.nodes();
        nodes.iter().position(|n| *n == self.addr).is_some_and(|p| shard as usize % nodes.len() == p)
    }

    /// Leader side of a heartbeat: our term and the live members, or nothing while this leader
    /// can't reach the bucket (the follower takes over: `follow`). (`said`: the follower's release
    /// and format, `format::said`; the answer keeps its shape, which every release reads.)
    pub fn beat(&self, addr: String, said: (String, u32)) -> Option<(u64, Vec<String>)> {
        self.beats.lock().unwrap().insert(addr, (Instant::now(), said.0, said.1));
        (!self.unreached()).then(|| (self.leader.n, self.nodes()))
    }

    /// Leader: the live members' releases, this node's too.
    pub fn releases(&self) -> BTreeMap<String, String> {
        let beats = self.beats.lock().unwrap();
        let mut out: BTreeMap<String, String> = beats.iter().filter(|(_, (t, ..))| t.elapsed() < LEASE).map(|(a, (_, v, _))| (a.clone(), if v.is_empty() { "older".into() } else { v.clone() })).collect();
        out.insert(self.addr.clone(), crate::format::VERSION.into());
        out
    }

    /// Leader: the newest format every live follower knows (None: no followers).
    pub fn least_known(&self) -> Option<u32> { self.beats.lock().unwrap().values().filter(|(t, ..)| t.elapsed() < LEASE).map(|(_, _, f)| *f).min() }

    /// Has this node heard from the leader within the lease? Peers ask before taking over.
    pub fn leader_ok(&self) -> bool { self.is_leader() || self.last_ok.lock().unwrap().elapsed() < LEASE }

    /// What peers asking `/cluster/leader` get: our leader's term, and whether we still hear it.
    pub fn leader_status(&self) -> (u64, bool) { (self.leader.n, self.leader_ok() && !self.unreached()) }

    /// Leader: has it gone `CUT_OFF` without reaching the bucket, with another node there to take
    /// over, now or at any time since? It turns requests away then. (One that took over beats to
    /// its own term, so after a lease this one would look alone, and held every write sent to it
    /// until its client gave up.) A leader alone all along serves what it can, as it always did:
    /// nobody else can lead, and its writes wait for the bucket.
    pub fn cut_off(&self) -> bool { self.unreached() && (self.others.load(std::sync::atomic::Ordering::Relaxed) || self.nodes().len() > 1) }

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
                    self.others.store(false, std::sync::atomic::Ordering::Relaxed);
                    eprintln!("{}", if cut { "this leader can't reach the bucket: it tells its followers, and one that reaches it takes over" } else { "this leader reaches the bucket again, and still leads" });
                }
                if cut && self.nodes().len() > 1 {
                    self.others.store(true, std::sync::atomic::Ordering::Relaxed);
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
                    // A leader that stepped down is gone for good: the next term at once.
                    Err(_) if stepped_down(&store, self.leader.n).await => {
                        if !matches!(latest(&store).await, Ok(Some(t)) if t.n > self.leader.n) {
                            claim(&store, self.leader.n + 1, &self.addr).await.ok();
                        }
                        restart("the leader stepped down");
                    }
                    // A member that lost the leader takes over after the lease (if no peer still
                    // hears it). One that never reached it — say, outside the cluster's network —
                    // only once the leader is gone (`alive`): it never deposes a live one.
                    Err(_) if if self.heard.load(std::sync::atomic::Ordering::Relaxed) { !self.leader_ok() } else { !alive(&store, &self.leader).await } => match latest(&store).await {
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

/// A node that just started under a leader, reading the commit stream (its view reads no WAL):
/// what the leader has committed, then until this node holds it too (`Lake::caught_up` waits on
/// it, and refuses meanwhile). Restarted after a failover, its catalog view lacks what the new
/// leader took over from the old one's WAL until the new leader flushes it, a moment after it
/// leads. The leader is asked for 30 s: a new one answers only once it has recovered what its
/// followers held (up to 20 s, `replica::recover`), and an answer from an older catalog given up
/// waiting sooner is what made a table just made "not found".
pub fn catch_up(lake: Arc<Lake>, store: Store, leader: Term) {
    lake.caught.send_replace(false);
    crate::panics::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let url = crate::tls::url(&format!("{}/cluster/visible", leader.addr));
        let mut upto = None;
        while upto.is_none() && tokio::time::Instant::now() < deadline {
            upto = async { http().get(&url).timeout(Duration::from_secs(2)).send().await?.error_for_status()?.json::<u64>().await }.await.ok();
            if upto.is_none() {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        match upto {
            Some(upto) => {
                let mut hwm = lake.hwm.subscribe();
                if tokio::time::timeout(Duration::from_secs(30), async { while lake.visible() < upto { let _ = tokio::time::timeout(Duration::from_millis(50), hwm.changed()).await; } }).await.is_err() {
                    eprintln!("this node's catalog hasn't reached its leader's commit {upto} in 30 s: answering from it anyway");
                }
            }
            // A leader whose mark is old by now died before we started: this node takes over, or
            // follows whoever does, in a moment, and starts again (`follow`). Answering from this
            // catalog meanwhile would miss what it committed last (a lone node killed, then the shell).
            None if !alive(&store, &leader).await => {
                let gone = tokio::time::Instant::now() + Duration::from_secs(60);
                while tokio::time::Instant::now() < gone && !alive(&store, &leader).await {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                eprintln!("the leader at {} is gone and nobody took over in 60 s: answering from this node's own catalog", leader.addr);
            }
            None => eprintln!("the leader at {} didn't answer in 30 s: answering from this node's own catalog", leader.addr),
        }
        lake.caught.send_replace(true);
    });
}

crate::per_node!(
    /// Is the leader's commit stream down (`mirror`)? A read-only node then looks for a new leader.
    static STREAM_DOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false));

/// Follower or read-only node: follow the leader's commit stream (see `store.rs`). If it breaks,
/// reconnect; meanwhile our own catalog view keeps us correct, just a little behind. A follower
/// (`replica`) of a leader that replicates commits also keeps every change on local disk and
/// says so: that is what lets the leader acknowledge a write before the bucket has it.
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

/// The nodes' key in hand before this node calls its leader for a caller (`write::on_node_as`): a
/// follower that just started may sign a user in, its catalog holding the users, a moment before the
/// loop that reads the key (`cli.rs`) has it, and a call sent without it is refused as unsigned (CI's
/// `harness.py guarded`, "a change a follower forwards…"). It waits 10 s at most, and not at all for a
/// key sealed by a master key this node doesn't have: waiting won't help that one.
pub async fn signed(lake: &crate::store::Lake) {
    if crate::env::is_set("PONDRA_TOKEN") || crate::env::is_set("PONDRA_ADMIN_TOKEN") || node_key().is_some() {
        return;
    }
    for _ in 0..40 {
        match crate::users::node_key(lake).await {
            Ok(k) => return set_node_key(k),
            Err(e) if format!("{e:#}").contains("master key") => return,
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

/// An HTTP client that carries no token of its own: the process serving a folder of databases (`dbserver.rs`) passes each client's on.
pub fn http_bare() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| reqwest::Client::builder().build().unwrap_or_default())
}

// The lake's key for its nodes (`users::node_key`), once this node knows it: what it calls the others
// with when no token is set. The node's, not the process's environment: the next node in a process
// serves another lake, whose nodes are never sent this one's key, and no child process inherits it.
crate::per_node!(static NODE_KEY: std::sync::RwLock<Option<String>> = std::sync::RwLock::new(None));

/// The key this node calls the others with: `PONDRA_NODE_KEY` as set, else the lake's own, once known.
pub fn node_key() -> Option<String> {
    crate::env::get("PONDRA_NODE_KEY").or_else(|| NODE_KEY.read().unwrap_or_else(|e| e.into_inner()).clone())
}

/// This node learned the lake's key for its nodes (`node.rs`).
pub fn set_node_key(k: String) { *NODE_KEY.write().unwrap_or_else(|e| e.into_inner()) = Some(k); }

/// One HTTP client (connection pool) for all node-to-node traffic. It carries this process's
/// token: a node's is the admin token, else the lake's own key for its nodes (`users::node_key`,
/// known once the lake is open: the client is made again then); a `pondra sql` writer's is
/// `PONDRA_TOKEN`.
pub fn http() -> reqwest::Client {
    crate::per_node!(static MINE: std::sync::Mutex<Option<(Option<String>, reqwest::Client)>> = std::sync::Mutex::new(None));
    // (a variable set empty is no token, as `env::get` takes it: compose passes an unset one on empty,
    // and "Bearer " sent in place of the nodes' key left a follower unable to reach its leader)
    let token = crate::env::get("PONDRA_TOKEN").or_else(|| crate::env::get("PONDRA_ADMIN_TOKEN")).or_else(node_key);
    let mut client = MINE.lock().unwrap();
    if let Some((t, c)) = client.as_ref().filter(|(t, _)| *t == token) {
        let _ = t;
        return c.clone();
    }
    let made = {
        let mut headers: reqwest::header::HeaderMap = token.iter().filter_map(|t| format!("Bearer {t}").parse().ok()).map(|v| (reqwest::header::AUTHORIZATION, v)).collect();
        headers.extend(crate::format::headers().into_iter().filter_map(|(k, v)| Some((reqwest::header::HeaderName::from_static(k), v.parse().ok()?)))); // (who calls: ADR-039)
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
    // (its id goes into the record: a process here can tell when we're gone; none reaches a lake in our memory)
    let lock = if crate::store::held_in_memory(store) { None } else { new_lock() };
    let term = Term { n, addr: addr.into(), lock: lock.as_ref().map(|(id, ..)| id.clone()).unwrap_or_default() };
    let lock = lock.map(|(_, path, f)| (path, f));
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    match store.put_opts(&Path::from(format!("cluster/term/{n:020}")), serde_json::to_vec(&term)?.into(), opts).await {
        Ok(_) => {
            if let Some(held) = lock {
                HELD.lock().unwrap().insert(n, held);
            }
            mark_alive(store, n).await.map(|_| Some(term))
        }
        Err(object_store::Error::AlreadyExists { .. }) => {
            discard(lock); // someone else won the term
            Ok(None)
        }
        Err(e) => {
            discard(lock);
            Err(e.into())
        }
    }
}

/// The holder of term `n` is still here (the leader: every 10 s).
pub async fn mark_alive(store: &Store, n: u64) -> Result<()> {
    store.put(&Path::from(format!("cluster/alive/{n:020}")), Vec::<u8>::new().into()).await?;
    Ok(())
}

/// Is the holder of term `t` alive? A claim made on this machine is alive while its process holds
/// its lock file; anywhere else, it is alive if it marked itself alive in the last 30 s.
pub async fn alive(store: &Store, t: &Term) -> bool {
    if gone_here(t) {
        return false; // a claimer on this machine that ended can't be alive, whatever its mark says
    }
    match store.head(&Path::from(format!("cluster/alive/{:020}", t.n))).await {
        // (a leader that stepped down isn't alive, whatever its mark says: its last mark, in flight
        // as it stepped down, can land after `release` took it away)
        Ok(m) => (crate::log::now_ms() as i64 - m.last_modified.timestamp_millis()) < 30_000 && !stepped_down(store, t.n).await,
        Err(_) => false,
    }
}

/// A one-off writer is done: whoever comes next doesn't wait for its mark to go stale, nor for its
/// lock file to be let go.
pub async fn release(store: &Store, n: u64) {
    let _ = store.delete(&Path::from(format!("cluster/alive/{n:020}"))).await;
    let held = HELD.lock().unwrap().remove(&n);
    discard(held);
}

crate::per_node!(
    /// The lock files of the terms this node claimed (or took back after a restart), each locked until
    /// the term is let go (`release`), the node's state is dropped as it stops (`reset`), or the process
    /// ends, however it ends: the OS lets go at whichever comes first. Keyed by term number. (A node
    /// holds one claim per term number: one lake at a time.)
    static HELD: Mutex<BTreeMap<u64, (PathBuf, std::fs::File)>> = Mutex::new(BTreeMap::new()));

/// This user's folder of claim lock files, in the temp folder: per user on macOS and Windows, and
/// named by the user's id on Linux, where `/tmp` is shared. Made 0700, and used only while it is
/// ours alone, so no other user can take a lock away (and make a live leader look gone). None: the
/// claims go on without locks, and the mark decides.
fn lock_dir() -> Option<PathBuf> { own_temp("pondra-terms") }

/// A folder named `name` in the temp folder that is this user's alone: named by the user's id on
/// Linux, made 0700, and used only while it is ours alone, so no other user can change what is in
/// it. None when it isn't ours (the caller decides what that means).
pub(crate) fn own_temp(name: &str) -> Option<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt};
        let uid = unsafe { libc::getuid() };
        let dir = std::env::temp_dir().join(format!("{name}-{uid}"));
        let _ = std::fs::DirBuilder::new().mode(0o700).create(&dir);
        let m = std::fs::symlink_metadata(&dir).ok()?;
        (m.is_dir() && m.uid() == uid && m.mode() & 0o077 == 0).then_some(dir)
    }
    #[cfg(not(unix))]
    {
        let dir = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&dir).ok()?;
        Some(dir)
    }
}

/// Where term `t`'s lock file would be here: its id is trusted only as 32 hex characters (it comes
/// from the bucket, so it must never become a path).
fn lock_path(t: &Term) -> Option<PathBuf> {
    let ok = t.lock.len() == 32 && t.lock.bytes().all(|b| b.is_ascii_hexdigit());
    ok.then(|| lock_dir().map(|d| d.join(format!("{}.lock", t.lock)))).flatten()
}

fn open_lock(path: &std::path::Path) -> std::io::Result<std::fs::File> { std::fs::OpenOptions::new().read(true).write(true).open(path) }

/// A new claim's lock file: a random id (written into the term record) and the file, locked by this
/// process. None when the folder can't take one: the claim goes on without it, and the next writer
/// waits for the mark, as before.
fn new_lock() -> Option<(String, PathBuf, std::fs::File)> {
    let dir = lock_dir()?;
    sweep_locks(&dir);
    let id = uuid::Uuid::new_v4().simple().to_string();
    let path = dir.join(format!("{id}.lock"));
    let f = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&path).ok()?;
    if f.try_lock().is_ok() {
        return Some((id, path, f));
    }
    discard(Some((path, f)));
    None
}

/// Removes the lock files of claims whose process has ended: files a day old that nobody holds. Best
/// effort. (The age keeps the sweep off any claim made just now.)
fn sweep_locks(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let path = e.path();
        let old = e.metadata().and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).is_some_and(|age| age > Duration::from_secs(24 * 3600));
        if !old || !path.extension().is_some_and(|x| x == "lock") {
            continue;
        }
        let Ok(f) = open_lock(&path) else { continue };
        if f.try_lock().is_ok() {
            drop(f); // (Windows can't delete a file that's open)
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Lets go of a claim's lock file and removes it (best effort).
fn discard(held: Option<(PathBuf, std::fs::File)>) {
    if let Some((path, f)) = held {
        drop(f); // (Windows can't delete a file that's open)
        let _ = std::fs::remove_file(path);
    }
}

/// What a claim's lock file says: nothing while its process runs, `restarting <ms>` from when it
/// restarts itself (`restart`) until it takes the lock back (`relock`).
fn say(f: &std::fs::File, what: &str) {
    use std::io::{Seek, Write};
    let mut f = f;
    let _ = f.set_len(0).and_then(|_| f.seek(std::io::SeekFrom::Start(0))).and_then(|_| f.write_all(what.as_bytes()));
}

/// Has the process that claimed term `t` on this machine ended? Its lock file is here, nobody holds
/// it, and it didn't say it was restarting in the last 30 s (an exec lets go of the lock, and so does
/// a restart on Windows: the claimer is coming back, so it isn't gone). A claim made on another
/// machine, or by another user, has no file here: its mark decides, as invariant 17 asks.
fn gone_here(t: &Term) -> bool {
    let Some(path) = lock_path(t) else { return false };
    if HELD.lock().unwrap().contains_key(&t.n) {
        return false; // this very process holds it
    }
    let Ok(mut f) = open_lock(&path) else { return false };
    if f.try_lock().is_err() {
        return false; // its claimer holds it (a lock taken here ends with `f`)
    }
    let mut said = String::new();
    let _ = std::io::Read::read_to_string(&mut f, &mut said);
    let restarting = said.strip_prefix("restarting ").and_then(|ms| ms.parse::<u64>().ok());
    !restarting.is_some_and(|ms| crate::log::now_ms().saturating_sub(ms) < 30_000)
}

/// Is this node coming back to its own term `t` after its own restart (an exec, Windows' new process,
/// the service supervisor's)? Then the term's lock file here is still held by the process on its way
/// out, or says `restarting`. Not when the process that claimed it ended (killed, and started again by
/// its service), nor when the file isn't here (it ran with another temp folder: a container started
/// again): taken back, the term's followers' lease ran out while the node opened the lake on a bucket,
/// one of them took the next term, and both opened the lake as leader, the first fenced by the second
/// (writes waited 12–15 s on simulated R2 for both). Such a node takes the next term as it starts. A
/// term with no lock file to look at (an older release's) is taken back, as before.
fn coming_back(t: &Term) -> bool {
    match lock_path(t) {
        Some(path) => path.exists() && !gone_here(t),
        None => t.lock.is_empty() || lock_dir().is_none(), // (no file named, or no folder here to look in: can't tell)
    }
}

/// Back after a restart, as the leader of term `t`: take its lock again (the exec, or the old
/// process's end, let go of it) and clear what it said. Windows may not have let the old process go
/// yet: then wait for its lock in the background.
fn relock(t: &Term) {
    let Some(path) = lock_path(t) else { return };
    let Ok(f) = open_lock(&path) else { return };
    let n = t.n;
    match f.try_lock() {
        Ok(()) => {
            say(&f, "");
            HELD.lock().unwrap().insert(n, (path, f));
        }
        Err(std::fs::TryLockError::WouldBlock) => {
            std::thread::spawn(move || {
                if f.lock().is_ok() {
                    say(&f, "");
                    HELD.lock().unwrap().insert(n, (path, f));
                }
            });
        }
        Err(_) => {} // the mark still covers it
    }
}

/// The leader of term `n` stops, drained (`drain.rs`): it says so (`cluster/left/{n}`), so its
/// followers take over at once instead of waiting out the lease, then releases its term.
pub async fn step_down(store: &Store, n: u64) {
    let _ = store.put_opts(&Path::from(format!("cluster/left/{n:020}")), Vec::<u8>::new().into(), PutOptions { mode: PutMode::Create, ..Default::default() }).await;
    release(store, n).await;
}

/// Has the leader of term `n` stepped down?
async fn stepped_down(store: &Store, n: u64) -> bool { store.head(&Path::from(format!("cluster/left/{n:020}"))).await.is_ok() }

/// Re-run this same binary with the same arguments: the new process re-reads its role.
/// (By the path it was started with: if the binary was upgraded in place, the new one starts.)
pub fn restart(why: &str) -> ! {
    restart_with(why, &[])
}

/// `restart`, with settings of the command line's own for the next run (`pondra sql`'s job and its
/// tries: `env.rs`), on top of what this process was given.
pub fn restart_with(why: &str, carried: &[(&str, String)]) -> ! {
    eprintln!("restarting to rejoin the cluster: {why}");
    // (our claims' lock files say we're coming back: an exec, or a new process on Windows, lets go of
    // them for a moment, and nobody here may take that for our end: `gone_here`)
    if let Ok(held) = HELD.lock() {
        held.values().for_each(|(_, f)| say(f, &format!("restarting {}", crate::log::now_ms())));
    }
    if crate::library::on() {
        crate::library::restart(why); // (in place: a library's host keeps its process)
    }
    let mut args = std::env::args_os();
    let mut cmd = std::process::Command::new(args.next().expect("argv[0]"));
    cmd.args(args).envs(crate::env::given()).envs(carried.iter().map(|(k, v)| (k, v)));
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
