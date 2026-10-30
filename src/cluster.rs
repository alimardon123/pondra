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
//!   a process on the same machine knows at once that a killed claimer is gone. Another machine
//!   has no file to look at: it rings the inbox's bell, which a claimer answers with a mark within
//!   a second or two (`listen`), and one that doesn't answer in `HEARD` is gone (`deaf`). A leader
//!   killed and started again takes the next term: only its own restart takes its term back.
use crate::members::Members;
use crate::node::Guest;
use crate::replica::ReplicaLog;
use crate::store::{Frame, Lake, Store};
use crate::terms::{alive, claim, deaf, gone_here, latest, listen, lock_dir, lock_path, mark_alive, open_lock, say, stepped_down, superseded, Term, HELD, LEASE};
use anyhow::Result;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const STARTUP: Duration = Duration::from_secs(30); // a new leader may need this long to open the catalog
const CUT_OFF: Duration = Duration::from_secs(15); // a leader that hasn't reached the bucket this long steps aside
pub const CUT_OFF_HEADER: &str = "x-pondra-cut-off"; // (a heartbeat's answer from a leader cut off)

pub struct Cluster {
    pub addr: String, // a node is known by its address
    pub reader: bool, // read-only nodes never lead, run tasks or accept writes
    pub leader: Term, // fixed for this process's lifetime: a new leader means a restart
    beats: Mutex<BTreeMap<String, (Instant, String, u32)>>, // leader: follower -> last heartbeat, its release and format
    view: Mutex<Vec<String>>,                // follower: live nodes, as told by the leader
    last_ok: Mutex<Instant>,                 // follower: last heartbeat the leader answered
    heard: std::sync::atomic::AtomicBool,    // follower: the leader has answered us at least once
    cut_off: std::sync::atomic::AtomicBool,       // leader: it can't reach the bucket (`keep_alive`)
    others: std::sync::atomic::AtomicBool,        // leader cut off: another node was there meanwhile
    may_lead: bool, // false: our store is another node's (pondra://), which leads: we never claim a term (`served.rs`)
    /// This node serves its lake's store (`--serve-store`, read once at its start): it holds the lake's only copy,
    /// so it never steps aside (`cut_off`), and its followers reach the store through its door (`served.rs`).
    pub serves_store: bool,
    /// The term was ours before this process started, and taken back (`coming_back`): its mark is as old as the
    /// restart, so `keep_alive` marks it at once.
    taken_back: bool,
    rang: tokio::sync::watch::Sender<u64>, // leader: rings of the inbox's bell heard (`keep_alive`), which `inbox::serve` drains on
}

impl Cluster {
    /// Follow a live leader; lead if nobody leads (or the latest term is ours, and we're coming back
    /// from our own restart: `coming_back`). `may_lead` false: our store is another node's (`pondra://`),
    /// which leads its lake; we follow it, and wait for it, and never claim a term (invariant 299). `serves_store`
    /// is the node's own `--serve-store` (`served.rs`), kept for `cut_off`.
    pub async fn join(store: &Store, addr: &str, reader: bool, may_lead: bool, serves_store: bool) -> Result<Arc<Cluster>> {
        let (mut said, mut taken_back) = (false, false);
        let leader = loop {
            let latest_term = match latest(store).await {
                Ok(t) => t,
                // (the holder went away since it answered: look again, rather than end the node)
                Err(_) if !may_lead => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match latest_term {
                Some(t) if reader => break t, // (we only read anyway)
                None if reader => break Term { n: 0, addr: String::new(), lock: String::new(), bell: false },
                Some(t) if !may_lead && !t.addr.is_empty() && alive(store, &t).await => break t,
                _ if !may_lead => {
                    // (the node that serves this store leads it: wait for it, never take its place)
                    if !std::mem::replace(&mut said, true) {
                        eprintln!("waiting for the node that serves {store} to lead it");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                // (ours. A term that stepped down is over, and taken again it would look dead to
                // everyone (`alive`): a new one, below. One we're coming back to after our own restart
                // is taken back: the exec let go of its lock)
                Some(t) if t.addr == addr && !stepped_down(store, t.n).await => {
                    if coming_back(&t) {
                        relock(&t);
                        taken_back = true;
                        break t;
                    }
                    if let Some(t) = claim(store, t.n + 1, addr).await? {
                        break t; // (or someone else just did: look again)
                    }
                }
                // (a leader elsewhere that doesn't answer from here, and didn't mark itself when we rang, is gone: the next
                // term, below. Following it would wait for it instead)
                Some(t) if alive(store, &t).await && !t.addr.is_empty() => {
                    if answers(&t).await || !deaf(store, &t).await {
                        break t;
                    }
                }
                Some(t) if alive(store, &t).await && !deaf(store, &t).await => tokio::time::sleep(Duration::from_secs(1)).await, // a `pondra sql` INSERT is recording: wait
                t => {
                    if let Some(t) = claim(store, t.map_or(1, |t| t.n + 1), addr).await? {
                        break t; // (or someone else just did: look again)
                    }
                }
            }
        };
        Ok(made(addr, leader, reader, may_lead, serves_store, taken_back))
    }

    /// A guest's cluster (`node.rs`: a program's node for its own statements, which ends after them). Not `join`:
    /// its "the latest term is mine" would take back another guest's stale empty-address term, and its loops restart by
    /// exec without the job. It asks the lake's leader as `write::deliver` does. A live leader that answers is followed,
    /// as a follower that never beats (so it is never listed, and gets no work). A live leader that doesn't answer from
    /// here is the reads of a `Reads` guest, and `None` for a `Writes` one (the caller goes through the bucket's inbox;
    /// `PONDRA_NO_DIRECT` makes a write take an answering leader for that). A term with no address that is alive is a
    /// one-off writer recording: a read reads beside it, a write waits for it. With nobody alive, a read reads, and a
    /// write claims the next term with no address and leads it (invariant 17). `may_lead` false is a store another
    /// node serves (`pondra://`): it is never led, so a write whose holder doesn't answer is an error, not a wait.
    /// A lake on a read-only reach (`write::across`) is refused by `Node::start` before this runs.
    pub async fn guest(store: &Store, guest: Guest, may_lead: bool) -> Result<Option<Arc<Cluster>>> {
        let writes = guest == Guest::Writes;
        let not_led = || anyhow::anyhow!("the node that serves this store doesn't lead it or answer from here, and a write can't go another way: start it, or read");
        loop {
            match latest(store).await? {
                Some(t) if !t.addr.is_empty() && alive(store, &t).await => {
                    let follower = made("", t.clone(), false, false, false, false);
                    if (!writes || !crate::env::is_set("PONDRA_NO_DIRECT")) && follower.leader_alive().await {
                        return Ok(Some(follower));
                    }
                    return match (writes, may_lead) {
                        (false, _) => Ok(Some(made("", t, true, may_lead, false, false))),
                        (true, false) => Err(not_led()),
                        (true, true) => Ok(None),
                    };
                }
                // (another one-off writer is recording: a read reads beside it, a write waits for it to finish)
                Some(t) if alive(store, &t).await => {
                    if !writes {
                        return Ok(Some(made("", t, true, may_lead, false, false)));
                    }
                    if !deaf(store, &t).await {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                // (nobody alive: a read reads what is there, and a write claims the next term and leads it)
                t => {
                    if !writes {
                        return Ok(Some(made("", t.unwrap_or(Term { n: 0, addr: String::new(), lock: String::new(), bell: false }), true, may_lead, false, false)));
                    }
                    if !may_lead {
                        return Err(not_led());
                    }
                    if let Some(term) = claim(store, t.map_or(1, |t| t.n + 1), "").await? {
                        return Ok(Some(made("", term, false, true, false, false)));
                    }
                    // (someone else claimed it just now: look again)
                }
            }
        }
    }

    /// Leader side of a heartbeat: our term and the live members, or nothing while this leader
    /// can't reach the bucket (the follower takes over: `follow`). (`said`: the follower's release
    /// and format, `format::said`; the answer keeps its shape, which every release reads.)
    pub fn beat(&self, addr: String, said: (String, u32)) -> Option<(u64, Vec<String>)> {
        self.beats.lock().unwrap().insert(addr, (Instant::now(), said.0, said.1));
        (!self.unreached()).then(|| (self.leader.n, self.nodes()))
    }

    /// Leader: each ring of the inbox's bell this node hears (`inbox::serve` drains on it).
    pub fn rings(&self) -> tokio::sync::watch::Receiver<u64> { self.rang.subscribe() }

    /// Leader: the live members' releases, this node's too.
    pub fn releases(&self) -> BTreeMap<String, String> {
        let beats = self.beats.lock().unwrap();
        let mut out: BTreeMap<String, String> = beats.iter().filter(|(_, (t, ..))| t.elapsed() < LEASE).map(|(a, (_, v, _))| (a.clone(), if v.is_empty() { "older".into() } else { v.clone() })).collect();
        out.insert(self.addr.clone(), crate::format::VERSION.into());
        out
    }

    /// Has this node heard from the leader within the lease? Peers ask before taking over.
    pub fn leader_ok(&self) -> bool { self.is_leader() || self.last_ok.lock().unwrap().elapsed() < LEASE }

    /// What peers asking `/cluster/leader` get: our leader's term, and whether we still hear it.
    pub fn leader_status(&self) -> (u64, bool) { (self.leader.n, self.leader_ok() && !self.unreached()) }

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
            // (the claim marked it a moment ago: C5, a key at most once a second. A term taken back after a
            // restart was last marked before the restart, so it is marked a second in, not ten: invariant 17's
            // 30 s count from that mark)
            let (mut due, mut bell) = (Instant::now() + Duration::from_secs(if me.taken_back { 1 } else { 10 }), None);
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                // (a ring is marked at once: a writer that can't reach us rang, and watches for the mark, `deaf`)
                let rang = listen(&store, &mut bell).await;
                if rang {
                    me.rang.send_modify(|n| *n += 1); // (and the inbox is drained: `inbox::serve`)
                }
                if !rang && Instant::now() < due {
                    continue;
                }
                let ok = mark_alive(&store, me.leader.n).await.is_ok();
                if ok {
                    m.store(crate::log::now_ms(), std::sync::atomic::Ordering::Relaxed);
                    // (a newer term claimed meanwhile: cut off a moment ago, or paused for longer
                    // than a lease. SlateDB's fencing stops our commits; this stops our reads too.)
                    if superseded(&store, me.leader.n).await {
                        crate::node::restart("a newer term has a leader");
                    }
                }
                due = Instant::now() + Duration::from_secs(if ok { 10 } else { 0 }); // (C5: a key at most once a second, each look)
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
                if self.may_lead && answer.as_ref().is_ok_and(|r| r.headers().contains_key(CUT_OFF_HEADER)) {
                    // The leader says it can't reach the bucket: no lease to wait out, no peer to
                    // ask. Take the next term once this node has reached the bucket for a few
                    // seconds (claiming needs it): if the bucket was down for everyone, the leader
                    // reaches it again in those seconds, and keeps leading.
                    match latest(&store).await {
                        Ok(Some(t)) if t.n > self.leader.n => crate::node::restart(&format!("term {} has a newer leader", t.n)),
                        Ok(_) if reached.get_or_insert_with(Instant::now).elapsed() >= Duration::from_secs(3) => {
                            if claim(&store, self.leader.n + 1, &self.addr).await.is_ok() {
                                crate::node::restart("the leader can't reach the bucket: the next term was claimed");
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
                            crate::node::restart("a new leader answers at the leader's address");
                        }
                        // (a holder away past the lease may hold another lake now: a lake in memory started
                        // again claims the same first term, so the term alone doesn't tell them apart)
                        if !self.may_lead && !self.leader_ok() {
                            crate::node::restart("its holder answers again after the lease: it may hold another lake now (a lake in memory started again)");
                        }
                        *self.view.lock().unwrap() = nodes;
                        *self.last_ok.lock().unwrap() = Instant::now();
                        self.heard.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    // A leader that stepped down is gone for good: the next term at once.
                    Err(_) if self.may_lead && stepped_down(&store, self.leader.n).await => {
                        if !matches!(latest(&store).await, Ok(Some(t)) if t.n > self.leader.n) {
                            claim(&store, self.leader.n + 1, &self.addr).await.ok();
                        }
                        crate::node::restart("the leader stepped down");
                    }
                    // A member that lost the leader takes over after the lease (if no peer still
                    // hears it). One that never reached it — say, outside the cluster's network —
                    // only once the leader is gone (`alive`): it never deposes a live one.
                    Err(_) if self.may_lead && if self.heard.load(std::sync::atomic::Ordering::Relaxed) { !self.leader_ok() } else { !alive(&store, &self.leader).await || deaf(&store, &self.leader).await } => match latest(&store).await {
                        Ok(Some(t)) if t.n > self.leader.n => crate::node::restart(&format!("term {} has a newer leader", t.n)), // follow it
                        Ok(_) if self.peer_sees_leader().await => {}     // only our link to the leader is down
                        Ok(_) => {
                            claim(&store, self.leader.n + 1, &self.addr).await.ok(); // we win the term, or someone else does
                            crate::node::restart("the leader stopped answering (and no peer hears it): the next term was claimed");
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
                        crate::node::restart("another leads");
                    }
                }
                // Following a leader that has been gone for two checks: reopen reading the
                // catalog's WAL as well, or what it committed in its last seconds (not yet in the
                // files our view reads) would stay invisible until a new leader appears.
                gone = if streamed && !self.leader_alive().await { gone + 1 } else { 0 };
                if gone >= 2 || matches!(latest(&store).await, Ok(Some(t)) if t.n != self.leader.n) {
                    crate::node::restart("the leader is gone or another leads");
                }
            }
        });
    }

    /// Does the leader of our term answer? (A read-only node follows its commit stream only then.)
    pub async fn leader_alive(&self) -> bool { answers(&self.leader).await }

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

impl Members for Cluster {
    fn me(&self) -> &str { &self.addr }
    fn leader(&self) -> &Term { &self.leader }
    fn reader(&self) -> bool { self.reader }

    /// Live members, sorted: shard `s` of a task runs on `nodes[s % nodes.len()]`.
    fn nodes(&self) -> Vec<String> {
        if !self.is_leader() {
            return if self.leader_ok() { self.view.lock().unwrap().clone() } else { vec![] }; // cut off: run no shards
        }
        let beats = self.beats.lock().unwrap();
        let mut nodes: Vec<String> = beats.iter().filter(|(_, (t, ..))| t.elapsed() < LEASE).map(|(a, _)| a.clone()).collect();
        nodes.push(self.addr.clone());
        nodes.sort();
        nodes
    }

    /// Leader: has it gone `CUT_OFF` without reaching the bucket, with another node there to take
    /// over, now or at any time since? It turns requests away then. (One that took over beats to
    /// its own term, so after a lease this one would look alone, and held every write sent to it
    /// until its client gave up.) A leader alone all along serves what it can, as it always did:
    /// nobody else can lead, and its writes wait for the bucket.
    fn cut_off(&self) -> bool {
        // (a holder never steps aside: it is the store's only copy, and nobody else could lead; a store it
        // can't reach is for the holder to fix, while its followers wait)
        !self.serves_store && self.unreached() && (self.others.load(std::sync::atomic::Ordering::Relaxed) || self.nodes().len() > 1)
    }

    /// Leader: the newest format every live follower knows (None: no followers).
    fn least_known(&self) -> Option<u32> { self.beats.lock().unwrap().values().filter(|(t, ..)| t.elapsed() < LEASE).map(|(_, _, f)| *f).min() }

    fn as_any(&self) -> &dyn std::any::Any { self }
}

/// The node's own cluster, when `app` is a node's: a door that needs what only a node has (beats, releases, the
/// leader's status) asks here, and answers 404 when there is none.
pub fn of(app: &crate::app::App) -> Option<&Cluster> { app.members.as_any().downcast_ref::<Cluster>() }

/// Does term `t`'s leader answer from here, as its leader?
async fn answers(t: &Term) -> bool {
    let r = http().get(crate::tls::url(&format!("{}/cluster/leader", t.addr))).timeout(Duration::from_secs(2)).send().await;
    match r {
        Ok(r) => r.json::<(u64, bool)>().await.is_ok_and(|(term, _)| term == t.n),
        Err(_) => false,
    }
}

/// A cluster as a node's start decided it (`Cluster::join`, `Cluster::guest`): the term it follows or leads, and what it is.
fn made(addr: &str, leader: Term, reader: bool, may_lead: bool, serves_store: bool, taken_back: bool) -> Arc<Cluster> {
    let view = Mutex::new(vec![]); // a follower runs no shards until the leader lists it
    // (a node that never leads gets no startup grace: a holder that changed under its name shows at
    // its first answer, not 30 s later, so the restart check in `follow` can fire)
    let last_ok = Mutex::new(if may_lead { Instant::now() + STARTUP } else { Instant::now() }); // until we first hear from the leader
    let (beats, heard) = (Default::default(), Default::default());
    Arc::new(Cluster { addr: addr.into(), reader, leader, beats, view, last_ok, heard, cut_off: Default::default(), others: Default::default(), may_lead, serves_store, taken_back, rang: tokio::sync::watch::Sender::new(0) })
}

/// A node that just started under a leader, reading the commit stream (its view reads no WAL):
/// what the leader has committed, then until this node holds it too (`Lake::caught_up` waits on
/// it, and refuses meanwhile). Restarted after a failover, its catalog view lacks what the new
/// leader took over from the old one's WAL until the new leader flushes it, a moment after it
/// leads. A new leader answers only once it has recovered what its followers held (up to 20 s,
/// `replica::recover`), and an answer from an older catalog given up waiting sooner is what made a
/// table just made "not found". After 30 s the leader is waited for as long as it holds its term:
/// one that keeps its mark but doesn't answer is still opening the lake (replaying its catalog's
/// WAL took one 11.5 minutes on R2 on 10-25), and this node's own catalog lacks what it took over,
/// so reads answered from it went back 26 minutes (reviews/soak-day-4205277.md). A leader that never
/// answers loses its term to its followers (`follow`), and this node starts again.
pub fn catch_up(lake: Arc<Lake>, store: Store, leader: Term) {
    lake.caught.send_replace(false);
    crate::panics::spawn(async move {
        let (since, mut said) = (tokio::time::Instant::now(), 0);
        let url = crate::tls::url(&format!("{}/cluster/visible", leader.addr));
        let upto = loop {
            if let Ok(upto) = async { http().get(&url).timeout(Duration::from_secs(2)).send().await?.error_for_status()?.json::<u64>().await }.await {
                break Some(upto);
            }
            if since.elapsed() < Duration::from_secs(30) {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            if !alive(&store, &leader).await {
                break None;
            }
            let mins = since.elapsed().as_secs() / 60;
            if mins > said {
                said = mins;
                eprintln!("the leader at {} holds its term but hasn't answered in {mins} min (it is opening the lake): reads here wait for it", leader.addr);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
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
            None => {
                let gone = tokio::time::Instant::now() + Duration::from_secs(60);
                while tokio::time::Instant::now() < gone && !alive(&store, &leader).await {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                eprintln!("the leader at {} is gone and nobody took over in 60 s: answering from this node's own catalog", leader.addr);
            }
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

/// One HTTP client (connection pool) for all node-to-node traffic. It carries this process's
/// token: a node's is the admin token, else the lake's own key for its nodes (`users::node_key`,
/// known once the lake is open: the client is made again then); a `pondra sql` writer's is
/// `PONDRA_TOKEN`.
pub fn http() -> reqwest::Client {
    crate::per_node!(static MINE: std::sync::Mutex<Option<(Option<String>, reqwest::Client)>> = std::sync::Mutex::new(None));
    // (a variable set empty is no token, as `env::get` takes it: compose passes an unset one on empty,
    // and "Bearer " sent in place of the nodes' key left a follower unable to reach its leader)
    let token = crate::env::get("PONDRA_TOKEN").or_else(|| crate::env::get("PONDRA_ADMIN_TOKEN")).or_else(crate::dial::node_key);
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

/// The node's dial: HTTP between nodes, with this process's token (`http()`).
pub struct Http;

impl crate::dial::Dial for Http {
    fn url(&self, to: &str) -> String {
        if to.contains("://") { to.to_string() } else { crate::tls::url(to) }
    }

    fn call<'a>(&'a self, to: &'a str, c: crate::dial::Call) -> futures::future::BoxFuture<'a, anyhow::Result<crate::dial::Reply>> {
        Box::pin(send_call(crate::dial::Dial::url(self, to), c))
    }

    fn unreached(&self, e: &anyhow::Error) -> bool {
        e.downcast_ref::<reqwest::Error>().is_some_and(|e| e.is_connect() || e.is_timeout())
    }
}

/// One call over `http()`: the request as the node's own calls have always sent it, and the answer as a `Reply`.
async fn send_call(url: String, c: crate::dial::Call) -> anyhow::Result<crate::dial::Reply> {
    use crate::dial::{Reply, Verb};
    let verb = match c.verb {
        Verb::Get => reqwest::Method::GET,
        Verb::Post => reqwest::Method::POST,
        Verb::Delete => reqwest::Method::DELETE,
    };
    let mut req = http().request(verb, format!("{url}{}", c.path));
    if c.json {
        req = req.header("content-type", "application/json");
    }
    if let Some(body) = c.body {
        req = req.body(body);
    }
    if let Some(d) = c.timeout {
        req = req.timeout(d);
    }
    if let Some(k) = c.bearer {
        req = req.bearer_auth(k);
    }
    if let Some(user) = c.user {
        req = req.header(crate::auth::AS_USER, user);
    }
    let res = req.send().await?;
    let status = res.status().as_u16();
    let sqlstate = res.headers().get("x-pondra-sqlstate").and_then(|v| v.to_str().ok()).map(str::to_string);
    let failed = res.error_for_status_ref().err().map(anyhow::Error::from);
    let body = futures::TryStreamExt::map_err(res.bytes_stream(), anyhow::Error::from);
    Ok(Reply::new(status, sqlstate, failed, futures::StreamExt::boxed(body)))
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
