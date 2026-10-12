//! Row locks, kept by the leader (ADR-067 §3–§7). A transaction's write locks the rows it changes;
//! a write that finds a row held waits, first come first served, until its holder commits or ends.
//! An autocommitted write registers as appending to its table once its locks are its own (`take`'s
//! `flight`), and a writer that changes rows under the lake's lock closes the table first (`close`,
//! the gate): the appends in flight land before it reads the rows, and no new one enters until it commits.
//!
//! The leader keeps them in memory, one table per lake, and nothing about them is written down: a
//! leader that takes over starts with none. A commit still checks what it changed (`txn::commit_here`),
//! so a lock lost to a lapsed lease (a holder silent for `PONDRA_LOCK_LEASE_SECS`) can only refuse a
//! commit, never lose an update.
//!
//! Each transaction waits for one lock at a time, so the waits form chains. A wait that would close a
//! loop is refused at once (40P01). A `lock_timeout` bounds a wait (55P03). The table itself is sync
//! code (`Table`), so its rules are unit-tested without a runtime; `take` and `wait_for` are its async
//! doors. The writers' side (`passes`, `take_on`, `Hold`, `close`) is at the end of this file.
use crate::codes::coded;
use crate::store::{Lake, TableMeta};
use crate::txn::{Keys, Reads};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// How often a waiter looks again at its lock: a lapsed lease gives way within this.
const TICK: Duration = Duration::from_millis(50);

/// How much longer than a `lock_timeout` a follower waits for the leader's answer before it asks again.
const MARGIN: Duration = Duration::from_secs(2);

/// Most row locks one write takes (more than this, its rows go through the leader's change path, ADR-067 §3).
pub const LOCK_MAX: usize = 10_000;

/// One lock: who holds it (its lease is its owner's: `Table::leases`), and who waits for it, in order.
#[derive(Default)]
struct Lock {
    holder: Option<String>,
    queue: VecDeque<Waiter>,
}

/// A place in a lock's queue: its owner, the token that names this place (a retry of the same owner has
/// another one), and the channel that grants it.
struct Waiter {
    owner: String,
    token: u64,
    grant: oneshot::Sender<()>,
}

/// One lake's locks. Each owner (a transaction, or an autocommitted write) has one lease: its locks last while
/// it asks or is renewed, and a lapsed one gives its keys to the next in line. An owner that let its locks go
/// is refused for a lease's time, so a request still on its way can't take them back.
pub struct Table {
    lease: Duration,
    locks: HashMap<String, Lock>,
    held: HashMap<String, HashSet<String>>, // owner → the keys it holds
    leases: HashMap<String, Instant>,       // owner → until when its locks last
    ended: HashMap<String, Instant>,        // owner → until when it is refused: it let its locks go
    waiting: HashMap<String, (String, u64)>, // owner → the key it waits for, and its place's token
    flying: HashMap<String, HashSet<String>>, // table → the owners appending to it, past their row locks (`enter`)
    closed: HashMap<String, usize>,         // table → the writers closing it (`close`): no new appender enters
    tokens: u64,
    swept: Instant,
}

/// The answer to asking for a lock.
pub enum Ask {
    Granted,
    Deadlock,
    Wait(oneshot::Receiver<()>, u64),
    Ended,
}

impl Table {
    pub fn new(lease: Duration) -> Self {
        Table {
            lease,
            locks: HashMap::new(),
            held: HashMap::new(),
            leases: HashMap::new(),
            ended: HashMap::new(),
            waiting: HashMap::new(),
            flying: HashMap::new(),
            closed: HashMap::new(),
            tokens: 0,
            swept: Instant::now(),
        }
    }

    /// `owner` asks for `key` at `now`: granted, refused (its wait would close a loop, or it let its locks go), or
    /// queued behind the holder and the waiters ahead of it. Asking renews its lease. A place it already has (a
    /// retry after its request timed out) is replaced, never doubled.
    pub fn ask(&mut self, owner: &str, key: &str, now: Instant) -> Ask {
        if self.is_ended(owner, now) {
            return Ask::Ended;
        }
        self.renew(owner, now);
        self.settle(key, now);
        if self.holds(key, owner) {
            return Ask::Granted;
        }
        if self.locks.get(key).is_none_or(|l| l.holder.is_none()) {
            self.grant(owner, key, now);
            return Ask::Granted;
        }
        self.unwait(owner, now);
        let blockers = self.ahead(key, owner, now);
        if self.loops(blockers, owner, now) {
            return Ask::Deadlock;
        }
        self.tokens += 1;
        let (token, (tx, rx)) = (self.tokens, oneshot::channel());
        self.locks.entry(key.to_string()).or_default().queue.push_back(Waiter { owner: owner.to_string(), token, grant: tx });
        self.waiting.insert(owner.to_string(), (key.to_string(), token));
        Ask::Wait(rx, token)
    }

    /// `owner` lets go of every lock it holds, and of its place in any queue (its commit or end). It is refused
    /// for a lease's time after that.
    pub fn release(&mut self, owner: &str, now: Instant) {
        for key in self.held.remove(owner).unwrap_or_default() {
            if let Some(lock) = self.locks.get_mut(&key) {
                if lock.holder.as_deref() == Some(owner) {
                    lock.holder = None;
                }
            }
            self.settle(&key, now);
        }
        self.leases.remove(owner);
        self.ended.insert(owner.to_string(), now + self.lease);
        self.unwait(owner, now); // (its transaction is over, so a wait it has fails)
        self.leave_all(owner);
    }

    /// `owner` stops appending to `table` (`leave`), or to every table (`release`). A closer waiting for its append goes on.
    fn leave_all(&mut self, owner: &str) {
        self.flying.values_mut().for_each(|owners| {
            owners.remove(owner);
        });
        self.flying.retain(|_, owners| !owners.is_empty());
    }

    /// `owner` registers as appending to `table`, now that its row locks are its own (`take`). One already appending
    /// goes on; a new one waits while a writer has `table` closed (`close`), and `false` says so. Asking renews its lease.
    pub fn enter(&mut self, owner: &str, table: &str, now: Instant) -> bool {
        let appending = self.flying.get(table).is_some_and(|owners| owners.contains(owner));
        if !appending && self.closed.get(table).is_some_and(|n| *n > 0) {
            return false;
        }
        self.renew(owner, now);
        self.flying.entry(table.to_string()).or_default().insert(owner.to_string());
        true
    }

    /// `owner` stops appending to `table`: it is about to wait for a row lock (its wait must not hold up a closer), or its
    /// append is answered.
    pub fn leave(&mut self, owner: &str, table: &str) {
        if let Some(owners) = self.flying.get_mut(table) {
            owners.remove(owner);
        }
        self.flying.retain(|_, owners| !owners.is_empty());
    }

    /// Does an owner with a lease that lasts append to `table`? A closer waits for it.
    fn appending(&self, table: &str, now: Instant) -> bool {
        self.flying.get(table).is_some_and(|owners| owners.iter().any(|o| self.leases.get(o).is_some_and(|until| *until > now)))
    }

    /// One more writer closes `table` to new appends (`close`); `open` undoes it.
    fn close(&mut self, table: &str) {
        *self.closed.entry(table.to_string()).or_default() += 1;
    }

    fn open(&mut self, table: &str) {
        let left = self.closed.get_mut(table).map(|n| {
            *n -= 1;
            *n
        });
        if left == Some(0) {
            self.closed.remove(table);
        }
    }

    fn is_ended(&self, owner: &str, now: Instant) -> bool {
        self.ended.get(owner).is_some_and(|until| *until > now)
    }

    /// The place `token` names stops waiting for `key` (a timeout, or its session ended while it waited).
    pub fn cancel(&mut self, key: &str, token: u64, now: Instant) {
        if let Some(lock) = self.locks.get_mut(key) {
            lock.queue.retain(|w| w.token != token);
        }
        self.waiting.retain(|_, (_, t)| *t != token); // (tokens are unique: only that place)
        self.settle(key, now);
    }

    /// Gives up `owner`'s place in a queue, if it has one (its wait ends without a grant).
    fn unwait(&mut self, owner: &str, now: Instant) {
        let Some((key, token)) = self.waiting.remove(owner) else { return };
        if let Some(lock) = self.locks.get_mut(&key) {
            lock.queue.retain(|w| w.token != token);
        }
        self.settle(&key, now);
    }

    /// Gives `key` to its queue, in order, once its holder is gone: a holder whose lease lapsed is
    /// gone, and a waiter who stopped listening is skipped.
    fn settle(&mut self, key: &str, now: Instant) {
        let (lease, leases) = (self.lease, &self.leases);
        let Some(lock) = self.locks.get_mut(key) else { return };
        let lapsed = lock.holder.as_ref().is_some_and(|h| leases.get(h).is_none_or(|until| *until <= now));
        if lapsed {
            let old = lock.holder.take().expect("a holder");
            if let Some(keys) = self.held.get_mut(&old) {
                keys.remove(key);
            }
        }
        while lock.holder.is_none() {
            let Some(w) = lock.queue.pop_front() else { break };
            if w.grant.send(()).is_ok() {
                lock.holder = Some(w.owner.clone());
                self.leases.insert(w.owner.clone(), now + lease);
                self.held.entry(w.owner.clone()).or_default().insert(key.to_string());
                if self.waiting.get(&w.owner).is_some_and(|(_, t)| *t == w.token) {
                    self.waiting.remove(&w.owner);
                }
            }
        }
        if lock.holder.is_none() && lock.queue.is_empty() {
            self.locks.remove(key);
        }
    }

    /// Gives `key` to `owner` (it is free), with its lease from `now`.
    fn grant(&mut self, owner: &str, key: &str, now: Instant) {
        self.locks.entry(key.to_string()).or_default().holder = Some(owner.to_string());
        self.held.entry(owner.to_string()).or_default().insert(key.to_string());
        self.leases.insert(owner.to_string(), now + self.lease);
    }

    fn holds(&self, key: &str, owner: &str) -> bool {
        self.locks.get(key).is_some_and(|l| l.holder.as_deref() == Some(owner))
    }

    /// Moves `owner`'s lease to `now` plus the lease, so what it holds lasts while it asks or is renewed. An owner that
    /// let its locks go stays let go.
    fn renew(&mut self, owner: &str, now: Instant) {
        if !self.is_ended(owner, now) {
            self.leases.insert(owner.to_string(), now + self.lease);
        }
    }

    /// Who `owner` would wait behind for `key`: the holder (while its lease lasts), and the waiters
    /// queued before it (all of them, if it isn't queued yet).
    fn ahead(&self, key: &str, owner: &str, now: Instant) -> Vec<String> {
        let Some(lock) = self.locks.get(key) else { return vec![] };
        let mut out = vec![];
        if let Some(h) = &lock.holder {
            if h != owner && self.leases.get(h).is_some_and(|until| *until > now) {
                out.push(h.clone()); // (a holder whose lease lapsed is no one to wait for: `settle` gives its keys away)
            }
        }
        for w in &lock.queue {
            if w.owner == owner {
                break;
            }
            out.push(w.owner.clone());
        }
        out
    }

    /// Does a wait by `owner` on `from` lead back to `owner`? Each waiter waits for one key, so the
    /// walk is a chain of keys, and a repeat of an owner ends it.
    fn loops(&self, from: Vec<String>, owner: &str, now: Instant) -> bool {
        let mut stack = from;
        let mut seen = HashSet::new();
        while let Some(o) = stack.pop() {
            if o == owner {
                return true;
            }
            if !seen.insert(o.clone()) {
                continue;
            }
            if let Some((key, _)) = self.waiting.get(&o) {
                stack.extend(self.ahead(key, &o, now));
            }
        }
        false
    }

    /// Drops what has lapsed: leases that ran out (their owners' keys go to the next in line), refusals that ended,
    /// owners with no keys left, and places of owners that no longer wait. Run now and then (`take`, `count`), so a
    /// follower that vanished leaves no entries behind.
    fn sweep(&mut self, now: Instant) {
        self.leases.retain(|_, until| *until > now);
        self.ended.retain(|_, until| *until > now);
        let leases = &self.leases;
        self.flying.values_mut().for_each(|owners| owners.retain(|o| leases.contains_key(o))); // (a lapsed appender is answered)
        self.flying.retain(|_, owners| !owners.is_empty());
        let keys: Vec<String> = self.locks.keys().cloned().collect();
        for key in keys {
            self.settle(&key, now);
        }
        self.held.retain(|_, keys| !keys.is_empty());
        let queued: HashSet<u64> = self.locks.values().flat_map(|l| l.queue.iter().map(|w| w.token)).collect();
        self.waiting.retain(|_, (_, token)| queued.contains(token));
        self.swept = now;
    }
}

// ---------------------------------------------------------------- the lakes' tables

static TABLES: LazyLock<Mutex<HashMap<String, Arc<Mutex<Table>>>>> = LazyLock::new(Default::default);

/// How long a holder's lock lasts without a request from it (`PONDRA_LOCK_LEASE_SECS`, 60). A transaction
/// renews its locks every third of it (`txn::upkeep`), so an idle session keeps them while it lives.
pub fn lease() -> Duration {
    Duration::from_secs(std::env::var("PONDRA_LOCK_LEASE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60))
}

fn table(lake: &str) -> Arc<Mutex<Table>> {
    TABLES.lock().unwrap().entry(lake.to_string()).or_insert_with(|| Arc::new(Mutex::new(Table::new(lease())))).clone()
}

/// The locks a lake's leader holds now (lapsed ones dropped first): `/stats`' `row_locks`.
pub fn count(url: &str) -> usize {
    let t = TABLES.lock().unwrap().get(url).cloned();
    t.map_or(0, |t| {
        let mut t = t.lock().unwrap();
        t.sweep(Instant::now());
        t.locks.len()
    })
}

/// Takes `keys` for `owner` on the lake at `lake` (its url), waiting at most `wait` for each (None:
/// as long as it takes; zero: NOWAIT, a held key is refused at once). Keys are taken in order, so
/// writers taking the same keys don't cross. With `flight` (an autocommitted write's table), the owner
/// then registers as appending to it (`Table::enter`): the writers that close the table wait for its append.
pub async fn take(lake: &str, owner: &str, keys: &[String], wait: Option<Duration>, flight: Option<&str>) -> Result<()> {
    let t = table(lake);
    let mut keys = keys.to_vec();
    keys.sort();
    keys.dedup();
    for key in &keys {
        let asked = {
            let mut t = t.lock().unwrap();
            if t.swept.elapsed() >= t.lease {
                t.sweep(Instant::now());
            }
            t.ask(owner, key, Instant::now())
        };
        match asked {
            Ask::Granted => {}
            Ask::Deadlock => return Err(coded("40P01", "deadlock detected: this write waits for a row another transaction holds, and that one waits for a row this one holds")),
            Ask::Ended => return Err(coded("55P03", "its locks were let go: its transaction ended, so it takes no more")),
            Ask::Wait(_, token) if wait == Some(Duration::ZERO) => {
                t.lock().unwrap().cancel(key, token, Instant::now()); // (its place in the queue goes: nothing waits)
                return Err(coded("55P03", "could not obtain lock on a row: it is held by another transaction (NOWAIT)"));
            }
            Ask::Wait(rx, token) => {
                // (an appender waits for no row lock: a writer closing its table waits for this append, and this wait would
                // wait for that writer's commit. It registers again once its locks are its own)
                if let Some(table) = flight {
                    t.lock().unwrap().leave(owner, table);
                }
                wait_for(&t, owner, key, rx, token, wait).await?
            }
        }
    }
    if let Some(table) = flight {
        enter(&t, owner, table, wait).await?;
    }
    Ok(())
}

/// The last step of `take` for an appender: it registers as appending to `table`, waiting while a writer has the table
/// closed (its lease renewed meanwhile, so the locks it holds last), as long as `wait` says.
async fn enter(t: &Arc<Mutex<Table>>, owner: &str, table: &str, wait: Option<Duration>) -> Result<()> {
    let deadline = wait.map(|w| Instant::now() + w);
    loop {
        {
            let mut t = t.lock().unwrap();
            let now = Instant::now();
            if t.enter(owner, table, now) {
                return Ok(());
            }
            t.renew(owner, now);
        }
        if crate::drain::draining() {
            return Err(coded("57P01", "this node is stopping: its lock waits end, and its transaction with them"));
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(coded("55P03", format!("canceling statement due to lock timeout: {table} is being written")));
        }
        tokio::time::sleep(TICK).await;
    }
}

/// Moves the leases of what each of `owners` holds on the lake at `url` (`Table::renew`): what an
/// asking would do for them, from a session that is idle in its transaction.
pub fn renew(url: &str, owners: &[String]) {
    let t = TABLES.lock().unwrap().get(url).cloned();
    if let Some(t) = t {
        let now = Instant::now();
        let mut t = t.lock().unwrap();
        owners.iter().for_each(|o| t.renew(o, now));
    }
}

/// A place in a lock's queue, given back if its wait ends another way (a timeout, or the future
/// dropped: the session ended while it waited).
struct Queued {
    t: Arc<Mutex<Table>>,
    key: String,
    token: u64,
    done: bool,
}

impl Drop for Queued {
    fn drop(&mut self) {
        if !self.done {
            self.t.lock().unwrap().cancel(&self.key, self.token, Instant::now());
        }
    }
}

async fn wait_for(t: &Arc<Mutex<Table>>, owner: &str, key: &str, mut rx: oneshot::Receiver<()>, token: u64, wait: Option<Duration>) -> Result<()> {
    let mut place = Queued { t: t.clone(), key: key.to_string(), token, done: false };
    let deadline = wait.map(|w| Instant::now() + w);
    loop {
        if let Ok(granted) = tokio::time::timeout(TICK, &mut rx).await {
            place.done = granted.is_ok();
            if place.done {
                return Ok(());
            }
            return Err(coded("55P03", format!("lock on {key}: its place ended without a grant (its transaction ended, or a newer wait of it took its place)")));
        }
        let now = Instant::now();
        let mut table = t.lock().unwrap();
        table.settle(key, now); // (a holder whose lease lapsed gives way)
        table.renew(owner, now); // (the locks it already holds last while it waits for this one: a wait is no reason to lose them)
        if rx.try_recv().is_ok() {
            place.done = true;
            return Ok(());
        }
        // (a stopping node's waits end at once: its transaction is refused, and the client asks another node)
        if crate::drain::draining() {
            table.cancel(key, token, now);
            place.done = true;
            return Err(coded("57P01", "this node is stopping: its lock waits end, and its transaction with them"));
        }
        if deadline.is_some_and(|d| now >= d) {
            table.cancel(key, token, now);
            place.done = true;
            return Err(coded("55P03", "canceling statement due to lock timeout"));
        }
    }
}

/// Lets go of every lock `owner` holds on the lake (`url`), and of its place in any queue.
pub fn release(url: &str, owner: &str) {
    let t = TABLES.lock().unwrap().get(url).cloned();
    if let Some(t) = t {
        t.lock().unwrap().release(owner, Instant::now());
    }
}

// ---------------------------------------------------------------- the leader's door

/// A node's request to the leader (`POST /cluster/lock`): take `keys` for `owner`, waiting at most
/// `wait_ms` for each; let all of `owner`'s locks go; or renew the leases of the locks of each of
/// `owners` (a follower's idle transactions, sent in one request: `txn::upkeep`).
#[derive(Clone, Serialize, Deserialize)]
pub struct Req {
    #[serde(default)]
    pub owner: String,
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub wait_ms: Option<u64>,
    #[serde(default)]
    pub release: bool,
    #[serde(default)]
    pub renew: bool,
    #[serde(default)]
    pub owners: Vec<String>,
    /// The table an autocommitted write appends to, once its keys are its own (`take`).
    #[serde(default)]
    pub flight: Option<String>,
}

impl Req {
    /// Take `keys` for `owner`, waiting at most `wait` for each (None: as long as it takes); with `flight`, then append
    /// to that table (`take`).
    pub fn take(owner: &str, keys: &[String], wait: Option<Duration>, flight: Option<&str>) -> Req {
        Req { owner: owner.to_string(), keys: keys.to_vec(), wait_ms: wait.map(|w| w.as_millis() as u64), flight: flight.map(str::to_string), ..Req::empty() }
    }

    /// Lets `owner`'s locks go.
    pub fn let_go(owner: &str) -> Req {
        Req { owner: owner.to_string(), release: true, ..Req::empty() }
    }

    /// Renews the leases of `owners`' locks.
    pub fn renewal(owners: Vec<String>) -> Req {
        Req { owners, renew: true, ..Req::empty() }
    }

    fn empty() -> Req {
        Req { owner: String::new(), keys: vec![], wait_ms: None, release: false, renew: false, owners: vec![], flight: None }
    }
}

/// The leader's side of `POST /cluster/lock`: its answer is the commit it has committed up to, which
/// the requester waits for before it reads the rows it locked.
pub async fn serve(lake: &Lake, r: Req) -> Result<u64> {
    if r.release {
        release(&lake.url, &r.owner);
    } else if r.renew {
        renew(&lake.url, &r.owners);
    } else {
        take(&lake.url, &r.owner, &r.keys, r.wait_ms.map(Duration::from_millis), r.flight.as_deref()).await?;
    }
    Ok(lake.visible())
}

/// The SQLSTATEs a leader's answer can carry (its `x-pondra-sqlstate` header), as static strings.
const ANSWERS: [&str; 4] = ["40P01", "55P03", "40001", "57P01"];

/// A node asks the leader at `addr` (`POST /cluster/lock`). Its answer: the commit the locks were
/// granted at. A refusal keeps the leader's SQLSTATE. A lock wait is bounded: the leader answers within
/// the wait the request names (what is left of the `lock_timeout`); a leader that doesn't answer in time
/// (a lost request, a partition) is asked again, and the place it may have left is replaced, not doubled
/// (`Table::ask`). A stopping node ends its waits at once (57P01).
pub async fn ask(addr: &str, r: &Req) -> Result<u64> {
    if r.release || r.renew {
        // (asked once: a release or a renewal that doesn't land is the lease's to end, and the next renewal says it again)
        return match post_within(addr, r, lease()).await? {
            Some(at) => Ok(at),
            None => Err(anyhow::anyhow!("the leader at {addr} didn't answer a lock request in time")),
        };
    }
    let started = Instant::now();
    let total = r.wait_ms.map(Duration::from_millis); // (None: as long as it takes)
    loop {
        let left = total.map(|t| t.saturating_sub(started.elapsed()));
        if left == Some(Duration::ZERO) {
            return Err(coded("55P03", "canceling statement due to lock timeout"));
        }
        let mut req = r.clone();
        req.wait_ms = left.map(|l| l.as_millis() as u64);
        let limit = left.map_or(lease(), |l| l + MARGIN);
        match post_within(addr, &req, limit).await? {
            Some(at) => return Ok(at),
            None => continue, // (no answer in time: asked again)
        }
    }
}

/// One request to the leader, `None` if it didn't answer within `limit`. A stopping node stops waiting for it.
async fn post_within(addr: &str, req: &Req, limit: Duration) -> Result<Option<u64>> {
    let call = tokio::time::timeout(limit, send(addr, req));
    tokio::pin!(call);
    loop {
        tokio::select! {
            res = &mut call => return Ok(match res {
                Ok(answer) => Some(answer?),
                Err(_) => None,
            }),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if crate::drain::draining() {
                    return Err(coded("57P01", "this node is stopping: its lock waits end, and its transaction with them"));
                }
            }
        }
    }
}

async fn send(addr: &str, r: &Req) -> Result<u64> {
    let res = crate::cluster::http().post(crate::tls::url(&format!("{addr}/cluster/lock"))).json(r).send().await?;
    if !res.status().is_success() {
        let code = res.headers().get("x-pondra-sqlstate").and_then(|v| v.to_str().ok()).and_then(|c| ANSWERS.iter().find(|a| **a == c).copied());
        let text = res.text().await?;
        return Err(match code {
            Some(code) => coded(code, text),
            None => anyhow::anyhow!("the leader at {addr}: {text}"),
        });
    }
    Ok(res.json::<u64>().await?)
}

// ---------------------------------------------------------------- the writers' side (ADR-067 §3–§7)

/// A transaction's or a statement's place on the leader: its lake's url, its leader (None: this node), and its owner.
pub type Place = (String, Option<String>, String);

/// What a write takes its locks as (`passes`): its owner (a transaction's id, or an autocommitted write's), and the
/// settings it was asked with. `txn`: a transaction's (its COMMIT or ROLLBACK lets the locks go); else an autocommitted
/// write's, which registers as appending to its table once its keys are its own. `locking`: the write takes row locks at
/// all. `committed`: READ COMMITTED, where a moved row is read again rather than refused. `wait`: `lock_timeout` (None: as
/// long as it takes). `taken`: the owner has asked for locks, so their release is due when its statement ends.
pub struct Locks<'a> {
    pub app: &'a crate::server::App,
    pub name: &'a str,
    pub meta: &'a TableMeta,
    pub owner: String,
    pub txn: bool,
    pub locking: bool,
    pub committed: bool,
    pub wait: Option<Duration>,
    pub taken: &'a AtomicBool,
}

impl Locks<'_> {
    /// The table an autocommitted write appends to: its owner registers as appending to it (`take`).
    fn flight(&self) -> Option<&str> {
        (!self.txn).then_some(self.name)
    }
}

/// The passes of a write or a locking read (ADR-067 §3, §6), the same at every door: `pass(at)` works its rows out as of
/// commit `at` and names their keys, which are locked; if one of them changed after `at`, the pass runs again as of the
/// commit the locks were granted at (READ COMMITTED), or the write is refused (REPEATABLE READ, 40001). With no locks
/// (`locking` false, or more than `LOCK_MAX` keys) the first pass stands. A pass that adds no key the statement doesn't
/// hold already works its rows out under locks it has, as of the newest commit: that pass stands, and the passes end. The
/// only cap is `LOCK_MAX`: each pass that goes on locks a key it hadn't held, and a write that needs more than that locks none. Returns the value, the commit
/// it is good as of, and the keys locked (None: none).
pub async fn passes<T, F, Fut>(l: &Locks<'_>, start: u64, mut pass: F) -> Result<(T, u64, Option<Keys>)>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<(T, Keys)>>,
{
    let lake = &l.app.lake;
    let mut at = start;
    let mut held: HashSet<Vec<String>> = HashSet::new();
    loop {
        let (value, keys) = match pass(at).await {
            Ok(p) => p,
            Err(e) if l.committed && crate::codes::of(&e) == "40001" => {
                at = lake.visible(); // (moved before this pass locked anything: the newest commit this node sees)
                continue;
            }
            Err(e) => return Err(e),
        };
        if !l.locking || keys.is_empty() || keys.len() > LOCK_MAX {
            return Ok((value, at, None));
        }
        if !held.is_empty() && keys.iter().all(|k| held.contains(k)) {
            return Ok((value, at, Some(keys)));
        }
        held.extend(keys.iter().cloned());
        if l.txn {
            crate::txn::mark_locked(); // (set first: a wait that fails still lets its locks go at the end)
        }
        l.taken.store(true, Ordering::SeqCst);
        let granted = take_on(l.app, &l.owner, &resources(l.name, &keys), l.wait, l.flight()).await?;
        let read = Reads { floor: Some(at), keys: HashMap::new() };
        match crate::txn::conflicts(lake, l.name, l.meta, &read, &keys.iter().cloned().collect()).await {
            Ok(()) => return Ok((value, granted, Some(keys))),
            Err(e) if l.committed && crate::codes::of(&e) == "40001" => at = granted,
            Err(e) => return Err(e),
        }
    }
}

/// Takes `keys` for `owner`, here or at its leader (ADR-067 §4), and returns the commit they were granted at, which this
/// node has committed up to when it returns. A write waits as `wait` says. With `flight`, the owner then registers as
/// appending to that table, in the same request.
pub async fn take_on(app: &crate::server::App, owner: &str, resources: &[String], wait: Option<Duration>, flight: Option<&str>) -> Result<u64> {
    if app.seq.is_some() {
        take(&app.lake.url, owner, resources, wait, flight).await?;
        return Ok(app.lake.visible());
    }
    let at = ask(&app.cluster.leader.addr, &Req::take(owner, resources, wait, flight)).await?;
    wait_visible(&app.lake, at).await?;
    Ok(at)
}

/// Waits until this node holds commit `upto` (its leader's answer), 10 s at most.
async fn wait_visible(lake: &Lake, upto: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while lake.visible() < upto {
        if Instant::now() >= deadline {
            return Err(coded("57P03", "this node hasn't caught up with its leader yet: try again"));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(())
}

/// The session's `lock_timeout` (`SET lock_timeout = '200ms'`): how long a lock may be waited for. None: as long as it takes.
pub fn lock_wait() -> Option<Duration> {
    parse_wait(&crate::settings::shown("lock_timeout")?)
}

/// A Postgres time: a number and its unit (`us`, `ms` when none, `s`, `min`, `h`, `d`). Zero is none.
pub fn parse_wait(v: &str) -> Option<Duration> {
    let v = v.trim().to_lowercase();
    let number: String = v.chars().take_while(|c| c.is_ascii_digit() || *c == '.').collect();
    let n: f64 = number.parse().ok()?;
    let per_ms = match v[number.len()..].trim() {
        "" | "ms" => 1.0,
        "us" => 0.001,
        "s" => 1000.0,
        "min" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => return None,
    };
    (n * per_ms > 0.0).then(|| Duration::from_secs_f64(n * per_ms / 1000.0))
}

/// The locks' names for `keys` of `table`: the table and the key's parts, apart by `KEY_SEP`.
pub fn resources(table: &str, keys: &[Vec<String>]) -> Vec<String> {
    keys.iter().map(|k| format!("{table}{}{}", crate::txn::KEY_SEP, k.join(crate::txn::KEY_SEP))).collect()
}

/// Lets an owner's row locks go on the lake: here if this node leads it, else at its leader, without waiting (a lease
/// covers a request that never arrives: ADR-067 §4). Its leader refuses a request from the owner that comes later.
pub fn release_place((lake, leader, owner): Place) {
    match leader {
        None => release(&lake, &owner),
        Some(addr) => tell(&addr, &Req::let_go(&owner)),
    }
}

/// Sends a lock request to the leader at `addr` without waiting for its answer.
fn tell(addr: &str, req: &Req) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let (addr, req) = (addr.to_string(), req.clone());
        handle.spawn(async move {
            let _ = ask(&addr, &req).await;
        });
    }
}

/// Renews the leases of the transactions that hold locks: here for a lake this node leads, else one request to its leader
/// carrying all of that leader's (`Req::renewal`).
pub fn renew_places(holding: Vec<Place>) {
    let mut by_place: BTreeMap<(String, Option<String>), Vec<String>> = BTreeMap::new();
    for (lake, leader, id) in holding {
        by_place.entry((lake, leader)).or_default().push(id);
    }
    for ((lake, leader), owners) in by_place {
        match leader {
            None => renew(&lake, &owners),
            Some(addr) => tell(&addr, &Req::renewal(owners)),
        }
    }
}

/// The node is stopping (`drain.rs`): the row locks its transactions hold go to their leaders now, so the waits of other
/// transactions end at once rather than at the lease's end (ADR-067 §4).
pub fn release_all() {
    crate::txn::unlock_all().into_iter().for_each(release_place);
}

/// A statement's row locks as one owner (ADR-067 §7). Dropped before its append is sent (the statement failed, or was
/// cancelled), they go at once. Once the append is on its way they go with its answer (`finish`), or with the lease if no
/// answer comes back: releasing before the append lands would let the next writer work from a snapshot without it.
/// An answer is any reply of the leader or the sequencer, a refusal included; a request that fails in transit, or a
/// reply that is lost, is no answer (`lost`), and then the locks are kept until the lease ends (nothing renews them after
/// `finish`), since the append may still commit.
pub struct Hold {
    place: Option<Place>,
    sent: bool,
    lost: bool,
}

impl Hold {
    pub fn new(place: Place) -> Hold {
        Hold { place: Some(place), sent: false, lost: false }
    }

    /// The append is on its way: from here the locks are `finish`'s, or the lease's.
    pub fn sending(&mut self) {
        self.sent = true;
    }

    /// The append's answer never came back: its locks wait for their lease (see `Hold`).
    pub fn lost(&mut self) {
        self.lost = true;
    }

    /// The statement is over: its append was answered (or refused), or it made none. Its locks go if it took any, unless
    /// its answer was lost.
    pub fn finish(&mut self, took: bool) {
        if let Some(place) = self.place.take() {
            if took && !self.lost {
                release_place(place);
            }
        }
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        if let Some(place) = self.place.take() {
            if !self.sent {
                release_place(place);
            }
        }
    }
}

/// The gate a writer that changes rows under the lake's lock holds on the tables it writes (`close`): opened when dropped.
pub struct Gate {
    t: Arc<Mutex<Table>>,
    tables: Vec<String>,
}

impl Drop for Gate {
    fn drop(&mut self) {
        let mut t = self.t.lock().unwrap();
        self.tables.iter().for_each(|n| t.open(n));
    }
}

/// Set for a check that runs without the gate (`PONDRA_TEST_NO_GATE`, read once): `close` then closes nothing.
static NO_GATE: LazyLock<bool> = LazyLock::new(|| std::env::var_os("PONDRA_TEST_NO_GATE").is_some());

/// A writer that changes rows under the lake's lock (a transaction's COMMIT, a change, a file commit that takes rows back)
/// closes `tables` before it reads or checks them (ADR-067 §7). No autocommitted write enters them meanwhile, and the
/// appends already in flight are waited for, so what the writer reads includes them all. An autocommitted write works its
/// rows out first and appends them after, so without the gate a change made between the two would be overwritten. Hold
/// the gate until the commit is made: dropping it opens the tables again. An appender whose lease lapses (60 s) no longer
/// holds the gate.
pub async fn close(url: &str, tables: &[String]) -> Result<Gate> {
    let t = table(url);
    if *NO_GATE {
        return Ok(Gate { t, tables: vec![] });
    }
    {
        let mut table = t.lock().unwrap();
        tables.iter().for_each(|n| table.close(n));
    }
    let gate = Gate { t: t.clone(), tables: tables.to_vec() }; // (opened when dropped, however the wait below ends)
    loop {
        let busy = {
            let table = t.lock().unwrap();
            let now = Instant::now();
            tables.iter().any(|n| table.appending(n, now))
        };
        if !busy {
            return Ok(gate);
        }
        tokio::time::sleep(TICK).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease_table() -> Table { Table::new(Duration::from_secs(60)) }

    fn granted(a: Ask) -> bool { matches!(a, Ask::Granted) }

    fn waiting(a: Ask) -> Option<(oneshot::Receiver<()>, u64)> {
        match a {
            Ask::Wait(rx, token) => Some((rx, token)),
            _ => None,
        }
    }

    #[test]
    fn a_free_lock_is_granted_and_held_by_its_owner() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        assert!(granted(t.ask("A", "k", now)), "asking again holds it");
        assert!(t.holds("k", "A"));
    }

    #[test]
    fn waiters_are_granted_first_come_first_served() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        let (mut b, _) = waiting(t.ask("B", "k", now)).expect("B waits");
        let (mut c, _) = waiting(t.ask("C", "k", now)).expect("C waits");
        t.release("A", now);
        assert!(b.try_recv().is_ok(), "B is first in the queue");
        assert!(c.try_recv().is_err(), "C still waits");
        t.release("B", now);
        assert!(c.try_recv().is_ok(), "then C");
        t.release("C", now);
        assert!(t.locks.is_empty(), "nothing is left behind");
    }

    #[test]
    fn a_wait_that_closes_a_loop_is_refused() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "x", now)));
        assert!(granted(t.ask("B", "y", now)));
        assert!(waiting(t.ask("A", "y", now)).is_some());
        assert!(matches!(t.ask("B", "x", now), Ask::Deadlock), "B would wait for A, which waits for B");
        assert!(!t.waiting.contains_key("B"), "the refused wait leaves no place behind");
    }

    #[test]
    fn a_wait_behind_a_waiter_that_is_not_in_a_loop_is_allowed() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "x", now)));
        assert!(waiting(t.ask("B", "x", now)).is_some());
        assert!(waiting(t.ask("C", "x", now)).is_some(), "C waits behind B: no loop");
    }

    #[test]
    fn a_lapsed_lease_gives_way_to_the_next_waiter() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        let later = now + Duration::from_secs(61);
        assert!(granted(t.ask("B", "k", later)), "A's lease lapsed: B takes the lock");
        assert!(!t.held.get("A").is_some_and(|k| k.contains("k")), "A holds nothing now");
    }

    #[test]
    fn asking_renews_the_leases_held() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        let soon = now + Duration::from_secs(50);
        assert!(granted(t.ask("A", "j", soon)), "asking for another key renews k");
        let later = now + Duration::from_secs(70);
        assert!(waiting(t.ask("B", "k", later)).is_some(), "k's lease runs to 110 s: B waits");
    }

    #[test]
    fn a_cancelled_wait_leaves_the_queue_and_a_granted_lock_goes_back() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        let (_, token) = waiting(t.ask("B", "k", now)).expect("B waits");
        t.cancel("k", token, now);
        t.release("A", now);
        assert!(granted(t.ask("C", "k", now)), "B's place is gone: C has the lock");
        t.release("C", now);
        assert!(granted(t.ask("D", "k", now)), "nothing is left queued behind C");
    }

    #[test]
    fn a_transaction_that_ends_while_queued_leaves_the_queue_and_its_wait_fails() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        let (mut b, _) = waiting(t.ask("B", "k", now)).expect("B waits");
        let (mut c, _) = waiting(t.ask("C", "k", now)).expect("C waits");
        t.release("B", now);
        assert!(b.try_recv().is_err(), "B's wait ends without a grant");
        t.release("A", now);
        assert!(c.try_recv().is_ok(), "C is next: B left the queue");
    }

    #[test]
    fn a_retry_replaces_its_place_instead_of_doubling_it() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        let (_, first) = waiting(t.ask("B", "k", now)).expect("B waits");
        let (mut again, second) = waiting(t.ask("B", "k", now)).expect("B asks again: its first place is replaced");
        assert_ne!(first, second);
        assert_eq!(t.locks["k"].queue.len(), 1, "one place, not two");
        t.release("A", now);
        assert!(again.try_recv().is_ok(), "the new place is the one granted");
    }

    #[test]
    fn a_renewal_keeps_an_idle_holders_lock() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        t.renew("A", now + Duration::from_secs(50)); // (A is idle: only its renewal reaches the table)
        assert!(waiting(t.ask("B", "k", now + Duration::from_secs(70))).is_some(), "renewed at 50 s: its lease runs to 110 s");
        t.renew("A", now + Duration::from_secs(100));
        assert!(waiting(t.ask("C", "k", now + Duration::from_secs(150))).is_some(), "renewed again at 100 s: held to 160 s");
    }

    #[test]
    fn an_owner_that_let_go_is_refused_for_a_lease_then_may_ask_again() {
        // (L4: a request still on its way after the release must not take the locks back)
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        t.release("A", now);
        assert!(matches!(t.ask("A", "k", now + Duration::from_secs(1)), Ask::Ended), "A let go: refused");
        assert!(granted(t.ask("B", "k", now + Duration::from_secs(1))), "its key is free for others");
        t.sweep(now + Duration::from_secs(61));
        assert!(granted(t.ask("A", "j", now + Duration::from_secs(61))), "the refusal ended with its lease");
    }

    #[test]
    fn a_renewal_of_an_owner_holding_nothing_is_harmless() {
        let (mut t, now) = (lease_table(), Instant::now());
        t.renew("nobody", now);
        assert!(granted(t.ask("A", "k", now)), "the lock is free as before");
    }

    #[test]
    fn a_sweep_drops_a_lapsed_holder_and_the_owner_with_no_keys() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(granted(t.ask("A", "k", now)));
        assert!(granted(t.ask("A", "j", now)));
        t.sweep(now + Duration::from_secs(61));
        assert!(t.locks.is_empty(), "A's leases lapsed: nothing is left");
        assert!(t.held.is_empty(), "nor an owner with no keys");
    }

    #[tokio::test]
    async fn nowait_refuses_a_held_key_at_once() {
        let lake = format!("test-locks-{}", uuid::Uuid::new_v4());
        take(&lake, "A", &["k".to_string()], None, None).await.expect("A takes k");
        let t0 = Instant::now();
        let err = take(&lake, "B", &["k".to_string()], Some(Duration::ZERO), None).await.expect_err("B is refused, not waited for");
        assert_eq!(crate::codes::of(&err), "55P03");
        assert!(t0.elapsed() < Duration::from_millis(40), "NOWAIT waits for nothing ({:?})", t0.elapsed());
        release(&lake, "A");
        take(&lake, "C", &["k".to_string()], Some(Duration::ZERO), None).await.expect("free: NOWAIT takes it");
        release(&lake, "C");
    }

    #[tokio::test]
    async fn take_waits_for_the_holder_and_times_out_with_55p03() {
        let lake = format!("test-locks-{}", uuid::Uuid::new_v4());
        take(&lake, "A", &["k".to_string()], None, None).await.expect("A takes k");
        let err = take(&lake, "B", &["k".to_string()], Some(Duration::from_millis(120)), None).await.expect_err("B times out");
        assert_eq!(crate::codes::of(&err), "55P03");
        let waiter = tokio::spawn({
            let lake = lake.clone();
            async move { take(&lake, "C", &["k".to_string()], None, None).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        release(&lake, "A");
        waiter.await.expect("the task").expect("C is granted k when A lets go");
        release(&lake, "C");
    }

    #[tokio::test]
    async fn a_waiter_whose_future_is_dropped_leaves_the_queue() {
        let lake = format!("test-locks-{}", uuid::Uuid::new_v4());
        take(&lake, "A", &["k".to_string()], None, None).await.expect("A takes k");
        let gone = tokio::spawn({
            let lake = lake.clone();
            async move { take(&lake, "B", &["k".to_string()], None, None).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        gone.abort();
        let _ = gone.await;
        release(&lake, "A");
        take(&lake, "C", &["k".to_string()], Some(Duration::from_millis(500)), None).await.expect("C gets k: B left");
        release(&lake, "C");
    }

    #[test]
    fn a_closed_table_admits_no_new_appender_until_it_opens() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(t.enter("A", "x", now), "an appender enters an open table");
        t.close("x");
        assert!(!t.enter("B", "x", now), "a new appender waits while the table is closed");
        assert!(t.enter("A", "x", now), "one already appending goes on");
        assert!(t.appending("x", now), "A's append is what a closer waits for");
        t.leave("A", "x");
        assert!(!t.appending("x", now), "once A left, a closer need not wait for it");
        t.open("x");
        assert!(t.enter("B", "x", now), "open again: B enters");
    }

    #[test]
    fn a_release_takes_its_owner_out_of_every_table_and_a_lapsed_appender_stops_holding_a_closer() {
        let (mut t, now) = (lease_table(), Instant::now());
        assert!(t.enter("A", "x", now) && t.enter("A", "y", now));
        t.release("A", now);
        assert!(!t.appending("x", now) && !t.appending("y", now), "a release leaves no append in flight");
        assert!(t.enter("B", "x", now) && t.enter("C", "y", now));
        assert!(t.appending("x", now));
        assert!(!t.appending("x", now + Duration::from_secs(61)), "a lapsed lease (60 s) no longer holds a closer");
    }

    #[test]
    fn a_lock_wait_is_a_postgres_time() {
        assert_eq!(parse_wait("200ms"), Some(Duration::from_millis(200)));
        assert_eq!(parse_wait("2s"), Some(Duration::from_secs(2)));
        assert_eq!(parse_wait("750"), Some(Duration::from_millis(750)));
        assert_eq!(parse_wait("0"), None, "zero is no timeout");
    }

    #[test]
    fn a_key_is_a_resource_of_its_table_apart_by_the_separator() {
        assert_eq!(resources("t", &[vec!["1".into()], vec!["a".into(), "b".into()]]), vec!["t\u{1}1".to_string(), "t\u{1}a\u{1}b".to_string()]);
    }

    #[tokio::test]
    async fn a_closer_waits_for_the_appends_in_flight() {
        let lake = format!("test-locks-{}", uuid::Uuid::new_v4());
        take(&lake, "A", &[], None, Some("x")).await.expect("A is appending to x");
        let closer = tokio::spawn({
            let lake = lake.clone();
            async move { close(&lake, &["x".to_string()]).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!closer.is_finished(), "the closer waits for A's append");
        release(&lake, "A"); // (its append is answered)
        let gate = closer.await.expect("the task").expect("the gate");
        drop(gate);
    }

    #[tokio::test]
    async fn a_new_appender_waits_at_the_gate_until_it_is_dropped() {
        let lake = format!("test-locks-{}", uuid::Uuid::new_v4());
        let gate = close(&lake, &["x".to_string()]).await.expect("no appender: the gate is closed at once");
        let waiting = tokio::spawn({
            let lake = lake.clone();
            async move { take(&lake, "B", &[], Some(Duration::from_secs(5)), Some("x")).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!waiting.is_finished(), "B waits while the table is closed");
        drop(gate);
        waiting.await.expect("the task").expect("B enters once the gate opens");
        release(&lake, "B");
    }

    #[tokio::test]
    async fn a_waiter_renews_the_keys_it_holds_while_it_waits() {
        let t = Arc::new(Mutex::new(Table::new(Duration::from_millis(300))));
        let now = Instant::now();
        assert!(granted(t.lock().unwrap().ask("B", "k0", now)), "B holds k0");
        assert!(granted(t.lock().unwrap().ask("A", "k1", now)), "A holds k1");
        let (rx, token) = waiting(t.lock().unwrap().ask("B", "k1", now)).expect("B waits for k1");
        let waiter = tokio::spawn({
            let t = t.clone();
            async move { wait_for(&t, "B", "k1", rx, token, None).await }
        });
        // (B's lease, 300 ms, would have lapsed twice over; A's, which holds k1, is renewed here so that it doesn't lapse first:
        // a lapsed holder gives k1 to B, and B would stop renewing once its wait ended)
        for _ in 0..14 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            t.lock().unwrap().renew("A", Instant::now());
        }
        {
            let mut table = t.lock().unwrap();
            table.settle("k0", Instant::now());
            assert!(table.holds("k0", "B"), "B keeps k0 while it waits: its lease is renewed on each tick");
        }
        t.lock().unwrap().release("A", Instant::now());
        waiter.await.expect("the task").expect("B is granted k1 once A lets go");
    }

    #[tokio::test]
    async fn a_write_dropped_before_its_append_lets_its_locks_go() {
        let lake = format!("test-locks-{}", uuid::Uuid::new_v4());
        take(&lake, "A", &["k".to_string()], None, None).await.expect("A takes k");
        drop(Hold::new((lake.clone(), None, "A".to_string()))); // (its statement failed before the append was sent)
        take(&lake, "B", &["k".to_string()], Some(Duration::ZERO), None).await.expect("B takes k at once: A's locks went with its drop");
        release(&lake, "B");
    }

    #[tokio::test]
    async fn a_sent_append_keeps_its_locks_until_its_answer() {
        let lake = format!("test-locks-{}", uuid::Uuid::new_v4());
        take(&lake, "A", &["k".to_string()], None, None).await.expect("A takes k");
        let mut hold = Hold::new((lake.clone(), None, "A".to_string()));
        hold.sending();
        let refused = take(&lake, "B", &["k".to_string()], Some(Duration::ZERO), None).await.expect_err("B is refused: A's append may still land");
        assert_eq!(crate::codes::of(&refused), "55P03");
        hold.finish(true); // (the append is answered)
        take(&lake, "B", &["k".to_string()], Some(Duration::ZERO), None).await.expect("B takes k once A's answer came");
        release(&lake, "B");
    }

    #[tokio::test]
    async fn an_append_whose_answer_was_lost_keeps_its_locks_to_the_lease() {
        let lake = format!("test-locks-{}", uuid::Uuid::new_v4());
        take(&lake, "A", &["k".to_string()], None, Some("x")).await.expect("A takes k, appending to x");
        let mut hold = Hold::new((lake.clone(), None, "A".to_string()));
        hold.sending();
        hold.lost(); // (the request failed in transit: the leader may still commit it)
        hold.finish(true);
        let refused = take(&lake, "B", &["k".to_string()], Some(Duration::ZERO), None).await.expect_err("B is refused: A's append may still land");
        assert_eq!(crate::codes::of(&refused), "55P03");
        let closer = tokio::spawn({
            let lake = lake.clone();
            async move { close(&lake, &["x".to_string()]).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!closer.is_finished(), "a closer waits for A's lost append too, while its lease lasts");
        // (the lease's end: nothing renews A's locks once `finish` has run, so its lease lapses and both let go)
        table(&lake).lock().unwrap().leases.insert("A".to_string(), Instant::now());
        drop(closer.await.expect("the task").expect("the gate"));
        take(&lake, "B", &["k".to_string()], Some(Duration::ZERO), None).await.expect("B takes k once A's lease lapsed");
        release(&lake, "B");
    }

    #[test]
    fn only_a_lost_answer_is_unanswered() {
        let lost: anyhow::Error = crate::log::Refusal { why: "gone".into(), lost: true }.into();
        let refused: anyhow::Error = crate::log::Refusal::from("no table t".to_string()).into();
        assert!(crate::log::unanswered(&lost));
        assert!(!crate::log::unanswered(&refused), "a refusal is an answer: its locks go");
        assert!(!crate::log::unanswered(&anyhow::anyhow!("not sent")));
    }
}
