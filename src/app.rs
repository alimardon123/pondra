//! The node's lake and what runs on it: `App`, the handle every door and form takes, with its queries and the answers
//! kept for them, tiering, and the tasks and views filled from the log. The doors (`server.rs` and the rest) hold one
//! and call into it.
use crate::cluster::Cluster;
use crate::log::{Log, Sequencer};
use crate::query::session;
use crate::store::*;
use crate::tier::{expire, tier_table};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use serde_json::{json as j, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct App {
    pub lake: Arc<Lake>,
    pub cluster: Arc<Cluster>,
    pub log: Option<Arc<Log>>,       // this node's batcher; None on read-only nodes
    pub seq: Option<Arc<Sequencer>>, // the leader's sequencer
    pub lock: Arc<Mutex<()>>,        // serialises everything that rewrites table metadata
    pub retain_ms: u64,
    pub results: Arc<Results>,       // recent query results (see `Results`)
    pub replica: Option<Arc<crate::replica::ReplicaLog>>, // what this follower holds for the leader
    pub auth: Arc<crate::auth::Auth>,
}

/// Recent query results. By default a result is reused only at exactly the catalog version it
/// was computed at (`Catalog::version`), so a dashboard's repeated query costs a hash lookup until
/// the next commit, however many ask. With `?stale_ms=N` a caller accepts one up to N ms old
/// instead — for dashboards over tables that change every few milliseconds — and while one
/// request recomputes an expired result, the others keep getting the previous one.
pub struct Results(std::sync::Mutex<(lru::LruCache<String, Cached>, usize)>, std::sync::Mutex<HashMap<String, Arc<Flight>>>);

/// Arrivals so far, and (under the lock: one computation at a time) the arrivals the last
/// computation covers, with its result.
type Flight = (std::sync::atomic::AtomicU64, Mutex<(u64, Option<bytes::Bytes>)>);

struct Cached {
    version: u64,
    at: std::time::Instant,
    body: bytes::Bytes,
    refreshing: Option<std::time::Instant>, // a request is computing a newer one (stale mode)
}

impl Default for Results {
    fn default() -> Self { Results(std::sync::Mutex::new((lru::LruCache::unbounded(), 0)), Default::default()) }
}

impl Results {
    const MAX: usize = 64 << 20;
    /// The biggest answer kept, by every door that keeps one: big results aren't what dashboards repeat.
    pub(crate) const BIGGEST: usize = 1 << 20;

    fn flight(&self, key: &str) -> Arc<Flight> {
        let mut f = self.1.lock().unwrap();
        if f.len() > 10_000 {
            f.clear(); // (a flight in progress keeps its own handle)
        }
        f.entry(key.to_string()).or_default().clone()
    }

    fn get(&self, key: &str, version: u64, stale: Option<Duration>) -> Option<bytes::Bytes> {
        let mut c = self.0.lock().unwrap();
        let e = c.0.get_mut(key)?;
        let fresh_enough = stale.is_some_and(|s| e.at.elapsed() <= s);
        let someone_refreshing = stale.is_some() && e.refreshing.is_some_and(|t| t.elapsed() < Duration::from_secs(10));
        if e.version == version || fresh_enough || someone_refreshing {
            return Some(e.body.clone());
        }
        if stale.is_some() {
            e.refreshing = Some(std::time::Instant::now()); // this caller recomputes; the rest wait on nobody
        }
        None
    }

    fn put(&self, key: String, version: u64, body: bytes::Bytes) {
        if body.len() > Self::BIGGEST {
            return;
        }
        let mut c = self.0.lock().unwrap();
        c.1 += body.len();
        if let Some(old) = c.0.put(key, Cached { version, at: std::time::Instant::now(), body, refreshing: None }) {
            c.1 -= old.body.len();
        }
        while c.1 > Self::MAX {
            let Some((_, old)) = c.0.pop_lru() else { break };
            c.1 -= old.body.len();
        }
    }

    /// The bytes of answers remembered now.
    pub fn held(&self) -> usize { self.0.lock().unwrap().1 }
}

impl App {
    /// Does nothing need a sign-in here? (No token set, no user who signs in, and the node listens on
    /// this machine only. A node on the network is never open: its leader made the lake's first user
    /// before it served (`users::first_user`), and a follower of a lake with none yet refuses everyone
    /// until one has a token (`pondra token`). Nor is a node a program started for itself (`owned`):
    /// it answers only that program, a token or a user. A node that just started asks once it holds
    /// what its leader had: a mirror seeded before the first user was flushed knows of none.)
    pub async fn open(&self) -> bool {
        if self.auth.on() || self.auth.network() || self.auth.owned() {
            return false;
        }
        let _ = self.lake.caught_up().await; // (still catching up after 10 s: its own catalog decides)
        if crate::users::signed(&self.lake).await {
            return false; // (served with tokens once: a node started without them answers no one, `users::signed`)
        }
        !crate::users::any(&self.lake).await
    }
}

tokio::task_local! {
    /// Set while this task holds one of the node's query turns (`App::query_listed`).
    static TURN: ();
}

/// The node's turns for queries: `PONDRA_QUERIES` run at once (4 a core, 16 at least), in the order
/// they asked, and a query that waits `PONDRA_QUEUE_SECS` (60) for one is refused (53300). Without
/// them a burst of clients ran at once, and the memory they used was DataFusion's to count only in
/// part (see `query::collect`), so the node could run out of it.
struct Turns {
    turns: tokio::sync::Semaphore,
    most: usize,
    wait: Duration,
}

static TURNS: std::sync::LazyLock<Turns> = std::sync::LazyLock::new(|| {
    let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
    let most = crate::env::or::<usize>("PONDRA_QUERIES", (4 * cores).max(16)).max(1);
    let wait = crate::env::or::<f64>("PONDRA_QUEUE_SECS", 60.0);
    Turns { turns: tokio::sync::Semaphore::new(most), most, wait: Duration::from_secs_f64(wait.max(0.0)) }
});

/// A turn for one query, once it is its turn (in line, counted as waiting), or 53300.
async fn turn() -> anyhow::Result<(tokio::sync::SemaphorePermit<'static>, crate::metrics::Now)> {
    let waiting = crate::metrics::Now::new(&crate::metrics::WAITING);
    let got = tokio::time::timeout(TURNS.wait, TURNS.turns.acquire()).await;
    drop(waiting);
    match got {
        Ok(Ok(permit)) => Ok((permit, crate::metrics::Now::new(&crate::metrics::RUNNING))),
        _ => Err(crate::codes::coded("53300", format!("this node is running {} queries, its most at once (PONDRA_QUERIES): try again, or ask another node", TURNS.most))),
    }
}

/// Each table's job, four at a time: the answers of those that worked, and a line for `failed`
/// naming each that didn't.
async fn each<'a, T>(jobs: Vec<impl std::future::Future<Output = (&'a String, anyhow::Result<T>)>>, failed: &mut Vec<String>) -> Vec<T> {
    let mut done = vec![];
    for (table, r) in futures::stream::iter(jobs).buffer_unordered(4).collect::<Vec<_>>().await {
        match r {
            Ok(v) => done.push(v),
            Err(e) => failed.push(format!("{table}: {e:#}")),
        }
    }
    done
}

impl App {
    pub fn log(&self) -> anyhow::Result<&Log> { self.log.as_deref().ok_or_else(|| anyhow::anyhow!("read-only node")) }

    /// Run a query: across the cluster when the tables are big (`spread`: "1" always, "0" never).
    pub async fn query(&self, query: &str, spread: Option<&str>) -> anyhow::Result<Vec<RecordBatch>> {
        match self.query_as(&crate::sys::planned(query)?, spread, false).await {
            // (`*` left as it is when `sys::hide`'s EXCLUDE names a column its table lacks: a
            // stored view's, say)
            Err(e) if crate::query::handed().is_none() && [crate::sys::mentioned, crate::query::names_deleted].iter().any(|m| m(query) && m(&format!("{e:#}"))) => self.query_as(&crate::asof::as_of(query)?, spread, false).await, // (an answer half sent is never run again)
            r => r,
        }
    }

    /// Run a query as rewritten (`sys::planned`: ASOF JOIN as DataFusion can plan it); `files`:
    /// it may read files on this machine (`owner`). Its work — over 100 KB of it, planning and
    /// running a query — is made on the heap, here: a caller waiting for it holds a pointer, so
    /// procedures calling procedures 16 deep don't run out of stack.
    #[inline(never)]
    pub fn query_as<'a>(&'a self, query: &'a str, spread: Option<&'a str>, files: bool) -> futures::future::BoxFuture<'a, anyhow::Result<Vec<RecordBatch>>> {
        Box::pin(crate::ext::listing(self.query_listed(query, spread, files))) // (every door: files listed once a statement)
    }

    /// Whether `query` must run on this node alone: rows sent with the request are here only; so are
    /// the session's temporary tables, its transaction, settings and variables, a past it reads, and
    /// a Python table function's call; and a user granted some tables has its grants checked where
    /// the query is planned, here. So does a view that says `current_schema()` read under a path of
    /// the session's own: the other nodes read it with no session, as its reader's path says. And
    /// so do a session's queries while it holds temporary secrets, which only this node has.
    pub async fn here_only(&self, query: &str) -> bool {
        crate::query::sent() || crate::temp::mentioned(query) || crate::temp::holds_secrets() || crate::past_names::mentioned(query) || crate::txn::open() || crate::settings::any() || crate::vars::mentioned(query) || crate::routines::pinned(&self.lake, query).await || crate::auth::limited().is_some()
            || (crate::path::current(&self.lake).is_some() && crate::path::reads_current(&self.lake, query).await)
    }

    /// A query on one of the node's turns (`TURNS`): a query run inside another one this task runs
    /// (a procedure's, a view's) is served by the turn its caller holds, never waiting for one.
    async fn query_listed(&self, query: &str, spread: Option<&str>, files: bool) -> anyhow::Result<Vec<RecordBatch>> {
        if TURN.try_with(|_| ()).is_ok() {
            return self.run_query(query, spread, files, false).await; // (a query inside another one: never the door's answer)
        }
        let _turn = turn().await?; // (held until the answer is made)
        TURN.scope((), self.run_query(query, spread, files, true)).await
    }

    /// `top`: the query a door sent (it took a turn), whose answer may go to the door in pieces (`query::collect_or_send`).
    async fn run_query(&self, query: &str, spread: Option<&str>, files: bool, top: bool) -> anyhow::Result<Vec<RecordBatch>> {
        use crate::metrics::{add, QUERIES, QUERY_ERRORS, QUERY_US, SPREAD};
        if crate::pg_catalog::asked(query) {
            return crate::pg_catalog::over_http(&self.lake, query).await; // (a request that speaks Postgres, reading its catalog: `?catalog=postgres`)
        }
        let start = std::time::Instant::now();
        crate::history::read_at(self.lake.visible()); // (its answer again: `t AT (VERSION => n)`)
        let explained = crate::explain::asked(query);
        let top = top && !explained; // (an explained query's operators' rows are kept whole)
        let run = async {
            let nodes = if spread == Some("0") || self.here_only(query).await { vec![] } else { self.cluster.nodes() };
            // (boxed, and only with nodes to spread over: its future is 300 KB, which every query on one node carried)
            let spread_out = match nodes.len() > 1 {
                true => Box::pin(crate::spmd::query(&self.lake, &nodes, &self.cluster.addr, query, spread == Some("1"))).await,
                false => Ok(None),
            };
            match spread_out {
                Ok(Some(batches)) => {
                    crate::guard::ran_spread(query, start.elapsed());
                    crate::history::spread(nodes.len());
                    return Ok((batches, true));
                }
                Ok(None) => {}
                Err(e) => eprintln!("distributed query failed, running it here: {e:#}"),
            }
            let here = std::time::Instant::now();
            let ctx = session(&self.lake, query, "").await?;
            crate::query::mark("session with its tables");
            let ctx = if files { ctx.enable_url_table() } else { ctx };
            crate::explain::prepare(&ctx, query); // (each operator's expected rows: `explain::expected`)
            let out = crate::query::frugally(&ctx, |ctx| async move {
                let df = crate::engine::sql(&ctx, query).await?;
                crate::query::mark("parsed to logical plan");
                let task = Arc::new(df.task_ctx());
                let (state, logical) = df.into_parts();
                let logical = state.optimize(&logical)?; // (as `create_physical_plan`, the plan it optimized kept: what its filters were about, `learned.rs`)
                crate::query::mark("optimized plan");
                let plan = state.query_planner().create_physical_plan(&logical, &state).await?;
                crate::query::mark("physical plan");
                let out = if top { crate::query::collect_or_send(plan.clone(), task).await? } else { crate::query::collect(plan.clone(), task).await? };
                crate::query::mark("run to the end");
                crate::history::planned(&logical, &plan); // (its shape; a slow statement's plan, with what each operator did; what it learned)
                // (no rows: still its columns, with the types its batches have, which the physical plan gives: not the planner's)
                anyhow::Ok(crate::engine::kept(out, &plan))
            }).await?;
            // How fast queries go here (`guard.rs`): every query's time, so a quick one is known to be
            // quick and stays here, and the bytes it read only for one long enough to say what rate
            // this node reads at. The time is taken before they are looked up, which isn't the query.
            let took = here.elapsed(); // (an answer handed in pieces: its time includes the client's reading, so it says nothing)
            if self.cluster.nodes().len() > 1 && crate::query::handed().is_none() {
                let bytes = match took >= Duration::from_millis(20) {
                    true => crate::spmd::reads_sql(&self.lake, query).await.unwrap_or(0),
                    false => 0,
                };
                crate::guard::ran_here(query, bytes, took);
            }
            Ok((out, false))
        };
        let (out, planned) = crate::learned::planning(query, run).await; // (planned with what runs learned, as its runs so far say)
        add(&QUERIES, 1);
        add(&QUERY_US, start.elapsed().as_micros() as u64);
        match out {
            Ok((batches, spread)) => {
                let handed = crate::query::handed(); // (rows sent in pieces: the batches went to the door, which holds none here)
                if handed.is_none() {
                    crate::learned::ran(query, planned, start.elapsed()); // (never worse: facts that made it slower twice are set aside)
                }
                add(&SPREAD, spread as u64);
                crate::history::rows(handed.unwrap_or_else(|| batches.iter().map(|b| b.num_rows() as u64).sum()));
                let batches = if explained { crate::explain::answered(batches)? } else { batches }; // (files as SQL named them: a node's EXPLAIN and a page's alike)
                Ok(batches.into_iter().map(crate::answer::compact).collect()) // (an answer kept or sent holds only its own strings)
            }
            Err(e) => {
                add(&QUERY_ERRORS, 1);
                Err(e)
            }
        }
    }

    /// Record an INSERT's files: here on the leader, or forwarded to it.
    pub async fn record_files(&self, f: crate::write::Files) -> anyhow::Result<Value> {
        let Some(seq) = &self.seq else {
            let r = crate::cluster::http().post(crate::tls::url(&format!("{}/cluster/files", self.cluster.leader.addr))).json(&f).send().await?;
            anyhow::ensure!(r.status().is_success(), "leader: {}", r.text().await?);
            return Ok(r.json().await?);
        };
        let _guard = self.lock.lock().await;
        crate::write::record(&self.lake, f, seq).await
    }

    /// Another engine's append (`iceberg_rest::update`), recorded by the leader under the lake's lock.
    pub async fn record_iceberg(&self, c: Vec<crate::iceberg::Commit>) -> anyhow::Result<Value> {
        let Some(seq) = &self.seq else { return crate::write::post(&self.cluster.leader.addr, &crate::write::Request::Iceberg(c)).await };
        let _guard = self.lock.lock().await;
        let nodes = if self.cluster.nodes().is_empty() { vec![self.cluster.addr.clone()] } else { self.cluster.nodes() };
        crate::iceberg::record(&self.lake, seq, c, &nodes, &self.cluster.addr, self.retain_ms).await
    }

    /// Where this node gets commit numbers (`log::To::reserve`): its own sequencer, or the leader's.
    pub fn to(&self) -> crate::log::To {
        match &self.seq {
            Some(seq) => crate::log::To::Local(seq.clone()),
            None => crate::log::To::Leader(self.cluster.leader.addr.clone()),
        }
    }

    /// Tier the tables with at least `min_rows` rows in the log (0: all of them), publish them
    /// for other engines, then expire what everything has consumed (leader).
    pub async fn tier_all(&self, min_rows: u64) -> anyhow::Result<u64> {
        if crate::backup::frozen(&self.lake) {
            return crate::backup::tend(&self.lake).await.map(|_| 0); // (its tables never change: only its clones' pins are kept)
        }
        let _guard = self.lock.lock().await;
        let _tiering = self.seq.as_ref().map(|s| s.tiering()); // (writers paused by backpressure stay paused: `log::Pace`)
        let start = std::time::Instant::now();
        let hwm = *self.lake.hwm.borrow();
        let tables = self.lake.cat.scan::<TableMeta>("t/", "t0").await?;
        let backlog = crate::tier::backlog(&self.lake, tables.iter().map(|(_, m)| m.tiered).min().unwrap_or(hwm), None).await?;
        let nodes = if self.cluster.nodes().is_empty() { vec![self.cluster.addr.clone()] } else { self.cluster.nodes() };
        // Up to 4 tables at once: on object storage each round is a few storage round trips, so
        // tables side by side finish in the time of one (memory stays bounded: ≤4M rows a job).
        let busy: Vec<String> = tables.iter().map(|(k, _)| k[2..].to_string()).filter(|t| backlog.get(t).copied().unwrap_or(0) >= min_rows).collect();
        // Tables with files to merge or seal, busy or not (bulk INSERTs don't go through the log).
        let heavy = |m: &TableMeta| m.files.iter().any(|f| f.deleted > 0 && f.deleted * 10 >= f.rows); // (`tier::maintain` rewrites them)
        let untidy = tables.iter().filter(|(k, m)| (m.files.len() >= 8 || heavy(m)) && !busy.contains(&k[2..].to_string())).map(|(k, _)| k[2..].to_string());
        let untidy: Vec<String> = busy.iter().cloned().chain(untidy).collect();
        // (tables whose changed rows may wait in files: `tier::purge` says whether it's time)
        let changed: Vec<String> = tables.iter().filter(|(_, m)| m.changed && m.purged() < m.tiered).map(|(k, _)| k[2..].to_string()).collect();
        if untidy.is_empty() && changed.is_empty() {
            return Ok(0); // (the pressure check, most of the time: don't hold the lock for nothing)
        }
        // Each table's work is its own: one that fails (rows its files can't hold, say) is named
        // once the round is done, and stops no other table's.
        let (nodes, mut failed) = (&nodes, vec![]);
        let per_table: Vec<_> = busy.iter().map(|t| async move { (t, self.tier_one(t, hwm, nodes).await) }).collect();
        let rows: u64 = each(per_table, &mut failed).await.iter().sum();
        // Changed rows out of the files (published tables: before they are published).
        let purge: Vec<_> = changed.iter().map(|t| async move { (t, crate::tier::purge(&self.lake, t, nodes, &self.cluster.addr, self.retain_ms, false).await) }).collect();
        each(purge, &mut failed).await;
        let tiered = start.elapsed();
        // A table whose publish fails is named by `publish_all`; it mustn't stop the merges and the
        // retention below (storage would grow with no way out), so its error joins `failed` and the
        // round carries on.
        if let Err(e) = crate::delta::publish_all(&self.lake).await { // what other engines read, as soon as it's tiered
            failed.push(format!("publish: {e:#}"));
            crate::metrics::job_failed("publish");
        }
        let first_publish = start.elapsed();
        // Then merges and compactions (published too, once done).
        let maintain: Vec<_> = untidy.iter().map(|t| async move { (t, crate::tier::maintain(&self.lake, t, nodes, &self.cluster.addr, false).await) }).collect();
        if each(maintain, &mut failed).await.contains(&true) {
            if let Err(e) = crate::delta::publish_all(&self.lake).await {
                failed.push(format!("publish: {e:#}"));
                crate::metrics::job_failed("publish");
            }
        }
        let published = start.elapsed();
        // Retention every 10 s is plenty, and keeps its deletes off the path to fresh Delta versions.
        crate::per_node!(static EXPIRED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0));
        let now = crate::log::now_ms();
        if now - EXPIRED.load(std::sync::atomic::Ordering::Relaxed) >= 10_000 {
            EXPIRED.store(now, std::sync::atomic::Ordering::Relaxed);
            expire(&self.lake, self.retain_ms).await?;
        }
        if start.elapsed() >= Duration::from_secs(5) {
            // one line when it drags
            let (publish, merges, expire) = (first_publish - tiered, published - first_publish, start.elapsed() - published);
            eprintln!("slow tiering: {rows} rows in {:?} (tables {tiered:?}, publish {publish:?}, merges {merges:?}, expire {expire:?})", start.elapsed());
        }
        anyhow::ensure!(failed.is_empty(), "{}", failed.join("; "));
        Ok(rows)
    }

    /// `CHECKPOINT`: every table's log into Parquet, and the catalog written down (the leader's
    /// work: a follower asks it).
    pub async fn checkpoint(&self) -> anyhow::Result<Value> {
        if self.seq.is_none() {
            let r = crate::cluster::http().post(crate::tls::url(&format!("{}/sql", self.cluster.leader.addr))).body("CHECKPOINT").send().await?;
            anyhow::ensure!(r.status().is_success(), "the leader: {}", r.text().await?);
            return Ok(r.json().await?);
        }
        let rows = self.tier_all(0).await?;
        let (mut purged, nodes) = (vec![], if self.cluster.nodes().is_empty() { vec![self.cluster.addr.clone()] } else { self.cluster.nodes() });
        {
            let _guard = self.lock.lock().await;
            for (k, m) in self.lake.cat.scan::<TableMeta>("t/", "t0").await? {
                // (changed rows out of the files; keyed tables that publish, compacted)
                let done = match m.key.is_empty() {
                    true => m.changed && crate::tier::purge(&self.lake, &k[2..], &nodes, &self.cluster.addr, self.retain_ms, true).await?,
                    false => crate::tier::maintain(&self.lake, &k[2..], &nodes, &self.cluster.addr, true).await?,
                };
                if done {
                    purged.push(k[2..].to_string());
                }
            }
            if !purged.is_empty() {
                crate::delta::publish_all(&self.lake).await?; // (publishing happens under the lock: `iceberg::record`)
            }
        }
        self.lake.cat.checkpoint().await?;
        Ok(j!({"checkpoint": true, "rows_tiered": rows, "rewritten": purged}))
    }

    /// One table's log up to `hwm`, a chunk at a time.
    async fn tier_one(&self, table: &str, hwm: u64, nodes: &[String]) -> anyhow::Result<u64> {
        let mut rows = 0;
        loop {
            let (n, done) = tier_table(&self.lake, table, hwm, nodes, &self.cluster.addr).await?;
            rows += n;
            if done || n == 0 {
                return Ok(rows);
            }
        }
    }

    pub async fn run_tasks(&self) -> anyhow::Result<()> {
        if crate::backup::frozen(&self.lake) {
            return Ok(()); // (a backup's views and tasks follow nothing: ADR-070)
        }
        crate::tasks::run_all(&self.lake, &self.cluster, self.log()?).await?;
        match &self.seq {
            Some(seq) => {
                crate::views::fill_all(&self.lake, seq, self.log()?, &self.lock).await?; // (the leader: views filled from the rows already there)
                crate::views::join_all(&self.lake, self.log()?).await?; // (and stream joins)
                crate::rerun::run_all(&self.lake, seq, &self.lock).await // (and views kept by key: each in a task of its own)
            }
            None => Ok(()),
        }
    }
}

/// An answer `App::remembered` gives: the bytes another run kept (remembered, or shared by the
/// identical queries asking at the same moment), or the answer this caller's own run made.
pub(crate) enum Kept<T> {
    Bytes(bytes::Bytes),
    Own(T),
}

impl App {
    /// The version an answer to `query` is remembered under (`Results`), as HTTP and the Postgres port
    /// remember them: same query, same catalog version, same answer. None when it may answer otherwise
    /// next time, or for a user granted some tables (planned as it may read them, never answered from
    /// what another read, nor kept: its grants may change).
    pub(crate) async fn remembers(&self, query: &str) -> Option<u64> {
        let asks = |text: &str| {
            // (AT (OFFSET => -n) is n seconds before now: a moment that moves. Expanded, a table as it was
            // is named `"at:…"`, which holds its AT (…) as written: `past_names::spelled`)
            static OFFSET: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\bAT\s*\(\s*OFFSET\s*=>").expect("a regex"));
            let text = &format!("{text} {}", crate::past_names::spelled(text));
            let q = text.to_lowercase();
            (q.contains("offset") && OFFSET.is_match(text)) || !crate::ext_names::names(text).is_empty() || ["now()", "today(", "random(", "current_", "uuid(", "explain", "pondra.runs", "pondra.tasks", "pondra.audit", "pondra$audit", "pondra.history", "pondra$history", "pondra.loads", "pondra.learned", "pondra.variables", "pondra.find", "pondra.parts", "files(", "nextval(", "currval(", "setval("].iter().any(|f| q.contains(f)) // (files outside the lake change on their own; `files()` lists objects put since)
        };
        let volatile = crate::auth::limited().is_some() || asks(query) || crate::pg_catalog::asked(query) // (Postgres's catalog: another answer to the same text)
            || crate::temp::mentioned(query) // (the session's temporary tables change without a commit)
            || crate::settings::any() // (and its settings may change the answer)
            || crate::routines::with_views(&self.lake, query).await.map_or(true, |t| t.len() > query.len() && asks(&t)) // (and so may a view it reads: `now()`, files)
            || crate::routines::volatile(&self.lake, query).await // (a Python function may answer differently each time)
            || (crate::path::current(&self.lake).is_some() && crate::path::reads_current(&self.lake, query).await); // (a view's current_schema() is its reader's: not one answer for every session)
        match volatile {
            true => None,
            false => self.lake.version_for(query).await,
        }
    }

    /// The answer under `key` at `version` (`remembers`): remembered, or made by `run`, once for
    /// everyone asking at the same moment. Each run covers everyone who arrived before it started, so
    /// no one gets an answer older than the lake they arrived at. A run gives its own caller's answer
    /// and the bytes to keep and share, when it has them; one that leaves none (an error, or a big
    /// answer) leaves each caller it covered to run on its own. Its own caller takes its answer as it
    /// is: the Postgres port never decodes the bytes it just encoded.
    pub(crate) async fn remembered<T, F, Fut>(&self, key: String, query: &str, version: u64, stale: Option<Duration>, run: F) -> anyhow::Result<Kept<T>>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<(Option<bytes::Bytes>, T)>>,
    {
        if let Some(body) = self.results.get(&key, version, stale) {
            return Ok(Kept::Bytes(body));
        }
        let flight = self.results.flight(&key);
        let ticket = flight.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        let mut slot = flight.1.lock().await;
        if slot.0 >= ticket {
            let Some(body) = &slot.1 else {
                drop(slot); // (each it covered runs on its own, beside the others)
                return Ok(Kept::Own(run().await?.1));
            };
            return Ok(Kept::Bytes(body.clone()));
        }
        (slot.0, slot.1) = (flight.0.load(std::sync::atomic::Ordering::Relaxed), None);
        let version = self.lake.version_for(query).await; // (read after `covers`: this run reads at least this)
        crate::query::mark("answer kept?");
        let run = run();
        tokio::pin!(run);
        let (keep, own) = tokio::select! {
            biased;
            out = &mut run => out?,
            () = crate::query::when_handed() => {
                // (its answer goes in pieces to its own caller as the query runs: those it covered run on their own
                // now, not once the client has read it all)
                drop(slot);
                return Ok(Kept::Own(run.await?.1));
            }
        };
        if let Some(body) = keep {
            if let Some(v) = version {
                self.results.put(key, v, body.clone());
            }
            slot.1 = Some(body);
        }
        Ok(Kept::Own(own))
    }
}
