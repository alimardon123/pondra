//! HTTP API. Every node ingests (`/append`), answers SQL and streams changes (`/watch`); metadata
//! writes (tables, views, tasks, bulk inserts, tiering) go to the leader (followers forward them).
use crate::cluster::Cluster;
use crate::log::{decode_flush, Ack, Log, Sequencer, Src};
use crate::query::{latest_sql, raw, schema, session, tail};
use crate::store::*;
use crate::tasks::Task;
use crate::tier::{expire, tier_table};
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::{compute::concat_batches, json as arrow_json, record_batch::RecordBatch, util::pretty::pretty_format_batches};
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;
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
        if body.len() > 1 << 20 {
            return; // big results aren't what dashboards repeat
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
}

pub fn router(app: App) -> Router {
    let leader_only = Router::new()
        .route("/tables/{name}", post(create_table))
        .route("/views/{name}", post(create_view))
        .route("/tasks/{name}", post(create_task))
        .route("/functions/{name}", post(create_function).delete(drop_function))
        .route("/tier", post(tier_now))
        .route_layer(middleware::from_fn_with_state(app.clone(), to_leader));
    Router::new()
        .merge(leader_only)
        .route("/append/{name}", post(append))
        .route("/insert/{name}", post(insert))
        .route("/cluster/files", post(files))
        .route("/sql", post(sql))
        .route("/mcp", post(crate::mcp::handle))
        .route("/files/{*path}", put(put_file).get(get_file))
        .route("/lookup/{name}/{key}", get(lookup))
        .route("/watch/{name}", get(watch))
        .route("/functions", get(list_functions))
        .route("/stats", get(stats))
        .route("/metrics", get(|State(app): State<App>| async move { crate::metrics::render(&app).await.map_err(E) }))
        .route("/cluster/commit", post(commit))
        .route("/cluster/log", get(feed))
        .route("/cluster/stage", post(stage))
        .route("/cluster/shuffle", get(bucket))
        .route("/cluster/job", post(job))
        .route("/cluster/beat", post(beat))
        .route("/cluster/ack", post(ack))
        .route("/cluster/replica", get(replica))
        .route("/cluster/leader", get(|State(app): State<App>| async move { Json(app.cluster.leader_status()) }))
        .route("/cluster/kafka", get(|| async { Json(crate::kafka::me()) }))
        .merge(crate::iceberg::rest())
        .layer(axum::extract::DefaultBodyLimit::max(1 << 30)) // batches up to 1 GiB
        .layer(middleware::from_fn_with_state(app.clone(), guard))
        .with_state(app)
}

/// `POST /functions/<name>`: a function this lake has, run by an Arrow Flight server of your own
/// (see `udf.rs`): `{"flight": "http://host:port", "args": ["Binary"], "returns": "Utf8"}`.
/// `DELETE /functions/<name>` takes it away; `GET /functions` lists them.
async fn create_function(State(app): State<App>, Path(name): Path<String>, body: Bytes) -> Result<Json<Value>, E> {
    let udf: crate::udf::Udf = serde_json::from_slice(&body).map_err(|e| E(anyhow::anyhow!("{e}: {{\"flight\": \"http://host:port\", \"args\": [\"Binary\"], \"returns\": \"Utf8\"}}")))?;
    for t in udf.args.iter().chain([&udf.returns]) {
        crate::query::dtype(t)?;
    }
    app.lake.cat.commit(vec![(crate::udf::key(&name), serde_json::to_vec(&udf)?)], &[]).await?;
    Ok(Json(j!({"function": name})))
}

async fn drop_function(State(app): State<App>, Path(name): Path<String>) -> Result<Json<Value>, E> {
    app.lake.cat.commit(vec![], &[crate::udf::key(&name)]).await?;
    Ok(Json(j!({"dropped": name})))
}

/// `PUT /files/<path>`: an object in the lake next to the tables — an image, a PDF, a model —
/// for `files('…')` to list and `file_read(path)` to read (see `files.rs`). Objects are never
/// overwritten: a path that exists is an error.
async fn put_file(State(app): State<App>, Path(path): Path<String>, body: Bytes) -> Result<Json<Value>, E> {
    let (path, bytes) = (format!("files/{}", path.trim_start_matches('/')), body.len());
    app.lake.put(&path, body.to_vec()).await?;
    Ok(Json(j!({"path": path, "bytes": bytes})))
}

/// `GET /files/<path>`: that object's bytes.
async fn get_file(State(app): State<App>, Path(path): Path<String>) -> Result<Response, E> {
    let bytes = app.lake.object(&format!("files/{}", path.trim_start_matches('/'))).await?;
    Ok(([("content-type", "application/octet-stream")], bytes).into_response())
}

/// `GET /functions`: the lake's own functions, by name.
async fn list_functions(State(app): State<App>) -> Result<Json<Value>, E> {
    let fns = app.lake.cat.scan::<crate::udf::Udf>("f/", "f0").await?;
    Ok(Json(j!(fns.into_iter().map(|(k, u)| (k[2..].to_string(), serde_json::to_value(u).unwrap_or_default())).collect::<serde_json::Map<_, _>>())))
}

/// Tokens (see `auth.rs`): the caller's role must cover the route; handlers see it too.
async fn guard(State(app): State<App>, mut req: Request, next: Next) -> Response {
    let token = req.headers().get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    let role = app.auth.role(token);
    if role < crate::auth::Auth::needed(req.uri().path(), req.method().as_str()) {
        return (StatusCode::UNAUTHORIZED, "this needs a token with more rights").into_response();
    }
    req.extensions_mut().insert(role);
    next.run(req).await
}

/// Followers forward metadata writes to the leader, unchanged.
async fn to_leader(State(app): State<App>, req: Request, next: Next) -> Response {
    if app.seq.is_some() {
        return next.run(req).await;
    }
    if app.cluster.reader {
        return E(anyhow::anyhow!("read-only node")).into_response();
    }
    proxy(&app.cluster.leader.addr, req).await.unwrap_or_else(|e| E(e).into_response())
}

async fn proxy(addr: &str, req: Request) -> anyhow::Result<Response> {
    let (parts, body) = req.into_parts();
    let url = format!("http://{addr}{}", parts.uri.path_and_query().map_or("", |p| p.as_str()));
    let mut out = crate::cluster::http().request(parts.method, url).body(axum::body::to_bytes(body, usize::MAX).await?);
    if let Some(ct) = parts.headers.get("content-type") {
        out = out.header("content-type", ct);
    }
    let res = out.send().await?;
    let (status, ct) = (res.status(), res.headers().get("content-type").cloned());
    let mut resp = Response::new(Body::from(res.bytes().await?));
    *resp.status_mut() = status;
    if let Some(ct) = ct {
        resp.headers_mut().insert("content-type", ct);
    }
    Ok(resp)
}

// ---------------------------------------------------------------- cluster internals

#[derive(Deserialize)]
struct BeatParams {
    from: String,
}

async fn beat(State(app): State<App>, Query(p): Query<BeatParams>) -> Result<Json<(u64, Vec<String>)>, StatusCode> {
    // Test hook: followers listed in the file $PONDRA_DROP_BEATS get no answer (a broken link to the leader).
    let dropped = std::env::var("PONDRA_DROP_BEATS").map(|f| std::fs::read_to_string(f).unwrap_or_default()).unwrap_or_default();
    if dropped.lines().any(|a| a == p.from) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Json(app.cluster.beat(p.from)))
}

/// A follower's flush, to be sequenced (leader only).
async fn commit(State(app): State<App>, body: Bytes) -> Result<Json<crate::log::Outcome>, E> {
    let seq = app.seq.as_ref().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
    Ok(Json(seq.submit(decode_flush(body)?).await?))
}

/// This node's share of a distributed query, sent a piece at a time (a big share is on disk).
async fn stage(State(app): State<App>, Json(slice): Json<crate::spmd::Slice>) -> Result<Response, E> {
    let (shape, parts) = crate::spmd::stage(&app.lake, &slice).await?;
    let done = slice.shuffle.is_none().then(|| crate::spill::Gone(slice.id.clone()));
    Ok(Body::from_stream(crate::spmd::reply(&shape, parts, done)).into_response())
}

#[derive(Deserialize)]
struct BucketParams {
    id: String,
    exchange: usize,
    to: usize,
    #[serde(default)]
    drop: bool, // the coordinator gave up on this shuffle: forget it and delete what it spilled
}

/// A shuffle bucket this node keeps for another node (see `spmd.rs`).
async fn bucket(Query(p): Query<BucketParams>) -> Result<Response, E> {
    if p.drop {
        crate::spmd::forget(&p.id);
        return Ok(Body::empty().into_response());
    }
    let spill = crate::spmd::bucket(&p.id, p.exchange, p.to)?;
    Ok(Body::from_stream(spill.framed()).into_response()) // (a piece at a time: a big bucket is on disk)
}

/// A share of the leader's data work (tiering, merging, compaction): the files written.
async fn job(State(app): State<App>, Json(job): Json<crate::tier::Job>) -> Result<Json<Vec<DataFile>>, E> {
    Ok(Json(crate::tier::run_job(&app.lake, job).await?))
}

/// The commit stream (see `Frame`): who leads, then the recent frames, then every new one.
async fn feed(State(app): State<App>) -> Response {
    let cat = &app.lake.cat;
    let start = Frame::Start { term: app.cluster.leader.n, replicated: cat.replicas > 1 };
    let (recent, rx) = cat.subscribe();
    let live = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.ok().map(|f| (f, rx)) }); // a lagging follower reconnects
    let frames = futures::stream::iter([start].into_iter().chain(recent)).chain(live).map(|f| Ok::<_, std::io::Error>(f.encode()));
    Body::from_stream(frames).into_response()
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct AckParams {
    from: String,
    term: u64,
    first: u64,
    upto: u64,
    after: u64,
}

/// Leader: a follower holds a run of changes (replicated commits). Acks meant for another
/// leader's term don't count.
async fn ack(State(app): State<App>, Query(p): Query<AckParams>) -> StatusCode {
    if !app.cluster.is_leader() || p.term != app.cluster.leader.n {
        return StatusCode::CONFLICT;
    }
    app.lake.cat.ack(&p.from, p.first, p.upto);
    StatusCode::OK
}

/// A new leader taking over collects what this node holds beyond the bucket (`replica::recover`).
async fn replica(State(app): State<App>, Query(p): Query<AckParams>) -> Result<Vec<u8>, StatusCode> {
    Ok(app.replica.as_ref().ok_or(StatusCode::NOT_FOUND)?.serve(p.term, p.after))
}

impl App {
    pub fn log(&self) -> anyhow::Result<&Log> { self.log.as_deref().ok_or_else(|| anyhow::anyhow!("read-only node")) }

    /// Run a query: across the cluster when the tables are big (`spread`: "1" always, "0" never).
    pub async fn query(&self, query: &str, spread: Option<&str>) -> anyhow::Result<Vec<RecordBatch>> {
        use crate::metrics::{add, QUERIES, QUERY_ERRORS, QUERY_US, SPREAD};
        let start = std::time::Instant::now();
        let run = async {
            let nodes = if spread == Some("0") { vec![] } else { self.cluster.nodes() };
            match crate::spmd::query(&self.lake, &nodes, &self.cluster.addr, query, spread == Some("1")).await {
                Ok(Some(batches)) => return Ok((batches, true)),
                Ok(None) => {}
                Err(e) => eprintln!("distributed query failed, running it here: {e:#}"),
            }
            let run = |frugal: bool| async move {
                let ctx = session(&self.lake, query, "").await?;
                if frugal {
                    // Hash joins can't spill, sort-merge joins can; sorts keep less aside to merge.
                    let state = ctx.state_ref();
                    let mut state = state.write();
                    let o = state.config_mut().options_mut();
                    (o.optimizer.prefer_hash_join, o.execution.sort_spill_reservation_bytes) = (false, 1 << 20);
                }
                anyhow::Ok(ctx.sql_with_options(query, crate::query::read_only()).await?.collect().await?)
            };
            match run(false).await {
                Err(e) if format!("{e:#}").contains("Resources exhausted") => Ok((run(true).await?, false)), // out of memory: try again frugally
                r => Ok((r?, false)),
            }
        };
        let out = run.await;
        add(&QUERIES, 1);
        add(&QUERY_US, start.elapsed().as_micros() as u64);
        match out {
            Ok((batches, spread)) => {
                add(&SPREAD, spread as u64);
                Ok(batches)
            }
            Err(e) => {
                add(&QUERY_ERRORS, 1);
                Err(e)
            }
        }
    }

    /// Record an INSERT's files: here on the leader, or forwarded to it.
    pub async fn record_files(&self, f: crate::write::Files) -> anyhow::Result<Value> {
        if self.seq.is_none() {
            let r = crate::cluster::http().post(format!("http://{}/cluster/files", self.cluster.leader.addr)).json(&f).send().await?;
            anyhow::ensure!(r.status().is_success(), "leader: {}", r.text().await?);
            return Ok(r.json().await?);
        }
        let _guard = self.lock.lock().await;
        crate::write::record(&self.lake, f).await
    }

    /// Tier the tables with at least `min_rows` rows in the log (0: all of them), publish them
    /// for other engines, then expire what everything has consumed (leader).
    pub async fn tier_all(&self, min_rows: u64) -> anyhow::Result<u64> {
        let _guard = self.lock.lock().await;
        let start = std::time::Instant::now();
        let hwm = *self.lake.hwm.borrow();
        let tables = self.lake.cat.scan::<TableMeta>("t/", "t0").await?;
        let backlog = crate::tier::backlog(&self.lake, tables.iter().map(|(_, m)| m.tiered).min().unwrap_or(hwm), None).await?;
        let nodes = if self.cluster.nodes().is_empty() { vec![self.cluster.addr.clone()] } else { self.cluster.nodes() };
        // Up to 4 tables at once: on object storage each round is a few storage round trips, so
        // tables side by side finish in the time of one (memory stays bounded: ≤4M rows a job).
        let busy: Vec<String> = tables.iter().map(|(k, _)| k[2..].to_string()).filter(|t| backlog.get(t).copied().unwrap_or(0) >= min_rows).collect();
        // Tables with files to merge or seal, busy or not (bulk INSERTs don't go through the log).
        let untidy = tables.iter().filter(|(k, m)| m.files.len() >= 8 && !busy.contains(&k[2..].to_string())).map(|(k, _)| k[2..].to_string());
        let untidy: Vec<String> = busy.iter().cloned().chain(untidy).collect();
        if untidy.is_empty() {
            return Ok(0); // (the pressure check, most of the time: don't hold the lock for nothing)
        }
        let per_table: Vec<_> = busy.iter().map(|t| self.tier_one(t, hwm, &nodes)).collect();
        let rows: u64 = futures::stream::iter(per_table).buffer_unordered(4).try_collect::<Vec<u64>>().await?.iter().sum();
        let tiered = start.elapsed();
        crate::delta::publish_all(&self.lake).await?; // what other engines read, as soon as it's tiered
        let first_publish = start.elapsed();
        // Then merges and compactions (published too, once done).
        let maintain: Vec<_> = untidy.iter().map(|t| crate::tier::maintain(&self.lake, t, &nodes, &self.cluster.addr)).collect();
        if futures::stream::iter(maintain).buffer_unordered(4).try_collect::<Vec<bool>>().await?.contains(&true) {
            crate::delta::publish_all(&self.lake).await?;
        }
        let published = start.elapsed();
        // Retention every 10 s is plenty, and keeps its deletes off the path to fresh Delta versions.
        static EXPIRED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
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
        Ok(rows)
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

    pub async fn run_tasks(&self) -> anyhow::Result<()> { crate::tasks::run_all(&self.lake, &self.cluster, self.log()?).await }
}

// ---------------------------------------------------------------- the API

/// Body: a table's definition (see `write::TableSpec`): `[["user","Utf8"],…]`, or
/// `{"columns": [...], "key": ["user"], "merge": {...}, "publish": [...], "cluster_by": [...]}`.
async fn create_table(State(app): State<App>, Path(name): Path<String>, body: String) -> Result<Json<Value>, E> {
    let _guard = app.lock.lock().await;
    Ok(Json(crate::write::create_table(&app.lake, &name, &body).await?))
}

#[derive(Deserialize)]
struct AppendParams {
    producer: String,
    seq: u64,
    prev: Option<u64>, // compare-and-swap on the producer's last seq
}

/// Body: NDJSON rows, or an Arrow IPC stream (content-type application/vnd.apache.arrow.stream).
async fn append(State(app): State<App>, Path(name): Path<String>, Query(p): Query<AppendParams>, headers: HeaderMap, body: Bytes) -> Result<Json<Ack>, E> {
    let log = app.log()?;
    let meta: TableMeta = app.lake.cat.get(&table_key(&name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?;
    let schema = schema(&meta.columns)?;
    let arrow = headers.get("content-type").is_some_and(|v| v.as_bytes().starts_with(b"application/vnd.apache.arrow"));
    let batches = if arrow {
        // Columns by name, cast to the table's types (pandas, Polars and Arrow differ in string
        // types); a column left out is null (as in JSON), e.g. `_deleted`, or one added since.
        let ipc = StreamReader::try_new(&body[..], None)?.collect::<Result<Vec<_>, _>>()?;
        ipc.iter().map(|b| crate::query::conform(b, &schema)).collect::<anyhow::Result<Vec<_>>>()?
    } else {
        arrow_json::ReaderBuilder::new(schema.clone()).build(&body[..])?.collect::<Result<Vec<_>, _>>()?
    };
    let batch = concat_batches(&schema, &batches)?;
    Ok(Json(log.append(name, Src { producer: p.producer, seq: p.seq, prev: p.prev }, batch).await?))
}

#[derive(Deserialize)]
struct InsertParams {
    job: String,
}

/// Bulk `INSERT INTO name <body SQL>`: this node runs the query and writes the Parquet (no log);
/// the leader records the files, together with `job` so a retried job is applied once. Creates
/// the table if missing.
async fn insert(State(app): State<App>, Path(name): Path<String>, Query(p): Query<InsertParams>, query: String) -> Result<Json<Value>, E> {
    ensure!(!app.cluster.reader, "read-only node");
    let ctx = session(&app.lake, &query, "").await?;
    match crate::write::write_files(&app.lake, &ctx, &name, &query, &p.job).await? {
        Some(f) => Ok(Json(app.record_files(f).await?)),
        None => Ok(Json(j!({"duplicate": true}))),
    }
}

/// Record an INSERT's files (from another node, or a `pondra sql` somewhere else).
async fn files(State(app): State<App>, Json(f): Json<crate::write::Files>) -> Result<Json<Value>, E> {
    Ok(Json(app.record_files(f).await?))
}

/// Body: `{"source": "events", "target": "per_user", "sql": "SELECT … FROM events …"}`.
async fn create_task(State(app): State<App>, Path(name): Path<String>, body: String) -> Result<Json<Value>, E> {
    let task: Task = serde_json::from_str(&body)?;
    crate::tasks::create(&app.lake, &name, &task).await?;
    Ok(Json(j!({"task": name})))
}

/// Body: the view's SQL, e.g. `SELECT user, sum(amount) AS total, count(*) AS n FROM events GROUP BY user`.
#[derive(Deserialize)]
struct ViewParams {
    window: Option<String>,
    size_secs: Option<u64>,
    lateness_secs: Option<u64>,
}

/// `POST /views/{name}` with the SQL; `?window=w&size_secs=60&lateness_secs=10` also emits each
/// window of column `w` once, final, to `{name}_final` (see `views.rs`).
async fn create_view(State(app): State<App>, Path(name): Path<String>, Query(p): Query<ViewParams>, sql: String) -> Result<Json<Value>, E> {
    let _guard = app.lock.lock().await;
    let emit = p.window.map(|window| crate::views::Emit { window, size_secs: p.size_secs.unwrap_or(60), lateness_secs: p.lateness_secs.unwrap_or(0) });
    crate::views::create(&app.lake, &name, &sql, emit).await?;
    Ok(Json(j!({"view": name})))
}

/// `GET /lookup/{table}/{key}`: the current row of one key, for serving reads — same answer as
/// `SELECT … WHERE key = …`. Upsert tables skip SQL entirely (`serve.rs`); merge tables combine
/// the key's partial rows with a filter + GROUP BY on one thread. Composite keys are
/// comma-separated, in the key's column order.
async fn lookup(State(app): State<App>, Path((name, key)): Path<(String, String)>) -> Result<Response, E> {
    let lake = &app.lake;
    let meta: TableMeta = lake.cat.get(&table_key(&name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?;
    ensure!(!meta.key.is_empty(), "{name} has no key: use /sql");
    if meta.merge.is_empty() && meta.ttl.is_none() {
        let names: Vec<&str> = meta.columns.iter().map(|(c, _)| c.as_str()).collect();
        let row = crate::serve::lookup(lake, &name, &meta, &key).await?.map(|r| r.project(&names.iter().map(|n| r.schema().index_of(n)).collect::<Result<Vec<_>, _>>()?)).transpose()?;
        let mut w = arrow_json::ArrayWriter::new(Vec::new());
        w.write_batches(&row.iter().collect::<Vec<_>>())?;
        w.finish()?;
        let body = if row.is_none() { b"[]".to_vec() } else { w.into_inner() };
        return Ok(([("content-type", "application/json")], body).into_response());
    }
    let mut where_ = vec![];
    for (col, val) in meta.key.iter().zip(key.split(',')) {
        let text = meta.columns.iter().any(|(c, t)| c == col && (t == "Utf8" || t == "LargeUtf8"));
        ensure!(!val.contains('\''), "quote in key");
        where_.push(match text {
            true => format!("\"{col}\" = '{val}'"),
            false => format!("\"{col}\" = {val}"),
        });
    }
    let (ctx, where_) = (lake.session_with(1), where_.join(" AND "));
    ctx.register_table("__raw", raw(lake, &ctx, &name, &meta, None).await?.into_view())?;
    let cols = meta.columns.iter().map(|(c, _)| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
    let sql = match meta.merge.is_empty() {
        // Upsert table: the newest version of the key wins (no window over the whole table).
        true => format!("SELECT * FROM (SELECT {cols} FROM __raw WHERE {where_} ORDER BY \"_ord\" DESC LIMIT 1){}", crate::query::live(&meta)),
        // Merge table: combine that key's partial rows.
        false => format!("{} ", latest_sql(&meta, "__raw", false, false)).replace(" GROUP BY ", &format!(" WHERE {where_} GROUP BY ")),
    };
    let batches = ctx.sql(&sql).await?.collect().await?;
    let mut w = arrow_json::ArrayWriter::new(Vec::new());
    w.write_batches(&batches.iter().collect::<Vec<_>>())?;
    w.finish()?;
    Ok(([("content-type", "application/json")], w.into_inner()).into_response())
}

#[derive(Deserialize)]
struct SqlParams {
    format: Option<String>, // json (default), table (text), arrow (Arrow IPC stream)
    after: Option<u64>,     // read-your-writes: first wait until this node has seen segment `after` (from an ack)
    spread: Option<String>, // "1": run across the cluster even for small tables; "0": only here
    stale_ms: Option<u64>,  // accept a cached result up to this old (see `Results`)
    job: Option<String>,    // writes: a retry with the same job id is applied once
}

async fn sql(State(app): State<App>, Query(p): Query<SqlParams>, role: axum::Extension<crate::auth::Role>, query: String) -> Result<Response, E> {
    if let Some(stmt) = crate::write::parse(&query) {
        app.auth.allows(role.0, &stmt)?;
        return Ok(Json(crate::write::on_node(&app, stmt, p.job.clone()).await?).into_response()); // CREATE / INSERT / UPDATE / DELETE
    }
    if let Some(seg) = p.after {
        let mut hwm = app.lake.hwm.subscribe();
        let _ = tokio::time::timeout(Duration::from_secs(30), async { while app.lake.visible() < seg { hwm.changed().await.ok()?; } Some(()) }).await;
    }
    let format = p.format.as_deref().unwrap_or("json");
    let kind = match format {
        "table" => "text/plain",
        "arrow" => "application/vnd.apache.arrow.stream",
        _ => "application/json",
    };
    let respond = |body: bytes::Bytes| ([("content-type", kind)], body).into_response();
    if format == "json" {
        if let Some(body) = crate::serve::point_sql(&app.lake, &query).await? {
            return Ok(respond(body.into())); // a key lookup: no planning
        }
    }
    // Same query, same catalog version: same answer (unless it asks for the time or randomness).
    let q = query.to_lowercase();
    let volatile = ["now()", "random(", "current_", "uuid(", "explain"].iter().any(|f| q.contains(f));
    let Some(version) = app.lake.cat.version().filter(|_| !volatile) else { return Ok(respond(run_sql(&app, &p, &query).await?)) };
    let key = format!("{format}|{}|{query}", p.spread.as_deref().unwrap_or(""));
    let stale = p.stale_ms.filter(|_| p.after.is_none()).map(Duration::from_millis); // (read-your-writes wins)
    if let Some(body) = app.results.get(&key, version, stale) {
        return Ok(respond(body));
    }
    // Identical queries share one computation at a time. Each covers everyone who arrived before
    // it started, so no one gets an answer older than the lake they arrived at.
    let flight = app.results.flight(&key);
    let ticket = flight.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let mut slot = flight.1.lock().await;
    if let (true, Some(body)) = (slot.0 >= ticket, &slot.1) {
        return Ok(respond(body.clone()));
    }
    (slot.0, slot.1) = (flight.0.load(std::sync::atomic::Ordering::Relaxed), None);
    let version = app.lake.cat.version(); // (read after `covers`: this run reads at least this)
    let body = run_sql(&app, &p, &query).await?;
    if let Some(v) = version {
        app.results.put(key, v, body.clone());
    }
    slot.1 = Some(body.clone());
    Ok(respond(body))
}

/// Run a query (across the cluster if it's worth it) and format the result.
async fn run_sql(app: &App, p: &SqlParams, query: &str) -> anyhow::Result<bytes::Bytes> {
    let batches = app.query(query, p.spread.as_deref()).await?;
    Ok(bytes::Bytes::from(match p.format.as_deref() {
        Some("table") => pretty_format_batches(&batches)?.to_string().into_bytes(),
        Some("arrow") => {
            // Arrow IPC: straight into pandas / Polars / DuckDB, no JSON parsing
            let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
            let mut w = datafusion::arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &schema)?;
            batches.iter().try_for_each(|b| w.write(b))?;
            w.finish()?;
            w.into_inner()?
        }
        _ => {
            let mut w = arrow_json::ArrayWriter::new(Vec::new());
            w.write_batches(&batches.iter().collect::<Vec<_>>())?;
            w.finish()?;
            w.into_inner()
        }
    }))
}

#[derive(Deserialize)]
struct WatchParams {
    after: Option<u64>, // default: from now on (earlier: a replay, as far back as the log is kept)
    #[serde(default)]
    marks: bool, // after each batch of rows, a `{"_after": N}` line: resume with `?after=N`
}

/// New rows of a table as NDJSON, pushed the moment they commit (a view's rows included).
async fn watch(State(app): State<App>, Path(name): Path<String>, Query(p): Query<WatchParams>) -> Response {
    let hwm = app.lake.hwm.subscribe();
    let after = p.after.unwrap_or_else(|| app.lake.visible());
    let marks = p.marks;
    let rows = futures::stream::unfold((app, hwm, after, name), move |(app, mut hwm, after, name)| async move {
        loop {
            hwm.borrow_and_update();
            let now = app.lake.visible();
            if now > after {
                let mark = |mut b: Vec<u8>| {
                    if marks && !b.is_empty() {
                        b.extend(format!("{{\"_after\":{now}}}\n").into_bytes());
                    }
                    b
                };
                let chunk = tail(&app.lake, &name, after, Some(now), false).await.and_then(|b| ndjson(&b)).map(mark);
                return Some((chunk.map_err(|e| std::io::Error::other(e.to_string())), (app, hwm, now, name)));
            }
            hwm.changed().await.ok()?;
        }
    });
    Body::from_stream(rows.filter(|c| std::future::ready(!matches!(c, Ok(b) if b.is_empty())))).into_response()
}

fn ndjson(batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> {
    let mut w = arrow_json::LineDelimitedWriter::new(Vec::new());
    w.write_batches(&batches.iter().collect::<Vec<_>>())?;
    w.finish()?;
    Ok(w.into_inner())
}

async fn tier_now(State(app): State<App>) -> Result<Json<Value>, E> {
    Ok(Json(j!({"rows_tiered": app.tier_all(0).await?})))
}

async fn stats(State(app): State<App>) -> Json<Value> {
    let c = &app.cluster;
    let role = if c.reader { "reader" } else if app.seq.is_some() { "leader" } else { "follower" };
    let mut s = j!({"role": role, "leader": c.leader.addr, "term": c.leader.n, "nodes": c.nodes(),
                    "hwm": *app.lake.hwm.borrow(), "shard_runs": c.shard_runs.load(std::sync::atomic::Ordering::Relaxed)});
    if let Some(seq) = &app.seq {
        s["untiered_rows"] = j!(app.lake.backlog.load(std::sync::atomic::Ordering::Relaxed));
        let mut ms = seq.commit_ms.lock().unwrap().clone();
        ms.sort_by(f64::total_cmp);
        let pct = |p: f64| ms.get(((ms.len() as f64 * p) as usize).min(ms.len().saturating_sub(1))).copied();
        s["commits"] = j!(ms.len());
        s["commit_ms_p50"] = j!(pct(0.5));
        s["commit_ms_p95"] = j!(pct(0.95));
    }
    Json(s)
}

/// `anyhow::ensure!` for handlers.
macro_rules! ensure {
    ($cond:expr, $($msg:tt)*) => {
        if !$cond {
            return Err(E(anyhow::anyhow!($($msg)*)));
        }
    };
}
use ensure;

/// Any error becomes a 500 with its message.
pub struct E(anyhow::Error);
impl<T: Into<anyhow::Error>> From<T> for E {
    fn from(e: T) -> Self { E(e.into()) }
}
impl IntoResponse for E {
    fn into_response(self) -> Response { (StatusCode::INTERNAL_SERVER_ERROR, format!("{:#}", self.0)).into_response() }
}
