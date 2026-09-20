//! HTTP API. Every node ingests (`/append`), answers SQL and streams changes (`/watch`); metadata
//! writes (tables, views, tasks, bulk inserts, tiering) go to the leader (followers forward them).
use crate::cluster::Cluster;
use crate::log::{decode_flush, Ack, Log, Sequencer, Src};
use crate::query::{latest_sql, raw, schema, session, tail};
use crate::store::*;
use crate::tasks::Task;
use crate::tier::{expire, tier_table, write_stream};
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::{compute::concat_batches, json as arrow_json, record_batch::RecordBatch, util::pretty::pretty_format_batches};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json as j, Value};
use std::collections::BTreeMap;
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
}

pub fn router(app: App) -> Router {
    let leader_only = Router::new()
        .route("/tables/{name}", post(create_table))
        .route("/views/{name}", post(create_view))
        .route("/tasks/{name}", post(create_task))
        .route("/insert/{name}", post(insert))
        .route("/tier", post(tier_now))
        .route_layer(middleware::from_fn_with_state(app.clone(), to_leader));
    Router::new()
        .merge(leader_only)
        .route("/append/{name}", post(append))
        .route("/sql", post(sql))
        .route("/lookup/{name}/{key}", get(lookup))
        .route("/watch/{name}", get(watch))
        .route("/stats", get(stats))
        .route("/cluster/commit", post(commit))
        .route("/cluster/log", get(feed))
        .route("/cluster/stage", post(stage))
        .route("/cluster/job", post(job))
        .route("/cluster/beat", post(beat))
        .route("/cluster/leader", get(|State(app): State<App>| async move { Json(app.cluster.leader_status()) }))
        .layer(axum::extract::DefaultBodyLimit::max(1 << 30)) // batches up to 1 GiB
        .with_state(app)
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

/// This node's share of a distributed query.
async fn stage(State(app): State<App>, Json(slice): Json<crate::spmd::Slice>) -> Result<Vec<u8>, E> {
    Ok(crate::spmd::encode_parts(&crate::spmd::stage(&app.lake, &slice).await?)?)
}

/// A share of the leader's data work (tiering, merging, compaction): the files written.
async fn job(State(app): State<App>, Json(job): Json<crate::tier::Job>) -> Result<Json<Vec<DataFile>>, E> {
    Ok(Json(crate::tier::run_job(&app.lake, job).await?))
}

async fn feed(State(app): State<App>) -> Response {
    let (recent, rx) = app.lake.cat.subscribe();
    let live = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.ok().map(|d| (d, rx)) }); // a lagging follower reconnects
    let frames = futures::stream::iter(recent).chain(live).map(|d| Ok::<_, std::io::Error>(d.frame()));
    Body::from_stream(frames).into_response()
}

impl App {
    fn log(&self) -> anyhow::Result<&Log> { self.log.as_deref().ok_or_else(|| anyhow::anyhow!("read-only node")) }

    /// Tier the tables with at least `min_rows` rows in the log (0: all of them), then expire
    /// what everything has consumed (leader).
    pub async fn tier_all(&self, min_rows: u64) -> anyhow::Result<u64> {
        let _guard = self.lock.lock().await;
        let start = std::time::Instant::now();
        let hwm = *self.lake.hwm.borrow();
        let tables = self.lake.cat.scan::<TableMeta>("t/", "t0").await?;
        let backlog = crate::tier::backlog(&self.lake, tables.iter().map(|(_, m)| m.tiered).min().unwrap_or(hwm)).await?;
        let mut rows = 0;
        for (key, _) in tables.iter().filter(|(k, _)| backlog.get(&k[2..]).copied().unwrap_or(0) >= min_rows) {
            let nodes = if self.cluster.nodes().is_empty() { vec![self.cluster.addr.clone()] } else { self.cluster.nodes() };
            loop {
                let (n, done) = tier_table(&self.lake, &key[2..], hwm, &nodes, &self.cluster.addr).await?;
                rows += n;
                if done || n == 0 {
                    break;
                }
            }
        }
        expire(&self.lake, self.retain_ms).await?;
        self.lake.cat.checkpoint().await?;
        if start.elapsed() >= Duration::from_secs(5) {
            eprintln!("slow tiering: {rows} rows in {:?}", start.elapsed()); // one line when it drags
        }
        Ok(rows)
    }

    pub async fn run_tasks(&self) -> anyhow::Result<()> { crate::tasks::run_all(&self.lake, &self.cluster, self.log()?).await }
}

// ---------------------------------------------------------------- the API

#[derive(Deserialize)]
#[serde(untagged)]
enum TableSpec {
    Columns(Vec<(String, String)>),
    Full {
        columns: Vec<(String, String)>,
        #[serde(default)]
        key: Vec<String>,
        #[serde(default)]
        merge: BTreeMap<String, String>,
    },
}

/// Body: `[["user","Utf8"],["amount","Int64"]]`; or `{"columns": [...], "key": ["user"]}` for an
/// upsert table (latest row per key wins; a Boolean `_deleted` column marks deletes); add
/// `"merge": {"total": "sum"}` for a merge table (rows per key combine: sum, min or max).
async fn create_table(State(app): State<App>, Path(name): Path<String>, body: String) -> Result<Json<Value>, E> {
    let (columns, key, merge) = match serde_json::from_str(&body)? {
        TableSpec::Columns(c) => (c, vec![], BTreeMap::new()),
        TableSpec::Full { columns, key, merge } => (columns, key, merge),
    };
    schema(&columns)?; // validate types
    ensure!(merge.values().all(|f| ["sum", "min", "max"].contains(&f.as_str())), "merge functions: sum, min, max");
    ensure!(merge.is_empty() || !key.is_empty(), "a merge table needs a key");
    let _guard = app.lock.lock().await;
    if app.lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_none() {
        app.lake.cat.commit(vec![(table_key(&name), json(&TableMeta { columns, key, merge, ..Default::default() }))], &[]).await?;
    }
    Ok(Json(j!({"table": name})))
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
        let ipc = StreamReader::try_new(&body[..], None)?.collect::<Result<Vec<_>, _>>()?;
        ipc.into_iter().map(|b| b.with_schema(schema.clone())).collect::<Result<Vec<_>, _>>()?
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

/// Bulk `INSERT INTO name <body SQL>`: results go straight to Parquet (no log), and the files are
/// committed together with `job` so a retried job is not applied twice. Creates the table if missing.
async fn insert(State(app): State<App>, Path(name): Path<String>, Query(p): Query<InsertParams>, query: String) -> Result<Json<Value>, E> {
    let (lake, producer) = (&app.lake, producer_key(&format!("job:{}", p.job)));
    if lake.cat.get::<u64>(&producer).await?.is_some() {
        return Ok(Json(j!({"duplicate": true})));
    }
    let df = session(lake, &query, "").await?.sql(&query).await?;
    let out = df.schema().as_arrow().clone();
    let keys = lake.cat.get::<TableMeta>(&table_key(&name)).await?.map(|m| m.key).unwrap_or_default();
    let files = write_stream(lake, &name, df.execute_stream().await?, 1_000_000, &keys).await?;
    let _guard = app.lock.lock().await;
    if lake.cat.get::<u64>(&producer).await?.is_some() {
        return Ok(Json(j!({"duplicate": true}))); // the same job finished concurrently
    }
    let mut meta = lake.cat.get::<TableMeta>(&table_key(&name)).await?.unwrap_or_else(|| TableMeta {
        columns: out.fields().iter().map(|f| (f.name().clone(), f.data_type().to_string())).collect(),
        ..Default::default()
    });
    ensure!(meta.key.is_empty(), "insert into keyed tables goes through /append");
    let types: Vec<_> = meta.columns.iter().map(|(_, t)| t.clone()).collect();
    ensure!(types == out.fields().iter().map(|f| f.data_type().to_string()).collect::<Vec<_>>(), "query columns {out:?} don't match table {name}");
    let rows: u64 = files.iter().map(|f| f.rows).sum();
    meta.files.extend(files);
    lake.cat.commit(vec![(table_key(&name), json(&meta)), (producer, json(&1u64))], &[]).await?;
    Ok(Json(j!({"rows": rows})))
}

/// Body: `{"source": "events", "target": "per_user", "sql": "SELECT … FROM events …"}`.
async fn create_task(State(app): State<App>, Path(name): Path<String>, body: String) -> Result<Json<Value>, E> {
    let task: Task = serde_json::from_str(&body)?;
    crate::tasks::create(&app.lake, &name, &task).await?;
    Ok(Json(j!({"task": name})))
}

/// Body: the view's SQL, e.g. `SELECT user, sum(amount) AS total, count(*) AS n FROM events GROUP BY user`.
async fn create_view(State(app): State<App>, Path(name): Path<String>, sql: String) -> Result<Json<Value>, E> {
    let _guard = app.lock.lock().await;
    crate::views::create(&app.lake, &name, &sql).await?;
    Ok(Json(j!({"view": name})))
}

/// `GET /lookup/{table}/{key}`: the current row of one key, for serving reads. Same answer as
/// `SELECT … WHERE key = …`, but planned as a filter + "newest wins" instead of a window over the
/// table, on one thread, so it costs a few milliseconds even with many queries in flight.
/// Composite keys are comma-separated, in the key's column order.
async fn lookup(State(app): State<App>, Path((name, key)): Path<(String, String)>) -> Result<Response, E> {
    let lake = &app.lake;
    let meta: TableMeta = lake.cat.get(&table_key(&name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?;
    ensure!(!meta.key.is_empty(), "{name} has no key: use /sql");
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
        true => format!("SELECT {cols} FROM __raw WHERE {where_} ORDER BY \"_ord\" DESC LIMIT 1"),
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
    format: Option<String>,
    after: Option<u64>,     // read-your-writes: first wait until this node has seen segment `after` (from an ack)
    spread: Option<String>, // "1": run across the cluster even for small tables; "0": only here
}

async fn sql(State(app): State<App>, Query(p): Query<SqlParams>, query: String) -> Result<Response, E> {
    if let Some(seg) = p.after {
        let mut hwm = app.lake.hwm.subscribe();
        let _ = tokio::time::timeout(Duration::from_secs(30), async { while app.lake.visible() < seg { hwm.changed().await.ok()?; } Some(()) }).await;
    }
    let nodes = if p.spread.as_deref() == Some("0") { vec![] } else { app.cluster.nodes() };
    let spread = crate::spmd::query(&app.lake, &nodes, &app.cluster.addr, &query, p.spread.as_deref() == Some("1")).await;
    let batches = match spread {
        Ok(Some(batches)) => batches,
        Ok(None) => session(&app.lake, &query, "").await?.sql(&query).await?.collect().await?,
        Err(e) => {
            eprintln!("distributed query failed, running it here: {e:#}");
            session(&app.lake, &query, "").await?.sql(&query).await?.collect().await?
        }
    };
    if p.format.as_deref() == Some("table") {
        return Ok(pretty_format_batches(&batches)?.to_string().into_response());
    }
    let mut w = arrow_json::ArrayWriter::new(Vec::new());
    w.write_batches(&batches.iter().collect::<Vec<_>>())?;
    w.finish()?;
    Ok(([("content-type", "application/json")], w.into_inner()).into_response())
}

#[derive(Deserialize)]
struct WatchParams {
    after: Option<u64>, // default: from now on
}

/// New rows of a table as NDJSON, pushed the moment they commit (a view's rows included).
async fn watch(State(app): State<App>, Path(name): Path<String>, Query(p): Query<WatchParams>) -> Response {
    let hwm = app.lake.hwm.subscribe();
    let after = p.after.unwrap_or_else(|| app.lake.visible());
    let rows = futures::stream::unfold((app, hwm, after, name), |(app, mut hwm, after, name)| async move {
        loop {
            hwm.borrow_and_update();
            let now = app.lake.visible();
            if now > after {
                let chunk = tail(&app.lake, &name, after, Some(now), false).await.and_then(|b| ndjson(&b));
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
