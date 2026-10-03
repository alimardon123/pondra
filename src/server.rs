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
        .route("/cluster/ddl", post(ddl))
        .route("/cluster/change", post(change))
        .route("/cluster/txn", post(|State(app): State<App>, Json(c): Json<crate::txn::Commit>| async move {
            let seq = app.seq.as_ref().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
            Ok::<_, E>(Json(crate::txn::commit_here(&app.lake, seq, &app.lock, c).await?))
        }))
        .route("/cluster/iceberg", post(|State(app): State<App>, Json(c): Json<Vec<crate::iceberg::Commit>>| async move { Ok::<_, E>(Json(app.record_iceberg(c).await?)) }))
        .route_layer(middleware::from_fn_with_state(app.clone(), to_leader));
    Router::new()
        .merge(leader_only)
        .route("/append/{name}", post(append))
        .route("/insert/{name}", post(insert))
        .route("/cluster/files", post(files))
        .route("/", get(crate::console::page))
        .route("/sql", post(sql))
        .route("/sql/pages/{id}", get(page))
        .route("/mcp", post(crate::mcp::handle))
        .route("/files/{*path}", put(put_file).get(get_file).delete(delete_file).post(restore_file))
        .route("/lookup/{name}/{key}", get(lookup))
        .route("/watch/{name}", get(watch))
        .route("/live", get(crate::live::live).post(crate::live::live))
        .route("/sessions/{id}", axum::routing::delete(|Path(id): Path<String>| async move { Json(j!({"ended": crate::temp::end(&crate::temp::id(&id))})) }))
        .route("/sessions/{id}/python", get(session_python).delete(session_python).post(session_python))
        .route("/python", get(list_pythons).put(choose_python))
        .route("/python/format", post(format_python))
        .route("/console/settings", get(crate::console::settings).put(crate::console::save_settings))
        .route("/console/{*file}", get(crate::console::file))
        .route("/functions", get(list_functions))
        .route("/routines", get(|State(app): State<App>| async move { Ok::<_, E>(Json(j!(*crate::routines::listed(&app.lake).await?))) }))
        .route("/secrets/{name}", get(secret))
        .route("/stats", get(stats))
        .route("/healthz", get(|| async { "ok" })) // (the process answers: a liveness probe)
        .route("/ready", get(ready))
        .route("/login", post(login))
        .route("/whoami", get(whoami))
        .route("/objects", get(|State(app): State<App>| async move { Ok::<_, E>(Json(crate::console::objects(&app.lake).await?)) }))
        .route("/metrics", get(|State(app): State<App>| async move { crate::metrics::render(&app).await.map_err(E) }))
        .route("/cluster/commit", post(commit))
        .route("/cluster/log", get(feed))
        .route("/cluster/stage", post(stage))
        .route("/cluster/copy", post(copy))
        .route("/cluster/shuffle", get(bucket))
        .route("/cluster/job", post(job))
        .route("/cluster/probe", get(|Query(p): Query<HashMap<String, usize>>| async move { crate::guard::probe(p.get("bytes").copied().unwrap_or(0)) }))
        .route("/cluster/beat", post(beat))
        .route("/cluster/ack", post(ack))
        .route("/cluster/replica", get(replica))
        .route("/cluster/leader", get(|State(app): State<App>| async move { Json(app.cluster.leader_status()) }))
        .route("/cluster/visible", get(|State(app): State<App>| async move { Json(app.lake.visible()) })) // (a follower's read-your-writes: `write::seen_here`)
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
    crate::udf::forget(&app.lake);
    Ok(Json(j!({"function": name})))
}

async fn drop_function(State(app): State<App>, Path(name): Path<String>) -> Result<Json<Value>, E> {
    app.lake.cat.commit(vec![], &[crate::udf::key(&name)]).await?;
    crate::udf::forget(&app.lake);
    Ok(Json(j!({"dropped": name})))
}

/// `PUT /files/<path>`: an object in the lake next to the tables — an image, a PDF, a model, a SQL
/// file — for `files('…')` to list and `file_read(path)` to read (see `files.rs`). A path that
/// exists is refused (409), unless `If-Match` names the version `GET` gave: then it is replaced,
/// if nobody replaced it meanwhile (412 if so; ADR-034). Each save is kept as a version too
/// (`files::keep`, ADR-035 §8).
async fn put_file(State(app): State<App>, Path(path): Path<String>, headers: HeaderMap, body: Bytes) -> Result<Response, E> {
    let (path, bytes) = (crate::files::under_files(&path), body.len());
    let expect = headers.get("if-match").and_then(|v| v.to_str().ok()).map(|v| v.trim_matches('"').to_string());
    let done = match &expect {
        Some(v) => app.lake.replace(&path, body.to_vec(), v).await,
        None => match app.lake.put(&path, body.to_vec()).await {
            Ok(()) => app.lake.version(&path).await,
            Err(e) => Err(e),
        },
    };
    match done {
        Ok(version) => {
            if let Err(e) = crate::files::keep(&app.lake, &path, &body).await {
                eprintln!("{path}: saved, but not kept as a version: {e:#}");
            }
            Ok(Json(j!({"path": path, "bytes": bytes, "version": version})).into_response())
        }
        Err(e) if e.is::<crate::store::Changed>() => Ok((StatusCode::PRECONDITION_FAILED, format!("{path}: {e}")).into_response()),
        Err(e) if expect.is_none() && format!("{e:#}").contains("already exists") =>
            Ok((StatusCode::CONFLICT, format!("{path} is there already: send If-Match with the version you read to replace it, or put it under another name")).into_response()),
        Err(e) => Err(E(e)),
    }
}

/// `GET /files/<path>`: that object's bytes, its version (`etag`, for `If-Match`) and a content
/// type by its name. `?versions`: the versions it keeps, newest first; `?version=<id>`: one's bytes.
async fn get_file(State(app): State<App>, Path(path): Path<String>, Query(q): Query<HashMap<String, String>>) -> Result<Response, E> {
    let path = crate::files::under_files(&path);
    if q.contains_key("versions") {
        let all = crate::files::versions(&app.lake, &path).await?;
        return Ok(Json(all.iter().map(|v| j!({"id": v.id, "at": v.ms, "who": v.who, "bytes": v.bytes})).collect::<Vec<_>>()).into_response());
    }
    let found = match q.get("version") {
        Some(id) => match crate::files::version(&app.lake, &path, id).await {
            Ok(b) => Ok((b, id.clone())),
            Err(e) => return Ok((StatusCode::NOT_FOUND, format!("{e:#}")).into_response()), // (not one of its versions, or gone)
        },
        None => app.lake.file(&path).await,
    };
    let (bytes, version) = match found {
        Ok(found) => found,
        Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) => {
            return Ok((StatusCode::NOT_FOUND, format!("no such file: {path}")).into_response());
        }
        Err(e) => return Err(E(e)),
    };
    let ext = path.rsplit('.').next().unwrap_or_default().to_ascii_lowercase();
    let kind = match ext.as_str() {
        "csv" => "text/csv; charset=utf-8",
        "tsv" => "text/tab-separated-values; charset=utf-8",
        "json" | "ipynb" => "application/json",
        "jsonl" | "ndjson" => "application/x-ndjson",
        "sql" | "py" | "md" | "txt" => "text/plain; charset=utf-8",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    };
    Ok(([("content-type", kind), ("etag", &format!("\"{version}\"")), ("cache-control", "no-store")], bytes).into_response())
}

/// `POST /files/<path>?restore=<id>`: that version made the file again (itself kept as the newest).
async fn restore_file(State(app): State<App>, Path(path): Path<String>, Query(q): Query<HashMap<String, String>>) -> Result<Json<Value>, E> {
    let path = crate::files::under_files(&path);
    let id = q.get("restore").ok_or_else(|| E(anyhow::anyhow!("POST /files/<path>?restore=<a version's id> (GET ?versions lists them)")))?;
    let bytes = crate::files::version(&app.lake, &path, id).await?;
    let version = match app.lake.version(&path).await {
        Ok(now) => app.lake.replace(&path, bytes.to_vec(), &now).await?,
        Err(_) => {
            app.lake.put(&path, bytes.to_vec()).await?; // (deleted since: back again)
            app.lake.version(&path).await?
        }
    };
    crate::files::keep(&app.lake, &path, &bytes).await?;
    Ok(Json(j!({"path": path, "version": version, "restored": id})))
}

/// `DELETE /files/<path>`: that object gone (a writer's, as `PUT`); its versions stay.
async fn delete_file(State(app): State<App>, Path(path): Path<String>) -> Result<Json<Value>, E> {
    let path = crate::files::under_files(&path);
    app.lake.remove_file(&path).await?;
    Ok(Json(j!({"removed": path})))
}

/// `GET /sessions/{id}/python`: the variables a session's Python holds, as the console's panel
/// lists them (none, if it has none yet); `DELETE`: stop it (its variables go; the session's
/// temporary tables stay), as a notebook's kernel is restarted. An admin's, as Python is.
async fn session_python(method: axum::http::Method, Path(id): Path<String>, role: axum::Extension<crate::auth::Role>) -> Result<Json<Value>, E> {
    let id = crate::temp::id(&id); // (as the client's requests name it: its user's)
    if *role < crate::auth::Role::Admin {
        return Err(E(anyhow::anyhow!("a session's Python is an admin's, as DO is")));
    }
    Ok(Json(match method {
        axum::http::Method::DELETE => j!({"restarted": crate::python::end_session(&id)}),
        axum::http::Method::POST => j!({"done": crate::python::interrupt(&id)}), // (interrupt its running cell)
        _ => crate::python::variables(&id).await?,
    }))
}

/// `GET /python`: the Python in use, and (`?all=1`) each Python this machine has, tried: the
/// console's Choose Python. An admin's, as Python is.
async fn list_pythons(Query(q): Query<HashMap<String, String>>, role: axum::Extension<crate::auth::Role>) -> Result<Json<Value>, E> {
    if *role < crate::auth::Role::Admin {
        return Err(E(anyhow::anyhow!("which Python runs is an admin's, as DO is")));
    }
    let mut v = crate::python::current();
    if q.contains_key("all") {
        v["pythons"] = j!(crate::python::pythons().await);
    }
    Ok(Json(v))
}

/// `PUT /python` `{"path": …}`: run workers with that Python from now on (and next time: kept on
/// this machine). From a page on this machine only, an admin's.
async fn choose_python(axum::extract::ConnectInfo(crate::tls::Peer { addr: from, .. }): axum::extract::ConnectInfo<crate::tls::Peer>, role: axum::Extension<crate::auth::Role>, Json(b): Json<Value>) -> Result<Json<Value>, E> {
    if *role < crate::auth::Role::Admin || !from.ip().is_loopback() {
        return Err(E(anyhow::anyhow!("which Python runs is chosen by an admin, on this machine")));
    }
    Ok(Json(crate::python::choose(b["path"].as_str().unwrap_or_default()).await?))
}

/// `POST /python/format` `{"code": …}`: the code formatted by the node's Python, as ruff (or black)
/// formats it: the console's Format for Python files and cells. The code is only read, never run.
async fn format_python(Json(b): Json<Value>) -> Result<Json<Value>, E> {
    crate::python::ready("Formatting Python uses the node's Python (its ruff or black)")?;
    let head = j!({"op": "format", "code": b["code"].as_str().unwrap_or_default()});
    let (said, _) = crate::python::ask("", crate::python::Use::Function, head, vec![], Some(std::time::Duration::from_secs(30)), &mut |_| {}).await?;
    Ok(Json(j!({"code": said["code"]})))
}

/// `GET /functions`: the lake's own functions, by name.
async fn list_functions(State(app): State<App>) -> Result<Json<Value>, E> {
    let fns = app.lake.cat.scan::<crate::udf::Udf>("f/", "f0").await?;
    Ok(Json(j!(fns.into_iter().map(|(k, u)| (k[2..].to_string(), serde_json::to_value(u).unwrap_or_default())).collect::<serde_json::Map<_, _>>())))
}

/// Who asks (see `auth.rs`, `users.rs`): a token, a user's token, session or password; the
/// program that started the node (`owner`); or, when nothing needs a sign-in, anyone, as an admin.
/// Its role must cover the route; the request then runs as it (`auth::WHO`), and handlers see its
/// role too.
async fn guard(State(app): State<App>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path();
    if crate::drain::draining() && !crate::drain::still_taken(path) {
        return (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "1")], "this node is stopping: ask another").into_response(); // (`drain.rs`)
    }
    if app.cluster.cut_off() && !crate::drain::still_taken(path) {
        // (a leader that can't reach the bucket can't commit, and its reads may already be stale:
        // another node leads soon. The cluster's own calls and the health checks still go through.)
        return (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "1")], crate::cluster::CUT_OFF_SAYS).into_response();
    }
    let header = req.headers().get("authorization").and_then(|v| v.to_str().ok()).map(String::from);
    let from = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>().map(|c| c.0.addr);
    if let Some(axum::extract::ConnectInfo(peer)) = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>() {
        if !peer.allowed() {
            return (StatusCode::FORBIDDEN, crate::tls::PLAIN).into_response(); // (`tls.rs`)
        }
        if header.as_deref().is_some_and(|h| h.starts_with("Bearer pn_")) && !peer.may_be_node() {
            return (StatusCode::UNAUTHORIZED, "the nodes' key is taken only with a certificate the nodes' authority signed (PONDRA_TLS_CA)").into_response();
        }
    }
    let signed = match &header {
        Some(h) => crate::users::who(&app.lake, &app.auth, Some(h)).await,
        None => None,
    };
    let who = match signed {
        Some(p) => p,
        None if owner(req.headers()) || app.open().await => crate::auth::Principal::of(crate::auth::Role::Admin),
        None if header.is_some() => {
            crate::audit::refused(&app, &basic_user(header.as_deref()), "http", from, &format!("{} {}", req.method(), req.uri().path()), "wrong token, or user name and password");
            tokio::time::sleep(Duration::from_millis(400)).await; // (a guess costs time)
            return (StatusCode::UNAUTHORIZED, "wrong token, or user name and password").into_response();
        }
        None => crate::auth::Principal::of(crate::auth::Role::None),
    };
    let who = who.at("http", from);
    if who.role < crate::auth::Auth::needed(req.uri().path(), req.method().as_str()) {
        return match who.role {
            crate::auth::Role::None => (StatusCode::UNAUTHORIZED, "sign in: a token, or a user's name and password").into_response(),
            _ => {
                crate::audit::refused(&app, &who.name, "http", from, &format!("{} {}", req.method(), req.uri().path()), "this needs more rights than this token's or user's");
                (StatusCode::FORBIDDEN, "this needs more rights than this token's or user's").into_response()
            }
        };
    }
    req.extensions_mut().insert(who.role);
    match crate::panics::door(crate::auth::WHO.scope(who, next.run(req))).await {
        Ok(r) => r,
        Err(m) => (StatusCode::INTERNAL_SERVER_ERROR, m).into_response(), // (and the node stays up)
    }
}

/// The user name in a Basic header (for the audit log: who tried).
fn basic_user(header: Option<&str>) -> String {
    use base64::Engine;
    let b = header.and_then(|h| h.strip_prefix("Basic ")).and_then(|b| base64::engine::general_purpose::STANDARD.decode(b.trim()).ok());
    b.and_then(|b| String::from_utf8(b).ok()).and_then(|up| up.split_once(':').map(|(u, _)| u.to_string())).unwrap_or_default()
}

/// `POST /login` `{"user", "password"}`: a session (`{"token": "ps_…", "until": ms}`) to send as
/// `Authorization: Bearer …` until it ends (`PONDRA_SESSION_HOURS`); the console's sign-in.
async fn login(State(app): State<App>, body: Bytes) -> Response {
    let b: Value = serde_json::from_slice(&body).unwrap_or_default(); // (JSON, whatever its content type says)
    let (user, password) = (b["user"].as_str().unwrap_or_default(), b["password"].as_str().unwrap_or_default());
    if !crate::users::password_ok(&app.lake, user, password).await {
        crate::audit::refused(&app, user, "http", crate::auth::current().and_then(|p| p.from), "sign in", "wrong user name or password");
        tokio::time::sleep(Duration::from_millis(400)).await; // (a guess costs time)
        return (StatusCode::UNAUTHORIZED, "wrong user name or password").into_response();
    }
    match crate::users::session(&app.lake, user).await {
        Ok((token, until)) => Json(j!({"token": token, "until": until, "user": user})).into_response(),
        Err(e) => E(e).into_response(),
    }
}

/// `GET /whoami`: who this request signs in as, and what it may do.
async fn whoami(State(app): State<App>) -> Json<Value> {
    let p = crate::auth::current().unwrap_or_else(|| crate::auth::Principal::of(crate::auth::Role::None));
    let role = match p.role {
        crate::auth::Role::Admin => "admin",
        crate::auth::Role::Write => "write",
        crate::auth::Role::Read => "read",
        crate::auth::Role::None => "none",
    };
    Json(j!({"user": p.name, "role": role, "superuser": p.role == crate::auth::Role::Admin && p.access.is_none(), "open": app.open().await}))
}

impl App {
    /// Does nothing need a sign-in here? (No token set, and no user who signs in.)
    pub async fn open(&self) -> bool { !self.auth.on() && !crate::users::any(&self.lake).await }
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
    let url = crate::tls::url(&format!("{addr}{}", parts.uri.path_and_query().map_or("", |p| p.as_str())));
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

async fn beat(State(app): State<App>, Query(p): Query<BeatParams>, headers: axum::http::HeaderMap) -> Result<Json<(u64, Vec<String>)>, Response> {
    // Test hook: followers listed in the file $PONDRA_DROP_BEATS get no answer (a broken link to the leader).
    let dropped = std::env::var("PONDRA_DROP_BEATS").map(|f| std::fs::read_to_string(f).unwrap_or_default()).unwrap_or_default();
    if dropped.lines().any(|a| a == p.from) {
        return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
    }
    let cut_off = || (StatusCode::SERVICE_UNAVAILABLE, [(crate::cluster::CUT_OFF_HEADER, "1")]).into_response(); // (its followers take over at once)
    app.cluster.beat(p.from, crate::format::said(&headers)).map(Json).ok_or_else(cut_off)
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

/// This node's share of a `COPY … TO` a folder, written (see `spmd::copy`): how many rows.
async fn copy(State(app): State<App>, Json((slice, target)): Json<(crate::spmd::Slice, crate::copy::Target)>) -> Result<Json<u64>, E> {
    Ok(Json(crate::spmd::copy_share(&app.lake, &slice, &target).await?))
}

#[derive(Deserialize)]
struct BucketParams {
    id: String,
    exchange: usize,
    to: usize,
    #[serde(default)]
    drop: bool, // the coordinator gave up on this shuffle: forget it and delete what it spilled
    part: Option<usize>, // just this partition of it (a hot one shared out: `skew.rs`)
}

/// A shuffle bucket this node keeps for another node (see `spmd.rs`).
async fn bucket(Query(p): Query<BucketParams>) -> Result<Response, E> {
    if p.drop {
        crate::spmd::forget(&p.id);
        return Ok(Body::empty().into_response());
    }
    let buckets = crate::spmd::bucket(&p.id, p.exchange, p.to, p.part)?;
    Ok(Body::from_stream(crate::spmd::reply("", buckets, None)).into_response()) // (a piece at a time: a big bucket is on disk)
}

/// A share of the leader's data work (tiering, merging, compaction): the files written.
async fn job(State(app): State<App>, Json(job): Json<crate::tier::Job>) -> Result<Json<Vec<DataFile>>, E> {
    Ok(Json(crate::tier::run_job(&app.lake, job).await?))
}

/// The commit stream (see `Frame`): who leads, then the recent frames, then every new one.
async fn feed(State(app): State<App>, headers: axum::http::HeaderMap) -> Response {
    let cat = &app.lake.cat;
    let streamed = crate::format::Streamed::to(crate::format::said(&headers).1); // (held while the stream is open: `format::raise`)
    let start = Frame::Start { term: app.cluster.leader.n, replicated: cat.replicas > 1 };
    let (recent, rx) = cat.subscribe();
    let live = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.ok().map(|f| (f, rx)) }); // a lagging follower reconnects
    let frames = futures::stream::iter([start].into_iter().chain(recent)).chain(live).map(move |f| {
        let _ = &streamed;
        Ok::<_, std::io::Error>(f.encode())
    });
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
        match self.query_as(&crate::asof::rewrite(query)?, spread, false).await {
            // (`*` left as it is when `sys::hide`'s EXCLUDE names a column its table lacks: a
            // stored view's, say)
            Err(e) if [crate::sys::mentioned, crate::query::names_deleted].iter().any(|m| m(query) && m(&format!("{e:#}"))) => self.query_as(&crate::asof::as_of(query)?, spread, false).await,
            r => r,
        }
    }

    /// Run a query as rewritten (`asof::rewrite`: ASOF JOIN as DataFusion can plan it); `files`:
    /// it may read files on this machine (`owner`). Its work — over 100 KB of it, planning and
    /// running a query — is made on the heap, here: a caller waiting for it holds a pointer, so
    /// procedures calling procedures 16 deep don't run out of stack.
    #[inline(never)]
    pub fn query_as<'a>(&'a self, query: &'a str, spread: Option<&'a str>, files: bool) -> futures::future::BoxFuture<'a, anyhow::Result<Vec<RecordBatch>>> {
        Box::pin(crate::ext::listing(self.query_listed(query, spread, files))) // (every door: files listed once a statement)
    }

    async fn query_listed(&self, query: &str, spread: Option<&str>, files: bool) -> anyhow::Result<Vec<RecordBatch>> {
        use crate::metrics::{add, QUERIES, QUERY_ERRORS, QUERY_US, SPREAD};
        let start = std::time::Instant::now();
        crate::history::read_at(self.lake.visible()); // (its answer again: `t AT (VERSION => n)`)
        let explained = crate::write::first_word(query).get(..7).is_some_and(|w| w.eq_ignore_ascii_case("explain"));
        let run = async {
            let here_only = spread == Some("0") || crate::query::sent() || crate::temp::mentioned(query) || crate::past::mentioned(query) || crate::txn::open() || crate::settings::any() || crate::vars::mentioned(query) || crate::routines::pinned(&self.lake, query).await // (rows sent with a request are here only; so are the session's temporary tables, its transaction and settings, and a Python table function's call)
                || crate::auth::limited().is_some(); // (and a user's granted some tables: its grants are checked where it is planned, here)
            let nodes = if here_only { vec![] } else { self.cluster.nodes() };
            match crate::spmd::query(&self.lake, &nodes, &self.cluster.addr, query, spread == Some("1")).await {
                Ok(Some(batches)) => {
                    crate::guard::ran_spread(query, start.elapsed());
                    crate::history::spread(nodes.len());
                    return Ok((batches, true));
                }
                Ok(None) => {}
                Err(e) => eprintln!("distributed query failed, running it here: {e:#}"),
            }
            let run = |frugal: bool| async move {
                let ctx = session(&self.lake, query, "").await?;
                let ctx = if files { ctx.enable_url_table() } else { ctx };
                if explained {
                    ctx.state_ref().write().config_mut().options_mut().explain.show_statistics = true; // (each operator's expected rows: `history::expected`)
                }
                if frugal {
                    // Hash joins can't spill, sort-merge joins can; sorts keep less aside to merge.
                    let state = ctx.state_ref();
                    let mut state = state.write();
                    let o = state.config_mut().options_mut();
                    (o.optimizer.prefer_hash_join, o.execution.sort_spill_reservation_bytes) = (false, 1 << 20);
                }
                let df = crate::query::sql(&ctx, query).await?;
                let (schema, task) = (Arc::new(df.schema().as_arrow().clone()), Arc::new(df.task_ctx()));
                let plan = df.create_physical_plan().await?;
                let out = datafusion::physical_plan::collect(plan.clone(), task).await?;
                crate::history::planned(&plan); // (its shape; a slow statement's plan, with what each operator did)
                let out = if explained { crate::history::explained(out)? } else { out };
                anyhow::Ok(if out.is_empty() { vec![RecordBatch::new_empty(schema)] } else { out }) // (no rows: still its columns)
            };
            let here = std::time::Instant::now();
            let out = match run(false).await {
                Err(e) if format!("{e:#}").contains("Resources exhausted") => run(true).await?, // out of memory: try again frugally
                r => r?,
            };
            if self.cluster.nodes().len() > 1 && here.elapsed() >= Duration::from_millis(20) {
                crate::guard::ran_here(query, crate::spmd::reads_sql(&self.lake, query).await?, here.elapsed()); // (how fast queries go here: `guard.rs`)
            }
            Ok((out, false))
        };
        let out = run.await;
        add(&QUERIES, 1);
        add(&QUERY_US, start.elapsed().as_micros() as u64);
        match out {
            Ok((batches, spread)) => {
                add(&SPREAD, spread as u64);
                crate::history::rows(batches.iter().map(|b| b.num_rows() as u64).sum());
                Ok(batches.into_iter().map(crate::query::compact).collect()) // (an answer kept or sent holds only its own strings)
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

    /// Another engine's append (`iceberg::update`), recorded by the leader under the lake's lock.
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
        let heavy = |m: &TableMeta| m.files.iter().any(|f| f.deleted > 0 && f.deleted * 10 >= f.rows); // (`tier::maintain` rewrites them)
        let untidy = tables.iter().filter(|(k, m)| (m.files.len() >= 8 || heavy(m)) && !busy.contains(&k[2..].to_string())).map(|(k, _)| k[2..].to_string());
        let untidy: Vec<String> = busy.iter().cloned().chain(untidy).collect();
        // (tables whose changed rows may wait in files: `tier::purge` says whether it's time)
        let changed: Vec<String> = tables.iter().filter(|(_, m)| m.changed && m.purged() < m.tiered).map(|(k, _)| k[2..].to_string()).collect();
        if untidy.is_empty() && changed.is_empty() {
            return Ok(0); // (the pressure check, most of the time: don't hold the lock for nothing)
        }
        let per_table: Vec<_> = busy.iter().map(|t| self.tier_one(t, hwm, &nodes)).collect();
        let rows: u64 = futures::stream::iter(per_table).buffer_unordered(4).try_collect::<Vec<u64>>().await?.iter().sum();
        // Changed rows out of the files (published tables: before they are published).
        let purge: Vec<_> = changed.iter().map(|t| crate::tier::purge(&self.lake, t, &nodes, &self.cluster.addr, self.retain_ms, false)).collect();
        futures::stream::iter(purge).buffer_unordered(4).try_collect::<Vec<bool>>().await?;
        let tiered = start.elapsed();
        crate::delta::publish_all(&self.lake).await?; // what other engines read, as soon as it's tiered
        let first_publish = start.elapsed();
        // Then merges and compactions (published too, once done).
        let maintain: Vec<_> = untidy.iter().map(|t| crate::tier::maintain(&self.lake, t, &nodes, &self.cluster.addr, false)).collect();
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
        crate::tasks::run_all(&self.lake, &self.cluster, self.log()?).await?;
        match &self.seq {
            Some(seq) => {
                crate::views::fill_all(&self.lake, seq, self.log()?, &self.lock).await?; // (the leader: views filled from the rows already there)
                crate::views::join_all(&self.lake, self.log()?).await // (and stream joins)
            }
            None => Ok(()),
        }
    }
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
    #[serde(default)]
    producer: String, // (none: at least once, as a Kafka producer that isn't idempotent)
    #[serde(default)]
    seq: u64,
    prev: Option<u64>, // compare-and-swap on the producer's last seq
}

/// Body: NDJSON rows, or an Arrow IPC stream (content-type application/vnd.apache.arrow.stream).
async fn append(State(app): State<App>, Path(name): Path<String>, Query(p): Query<AppendParams>, headers: HeaderMap, body: Bytes) -> Result<Json<Ack>, E> {
    let log = app.log()?;
    if !p.producer.is_empty() && p.seq == 0 {
        return Err(E(anyhow::anyhow!(crate::log::SEQ_FROM_1))); // (0 is "nothing yet": its batch would be taken for a retry)
    }
    let meta: TableMeta = app.lake.cat.get(&table_key(&name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?;
    let schema = schema(&meta.logical().columns)?; // (rows come under SQL's names; the log keeps stored ones: ADR-022)
    let arrow = headers.get("content-type").is_some_and(|v| v.as_bytes().starts_with(b"application/vnd.apache.arrow"));
    let (batches, given) = if arrow {
        // Columns by name, cast to the table's types (pandas, Polars and Arrow differ in string
        // types); a column left out takes its DEFAULT, or is null (as in JSON), e.g. `_deleted`,
        // or one added since.
        let ipc = StreamReader::try_new(&body[..], None)?;
        let given: Vec<String> = ipc.schema().fields().iter().map(|f| f.name().clone()).collect();
        let ipc = ipc.collect::<Result<Vec<_>, _>>()?;
        (ipc.iter().map(|b| crate::query::conform(b, &schema)).collect::<anyhow::Result<Vec<_>>>()?, Some(given))
    } else {
        (arrow_json::ReaderBuilder::new(schema.clone()).build(&body[..])?.collect::<Result<Vec<_>, _>>()?, None)
    };
    let batch = concat_batches(&schema, &batches)?;
    let batch = match (crate::defaults::any(&meta), given) {
        (false, _) => batch,
        (true, Some(given)) => crate::defaults::fill(&meta, batch.clone(), |c| (!given.iter().any(|g| g == c)).then(|| crate::defaults::all(batch.num_rows()))).await?,
        (true, None) => {
            let absent = crate::defaults::absent_keys(&meta, &body)?; // (a JSON row without the key)
            crate::defaults::fill(&meta, batch, |c| absent.get(c).cloned()).await?
        }
    };
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
    let query = crate::routines::expand(&app.lake, &query).await?;
    let ctx = session(&app.lake, &query, "").await?;
    match crate::write::write_files(&app.lake, &ctx, &name, &query, &p.job, crate::write::stamp(&app.lake, &name, Some(&app.to())).await?).await? {
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
    let mut task: Task = serde_json::from_str(&body)?;
    task.sql = crate::routines::expand(&app.lake, &task.sql).await?;
    crate::tasks::create(&app.lake, &name, &task).await?;
    Ok(Json(j!({"task": name})))
}

/// `POST /views/{name}` with the view's SQL, e.g. `SELECT user, sum(amount) AS total, count(*) AS n
/// FROM events GROUP BY user`; `?window=w&size_secs=60&lateness_secs=10` also emits each
/// window of column `w` once, final, to `{name}_final`; `?session=ts&gap_secs=30&lateness_secs=5`
/// makes it a session view (`views.rs`): `CREATE MATERIALIZED VIEW … WITH (…)` in SQL.
async fn create_view(State(app): State<App>, Path(name): Path<String>, Query(options): Query<std::collections::BTreeMap<String, String>>, sql: String) -> Result<Json<Value>, E> {
    let sql = crate::routines::expand(&app.lake, &sql).await?; // (as they are now: ADR-023)
    let d = crate::ddl::Ddl::CreateMaterialized { name, sql, options };
    let out = {
        let _guard = app.lock.lock().await;
        crate::ddl::apply(&app.lake, d.clone()).await?
    };
    crate::ddl::settle(&app.lake, &d, &app.cluster.addr).await?; // (once it's filled from the rows already there)
    Ok(Json(out))
}

/// An UPDATE, DELETE or MERGE a follower's SQL asked for (`change.rs`): `[sql, job]`.
async fn change(State(app): State<App>, Json((sql, job, sent)): Json<(String, String, crate::change::Sent)>) -> Result<Json<Value>, E> {
    let seq = app.seq.as_ref().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
    let _guard = app.lock.lock().await;
    let sent = Arc::new(crate::change::unpack(&sent)?); // (what it reads that this node can't: `change::for_leader`)
    Ok(Json(crate::query::SENT.scope(sent, crate::change::run(&app.lake, seq, &sql, &job)).await?))
}

/// A `CREATE`/`DROP` of a schema, view or table that a follower's SQL asked for (`ddl.rs`).
async fn ddl(State(app): State<App>, Json(d): Json<crate::ddl::Ddl>) -> Result<Json<Value>, E> {
    let out = {
        let _guard = app.lock.lock().await;
        crate::ddl::apply(&app.lake, d.clone()).await?
    };
    crate::ddl::settle(&app.lake, &d, &app.cluster.addr).await?; // (ATTACH, CREATE DATABASE: the leader too at once, not in a second)
    Ok(Json(out))
}

/// `GET /lookup/{table}/{key}`: the current row of one key, for serving reads — same answer as
/// `SELECT … WHERE key = …`. Upsert tables skip SQL entirely (`serve.rs`); merge tables combine
/// the key's partial rows with a filter + GROUP BY on one thread. Composite keys are
/// comma-separated, in the key's column order.
async fn lookup(State(app): State<App>, Path((name, key)): Path<(String, String)>) -> Result<Response, E> {
    crate::auth::check_all(&name)?; // (a user's: the whole row)
    let lake = &app.lake;
    let meta: TableMeta = lake.cat.get(&table_key(&name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?;
    ensure!(!meta.key.is_empty(), "{name} has no key: use /sql");
    if meta.merge.is_empty() && meta.ttl.is_none() && meta.order.is_none() {
        let names: Vec<&str> = meta.columns.iter().map(|(c, _)| c.as_str()).filter(|c| *c != "_deleted").collect(); // (as `SELECT *` shows it)
        let row = crate::serve::lookup(lake, &name, &meta, &key).await?.map(|r| r.project(&names.iter().map(|n| r.schema().index_of(n)).collect::<Result<Vec<_>, _>>()?)).transpose()?;
        let row = row.map(|r| meta.to_logical(&r)).transpose()?;
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
        true => format!("SELECT * FROM (SELECT {cols} FROM __raw WHERE {where_} ORDER BY {}\"_ord\" DESC LIMIT 1){}", meta.order.as_ref().map(|o| format!("\"{o}\" DESC NULLS LAST, ")).unwrap_or_default(), crate::query::live(&meta)),
        // Merge table: combine that key's partial rows.
        false => format!("{} ", latest_sql(&meta, "__raw", false, false)).replace(" GROUP BY ", &format!(" WHERE {where_} GROUP BY ")),
    };
    let shown = |b: &RecordBatch| b.project(&(0..b.num_columns()).filter(|&i| b.schema().field(i).name() != "_deleted").collect::<Vec<_>>());
    let batches = ctx.sql(&sql).await?.collect().await?.iter().map(|b| meta.to_logical(&shown(b)?)).collect::<anyhow::Result<Vec<_>>>()?;
    let mut w = arrow_json::ArrayWriter::new(Vec::new());
    w.write_batches(&batches.iter().collect::<Vec<_>>())?;
    w.finish()?;
    Ok(([("content-type", "application/json")], w.into_inner()).into_response())
}

#[derive(Deserialize)]
struct SqlParams {
    format: Option<String>, // json (default), table (text), arrow (Arrow IPC stream), typed (columns and types, then rows: the console's)
    after: Option<u64>,     // read-your-writes: first wait until this node has seen segment `after` (from an ack)
    spread: Option<String>, // "1": run across the cluster even for small tables; "0": only here
    stale_ms: Option<u64>,  // accept a cached result up to this old (see `Results`)
    job: Option<String>,    // writes: a retry with the same job id is applied once
    rows: Option<usize>,    // typed: the rows of the first page (the console's rows a page; `SHOWN` if not said)
}

/// Is this request from whoever started the node here (the shell: `PONDRA_OWNER_KEY`)? Then its
/// SQL may read files on this machine, as `pondra sql` may (invariant 21: nobody else's).
fn owner(headers: &axum::http::HeaderMap) -> bool {
    static KEY: std::sync::LazyLock<Option<String>> = std::sync::LazyLock::new(|| std::env::var("PONDRA_OWNER_KEY").ok().filter(|k| k.len() >= 16));
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    crate::auth::lent(token).is_some_and(|l| l.1) // (a Python procedure the owner called)
        || KEY.as_deref().is_some_and(|k| headers.get("x-pondra-owner").is_some_and(|h| h.as_bytes() == k.as_bytes()))
}

/// `POST /sql`: statements (SQL, or JSON with `$name` parameters, or that and tables of the
/// caller's own: `routines::Request`), run in order; the last one's answer. A single query as it
/// was sent may be answered from `Results`.
async fn sql(State(app): State<App>, Query(p): Query<SqlParams>, role: axum::Extension<crate::auth::Role>, headers: axum::http::HeaderMap, body: Bytes) -> Response {
    let files = owner(&headers); // (the program that started this node: its files, and URLs no secret covers)
    let session = crate::temp::of(&headers); // (its temporary tables: `temp.rs`)
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    let vars = crate::auth::lent_vars(token); // (Python code a run lent a connection to: the run's variables)
    let (out, heard) = crate::routines::with_notices(crate::vars::within(vars, crate::temp::SESSION.scope(session.clone(), crate::ext::scope(files, sql_as(app, p, role, headers, body))))).await;
    let mut r = out.unwrap_or_else(IntoResponse::into_response);
    if session.as_deref().is_some_and(crate::temp::holds) {
        r.headers_mut().insert("x-pondra-session", axum::http::HeaderValue::from_static("held")); // (a client keeps to this node meanwhile)
    }
    if let Some(h) = notices(&heard) {
        r.headers_mut().insert("x-pondra-notices", h);
    }
    r
}

/// What the statement's procedures printed, for the caller: a JSON list of lines in a header (all
/// of it in ASCII, the first 32 KB of it; the run log has the rest).
fn notices(heard: &[String]) -> Option<axum::http::HeaderValue> {
    if heard.is_empty() {
        return None;
    }
    let (mut kept, mut size) = (vec![], 0);
    for n in heard {
        size += n.len() + 4;
        if size > 32 << 10 {
            kept.push("…".to_string());
            break;
        }
        kept.push(n.clone());
    }
    let text = serde_json::to_string(&kept).ok()?;
    let ascii: String = text.chars().map(|c| if c.is_ascii() && !c.is_ascii_control() { c.to_string() } else { c.encode_utf16(&mut [0; 2]).iter().map(|u| format!("\\u{u:04x}")).collect() }).collect();
    axum::http::HeaderValue::from_str(&ascii).ok()
}

/// `GET /secrets/{name}`: a secret's values (`CREATE SECRET`), for a Python procedure's own code
/// (`pondra.secret`) and nothing else: the token must be one lent to a procedure while it runs.
/// The values are kept out of what that procedure says (its notices, its error, the run log).
async fn secret(State(app): State<App>, Path(name): Path<String>, headers: axum::http::HeaderMap) -> Result<Json<Value>, E> {
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    ensure!(crate::auth::lent(token).is_some(), "only a procedure's code reads a secret: pondra.secret(name), in a Python procedure");
    let values = crate::ext::reveal(&app.lake, &name.to_lowercase()).await?;
    crate::auth::revealed(token, values.iter().filter(|(k, _)| *k != "type" && *k != "scope").map(|(_, v)| v.clone()));
    Ok(Json(j!(values)))
}

async fn sql_as(app: App, p: SqlParams, role: axum::Extension<crate::auth::Role>, headers: axum::http::HeaderMap, body: Bytes) -> Result<Response, E> {
    use crate::routines::{Outcome, Who};
    let kind = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or_default();
    let req = crate::routines::Request::read(kind, &body)?;
    let depth = headers.get("x-pondra-depth").and_then(|v| v.to_str().ok()?.parse().ok()).unwrap_or(0); // (a Python procedure's own calls)
    let who = Who { role: role.0, files: owner(&headers), depth };
    if let Some(seg) = p.after {
        let mut hwm = app.lake.hwm.subscribe();
        let _ = tokio::time::timeout(Duration::from_secs(30), async { while app.lake.visible() < seg { hwm.changed().await.ok()?; } Some(()) }).await;
    }
    if let ([one], true, true, true, None, false) = (&crate::routines::split(&req.sql)[..], req.params.is_empty(), req.tables.is_empty(), req.views.is_empty(), crate::vars::change(&req.sql), crate::script::is(&req.sql)) {
        let one = crate::routines::expand(&app.lake, &crate::vars::bound(one)?).await?; // (`$name`: the session's variables)
        if !crate::write::checkpoint(&one) && !crate::routines::runs_procedure(&one) && crate::write::parse(&one).is_none() && crate::txn::control(&one).is_none() && !crate::txn::open() && !crate::settings::is(&one) {
            return Ok(crate::audit::statement(&app, &one, query(&app, &p, &one, who.files)).await?);
        }
    }
    let tables = Arc::new(req.tables);
    // A script sent without a session is one of its own while it runs: its SET, PREPARE, BEGIN
    // and temporary tables last to its end.
    let own = (crate::temp::current().is_none() && crate::routines::split(&req.sql).len() > 1).then(crate::temp::of_script);
    let run = crate::query::SENT.scope(tables, crate::routines::script(&app, &req.sql, &req.params, &req.views, who, p.job.clone()));
    let out = match &own {
        Some(s) => crate::temp::SESSION.scope(Some(s.clone()), run).await,
        None => run.await,
    };
    own.inspect(|s| _ = crate::temp::end(s));
    match out? {
        Outcome::Rows(batches) => Ok(([("content-type", content_type(p.format.as_deref()))], answer(&batches, &p)?).into_response()),
        Outcome::Done(v) => Ok(Json(v).into_response()),
    }
}

fn content_type(format: Option<&str>) -> &'static str {
    match format {
        Some("table") => "text/plain",
        Some("arrow") => "application/vnd.apache.arrow.stream",
        Some("csv") => "text/csv; charset=utf-8",
        Some("tsv") => "text/tab-separated-values; charset=utf-8",
        Some("ndjson") => "application/x-ndjson",
        Some("parquet") => "application/vnd.apache.parquet",
        Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        _ => "application/json",
    }
}

/// One query: a key lookup without planning, a remembered answer, or run (once for everyone who
/// asks at the same moment).
async fn query(app: &App, p: &SqlParams, query: &str, files: bool) -> anyhow::Result<Response> {
    let format = p.format.as_deref().unwrap_or("json");
    let respond = |body: bytes::Bytes| ([("content-type", content_type(p.format.as_deref()))], body).into_response();
    let limited = crate::auth::limited().is_some(); // (a user granted some tables: planned as it may read them, never answered from what another read, nor kept: its grants may change)
    if format == "json" && !crate::temp::mentioned(query) && !limited {
        if let Some(body) = crate::serve::point_sql(&app.lake, query).await? {
            return Ok(respond(body.into())); // a key lookup: no planning
        }
    }
    // Same query, same catalog version: same answer (unless it asks for the time or randomness,
    // or may read a file on this machine).
    let q = query.to_lowercase();
    let volatile = files || limited || !crate::ext::names(query).is_empty() || ["now()", "random(", "current_", "uuid(", "explain", "pondra.runs", "pondra.tasks", "pondra.audit", "pondra.history", "pondra$history", "pondra.variables", "files("].iter().any(|f| q.contains(f)) // (files outside the lake change on their own; `files()` lists objects put since)
        || crate::temp::mentioned(query) // (the session's temporary tables change without a commit)
        || crate::settings::any() // (and its settings may change the answer)
        || crate::routines::volatile(&app.lake, query).await; // (a Python function may answer differently each time)
    let Some(version) = app.lake.version_for(query).await.filter(|_| !volatile) else { return Ok(respond(run_sql(app, p, query, files).await?)) };
    let key = format!("{format}{}|{}|{query}", p.rows.map(|n| format!(":{n}")).unwrap_or_default(), p.spread.as_deref().unwrap_or(""));
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
    let version = app.lake.version_for(query).await; // (read after `covers`: this run reads at least this)
    let body = run_sql(app, p, query, false).await?;
    if let Some(v) = version {
        app.results.put(key, v, body.clone());
    }
    slot.1 = Some(body.clone());
    Ok(respond(body))
}

/// Run a query (across the cluster if it's worth it) and format the result.
async fn run_sql(app: &App, p: &SqlParams, query: &str, files: bool) -> anyhow::Result<bytes::Bytes> {
    let batches = match files {
        true => app.query_as(&crate::asof::rewrite(query)?, Some("0"), true).await?, // (a file here: this node only)
        false => app.query(query, p.spread.as_deref()).await?,
    };
    let explained = crate::write::first_word(query).get(..7).is_some_and(|w| w.eq_ignore_ascii_case("explain"));
    let batches = if explained { crate::ext::readable_rows(batches)? } else { batches }; // (files as SQL named them)
    answer(&batches, p)
}

/// Rows as `p` asks: `render`'s, a typed answer's first page as many rows as `?rows=` says.
fn answer(batches: &[RecordBatch], p: &SqlParams) -> anyhow::Result<bytes::Bytes> {
    match (p.format.as_deref(), p.rows) {
        (Some("typed"), Some(n)) => Ok(typed(batches, n.clamp(1, MOST))?.into()),
        (format, _) => render(batches, format),
    }
}

/// Rows as `?format=` asks: JSON, a text table, or Arrow IPC (straight into pandas, Polars and
/// DuckDB, no JSON parsing); or a file of them to download: CSV, TSV, NDJSON, Parquet, Excel.
pub fn render(batches: &[RecordBatch], format: Option<&str>) -> anyhow::Result<bytes::Bytes> {
    Ok(bytes::Bytes::from(match format {
        Some("table") => pretty_format_batches(batches)?.to_string().into_bytes(),
        Some("typed") => typed(batches, SHOWN)?,
        Some("arrow") => crate::query::ipc(batches)?,
        Some(f @ ("csv" | "tsv")) => {
            let mut w = datafusion::arrow::csv::WriterBuilder::new().with_header(true).with_delimiter(if f == "tsv" { b'\t' } else { b',' }).build(Vec::new());
            for b in batches {
                w.write(b)?;
            }
            w.into_inner()
        }
        Some("ndjson") => {
            let mut w = arrow_json::LineDelimitedWriter::new(Vec::new());
            w.write_batches(&batches.iter().collect::<Vec<_>>())?;
            w.finish()?;
            w.into_inner()
        }
        Some("parquet") => {
            let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
            let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None)?;
            for b in batches {
                w.write(b)?;
            }
            w.into_inner()?
        }
        Some("xlsx") => crate::xlsx::workbook(batches)?,
        _ => {
            let mut w = arrow_json::ArrayWriter::new(Vec::new());
            w.write_batches(&batches.iter().collect::<Vec<_>>())?;
            w.finish()?;
            w.into_inner()
        }
    }))
}

/// The rows of an answer the console is sent at once, a page, unless it asks for another number
/// (`?rows=`, its rows a page), at most `MOST`.
const SHOWN: usize = 10_000;
const MOST: usize = 100_000;

/// The console's answer (`?format=typed`, `console/console.js`): the columns with their types, the
/// first `shown` rows as lists in the columns' order (two columns may share a name: a join's), how
/// many rows there were, and the id it is kept under (`pages.rs`): its other pages, and every row
/// downloaded, are read from there (`page`), not run again.
fn typed(batches: &[RecordBatch], shown: usize) -> anyhow::Result<Vec<u8>> {
    let columns: Vec<Value> = batches.first().map(|b| b.schema().fields().iter().map(|f| j!({"name": f.name(), "type": crate::query::type_name(f.data_type())})).collect()).unwrap_or_default();
    let mut w = arrow_json::ArrayWriter::new(Vec::new());
    let mut left = shown;
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let b = b.slice(0, left.min(b.num_rows()));
        left -= b.num_rows();
        let exact: Vec<datafusion::arrow::array::ArrayRef> = b.columns().iter().map(exact).collect::<Result<_, _>>()?;
        let numbered: Vec<_> = b.schema().fields().iter().zip(&exact).enumerate().map(|(i, (f, c))| f.as_ref().clone().with_name(i.to_string()).with_data_type(c.data_type().clone())).collect(); // (unique keys)
        w.write(&RecordBatch::try_new(Arc::new(datafusion::arrow::datatypes::Schema::new(numbered)), exact)?)?;
        if left == 0 {
            break;
        }
    }
    w.finish()?;
    let objects: Vec<serde_json::Map<String, Value>> = serde_json::from_slice(&w.into_inner()).unwrap_or_default();
    let rows: Vec<Value> = objects.into_iter().map(|mut o| Value::Array((0..columns.len()).map(|i| o.remove(&i.to_string()).unwrap_or(Value::Null)).collect())).collect();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let pages = crate::pages::keep(batches);
    Ok(serde_json::to_vec(&j!({"columns": columns, "rows": rows, "total": total, "pages": pages}))?)
}

/// `GET /sql/pages/{id}?from=10000&rows=10000`: more rows of an answer the console was sent
/// (`pages.rs`), as `typed` has them; with `&format=csv|tsv|ndjson|parquet|xlsx`, every row of it as
/// a file to download (`render`), not run again. 410 once it is no longer kept (run it again).
async fn page(Path(id): Path<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    let num = |k: &str, or: usize| q.get(k).and_then(|v| v.parse().ok()).unwrap_or(or);
    let (from, rows, format) = (num("from", 0), num("rows", SHOWN).clamp(1, MOST), q.get("format").map(String::as_str).filter(|f| *f != "typed"));
    let got = match format {
        Some(f) => crate::pages::page(&id, 0, usize::MAX).map(|b| render(&b, Some(f))),
        None => crate::pages::page(&id, from, rows).map(|b| typed(&b, rows).map(bytes::Bytes::from)),
    };
    match got {
        Some(Ok(body)) => ([("content-type", content_type(format))], body).into_response(),
        Some(Err(e)) => E(e).into_response(),
        None => (StatusCode::GONE, "this answer's rows are no longer kept here: run it again").into_response(),
    }
}

/// A column as JavaScript can hold it exactly: decimals as their digits (`1.50`), and 64-bit
/// integers past 2^53 as text.
fn exact(c: &datafusion::arrow::array::ArrayRef) -> anyhow::Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::{array::AsArray, compute, datatypes::{DataType, Int64Type, UInt64Type}};
    const SAFE: i64 = (1 << 53) - 1;
    let text = match c.data_type() {
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..) => true,
        DataType::Int64 => compute::min(c.as_primitive::<Int64Type>()).is_some_and(|m| m < -SAFE) || compute::max(c.as_primitive::<Int64Type>()).is_some_and(|m| m > SAFE),
        DataType::UInt64 => compute::max(c.as_primitive::<UInt64Type>()).is_some_and(|m| m > SAFE as u64),
        _ => false,
    };
    Ok(if text { compute::cast(c, &DataType::Utf8)? } else { c.clone() })
}

#[derive(Deserialize)]
struct WatchParams {
    after: Option<u64>, // default: from now on (earlier: a replay, as far back as the log is kept)
    #[serde(default)]
    marks: bool, // after each batch of rows, a `{"_after": N}` line: resume with `?after=N`
    #[serde(default)]
    changes: bool, // the change feed (`change::feed`): old versions too, and what each row is
}

/// New rows of a table as NDJSON, pushed the moment they commit (a view's rows included); with
/// `?changes=true`, its changes: UPDATE's and DELETE's too, with the rows' system columns.
async fn watch(State(app): State<App>, Path(name): Path<String>, Query(p): Query<WatchParams>) -> Response {
    if let Err(e) = crate::auth::check_all(&name) {
        return (StatusCode::FORBIDDEN, e.to_string()).into_response(); // (a user's: the whole rows)
    }
    let hwm = app.lake.hwm.subscribe();
    let after = p.after.unwrap_or_else(|| app.lake.visible());
    let (marks, changes) = (p.marks, p.changes);
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
                let rows = match changes {
                    true => crate::change::feed(&app.lake, &name, after, Some(now)).await,
                    false => tail(&app.lake, &name, after, Some(now), false).await,
                };
                let chunk = rows.and_then(|b| ndjson(&b)).map(mark);
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

/// `GET /ready`: 200 once this node answers as it should (it holds what its leader had: invariant
/// 197), 503 while it catches up or drains (`drain.rs`): a load balancer's readiness probe.
async fn ready(State(app): State<App>) -> Response {
    match (crate::drain::draining(), *app.lake.caught.borrow()) {
        (true, _) => (StatusCode::SERVICE_UNAVAILABLE, "stopping").into_response(),
        (_, false) => (StatusCode::SERVICE_UNAVAILABLE, "catching up with the leader").into_response(),
        _ if app.cluster.cut_off() => (StatusCode::SERVICE_UNAVAILABLE, "can't reach the bucket: another node leads").into_response(),
        _ => "ready".into_response(),
    }
}

async fn stats(State(app): State<App>) -> Json<Value> {
    let c = &app.cluster;
    let role = if c.reader { "reader" } else if app.seq.is_some() { "leader" } else { "follower" };
    let mut s = j!({"lake": crate::ddl::lake_name(&app.lake), "role": role, "leader": c.leader.addr, "term": c.leader.n, "nodes": c.nodes(),
                    "hwm": *app.lake.hwm.borrow(), "shard_runs": c.shard_runs.load(std::sync::atomic::Ordering::Relaxed), "python_workers": crate::python::workers(),
                    "live_queries": crate::live::OPEN.load(std::sync::atomic::Ordering::Relaxed)});
    s["version"] = j!(crate::format::VERSION);
    s["format"] = j!(crate::format::of(&app.lake.cat).await.map(|f| f.format).unwrap_or_default()); // (the lake's: ADR-039)
    if c.is_leader() {
        s["releases"] = j!(c.releases()); // (each live node's: a rolling upgrade's progress)
    }
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
    fn into_response(self) -> Response {
        let code = crate::codes::of(&self.0); // (Postgres's SQLSTATE, for any client: ADR-036 §4)
        (StatusCode::INTERNAL_SERVER_ERROR, [("x-pondra-sqlstate", code)], crate::ext::said(&self.0)).into_response()
    }
}
