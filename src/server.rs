//! HTTP API. Every node ingests (`/append`), answers SQL and streams changes (`/watch`); metadata
//! writes (tables, views, tasks, bulk inserts, tiering) go to the leader (followers forward them).
use crate::cluster::Cluster;
use crate::log::{decode_flush, Ack, Log, Sequencer, Src};
use crate::query::{latest_sql, raw, session, tail};
use crate::answer::schema;
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
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::arrow::{compute::concat_batches, json as arrow_json, record_batch::RecordBatch};
use futures::StreamExt;
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

    /// The bytes of answers remembered now.
    pub fn held(&self) -> usize { self.0.lock().unwrap().1 }
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
        .route("/cluster/checked", post(|State(app): State<App>, body: Bytes| async move {
            let seq = app.seq.clone().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
            let (table, src, batch) = crate::constraints::decode(body)?;
            Ok::<_, E>(Json(crate::constraints::leader(app.lake.clone(), seq, table, src, batch).await?))
        }))
        .route("/cluster/lock", post(|State(app): State<App>, Json(r): Json<crate::locks::Req>| async move {
            Ok::<_, E>(Json(crate::locks::serve(&app.lake, r).await?))
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
        .route("/mcp", post(crate::mcp::handle).delete(crate::mcp::end))
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
        .route("/kinds", get(|| async { Json(crate::objects::kinds()) }))
        .route("/routines", get(routines_shown))
        .route("/secrets/{name}", get(secret))
        .route("/stats", get(stats))
        .route("/healthz", get(|| async { "ok" })) // (the process answers: a liveness probe)
        .route("/ready", get(ready))
        .route("/login", post(login).layer(axum::extract::DefaultBodyLimit::max(64 << 10))) // (asked before any sign-in: a small body)
        .route("/whoami", get(whoami))
        .route("/sso", get(|State(app): State<App>| async move { Json(crate::sso::listed(&app.lake).await) }))
        .route("/sso/{provider}/start", get(crate::sso_flow::start)) // (the browser's way in: ADR-074 §5)
        .route("/sso/callback", get(crate::sso_flow::callback))
        .route("/sso/token", post(crate::sso_flow::redeem).layer(axum::extract::DefaultBodyLimit::max(64 << 10)))
        .route("/.well-known/oauth-protected-resource", get(protected_resource))
        .route("/plan", post(apply))
        .route("/apply", post(apply))
        .route("/test", post(apply))
        .route("/status", post(apply))
        .route("/capture", post(apply))
        .route("/export", get(|State(app): State<App>, Query(q): Query<HashMap<String, String>>| async move {
            Ok::<_, E>(Json(match q.get("database") {
                Some(name) => crate::apply::export_attached(&app.lake, name).await?,
                None => crate::apply::export_here(&app.lake).await?,
            }))
        }))
        .route("/objects", get(|State(app): State<App>| async move { Ok::<_, E>(Json(crate::console::objects(&app.lake).await?)) }))
        .route("/metrics", get(|State(app): State<App>| async move { crate::metrics::render(&app).await.map_err(E) }))
        .route("/cluster/commit", post(commit))
        .route("/cluster/log", get(feed))
        .route("/cluster/stage", post(stage))
        .route("/cluster/copy", post(copy))
        .route("/cluster/insert", post(insert_share))
        .route("/cluster/shuffle", get(bucket))
        .route("/cluster/job", post(job))
        .route("/cluster/probe", get(|Query(p): Query<HashMap<String, usize>>| async move { crate::guard::probe(p.get("bytes").copied().unwrap_or(0)) }))
        .route("/cluster/beat", post(beat))
        .nest_service("/cluster/store", crate::served::door_for(app.lake.store.clone(), app.cluster.serves_store)) // (a lake's store, for nodes on other machines: ADR-072 §8)
        .route("/cluster/ack", post(ack))
        .route("/cluster/replica", get(replica))
        .route("/cluster/leader", get(|State(app): State<App>| async move { Json(app.cluster.leader_status()) }))
        .route("/cluster/visible", get(|State(app): State<App>| async move { Json(app.lake.visible()) })) // (a follower's read-your-writes: `write::seen_here`)
        .route("/cluster/kafka", get(|| async { Json(crate::kafka::me()) }))
        .merge(crate::iceberg::rest())
        .merge(crate::sharing::routes()) // (recipients sign in there with their own tokens: `sharing.rs`)
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
        crate::answer::dtype(t)?;
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
    let (path, bytes) = (crate::lake_files::under_files(&path), body.len());
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
        Err(e) if e.is::<crate::store::Changed>() => Ok((StatusCode::PRECONDITION_FAILED, crate::lake_files::changed(&path)).into_response()),
        Err(e) if expect.is_none() && format!("{e:#}").contains("already exists") =>
            Ok((StatusCode::CONFLICT, crate::lake_files::there_already(&path)).into_response()),
        Err(e) => Err(E(e)),
    }
}

/// `GET /files/<path>`: that object's bytes, its version (`etag`, for `If-Match`) and a content
/// type by its name. `?versions`: the versions it keeps, newest first; `?version=<id>`: one's bytes.
async fn get_file(State(app): State<App>, Path(path): Path<String>, Query(q): Query<HashMap<String, String>>) -> Result<Response, E> {
    let path = crate::lake_files::under_files(&path);
    if q.contains_key("versions") {
        let all = crate::files::versions(&app.lake, &path).await?;
        return Ok(Json(all.iter().map(|v| j!({"id": v.id, "at": v.ms, "who": v.who, "bytes": v.bytes})).collect::<Vec<_>>()).into_response());
    }
    let found = match q.get("version") {
        Some(id) => match crate::files::version(&app.lake, &path, id).await {
            Ok(b) => Ok((b, id.clone())),
            Err(e) => return Ok((StatusCode::NOT_FOUND, crate::ext::said(&e)).into_response()), // (not one of its versions, or gone)
        },
        None => app.lake.file(&path).await,
    };
    let (bytes, version) = match found {
        Ok(found) => found,
        Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) => {
            return Ok((StatusCode::NOT_FOUND, crate::lake_files::not_found(&path)).into_response());
        }
        Err(e) => return Err(E(e)),
    };
    Ok(([("content-type", crate::lake_files::content_type(&path)), ("etag", &format!("\"{version}\"")), ("cache-control", "no-store")], bytes).into_response())
}

/// `POST /files/<path>?restore=<id>`: that version made the file again (itself kept as the newest).
async fn restore_file(State(app): State<App>, Path(path): Path<String>, Query(q): Query<HashMap<String, String>>) -> Result<Json<Value>, E> {
    let path = crate::lake_files::under_files(&path);
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

/// `DELETE /files/<path>`: that object gone (a writer's, as `PUT`); its versions stay. A path no file is at: 404.
async fn delete_file(State(app): State<App>, Path(path): Path<String>) -> Result<Response, E> {
    let path = crate::lake_files::under_files(&path);
    match app.lake.remove_file(&path).await {
        Ok(()) => Ok(Json(j!({"removed": path})).into_response()),
        Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) => {
            Ok((StatusCode::NOT_FOUND, crate::lake_files::not_found(&path)).into_response())
        }
        Err(e) => Err(E(e)),
    }
}

/// `GET /sessions/{id}/python`: the variables a session's Python holds, as the console's panel
/// lists them (none, if it has none yet); `DELETE`: stop it (its variables go; the session's
/// temporary tables stay), as a notebook's kernel is restarted. An admin's, as Python is.
async fn session_python(method: axum::http::Method, Path(id): Path<String>, role: axum::Extension<crate::auth::Role>) -> Result<Json<Value>, E> {
    let id = crate::temp::id(&id); // (as the client's requests name it: its user's)
    if *role < crate::auth::Role::Admin {
        return Err(E(crate::codes::coded("42501", "a session's Python is an admin's, as DO is")));
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
async fn choose_python(axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<crate::tls::Peer>, headers: axum::http::HeaderMap, role: axum::Extension<crate::auth::Role>, Json(b): Json<Value>) -> Result<Json<Value>, E> {
    if *role < crate::auth::Role::Admin || !peer.from_here(&headers) {
        return Err(E(crate::codes::coded("42501", "which Python runs is chosen by an admin, on this machine")));
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

/// `GET /routines`: every routine's name, parameters and result; its code only to whoever is shown code
/// (a user granted some tables needs the first three to call one, never the body: S27).
async fn routines_shown(State(app): State<App>) -> Result<Json<Value>, E> {
    let all = crate::routines::listed(&app.lake).await?;
    if crate::auth::shows_code() {
        return Ok(Json(j!(*all)));
    }
    let mut hidden = (*all).clone();
    for r in hidden.values_mut() {
        r.body.clear();
    }
    Ok(Json(j!(hidden)))
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
        if header.as_deref().is_some_and(|h| h.starts_with("Basic ")) && !peer.may_send_password() {
            return (StatusCode::FORBIDDEN, crate::tls::PASSWORD_PLAIN).into_response(); // (under PONDRA_TLS=optional: a token may cross, a password never)
        }
        if header.as_deref().is_some_and(|h| h.starts_with("Bearer pn_")) && !peer.may_be_node() {
            return (StatusCode::UNAUTHORIZED, "the nodes' key is taken only with a certificate the nodes' authority signed (PONDRA_TLS_CA)").into_response();
        }
    }
    if header.as_deref().is_some_and(|h| h.starts_with("Bearer pb_")) {
        // (a branch's key: it renews or lets go of its own pin, which `ddl` checks, and opens nothing else: ADR-058)
        if req.uri().path() != "/cluster/ddl" {
            return (StatusCode::UNAUTHORIZED, "a branch's key renews its pin, and opens nothing else").into_response();
        }
        let who = crate::auth::Principal::of(crate::auth::Role::None).at("http", from);
        return match crate::panics::door(crate::auth::WHO.scope(who, next.run(req))).await {
            Ok(r) => r,
            Err(m) => E(anyhow::anyhow!(m)).into_response(),
        };
    }
    if req.uri().path().starts_with("/delta-sharing/") {
        // (a recipient's token or a file's signed link, which the door checks: never a user's sign-in)
        return match crate::panics::door(next.run(req)).await {
            Ok(r) => r,
            Err(m) => E(anyhow::anyhow!(m)).into_response(),
        };
    }
    let needed = crate::auth::Auth::needed(req.uri().path(), req.method().as_str());
    let starting = !*app.lake.caught.borrow();
    let token = header.as_deref().and_then(|h| h.strip_prefix("Bearer "));
    let node = token.is_some_and(|t| t.starts_with("pn_") || app.auth.token_role(t) > crate::auth::Role::None); // (the cluster's own calls: never held)
    if starting && needed > crate::auth::Role::None && !node {
        let _ = app.lake.caught_up().await; // (a node that just started signs users in from what its leader had: `open`)
    }
    // (a sign-in with a user's name and password is counted per address and name: past ten failures a minute
    // that pair is refused at once, the right password too; a bearer token isn't counted: `auth::guessing`)
    let client = req.extensions().get::<axum::extract::ConnectInfo<crate::tls::Peer>>().map(|c| c.0.client(req.headers()));
    let basic_by = header.as_deref().filter(|h| h.starts_with("Basic ")).map(|h| (basic_user(Some(h)), client));
    if let Some((name, ip)) = &basic_by {
        if let Some(wait) = crate::auth::guessing(*ip, name) {
            let why = crate::auth::guessing_says(name, wait);
            crate::audit::refused(&app, name, "http", from, &format!("{} {}", req.method(), req.uri().path()), &why);
            tokio::time::sleep(Duration::from_millis(400)).await; // (as a wrong guess does: refused at once, guesses would cost nothing)
            return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", wait.to_string())], why).into_response();
        }
    }
    let signed = match &header {
        Some(h) => crate::users::who(&app.lake, &app.auth, Some(h)).await,
        None => None,
    };
    let who = match signed {
        Some(p) => p,
        None if owner(&app.auth, req.headers()) => crate::auth::Principal::token(crate::auth::Role::Admin), // (the shell's, local()'s: this machine's own folder)
        None if starting && needed == crate::auth::Role::None => crate::auth::Principal::of(crate::auth::Role::None), // (a probe while it catches up: answered, never held)
        None if app.open().await => match crate::auth::cross_site(req.headers(), req.method(), from, &app.cluster.addr) {
            Some(why) => return (StatusCode::FORBIDDEN, why).into_response(), // (a browser's page from elsewhere: `auth::cross_site`)
            None => crate::auth::Principal::of(crate::auth::Role::Admin),
        },
        None if header.is_some() => {
            if let Some((name, ip)) = &basic_by {
                crate::auth::guessed(*ip, name);
            }
            // (an identity provider's token says why it was refused: expired, another audience, no such user here)
            let why = match token.map(str::to_string).or_else(|| basic_password(header.as_deref())) {
                Some(t) => crate::sso::why(&app.lake, &t).await,
                None => None,
            };
            let why = why.unwrap_or_else(|| "wrong token, or user name and password".into());
            crate::audit::refused(&app, &basic_user(header.as_deref()), "http", from, &format!("{} {}", req.method(), req.uri().path()), &why);
            tokio::time::sleep(Duration::from_millis(400)).await; // (a guess costs time)
            return challenge(&app, req.uri().path(), req.headers(), (StatusCode::UNAUTHORIZED, why).into_response()).await;
        }
        None => crate::auth::Principal::of(crate::auth::Role::None),
    };
    let who = who.at("http", from);
    // (a branch's pin: a user granted CLONE may ask for it, and `ddl` lets nothing else of theirs through: ADR-058)
    let pin = req.uri().path() == "/cluster/ddl";
    let cloning = pin && who.access.as_ref().is_some_and(|a| a.clones().is_some());
    if who.role < needed && !cloning {
        return match who.role {
            crate::auth::Role::None if header.is_none() && app.auth.owned() => (StatusCode::UNAUTHORIZED, "this node answers only the program that started it (the shell, connect()): open the console from the link it printed, or start the node with --admin-token to connect other tools").into_response(),
            crate::auth::Role::None if header.is_none() && !app.auth.on() && crate::users::signed(&app.lake).await => (StatusCode::UNAUTHORIZED, "this lake has been served with tokens: start the node with them (--admin-token, --write-token, --read-token), or sign in as a user").into_response(),
            crate::auth::Role::None => challenge(&app, req.uri().path(), req.headers(), (StatusCode::UNAUTHORIZED, "sign in: a token, or a user's name and password").into_response()).await,
            _ => {
                let why = match pin {
                    true => "a pin needs CLONE on this database or a schema of it (GRANT CLONE ON DATABASE … TO …)",
                    false => "this needs more rights than this token's or user's",
                };
                crate::audit::refused(&app, &who.name, "http", from, &format!("{} {}", req.method(), req.uri().path()), why);
                (StatusCode::FORBIDDEN, why).into_response()
            }
        };
    }
    // (a change a node sends its leader for a user runs as that user: `auth::AS_USER`)
    let who = match req.headers().get(crate::auth::AS_USER).and_then(|v| v.to_str().ok()) {
        Some(user) if who.operator => match crate::users::principal(&app.lake, user).await {
            Ok(p) => p.at("http", from),
            Err(e) => return (StatusCode::FORBIDDEN, crate::ext::said(&e)).into_response(),
        },
        _ => who,
    };
    req.extensions_mut().insert(who.role);
    let heavy = queries(req.uri().path());
    let stoppable = stoppable(req.uri().path()); // (a query the node may stop for memory: `oom.rs`)
    // (a stopped statement fails its session's transaction: who asked, and the headers `temp::of` reads the session from)
    let asked = stoppable.then(|| (who.clone(), req.headers().clone()));
    // (a Python procedure of an apply calls back with its lent token: those calls are the apply's, as its statements are)
    let applying = crate::auth::lent_applying(token);
    let run = crate::auth::WHO.scope(who, next.run(req));
    let run = async move {
        match applying {
            true => crate::protect::APPLYING.scope((), run).await,
            false => run.await,
        }
    };
    match if heavy { crate::panics::work(stoppable, run).await } else { crate::panics::door(run).await } {
        Ok(r) => r,
        Err(m) => {
            if let (true, Some((who, headers))) = (m == crate::oom::OOM, asked) {
                failed_for_memory(who, &headers); // (the node stopped the statement: its transaction fails, as an error's does)
            }
            E(anyhow::anyhow!(m)).into_response() // (and the node stays up)
        }
    }
}

/// The queries the node may stop for memory (`oom.rs`): a client's query, which its client asks again
/// or gives up on. Not the cluster's own calls, the bulk writes or a live query, which lives as long as
/// its client.
fn stoppable(path: &str) -> bool {
    matches!(path, "/sql" | "/mcp") || path.starts_with("/sql/pages/")
}

/// A statement the node stopped for memory fails its session's transaction, as an error does
/// (`txn::failed`): the session is found as `sql` finds it, for the caller that asked.
fn failed_for_memory(who: crate::auth::Principal, headers: &axum::http::HeaderMap) {
    let session = crate::auth::WHO.sync_scope(who, || crate::temp::of(headers)).ok().flatten();
    if let Some(s) = session {
        crate::temp::SESSION.sync_scope(Some(s), || crate::txn::failed(crate::oom::OOM));
    }
}

/// The routes whose work is queries' (`panics::work`): planned and run on a runtime of their own,
/// so appends, commits, heartbeats and the commit stream keep theirs.
fn queries(path: &str) -> bool {
    matches!(path, "/sql" | "/mcp" | "/live" | "/tier" | "/cluster/stage" | "/cluster/copy" | "/cluster/insert" | "/cluster/job") || path.starts_with("/sql/pages/") || path.starts_with("/insert/")
}

/// The user name in a Basic header (for the audit log: who tried).
fn basic_user(header: Option<&str>) -> String {
    use base64::Engine;
    let b = header.and_then(|h| h.strip_prefix("Basic ")).and_then(|b| base64::engine::general_purpose::STANDARD.decode(b.trim()).ok());
    b.and_then(|b| String::from_utf8(b).ok()).and_then(|up| up.split_once(':').map(|(u, _)| u.to_string())).unwrap_or_default()
}

/// The password in a Basic header (an identity provider's token sent as one says why it was refused).
fn basic_password(header: Option<&str>) -> Option<String> {
    use base64::Engine;
    let b = header.and_then(|h| h.strip_prefix("Basic ")).and_then(|b| base64::engine::general_purpose::STANDARD.decode(b.trim()).ok())?;
    String::from_utf8(b).ok()?.split_once(':').map(|(_, p)| p.to_string())
}

/// A 401 for MCP says where its identity providers are (RFC 9728, as MCP's clients look for them):
/// `WWW-Authenticate: Bearer resource_metadata="…/.well-known/oauth-protected-resource"`.
async fn challenge(app: &App, path: &str, headers: &axum::http::HeaderMap, mut r: Response) -> Response {
    if path.starts_with("/mcp") && crate::sso::all(&app.lake).await.is_ok_and(|p| !p.is_empty()) {
        let host = headers.get("host").and_then(|h| h.to_str().ok()).unwrap_or(&app.cluster.addr);
        if let Ok(v) = format!("Bearer resource_metadata=\"{}\"", crate::tls::url(&format!("{host}/.well-known/oauth-protected-resource"))).parse() {
            r.headers_mut().insert("www-authenticate", v);
        }
    }
    r
}

/// `GET /.well-known/oauth-protected-resource`: what MCP's clients read to sign in (RFC 9728): this
/// node's MCP and the identity providers that sign its users in. 404 without one.
async fn protected_resource(State(app): State<App>, headers: axum::http::HeaderMap) -> Response {
    let host = headers.get("host").and_then(|h| h.to_str().ok()).unwrap_or(&app.cluster.addr).to_string();
    match crate::sso::resource(&app.lake, &crate::tls::url(&format!("{host}/mcp"))).await {
        Some(v) => Json(v).into_response(),
        None => (StatusCode::NOT_FOUND, "no identity provider signs users in here (CREATE IDENTITY PROVIDER)").into_response(),
    }
}

/// `POST /login` `{"user", "password"}`: a session (`{"token": "ps_…", "until": ms}`) to send as
/// `Authorization: Bearer …` until it ends (`PONDRA_SESSION_HOURS`); the console's sign-in.
async fn login(State(app): State<App>, axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<crate::tls::Peer>, headers: axum::http::HeaderMap, body: Bytes) -> Response {
    let b: Value = serde_json::from_slice(&body).unwrap_or_default(); // (JSON, whatever its content type says)
    let (user, password) = (b["user"].as_str().unwrap_or_default(), b["password"].as_str().unwrap_or_default());
    if !peer.may_send_password() {
        return (StatusCode::FORBIDDEN, crate::tls::PASSWORD_PLAIN).into_response(); // (`tls.rs`)
    }
    let ip = Some(peer.client(&headers));
    if let Some(wait) = crate::auth::guessing(ip, user) {
        let why = crate::auth::guessing_says(user, wait);
        crate::audit::refused(&app, user, "http", Some(peer.addr), "sign in", &why);
        tokio::time::sleep(Duration::from_millis(400)).await; // (as a wrong guess does)
        return (StatusCode::TOO_MANY_REQUESTS, [("retry-after", wait.to_string())], why).into_response();
    }
    if !crate::users::password_ok(&app.lake, user, password).await {
        crate::auth::guessed(ip, user);
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
    // (the leader's answer as it is: its status, content type and the x-pondra- headers, the SQLSTATE and notices among
    // them, which a client of this node reads)
    let status = res.status();
    let kept: Vec<_> = res.headers().iter().filter(|(k, _)| k.as_str() == "content-type" || k.as_str().starts_with("x-pondra-")).map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut resp = Response::new(Body::from(res.bytes().await?));
    *resp.status_mut() = status;
    for (k, v) in kept {
        resp.headers_mut().append(k, v);
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
    let dropped = crate::env::get("PONDRA_DROP_BEATS").map(|f| std::fs::read_to_string(f).unwrap_or_default()).unwrap_or_default();
    if dropped.lines().any(|a| a == p.from) {
        return Err(StatusCode::SERVICE_UNAVAILABLE.into_response());
    }
    let cut_off = || (StatusCode::SERVICE_UNAVAILABLE, [(crate::cluster::CUT_OFF_HEADER, "1")]).into_response(); // (its followers take over at once)
    app.cluster.beat(p.from, crate::format::said(&headers)).map(Json).ok_or_else(cut_off)
}

/// A follower's flush, to be sequenced (leader only).
async fn commit(State(app): State<App>, body: Bytes) -> Result<Json<crate::log::Outcome>, E> {
    let seq = app.seq.as_ref().ok_or_else(|| anyhow::anyhow!("not the leader"))?;
    let flush = decode_flush(body)?;
    crate::auth::flush_from_writer(&flush)?; // (a write token's: rows, and no more)
    Ok(Json(seq.submit(flush).await?))
}

/// This node's share of a distributed query, sent a piece at a time (a big share is on disk).
async fn stage(State(app): State<App>, Json(slice): Json<crate::spmd::Slice>) -> Result<Response, E> {
    let (shape, parts) = crate::spmd::stage(&app.lake, &slice).await?;
    let done = slice.shuffle.is_none().then(|| crate::spill::Gone(slice.id.clone()));
    Ok(Body::from_stream(crate::spmd::reply(&shape, parts, done)).into_response())
}

/// This node's share of an `INSERT … SELECT`, written into the table's folder (see `spmd::insert`):
/// its columns and files.
async fn insert_share(State(app): State<App>, Json((slice, table, stamp)): Json<(crate::spmd::Slice, String, Option<crate::log::Reserved>)>) -> Result<Json<(crate::spmd::Columns, Vec<crate::store::DataFile>)>, E> {
    Ok(Json(crate::spmd::insert_share(&app.lake, &slice, &table, stamp).await?))
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
        crate::query::sent() || crate::temp::mentioned(query) || crate::temp::holds_secrets() || crate::past::mentioned(query) || crate::txn::open() || crate::settings::any() || crate::vars::mentioned(query) || crate::routines::pinned(&self.lake, query).await || crate::auth::limited().is_some()
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
        let explained = crate::text::first_word(query).get(..7).is_some_and(|w| w.eq_ignore_ascii_case("explain"));
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
            let ctx = if files { ctx.enable_url_table() } else { ctx };
            if explained && !query.to_lowercase().contains("pgjson") {
                // (each operator's expected rows: `history::expected`; Postgres's JSON has no place for them)
                ctx.state_ref().write().config_mut().options_mut().explain.show_statistics = true;
            }
            let out = crate::query::frugally(&ctx, |ctx| async move {
                let df = crate::engine::sql(&ctx, query).await?;
                let task = Arc::new(df.task_ctx());
                let (state, logical) = df.into_parts();
                let logical = state.optimize(&logical)?; // (as `create_physical_plan`, the plan it optimized kept: what its filters were about, `learned.rs`)
                let plan = state.query_planner().create_physical_plan(&logical, &state).await?;
                let out = if top { crate::query::collect_or_send(plan.clone(), task).await? } else { crate::query::collect(plan.clone(), task).await? };
                crate::history::planned(&logical, &plan); // (its shape; a slow statement's plan, with what each operator did; what it learned)
                let out = if explained { crate::history::explained(out)? } else { out };
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

// ---------------------------------------------------------------- the API

/// Body: a table's definition (see `write::TableSpec`): `[["user","Utf8"],…]`, or
/// `{"columns": [...], "key": ["user"], "merge": {...}, "publish": [...], "cluster_by": [...]}`.
async fn create_table(State(app): State<App>, Path(name): Path<String>, headers: HeaderMap, body: String) -> Result<Json<Value>, E> {
    if crate::sys::hidden(&name) {
        return Err(E(anyhow::anyhow!("{name} is written by Pondra alone"))); // (a replace's staged table is the leader's own: `write::create_as`)
    }
    if !cluster_call(&app, &headers) {
        crate::protect::check_table(&app.lake, &name).await?; // (it may add columns to a project's table)
    }
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
    let start = std::time::Instant::now();
    let out = append_rows(&app, &name, p, headers, body).await;
    crate::audit::rows(&app, "/append", &name, out.as_ref().err().map(|e| e.to_string()), start.elapsed()); // (the audit log's line for the rows: S23)
    out
}

async fn append_rows(app: &App, name: &str, p: AppendParams, headers: HeaderMap, body: Bytes) -> Result<Json<Ack>, E> {
    let log = app.log()?;
    if !p.producer.is_empty() && p.seq == 0 {
        return Err(E(anyhow::anyhow!(crate::log::SEQ_FROM_1))); // (0 is "nothing yet": its batch would be taken for a retry)
    }
    crate::auth::check_rows(&name)?; // (its INSERT grant, as SQL's)
    let meta = app.lake.cat.meta(&table_key(&name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?;
    let schema = schema(&meta.logical_columns())?; // (rows come under SQL's names; the log keeps stored ones: ADR-022)
    let arrow = headers.get("content-type").is_some_and(|v| v.as_bytes().starts_with(b"application/vnd.apache.arrow"));
    let read = || -> anyhow::Result<_> {
        Ok(if arrow {
            // Columns by name, cast to the table's types (pandas, Polars and Arrow differ in string
            // types); a column left out takes its DEFAULT, or is null (as in JSON), e.g. `_deleted`,
            // or one added since.
            let ipc = StreamReader::try_new(&body[..], None)?;
            let given: Vec<String> = ipc.schema().fields().iter().map(|f| f.name().clone()).collect();
            let ipc = ipc.collect::<Result<Vec<_>, _>>()?;
            (ipc.iter().map(|b| crate::query::conform(b, &schema)).collect::<anyhow::Result<Vec<_>>>()?, Some(given))
        } else {
            (vec![crate::json_rows::from_json(&body, &schema)?], None) // (integers round as SQL writes them: invariant 293)
        })
    };
    // (rows that can't be read as the table's are the sender's: 22P02, never the node's 500)
    let (batches, given) = read().map_err(|e| crate::codes::coded("22P02", format!("the rows can't be read as {name}'s: {}", crate::ext::said(&e))))?;
    let batch = concat_batches(&schema, &batches)?;
    let batch = match (crate::defaults::any(&meta), given) {
        (false, _) => batch,
        (true, Some(given)) => crate::defaults::fill(&app.lake, &meta, batch.clone(), |c| (!given.iter().any(|g| g == c)).then(|| crate::defaults::all(batch.num_rows()))).await?,
        (true, None) => {
            let absent = crate::defaults::absent_keys(&meta, &body)?; // (a JSON row without the key)
            crate::defaults::fill(&app.lake, &meta, batch, |c| absent.get(c).cloned()).await?
        }
    };
    crate::auth::check_load(&meta, name, &batch)?; // (a keyed table's rows replace their keys': UPDATE too; marked _deleted, DELETE: S11)
    Ok(Json(log.append(name.to_string(), Src { producer: p.producer, seq: p.seq, prev: p.prev }, batch).await?))
}

#[derive(Deserialize)]
struct InsertParams {
    job: String,
}

/// Bulk `INSERT INTO name <body SQL>`: this node runs the query and writes the Parquet (no log);
/// the leader records the files, together with `job` so a retried job is applied once. Creates
/// the table if missing.
async fn insert(State(app): State<App>, Path(name): Path<String>, Query(p): Query<InsertParams>, query: String) -> Result<Json<Value>, E> {
    let start = std::time::Instant::now();
    let out = insert_rows(&app, &name, p, query).await;
    crate::audit::rows(&app, "/insert", &name, out.as_ref().err().map(|e| e.to_string()), start.elapsed()); // (the audit log's line for the rows: S23)
    out
}

async fn insert_rows(app: &App, name: &str, p: InsertParams, query: String) -> Result<Json<Value>, E> {
    ensure!(!app.cluster.reader, "read-only node");
    if app.lake.cat.meta(&table_key(&name)).await?.is_none() {
        crate::auth::at_least(crate::auth::Role::Admin, "making a table")?; // (as CREATE TABLE is)
    }
    crate::auth::check_rows(&name)?; // (its INSERT grant, as SQL's)
    let query = crate::routines::expand(&app.lake, &query).await?;
    let ctx = session(&app.lake, &query, "").await?;
    match crate::write::write_files(&app.lake, &ctx, &name, &query, &p.job, crate::write::stamp(&app.lake, &name, Some(&app.to())).await?).await? {
        Some(f) => Ok(Json(app.record_files(f).await?)),
        None => Ok(Json(j!({"duplicate": true}))),
    }
}

/// Record an INSERT's files (from another node, or a `pondra sql` somewhere else).
async fn files(State(app): State<App>, Json(f): Json<crate::write::Files>) -> Result<Json<Value>, E> {
    let meta = app.lake.cat.meta(&table_key(f.table())).await?;
    crate::auth::files_from_writer(meta.as_ref().map(|m| m.folder(f.table())), &f.paths())?; // (a write token's: into its table's folder)
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

/// Whether a request is the cluster's own (the nodes' key, an operator's token, or none on an open
/// lake), which the door it first came in by checked. A user's sign-in, Basic included, is not, so
/// the routes the cluster forwards to check a protected database's objects for it (`protect.rs`).
fn cluster_call(app: &App, headers: &HeaderMap) -> bool {
    match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        None => true,
        // (a `pn_` key counts only without an admin token, as in `users::who`)
        Some(h) => h.strip_prefix("Bearer ").is_some_and(|b| (b.starts_with("pn_") && !app.auth.admin_set()) || app.auth.token_role(b) > crate::auth::Role::None),
    }
}

/// A `CREATE`/`DROP` of a schema, view or table that a follower's SQL asked for (`ddl.rs`), and a
/// branch's pin (ADR-058). A follower sends it to the leader, asked with the caller's own token.
async fn ddl(State(app): State<App>, headers: HeaderMap, role: Option<axum::Extension<crate::auth::Role>>, Json(mut d): Json<crate::ddl::Ddl>) -> Result<Json<Value>, E> {
    let bearer = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|h| h.strip_prefix("Bearer ")).map(String::from);
    let key = bearer.as_deref().filter(|k| k.starts_with("pb_"));
    if let Some(key) = key {
        crate::branch::keyed(&app.lake, &d, key).await?; // (a branch's key: its own pin, nothing else)
    } else if role.is_none_or(|r| r.0 < crate::auth::Role::Admin) {
        crate::branch::cloning(&d)?; // (a user with CLONE: a pin, and nothing else)
    }
    if !cluster_call(&app, &headers) && key.is_none() {
        crate::protect::check_ddl(&app.lake, &d).await?;
    }
    crate::protect::stamp(&mut d); // (who lifts a protection, when the statement doesn't say)
    let out = match &app.seq {
        Some(seq) => crate::write::ddl_here(&app.lake, seq, &app.lock, d.clone()).await?,
        None if !app.cluster.is_leader() => Box::pin(crate::write::send_as(&app.lake.url, crate::write::Request::Ddl(d.clone()), bearer.as_deref())).await?, // (the leader makes it)
        None => {
            let _guard = app.lock.lock().await;
            crate::ddl::apply(&app.lake, d.clone()).await?
        }
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
        let shown = crate::answer::schema(&meta.columns.iter().filter(|(c, _)| c != "_deleted").cloned().collect::<Vec<_>>())?; // (as `SELECT *` shows it)
        let row = crate::serve::lookup(lake, &name, &meta, &key).await?.map(|r| crate::query::conform(&r, &shown)).transpose()?; // (by name: a row from before an ADD COLUMN lacks it)
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
    catalog: Option<String>, // "postgres": Postgres's catalog, as the Postgres port answers it (the DB-API, SQLAlchemy: `pg_catalog`)
}

/// Is this request from whoever started the node here (the shell's or `local()`'s `PONDRA_OWNER_KEY`, a
/// library's host's key: the node's `Auth::owned_by`)? Then its SQL may read files on this machine, as
/// `pondra sql` may (invariant 21: nobody else's).
fn owner(auth: &crate::auth::Auth, headers: &axum::http::HeaderMap) -> bool {
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    crate::auth::lent(token).is_some_and(|l| l.1) // (a Python procedure the owner called)
        || headers.get("x-pondra-owner").and_then(|h| h.to_str().ok()).is_some_and(|h| auth.is_owner(h))
}

/// `POST /sql`: statements (SQL, or JSON with `$name` parameters, or that and tables of the
/// caller's own: `routines::Request`), run in order; the last one's answer. A single query as it
/// was sent may be answered from `Results`.
async fn sql(State(app): State<App>, Query(p): Query<SqlParams>, role: axum::Extension<crate::auth::Role>, headers: axum::http::HeaderMap, body: Bytes) -> Response {
    let files = owner(&app.auth, &headers); // (the program that started this node: its files, and URLs no secret covers)
    let session = match crate::temp::of(&headers) { // (its temporary tables: `temp.rs`)
        Ok(session) => session,
        Err(e) => return E::from(e).into_response(), // (a session name that isn't one: refused, saying what one is)
    };
    let postgres = match p.catalog.as_deref() {
        None => false,
        Some("postgres") => true,
        Some(other) => return E(crate::codes::coded("22023", format!("catalog={other}: the catalog a request may ask for is postgres (Postgres's, as the Postgres port answers it)"))).into_response(),
    };
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    let vars = crate::auth::lent_vars(token); // (Python code a run lent a connection to: the run's variables)
    let format = p.format.clone(); // (an answer handed to the door in pieces is sent in this format)
    let (hand, mut handed) = tokio::sync::oneshot::channel(); // (the door's end of an answer in pieces: `query::Pieces`)
    let statement = crate::query::PIECES.scope(crate::query::Pieces::new(hand), sql_as(app, p, role, headers, body));
    // (the answer's memory is counted here until its body is sent: `query::collect`, `answer`)
    let go = crate::query::HELD.scope(Default::default(), crate::vars::within(vars.clone(), crate::temp::SESSION.scope(session.clone(), crate::ext::scope(files, crate::pg_catalog::speaking(postgres, Box::pin(statement))))));
    // (`guard` gave this request its principal and its apply flag, outside this handler: a request whose answer is
    // handed over runs on after it, and its history and audit still need who asked, so `auth::carried` takes WHO here)
    let applying = crate::protect::APPLYING.try_with(|_| ()).is_ok();
    let applied = async move { if applying { crate::protect::APPLYING.scope((), go).await } else { go.await } };
    let mut go = Box::pin(crate::auth::carried(crate::routines::with_notices(crate::python::holder(vars.is_some(), applied)))); // (a lent token's code holds a slot: what it calls takes none; boxed: a statement's future is big)
    let (out, heard) = tokio::select! {
        biased;
        done = &mut go => done,
        Ok(h) = &mut handed => {
            // Its answer goes in pieces as its plan makes them (`query::send_rest`): the rest of the request, the
            // pieces and the statement's end, is polled on the queries' runtime while its client reads; the plan's
            // partition tasks stay where the prefix spawned them. The node may still stop it for memory (`oom.rs`).
            crate::panics::aside(crate::panics::work(true, go));
            return held_session(Rendered::Pieces(pieces(h)).respond(format.as_deref()), session.as_deref());
        }
    };
    let mut r = match handed.try_recv() {
        Ok(h) => Rendered::Pieces(pieces(h)).respond(format.as_deref()), // (handed over as the request ended: its rows still come in pieces)
        Err(_) => out.unwrap_or_else(IntoResponse::into_response),
    };
    r = held_session(r, session.as_deref());
    if let Some(h) = notices(&heard) {
        r.headers_mut().insert("x-pondra-notices", h);
    }
    r
}

/// The header that keeps a client to this node while its session holds temporary tables (`temp::holds`).
fn held_session(mut r: Response, session: Option<&str>) -> Response {
    if session.is_some_and(crate::temp::holds) {
        r.headers_mut().insert("x-pondra-session", axum::http::HeaderValue::from_static("held")); // (a client keeps to this node meanwhile)
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
    if crate::auth::lent(token).is_none() {
        return Err(E(crate::codes::coded("42501", "only a procedure's code reads a secret: pondra.secret(name), in a Python procedure")));
    }
    let values = crate::ext::reveal(&app.lake, &name.to_lowercase()).await?;
    crate::auth::revealed(token, values.iter().filter(|(k, _)| *k != "type" && *k != "scope").map(|(_, v)| v.clone()));
    Ok(Json(j!(values)))
}

/// `POST /plan`, `/apply`, `/test`, `/status`, `/capture`: a project's files planned here, applied, its tests run,
/// what differs from the database, or what writing it into the files takes (ADR-047 §4, ADR-060 §7), each with the
/// same body.
async fn apply(State(app): State<App>, uri: axum::http::Uri, role: axum::Extension<crate::auth::Role>, headers: axum::http::HeaderMap, Json(ask): Json<crate::apply::Ask>) -> Result<Json<Value>, E> {
    use crate::apply::Verb;
    let verb = match uri.path().rsplit('/').next() {
        Some("plan") => Verb::Plan,
        Some("test") => Verb::Test,
        Some("status") => Verb::Status,
        Some("capture") => Verb::Capture,
        _ => Verb::Apply,
    };
    let who = crate::routines::Who { role: role.0, files: owner(&app.auth, &headers), depth: 0 };
    Ok(Json(crate::apply::ask(&app, verb, ask, who).await?))
}

async fn sql_as(app: App, p: SqlParams, role: axum::Extension<crate::auth::Role>, headers: axum::http::HeaderMap, body: Bytes) -> Result<Response, E> {
    use crate::routines::{Outcome, Who};
    let kind = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or_default();
    let req = crate::routines::Request::read(kind, &body)?;
    // (a Python procedure's own calls, through the token lent to it; anyone else's is a first call)
    let lent = crate::auth::lent(headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "))).is_some();
    let depth = headers.get("x-pondra-depth").filter(|_| lent).and_then(|v| v.to_str().ok()?.parse().ok()).unwrap_or(0);
    let who = Who { role: role.0, files: owner(&app.auth, &headers), depth };
    if let Some(seg) = p.after {
        let mut hwm = app.lake.hwm.subscribe();
        let _ = tokio::time::timeout(Duration::from_secs(30), async { while app.lake.visible() < seg { hwm.changed().await.ok()?; } Some(()) }).await;
    }
    if let ([one], true, true, true, None, false) = (&crate::routines::split(&req.sql)[..], req.params.is_empty(), req.tables.is_empty(), req.views.is_empty(), crate::vars::change(&req.sql), crate::script::is(&req.sql)) {
        let sent = one;
        let one = crate::routines::expand(&app.lake, &crate::vars::bound(one)?).await?; // (`$name`: the session's variables)
        if !crate::write::checkpoint(&one) && !crate::routines::runs_procedure(&one) && crate::write::parse(&one).is_none() && !crate::routines::several(&one) && crate::txn::control(&one).is_none() && !crate::txn::open() && !crate::settings::is(&one) {
            return Ok(crate::audit::statement(&app, sent, query(&app, &p, &one, who.files)).await?); // (the audit log and history keep it as it was sent)
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
        Outcome::Rows(batches) => Ok(answer(&batches, &p)?.respond(p.format.as_deref())),
        Outcome::Done(v) => Ok(Json(v).into_response()),
    }
}

/// One query: a key lookup without planning, a remembered answer, or run (once for everyone who
/// asks at the same moment).
async fn query(app: &App, p: &SqlParams, query: &str, files: bool) -> anyhow::Result<Response> {
    let format = p.format.as_deref().unwrap_or("json");
    let respond = |body: bytes::Bytes| ([("content-type", crate::answer::content_type(p.format.as_deref()))], body).into_response();
    let limited = crate::auth::limited().is_some(); // (a user granted some tables: planned as it may read them, never answered from what another read, nor kept: its grants may change)
    if format == "json" && !crate::temp::mentioned(query) && !limited {
        if let Some(body) = crate::serve::point_sql(&app.lake, query).await? {
            return Ok(respond(body.into())); // a key lookup: no planning
        }
    }
    // The program that started the node (`files`) has its answers remembered apart, run with its
    // rights: a file here it reads is named in the statement as expanded, or in a view it reads, and
    // so never remembered.
    let Some(version) = app.remembers(query).await else { return Ok(run_sql(app, p, query, files).await?.respond(p.format.as_deref())) };
    let key = format!("{format}{}|{}|{}|{query}", p.rows.map(|n| format!(":{n}")).unwrap_or_default(), p.spread.as_deref().unwrap_or(""), if files { "owner" } else { "" });
    let stale = p.stale_ms.filter(|_| p.after.is_none()).map(Duration::from_millis); // (read-your-writes wins)
    let run = || async {
        let rendered = run_sql(app, p, query, files).await?;
        let keep = match &rendered {
            Rendered::Whole(body) => Some(body.clone()),
            Rendered::Pieces(_) | Rendered::Sent => None, // (a big answer: sent to its own caller only)
        };
        Ok((keep, rendered))
    };
    Ok(match app.remembered(key, query, version, stale, run).await? {
        Kept::Bytes(body) => respond(body),
        Kept::Own(rendered) => rendered.respond(p.format.as_deref()),
    })
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
            // is named `"at:…"`, which holds its AT (…) as written: `past::spelled`)
            static OFFSET: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\bAT\s*\(\s*OFFSET\s*=>").expect("a regex"));
            let text = &format!("{text} {}", crate::past::spelled(text));
            let q = text.to_lowercase();
            OFFSET.is_match(text) || !crate::ext_names::names(text).is_empty() || ["now()", "today(", "random(", "current_", "uuid(", "explain", "pondra.runs", "pondra.tasks", "pondra.audit", "pondra$audit", "pondra.history", "pondra$history", "pondra.loads", "pondra.learned", "pondra.variables", "pondra.find", "pondra.parts", "files(", "nextval(", "currval(", "setval("].iter().any(|f| q.contains(f)) // (files outside the lake change on their own; `files()` lists objects put since)
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

/// Run a query (across the cluster if it's worth it) and format the result.
async fn run_sql(app: &App, p: &SqlParams, query: &str, files: bool) -> anyhow::Result<Rendered> {
    let explained = crate::text::first_word(query).get(..7).is_some_and(|w| w.eq_ignore_ascii_case("explain"));
    // (an answer in a format that streams may be sent to the door from its plan: `query::collect_or_send`)
    let _armed = (crate::answer::streams(p.format.as_deref()) && !explained).then(|| crate::query::arm(p.format.as_deref()));
    let batches = match files {
        true => app.query_as(&crate::sys::planned(query)?, Some("0"), true).await?, // (a file here: this node only)
        false => app.query(query, p.spread.as_deref()).await?,
    };
    if crate::query::handed().is_some() {
        return Ok(Rendered::Sent); // (its answer went to the door in pieces as its plan made them)
    }
    let batches = if explained { crate::ext::readable_rows(batches)? } else { batches }; // (files as SQL named them)
    answer(&batches, p)
}

/// An answer as the HTTP door sends it: rendered whole (what `Results` keeps and the others asking at
/// the same moment share), or as a body in pieces, for an answer too big to render as one (`stream`), or
/// `Sent`: its answer went to the door in pieces as its plan made them (`query::collect_or_send`), and the
/// door already has its body (`sql`).
enum Rendered {
    Whole(bytes::Bytes),
    Pieces(Body),
    Sent,
}

impl Rendered {
    fn respond(self, format: Option<&str>) -> Response {
        let body = match self {
            Rendered::Whole(bytes) => Body::from(bytes),
            Rendered::Pieces(body) => body,
            Rendered::Sent => return StatusCode::NO_CONTENT.into_response(),
        };
        ([("content-type", crate::answer::content_type(format))], body).into_response()
    }
}

/// Rows as `p` asks: `render`'s, a typed answer's first page as many rows as `?rows=` says, or, from
/// `PONDRA_STREAM_MB` of Arrow up, in pieces (`stream`).
fn answer(batches: &[RecordBatch], p: &SqlParams) -> anyhow::Result<Rendered> {
    match (p.format.as_deref(), p.rows) {
        (Some("typed"), Some(n)) => Ok(Rendered::Whole(crate::answer::typed(batches, n.clamp(1, crate::answer::MOST), crate::pages::keep)?.into())),
        (format, _) if crate::answer::streams(format) && big(batches) => Ok(Rendered::Pieces(stream(batches.to_vec(), format, crate::query::take_held())?)),
        (format, _) => Ok(Rendered::Whole(crate::answer::render(batches, format, crate::pages::keep)?)),
    }
}

/// Is an answer big enough to send in pieces (`query::stream_bytes`: `PONDRA_STREAM_MB` of Arrow)?
fn big(batches: &[RecordBatch]) -> bool {
    let mut counted = datafusion::common::utils::memory::RecordBatchMemoryCounter::new(); // (as `query::collect` counts it)
    batches.iter().map(|b| counted.count_batch(b)).sum::<usize>() >= crate::query::stream_bytes()
}

/// An answer already collected, sent in pieces of about 1 MB of Arrow, each written as `render` writes its rows
/// (`query::hand_out`), so the pieces together are the answer's bytes. The first piece is written before the body
/// starts, so a format that can't write the answer's columns (a list in CSV) is refused as `render` refuses it,
/// with its message; a failure later cuts the body short.
fn stream(batches: Vec<RecordBatch>, format: Option<&str>, held: Option<MemoryReservation>) -> anyhow::Result<Body> {
    Ok(pieces(crate::query::hand_out(batches, format, held)?))
}

/// The body of an answer in pieces (`query::Handed`): its first piece, then the rest as they are written. A channel
/// that closes before the last piece was written fails the body, so a client never takes a cut answer for a whole one.
fn pieces(h: crate::query::Handed) -> Body {
    let crate::query::Handed { first, rest, ended } = h;
    // (counted until the body's last piece is read, not until its writer ends: the pieces still in the channel are what
    // a node stopping after the writer would cut short, so a node told to stop waits for them: `drain::Busy::carried`)
    let busy = crate::drain::Busy::carried();
    let head = futures::stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(first)) });
    let rest = futures::stream::unfold(Some((rest, ended, busy)), |state| async move {
        let (mut rest, ended, busy) = state?;
        match rest.recv().await {
            Some(piece) => Some((piece, Some((rest, ended, busy)))),
            None if ended.load(std::sync::atomic::Ordering::Acquire) => None, // (the count goes with the state here)
            None => Some((Err(std::io::Error::other("the answer stopped before its end")), None)),
        }
    });
    Body::from_stream(head.chain(rest))
}

/// `GET /sql/pages/{id}?from=10000&rows=10000`: more rows of an answer the console was sent
/// (`pages.rs`), as `typed` has them; with `&format=csv|tsv|ndjson|parquet|xlsx`, every row of it as
/// a file to download (`render`), not run again. 410 once it is no longer kept (run it again).
async fn page(Path(id): Path<String>, Query(q): Query<HashMap<String, String>>) -> Response {
    let num = |k: &str, or: usize| q.get(k).and_then(|v| v.parse().ok()).unwrap_or(or);
    let (from, rows, format) = (num("from", 0), num("rows", crate::answer::SHOWN).clamp(1, crate::answer::MOST), q.get("format").map(String::as_str).filter(|f| *f != "typed"));
    let got = match format {
        Some(f) => crate::pages::page(&id, 0, usize::MAX).map(|b| crate::answer::render(&b, Some(f), crate::pages::keep)),
        None => crate::pages::page(&id, from, rows).map(|b| crate::answer::typed(&b, rows, crate::pages::keep).map(bytes::Bytes::from)),
    };
    match got {
        Some(Ok(body)) => ([("content-type", crate::answer::content_type(format))], body).into_response(),
        Some(Err(e)) => E(e).into_response(),
        None => (StatusCode::GONE, "this answer's rows are no longer kept here: run it again").into_response(),
    }
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
        return (StatusCode::FORBIDDEN, crate::ext::said(&e)).into_response(); // (a user's: the whole rows)
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

async fn stats(State(app): State<App>, headers: axum::http::HeaderMap) -> Json<Value> {
    let c = &app.cluster;
    let role = if c.reader { "reader" } else if app.seq.is_some() { "leader" } else { "follower" };
    let mut s = j!({"lake": crate::ddl::lake_name(&app.lake), "role": role, "leader": c.leader.addr, "term": c.leader.n, "nodes": c.nodes(),
                    "hwm": *app.lake.hwm.borrow(), "shard_runs": c.shard_runs.load(std::sync::atomic::Ordering::Relaxed), "python_workers": crate::python::workers(),
                    "live_queries": crate::live::OPEN.load(std::sync::atomic::Ordering::Relaxed)});
    s["version"] = j!(crate::format::VERSION);
    s["owner"] = j!(owner(&app.auth, &headers)); // (asked with its key: is this the node the asker started? `local()` on a port another node holds)
    s["format"] = j!(crate::format::of(&app.lake.cat).await.map(|f| f.format).unwrap_or_default()); // (the lake's: ADR-039)
    s["keys"] = j!(crate::users::kept(&app.lake).await); // (the lake's own keys: sealed by its master key, or in the clear)
    // Background jobs that failed, by kind: a rising count is a stall (tiering, publishing, …) that
    // otherwise shows only on this node's stderr.
    s["background_failures"] = j!(crate::metrics::failures().into_iter().filter(|(_, n)| *n > 0).collect::<std::collections::BTreeMap<_, _>>());
    s["signed"] = j!(crate::users::signed(&app.lake).await); // (served with tokens once: a node without them answers no one unsigned)
    s["move_failed"] = j!(crate::memory::MOVE_FAILED.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()); // (a move that failed: the lake stays in memory, and why)
    if c.is_leader() {
        s["releases"] = j!(c.releases()); // (each live node's: a rolling upgrade's progress)
    }
    s["row_locks"] = j!(crate::locks::count(&app.lake.url)); // (ADR-067 §3: the row locks this lake's leader holds now)
    if let Some(seq) = &app.seq {
        s["untiered_rows"] = j!(app.lake.backlog.load(std::sync::atomic::Ordering::Relaxed));
        let mut ms = seq.commit_ms.lock().unwrap().clone();
        ms.sort_by(f64::total_cmp);
        let pct = |p: f64| ms.get(((ms.len() as f64 * p) as usize).min(ms.len().saturating_sub(1))).copied();
        s["commits"] = j!(ms.len());
        s["commit_ms_p50"] = j!(pct(0.5));
        s["commit_ms_p95"] = j!(pct(0.95));
    }
    if crate::auth::current().is_none_or(|p| p.role == crate::auth::Role::None) {
        // (the cluster's names and addresses are a signed-in caller's; the rest is a probe's)
        if let Some(o) = s.as_object_mut() {
            for k in ["lake", "leader", "nodes", "releases"] {
                o.remove(k);
            }
        }
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

/// Any error becomes a response whose HTTP status follows its SQLSTATE: a client's bad request is a
/// 4xx, a transient condition 503, a real fault 500 (`codes::http`). The message is the body and
/// `x-pondra-sqlstate` the exact code (ADR-036 §4).
pub struct E(anyhow::Error);
impl<T: Into<anyhow::Error>> From<T> for E {
    fn from(e: T) -> Self { E(e.into()) }
}
impl std::fmt::Display for E {
    // (the words a client is sent, so the audit log says what the client was told: `ext::said`)
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", crate::ext::said(&self.0)) }
}
impl IntoResponse for E {
    fn into_response(self) -> Response {
        let code = crate::codes::of(&self.0); // (Postgres's SQLSTATE, for any client: ADR-036 §4)
        let (status, told) = (crate::codes::http(code), crate::ext::said(&self.0)); // (the words its caller may see)
        let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, [("x-pondra-sqlstate", code)], told).into_response()
    }
}
