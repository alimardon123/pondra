//! `pondra serve <folder of lakes>` (ADR-030, ADR-032): every lake in a folder served as a database, as a
//! database server serves its databases. The server holds no lake itself: each database is a
//! node of its own, in this process (`node::Options::database`, invariant 301), kept by a supervisor of
//! its own (`node::kept`), started the first time a connection or request names it and
//! stopped once nothing has used it for `PONDRA_DATABASE_IDLE_SECS` (600) and it has nothing
//! left to tier. A database's node binds no port: the server reaches it here.
//!
//! * Postgres (`--pg`): by the startup message's `database`, then the connection is its node's
//!   (handed to its Postgres door, `pg::Door`);
//! * HTTP: `/db/{name}/…` is that database's whole API (its node's router, called here), `/databases`
//!   lists them, `/` is the console, and anything else goes to the default database (`--default`,
//!   else `lake`, else the only one).
//!
//! A database's failure or restart is its own: the server and the other databases go on.
//! Every database sees the others as `name.schema.table` (its node attaches the folder's lakes).
//! Its node advertises `host:port/db/name`, so another node can join its cluster through here.
use crate::auth::{Auth, Role};
use anyhow::{bail, Context, Result};
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use serde_json::{json as j, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// A database's running node, kept by its supervisor (`node::kept::supervise`) in this process.
struct Node {
    /// What it is doing now: starting, running, restarting in place, failed or closed.
    state: Arc<std::sync::Mutex<crate::node::State>>,
    /// Its router and its Postgres door, while it runs.
    running: crate::node::kept::Slot,
    /// Its host, which the node tells of a failure or a restart, and the server of a close (`Host::beside`).
    host: crate::node::Host,
    /// Its supervisor, which ends once the node is closed.
    kept: tokio::task::JoinHandle<()>,
    activity: Arc<Activity>,
}

impl Node {
    /// Is it there to answer: not failed, nor closed? (A node restarting in place still is: it answers once started again.)
    fn serves(&self) -> bool {
        !self.kept.is_finished() && !matches!(*self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner), crate::node::State::Failed(_) | crate::node::State::Closed)
    }

    /// Its doors now, or why there are none (it is starting again in place).
    fn doors(&self) -> Result<crate::node::kept::Running, String> {
        if let Some(r) = self.running.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone() {
            return Ok(r);
        }
        Err(match self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone() {
            crate::node::State::Restarting(why) | crate::node::State::Failed(why) => why,
            _ => "the database's node is starting".into(),
        })
    }

    /// Drained (its requests in flight answered) and stopped, as on Ctrl-C, so the next node leads at once; its supervisor
    /// waited for 20 s at most (past that it finishes the stop on its own).
    async fn close(self) {
        self.host.tell(crate::node::Event::Close);
        let _ = tokio::time::timeout(Duration::from_secs(20), self.kept).await;
    }
}

/// What reaches a database's node: the connections and requests in flight, and when the last one
/// ended. A node is idle only when none is in flight (a statement running longer than
/// `PONDRA_DATABASE_IDLE_SECS` isn't cut off), counted from when the last one ended.
struct Activity {
    busy: std::sync::atomic::AtomicUsize,
    last: std::sync::Mutex<Instant>,
}

/// A connection or request in flight to a database's node, from when it is routed until its
/// answer has gone (a Postgres connection: until it closes).
struct Busy(Arc<Activity>);

impl Busy {
    fn new(a: &Arc<Activity>) -> Busy {
        a.busy.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Busy(a.clone())
    }
}

impl Drop for Busy {
    fn drop(&mut self) {
        *self.0.last.lock().unwrap() = Instant::now();
        self.0.busy.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Activity {
    fn idle(&self) -> bool { self.busy.load(std::sync::atomic::Ordering::SeqCst) == 0 && self.last.lock().unwrap().elapsed() >= idle() }
}

struct Server {
    folder: String,
    addr: String,       // (where other nodes and clients reach this server)
    default: Option<String>,
    start: crate::node::Start, // (what each database's node starts with: `pondra serve`'s flags for a lake)
    auth: Arc<Auth>,
    nodes: Mutex<HashMap<String, Node>>,
    starting: Mutex<()>, // (one database's node started at a time: two requests don't start two)
    tasks: crate::spawns::Tasks, // (the server's own loops and connections, ended as it stops; a database's node has its own)
}

type Shared = Arc<Server>;

fn idle() -> Duration { crate::env::secs("PONDRA_DATABASE_IDLE_SECS") }

/// Does `path` hold lakes rather than be one (`pondra serve data`: each a database)? Not when it
/// holds one itself, or is new or empty (it becomes a lake).
pub async fn holds_lakes(path: &str) -> anyhow::Result<bool> {
    Ok(!is_lake(path).await? && !crate::ddl::lakes_found(path, None).await.unwrap_or_default().is_empty()) // (dropped ones too: kept, they are still its)
}

/// Is there a lake at `dir` (a folder, or a bucket's prefix)? One being made counts: its first
/// node claims a term before it writes the catalog, and a node started at the same instant takes
/// it for the lake it is becoming (it exited, "holds other things than lakes").
pub async fn is_lake(dir: &str) -> Result<bool> {
    if !dir.contains("://") {
        let d = std::path::Path::new(dir);
        // (`cluster/`, not `cluster/term/`: a node claiming the first term makes one folder, then the
        // other, and a node started at the same moment looked in between)
        return Ok(d.join("catalog").is_dir() || d.join("cluster").is_dir());
    }
    let store = crate::store::open_store(dir, None)?.1; // (the database server's own folder or bucket, as the environment reaches it)
    for prefix in ["catalog", "cluster/term"] {
        if futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from(prefix)))).await.transpose()?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Where database `name` is: a subfolder, or a prefix under the bucket's.
fn place(folder: &str, name: &str) -> String { format!("{}/{name}", folder.trim_end_matches('/')) }

/// The lakes in the folder: its subfolders that hold one, by name.
async fn databases(folder: &str) -> Vec<String> {
    match crate::ddl::lakes_in(folder, None).await {
        Ok(all) => all.into_iter().map(|(n, _)| n).collect(),
        Err(e) => {
            eprintln!("listing the databases in {folder}: {e:#}");
            vec![]
        }
    }
}

impl Server {
    /// The database a request without `/db/` means.
    async fn default_db(&self) -> String {
        let all = databases(&self.folder).await;
        self.default.clone().or_else(|| all.iter().find(|d| *d == "lake").cloned()).or_else(|| (all.len() == 1).then(|| all[0].clone())).unwrap_or_else(|| "lake".into())
    }

    /// The node of database `name`, started if it isn't running: its doors (or why it has none for now: it is starting
    /// again in place), and the mark of this use (it isn't stopped until that is dropped).
    /// `create`: make the lake if there is none (the default database, on first use).
    async fn node(&self, name: &str, create: bool) -> Result<(Result<crate::node::kept::Running, String>, Busy)> {
        // (a new database's name is checked for the server's own; one that is there is only looked up)
        match create {
            true => crate::ddl::database_name(name)?,
            false => crate::ddl::check(name)?,
        }
        if let Some(n) = self.nodes.lock().await.get(name).filter(|n| n.serves()) {
            return Ok((n.doors(), Busy::new(&n.activity)));
        }
        let _one = self.starting.lock().await;
        let gone = {
            let mut nodes = self.nodes.lock().await;
            if let Some(n) = nodes.get(name).filter(|n| n.serves()) {
                return Ok((n.doors(), Busy::new(&n.activity))); // (another request started it meanwhile)
            }
            nodes.remove(name) // (one that failed: it starts again, as a node that ended does)
        };
        if let Some(n) = gone {
            n.close().await;
        }
        let dir = place(&self.folder, name);
        if let Some(k) = crate::undrop::lake_kept(&dir, None).await? {
            bail!("database \"{name}\" does not exist: it was dropped, and is kept until {} (UNDROP DATABASE {name} brings it back)", crate::undrop::when(k.until()));
        }
        if !create && !is_lake(&dir).await? {
            bail!("database \"{name}\" does not exist (CREATE DATABASE {name})");
        }
        let (host, told) = crate::node::Host::beside();
        let options = crate::node::Options::database(&dir, format!("{}/db/{name}", self.addr), &self.folder, self.start.clone(), &self.addr, host.clone());
        let (state, running) = (Arc::new(std::sync::Mutex::new(crate::node::State::Starting)), crate::node::kept::Slot::default());
        let (first, started) = std::sync::mpsc::channel();
        // (its own task, not the server's: the server's end with its doors, before its databases are closed)
        let kept = crate::spawns::untracked(crate::node::kept::supervise(options, state.clone(), running.clone(), told, first));
        let node = Node { state, running, host, kept, activity: Arc::new(Activity { busy: Default::default(), last: std::sync::Mutex::new(Instant::now()) }) };
        let first = tokio::task::spawn_blocking(move || started.recv_timeout(Duration::from_secs(120))).await?;
        if let Err(why) = first.map_err(|_| "it didn't start in two minutes".to_string()).and_then(|r| r) {
            node.close().await;
            bail!("the node of database {name} stopped as it started: {why}");
        }
        let busy = Busy::new(&node.activity);
        let doors = node.doors();
        self.nodes.lock().await.insert(name.to_string(), node);
        Ok((doors, busy))
    }

    /// Stop the nodes nobody used lately, once they have nothing left to tier.
    async fn reap(&self) {
        let idle: Vec<(String, Option<axum::Router>)> = self.nodes.lock().await.iter().filter(|(_, n)| n.activity.idle()).map(|(k, n)| (k.clone(), n.doors().ok().map(|d| d.router))).collect();
        for (name, router) in idle {
            let untiered = match router {
                Some(router) => untiered_rows(router).await,
                None => 0, // (failed, or starting again: nothing of it to wait for)
            };
            if untiered > 0 {
                continue; // (it tiers first: another look next time)
            }
            let mut nodes = self.nodes.lock().await;
            if !nodes.get(&name).is_some_and(|n| n.activity.idle()) {
                continue; // (used again meanwhile)
            }
            if let Some(n) = nodes.remove(&name) {
                drop(nodes);
                n.close().await;
            }
        }
    }

    /// `DROP DATABASE name` (by `by`): its node stopped, its folder kept as it was, marked dropped, for UNDROP DATABASE
    /// to bring back until `sweep` lets it go (`undrop::LAKE_MARK`); with `purge`, deleted at once. Refused while another
    /// process leads that lake.
    async fn drop_db(&self, name: &str, if_exists: bool, purge: bool, by: &str) -> Result<Value> {
        crate::ddl::check(name)?;
        let dir = place(&self.folder, name);
        if !is_lake(&dir).await? || crate::undrop::lake_kept(&dir, None).await?.is_some() {
            anyhow::ensure!(if_exists, "database \"{name}\" does not exist");
            return Ok(j!({"database": name, "dropped": false}));
        }
        let running = self.nodes.lock().await.remove(name);
        if let Some(n) = running {
            n.close().await;
        }
        if purge {
            crate::ddl::delete_lake(&dir, None).await.with_context(|| format!("database {name}"))?; // (a bucket's prefix: every object under it)
            return Ok(j!({"database": name, "dropped": true}));
        }
        let kept = crate::undrop::keep_lake(&dir, by, None).await.with_context(|| format!("database {name}"))?;
        Ok(j!({"database": name, "dropped": true, "kept_until": kept.until()}))
    }

    /// `UNDROP DATABASE name`: the mark taken off its folder, a database again as it was dropped; its node starts as
    /// the next request for it comes.
    async fn undrop_db(&self, name: &str) -> Result<Value> {
        crate::ddl::check(name)?;
        let dir = place(&self.folder, name);
        if crate::undrop::lake_kept(&dir, None).await?.is_none() {
            return Err(crate::codes::coded("3D000", format!("no dropped database {name} is kept (pondra.dropped lists those that are)")));
        }
        crate::undrop::unkeep_lake(&dir, None).await?;
        Ok(j!({"database": name, "undropped": true, "dir": dir}))
    }
}

/// How many of a database's rows aren't tiered yet, as its `/stats` says (asked in this process, from this machine).
async fn untiered_rows(router: axum::Router) -> u64 {
    use tower::ServiceExt;
    let Ok(req) = crate::node::request("GET", "/stats", &[], vec![], None) else { return 0 };
    let res = router.oneshot(req).await.unwrap_or_else(|never| match never {});
    let body = axum::body::to_bytes(res.into_body(), 16 << 20).await.unwrap_or_default();
    serde_json::from_slice::<Value>(&body).ok().and_then(|s| s["untiered_rows"].as_u64()).unwrap_or(0)
}

/// Run the server until stopped. Each database's node starts with `start` (`pondra serve`'s flags for a lake).
pub async fn serve(folder: String, addr: String, pg: Option<String>, default: Option<String>, start: crate::node::Start, auth: Arc<Auth>) -> Result<()> {
    let folder = match folder.contains("://") {
        true => folder.trim_end_matches('/').to_string(), // (a bucket's prefix)
        false => {
            std::fs::create_dir_all(&folder)?;
            std::fs::canonicalize(&folder)?.to_string_lossy().trim_start_matches(r"\\?\").to_string() // (Windows verbatim prefix)
        }
    };
    let tasks = crate::spawns::Tasks::new(crate::node::native(false).life.clone()); // (a panic in a loop of its own stops the process, as a node's)
    let server: Shared = Arc::new(Server { folder: folder.clone(), addr: addr.clone(), default, start, auth, nodes: Default::default(), starting: Default::default(), tasks });
    let s = server.clone();
    server.tasks.spawn_node(crate::spawns::Vital::Stop, async move {
        for round in 0u64.. {
            if round % 60 == 0 {
                crate::undrop::sweep_lakes(&s.folder, crate::log::now_ms()).await; // (every ten minutes: the dropped databases whose time is up)
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
            s.reap().await;
        }
    });
    if let Some(pg) = pg {
        let (s, listener) = (server.clone(), tokio::net::TcpListener::bind(&pg).await.with_context(|| format!("the Postgres port {pg}"))?);
        server.tasks.spawn_node(crate::spawns::Vital::Stop, async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else { continue };
                let _ = socket.set_nodelay(true);
                let s2 = s.clone();
                s.tasks.spawn_as(crate::auth::Door, async move {
                    let s = s2;
                    if let Err(e) = postgres(&s, socket).await {
                        eprintln!("postgres connection: {e:#}");
                    }
                });
            }
        });
    }
    let app = Router::new()
        .route("/", get(crate::console::server_page))
        .route("/console/settings", get(crate::console::settings).put(crate::console::save_settings))
        .route("/console/{*file}", get(crate::console::file))
        .route("/databases", get(list).post(create))
        .route("/databases/{name}", axum::routing::delete(drop_db))
        .route("/databases/{name}/undrop", axum::routing::post(undrop_db))
        .route("/db/{name}", any(|State(s): State<Shared>, Path(name): Path<String>, req: Request| async move { route(&s, &name, "/", req).await }))
        .route("/db/{name}/{*rest}", any(|State(s): State<Shared>, Path((name, rest)): Path<(String, String)>, req: Request| async move { route(&s, &name, &format!("/{rest}"), req).await }))
        .fallback(|State(s): State<Shared>, req: Request| async move {
            let path = req.uri().path().to_string();
            let db = s.default_db().await;
            route(&s, &db, &path, req).await
        })
        .layer(axum::middleware::from_fn_with_state(server.clone(), plain))
        .with_state(server.clone());
    eprintln!("pondra serve: the lakes in {folder} as databases, on {addr}");
    let listener = crate::tls::Doors::bind(&addr, crate::tls::Door::Http, &server.tasks, crate::auth::Door).await?; // (TLS too: `tls.rs`)
    let stop = async {
        crate::cli::stopped(false).await;
    };
    axum::serve(listener, app.into_make_service_with_connect_info::<crate::tls::Peer>()).with_graceful_shutdown(stop).await?;
    server.tasks.abort_all(); // (its reaper and doors end before its databases' nodes are let go)
    let nodes: Vec<Node> = server.nodes.lock().await.drain().map(|(_, n)| n).collect();
    futures::future::join_all(nodes.into_iter().map(Node::close)).await; // (each drained and stopped, side by side)
    Ok(())
}

fn error(status: StatusCode, e: anyhow::Error) -> Response { crate::server::error_response(Some(status), &e) }

/// The token a request carries, and its role on this server.
fn role(s: &Server, req: &axum::http::HeaderMap) -> Role {
    s.auth.role(crate::auth::Credential::of_headers(req).token())
}

async fn list(State(s): State<Shared>, req: Request) -> Response {
    if role(&s, req.headers()) < Role::Read {
        return (StatusCode::UNAUTHORIZED, "this needs a token").into_response();
    }
    let running: Vec<String> = s.nodes.lock().await.keys().cloned().collect();
    let default = s.default_db().await;
    Json(databases(&s.folder).await.into_iter().map(|d| j!({"name": d, "running": running.contains(&d), "default": d == default})).collect::<Vec<_>>()).into_response()
}

/// `POST /databases` `{"name": "x"}`: a new, empty lake in the folder (what `CREATE DATABASE x` does).
async fn create(State(s): State<Shared>, req: Request) -> Response {
    if role(&s, req.headers()) < Role::Admin {
        return (StatusCode::UNAUTHORIZED, "this needs an admin token").into_response();
    }
    let Ok(body) = axum::body::to_bytes(req.into_body(), 1 << 20).await else { return StatusCode::BAD_REQUEST.into_response() };
    let name = serde_json::from_slice::<Value>(&body).ok().and_then(|v| v["name"].as_str().map(str::to_lowercase)).unwrap_or_default();
    match s.node(&name, true).await {
        Ok(_) => Json(j!({"database": name})).into_response(),
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}

async fn drop_db(State(s): State<Shared>, Path(name): Path<String>, req: Request) -> Response {
    if role(&s, req.headers()) < Role::Admin {
        return (StatusCode::UNAUTHORIZED, "DROP DATABASE needs an admin token").into_response();
    }
    let if_exists = req.uri().query().is_some_and(|q| q.contains("if_exists=true"));
    let purge = req.uri().query().is_some_and(|q| q.contains("purge=true"));
    // (who dropped it, as the node it came through names its user: `pondra.dropped` shows it)
    let by = url::form_urlencoded::parse(req.uri().query().unwrap_or_default().as_bytes()).find(|(k, _)| k == "by").map(|(_, v)| v.to_string()).unwrap_or_default();
    match s.drop_db(&name.to_lowercase(), if_exists, purge, &by).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}

async fn undrop_db(State(s): State<Shared>, Path(name): Path<String>, req: Request) -> Response {
    if role(&s, req.headers()) < Role::Admin {
        return (StatusCode::UNAUTHORIZED, "UNDROP DATABASE needs an admin token").into_response();
    }
    match s.undrop_db(&name.to_lowercase()).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}

/// A request for database `name`, passed to its node's router in this process, as from this machine (the answer streamed
/// back as it comes). While the node starts again in place, 503 57P03 with why, which clients ask again on.
async fn route(s: &Server, name: &str, path: &str, req: Request) -> Response {
    use tower::ServiceExt;
    let name = name.to_lowercase();
    let create = name == s.default_db().await && databases(&s.folder).await.is_empty();
    let (doors, busy) = match s.node(&name, create).await {
        Ok(p) => p,
        Err(e) => return error(StatusCode::NOT_FOUND, e),
    };
    let router = match doors {
        Ok(d) => d.router,
        Err(why) => return (StatusCode::SERVICE_UNAVAILABLE, [("x-pondra-sqlstate", "57P03")], why).into_response(),
    };
    let (parts, body) = req.into_parts();
    let query = parts.uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    let mut out = axum::http::Request::builder().method(parts.method).uri(format!("{path}{query}"));
    for (k, v) in parts.headers.iter().filter(|(k, _)| !matches!(k.as_str(), "host" | "content-length" | "transfer-encoding" | "connection")) {
        out = out.header(k, v);
    }
    let Ok(mut out) = out.body(body) else { return StatusCode::BAD_REQUEST.into_response() };
    // (from this machine, as the node's own door would see this server: who the client is, the `plain` layer said)
    let here = crate::tls::Peer { addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)), tls: false, signed: false, local: true };
    out.extensions_mut().insert(axum::extract::ConnectInfo(here));
    let res = router.oneshot(out).await.unwrap_or_else(|never| match never {});
    let (parts, body) = res.into_parts();
    let answer = futures::StreamExt::map(body.into_data_stream(), move |chunk| {
        let _ = &busy; // (in use until the whole answer has gone: a live query's for as long as it runs)
        chunk
    });
    Response::from_parts(parts, Body::from_stream(answer))
}

// ---------------------------------------------------------------- Postgres

/// A Postgres connection: its startup message says the database; from then on, the bytes go to
/// and from that database's node as they are.
/// A plain connection from another machine, when this server takes TLS: refused, saying why. With
/// no tokens, a browser's page from elsewhere is refused here too (`auth::cross_site`), and what the
/// browser said of its page goes no further: the databases' nodes are asked from this machine.
async fn plain(State(s): State<Shared>, axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<crate::tls::Peer>, mut req: Request, next: axum::middleware::Next) -> Response {
    if !peer.allowed() {
        return (StatusCode::FORBIDDEN, crate::tls::PLAIN).into_response();
    }
    // (a password from another machine crosses only TLS, as a node's own door says: its node sees this machine)
    let credential = crate::auth::Credential::of_headers(req.headers()); // (read alike at every door: `auth::Credential`)
    if let Some(why) = credential.malformed() {
        return (StatusCode::UNAUTHORIZED, why).into_response();
    }
    let basic = matches!(credential, crate::auth::Credential::Password { .. });
    if (basic || req.uri().path().ends_with("/login")) && !peer.may_send_password() {
        return (StatusCode::FORBIDDEN, crate::tls::PASSWORD_PLAIN).into_response();
    }
    if !s.auth.on() && !req.headers().contains_key("authorization") {
        if let Some(why) = crate::auth::cross_site(req.headers(), req.method(), Some(peer.addr), &s.addr) {
            return (StatusCode::FORBIDDEN, why).into_response();
        }
    }
    for k in ["origin", "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest", "sec-fetch-user"] {
        req.headers_mut().remove(k);
    }
    // (who the client is, for the node to count its failed sign-ins by, with the key this server gave its
    // nodes: `Peer::client`, behind a trusted proxy the client it names; whatever the client said goes)
    let client = format!("{} {}", crate::tls::lakes_key(), peer.client(req.headers()));
    req.headers_mut().insert(crate::tls::CLIENT, client.parse().expect("a key and an address are a header value"));
    // (and where it reached this server, with its database's path: a sign-in's redirect address, `Peer::origin`)
    let db = req.uri().path().strip_prefix("/db/").and_then(|r| r.split('/').next()).map(|d| format!("/db/{d}")).unwrap_or_default();
    match peer.origin(req.headers(), req.uri()).and_then(|o| format!("{} {o}{db}", crate::tls::lakes_key()).parse().ok()) {
        Some(origin) => req.headers_mut().insert(crate::tls::ORIGIN, origin),
        None => req.headers_mut().remove(crate::tls::ORIGIN),
    };
    next.run(req).await
}

async fn postgres(s: &Server, tcp: tokio::net::TcpStream) -> Result<()> {
    let addr = tcp.peer_addr()?;
    let mut client = crate::tls::Conn::Plain(tcp);
    let startup = loop {
        let len = client.read_i32().await? as usize;
        anyhow::ensure!((8..=10_000).contains(&len), "not a Postgres client");
        let mut body = vec![0; len - 4];
        client.read_exact(&mut body).await?;
        match (i32::from_be_bytes(body[..4].try_into()?), client.is_tls(), crate::tls::pg()) {
            (80_877_103, false, Some(tls)) => {
                // (SSL: the client's TLS ends here; its database's node is on this machine)
                client.write_all(b"S").await?;
                let crate::tls::Conn::Plain(tcp) = client else { unreachable!() };
                client = crate::tls::Conn::Tls(Box::new(tls.accept(tcp).await?));
            }
            (80_877_103 | 80_877_104, _, _) => client.write_all(b"N").await?, // (GSS: not here)
            (80_877_102, _, _) => return Ok(()),                              // (a cancel: its node is unknown here)
            _ => break [(len as i32).to_be_bytes().to_vec(), body].concat(),
        }
    };
    if !crate::tls::pg_allowed(client.is_tls(), addr) {
        let fields = [("S", "FATAL"), ("V", "FATAL"), ("C", "28000"), ("M", crate::tls::PLAIN)];
        let body: Vec<u8> = fields.iter().flat_map(|(k, v)| [k.as_bytes(), v.as_bytes(), b"\0"].concat()).chain([0u8]).collect();
        client.write_all(&[b"E".to_vec(), ((body.len() + 4) as i32).to_be_bytes().to_vec(), body].concat()).await?;
        return Ok(());
    }
    let params: Vec<String> = startup[8..].split(|b| *b == 0).map(|p| String::from_utf8_lossy(p).to_string()).collect();
    let param = |k: &str| params.chunks(2).find(|p| p.len() == 2 && p[0] == k).map(|p| p[1].clone());
    let fallback = s.default_db().await;
    let name = param("database").or_else(|| param("user")).unwrap_or_else(|| fallback.clone()).to_lowercase();
    let known = databases(&s.folder).await;
    let create = name == fallback && known.is_empty();
    let fatal = |code: &str, text: &str| {
        let fields = [("S", "FATAL"), ("V", "FATAL"), ("C", code), ("M", text)];
        let body: Vec<u8> = fields.iter().flat_map(|(k, v)| [k.as_bytes(), v.as_bytes(), b"\0"].concat()).chain([0u8]).collect();
        [b"E".to_vec(), ((body.len() + 4) as i32).to_be_bytes().to_vec(), body].concat()
    };
    let (door, _busy) = match s.node(&name, create).await {
        Ok((Ok(d), busy)) => (d.pg, busy),
        Ok((Err(why), _)) => {
            client.write_all(&fatal("57P03", &why)).await?; // (starting again in place: the client connects again)
            return Ok(());
        }
        Err(e) => {
            let text = crate::ext::said(&e); // (the words a client is sent; "does not exist" survives said, so the code reads the same)
            let code = if text.contains("does not exist") { "3D000" } else { "XX000" };
            client.write_all(&fatal(code, &text)).await?;
            return Ok(());
        }
    };
    // (the connection is the node's from here: its startup message, then the client's bytes as they come)
    let door = door.context("a database's node has a Postgres door")?;
    let mut ours = handed(&door, (addr, client.is_tls())).await?;
    ours.write_all(&startup).await?;
    tokio::io::copy_bidirectional(&mut client, &mut ours).await?;
    Ok(())
}

/// A connection handed to a database's Postgres door, and this end of it: a pair of sockets in this process, no port.
#[cfg(unix)]
async fn handed(door: &crate::pg::Door, peer: crate::pg::Peer) -> Result<tokio::net::UnixStream> {
    let (ours, theirs) = tokio::net::UnixStream::pair()?;
    door.take_passed(theirs, peer);
    Ok(ours)
}

/// (Windows has no pair of sockets: a listener on this machine for this one connection, which takes ours alone)
#[cfg(not(unix))]
async fn handed(door: &crate::pg::Door, peer: crate::pg::Peer) -> Result<tokio::net::TcpStream> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let ours = tokio::net::TcpStream::connect(listener.local_addr()?).await?;
    ours.set_nodelay(true)?;
    loop {
        let (theirs, from) = listener.accept().await?;
        if from == ours.local_addr()? {
            door.take_handed(theirs, peer);
            return Ok(ours);
        }
    }
}
