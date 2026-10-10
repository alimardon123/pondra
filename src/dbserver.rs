//! `pondra serve <folder of lakes>` (ADR-030, ADR-032): every lake in a folder served as a database, as a
//! database server serves its databases. The server holds no lake itself: each database is a
//! node of its own (`pondra serve`), started the first time a connection or request names it and
//! stopped once nothing has used it for `PONDRA_DATABASE_IDLE_SECS` (600) and it has nothing
//! left to tier. The server routes to them:
//!
//! * Postgres (`--pg`): by the startup message's `database`, then it passes the bytes through;
//! * HTTP: `/db/{name}/…` is that database's whole API, `/databases` lists them, `/` is the
//!   console, and anything else goes to the default database (`--default`, else `lake`, else the
//!   only one).
//!
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

/// What a database's node is started with, besides its lake and ports.
#[derive(Clone, Default)]
pub struct Options {
    pub python: Option<String>,
    pub tier_secs: Option<f64>,
    pub reader: bool, // (every database's node read-only: `--reader`)
    pub node: Vec<String>, // the rest of `pondra serve`'s options, for each database's node
}

/// A database's running node.
struct Node {
    http: u16,
    pg: u16,
    child: tokio::process::Child,
    activity: Arc<Activity>,
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
    options: Options,
    auth: Arc<Auth>,
    nodes: Mutex<HashMap<String, Node>>,
    starting: Mutex<()>, // (one database's node started at a time: two requests don't start two)
}

type Shared = Arc<Server>;

fn idle() -> Duration { Duration::from_secs(std::env::var("PONDRA_DATABASE_IDLE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(600)) }

/// Does `path` hold lakes rather than be one (`pondra serve data`: each a database)? Not when it
/// holds one itself, or is new or empty (it becomes a lake).
pub async fn holds_lakes(path: &str) -> anyhow::Result<bool> {
    Ok(!is_lake(path).await? && !databases(path).await.is_empty())
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
    let store = crate::store::open_store(dir)?.1;
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
    match crate::ddl::lakes_in(folder).await {
        Ok(all) => all.into_iter().map(|(n, _)| n).collect(),
        Err(e) => {
            eprintln!("listing the databases in {folder}: {e:#}");
            vec![]
        }
    }
}

/// A port nobody listens on (the OS picks it).
fn free_port() -> Result<u16> { Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port()) }

impl Server {
    /// The database a request without `/db/` means.
    async fn default_db(&self) -> String {
        let all = databases(&self.folder).await;
        self.default.clone().or_else(|| all.iter().find(|d| *d == "lake").cloned()).or_else(|| (all.len() == 1).then(|| all[0].clone())).unwrap_or_else(|| "lake".into())
    }

    /// The node of database `name`, started if it isn't running: its HTTP port, its Postgres
    /// port, and the mark of this use (it isn't stopped until that is dropped).
    /// `create`: make the lake if there is none (the default database, on first use).
    async fn node(&self, name: &str, create: bool) -> Result<(u16, u16, Busy)> {
        crate::ddl::check(name)?;
        if let Some(n) = self.nodes.lock().await.get_mut(name) {
            if n.child.try_wait()?.is_none() {
                return Ok((n.http, n.pg, Busy::new(&n.activity)));
            }
        }
        let _one = self.starting.lock().await;
        if let Some(n) = self.nodes.lock().await.get_mut(name) {
            if n.child.try_wait()?.is_none() {
                return Ok((n.http, n.pg, Busy::new(&n.activity))); // (another request started it meanwhile)
            }
        }
        let dir = place(&self.folder, name);
        if !create && !is_lake(&dir).await? {
            bail!("database \"{name}\" does not exist (CREATE DATABASE {name})");
        }
        let (http, pg) = (free_port()?, free_port()?);
        let exe = std::env::current_exe()?;
        let mut cmd = tokio::process::Command::new(exe);
        cmd.args(["serve", "--dir", &dir, "--addr", &format!("127.0.0.1:{http}"), "--pg", &format!("127.0.0.1:{pg}"), "--attach-found", &self.folder,
                  "--advertise", &format!("{}/db/{name}", self.addr), "--stop-with-stdin"]);
        if let Some(p) = &self.options.python {
            cmd.args(["--python", p]);
        }
        if self.options.reader {
            cmd.arg("--reader");
        }
        cmd.args(&self.options.node);
        if let Some(t) = self.options.tier_secs {
            cmd.args(["--tier-secs", &t.to_string()]);
        }
        for v in ["PONDRA_TLS_CERT", "PONDRA_TLS_KEY", "PONDRA_TLS_CA"] {
            cmd.env_remove(v); // (a database's node listens on this machine only: TLS is the server's)
        }
        cmd.env("PONDRA_SERVER_URL", format!("http://{}", self.addr)).stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::null()).kill_on_drop(true);
        let mut child = cmd.spawn().context("starting a database's node")?;
        let started = Instant::now();
        loop {
            if let Some(status) = child.try_wait()? {
                bail!("the node of database {name} stopped as it started ({status})");
            }
            let up = crate::cluster::http().get(format!("http://127.0.0.1:{http}/stats")).timeout(Duration::from_secs(1)).send().await.is_ok_and(|r| r.status().is_success());
            if up {
                break;
            }
            anyhow::ensure!(started.elapsed() < Duration::from_secs(120), "the node of database {name} didn't start in two minutes");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let activity = Arc::new(Activity { busy: Default::default(), last: std::sync::Mutex::new(Instant::now()) });
        let busy = Busy::new(&activity);
        self.nodes.lock().await.insert(name.to_string(), Node { http, pg, child, activity });
        Ok((http, pg, busy))
    }

    /// Stop the nodes nobody used lately, once they have nothing left to tier.
    async fn reap(&self) {
        let idle: Vec<(String, u16)> = self.nodes.lock().await.iter().filter(|(_, n)| n.activity.idle()).map(|(k, n)| (k.clone(), n.http)).collect();
        for (name, http) in idle {
            let stats: Option<Value> = async { crate::cluster::http().get(format!("http://127.0.0.1:{http}/stats")).send().await.ok()?.json().await.ok() }.await;
            if stats.as_ref().and_then(|s| s["untiered_rows"].as_u64()).unwrap_or(0) > 0 {
                continue; // (it tiers first: another look next time)
            }
            let mut nodes = self.nodes.lock().await;
            if !nodes.get(&name).is_some_and(|n| n.activity.idle()) {
                continue; // (used again meanwhile)
            }
            if let Some(mut n) = nodes.remove(&name) {
                drop(nodes);
                drop(n.child.stdin.take()); // (stops as on Ctrl-C: the next node leads at once)
                let _ = tokio::time::timeout(Duration::from_secs(20), n.child.wait()).await;
            }
        }
    }

    /// `DROP DATABASE name`: its node stopped, its folder deleted (refused while another process
    /// leads that lake).
    async fn drop_db(&self, name: &str, if_exists: bool) -> Result<Value> {
        crate::ddl::check(name)?;
        let dir = place(&self.folder, name);
        if !is_lake(&dir).await? {
            anyhow::ensure!(if_exists, "database \"{name}\" does not exist");
            return Ok(j!({"database": name, "dropped": false}));
        }
        if let Some(mut n) = self.nodes.lock().await.remove(name) {
            drop(n.child.stdin.take());
            let _ = tokio::time::timeout(Duration::from_secs(20), n.child.wait()).await;
        }
        crate::ddl::delete_lake(&dir).await.with_context(|| format!("database {name}"))?; // (a bucket's prefix: every object under it)
        Ok(j!({"database": name, "dropped": true}))
    }
}

/// Run the server until stopped.
pub async fn serve(folder: String, addr: String, pg: Option<String>, default: Option<String>, options: Options, auth: Arc<Auth>) -> Result<()> {
    let folder = match folder.contains("://") {
        true => folder.trim_end_matches('/').to_string(), // (a bucket's prefix)
        false => {
            std::fs::create_dir_all(&folder)?;
            std::fs::canonicalize(&folder)?.to_string_lossy().trim_start_matches(r"\\?\").to_string() // (Windows verbatim prefix)
        }
    };
    let server: Shared = Arc::new(Server { folder: folder.clone(), addr: addr.clone(), default, options, auth, nodes: Default::default(), starting: Default::default() });
    let s = server.clone();
    crate::panics::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            s.reap().await;
        }
    });
    if let Some(pg) = pg {
        let (s, listener) = (server.clone(), tokio::net::TcpListener::bind(&pg).await.with_context(|| format!("the Postgres port {pg}"))?);
        crate::panics::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else { continue };
                let _ = socket.set_nodelay(true);
                let s = s.clone();
                tokio::spawn(async move {
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
        .route("/db/{name}", any(|State(s): State<Shared>, Path(name): Path<String>, req: Request| async move { route(&s, &name, "/", req).await }))
        .route("/db/{name}/{*rest}", any(|State(s): State<Shared>, Path((name, rest)): Path<(String, String)>, req: Request| async move { route(&s, &name, &format!("/{rest}"), req).await }))
        .fallback(|State(s): State<Shared>, req: Request| async move {
            let path = req.uri().path().to_string();
            let db = s.default_db().await;
            route(&s, &db, &path, req).await
        })
        .layer(axum::middleware::from_fn(plain))
        .with_state(server.clone());
    eprintln!("pondra serve: the lakes in {folder} as databases, on {addr}");
    let listener = crate::tls::Doors::bind(&addr, crate::tls::Door::Http).await?; // (TLS too: `tls.rs`)
    let stop = async {
        crate::stopped(false).await;
    };
    axum::serve(listener, app.into_make_service_with_connect_info::<crate::tls::Peer>()).with_graceful_shutdown(stop).await?;
    let nodes: Vec<Node> = server.nodes.lock().await.drain().map(|(_, n)| n).collect();
    for mut n in nodes {
        drop(n.child.stdin.take());
        let _ = tokio::time::timeout(Duration::from_secs(20), n.child.wait()).await;
    }
    Ok(())
}

fn error(status: StatusCode, e: anyhow::Error) -> Response { (status, crate::ext::said(&e)).into_response() }

/// The token a request carries, and its role on this server.
fn role(s: &Server, req: &axum::http::HeaderMap) -> Role {
    let token = req.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    s.auth.role(token)
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
    match s.drop_db(&name.to_lowercase(), if_exists).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}

/// A request for database `name`, passed to its node (the answer streamed back as it comes).
async fn route(s: &Server, name: &str, path: &str, req: Request) -> Response {
    let name = name.to_lowercase();
    let create = name == s.default_db().await && databases(&s.folder).await.is_empty();
    let (http, _, busy) = match s.node(&name, create).await {
        Ok(p) => p,
        Err(e) => return error(StatusCode::NOT_FOUND, e),
    };
    let (parts, body) = req.into_parts();
    let query = parts.uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    let url = format!("http://127.0.0.1:{http}{path}{query}");
    let mut out = crate::cluster::http_bare().request(parts.method, url).body(reqwest::Body::wrap_stream(body.into_data_stream()));
    for (k, v) in parts.headers.iter().filter(|(k, _)| !matches!(k.as_str(), "host" | "content-length" | "transfer-encoding" | "connection")) {
        out = out.header(k, v);
    }
    match out.send().await {
        Ok(res) => {
            let mut resp = Response::builder().status(res.status().as_u16());
            for (k, v) in res.headers().iter().filter(|(k, _)| !matches!(k.as_str(), "content-length" | "transfer-encoding" | "connection")) {
                resp = resp.header(k, v);
            }
            let answer = futures::StreamExt::map(res.bytes_stream(), move |chunk| {
                let _ = &busy; // (in use until the whole answer has gone: a live query's for as long as it runs)
                chunk
            });
            resp.body(Body::from_stream(answer)).unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
        }
        Err(e) => error(StatusCode::BAD_GATEWAY, e.into()),
    }
}

// ---------------------------------------------------------------- Postgres

/// A Postgres connection: its startup message says the database; from then on, the bytes go to
/// and from that database's node as they are.
/// A plain connection from another machine, when this server takes TLS: refused, saying why.
async fn plain(axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<crate::tls::Peer>, req: Request, next: axum::middleware::Next) -> Response {
    match peer.allowed() {
        true => next.run(req).await,
        false => (StatusCode::FORBIDDEN, crate::tls::PLAIN).into_response(),
    }
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
    let (pg, _busy) = match s.node(&name, create).await {
        Ok((_, pg, busy)) => (pg, busy),
        Err(e) => {
            let text = format!("{e:#}");
            let code = if text.contains("does not exist") { "3D000" } else { "XX000" };
            let fields = [("S", "FATAL"), ("V", "FATAL"), ("C", code), ("M", text.as_str())];
            let body: Vec<u8> = fields.iter().flat_map(|(k, v)| [k.as_bytes(), v.as_bytes(), b"\0"].concat()).chain([0u8]).collect();
            client.write_all(&[b"E".to_vec(), ((body.len() + 4) as i32).to_be_bytes().to_vec(), body].concat()).await?;
            return Ok(());
        }
    };
    let mut node = tokio::net::TcpStream::connect(("127.0.0.1", pg)).await?;
    node.set_nodelay(true)?;
    node.write_all(&startup).await?;
    tokio::io::copy_bidirectional(&mut client, &mut node).await?;
    Ok(())
}
