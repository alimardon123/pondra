//! `pondra server <folder>` (ADR-030): every lake in a folder served as a database, as a
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
}

/// A database's running node.
struct Node {
    http: u16,
    pg: u16,
    child: tokio::process::Child,
    used: Instant,
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

/// The lakes in the folder: its subfolders that hold one, by name.
fn databases(folder: &str) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(folder).into_iter().flatten().flatten()
        .filter(|e| e.path().join("catalog").is_dir())
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|n| crate::ddl::check(n).is_ok())
        .collect();
    out.sort();
    out
}

/// A port nobody listens on (the OS picks it).
fn free_port() -> Result<u16> { Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port()) }

impl Server {
    /// The database a request without `/db/` means.
    fn default_db(&self) -> String {
        let all = databases(&self.folder);
        self.default.clone().or_else(|| all.iter().find(|d| *d == "lake").cloned()).or_else(|| (all.len() == 1).then(|| all[0].clone())).unwrap_or_else(|| "lake".into())
    }

    /// The node of database `name`, started if it isn't running: (HTTP port, Postgres port).
    /// `create`: make the lake if there is none (the default database, on first use).
    async fn node(&self, name: &str, create: bool) -> Result<(u16, u16)> {
        crate::ddl::check(name)?;
        if let Some(n) = self.nodes.lock().await.get_mut(name) {
            if n.child.try_wait()?.is_none() {
                n.used = Instant::now();
                return Ok((n.http, n.pg));
            }
        }
        let _one = self.starting.lock().await;
        if let Some(n) = self.nodes.lock().await.get_mut(name) {
            if n.child.try_wait()?.is_none() {
                n.used = Instant::now();
                return Ok((n.http, n.pg)); // (another request started it meanwhile)
            }
        }
        let dir = std::path::Path::new(&self.folder).join(name);
        if !dir.join("catalog").is_dir() && !create {
            bail!("database \"{name}\" does not exist (CREATE DATABASE {name})");
        }
        let (http, pg) = (free_port()?, free_port()?);
        let exe = std::env::current_exe()?;
        let mut cmd = tokio::process::Command::new(exe);
        cmd.args(["serve", "--dir", &dir.to_string_lossy(), "--addr", &format!("127.0.0.1:{http}"), "--pg", &format!("127.0.0.1:{pg}"), "--attach-found", &self.folder,
                  "--advertise", &format!("{}/db/{name}", self.addr), "--stop-with-stdin"]);
        if let Some(p) = &self.options.python {
            cmd.args(["--python", p]);
        }
        if let Some(t) = self.options.tier_secs {
            cmd.args(["--tier-secs", &t.to_string()]);
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
        self.nodes.lock().await.insert(name.to_string(), Node { http, pg, child, used: Instant::now() });
        Ok((http, pg))
    }

    /// Stop the nodes nobody used lately, once they have nothing left to tier.
    async fn reap(&self) {
        let idle: Vec<(String, u16)> = self.nodes.lock().await.iter().filter(|(_, n)| n.used.elapsed() >= idle()).map(|(k, n)| (k.clone(), n.http)).collect();
        for (name, http) in idle {
            let stats: Option<Value> = async { crate::cluster::http().get(format!("http://127.0.0.1:{http}/stats")).send().await.ok()?.json().await.ok() }.await;
            if stats.as_ref().and_then(|s| s["untiered_rows"].as_u64()).unwrap_or(0) > 0 {
                continue; // (it tiers first: another look next time)
            }
            if let Some(mut n) = self.nodes.lock().await.remove(&name) {
                drop(n.child.stdin.take()); // (stops as on Ctrl-C: the next node leads at once)
                let _ = tokio::time::timeout(Duration::from_secs(20), n.child.wait()).await;
            }
        }
    }

    /// `DROP DATABASE name`: its node stopped, its folder deleted (refused while another process
    /// leads that lake).
    async fn drop_db(&self, name: &str, if_exists: bool) -> Result<Value> {
        crate::ddl::check(name)?;
        let dir = std::path::Path::new(&self.folder).join(name);
        if !dir.join("catalog").is_dir() {
            anyhow::ensure!(if_exists, "database \"{name}\" does not exist");
            return Ok(j!({"database": name, "dropped": false}));
        }
        if let Some(mut n) = self.nodes.lock().await.remove(name) {
            drop(n.child.stdin.take());
            let _ = tokio::time::timeout(Duration::from_secs(20), n.child.wait()).await;
        }
        let store = crate::store::open_store(&dir.to_string_lossy())?.1;
        if let Some(t) = crate::cluster::latest(&store).await? {
            anyhow::ensure!(!crate::cluster::alive(&store, &t).await, "another process leads database {name} ({}): stop it first", if t.addr.is_empty() { "a pondra sql" } else { &t.addr });
        }
        std::fs::remove_dir_all(&dir).with_context(|| format!("deleting {}", dir.display()))?;
        Ok(j!({"database": name, "dropped": true}))
    }
}

/// Run the server until stopped.
pub async fn serve(folder: String, addr: String, pg: Option<String>, default: Option<String>, options: Options, auth: Arc<Auth>) -> Result<()> {
    std::fs::create_dir_all(&folder)?;
    let folder = std::fs::canonicalize(&folder)?.to_string_lossy().to_string();
    let server: Shared = Arc::new(Server { folder: folder.clone(), addr: addr.clone(), default, options, auth, nodes: Default::default(), starting: Default::default() });
    let s = server.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            s.reap().await;
        }
    });
    if let Some(pg) = pg {
        let (s, listener) = (server.clone(), tokio::net::TcpListener::bind(&pg).await.with_context(|| format!("the Postgres port {pg}"))?);
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else { continue };
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
        .route("/", get(|| async { crate::console::page() }))
        .route("/databases", get(list).post(create))
        .route("/databases/{name}", axum::routing::delete(drop_db))
        .route("/db/{name}", any(|State(s): State<Shared>, Path(name): Path<String>, req: Request| async move { route(&s, &name, "/", req).await }))
        .route("/db/{name}/{*rest}", any(|State(s): State<Shared>, Path((name, rest)): Path<(String, String)>, req: Request| async move { route(&s, &name, &format!("/{rest}"), req).await }))
        .fallback(|State(s): State<Shared>, req: Request| async move {
            let path = req.uri().path().to_string();
            let db = s.default_db();
            route(&s, &db, &path, req).await
        })
        .with_state(server.clone());
    eprintln!("pondra server: the lakes in {folder} as databases, on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let stop = async {
        crate::stopped(false).await;
    };
    axum::serve(listener, app).with_graceful_shutdown(stop).await?;
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
    let default = s.default_db();
    Json(databases(&s.folder).into_iter().map(|d| j!({"name": d, "running": running.contains(&d), "default": d == default})).collect::<Vec<_>>()).into_response()
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
    let create = name == s.default_db() && databases(&s.folder).is_empty();
    let (http, _) = match s.node(&name, create).await {
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
            resp.body(Body::from_stream(res.bytes_stream())).unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
        }
        Err(e) => error(StatusCode::BAD_GATEWAY, e.into()),
    }
}

// ---------------------------------------------------------------- Postgres

/// A Postgres connection: its startup message says the database; from then on, the bytes go to
/// and from that database's node as they are.
async fn postgres(s: &Server, mut client: tokio::net::TcpStream) -> Result<()> {
    client.set_nodelay(true)?;
    let startup = loop {
        let len = client.read_i32().await? as usize;
        anyhow::ensure!((8..=10_000).contains(&len), "not a Postgres client");
        let mut body = vec![0; len - 4];
        client.read_exact(&mut body).await?;
        match i32::from_be_bytes(body[..4].try_into()?) {
            80_877_103 | 80_877_104 => client.write_all(b"N").await?, // (SSL, GSS: not here)
            80_877_102 => return Ok(()),                              // (a cancel: its node is unknown here)
            _ => break [(len as i32).to_be_bytes().to_vec(), body].concat(),
        }
    };
    let params: Vec<String> = startup[8..].split(|b| *b == 0).map(|p| String::from_utf8_lossy(p).to_string()).collect();
    let param = |k: &str| params.chunks(2).find(|p| p.len() == 2 && p[0] == k).map(|p| p[1].clone());
    let name = param("database").or_else(|| param("user")).unwrap_or_else(|| s.default_db()).to_lowercase();
    let known = databases(&s.folder);
    let create = name == s.default_db() && known.is_empty();
    let pg = match s.node(&name, create).await {
        Ok((_, pg)) => pg,
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
    if let Some(n) = s.nodes.lock().await.get_mut(&name) {
        n.used = Instant::now();
    }
    Ok(())
}
