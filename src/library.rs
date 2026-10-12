//! A node in a library (ADR-061 §5, §9): the node never takes the process down. In the program, a node
//! that must start again execs itself, and one whose own work fails aborts; a library's host (a
//! notebook, a web server) can't lose its process to either. So a library's node is kept by a
//! supervisor, a thread of its own: a restart halts the node and starts it again in place, and a
//! failure stops it and says why to every request after, until it is opened again.
//!
//! The places that exec, abort or exit ask [`on`] first and call [`restart`] or [`failed`] instead
//! (`cluster::restart_with`, `panics::spawn`, `format::watch`, the catalog's compactor). Helpers the
//! node starts (`pondra lead`, Python looked for beside it) are the program the packages give
//! ([`exe`]), never the host's own executable.
//!
//! A host reaches the node through [`Opened::request`], straight to its router as its owner (the C
//! ABI, `capi/`, and `pondra.open` in Python: `designs/l2-step4-open.md`), and through the loopback
//! door the node opens anyway for answers that stream.
use crate::node::{Node, Options};
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use futures::FutureExt;
use tokio::sync::mpsc;

/// Whether this process runs its node as a library: set by the first [`Opened::open`], never by the
/// program. What the process is, so it outlives each node.
static LIBRARY: AtomicBool = AtomicBool::new(false);

/// Where a node's restarts and failures are sent: the supervisor of the node open now, if any. One
/// node is open at a time in a process.
static SUPERVISOR: Mutex<Option<mpsc::UnboundedSender<Event>>> = Mutex::new(None);

/// Is this process a library's (`Opened::open`), not the program's?
pub fn on() -> bool { LIBRARY.load(SeqCst) }

/// The program a node starts helpers with (`pondra lead`, Python workers looked for beside it): this
/// executable in the program; in a library, the one the packages ship beside it (`PONDRA_LAUNCHER`,
/// which `pondra.open` gives), since the host's executable is Python or Node.
pub fn exe() -> anyhow::Result<PathBuf> {
    match on() {
        false => Ok(std::env::current_exe()?),
        true => crate::env::get("PONDRA_LAUNCHER").map(PathBuf::from).ok_or_else(|| {
            anyhow::anyhow!("this needs the pondra program, and the library was opened without it: give PONDRA_LAUNCHER, the path of the pondra binary (pondra.open gives the one its package ships)")
        }),
    }
}

/// What a node restarting in place unwinds with ([`restart`]): `panics::spawn` lets it go, and
/// `panics::door` answers the request 57P03, as a node catching up does.
#[derive(Debug)]
pub struct Restarting(pub String);

impl std::fmt::Display for Restarting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the node is restarting to rejoin the cluster ({}), catching up with its leader: try again, or ask another node", self.0)
    }
}

enum Event {
    /// Halt the node and start it again (`cluster::restart_with`).
    Restart(String),
    /// Stop the node and say why until it is opened again.
    Failed(String),
    /// `Opened::close`.
    Close,
}

fn send(e: Event) {
    if let Some(s) = SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).as_ref() {
        let _ = s.send(e);
    }
}

/// In a library, a node that must start again (`cluster::restart_with` calls this after its lock
/// files say "restarting"): the supervisor is told, and the task that asked unwinds, past the panic
/// hook (a host's error reporting sees no panic for a takeover).
pub fn restart(why: &str) -> ! {
    send(Event::Restart(why.to_string()));
    std::panic::resume_unwind(Box::new(Restarting(why.to_string())))
}

/// In a library, the node stops with this reason (a vital loop's panic, the compactor, a lake that
/// moved past this build), and every request is answered with it until the node is opened again.
pub fn failed(why: String) {
    eprintln!("stopping: {why}");
    send(Event::Failed(why));
}

/// What the node open in this process is doing now.
#[derive(Clone, Debug, PartialEq)]
pub enum State {
    Starting,
    Running,
    /// Halted, about to start again: why.
    Restarting(String),
    /// Stopped, until opened again: why.
    Failed(String),
    Closed,
}

/// A node in this process, kept by its supervisor across restarts until it is closed.
pub struct Opened {
    state: Arc<Mutex<State>>,
    /// The node's router while it runs (`Node::router`): what [`Opened::request`] calls.
    router: Arc<Mutex<Option<axum::Router>>>,
    events: mpsc::UnboundedSender<Event>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// The key that makes a request the owner's (invariant 61), given by its host (`pondra.open` gives its
    /// connection's) or made at open: what [`Opened::request`] signs with.
    owner: Option<String>,
    /// A lake made for this node alone (`temporary` in the options: `pondra.open()` with no path), removed
    /// once the node has let go of it, by whichever close comes: the host's, or Node's at its end.
    temporary: Mutex<Option<std::path::PathBuf>>,
}

/// An answer to [`Opened::request`]: its status and headers once the node has them, and its body as the
/// node writes it.
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Body,
}

/// An answer's body, a piece at a time as the node writes it (about 1 MB each for a big answer, invariant
/// 288). The node writes a piece or two ahead of whoever reads it, so a big answer is never held whole here:
/// held whole, a 10 M-row answer was 80 MB of new memory to fault in, and `pondra.open()` took twice
/// `local()`'s time and peak memory. Dropping it ends the work that writes it, as a client that goes does.
pub struct Body(mpsc::Receiver<Option<anyhow::Result<bytes::Bytes>>>);

impl Body {
    /// The next piece, waiting for it, or `None` at the end. Any thread may wait: no runtime of its own
    /// is needed. A body whose work ended before its end (a panic) is an error, never a shorter answer.
    pub fn next(&mut self) -> Option<anyhow::Result<bytes::Bytes>> {
        match futures::executor::block_on(self.0.recv()) {
            Some(piece) => piece,
            None => Some(Err(anyhow::anyhow!("the answer's work ended before its end"))),
        }
    }

    /// A body of one piece, made here.
    fn whole(piece: bytes::Bytes) -> Body {
        let (tx, rx) = mpsc::channel(2);
        let _ = tx.try_send(Some(Ok(piece)));
        let _ = tx.try_send(None);
        Body(rx)
    }
}

/// The runtime a library's nodes run on, made by the first open and kept for the next: its threads
/// are the process's, as the queries' runtime's are (`panics::queries`).
fn runtime() -> &'static tokio::runtime::Runtime {
    static R: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    R.get_or_init(|| crate::panics::runtime().thread_name("pondra").build().expect("a runtime"))
}

/// Pauses between tries to start again; five restarts within a minute of each other is giving up.
const PAUSES: [f64; 5] = [0.5, 1.0, 2.0, 4.0, 8.0];

impl Opened {
    /// Opens a node in this process, once it serves (or has failed to, saying why). One at a time: a
    /// second open while one is open is refused by name.
    pub fn open(options: Options) -> anyhow::Result<Opened> {
        let (events, rx) = mpsc::unbounded_channel();
        {
            let mut s = SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner);
            anyhow::ensure!(s.is_none(), "a node is open in this process already: close it first (one at a time)");
            *s = Some(events.clone());
        }
        LIBRARY.store(true, SeqCst);
        let state = Arc::new(Mutex::new(State::Starting));
        let router = Arc::new(Mutex::new(None));
        let (first, started) = std::sync::mpsc::channel();
        let (st, ro) = (state.clone(), router.clone());
        let thread = std::thread::Builder::new().name("pondra-supervisor".into()).stack_size(8 << 20).spawn(move || runtime().block_on(supervise(options, st, ro, rx, first)));
        let thread = match thread {
            Ok(t) => t,
            Err(e) => {
                SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).take();
                return Err(e.into());
            }
        };
        let opened = Opened { state, router, events, thread: Mutex::new(Some(thread)), owner: None, temporary: Mutex::new(None) };
        match started.recv() {
            Ok(Ok(())) => Ok(opened),
            Ok(Err(why)) => Err(anyhow::anyhow!(why)), // (`opened` dropped: closed)
            Err(_) => Err(anyhow::anyhow!("the node's supervisor ended before the node started")),
        }
    }

    /// What the node is doing now: `Err` (why) while it restarts or after it failed.
    pub fn state(&self) -> State { self.state.lock().unwrap_or_else(PoisonError::into_inner).clone() }

    /// Drains the node (its requests in flight answered), stops it, and ends its supervisor. Again
    /// is nothing.
    pub fn close(&self) {
        let _ = self.events.send(Event::Close);
        let thread = self.thread.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(t) = thread {
            let _ = t.join();
            SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).take();
        }
        if let Some(dir) = self.temporary.lock().unwrap_or_else(PoisonError::into_inner).take() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    /// Opens a node from a JSON object of options (`pondra_open`'s, `designs/l2-step4-open.md`): its
    /// own keys `lake`, `addr`, `owner`, `python`, `attach` and `temporary` (the lake is removed at
    /// close), and every other key a setting by name (`memory_gb` is `PONDRA_MEMORY_GB`; `launcher` is
    /// `PONDRA_LAUNCHER`), given through `env::give`, so a misspelt one is refused naming the nearest and
    /// a changed one naming what changed.
    pub fn open_json(json: &str) -> anyhow::Result<Opened> {
        use anyhow::Context;
        let mut o: serde_json::Map<String, serde_json::Value> = serde_json::from_str(json).context("the options are a JSON object")?;
        o.retain(|_, v| !v.is_null());
        let text = |v: serde_json::Value| v.as_str().map_or_else(|| v.to_string(), str::to_string);
        let lake = o.remove("lake").map(text).context("the options name no lake: a folder, or a bucket's URL")?;
        let addr = match o.remove("addr").map(text) {
            Some(a) => a,
            None => std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.to_string(), // (a free port, let go for the node)
        };
        let owner = o.remove("owner").map(text).unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
        anyhow::ensure!(owner.len() >= 16, "an owner key has 16 characters at least");
        let python = o.remove("python").map(text);
        let temporary = o.remove("temporary").is_some_and(|v| v.as_bool() == Some(true) || v.as_str() == Some("true"));
        anyhow::ensure!(!temporary || !lake.contains("://"), "a temporary lake is a folder on this machine, not {lake}");
        let attach = match o.remove("attach") {
            Some(serde_json::Value::Array(a)) => a.into_iter().map(text).collect(),
            Some(v) => vec![text(v)],
            None => vec![],
        };
        // (one at a time, said before any setting is given: a refused open changes nothing)
        anyhow::ensure!(SUPERVISOR.lock().unwrap_or_else(PoisonError::into_inner).is_none(), "a node is open in this process already: close it first (one at a time)");
        crate::env::give(o.into_iter().map(|(k, v)| (if k == "launcher" { "PONDRA_LAUNCHER".into() } else { k }, text(v))))?;
        crate::tls::init()?; // (a certificate that can't be read: said now)
        let set = crate::env::get;
        let auth = crate::auth::Auth::new(set("PONDRA_READ_TOKEN"), set("PONDRA_WRITE_TOKEN"), set("PONDRA_ADMIN_TOKEN"));
        let mut options = Options::local(&lake, &addr);
        options.python = python;
        options.attach = attach;
        // (its loopback door answers only its host, with the key, as a node `local()` starts does: S21)
        options.auth = Arc::new(auth.owned_by(Some(owner.clone())));
        let mut opened = Opened::open(options)?;
        opened.owner = Some(owner);
        if temporary {
            *opened.temporary.get_mut().unwrap_or_else(PoisonError::into_inner) = Some(lake.into());
        }
        Ok(opened)
    }

    /// One request, as the HTTP API takes it (`path` with its query), answered whole: straight to the
    /// node's router, as its owner from this machine, so everything the API does works here with the
    /// same checks (invariants 21, 61). It runs on the library's runtime, never on the caller's thread
    /// (a host's thread may have a small stack, invariant 195, and no runtime). While the node starts,
    /// restarts or has failed, the answer is 503 57P03 with the reason, which clients ask again on.
    pub fn request(&self, method: &str, path: &str, headers: &[(String, String)], body: Vec<u8>) -> anyhow::Result<Answer> {
        let router = self.router.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let Some(router) = router else {
            let why = match self.state() {
                State::Closed => anyhow::bail!("this node is closed: open the lake again"),
                State::Restarting(why) | State::Failed(why) => why,
                State::Starting | State::Running => "the node is starting".into(),
            };
            let headers = vec![("x-pondra-sqlstate".into(), "57P03".into()), ("content-type".into(), "text/plain; charset=utf-8".into())];
            return Ok(Answer { status: 503, headers, body: Body::whole(why.into()) });
        };
        let mut req = http::Request::builder().method(method).uri(path);
        for (k, v) in headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(key) = &self.owner {
            req = req.header("x-pondra-owner", key.as_str());
        }
        let mut req = req.body(axum::body::Body::from(body))?;
        let from = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
        req.extensions_mut().insert(axum::extract::ConnectInfo(crate::tls::Peer { addr: from, tls: false, signed: false, local: true }));
        // (the work runs on the library's runtime; its head comes back first, then its body's pieces, one or
        // two ahead of the reader, a piece of `None` marking the end)
        let (head, headed) = futures::channel::oneshot::channel();
        let (pieces, body) = mpsc::channel(2);
        runtime().spawn(async move {
            use futures::StreamExt;
            use tower::ServiceExt;
            let (parts, body) = router.oneshot(req).await.unwrap_or_else(|never| match never {}).into_parts();
            if head.send(parts).is_err() {
                return;
            }
            let mut stream = body.into_data_stream();
            while let Some(piece) = stream.next().await {
                let (piece, failed) = match piece {
                    Ok(p) if p.is_empty() => continue,
                    Ok(p) => (Ok(p), false),
                    Err(e) => (Err(anyhow::Error::from(e)), true),
                };
                // (the reader gone, the stream goes with this task, and its work with it; a failed piece is the last)
                if pieces.send(Some(piece)).await.is_err() || failed {
                    return;
                }
            }
            let _ = pieces.send(None).await;
        });
        // (any thread may wait for it: no runtime of its own needed, as `block_on` of tokio's would)
        let parts = futures::executor::block_on(headed).map_err(|_| anyhow::anyhow!("the request's work ended before it was answered"))?;
        let headers = parts.headers.iter().filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_string()))).collect();
        Ok(Answer { status: parts.status.as_u16(), headers, body: Body(body) })
    }
}

impl Drop for Opened {
    fn drop(&mut self) { self.close(); }
}

/// The node in place: what exec does to a process. Its tasks end, its statics (its term's lock file,
/// whose file says "restarting", its Python workers) are let go, and its lake with them; nothing waits
/// on the bucket, and the term isn't given back: a fenced or cut-off node has nothing to make durable.
fn halt(node: Option<Node>) {
    crate::panics::abort_all();
    drop(node);
    crate::reset::reset();
}

async fn supervise(options: Options, state: Arc<Mutex<State>>, router: Arc<Mutex<Option<axum::Router>>>, mut events: mpsc::UnboundedReceiver<Event>, first: std::sync::mpsc::Sender<Result<(), String>>) {
    // (the router is there exactly while the node runs: a request at any other time is told why)
    let set = |s: State| {
        let mut r = router.lock().unwrap_or_else(PoisonError::into_inner);
        if s != State::Running {
            r.take();
        }
        *state.lock().unwrap_or_else(PoisonError::into_inner) = s;
    };
    let mut first = Some(first);
    let mut restarts: Vec<Instant> = vec![];
    loop {
        set(State::Starting);
        // (a start can be told to restart by a task it started, or unwind with `Restarting` itself)
        let start = AssertUnwindSafe(Node::open(options.clone())).catch_unwind();
        let why = tokio::select! {
            started = start => match started {
                Ok(Ok(node)) => {
                    *router.lock().unwrap_or_else(PoisonError::into_inner) = Some(node.router());
                    set(State::Running);
                    if let Some(f) = first.take() {
                        let _ = f.send(Ok(()));
                    }
                    let since = Instant::now();
                    match events.recv().await {
                        Some(Event::Restart(why)) => {
                            router.lock().unwrap_or_else(PoisonError::into_inner).take(); // (no request reaches a node being halted)
                            halt(Some(node));
                            if since.elapsed() >= Duration::from_secs(60) {
                                restarts.clear(); // (a node that served a minute starts the count again)
                            }
                            why
                        }
                        Some(Event::Failed(why)) => {
                            set(State::Failed(why.clone())); // (said at once: the stop may wait on the bucket)
                            if tokio::time::timeout(Duration::from_secs(30), node.stop()).await.is_err() {
                                halt(None);
                            }
                            return failed_until_closed(&set, why, &mut events, first.take()).await;
                        }
                        Some(Event::Close) | None => {
                            crate::drain::drain().await; // (its requests in flight answered: the router stays till then)
                            router.lock().unwrap_or_else(PoisonError::into_inner).take();
                            node.stop().await;
                            set(State::Closed);
                            return;
                        }
                    }
                }
                Ok(Err(e)) if e.downcast_ref::<crate::format::Newer>().is_some() => {
                    return failed_until_closed(&set, format!("{e:#}"), &mut events, first.take()).await; // (no try will open it)
                }
                Ok(Err(e)) => format!("{e:#}"), // (`Node::open` gave back what it took)
                Err(p) => {
                    halt(None);
                    match p.downcast::<Restarting>() {
                        Ok(r) => r.0,
                        Err(p) => return failed_until_closed(&set, crate::panics::said(&*p), &mut events, first.take()).await,
                    }
                }
            },
            e = events.recv() => match e {
                Some(Event::Restart(why)) => {
                    halt(None); // (the start's future is dropped: what it took goes with its statics)
                    why
                }
                Some(Event::Failed(why)) => {
                    halt(None);
                    return failed_until_closed(&set, why, &mut events, first.take()).await;
                }
                Some(Event::Close) | None => {
                    halt(None);
                    set(State::Closed);
                    return;
                }
            },
        };
        while let Ok(e) = events.try_recv() {
            // (the same trouble said by several tasks at once: one restart; a close still closes)
            if matches!(e, Event::Close) {
                set(State::Closed);
                return;
            }
        }
        restarts.retain(|t| t.elapsed() < Duration::from_secs(60));
        let Some(pause) = PAUSES.get(restarts.len()) else {
            return failed_until_closed(&set, format!("restarted {} times within a minute; the last time: {why}", PAUSES.len()), &mut events, first.take()).await;
        };
        restarts.push(Instant::now());
        eprintln!("restarting the node in place: {why}");
        set(State::Restarting(why));
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs_f64(*pause)) => {}
            e = events.recv() => if matches!(e, Some(Event::Close) | None) {
                set(State::Closed);
                return;
            },
        }
    }
}

/// Failed: said to whoever opened it, then kept until it is closed.
async fn failed_until_closed(set: &impl Fn(State), why: String, events: &mut mpsc::UnboundedReceiver<Event>, first: Option<std::sync::mpsc::Sender<Result<(), String>>>) {
    set(State::Failed(why.clone()));
    if let Some(f) = first {
        let _ = f.send(Err(why));
    }
    while let Some(e) = events.recv().await {
        if matches!(e, Event::Close) {
            break;
        }
    }
    set(State::Closed);
}
