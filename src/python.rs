//! Python beside the node (ADR-027): a pool of warm workers, `python -m pondra.worker`, each
//! serving one request at a time over its standard input and output — a function's batch
//! (`apply`), a procedure's call (`call`) or a body to compile (`check`) — as length-prefixed
//! frames: a JSON head, then the parts it announces (Arrow IPC). Workers start when first needed
//! (functions' batches one per core at once, procedures `PONDRA_PROCEDURES`), keep what they
//! imported and compiled, stop after a minute idle, and are replaced when they die, run out of
//! time or grow past their memory. Python never runs inside the node: a crashing library can't
//! take it down, and the binary stays one file for glibc 2.17.
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

static EXE: OnceLock<Option<String>> = OnceLock::new();

/// The Python this node runs routines with (`--python`), set once at start.
pub fn init(exe: Option<String>) { let _ = EXE.set(exe); }

/// Does this node run Python (`--python`)?
pub fn runs() -> bool { exe().is_ok() }

/// Ready to run `what` (a routine's name and kind), or why not: no `--python`, or none found
/// with the pondra package (`--python auto`).
pub fn ready(what: &str) -> Result<()> { exe().map(|_| ()).with_context(|| format!("{what}, so this node needs Python")) }

/// The Python: `--python`'s, or with `--python auto` (the shell's) the first found with the
/// `pondra` package and pyarrow — `$PONDRA_PYTHON`, the one beside this binary (pip put it
/// there), `python3`, `python` — looked for once, when first needed.
fn exe() -> Result<&'static str> {
    static FOUND: OnceLock<Option<String>> = OnceLock::new();
    let given = EXE.get().and_then(|e| e.as_deref()).context("it was started without --python (start it with --python <python>, or --python auto)")?;
    if given != "auto" {
        return Ok(given);
    }
    FOUND.get_or_init(|| {
        let beside = std::env::current_exe().ok().and_then(|e| Some(e.parent()?.to_path_buf()));
        let near = ["python3", "python", "python.exe"].iter().filter_map(|p| Some(beside.as_ref()?.join(p).to_string_lossy().to_string()));
        let all: Vec<String> = std::env::var("PONDRA_PYTHON").ok().into_iter().chain(near).chain(["python3".to_string(), "python".to_string()]).collect();
        all.into_iter().find(|p| std::process::Command::new(p).args(["-c", "import pondra.worker, pyarrow"]).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).status().is_ok_and(|s| s.success()))
    }).as_deref().context("no Python with the pondra package and pyarrow was found (pip install pondra pyarrow; or --python <python>)")
}

fn env_u64(var: &str, default: u64) -> u64 { std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default) }

struct Worker {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    idle_since: Instant,
}

/// What a request is for, which decides the slot it waits for. Functions' batches share one per
/// core (a query may run one over millions of rows on every node); procedures have their own
/// (`PONDRA_PROCEDURES`, four per core by default), and a procedure's own calls none: it already
/// holds one, and waiting for another could wait forever.
#[derive(Clone, Copy, PartialEq)]
pub enum Use {
    Function,
    Procedure { nested: bool },
}

/// The workers of one set of packages (`WITH (packages = …)`; most routines: none).
struct Pool {
    path: Option<String>, // where its packages are (PYTHONPATH), if any
    idle: Mutex<Vec<Worker>>,
    functions: Arc<Semaphore>,
    procedures: Arc<Semaphore>,
    busy: AtomicUsize,
    reaping: AtomicBool,
}

static POOLS: LazyLock<Mutex<HashMap<String, Arc<Pool>>>> = LazyLock::new(Default::default);

fn cores() -> usize { std::thread::available_parallelism().map_or(2, |n| n.get()) }

/// The pool for these packages, installed on this node the first time they are asked for.
async fn pool(packages: &str) -> Result<Arc<Pool>> {
    let key = packages.split(',').map(str::trim).filter(|p| !p.is_empty()).collect::<Vec<_>>().join(",");
    if let Some(p) = POOLS.lock().unwrap().get(&key) {
        return Ok(p.clone());
    }
    let path = match key.is_empty() {
        true => None,
        false => Some(install(&key).await?),
    };
    let procedures = env_u64("PONDRA_PROCEDURES", 4 * cores() as u64).max(1) as usize;
    let fresh = Arc::new(Pool { path, idle: Mutex::new(vec![]), functions: Arc::new(Semaphore::new(cores())), procedures: Arc::new(Semaphore::new(procedures)), busy: AtomicUsize::new(0), reaping: AtomicBool::new(false) });
    Ok(POOLS.lock().unwrap().entry(key).or_insert(fresh).clone())
}

/// A routine's packages, into a folder of this node's keyed by the list (uv when it is there,
/// else pip), once: `--target`, so the `--python` environment itself never changes.
async fn install(packages: &str) -> Result<String> {
    static BUSY: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(Default::default);
    let _one = BUSY.lock().await;
    let dir = std::env::temp_dir().join("pondra-python").join(format!("{:016x}", fnv(packages)));
    let done = dir.join(".installed");
    if !done.exists() {
        std::fs::create_dir_all(&dir)?;
        let list: Vec<&str> = packages.split(',').collect();
        let target = dir.to_string_lossy().to_string();
        let out = match which("uv") {
            Some(uv) => tokio::process::Command::new(uv).args(["pip", "install", "--quiet", "--python", exe()?, "--target", &target]).args(&list).output().await?,
            None => tokio::process::Command::new(exe()?).args(["-m", "pip", "install", "--quiet", "--disable-pip-version-check", "--target", &target]).args(&list).output().await?,
        };
        ensure!(out.status.success(), "installing {packages}: {}", String::from_utf8_lossy(&out.stderr).trim());
        std::fs::write(&done, packages)?;
    }
    Ok(dir.to_string_lossy().to_string())
}

fn which(name: &str) -> Option<std::path::PathBuf> {
    std::env::var_os("PATH").and_then(|paths| std::env::split_paths(&paths).map(|p| p.join(name)).find(|p| p.is_file()))
}

/// A short, stable name for a text (FNV-1a).
fn fnv(s: &str) -> u64 { s.bytes().fold(0xcbf29ce484222325u64, |h, b| (h ^ b as u64).wrapping_mul(0x100000001b3)) }

/// A worker lent out, and the slot it holds; back to its pool (or stopped) when done.
struct Lent {
    pool: Arc<Pool>,
    worker: Option<Worker>,
    _slot: Option<OwnedSemaphorePermit>,
}

impl Drop for Lent {
    fn drop(&mut self) {
        self.pool.busy.fetch_sub(1, Ordering::SeqCst); // (a worker not given back is dropped: killed)
    }
}

impl Pool {
    /// An idle worker (one still running), or a new one, once a slot is free.
    async fn take(self: &Arc<Self>, kind: Use) -> Result<Lent> {
        let slot = match kind {
            Use::Function => Some(self.functions.clone().acquire_owned().await?),
            Use::Procedure { nested: false } => Some(self.procedures.clone().acquire_owned().await?),
            Use::Procedure { nested: true } => None,
        };
        self.busy.fetch_add(1, Ordering::SeqCst); // (before the reaper can look: it stops only when none is busy)
        let mut lent = Lent { pool: self.clone(), worker: None, _slot: slot };
        while let Some(mut w) = self.idle.lock().unwrap().pop() {
            if w.child.try_wait().ok().flatten().is_none() {
                lent.worker = Some(w);
                return Ok(lent);
            }
        }
        lent.worker = Some(spawn(self.path.as_deref())?);
        let _idle = self.idle.lock().unwrap(); // (the reaper decides to stop under this lock)
        if !self.reaping.swap(true, Ordering::SeqCst) {
            tokio::spawn(reap(self.clone()));
        }
        Ok(lent)
    }
}

/// A new worker, its packages (if any) on its path.
fn spawn(path: Option<&str>) -> Result<Worker> {
    let mut cmd = tokio::process::Command::new(exe()?);
    cmd.args(["-m", "pondra.worker"]).stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).kill_on_drop(true);
    if let Some(p) = path {
        let old = std::env::var("PYTHONPATH").unwrap_or_default();
        cmd.env("PYTHONPATH", if old.is_empty() { p.to_string() } else { format!("{p}{}{old}", if cfg!(windows) { ";" } else { ":" }) });
    }
    let mut child = cmd.spawn().with_context(|| format!("couldn't start {} -m pondra.worker", exe().unwrap_or("python")))?;
    let (input, output) = (child.stdin.take().expect("piped"), BufReader::new(child.stdout.take().expect("piped")));
    Ok(Worker { child, input, output, idle_since: Instant::now() })
}

impl Lent {
    /// The worker back, unless it has grown past its memory (it is then stopped: a new one comes).
    fn give(mut self) {
        let Some(mut w) = self.worker.take() else { return };
        if resident_mb(&w.child).is_some_and(|mb| mb > env_u64("PONDRA_WORKER_MB", 2048)) {
            return; // (dropped: killed)
        }
        w.idle_since = Instant::now();
        self.pool.idle.lock().unwrap().push(w);
    }
}

/// Stop the workers idle for a minute (`PONDRA_WORKER_IDLE_SECS`); done when none is left.
async fn reap(pool: Arc<Pool>) {
    let idle = Duration::from_secs(env_u64("PONDRA_WORKER_IDLE_SECS", 60));
    loop {
        tokio::time::sleep(Duration::from_secs(2).min(idle)).await;
        let mut ws = pool.idle.lock().unwrap();
        ws.retain(|w| w.idle_since.elapsed() < idle);
        if ws.is_empty() && pool.busy.load(Ordering::SeqCst) == 0 {
            pool.reaping.store(false, Ordering::SeqCst);
            return;
        }
    }
}

/// The workers running now on this node (idle or busy, sessions' too), for `/stats` and the tests.
pub fn workers() -> usize {
    let pooled: usize = POOLS.lock().unwrap().values().map(|p| p.idle.lock().unwrap().len() + p.busy.load(Ordering::SeqCst)).sum();
    pooled + KERNELS.lock().unwrap().values().filter(|k| k.0.try_lock().map_or(true, |k| k.is_some())).count()
}

/// A worker's resident memory in MB (Linux; elsewhere unknown).
fn resident_mb(child: &Child) -> Option<u64> {
    let statm = std::fs::read_to_string(format!("/proc/{}/statm", child.id()?)).ok()?;
    Some(statm.split_whitespace().nth(1)?.parse::<u64>().ok()? * 4096 >> 20)
}

async fn put(w: &mut Worker, bytes: &[u8]) -> Result<()> {
    w.input.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
    Ok(w.input.write_all(bytes).await?)
}

async fn get(w: &mut Worker) -> Result<Vec<u8>> {
    let mut n = [0u8; 4];
    w.output.read_exact(&mut n).await.context("the worker stopped")?;
    let mut buf = vec![0u8; u32::from_le_bytes(n) as usize];
    w.output.read_exact(&mut buf).await.context("the worker stopped")?;
    Ok(buf)
}

/// One message: its head, then its parts.
async fn send(w: &mut Worker, head: &Value, parts: &[Vec<u8>]) -> Result<()> {
    put(w, &serde_json::to_vec(&json!({"parts": parts.len(), "head": head}))?).await?;
    for p in parts {
        put(w, p).await?;
    }
    Ok(w.input.flush().await?)
}

async fn recv(w: &mut Worker) -> Result<(Value, Vec<Vec<u8>>)> {
    let head: Value = serde_json::from_slice(&get(w).await?)?;
    let mut parts = vec![];
    for _ in 0..head["parts"].as_u64().unwrap_or(0) {
        parts.push(get(w).await?);
    }
    Ok((head["head"].clone(), parts))
}

/// A request to a worker of `packages`' pool, and its answer. Notices it sends first (a
/// procedure's prints) go to `notice`. Past `limit` (if any) the worker is stopped, as it is if it
/// dies; the request then fails with the reason, and the next one gets a new worker.
pub async fn ask(packages: &str, kind: Use, head: Value, parts: Vec<Vec<u8>>, limit: Option<Duration>, notice: &mut (dyn FnMut(String) + Send)) -> Result<(Value, Vec<Vec<u8>>)> {
    let mut lent = pool(packages).await?.take(kind).await?;
    let w = lent.worker.as_mut().expect("a worker");
    let exchange = async {
        send(w, &head, &parts).await?;
        loop {
            let (h, parts) = recv(w).await?;
            match h.get("notice").and_then(Value::as_str) {
                Some(n) => notice(n.to_string()),
                None => return anyhow::Ok((h, parts)),
            }
        }
    };
    let answer = match limit {
        Some(l) => match tokio::time::timeout(l, exchange).await {
            Ok(a) => a,
            Err(_) => bail!("it took longer than {} s, its limit (the worker was stopped)", l.as_secs_f64()), // (not given back: killed)
        },
        None => exchange.await,
    };
    match answer {
        Ok(a) => {
            lent.give();
            match a.0.get("error").and_then(Value::as_str) {
                Some(e) => bail!("{e}"),
                None => Ok(a),
            }
        }
        Err(e) => {
            // A worker that dies closes its pipes a moment before the OS reports it gone: wait for
            // that (a second at most), so the reason is always said.
            let status = tokio::time::timeout(Duration::from_secs(1), w.child.wait()).await.ok().and_then(Result::ok);
            Err(match status {
                Some(s) => e.context(format!("the Python worker ended ({s})")),
                None => e,
            })
        }
    }
}

// ---------------------------------------------------------------- a session's own (ADR-032)

/// A session's Python: its cells — the console's, and DO blocks a client sends in a session — run
/// on one worker kept for it, in one namespace, so each sees what the cells before it made, as a
/// notebook's kernel does. One cell runs at a time. It ends with its session (`temp::end`), after
/// `PONDRA_SESSION_IDLE_SECS` idle (3600), or past its memory (`PONDRA_WORKER_MB`); the next cell
/// then starts a new one, empty, and says so.
struct Kernel {
    worker: Worker,
}

type Slot = Arc<tokio::sync::Mutex<Option<Kernel>>>;
static KERNELS: LazyLock<Mutex<HashMap<String, (Slot, Instant)>>> = LazyLock::new(Default::default);

/// A cell of `session`'s, on its worker (made now if it has none), and its answer.
pub async fn ask_session(session: &str, head: Value, parts: Vec<Vec<u8>>, limit: Option<Duration>, notice: &mut (dyn FnMut(String) + Send)) -> Result<(Value, Vec<Vec<u8>>)> {
    let slot = {
        let mut all = KERNELS.lock().unwrap();
        if all.is_empty() {
            tokio::spawn(reap_kernels()); // (only while there are some)
        }
        let e = all.entry(session.to_string()).or_insert_with(|| (Slot::default(), Instant::now()));
        e.1 = Instant::now();
        e.0.clone()
    };
    let mut kernel = slot.lock().await; // (a session's cells, one after another)
    let _slot = pool("").await?.procedures.clone().acquire_owned().await?; // (as a procedure: the node's slots for them)
    if kernel.as_mut().is_some_and(|k| k.worker.child.try_wait().ok().flatten().is_some()) {
        *kernel = None;
        notice("Python started again: this session's worker had stopped, and the variables it held are gone".into());
    }
    if kernel.is_none() {
        *kernel = Some(Kernel { worker: spawn(None)? });
    }
    let w = &mut kernel.as_mut().expect("a kernel").worker;
    let exchange = async {
        send(w, &head, &parts).await?;
        loop {
            let (h, parts) = recv(w).await?;
            match h.get("notice").and_then(Value::as_str) {
                Some(n) => notice(n.to_string()),
                None => return anyhow::Ok((h, parts)),
            }
        }
    };
    let answer = match limit {
        Some(l) => tokio::time::timeout(l, exchange).await.unwrap_or_else(|_| Err(anyhow::anyhow!("it took longer than {} s, its limit", l.as_secs_f64()))),
        None => exchange.await,
    };
    let answer = match answer {
        Ok(a) => a,
        Err(e) => {
            *kernel = None; // (stopped: its variables with it)
            return Err(e.context("this session's Python was stopped, and the variables it held are gone"));
        }
    };
    if resident_mb(&kernel.as_ref().expect("a kernel").worker.child).is_some_and(|mb| mb > env_u64("PONDRA_WORKER_MB", 2048)) {
        *kernel = None;
        notice(format!("this session's Python grew past {} MB and was stopped: the next cell starts with no variables", env_u64("PONDRA_WORKER_MB", 2048)));
    }
    KERNELS.lock().unwrap().entry(session.to_string()).and_modify(|e| e.1 = Instant::now());
    match answer.0.get("error").and_then(Value::as_str) {
        Some(e) => bail!("{e}"),
        None => Ok(answer),
    }
}

/// A session ended: its worker stops (once a cell it runs is done).
pub fn end_session(session: &str) -> bool { KERNELS.lock().unwrap().remove(session).is_some() }

/// Sessions' workers idle for `PONDRA_SESSION_IDLE_SECS` stop; once there are none, this stops.
async fn reap_kernels() {
    let idle = Duration::from_secs(env_u64("PONDRA_SESSION_IDLE_SECS", 3600));
    loop {
        tokio::time::sleep(idle.min(Duration::from_secs(30))).await;
        let mut all = KERNELS.lock().unwrap();
        all.retain(|_, (k, used)| used.elapsed() < idle || k.try_lock().is_err()); // (one running a cell stays)
        if all.is_empty() {
            return;
        }
    }
}
