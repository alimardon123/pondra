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
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

static EXE: OnceLock<Option<String>> = OnceLock::new();

/// The Python this node runs routines with (`--python`), set once at start.
pub fn init(exe: Option<String>) { let _ = EXE.set(exe); }

/// Does this node run Python (`--python`)?
pub fn runs() -> bool { EXE.get().is_some_and(|e| e.is_some()) }

/// Started with Python, and (with `--python auto`) one was found: a routine made on a node with
/// none is checked when it is first used instead, where the error says why.
pub async fn found() -> bool { runs() && exe().await.is_ok() }

/// Ready to run `what` (a routine's name and kind), or why not: no `--python`.
pub fn ready(what: &str) -> Result<()> {
    ensure!(runs(), "{what}, so this node needs Python: it was started without --python (start it with --python <python>, or --python auto)");
    Ok(())
}

/// The Python chosen now: `--python`'s, one chosen in the console since (`choose`), or, with
/// `--python auto` (the shell's), the first that works of those this machine has (`pythons`).
static CHOSEN: std::sync::RwLock<Option<String>> = std::sync::RwLock::new(None);

/// The Python to run workers with, looked for the first time it is needed.
async fn exe() -> Result<String> {
    let given = EXE.get().and_then(|e| e.as_deref()).context("it was started without --python (start it with --python <python>, or --python auto)")?;
    if let Some(p) = CHOSEN.read().unwrap().clone() {
        return Ok(p);
    }
    if given != "auto" {
        return Ok(given.to_string());
    }
    static LOOKING: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(Default::default);
    let _one = LOOKING.lock().await;
    if let Some(p) = CHOSEN.read().unwrap().clone() {
        return Ok(p);
    }
    let found = pythons().await;
    let good = found.iter().find(|p| p["ok"] == true).and_then(|p| p["path"].as_str()).map(str::to_string);
    let Some(good) = good else {
        let tried: Vec<String> = found.iter().map(|p| format!("{} ({})", p["path"].as_str().unwrap_or("?"), p["error"].as_str().unwrap_or("no pondra or pyarrow"))).collect();
        bail!("no Python with the pondra package and pyarrow was found (tried: {}). Install them into one: <python> -m pip install pondra pyarrow; or start with --python <python>, or choose one in the console", if tried.is_empty() { "none found".into() } else { tried.join("; ") });
    };
    *CHOSEN.write().unwrap() = Some(good.clone());
    Ok(good)
}

/// Where the console's choice of Python is kept on this machine (beside its settings).
fn kept_choice() -> Option<std::path::PathBuf> { crate::console::settings_dir().map(|d| d.join("python.txt")) }

/// The Pythons this machine has, the likeliest first: `$PONDRA_PYTHON`, the one chosen before, the
/// one beside this binary (pip put it there, a virtualenv's), those on the PATH, Windows' `py`
/// launcher's, and Anaconda's and Miniconda's (their environments too).
async fn candidates() -> Vec<String> {
    let mut out: Vec<std::path::PathBuf> = vec![];
    let var = |k: &str| std::env::var_os(k).filter(|v| !v.is_empty()).map(std::path::PathBuf::from);
    out.extend(var("PONDRA_PYTHON"));
    out.extend(kept_choice().and_then(|f| std::fs::read_to_string(f).ok()).map(|p| p.trim().into()));
    let names: &[&str] = if cfg!(windows) { &["python.exe", "python3.exe"] } else { &["python3", "python"] };
    let bin = if cfg!(windows) { "" } else { "bin" };
    if let Some(dir) = std::env::current_exe().ok().and_then(|e| Some(e.parent()?.to_path_buf())) {
        out.extend(names.iter().map(|n| dir.join(n)));
        out.extend(names.iter().filter_map(|n| Some(dir.parent()?.join(n)))); // (a Windows install's Scripts\ is beside its python.exe)
    }
    for dir in var("PATH").map(|p| std::env::split_paths(&p).collect::<Vec<_>>()).unwrap_or_default() {
        out.extend(names.iter().map(|n| dir.join(n)));
    }
    if cfg!(windows) { // (the py launcher knows every python.org install)
        let listed = tokio::time::timeout(Duration::from_secs(5), tokio::process::Command::new("py").arg("-0p").stdin(std::process::Stdio::null()).kill_on_drop(true).output()).await;
        if let Ok(Ok(o)) = listed {
            out.extend(String::from_utf8_lossy(&o.stdout).lines().filter_map(|l| l.split_whitespace().last()).filter(|p| p.to_lowercase().ends_with(".exe")).map(std::path::PathBuf::from));
        }
    }
    let home = var("USERPROFILE").or_else(|| var("HOME"));
    let conda: Vec<std::path::PathBuf> = var("CONDA_PREFIX").into_iter()
        .chain(["anaconda3", "miniconda3", "Anaconda3", "Miniconda3"].iter().filter_map(|d| Some(home.as_ref()?.join(d))))
        .chain(var("ProgramData").map(|p| p.join("Anaconda3"))).collect();
    for base in conda {
        out.push(base.join(bin).join(names[0]));
        if let Ok(envs) = std::fs::read_dir(base.join("envs")) {
            out.extend(envs.flatten().map(|e| e.path().join(bin).join(names[0])));
        }
    }
    let mut seen = std::collections::HashSet::new();
    out.into_iter().filter(|p| p.is_file()).filter(|p| seen.insert(std::fs::canonicalize(p).unwrap_or_else(|_| p.clone()))).map(|p| p.to_string_lossy().to_string()).take(24).collect()
}

/// Each Python this machine has, tried at once, each for at most `PONDRA_PYTHON_PROBE_SECS` (15):
/// its version, whether it has the pondra package and pyarrow, and whether they import — one that
/// hangs (a broken install) is one that doesn't work, never a wait for everyone.
pub async fn pythons() -> Vec<Value> { probe(candidates().await).await }

async fn probe(paths: Vec<String>) -> Vec<Value> {
    const TRY: &str = "import sys, json, importlib.util as u\nr = {'version': sys.version.split()[0], 'pondra': bool(u.find_spec('pondra')), 'pyarrow': bool(u.find_spec('pyarrow'))}\ntry:\n    import pyarrow, pondra, pondra.worker\n    r['ok'] = True; r['pondra_version'] = getattr(pondra, '__version__', '')\nexcept BaseException as e:\n    r['ok'] = False; r['error'] = (type(e).__name__ + ': ' + str(e))[:300]\nprint(json.dumps(r))";
    let limit = Duration::from_secs(env_u64("PONDRA_PYTHON_PROBE_SECS", 15));
    let probes = paths.into_iter().map(|path| async move {
        let run = tokio::process::Command::new(&path).args(["-c", TRY]).stdin(std::process::Stdio::null()).stderr(std::process::Stdio::null()).kill_on_drop(true).output();
        let mut v = match tokio::time::timeout(limit, run).await {
            Ok(Ok(o)) => String::from_utf8_lossy(&o.stdout).lines().last().and_then(|l| serde_json::from_str::<Value>(l).ok()).unwrap_or_else(|| json!({"ok": false, "error": format!("it didn't start (exit {})", o.status)})),
            Ok(Err(e)) => json!({"ok": false, "error": format!("it didn't start: {e}")}),
            Err(_) => json!({"ok": false, "error": format!("it didn't answer within {} s: a broken install?", limit.as_secs())}),
        };
        v["path"] = json!(path);
        v
    });
    futures::future::join_all(probes).await
}

/// The Python in use, and how it came to be: for the console's Python menu.
pub fn current() -> Value {
    let given = EXE.get().and_then(|e| e.clone());
    json!({"python": CHOSEN.read().unwrap().clone().or_else(|| given.clone().filter(|g| g != "auto")), "given": given, "worker": PYTHON.get()})
}

/// Use `path` from now on (the console's Choose Python): it must work (pondra and pyarrow import);
/// kept on this machine for the next start; workers and sessions start again on it.
pub async fn choose(path: &str) -> Result<Value> {
    ensure!(runs(), "this node was started without --python");
    let path = path.trim().trim_matches('"');
    ensure!(std::path::Path::new(path).is_file(), "{path}: no such Python");
    let probe = probe(vec![path.to_string()]).await.pop().context("no answer")?;
    ensure!(probe["ok"] == true, "{path} can't run Pondra's Python: {}", probe["error"].as_str().unwrap_or("it lacks the pondra package or pyarrow (<python> -m pip install pondra pyarrow)"));
    *CHOSEN.write().unwrap() = Some(path.to_string());
    if let Some(f) = kept_choice() {
        let _ = f.parent().map(std::fs::create_dir_all);
        let _ = std::fs::write(f, path);
    }
    POOLS.lock().unwrap().clear(); // (idle workers stop; busy ones finish first)
    for s in KERNELS.lock().unwrap().drain().map(|(s, _)| s).collect::<Vec<_>>() {
        PIDS.lock().unwrap().remove(&s);
    }
    Ok(probe)
}

fn env_u64(var: &str, default: u64) -> u64 { std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default) }

struct Worker {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    idle_since: Instant,
    said: Arc<Mutex<VecDeque<String>>>, // its standard error's last lines: why, if it stops
}

impl Worker {
    /// What its standard error said last (a traceback, a missing package), for an error.
    async fn tail(&self) -> String {
        tokio::time::sleep(Duration::from_millis(100)).await; // (its last lines, read by now)
        let said = self.said.lock().unwrap();
        match said.is_empty() {
            true => String::new(),
            false => format!("; it said: {}", said.iter().cloned().collect::<Vec<_>>().join(" / ")),
        }
    }
}

/// The Python the workers run, as the first one said it (`/sessions/{id}/python` shows it).
static PYTHON: OnceLock<Value> = OnceLock::new();

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
            Some(uv) => tokio::process::Command::new(uv).args(["pip", "install", "--quiet", "--python", &exe().await?, "--target", &target]).args(&list).output().await?,
            None => tokio::process::Command::new(exe().await?).args(["-m", "pip", "install", "--quiet", "--disable-pip-version-check", "--target", &target]).args(&list).output().await?,
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
        lent.worker = Some(spawn(self.path.as_deref()).await?);
        let _idle = self.idle.lock().unwrap(); // (the reaper decides to stop under this lock)
        if !self.reaping.swap(true, Ordering::SeqCst) {
            tokio::spawn(reap(self.clone()));
        }
        Ok(lent)
    }
}

/// A new worker, its packages (if any) on its path, once it has answered: a Python that doesn't
/// start (a missing package, one that hangs importing) is an error that says so, within
/// `PONDRA_WORKER_START_SECS` (60), not a cell that waits forever. What it writes to its standard
/// error goes to the node's, its last lines kept for that error.
async fn spawn(path: Option<&str>) -> Result<Worker> {
    let exe = &exe().await?;
    let mut cmd = tokio::process::Command::new(exe);
    cmd.args(["-m", "pondra.worker"]).stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).kill_on_drop(true);
    if let Some(p) = path {
        let old = std::env::var("PYTHONPATH").unwrap_or_default();
        cmd.env("PYTHONPATH", if old.is_empty() { p.to_string() } else { format!("{p}{}{old}", if cfg!(windows) { ";" } else { ":" }) });
    }
    let mut child = cmd.spawn().with_context(|| format!("couldn't start {exe} -m pondra.worker"))?;
    let said = Arc::new(Mutex::new(VecDeque::new()));
    if let Some(err) = child.stderr.take() {
        let said = said.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(err).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("{line}");
                let mut s = said.lock().unwrap();
                if s.len() == 20 {
                    s.pop_front();
                }
                s.push_back(line);
            }
        });
    }
    let (input, output) = (child.stdin.take().expect("piped"), BufReader::new(child.stdout.take().expect("piped")));
    let mut w = Worker { child, input, output, idle_since: Instant::now(), said };
    let limit = Duration::from_secs(env_u64("PONDRA_WORKER_START_SECS", 60));
    let hello = async {
        send(&mut w, &json!({"op": "hello"}), &[]).await?;
        recv(&mut w).await // (an older worker answers an error: it is alive all the same)
    };
    match tokio::time::timeout(limit, hello).await {
        Ok(Ok((head, _))) => {
            if head.get("python").is_some() {
                let _ = PYTHON.set(head);
            }
            Ok(w)
        }
        Ok(Err(e)) => bail!("{exe} -m pondra.worker stopped as it started ({e:#}){}", w.tail().await),
        Err(_) => bail!("{exe} -m pondra.worker didn't answer within {} s of starting{}", limit.as_secs(), w.tail().await),
    }
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
    busy: bool, // a cell sent, its answer not read: its request went away (the page's Stop) before it came
}

/// Each session's worker's process id: an interrupt reaches it while a cell holds its slot.
static PIDS: LazyLock<Mutex<HashMap<String, u32>>> = LazyLock::new(Default::default);

/// Interrupt `session`'s running cell: its worker gets SIGINT, and the cell ends with
/// `KeyboardInterrupt`, its variables kept, as a notebook's kernel does. Windows has no such
/// signal for another process: the worker is stopped instead, its variables with it. What was done.
pub fn interrupt(session: &str) -> &'static str {
    let Some(pid) = PIDS.lock().unwrap().get(session).copied() else { return "none" };
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, libc::SIGINT) };
        "interrupted"
    }
    #[cfg(not(unix))]
    {
        let _ = std::process::Command::new("taskkill").args(["/F", "/T", "/PID", &pid.to_string()]).output();
        end_session(session);
        "restarted"
    }
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
    if let Some(k) = kernel.as_mut().filter(|k| k.busy) {
        // A cell whose request went away: its answer (an interrupted one's comes at once) is read
        // and left, so this cell's is this cell's; one still running after a few seconds is stopped.
        let drained = tokio::time::timeout(Duration::from_secs(3), async {
            while recv(&mut k.worker).await?.0.get("notice").is_some() {}
            anyhow::Ok(())
        });
        match drained.await {
            Ok(Ok(())) => k.busy = false,
            _ => {
                *kernel = None;
                notice("Python started again: the cell before was stopped while it ran, and the variables it held are gone".into());
            }
        }
    }
    if kernel.is_none() {
        let worker = spawn(None).await?;
        if let Some(pid) = worker.child.id() {
            PIDS.lock().unwrap().insert(session.to_string(), pid);
        }
        *kernel = Some(Kernel { worker, busy: false });
    }
    kernel.as_mut().expect("a kernel").busy = true;
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
            let said = kernel.as_ref().expect("a kernel").worker.tail().await;
            *kernel = None; // (stopped: its variables with it)
            return Err(e.context(format!("this session's Python was stopped, and the variables it held are gone{said}")));
        }
    };
    kernel.as_mut().expect("a kernel").busy = false;
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

/// The variables a session's worker holds: name, type, size and a short look at each, or none if
/// it has no worker yet; `busy` while a cell runs (nothing waits for it).
pub async fn variables(session: &str) -> Result<Value> {
    let Some(slot) = KERNELS.lock().unwrap().get(session).map(|k| k.0.clone()) else { return Ok(json!({"running": false, "variables": []})) };
    let Ok(mut kernel) = slot.try_lock() else { return Ok(json!({"running": true, "busy": true, "variables": []})) };
    let Some(k) = kernel.as_mut() else { return Ok(json!({"running": false, "variables": []})) };
    if k.busy {
        return Ok(json!({"running": true, "busy": true, "variables": [], "python": PYTHON.get()})); // (a cell left running: the next cell sees to it)
    }
    send(&mut k.worker, &json!({"op": "vars", "session": session}), &[]).await?;
    let (head, _) = recv(&mut k.worker).await?;
    Ok(json!({"running": true, "busy": false, "variables": head["variables"], "python": PYTHON.get()}))
}

/// A session ended: its worker stops (once a cell it runs is done).
pub fn end_session(session: &str) -> bool {
    PIDS.lock().unwrap().remove(session);
    KERNELS.lock().unwrap().remove(session).is_some()
}

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
