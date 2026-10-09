//! The lake's format (ADR-039): what a binary must understand to read and write a lake.
//!
//! A lake's format is a number in its catalog (`format`: the number and the release that set it).
//! A lake without one is format 0: every lake made before ADR-039, which this build opens as it is.
//! A binary knows the formats up to its `FORMAT` and opens nothing newer (`check`): a lake a newer
//! Pondra wrote is refused, by name, before anything is read or written there, so an older binary
//! started by mistake (a rollback, a machine left behind) never reads one wrongly.
//!
//! A lake moves past format 1 only when something written needs it (`require`), as a Delta table's
//! protocol moves only for a feature it uses: a lake this build opened but used nothing new in
//! still opens in the release before, so going back after an upgrade works for every lake that
//! didn't use what came with it. Even then the format moves only to what every node knows (`raise`
//! keeps it): each node says its release and format on every call to another (`headers`), and the
//! leader keeps what its followers' heartbeats and commit streams said. In a rolling upgrade
//! nothing that needs the new format is written until the last older node has gone; asked for
//! before that, it is refused by name.
use crate::store::{Catalog, Lake};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// The newest format this build reads and writes. 2: a materialized view that finishes its answers
/// as it is read (`TableMeta::finish`, ADR-055).
pub const FORMAT: u32 = 2;

/// The format every lake gets without asking: the mark itself (`raise`).
const BASE: u32 = 1;

/// The format every lake gets without asking: the mark itself (`raise`).
const BASE: u32 = 1;

/// `FORMAT`, unless a test says otherwise (`PONDRA_TEST_FORMAT`: a build of a later format).
pub fn known() -> u32 {
    static KNOWN: std::sync::LazyLock<u32> = std::sync::LazyLock::new(|| std::env::var("PONDRA_TEST_FORMAT").ok().and_then(|f| f.parse().ok()).unwrap_or(FORMAT));
    *KNOWN
}
/// This build's release.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The catalog key that holds it.
const KEY: &str = "format";
/// What a node says of itself on every call to another: its release, and the newest format it knows.
pub fn headers() -> [(&'static str, String); 2] { [("x-pondra-version", VERSION.into()), ("x-pondra-format", known().to_string())] }

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Stamp {
    pub format: u32,
    #[serde(default)]
    pub by: String, // the release that set it
}

/// The lake's format (0: made before there was one).
pub async fn of(cat: &Catalog) -> Result<Stamp> { Ok(cat.get::<Stamp>(KEY).await?.unwrap_or_default()) }

/// A lake written by a newer Pondra than this one.
#[derive(Debug)]
pub struct Newer {
    pub url: String,
    pub stamp: (u32, String),
}

impl std::fmt::Display for Newer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (n, by) = &self.stamp;
        write!(f, "{} is a lake of format {n}, written by Pondra {by}; this is Pondra {VERSION}, which knows formats up to {}: run Pondra {by} or newer here", self.url, known())
    }
}

impl std::error::Error for Newer {}

/// Did this process make its lake (open it to write with nothing ever committed there)?
static MADE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Refuse a lake newer than this build; its format otherwise.
pub async fn check(cat: &Catalog, url: &str, writer: bool) -> Result<u32> {
    let s = of(cat).await?;
    anyhow::ensure!(s.format <= known(), Newer { url: url.into(), stamp: (s.format, s.by) });
    if writer && cat.get::<u64>("c").await?.is_none() {
        MADE.store(true, std::sync::atomic::Ordering::Relaxed); // (every commit writes `c`)
    }
    Ok(s.format)
}

/// Commit the lake's format.
async fn set(lake: &Lake, to: u32) -> Result<()> {
    let stamp = Stamp { format: to, by: VERSION.into() };
    lake.cat.commit(vec![(KEY.into(), serde_json::to_vec(&stamp).expect("json"))], &[]).await?;
    eprintln!("the lake is format {to} now (Pondra {VERSION})");
    Ok(())
}

/// What `raise` saw: 0 when it doesn't run in this process, 1 while it takes its first look, then
/// the newest format every node knows, plus 2.
static READY: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Leader: before writing something an older release would read wrongly (`what`), move the lake to
/// format `n`, or refuse by name while a node that doesn't know `n` runs. A leader that only just
/// took over looks first (`raise`, 10 s at most); a process that leads without `raise` (a `pondra
/// sql` leading for a moment: nobody else is there) moves it as far as this build knows.
pub async fn require(lake: &Lake, n: u32, what: &str) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    if of(&lake.cat).await?.format >= n {
        return Ok(());
    }
    let started = std::time::Instant::now();
    while READY.load(Relaxed) == 1 && started.elapsed() < Duration::from_secs(15) {
        tokio::time::sleep(Duration::from_millis(100)).await; // (the first look: `raise`)
    }
    let ready = match READY.load(Relaxed) {
        0 => known(),
        1 => 0, // (no look in 15 s: refused, never guessed)
        r => r - 2,
    };
    anyhow::ensure!(
        n <= ready,
        "{what} needs the lake at format {n}, and a node of this cluster runs a Pondra that knows only format {ready}: once every node runs Pondra {VERSION} or newer, ask again (the lake stays as it is, so the release before still opens it)"
    );
    set(lake, n).await
}

/// The newest format a node knows, from what it said (`headers`; a node that says nothing is from
/// before formats: 0).
pub fn said(headers: &axum::http::HeaderMap) -> (String, u32) {
    let get = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
    (get("x-pondra-version").unwrap_or_default(), get("x-pondra-format").and_then(|f| f.parse().ok()).unwrap_or(0))
}

/// Commit streams open to other nodes, by the format each knows (read-only nodes don't
/// heartbeat: their stream is how the leader knows they are there).
static STREAMS: std::sync::Mutex<std::collections::BTreeMap<u32, usize>> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// Held by a commit stream for as long as it is open.
pub struct Streamed(u32);

impl Streamed {
    pub fn to(format: u32) -> Streamed {
        *STREAMS.lock().unwrap().entry(format).or_default() += 1;
        Streamed(format)
    }
}

impl Drop for Streamed {
    fn drop(&mut self) {
        let mut s = STREAMS.lock().unwrap();
        if let Some(n) = s.get_mut(&self.0) {
            *n -= 1;
            if *n == 0 {
                s.remove(&self.0);
            }
        }
    }
}

/// Leader: keep what every node knows for `require`, and give a lake without a format the mark
/// (`BASE`) once every node knows it. It first waits out two leases, so that every follower alive
/// has said what it knows, then looks every 10 s. A lake this process made has the mark at once,
/// and every node knows this build's format until one says otherwise: no node older than it has
/// read it. A migration, when a format needs one, goes in the commit that sets it.
pub fn raise(lake: Arc<Lake>, cluster: Arc<crate::cluster::Cluster>) {
    use std::sync::atomic::Ordering::Relaxed;
    crate::panics::spawn(async move {
        let mut marked = false;
        if MADE.load(Relaxed) {
            READY.store(known() + 2, Relaxed);
            marked = set(&lake, BASE).await.map_err(|e| eprintln!("the lake's format: {e:#}")).is_ok();
        } else {
            READY.store(1, Relaxed); // (looking: `require` waits for it)
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        loop {
            let streamed = STREAMS.lock().unwrap().keys().next().copied();
            let ready = [Some(known()), cluster.least_known(), streamed].into_iter().flatten().min().unwrap_or_default();
            READY.store(ready + 2, Relaxed);
            if !marked && ready >= BASE {
                let done = match of(&lake.cat).await {
                    Ok(s) if s.format >= BASE => Ok(()),
                    Ok(_) => set(&lake, BASE).await,
                    Err(e) => Err(e),
                };
                marked = done.map_err(|e| eprintln!("the lake's format: {e:#}")).is_ok();
            }
            if known() > FORMAT {
                // (a build posing as a later one, `PONDRA_TEST_FORMAT`: it writes what needs it)
                let _ = require(&lake, known(), "this test build").await;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}

/// A node that isn't the leader: stop if the lake moves past what this build knows. (It can't,
/// unless this node was cut off while the others agreed, `raise`, or it started on such a lake
/// before its catalog view held the format: it holds it once caught up, invariant 197.)
pub fn watch(lake: Arc<Lake>) {
    crate::panics::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Err(e) = check(&lake.cat, &lake.url, false).await {
                if e.downcast_ref::<Newer>().is_some() {
                    eprintln!("stopping: {e}");
                    std::process::exit(1);
                }
            }
        }
    });
}
