//! The lake's format (ADR-039): what a binary must understand to read and write a lake.
//!
//! A lake's format is a number in its catalog (`format`: the number and the release that set it).
//! A lake without one is format 0: every lake made before ADR-039, which this build opens as it is.
//! A binary knows the formats up to its `FORMAT` and opens nothing newer (`check`): a lake a newer
//! Pondra wrote is refused, by name, before anything is read or written there, so an older binary
//! started by mistake (a rollback, a machine left behind) never reads one wrongly.
//!
//! The leader moves the lake to its `FORMAT` only once every node it hears from knows that format
//! (`raise`): each node says its release and format on every call to another (`headers`), and the
//! leader keeps what its followers' heartbeats and commit streams said. In a rolling upgrade the
//! format moves after the last older node has gone, never before; whatever this build writes that
//! an older one would read wrongly waits for that format (none yet: format 1 is the mark itself).
use crate::store::{Catalog, Lake};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

/// The newest format this build reads and writes.
pub const FORMAT: u32 = 1;

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

/// Refuse a lake newer than this build; its format otherwise.
pub async fn check(cat: &Catalog, url: &str) -> Result<u32> {
    let s = of(cat).await?;
    anyhow::ensure!(s.format <= known(), Newer { url: url.into(), stamp: (s.format, s.by) });
    Ok(s.format)
}

/// The newest format a node knows, from what it said (`headers`; a node that says nothing is from
/// before formats: 0).
pub fn said(headers: &axum::http::HeaderMap) -> (String, u32) {
    let get = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
    (get("x-pondra-version").unwrap_or_default(), get("x-pondra-format").and_then(|f| f.parse().ok()).unwrap_or(0))
}

/// Commit streams open to nodes that don't know this build's format (read-only nodes don't
/// heartbeat: their stream is how the leader knows they are there).
static OLDER_STREAMS: AtomicUsize = AtomicUsize::new(0);

/// Held by a commit stream to an older node for as long as it is open.
pub struct Older;

impl Older {
    pub fn of(format: u32) -> Option<Older> {
        (format < known()).then(|| {
            OLDER_STREAMS.fetch_add(1, Relaxed);
            Older
        })
    }
}

impl Drop for Older {
    fn drop(&mut self) { OLDER_STREAMS.fetch_sub(1, Relaxed); }
}

/// Leader: move the lake to `FORMAT` once every node knows it. It first waits out two leases, so
/// that every follower alive has said what it knows, then looks every 10 s. A migration, when a
/// format needs one, goes here, in the commit that sets the format.
pub fn raise(lake: Arc<Lake>, cluster: Arc<crate::cluster::Cluster>) {
    crate::panics::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        loop {
            match of(&lake.cat).await {
                Ok(s) if s.format >= known() => return,
                Ok(_) if cluster.everyone_knows(known()) && OLDER_STREAMS.load(Relaxed) == 0 => {
                    let stamp = Stamp { format: known(), by: VERSION.into() };
                    match lake.cat.commit(vec![(KEY.into(), serde_json::to_vec(&stamp).expect("json"))], &[]).await {
                        Ok(()) => return eprintln!("the lake is format {} now (Pondra {VERSION})", known()),
                        Err(e) => eprintln!("the lake's format: {e:#}"),
                    }
                }
                Ok(_) => {} // (an older node still runs: wait for it to go)
                Err(e) => eprintln!("the lake's format: {e:#}"),
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}

/// A node that isn't the leader: stop if the lake moves past what this build knows (it can't,
/// unless this node was cut off while the others agreed, `raise`).
pub fn watch(lake: Arc<Lake>) {
    crate::panics::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if let Err(e) = check(&lake.cat, &lake.url).await {
                if e.downcast_ref::<Newer>().is_some() {
                    eprintln!("stopping: {e}");
                    std::process::exit(1);
                }
            }
        }
    });
}
