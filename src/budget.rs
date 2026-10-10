//! One request budget per node and bucket (C5). Every request to a bucket — retries included, for
//! it sits in the HTTP layer object_store calls — takes a turn here: at most `cap` at once. When
//! the store says slow down (503, 429), `cap` halves (once a second at most); each run of `cap`
//! answers without one adds a turn back. AIMD, as TCP and the AWS SDK's adaptive retries do. So
//! any code may fan out as wide as it likes, and the node still backs off within a second of the
//! store asking. `PONDRA_BUCKET_REQUESTS` (256) is where `cap` starts, and the most it grows to
//! is four times that.
//!
//! A branch's reads of its base's objects (ADR-047) take a turn of a share too: a quarter of the
//! node's turns, halving when the store asks as the bucket's do. A database cloned from prod then
//! never reads prod's files as hard as prod's own nodes may, on one server or another.
use object_store::client::{ClientOptions, HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::Semaphore;

/// A bucket's turns.
#[derive(Debug)]
struct Gate {
    turns: Semaphore,
    cap: AtomicUsize,  // turns there are now
    owed: AtomicUsize, // turns to take back as they are handed in (`cap` halved while they were out)
    calm: AtomicUsize, // answers in a row without a slow-down
    shrunk: AtomicU64, // when `cap` last halved (ms)
    answered: AtomicU64, // when the store last answered, but for a server error (ms): `answered`
    most: usize,
    peak: AtomicUsize, // the most turns out at once so far (`/metrics`)
}

impl Gate {
    fn new(start: usize) -> Gate {
        Gate { turns: Semaphore::new(start), cap: AtomicUsize::new(start), owed: AtomicUsize::new(0), calm: AtomicUsize::new(0), shrunk: AtomicU64::new(0), answered: AtomicU64::new(0), most: start * 4, peak: AtomicUsize::new(0) }
    }

    /// Wait for a turn.
    async fn take(&self) -> tokio::sync::SemaphorePermit<'_> {
        let turn = self.turns.acquire().await.expect("never closed");
        self.peak.fetch_max(self.cap.load(Relaxed).saturating_sub(self.turns.available_permits()), Relaxed);
        turn
    }

    /// What the store answered: slow down, or a turn nearer growing back.
    fn heard(&self, answer: &Result<HttpResponse, HttpError>) {
        match answer {
            Ok(r) if matches!(r.status().as_u16(), 429 | 503) => self.slow_down(),
            Ok(_) => self.calm(),
            Err(_) => {} // (no answer: not the store's word on its load)
        }
    }

    /// The store said slow down: half the turns, once a second at most.
    fn slow_down(&self) {
        let now = crate::log::now_ms();
        if now.saturating_sub(self.shrunk.load(Relaxed)) < 1000 {
            return;
        }
        self.shrunk.store(now, Relaxed);
        self.calm.store(0, Relaxed);
        let cap = self.cap.load(Relaxed);
        let less = cap - (cap / 2).max(4).min(cap);
        self.cap.fetch_sub(less, Relaxed);
        let taken = self.turns.forget_permits(less); // (those free now; the rest as they come back)
        self.owed.fetch_add(less - taken, Relaxed);
        eprintln!("the bucket asks to slow down: {} requests at once (from {cap})", cap - less);
    }

    /// An answer without a slow-down: a run of `cap` of them adds a turn.
    fn calm(&self) {
        let cap = self.cap.load(Relaxed);
        if self.calm.fetch_add(1, Relaxed) + 1 >= cap && cap < self.most {
            self.calm.store(0, Relaxed);
            self.cap.fetch_add(1, Relaxed);
            match self.owed.load(Relaxed) {
                0 => self.turns.add_permits(1),
                _ => drop(self.owed.try_update(Relaxed, Relaxed, |o| o.checked_sub(1))), // (one owed less instead)
            }
        }
    }

    /// A turn handed back: kept, or taken back if owed.
    fn hand_back(&self, turn: tokio::sync::SemaphorePermit<'_>) {
        if self.owed.try_update(Relaxed, Relaxed, |o| o.checked_sub(1)).is_ok() {
            turn.forget();
        }
    }
}

/// The HTTP layer of a bucket's store: object_store's own client, with the bucket's turns.
#[derive(Debug)]
pub struct Budget {
    root: String,
    gate: Arc<Gate>,
    inner: ReqwestConnector,
}

static GATES: LazyLock<Mutex<HashMap<String, Arc<Gate>>>> = LazyLock::new(Default::default);
/// Each bucket's shares: the path of another lake in it (`a/b`), and its turns.
static SHARES: LazyLock<Mutex<HashMap<String, Vec<(String, Arc<Gate>)>>>> = LazyLock::new(Default::default);

/// Where a node's turns of a bucket start (`PONDRA_BUCKET_REQUESTS`).
fn start() -> usize { std::env::var("PONDRA_BUCKET_REQUESTS").ok().and_then(|n| n.parse().ok()).unwrap_or(256usize).max(4) }

/// The bucket and the path of a lake on object storage (`s3://b/a/b` → `s3://b`, `a/b`).
fn split(url: &str) -> Option<(String, String)> {
    let (scheme, rest) = url.trim_end_matches('/').split_once("://")?;
    let (bucket, path) = rest.split_once('/')?;
    Some((format!("{scheme}://{bucket}"), path.to_string()))
}

/// Reads of the lake at `url` take a share's turn too, a quarter of the bucket's (a branch's base,
/// ADR-047). A lake at a bucket's root has no path to tell its requests by: none.
pub fn share(url: &str) {
    let Some((root, path)) = split(url) else { return };
    let mut shares = SHARES.lock().unwrap();
    let list = shares.entry(root).or_default();
    if !list.iter().any(|(p, _)| *p == path) {
        list.push((path, Arc::new(Gate::new((start() / 4).max(1)))));
    }
}

/// The share a request's path takes a turn of: path-style (`/bucket/a/b/…`, as object_store asks
/// S3, R2, GCS and Azure with an endpoint) or virtual-hosted (`/a/b/…`). Database names are SQL
/// names, so their paths need no percent-encoding to compare.
fn shared(root: &str, path: &str) -> Option<Arc<Gate>> {
    let shares = SHARES.lock().unwrap();
    let bucket = root.split_once("://").map_or(root, |(_, b)| b);
    let under = |p: &str| path.strip_prefix('/').is_some_and(|rest| rest.strip_prefix(p).or_else(|| rest.strip_prefix(&format!("{bucket}/{p}"))).is_some_and(|r| r.starts_with('/')));
    shares.get(root)?.iter().find(|(p, _)| under(p)).map(|(_, g)| g.clone())
}

/// Every bucket's and share's turns now and the most out at once (`/metrics`): (bucket, lake path
/// or "", turns, most).
pub fn turns() -> Vec<(String, String, usize, usize)> {
    let mut out: Vec<_> = GATES.lock().unwrap().iter().map(|(r, g)| (r.clone(), String::new(), g.cap.load(Relaxed), g.peak.load(Relaxed))).collect();
    for (r, list) in SHARES.lock().unwrap().iter() {
        out.extend(list.iter().map(|(p, g)| (r.clone(), p.clone(), g.cap.load(Relaxed), g.peak.load(Relaxed))));
    }
    out.sort();
    out
}

impl Budget {
    /// The budget of the bucket at `root` (`s3://bucket`): one per node, whatever opens it.
    pub fn of(root: &str) -> Budget {
        let gate = GATES.lock().unwrap().entry(root.to_string()).or_insert_with(|| Arc::new(Gate::new(start()))).clone();
        Budget { root: root.to_string(), gate, inner: ReqwestConnector::default() }
    }
}

/// When the bucket at `root` last answered this node with anything but a server error (ms; 0:
/// never): slow or asking to slow down, it is still there (`cluster::keep_alive`).
pub fn answered(root: &str) -> u64 {
    GATES.lock().unwrap().get(root).map_or(0, |g| g.answered.load(Relaxed))
}

impl HttpConnector for Budget {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(Turns { root: self.root.clone(), gate: self.gate.clone(), inner: self.inner.connect(options)? }))
    }
}

#[derive(Debug)]
struct Turns {
    root: String,
    gate: Arc<Gate>,
    inner: HttpClient,
}

#[async_trait::async_trait]
impl HttpService for Turns {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        // (the share's turn first: a request waiting on it holds none of the bucket's)
        let share = shared(&self.root, req.uri().path());
        let lent = match &share {
            Some(s) => Some(s.take().await),
            None => None,
        };
        let turn = self.gate.take().await;
        let answer = self.inner.execute(req).await;
        self.gate.heard(&answer);
        if answer.as_ref().is_ok_and(|r| !matches!(r.status().as_u16(), 500 | 502 | 504)) {
            self.gate.answered.store(crate::log::now_ms(), Relaxed);
        }
        self.gate.hand_back(turn);
        if let (Some(s), Some(t)) = (&share, lent) {
            s.heard(&answer);
            s.hand_back(t);
        }
        answer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn halves_and_grows_back() {
        let g = Gate::new(64);
        g.slow_down();
        assert_eq!((g.cap.load(Relaxed), g.turns.available_permits()), (32, 32));
        g.slow_down(); // (within the second: once)
        assert_eq!(g.cap.load(Relaxed), 32);
        for _ in 0..32 {
            g.calm();
        }
        assert_eq!((g.cap.load(Relaxed), g.turns.available_permits()), (33, 33));
    }

    #[test]
    fn a_base_takes_a_share() {
        share("s3://shares-test/lakes/prod");
        share("s3://shares-test/lakes/prod/"); // (once)
        assert_eq!(SHARES.lock().unwrap()["s3://shares-test"].len(), 1);
        let cap = |p: &str| shared("s3://shares-test", p).map(|g| g.cap.load(Relaxed));
        assert_eq!(cap("/shares-test/lakes/prod/data/t/a.parquet"), Some((start() / 4).max(1))); // (path-style)
        assert!(cap("/lakes/prod/data/t/a.parquet").is_some()); // (virtual-hosted)
        assert_eq!(cap("/shares-test/lakes/prod2/data/t/a.parquet"), None); // (another lake whose name starts alike)
        assert_eq!(cap("/shares-test/lakes/dev/data/lakes/prod/a.parquet"), None); // (the branch's own, whatever its tables are called)
        assert!(shared("s3://other", "/other/lakes/prod/a").is_none()); // (another bucket)
        share("s3://shares-test"); // (a bucket's root: nothing to tell it by)
        assert_eq!(SHARES.lock().unwrap()["s3://shares-test"].len(), 1);
    }
}
