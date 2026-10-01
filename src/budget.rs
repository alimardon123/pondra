//! One request budget per node and bucket (C5). Every request to a bucket — retries included, for
//! it sits in the HTTP layer object_store calls — takes a turn here: at most `cap` at once. When
//! the store says slow down (503, 429), `cap` halves (once a second at most); each run of `cap`
//! answers without one adds a turn back. AIMD, as TCP and the AWS SDK's adaptive retries do. So
//! any code may fan out as wide as it likes, and the node still backs off within a second of the
//! store asking. `PONDRA_BUCKET_REQUESTS` (256) is where `cap` starts, and the most it grows to
//! is four times that.
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
    most: usize,
}

impl Gate {
    fn new(start: usize) -> Gate {
        Gate { turns: Semaphore::new(start), cap: AtomicUsize::new(start), owed: AtomicUsize::new(0), calm: AtomicUsize::new(0), shrunk: AtomicU64::new(0), most: start * 4 }
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
                _ => drop(self.owed.fetch_update(Relaxed, Relaxed, |o| o.checked_sub(1))), // (one owed less instead)
            }
        }
    }

    /// A turn handed back: kept, or taken back if owed.
    fn hand_back(&self, turn: tokio::sync::SemaphorePermit<'_>) {
        if self.owed.fetch_update(Relaxed, Relaxed, |o| o.checked_sub(1)).is_ok() {
            turn.forget();
        }
    }
}

/// The HTTP layer of a bucket's store: object_store's own client, with the bucket's turns.
#[derive(Debug)]
pub struct Budget {
    gate: Arc<Gate>,
    inner: ReqwestConnector,
}

impl Budget {
    /// The budget of the bucket at `root` (`s3://bucket`): one per node, whatever opens it.
    pub fn of(root: &str) -> Budget {
        static GATES: LazyLock<Mutex<HashMap<String, Arc<Gate>>>> = LazyLock::new(Default::default);
        let start = std::env::var("PONDRA_BUCKET_REQUESTS").ok().and_then(|n| n.parse().ok()).unwrap_or(256usize).max(4);
        let gate = GATES.lock().unwrap().entry(root.to_string()).or_insert_with(|| Arc::new(Gate::new(start))).clone();
        Budget { gate, inner: ReqwestConnector::default() }
    }
}

impl HttpConnector for Budget {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(Turns { gate: self.gate.clone(), inner: self.inner.connect(options)? }))
    }
}

#[derive(Debug)]
struct Turns {
    gate: Arc<Gate>,
    inner: HttpClient,
}

#[async_trait::async_trait]
impl HttpService for Turns {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let turn = self.gate.turns.acquire().await.expect("never closed");
        let answer = self.inner.execute(req).await;
        match &answer {
            Ok(r) if matches!(r.status().as_u16(), 429 | 503) => self.gate.slow_down(),
            Ok(_) => self.gate.calm(),
            Err(_) => {} // (no answer: not the store's word on its load)
        }
        self.gate.hand_back(turn);
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
}
