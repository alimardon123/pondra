//! A sim cluster (redesign step 3, piece 3e: l7-wip/step3-sim-plan.md): N members of one lake in this process, on the
//! test's runtime. Each is a node as a library runs it (`library::supervise`: started again in place when its own work
//! says so), whose lake is the cluster's one bucket seen through its own faults (`store.rs`), who listens on no port and
//! is reached by name over the sim's network (`net.rs`). A test starts, kills, freezes and cuts members, and asks any of
//! them as its HTTP door is asked.
use super::faults::Faults;
use super::net::Net;
use super::store::Bucket;
use crate::library::State;
use anyhow::{anyhow, bail, Result};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

/// One member: its name and faults, and while it is up, its supervisor and the node its last start made.
struct Member {
    name: String,
    faults: Arc<Faults>,
    net: Arc<Net>,
    /// What answers it while it serves (`library::supervise` keeps it).
    router: Arc<Mutex<Option<axum::Router>>>,
    state: Arc<Mutex<State>>,
    supervisor: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Its host's end: dropped as it is killed, so nothing is told to a supervisor that's gone.
    host: Mutex<Option<crate::node::Host>>,
    node: Mutex<Weak<crate::node_state::NodeState>>,
}

impl Member {
    /// Killed: its supervisor and every task of its node end where they are, the calls it was answering get no answer,
    /// and nothing reaches it, as when its process is killed. Its objects stay in the bucket.
    fn kill(&self) {
        if let Some(s) = lock(&self.supervisor).take() {
            s.abort();
        }
        if let Some(node) = lock(&self.node).upgrade() {
            node.tasks.abort_all();
        }
        lock(&self.host).take();
        lock(&self.router).take();
        self.net.down(&self.name);
    }
}

/// N members of one lake in one process.
pub struct Cluster {
    /// The seed the members' dice are thrown from (each member's its own, from this one).
    pub seed: u64,
    name: String,
    bucket: Arc<Bucket>,
    /// The members' network.
    pub net: Arc<Net>,
    members: Vec<Arc<Member>>,
}

impl Cluster {
    /// A cluster of `n` members, with no faults and none up yet (`start` each).
    pub fn new(seed: u64, n: usize) -> Cluster {
        let name = format!("c{seed}x{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let (bucket, net) = (super::store::bucket(), Net::new());
        let members = (1..=n)
            .map(|i| {
                let faults = Faults::new(seed.wrapping_mul(1_000_003).wrapping_add(i as u64));
                super::store::give(&format!("{name}-m{i}"), bucket.clone(), faults.clone());
                Arc::new(Member {
                    name: format!("m{i}"),
                    faults,
                    net: net.clone(),
                    router: Default::default(),
                    state: Arc::new(Mutex::new(State::Closed)),
                    supervisor: Default::default(),
                    host: Default::default(),
                    node: Default::default(),
                })
            })
            .collect();
        Cluster { seed, name, bucket, net, members }
    }

    /// Member `m`'s lake (0-based): `sim://<cluster>-<member>/lake`, the cluster's one bucket through its faults.
    pub fn dir(&self, m: usize) -> String { format!("sim://{}-{}/lake", self.name, self.members[m].name) }

    /// Member `m`'s name, as the others reach it.
    pub fn name(&self, m: usize) -> &str { &self.members[m].name }

    /// Starts member `m`, and waits until it serves.
    pub async fn start(&self, m: usize) -> Result<()> {
        let member = self.members[m].clone();
        anyhow::ensure!(lock(&member.supervisor).is_none(), "{} is up already", member.name);
        let (host, told) = crate::node::Host::new();
        let (kept, killed) = (Arc::downgrade(&member), Arc::downgrade(&member));
        let given = crate::node_state::Given {
            dial: Some(self.net.dial(&member.name)),
            started: Some(Arc::new(move |n: &Arc<crate::node_state::NodeState>| {
                if let Some(m) = kept.upgrade() {
                    *lock(&m.node) = Arc::downgrade(n);
                }
            })),
            // (a point it stops at kills it: every task of its node ends, the one that got there by its panic)
            stops: Some(Arc::new(move |point: &str| {
                let Some(m) = killed.upgrade() else { return false };
                let stops = m.faults.stops_at(point);
                if stops {
                    m.kill();
                }
                stops
            })),
            no_port: true,
        };
        let mut options = crate::node::Options::member(&self.dir(m), &member.name, given);
        options.host = Some(host.clone());
        *lock(&member.host) = Some(host);
        self.net.join(&member.name, member.router.clone(), member.faults.clone());
        let (first, started) = std::sync::mpsc::channel();
        let supervised = crate::library::supervise(options, member.state.clone(), member.router.clone(), told, first);
        *lock(&member.supervisor) = Some(crate::spawns::untracked(crate::auth::as_node(supervised)));
        // (the supervisor says when the first start serves, or why it didn't: asked as the test's time passes)
        loop {
            match started.try_recv() {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(why)) => bail!("{}: {why}", member.name),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => bail!("{}: its supervisor ended before it served", member.name),
                Err(std::sync::mpsc::TryRecvError::Empty) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    /// Kills member `m` (`Member::kill`).
    pub fn kill(&self, m: usize) { self.members[m].kill(); }

    /// Kills member `m` and starts it again.
    pub async fn restart(&self, m: usize) -> Result<()> {
        self.kill(m);
        self.start(m).await
    }

    /// Member `m`'s faults: what its calls to the bucket, and to and from the others, meet.
    pub fn faults(&self, m: usize) -> &Arc<Faults> { &self.members[m].faults }

    /// Asks member `m` `sql` (`POST /sql`, as its door is asked by a client of its own), and gives the answer as JSON.
    pub async fn sql(&self, m: usize, sql: &str) -> Result<serde_json::Value> {
        let (status, body) = self.ask(m, "POST", "/sql", sql.as_bytes().to_vec()).await?;
        match status {
            200..=299 => Ok(serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)),
            s => Err(anyhow!("{} answered {s}: {}", self.members[m].name, String::from_utf8_lossy(&body))),
        }
    }

    /// One request to member `m`'s router, from no member: its status and whole body.
    pub async fn ask(&self, m: usize, method: &str, path: &str, body: Vec<u8>) -> Result<(u16, Vec<u8>)> {
        use tower::ServiceExt;
        let Some(router) = lock(&self.members[m].router).clone() else { bail!("{} isn't serving", self.members[m].name) };
        let req = crate::node::request(method, path, &[], body, None)?;
        let res = router.oneshot(req).await.unwrap_or_else(|never| match never {});
        let status = res.status().as_u16();
        let body = axum::body::to_bytes(res.into_body(), usize::MAX).await?;
        Ok((status, body.to_vec()))
    }

    /// Member `m`'s role, as its `/stats` says (`leader`, `follower`, …), or None while it doesn't answer.
    pub async fn role(&self, m: usize) -> Option<String> {
        let (status, body) = self.ask(m, "GET", "/stats", vec![]).await.ok()?;
        let stats: serde_json::Value = serde_json::from_slice(&body).ok().filter(|_| status == 200)?;
        stats["role"].as_str().map(str::to_string)
    }

    /// Waits up to `within` on the sim's clock for `ready`, asked every 100 ms; past it, the error says the faults met.
    pub async fn until<F: std::future::Future<Output = bool>>(&self, what: &str, within: Duration, mut ready: impl FnMut() -> F) -> Result<Duration> {
        let (began, end) = (crate::clock::now(), crate::clock::now() + within);
        while crate::clock::now() < end {
            if ready().await {
                return Ok(began.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        bail!("{what}: not within {} s (seed {}; faults met: {})", within.as_secs_f64(), self.seed, self.met())
    }

    /// Every fault each member met, in order: what a failing seed says.
    pub fn met(&self) -> String { self.members.iter().map(|m| format!("{}: {:?}", m.name, m.faults.met())).collect::<Vec<_>>().join("; ") }

    /// The cluster's one bucket (what every member's lake is).
    pub fn bucket(&self) -> &Arc<Bucket> { &self.bucket }
}

impl Drop for Cluster {
    /// The members go with the test, and their buckets' names with them.
    fn drop(&mut self) {
        for (i, m) in self.members.iter().enumerate() {
            m.kill();
            super::store::forget(&format!("{}-m{}", self.name, i + 1));
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> { m.lock().unwrap_or_else(PoisonError::into_inner) }
