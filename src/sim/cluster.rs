//! A sim cluster (redesign step 3, piece 3e: l7-wip/step3-sim-plan.md): N members of one lake in this process, on the
//! test's runtime. Each is kept as a lakes server keeps each of its databases (`kept::supervise` under `Host::beside`:
//! started again in place when its own work says so, with no port of its own), its lake the cluster's one bucket seen
//! through its own faults (`store.rs`), reached by name over the sim's network (`net.rs`). A test starts, kills, freezes and cuts members, and asks any of
//! them as its HTTP door is asked.
use super::faults::Faults;
use super::net::Net;
use super::store::Bucket;
use crate::node::kept::Slot;
use crate::node::State;
use anyhow::{anyhow, bail, Result};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

/// How long the test's own request waits for its answer (`Cluster::ask`): the harness's `call`'s 30 s.
const ASKED: Duration = Duration::from_secs(30);

/// One member: its name and faults, and while it is up, its supervisor and the node its last start made.
struct Member {
    name: String,
    faults: Arc<Faults>,
    net: Arc<Net>,
    /// Its doors while it serves (`kept::supervise` holds them).
    running: Slot,
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
        lock(&self.running).take();
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
    /// What each start gives its member besides its lake (`pondra serve`'s flags: `set_start`).
    start: Mutex<crate::node::Start>,
    /// Each guest's faults, by its number, from its first statement on (`guest`).
    guests: Mutex<std::collections::BTreeMap<usize, Arc<Faults>>>,
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
                    running: Default::default(),
                    state: Arc::new(Mutex::new(State::Closed)),
                    supervisor: Default::default(),
                    host: Default::default(),
                    node: Default::default(),
                })
            })
            .collect();
        Cluster { seed, name, bucket, net, members, start: Default::default(), guests: Default::default() }
    }

    /// Member `m`'s lake (0-based): `sim://<cluster>-<member>/lake`, the cluster's one bucket through its faults.
    pub fn dir(&self, m: usize) -> String { format!("sim://{}-{}/lake", self.name, self.members[m].name) }

    /// Member `m`'s name, as the others reach it.
    pub fn name(&self, m: usize) -> &str { &self.members[m].name }

    /// Starts member `m`, and waits until it serves.
    pub async fn start(&self, m: usize) -> Result<()> {
        let member = self.members[m].clone();
        anyhow::ensure!(lock(&member.supervisor).is_none(), "{} is up already", member.name);
        let (host, told) = crate::node::Host::beside(); // (kept beside the others, reached through its router: no port of its own, restarted in place)
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
        };
        let mut options = crate::node::Options::member(&self.dir(m), &member.name, given);
        options.start = lock(&self.start).clone();
        options.host = Some(host.clone());
        *lock(&member.host) = Some(host);
        self.net.join(&member.name, member.running.clone(), member.faults.clone());
        let (first, started) = std::sync::mpsc::channel();
        let supervised = crate::node::kept::supervise(options, member.state.clone(), member.running.clone(), told, first);
        *lock(&member.supervisor) = Some(crate::spawns::untracked(supervised));
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

    /// Each start from now on is given these (`pondra serve`'s flags: `--flush-ms`, `--tier-secs`, …).
    pub fn set_start(&self, set: impl FnOnce(&mut crate::node::Start)) { set(&mut lock(&self.start)); }

    /// Is member `m` up: started, and not killed since (by the test or a point it stopped at)?
    pub fn up(&self, m: usize) -> bool { lock(&self.members[m].supervisor).as_ref().is_some_and(|s| !s.is_finished()) }

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
    /// Answered on a task its kill ends (`Net::answer`), within `ASKED` on the sim's clock, as the harness's `call` waits.
    pub async fn ask(&self, m: usize, method: &str, path: &str, body: Vec<u8>) -> Result<(u16, Vec<u8>)> {
        let name = &self.members[m].name;
        let asked = async {
            self.members[m].faults.thawed().await; // (a frozen member answers no one until it is thawed)
            let res = self.net.answer(name, crate::node::request(method, path, &[], body, None)?).await?;
            let status = res.status().as_u16();
            let body = axum::body::to_bytes(res.into_body(), usize::MAX).await?;
            Ok((status, body.to_vec()))
        };
        tokio::time::timeout(ASKED, asked).await.unwrap_or_else(|_| Err(anyhow!("{name}: no answer in {} s", ASKED.as_secs())))
    }

    /// Member `m`'s `/stats`, or None while it doesn't answer.
    pub async fn stats(&self, m: usize) -> Option<serde_json::Value> {
        let (status, body) = self.ask(m, "GET", "/stats", vec![]).await.ok()?;
        serde_json::from_slice(&body).ok().filter(|_| status == 200)
    }

    /// Member `m`'s role, as its `/stats` says (`leader`, `follower`, …), or None while it doesn't answer.
    pub async fn role(&self, m: usize) -> Option<String> { self.stats(m).await?["role"].as_str().map(str::to_string) }

    /// The members member `m` knows of, as its `/stats` says: a follower's are its leader's last answer, so a follower
    /// knows two or more once it has heard its leader.
    pub async fn known(&self, m: usize) -> usize { self.stats(m).await.and_then(|s| s["nodes"].as_array().map(Vec::len)).unwrap_or(0) }

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
    pub fn met(&self) -> String {
        let guests = lock(&self.guests).iter().map(|(g, f)| format!("g{g}: {:?}", f.met())).collect::<Vec<_>>();
        self.members.iter().map(|m| format!("{}: {:?}", m.name, m.faults.met())).chain(guests).collect::<Vec<_>>().join("; ")
    }

    /// Guest `g`'s faults (`g<g>`: the lake as `pondra sql` reaches it, `sim://<cluster>-g<g>/lake`), none until set.
    pub fn guest_faults(&self, g: usize) -> Arc<Faults> {
        let made = || {
            let faults = Faults::new(self.seed.wrapping_mul(1_000_003).wrapping_add(1_000 + g as u64));
            super::store::give(&format!("{}-g{g}", self.name), self.bucket.clone(), faults.clone());
            faults
        };
        lock(&self.guests).entry(g).or_insert_with(made).clone()
    }

    /// `sql` run as `pondra sql` runs it, with job `job`, by guest `g`: a node with no address and no doors, which leads
    /// the lake for the moment if nobody does, or goes through the leader, or its inbox; it answers, waits for what it
    /// wrote to be durable, and ends. Fenced out by a newer writer, or stopped at a point (its process killed), it runs
    /// again with the same job, as the command does, so whoever records the job records it once; it gives up after five.
    pub async fn guest(&self, g: usize, sql: &str, job: &str) -> Result<serde_json::Value> {
        let (name, faults) = (format!("g{g}"), self.guest_faults(g));
        let dir = format!("sim://{}-{name}/lake", self.name);
        let path = format!("/sql?job={}", url::form_urlencoded::byte_serialize(job.as_bytes()).collect::<String>());
        let mut last = anyhow!("not run");
        for _ in 0..5 {
            // (its process: the task that runs it, and its node's tasks, all ended where they are when it stops at a point)
            let node: Arc<Mutex<Weak<crate::node_state::NodeState>>> = Default::default();
            let ran: Arc<Mutex<Option<tokio::task::AbortHandle>>> = Default::default();
            let (kept, killed, run_by, stopper) = (node.clone(), node.clone(), ran.clone(), faults.clone());
            let given = crate::node_state::Given {
                dial: Some(self.net.dial(&name)),
                started: Some(Arc::new(move |n: &Arc<crate::node_state::NodeState>| *lock(&kept) = Arc::downgrade(n))),
                stops: Some(Arc::new(move |point: &str| {
                    let stops = stopper.stops_at(point);
                    if stops {
                        lock(&run_by).take().inspect(tokio::task::AbortHandle::abort);
                        lock(&killed).upgrade().inspect(|n| n.tasks.abort_all());
                    }
                    stops
                })),
            };
            let mut options = crate::node::Options::guest(&dir, vec![], crate::node::Guest::Writes);
            (options.given, options.host) = (given, Some(crate::node::Host::beside().0));
            let (path, body) = (path.clone(), sql.as_bytes().to_vec());
            let run = crate::spawns::untracked(async move {
                let node = crate::node::Node::open(options).await?;
                let answer = async {
                    let res = node.ask("POST", &path, body).await?;
                    let (ok, sqlstate) = (res.status().is_success(), res.headers().get("x-pondra-sqlstate").and_then(|v| v.to_str().ok()).map(str::to_string));
                    let body = axum::body::to_bytes(res.into_body(), usize::MAX).await?;
                    anyhow::Ok((ok, sqlstate, String::from_utf8_lossy(&body).into_owned()))
                }
                .await;
                let finished = node.finish().await; // (whatever the answer: a guest that led gives its term back)
                let (ok, sqlstate, said) = answer?;
                if !ok {
                    return Err(crate::write::refusal(sqlstate.as_deref(), said));
                }
                finished?;
                Ok(serde_json::from_str(&said).unwrap_or(serde_json::Value::Null))
            });
            *lock(&ran) = Some(run.abort_handle());
            match run.await {
                Ok(Ok(answer)) => return Ok(answer),
                Ok(Err(e)) if crate::write::fenced(&e) => last = e,
                Ok(Err(e)) => return Err(e.context(format!("{name}: {sql}"))),
                // (stopped at a point, or sent back to run again: `Restarting`)
                Err(e) => last = anyhow!("{name} stopped: {e}"),
            }
        }
        Err(last.context(format!("{name}: gave up after five runs of {sql}")))
    }

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
        for g in lock(&self.guests).keys() {
            super::store::forget(&format!("{}-g{g}", self.name));
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> { m.lock().unwrap_or_else(PoisonError::into_inner) }
