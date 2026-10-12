//! A node opened in this process (ADR-061, `library.rs`), in four checks run in order in one test: a
//! process opens one node at a time. `cargo test --release --test node_library`.
//! 1. A follower in this process takes over in place when its leader, another process, is killed: the
//!    node restarts itself, the write lands once, and the node leads afterwards.
//! 2. A panic in one of the node's own loops stops the node and says why, and this process stays; the
//!    lake opens again with the rows acked before the panic.
//! 3. ATTACH of a lake nobody leads needs the pondra program, which a host's process doesn't have: the
//!    error says so by name (invariant 62).
//! 4. A second node can't open while one is open: refused by name, and the first keeps running.
#![cfg(feature = "server")]
#![recursion_limit = "256"] // (as the program's: a node's futures are deep)
use pondra::library::{Opened, State};
use pondra::node::Options;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Talks to nodes over HTTP. It has a runtime of its own, since `Opened::open` and `close` block the
/// thread that calls them: each request waits here for its answer, one at a time.
struct At {
    rt: tokio::runtime::Runtime,
    http: reqwest::Client,
}

/// What one request got. Status 0: no connection was made (the node isn't listening: it restarts, or
/// hasn't bound its port yet), so the node never had the request. Status 1: it was sent and no answer
/// came back (a reset, a timeout), so whether it ran isn't known.
struct Reply {
    status: u16,
    code: String, // (`x-pondra-sqlstate`)
    body: String,
}

impl Reply {
    /// How the notes name the answer: its status, and its SQLSTATE when it has one.
    fn label(&self) -> String {
        match (self.status, self.code.as_str()) {
            (0, _) => "no connection".into(),
            (1, _) => "no answer".into(),
            (s, "") => s.to_string(),
            (s, c) => format!("{s} {c}"),
        }
    }

    /// The `n` of a `count(*) AS n` answer that came with 200.
    fn count(&self) -> Option<i64> {
        if self.status != 200 {
            return None;
        }
        let rows: Vec<Value> = serde_json::from_str(&self.body).ok()?;
        rows.first()?.get("n")?.as_i64()
    }
}

impl At {
    fn new() -> At {
        At {
            rt: tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_stack_size(8 << 20).enable_all().build().unwrap(),
            // (a new connection for each request: no pooled one is left to a node that has gone)
            http: reqwest::Client::builder().pool_max_idle_per_host(0).build().unwrap(),
        }
    }

    /// What `request` got back.
    fn reply(&self, request: reqwest::RequestBuilder) -> Reply {
        self.rt.block_on(async {
            match request.send().await {
                Ok(r) => {
                    let status = r.status().as_u16();
                    let code = r.headers().get("x-pondra-sqlstate").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
                    Reply { status, code, body: r.text().await.unwrap_or_default() }
                }
                Err(e) if e.is_connect() => Reply { status: 0, code: String::new(), body: e.to_string() },
                Err(e) => Reply { status: 1, code: String::new(), body: format!("sent, no answer: {e}") },
            }
        })
    }

    /// `sql` at `url`, waiting at most `within` for its answer.
    fn send_within(&self, url: &str, sql: &str, within: Duration) -> Reply {
        self.reply(self.http.post(format!("{url}/sql")).body(sql.to_string()).timeout(within))
    }

    fn send(&self, url: &str, sql: &str) -> Reply {
        self.send_within(url, sql, Duration::from_secs(20))
    }

    /// `sql` at `url`, which must answer 200.
    fn ok(&self, url: &str, sql: &str) {
        let r = self.send(url, sql);
        assert_eq!(r.status, 200, "{sql} at {url}: {} {}", r.label(), r.body);
    }

    fn get(&self, url: &str, path: &str) -> Reply {
        self.reply(self.http.get(format!("{url}{path}")).timeout(Duration::from_secs(5)))
    }

    /// The node's `/stats`, as a signed-out caller sees it.
    fn stats(&self, url: &str) -> Value {
        serde_json::from_str(&self.get(url, "/stats").body).unwrap_or(Value::Null)
    }

    /// Waits until `/ready` answers 200 (up to 30 s).
    fn wait_ready(&self, url: &str) {
        let ready = poll(30, || (self.get(url, "/ready").status == 200).then_some(()));
        assert!(ready.is_some(), "the leader at {url} is ready within 30 s");
    }

    /// The row count of `table` at `url`, once it answers 200 (up to `secs`: a node catching up answers
    /// 503 for a while).
    fn count_within(&self, url: &str, table: &str, secs: u64) -> Option<i64> {
        poll(secs, || self.send(url, &count_sql(table)).count())
    }
}

fn count_sql(table: &str) -> String {
    format!("SELECT count(*) AS n FROM {table}")
}

/// `step` every 250 ms until it gives an answer, or `secs` pass: the answer, or None.
fn poll<T>(secs: u64, mut step: impl FnMut() -> Option<T>) -> Option<T> {
    let since = std::time::Instant::now();
    loop {
        if let Some(v) = step() {
            return Some(v);
        }
        if since.elapsed() > Duration::from_secs(secs) {
            return None;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// The node's state once `ok` holds of it, or its state after `secs` if it never does.
fn state_within(opened: &Opened, secs: u64, ok: impl Fn(&State) -> bool) -> State {
    poll(secs, || Some(opened.state()).filter(|s| ok(s))).unwrap_or_else(|| opened.state())
}

/// A state's name, without the reason a restart or a failure gives.
fn state_name(state: &State) -> &'static str {
    match state {
        State::Starting => "Starting",
        State::Running => "Running",
        State::Restarting(_) => "Restarting",
        State::Failed(_) => "Failed",
        State::Closed => "Closed",
    }
}

/// A free `host:port` on this machine: the port is bound and let go, so a node can have it next.
fn free_addr() -> String {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    format!("127.0.0.1:{port}")
}

/// A leader in a process of its own, as `pondra serve --lake` starts one. Its standard input stays
/// open to it (nothing reads it: the node isn't given `--stop-with-stdin`). Killed when dropped, so a
/// check that fails leaves no node running.
struct Leader(Child);

impl Leader {
    fn start(lake: &str, addr: &str, log: &Path) -> Leader {
        let log = std::fs::File::create(log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_pondra"))
            .args(["serve", "--lake", lake, "--addr", addr])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("the pondra program starts");
        Leader(child)
    }

    /// SIGKILL, as a machine losing the process: nothing is said to the lake, and the process is reaped.
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Leader {
    fn drop(&mut self) {
        self.kill();
    }
}

#[test]
fn a_library_node_takes_over_recovers_and_refuses_by_name() {
    // (the node's own stacks, as the program's: a query's planning needs more than a test thread's 2 MB, invariant 195)
    std::thread::Builder::new().stack_size(8 << 20).spawn(run).unwrap().join().unwrap();
}

fn run() {
    let at = At::new();
    let dir = std::env::temp_dir().join(format!("pondra-library-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    takes_over(&at, &dir);
    stops_on_a_panic(&at, &dir);
    let open = attach_needs_the_program(&at, &dir);
    one_at_a_time(open, &dir);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 1. A follower in this process takes over in place when its leader, another process, is killed.
fn takes_over(at: &At, dir: &Path) {
    let lake = dir.join("one").display().to_string();
    let (leader_addr, node_addr) = (free_addr(), free_addr());
    let (leader_url, node_url) = (format!("http://{leader_addr}"), format!("http://{node_addr}"));
    let mut leader = Leader::start(&lake, &leader_addr, &dir.join("leader-one.stderr"));
    at.wait_ready(&leader_url);
    at.ok(&leader_url, "CREATE TABLE t (id INT)");
    at.ok(&leader_url, "INSERT INTO t VALUES (1), (2), (3)");

    // The node in this process joins the lake as a follower of the leader in the other process.
    let node = Opened::open(Options::local(&lake, &node_addr)).expect("a node opens in this process on the leader's lake");
    assert_eq!(at.count_within(&node_url, "t", 30), Some(3), "the node in this process reads the leader's three rows");

    // The leader is killed without a word to the lake. The node sees its leader is gone (at once: the
    // leader's process ended on this machine, invariant 370), claims the next term and restarts in
    // place: its state says Restarting, and its door answers no connection (or 503) until it is up again. A read before the restart may still come from the node's own view
    // of the lake, so reads are asked until one is answered after the restart.
    leader.kill();
    let mut seen = BTreeSet::new(); // (the reads' answers)
    let mut states = BTreeSet::new(); // (the node's states, from the kill on)
    let mut restarted = false;
    let mut read = None;
    let since = std::time::Instant::now();
    while read.is_none() && since.elapsed() < Duration::from_secs(60) {
        let state = node.state();
        restarted |= matches!(state, State::Restarting(_));
        states.insert(state_name(&state));
        let r = at.send(&node_url, &count_sql("t"));
        seen.insert(r.label());
        if restarted {
            read = r.count();
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    eprintln!("reads from the kill on: {seen:?}; the node's states: {states:?}");
    assert!(restarted, "the node restarted in place when its leader died: its state said Restarting");
    assert!(
        seen.iter().all(|s| ["200", "no connection", "no answer", "503 57P03"].contains(&s.as_str())),
        "only what a node restarting in place answers: {seen:?}"
    );
    assert_eq!(read.expect("reads answer again within 60 s of the leader's death"), 3, "a read after the takeover sees the three rows");

    // The write goes to the node, which leads once it has taken over. It is asked again only after a
    // 503 or no connection, which never ran. After no answer the count says whether it ran, since a
    // request that was sent may have run.
    let mut wrote_seen = BTreeSet::new();
    let wrote = poll(60, || {
        let r = at.send(&node_url, "INSERT INTO t VALUES (4)");
        wrote_seen.insert(r.label());
        match r.status {
            200 => Some(Ok(())),
            0 | 503 => None,
            _ => match at.count_within(&node_url, "t", 20) {
                Some(4) => Some(Ok(())), // (it ran; only its answer was lost)
                Some(3) => None,         // (it didn't run: asked again)
                n => Some(Err(format!("no answer to the INSERT, and the count is {n:?}"))),
            },
        }
    });
    eprintln!("the INSERT's answers: {wrote_seen:?}");
    match wrote {
        Some(Ok(())) => {}
        Some(Err(e)) => panic!("the INSERT into the node after the takeover: {e}"),
        None => panic!("the INSERT into the node after the takeover wasn't answered within 60 s"),
    }
    assert_eq!(at.count_within(&node_url, "t", 10), Some(4), "the write landed once: four rows");

    // The node was restarted in place by its supervisor, so it is Running again, and it leads: `/stats`
    // says `role` (the `leader` address is left out for a signed-out caller).
    assert_eq!(state_within(&node, 10, |s| *s == State::Running), State::Running, "the node runs after taking over");
    let stats = at.stats(&node_url);
    assert_eq!(stats["role"], "leader", "the node leads after taking over: {stats}");
    node.close();
}

/// 2. A panic in one of the node's own loops stops the node and says why; this process stays, and the
/// lake opens again with the rows acked before the panic.
fn stops_on_a_panic(at: &At, dir: &Path) {
    let lake = dir.join("two").display().to_string();
    let addr = free_addr();
    let url = format!("http://{addr}");
    let node = Opened::open(Options::local(&lake, &addr)).expect("a node opens on a new lake");
    at.ok(&url, "CREATE TABLE u (id INT)");
    at.ok(&url, "INSERT INTO u VALUES (1)"); // (acked: committed and durable before this returns)

    // The crash hook panics after a commit's rows are durable, in the commit's own task (log.rs), which
    // the node runs as one of its own loops: the node fails with the panic's words.
    std::env::set_var("PONDRA_CRASH", "after_commit:1");
    // The answer to this INSERT never comes, or is an error: it isn't asked about.
    let _ = at.send_within(&url, "INSERT INTO u VALUES (2)", Duration::from_secs(5));
    let state = state_within(&node, 20, |s| matches!(s, State::Failed(_)));
    std::env::remove_var("PONDRA_CRASH");
    let why = match state {
        State::Failed(why) => why,
        other => panic!("the node is Failed within 20 s of the panic, not {other:?}"),
    };
    assert!(why.contains("PONDRA_CRASH"), "the failure says the panic's words: {why}");
    node.close();

    let again_addr = free_addr();
    let again = Opened::open(Options::local(&lake, &again_addr)).expect("the lake opens again in this process");
    let n = at.count_within(&format!("http://{again_addr}"), "u", 30);
    assert!(n.is_some_and(|n| n >= 1), "the row acked before the panic is still there: {n:?}");
    again.close();
}

/// 3. A lake nobody leads is made by the pondra program (`inbox::lead_once`), which a host's process
/// doesn't have: ATTACH of a fresh folder says so by name. The node stays open for check 4.
fn attach_needs_the_program(at: &At, dir: &Path) -> Opened {
    // (nothing in this process may name the program: PONDRA_LAUNCHER is what a host gives it)
    std::env::remove_var("PONDRA_LAUNCHER");
    let lake = dir.join("three").display().to_string();
    let addr = free_addr();
    let url = format!("http://{addr}");
    let node = Opened::open(Options::local(&lake, &addr)).expect("a node opens on a new lake");
    let fresh = dir.join("fresh"); // (an empty folder: no lake in it yet)
    let r = at.send(&url, &format!("ATTACH '{}' AS other", fresh.display()));
    assert!(r.status != 200 && r.body.contains("PONDRA_LAUNCHER"), "ATTACH of a lake nobody leads fails, naming PONDRA_LAUNCHER: {} {}", r.label(), r.body);
    node
}

/// 4. A second node can't open while one is open: refused by name, and the first one keeps running.
fn one_at_a_time(open: Opened, dir: &Path) {
    assert_eq!(open.state(), State::Running, "the node from check 3 is open and running");
    let refused = Opened::open(Options::local(&dir.join("four").display().to_string(), &free_addr())).err().expect("a second open is refused");
    let text = format!("{refused:#}");
    assert!(text.contains("close it first"), "the refusal says to close the open node first: {text}");
    assert_eq!(open.state(), State::Running, "the first node is still running after the refusal");
    open.close();
}
