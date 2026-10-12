//! Two lakes one after the other in one process (ADR-061 §3, invariant 301). A node stopped leaves
//! nothing of itself, so the next one, on another lake, starts as in a process of its own: none of
//! the first lake's users (nor what it remembered of their sign-ins), grants, tables, history writer
//! or counters, its port free again, and no threads or files left over from it. `cargo test --release --test node_twice`.
#![cfg(feature = "server")]
#![recursion_limit = "256"] // (as the program's: a node's futures are deep)
use pondra::node::{Node, Options};
use serde_json::Value;
use std::time::{Duration, Instant};

const BOSS: (&str, &str) = ("boss", "boss-password-1");
const ANN: (&str, &str) = ("ann", "ann-password-1");
const CARL: (&str, &str) = ("carl", "carl-password-1");

struct At {
    url: String,
    http: reqwest::Client,
}

impl At {
    /// `sql` as `who` (None: no sign-in): its rows, or the status and the error.
    async fn sql(&self, sql: &str, who: Option<(&str, &str)>) -> Result<Vec<Value>, String> {
        let mut r = self.http.post(format!("{}/sql", self.url)).body(sql.to_string());
        if let Some((user, password)) = who {
            r = r.basic_auth(user, Some(password));
        }
        let r = r.send().await.map_err(|e| e.to_string())?;
        let (status, body) = (r.status(), r.text().await.unwrap_or_default());
        match status.is_success() {
            true => Ok(serde_json::from_str::<Value>(&body).ok().and_then(|v| v.as_array().cloned()).unwrap_or_default()),
            false => Err(format!("{status}: {body}")),
        }
    }

    /// The first column of the first row.
    async fn one(&self, sql: &str, who: Option<(&str, &str)>) -> Value {
        let rows = self.sql(sql, who).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        rows.first().and_then(|r| r.as_object()).and_then(|r| r.values().next().cloned()).unwrap_or(Value::Null)
    }

    /// `pondra_{name}` from `/metrics`, asked as `who`.
    async fn metric(&self, name: &str, who: Option<(&str, &str)>) -> f64 {
        let mut r = self.http.get(format!("{}/metrics", self.url));
        if let Some((user, password)) = who {
            r = r.basic_auth(user, Some(password));
        }
        let text = r.send().await.unwrap().text().await.unwrap();
        let line = text.lines().find(|l| l.starts_with(&format!("pondra_{name} "))).unwrap_or_else(|| panic!("no {name} in {text:.300}"));
        line.rsplit(' ').next().unwrap().parse().unwrap()
    }

    /// How many of `pondra.history`'s rows have `like` in their statement, once one has, or after 10 s.
    async fn in_history(&self, like: &str, who: Option<(&str, &str)>) -> i64 {
        // (these queries are rows of the history too, with `like` in them: left out)
        let sql = format!("SELECT count(*) AS n FROM pondra.history WHERE statement LIKE '%{like}%' AND statement NOT LIKE '%pondra.history%' AND class <> 'skipped'");
        let since = Instant::now();
        loop {
            let n = self.one(&sql, who).await.as_i64().unwrap_or(0);
            if n > 0 || since.elapsed() > Duration::from_secs(10) {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// The process's open files and threads (Linux), and the tasks alive on the runtime: a task left
/// running (SlateDB's, which outlive its handles) holds no file or thread of its own.
fn held() -> (usize, usize, usize) {
    let files = std::fs::read_dir("/proc/self/fd").map_or(0, |d| d.count());
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let threads = status.lines().find_map(|l| l.strip_prefix("Threads:")).and_then(|n| n.trim().parse().ok()).unwrap_or(0);
    (files, threads, tokio::runtime::Handle::current().metrics().num_alive_tasks())
}

/// Within `bound`: files and threads within 2 each, tasks no more.
fn within(now: (usize, usize, usize), bound: (usize, usize, usize)) -> bool {
    now.0 <= bound.0 + 2 && now.1 <= bound.1 + 2 && now.2 <= bound.2
}

/// `held()` once it is within `bound`, or the least seen in 10 s.
async fn settled(bound: Option<(usize, usize, usize)>) -> (usize, usize, usize) {
    let mut least = held();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let now = held();
        least = (least.0.min(now.0), least.1.min(now.1), least.2.min(now.2));
        if bound.is_some_and(|b| within(now, b)) {
            return now;
        }
    }
    least
}

#[test]
fn a_node_leaves_nothing_of_itself() {
    // (the node's own stacks: a query's planning needs more than a test thread's 2 MB, invariant 195)
    std::thread::Builder::new().stack_size(8 << 20).spawn(run).unwrap().join().unwrap();
}

fn run() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).thread_stack_size(8 << 20).thread_keep_alive(Duration::from_secs(1)).enable_all().build().unwrap();
    runtime.block_on(async {
        let dir = std::env::temp_dir().join(format!("pondra-twice-{}", std::process::id()));
        let (a, b) = (dir.join("a").display().to_string(), dir.join("b").display().to_string());
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{port}");
        let at = At { url: format!("http://{addr}"), http: reqwest::Client::new() };

        // Lake A: a table, a user signed in with a grant, statements in its history.
        let node = Node::open(Options::local(&a, &addr)).await.expect("lake A");
        at.sql("CREATE TABLE t (id INT)", None).await.unwrap();
        at.sql("INSERT INTO t VALUES (1), (2), (3)", None).await.unwrap();
        assert_eq!(at.one("SELECT count(*) AS n FROM t", None).await, 3);
        at.sql("CREATE USER boss PASSWORD 'boss-password-1' SUPERUSER", None).await.unwrap();
        at.sql("CREATE USER ann PASSWORD 'ann-password-1'", Some(BOSS)).await.unwrap();
        at.sql("GRANT SELECT ON t TO ann", Some(BOSS)).await.unwrap();
        assert_eq!(at.one("SELECT count(*) AS n FROM t", Some(ANN)).await, 3, "ann reads t on A");
        assert!(at.sql("SELECT 1 AS one", None).await.is_err(), "A has users: nobody unsigned");
        assert!(at.in_history("INSERT INTO t", Some(BOSS)).await > 0, "A's history has its INSERT");
        let queries_of_a = at.metric("queries_total", Some(BOSS)).await;
        assert!(queries_of_a > 1.0, "A counted its queries: {queries_of_a}");
        node.stop().await;

        // Lake B, on the same port (A's door is closed): nothing of A.
        let node = Node::open(Options::local(&b, &addr)).await.expect("lake B, on A's port");
        // (B's start asks pondra.runs once for runs its address left running, `runs::mark_stopped`,
        // which counts as a query here, as soon as it has run: 0 or 1, never A's)
        let queries_of_b = at.metric("queries_total", None).await;
        assert!(queries_of_b <= 1.0, "B's counters start at 0: {queries_of_b} queries (A ran {queries_of_a})");
        assert_eq!(at.metric("rows_in_total", None).await, 0.0, "B's counters start at 0");
        assert_eq!(at.metric("hot_bytes", None).await, 0.0, "B holds none of A's columns in memory");
        assert!(at.sql("SELECT 1 AS one", None).await.is_ok(), "B has no users: open to the machine, as a new lake is");
        assert!(at.sql("SELECT count(*) FROM t", None).await.is_err(), "B has no table t");
        at.sql("SELECT 42 AS marker_of_b", None).await.unwrap();
        assert!(at.in_history("marker_of_b", None).await > 0, "B's history is written, by B's own writer");
        assert_eq!(at.in_history("INSERT INTO t", None).await, 0, "B's history has none of A's");
        // (once B has a user of its own, a sign-in counts: A's users, and what A remembered of them, are gone)
        at.sql("CREATE USER carl PASSWORD 'carl-password-1' SUPERUSER", None).await.unwrap();
        assert!(at.sql("SELECT 1 AS one", Some(CARL)).await.is_ok(), "carl (B's) signs in on B");
        assert!(at.sql("SELECT 1 AS one", Some(ANN)).await.is_err(), "ann (A's) can't sign in on B");
        assert!(at.sql("SELECT 1 AS one", Some(BOSS)).await.is_err(), "boss (A's) can't sign in on B");
        assert!(at.sql("SELECT 1 AS one", None).await.is_err(), "B has users now: nobody unsigned");
        node.stop().await;

        // A again: as it was, and none of B's history written into it.
        let node = Node::open(Options::local(&a, &addr)).await.expect("lake A again");
        assert!(at.sql("SELECT 1 AS one", None).await.is_err(), "A still has users");
        assert_eq!(at.one("SELECT count(*) AS n FROM t", Some(ANN)).await, 3, "ann reads t on A again");
        assert_eq!(at.in_history("marker_of_b", Some(BOSS)).await, 0, "A's history has none of B's");
        node.stop().await;

        // Nothing left over: three more opens and stops hold no more files, threads or tasks.
        // (Blocking threads go after a second idle, so the count is taken once it settles, within 10 s.)
        let before = settled(None).await;
        for _ in 0..3 {
            let node = Node::open(Options::local(&a, &addr)).await.expect("lake A, again and again");
            assert_eq!(at.one("SELECT count(*) AS n FROM t", Some(ANN)).await, 3);
            node.stop().await;
        }
        let after = settled(Some(before)).await;
        assert!(within(after, before), "files, threads and tasks before {before:?}, after three more {after:?}");
        let _ = std::fs::remove_dir_all(&dir);
    });
}
