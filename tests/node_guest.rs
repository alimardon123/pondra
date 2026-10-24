//! A guest node (ADR-072 §1, `node.rs` and `cluster.rs`): a node a program starts for its own statements, which answers
//! them and ends. Four checks in order in one test: a process opens one node at a time (ADR-061 §3).
//! `cargo test --release --test node_guest`.
//! 1. A guest that writes leads an empty lake, writes, and lets go: its term is given back in `cluster/left`.
//! 2. A guest that reads claims nothing: no term is added.
//! 3. A node opened right after leads: the next term, with its address.
//! 4. A guest that writes beside a live leader follows it: its write is quick, it reads its own write, it is no member
//!    of the leader, and the leader has the row afterwards.
#![cfg(feature = "server")]
#![recursion_limit = "256"] // (as the program's: a node's futures are deep)
use pondra::node::{Guest, Node, Options};
use serde_json::Value;
use std::path::Path;
use std::time::{Duration, Instant};

mod common;
use common::{free_addr, Leader};

/// The longest any one open, finish or statement may take: a failed case must not hang the test.
const BOUND: Duration = Duration::from_secs(60);

#[test]
fn a_guest_leads_reads_and_follows() {
    // (the node's own stacks, as the program's: a query's planning needs more than a test thread's 2 MB, invariant 195)
    std::thread::Builder::new().stack_size(8 << 20).spawn(run).unwrap().join().unwrap();
}

fn run() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).thread_stack_size(8 << 20).thread_keep_alive(Duration::from_secs(1)).enable_all().build().unwrap();
    runtime.block_on(async {
        let dir = std::env::temp_dir().join(format!("pondra-guest-{}", std::process::id()));
        let lake = dir.join("lake");
        std::fs::create_dir_all(&lake).unwrap();
        let lake_text = lake.display().to_string();

        leads_writes_and_lets_go(&lake_text, &lake).await;
        reads_claim_nothing(&lake_text, &lake).await;
        a_node_leads_right_after(&lake_text, &lake).await;
        follows_a_live_leader(&lake_text, &dir).await;
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// One statement through a node's own router (`Node::ask`): its status and its body.
async fn sql(node: &Node, text: &str) -> (u16, String) {
    let answer = tokio::time::timeout(BOUND, node.ask("POST", "/sql", text.into())).await.expect("the statement answers within 60 s").expect("the statement is asked");
    let status = answer.status().as_u16();
    let body = axum::body::to_bytes(answer.into_body(), 64 << 20).await.expect("the answer's body");
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// `n` of a `count(*) AS n` answer that came with 200.
fn count_of(answer: &(u16, String)) -> Option<i64> {
    if answer.0 != 200 {
        return None;
    }
    let rows: Vec<Value> = serde_json::from_str(&answer.1).ok()?;
    rows.first()?.get("n")?.as_i64()
}

/// Opens a node, within 60 s.
async fn open(o: Options) -> Node {
    tokio::time::timeout(BOUND, Node::open(o)).await.expect("the node opens within 60 s").expect("the node opens")
}

/// Finishes a guest, within 60 s.
async fn finish(node: Node) {
    tokio::time::timeout(BOUND, node.finish()).await.expect("the guest finishes within 60 s").expect("the guest finishes");
}

/// Stops a node, within 60 s.
async fn stop(node: Node) {
    tokio::time::timeout(BOUND, node.stop()).await.expect("the node stops within 60 s");
}

/// The terms of a lake (`cluster/term/`), by number, each with its address: what the record says.
fn terms(lake: &Path) -> Vec<(u64, String)> {
    let mut out = vec![];
    for e in std::fs::read_dir(lake.join("cluster/term")).into_iter().flatten().flatten() {
        let record: Value = serde_json::from_slice(&std::fs::read(e.path()).unwrap()).unwrap();
        out.push((record["n"].as_u64().unwrap(), record["addr"].as_str().unwrap_or("").to_string()));
    }
    out.sort();
    out
}

/// The terms a lake's leaders gave back (`cluster/left/`), by number.
fn left(lake: &Path) -> Vec<u64> {
    let mut out: Vec<u64> = std::fs::read_dir(lake.join("cluster/left")).into_iter().flatten().flatten().map(|e| e.file_name().to_string_lossy().parse().unwrap()).collect();
    out.sort();
    out
}

/// The row count of `t` at `url` over HTTP, once it answers 200 (up to 30 s: a node that has just opened answers 503 for a while).
async fn count_at(url: &str) -> Option<i64> {
    let http = reqwest::Client::new();
    let since = Instant::now();
    while since.elapsed() < Duration::from_secs(30) {
        if let Ok(r) = http.post(format!("{url}/sql")).body("SELECT count(*) AS n FROM t").timeout(Duration::from_secs(5)).send().await {
            if r.status().is_success() {
                let rows: Vec<Value> = serde_json::from_str(&r.text().await.unwrap_or_default()).unwrap_or_default();
                return rows.first().and_then(|row| row["n"].as_i64());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

/// 1. A guest that writes leads an empty lake, writes, and lets go: its term is in `cluster/left`.
async fn leads_writes_and_lets_go(lake_text: &str, lake: &Path) {
    let t0 = Instant::now();
    let guest = open(Options::guest(lake_text, vec![], Guest::Writes)).await;
    let opened = t0.elapsed();
    assert_eq!(sql(&guest, "CREATE TABLE t (a INT)").await.0, 200, "CREATE TABLE on a guest that leads");
    let insert = sql(&guest, "INSERT INTO t VALUES (1)").await;
    assert_eq!(insert.0, 200, "INSERT on a guest that leads: {}", insert.1);
    let t1 = Instant::now();
    finish(guest).await;
    eprintln!("node_guest 1: open {:.2} s, finish {:.2} s", opened.as_secs_f64(), t1.elapsed().as_secs_f64());
    assert_eq!(terms(lake), vec![(1, String::new())], "the guest claimed term 1 with no address, and nothing else");
    assert_eq!(left(lake), vec![1], "the guest's term is given back: cluster/left holds it");
}

/// 2. A guest that reads claims nothing.
async fn reads_claim_nothing(lake_text: &str, lake: &Path) {
    let t0 = Instant::now();
    let guest = open(Options::guest(lake_text, vec![], Guest::Reads)).await;
    let opened = t0.elapsed();
    let answer = sql(&guest, "SELECT count(*) AS n FROM t").await;
    assert_eq!(count_of(&answer), Some(1), "the guest that reads sees the one row: {}", answer.1);
    let t1 = Instant::now();
    finish(guest).await;
    eprintln!("node_guest 2: open {:.2} s, finish {:.2} s", opened.as_secs_f64(), t1.elapsed().as_secs_f64());
    assert_eq!(terms(lake), vec![(1, String::new())], "a read claims no term");
}

/// 3. A node opened right after leads: its term is the next, with its address, from the open on.
async fn a_node_leads_right_after(lake_text: &str, lake: &Path) {
    let addr = free_addr();
    let node = open(Options::local(lake_text, &addr)).await;
    assert!(terms(lake).contains(&(2, addr.clone())), "the node leads term 2 as it opens: {:?}", terms(lake));
    assert_eq!(count_at(&format!("http://{addr}")).await, Some(1), "over HTTP, the node reads the guest's one row");
    stop(node).await;
}

/// 4. A guest that writes beside a live leader follows it, and the leader has its row afterwards.
async fn follows_a_live_leader(lake_text: &str, dir: &Path) {
    let leader_addr = free_addr();
    let leader_url = format!("http://{leader_addr}");
    let mut leader = Leader::start(lake_text, &leader_addr, &dir.join("leader.stderr"));
    assert!(count_at(&leader_url).await.is_some(), "the leader in its own process answers within 30 s");

    let guest = open(Options::guest(lake_text, vec![], Guest::Writes)).await;
    let t0 = Instant::now();
    let insert = sql(&guest, "INSERT INTO t VALUES (2)").await;
    let took = t0.elapsed();
    assert_eq!(insert.0, 200, "INSERT on a guest beside a live leader: {}", insert.1);
    assert!(took < Duration::from_secs(5), "the write beside a live leader is quick (no wait for the stream to catch up): {took:?}");
    let count = sql(&guest, "SELECT count(*) AS n FROM t").await;
    assert_eq!(count_of(&count), Some(2), "the guest reads its own write through the leader's stream: {}", count.1);

    // A follower beats every second; a guest never beats, so the leader lists no node but itself.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let stats: Value = serde_json::from_str(&reqwest::get(format!("{leader_url}/stats")).await.unwrap().text().await.unwrap()).unwrap();
    assert_eq!(stats["nodes"].as_array().map(Vec::len), Some(1), "the leader's nodes are itself alone: {stats}");

    finish(guest).await;
    assert_eq!(count_at(&leader_url).await, Some(2), "the leader has both rows after the guest finishes");
    leader.kill();
}
