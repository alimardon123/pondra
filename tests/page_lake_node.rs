//! A page's node on the core's lake (designs/page-p3.md, piece 4): a page asks its node for the tables it doesn't have, with
//! the rows of its own tables a query names, and a fill (CREATE TABLE … AS, INSERT … SELECT into its own table) takes the node's
//! rows and runs its write over them on the lake. The node is a leader in a process of its own, as `pondra serve --lake` starts
//! one. The steps are web_check's `nodeChecks`, and their answers are the same. `cargo test --test page_lake_node`.
#![cfg(feature = "server")]
#![recursion_limit = "256"] // (as the program's: a node's futures are deep)
use pondra::page::{Answer, Asked, Node, Options, Page, Sent};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod common;
use common::{free_addr, Leader};

/// The join of a page's `orders` (or the leader's `page_orders`) with `sales`, summed per id: the same body from every page and the node.
const JOINED: &str = "id,name,total\n1,a,15\n3,c,7\n";

fn joined(table: &str) -> String {
    format!("SELECT o.id, o.name, sum(s.amount) AS total FROM {table} o JOIN sales s ON s.id = o.id GROUP BY o.id, o.name ORDER BY o.id")
}

/// `text` as SQL on `page` (`POST /sql?format=csv`): the status, the `x-pondra-sqlstate` header, and the body as text.
async fn sql(page: &Page, text: &str) -> (u16, Option<String>, String) {
    let answer = page.request("POST", "/sql?format=csv", &[], bytes::Bytes::copy_from_slice(text.as_bytes())).await;
    (answer.status, answer.header("x-pondra-sqlstate").map(str::to_string), String::from_utf8_lossy(&answer.body).into_owned())
}

/// `text` as SQL on the leader at `url` (`POST /sql?format=csv`): the status and the body as text.
async fn leader_sql(http: &reqwest::Client, url: &str, text: &str) -> (u16, String) {
    let r = http.post(format!("{url}/sql?format=csv")).body(text.to_string()).send().await.expect("the leader answers");
    let status = r.status().as_u16();
    (status, r.text().await.unwrap_or_default())
}

/// `text` as SQL on `page` in the session `session` (`x-pondra-session`): the status, the `x-pondra-sqlstate` header, and the body as text.
async fn sql_in(page: &Page, session: &str, text: &str) -> (u16, Option<String>, String) {
    let headers = [("x-pondra-session".to_string(), session.to_string())];
    let answer = page.request("POST", "/sql?format=csv", &headers, bytes::Bytes::copy_from_slice(text.as_bytes())).await;
    (answer.status, answer.header("x-pondra-sqlstate").map(str::to_string), String::from_utf8_lossy(&answer.body).into_owned())
}

/// A page's node (`node_at`) that counts the requests it is sent, so a step can check a statement never went to the node.
fn counted_node(url: String, asked: Arc<AtomicUsize>) -> Node {
    let node = node_at(url);
    Arc::new(move |sent: Sent| -> Asked {
        asked.fetch_add(1, Ordering::SeqCst);
        node(sent)
    })
}

/// A page's node: the leader at `url`, asked as `Sent` says (`POST /sql?{query}` with its body and content type), answered as a
/// page gets an answer: its status, `content-type`, `x-pondra-sqlstate` and body.
fn node_at(url: String) -> Node {
    let http = reqwest::Client::new();
    Arc::new(move |sent: Sent| -> Asked {
        let (http, url) = (http.clone(), url.clone());
        Box::pin(async move {
            let r = http.post(format!("{url}/sql?{}", sent.query)).header("content-type", sent.content_type).body(sent.body).send().await.map_err(|e| e.to_string())?;
            let status = r.status().as_u16();
            let mut headers: Vec<(&'static str, String)> = vec![];
            if let Some(v) = r.headers().get("content-type").and_then(|v| v.to_str().ok()) {
                headers.push(("content-type", v.to_string()));
            }
            if let Some(v) = r.headers().get("x-pondra-sqlstate").and_then(|v| v.to_str().ok()) {
                headers.push(("x-pondra-sqlstate", v.to_string()));
            }
            let body = r.bytes().await.map_err(|e| e.to_string())?;
            Ok(Answer { status, headers, body })
        })
    })
}

#[test]
fn a_page_asks_its_node_for_what_it_lacks() {
    // (the page's planning needs more than a test thread's 2 MB, as page_lake's does: invariant 195). Each part runs on a thread of
    // its own with 8 MB of stack: a part's frames are its thread's, so the transaction's steps, many and deep, don't share the stack of
    // the steps before them.
    std::thread::Builder::new().stack_size(8 << 20).spawn(run).unwrap().join().unwrap();
    std::thread::Builder::new().stack_size(8 << 20).spawn(run_transactions).unwrap().join().unwrap();
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).thread_stack_size(8 << 20).thread_keep_alive(Duration::from_secs(1)).enable_all().build().unwrap()
}

fn run() {
    runtime().block_on(steps());
}

fn run_transactions() {
    runtime().block_on(transactions());
}

/// A node of its own in `dir`, with the tables the steps ask it for: `sales` (4 rows), and `page_orders`, the copy of the page's
/// `orders` that the node's own answer is taken from. The node answers 200 once it is ready (up to 30 s). Returns its leader and url.
async fn node_in(dir: &std::path::Path) -> (Leader, String) {
    let lake = dir.join("lake");
    std::fs::create_dir_all(&lake).unwrap();
    let addr = free_addr();
    let url = format!("http://{addr}");
    let leader = Leader::start(&lake.display().to_string(), &addr, &dir.join("leader.stderr"));
    let http = reqwest::Client::new();
    let since = Instant::now();
    while since.elapsed() < Duration::from_secs(30) && !http.get(format!("{url}/ready")).send().await.is_ok_and(|r| r.status().is_success()) {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    for text in [
        "CREATE TABLE sales (id INT, amount INT)",
        "INSERT INTO sales VALUES (1, 10), (1, 5), (3, 7), (4, 1)",
        "CREATE TABLE page_orders AS SELECT * FROM (VALUES (1, 'a', 5), (2, 'b', 7), (3, 'c', 9)) t(id, name, qty)",
    ] {
        let (status, body) = leader_sql(&http, &url, text).await;
        assert_eq!(status, 200, "the node makes its table: {text}: {body}");
    }
    let (status, own) = leader_sql(&http, &url, &joined("page_orders")).await;
    assert_eq!(status, 200, "the node's own join: {own}");
    assert_eq!(own, JOINED, "the node's own join is the body every page's join must equal");
    (leader, url)
}

/// The steps 1 to 6, in order: each one's assert message names it.
async fn steps() {
    let dir = std::env::temp_dir().join(format!("pondra-page-node-{}", std::process::id()));
    let (mut leader, url) = node_in(&dir).await;

    // 1. A page's `orders` (made as a page makes a table, from its own VALUES) joined with the node's `sales`: the page sends the
    // node the rows of `orders`, and gives on the node's answer.
    let mut page = Page::open_lake(&Options { node: Some(node_at(url.clone())), ..Default::default() }).await.expect("step 1: a page opens with its node");
    let (status, _, body) = sql(&page, "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'a', 5), (2, 'b', 7), (3, 'c', 9)) t(id, name, qty)").await;
    assert_eq!(status, 200, "step 1: the page makes orders: {body}");
    let (status, sqlstate, body) = sql(&page, &joined("orders")).await;
    assert_eq!(status, 200, "step 1: orders joined with sales: {body} ({sqlstate:?})");
    assert_eq!(body, JOINED, "step 1: the page's join is the node's: {body:?}");

    // 2. A copy of the node's table, made from the node's rows: the node answers the query, the copy holds its four rows.
    let (status, sqlstate, body) = sql(&page, "CREATE TABLE copy AS SELECT * FROM sales").await;
    assert_eq!(status, 200, "step 2: CREATE TABLE copy from the node's rows: {body} ({sqlstate:?})");
    let (status, _, body) = sql(&page, "SELECT count(*) AS n FROM copy").await;
    assert_eq!((status, body.as_str()), (200, "n\n4\n"), "step 2: the copy holds the node's four rows");

    // 3. What the page can't take is refused by name, before anything is sent: a write into the node's table, and a file joined
    // with one (the file is not put: its name is the page's, and the query names a node's table beside it).
    let (status, sqlstate, body) = sql(&page, "INSERT INTO sales VALUES (9, 9)").await;
    assert_eq!((status, sqlstate.as_deref()), (501, Some("0A000")), "step 3: a write into the node's table: {body}");
    let (status, sqlstate, body) = sql(&page, "SELECT count(*) FROM 'orders.csv' o JOIN sales s ON s.id = o.id").await;
    assert_eq!((status, sqlstate.as_deref()), (501, Some("0A000")), "step 3: a file beside the node's table: {body}");

    // 4. A table nobody has: the node's own answer, 404 with 42P01.
    let (status, sqlstate, body) = sql(&page, "SELECT * FROM nowhere").await;
    assert_eq!((status, sqlstate.as_deref()), (404, Some("42P01")), "step 4: a table nobody has: {body}");

    // 5. Only a plain-SQL body may be asked of the node: a JSON body that names the node's table is the page's own 42P01.
    let json = bytes::Bytes::from_static(br#"{"sql": "SELECT count(*) FROM sales"}"#);
    let answer = page.request("POST", "/sql?format=csv", &[("content-type".to_string(), "application/json".to_string())], json).await;
    assert_eq!((answer.status, answer.header("x-pondra-sqlstate")), (404, Some("42P01")), "step 5: a JSON body never goes to the node: {}", String::from_utf8_lossy(&answer.body));

    page.close().await;

    // 6. A node that doesn't answer is 08001, and its words say which node did not answer: the page's own answer (`ask`), with a
    // table the page doesn't have, and a node that gives no answer.
    let gone: Node = Arc::new(|_sent: Sent| -> Asked { Box::pin(async { Err("no answer from http://x".to_string()) }) });
    let mut page = Page::open_lake(&Options { node: Some(gone), ..Default::default() }).await.expect("step 6: a page opens with a node that doesn't answer");
    let (status, sqlstate, body) = sql(&page, "SELECT count(*) FROM lake_orders").await;
    assert_eq!((status, sqlstate.as_deref()), (503, Some("08001")), "step 6: a node not reached is 08001: {body}");
    assert_eq!(body, "the page's node didn't answer: no answer from http://x", "step 6: its words say which node did not answer");
    page.close().await;

    leader.kill();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Steps 7 to 10, in order, on a node of their own (`run_transactions`): a transaction's statements over the node's tables.
async fn transactions() {
    let dir = std::env::temp_dir().join(format!("pondra-page-node-tx-{}", std::process::id()));
    let (mut leader, url) = node_in(&dir).await;

    // 7. In a transaction, a query over the node's table answers its rows, and the transaction's next statement runs (no 25P02): the
    // node's part is decided and answered in the request's session, which stays open.
    let mut page = Page::open_lake(&Options { node: Some(node_at(url.clone())), ..Default::default() }).await.expect("step 7: a page opens with its node");
    let (status, _, body) = sql(&page, "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'a', 5), (2, 'b', 7)) t(id, name, qty)").await;
    assert_eq!(status, 200, "step 7: the page makes orders: {body}");
    let (status, _, body) = sql_in(&page, "step-seven", "BEGIN").await;
    assert_eq!(status, 200, "step 7: BEGIN: {body}");
    let (status, sqlstate, body) = sql_in(&page, "step-seven", "SELECT count(*) AS n FROM sales").await;
    assert_eq!((status, body.as_str()), (200, "n\n4\n"), "step 7: a query over the node's table in BEGIN answers its rows: {body} ({sqlstate:?})");
    let (status, sqlstate, body) = sql_in(&page, "step-seven", "SELECT 1 AS one").await;
    assert_eq!((status, sqlstate.as_deref(), body.as_str()), (200, None, "one\n1\n"), "step 7: the transaction's next statement runs: {body}");
    let (status, _, body) = sql_in(&page, "step-seven", "ROLLBACK").await;
    assert_eq!(status, 200, "step 7: ROLLBACK: {body}");
    page.close().await;

    // 8. A fill in a transaction is the transaction's: it is written in the session, the session sees it, and ROLLBACK takes it back
    // out, so the page's table is as it was.
    let mut page = Page::open_lake(&Options { node: Some(node_at(url.clone())), ..Default::default() }).await.expect("step 8: a page opens with its node");
    let (status, _, body) = sql(&page, "CREATE TABLE filled (id INT, amount INT)").await;
    assert_eq!(status, 200, "step 8: the page makes filled: {body}");
    let (status, _, body) = sql_in(&page, "step-eight", "BEGIN").await;
    assert_eq!(status, 200, "step 8: BEGIN: {body}");
    let (status, sqlstate, body) = sql_in(&page, "step-eight", "INSERT INTO filled SELECT * FROM sales").await;
    assert_eq!(status, 200, "step 8: a fill of the node's rows in BEGIN: {body} ({sqlstate:?})");
    let (status, _, body) = sql_in(&page, "step-eight", "SELECT count(*) AS n FROM filled").await;
    assert_eq!((status, body.as_str()), (200, "n\n4\n"), "step 8: the transaction sees its own fill: {body}");
    let (status, _, body) = sql_in(&page, "step-eight", "ROLLBACK").await;
    assert_eq!(status, 200, "step 8: ROLLBACK: {body}");
    let (status, _, body) = sql(&page, "SELECT count(*) AS n FROM filled").await;
    assert_eq!((status, body.as_str()), (200, "n\n0\n"), "step 8: the rollback leaves the page's table as it was: {body}");
    page.close().await;

    // 9. In a transaction, a row inserted into a page's table is in what a join with the node's table answers; ROLLBACK takes it out
    // again, and the join outside the transaction is the committed rows' only.
    let mut page = Page::open_lake(&Options { node: Some(node_at(url.clone())), ..Default::default() }).await.expect("step 9: a page opens with its node");
    let (status, _, body) = sql(&page, "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'a', 5), (2, 'b', 7)) t(id, name, qty)").await;
    assert_eq!(status, 200, "step 9: the page makes orders: {body}");
    let (status, _, body) = sql_in(&page, "step-nine", "BEGIN").await;
    assert_eq!(status, 200, "step 9: BEGIN: {body}");
    let (status, sqlstate, body) = sql_in(&page, "step-nine", "INSERT INTO orders VALUES (3, 'c', 9)").await;
    assert_eq!(status, 200, "step 9: an insert into the page's table in BEGIN: {body} ({sqlstate:?})");
    let (status, sqlstate, body) = sql_in(&page, "step-nine", &joined("orders")).await;
    assert_eq!((status, body.as_str()), (200, JOINED), "step 9: the join in BEGIN has the inserted row: {body} ({sqlstate:?})");
    let (status, _, body) = sql_in(&page, "step-nine", "ROLLBACK").await;
    assert_eq!(status, 200, "step 9: ROLLBACK: {body}");
    let (status, _, body) = sql(&page, &joined("orders")).await;
    assert_eq!((status, body.as_str()), (200, "id,name,total\n1,a,15\n"), "step 9: after ROLLBACK the join has the committed rows only: {body}");
    page.close().await;

    // 10. A query over a page's view is answered by the lake, and never sent to the node: the view is the page's, as its table is.
    let asked = Arc::new(AtomicUsize::new(0));
    let mut page = Page::open_lake(&Options { node: Some(counted_node(url.clone(), asked.clone())), ..Default::default() }).await.expect("step 10: a page opens with its node");
    let (status, _, body) = sql(&page, "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'a', 5), (2, 'b', 7)) t(id, name, qty)").await;
    assert_eq!(status, 200, "step 10: the page makes orders: {body}");
    let (status, _, body) = sql(&page, "CREATE VIEW named_orders AS SELECT id, name FROM orders").await;
    assert_eq!(status, 200, "step 10: the page makes a view over its table: {body}");
    let (status, _, body) = sql(&page, "SELECT id, name FROM named_orders ORDER BY id").await;
    assert_eq!((status, body.as_str()), (200, "id,name\n1,a\n2,b\n"), "step 10: the view answers from the lake: {body}");
    assert_eq!(asked.load(Ordering::SeqCst), 0, "step 10: no statement over the page's view was sent to the node");
    page.close().await;

    leader.kill();
    let _ = std::fs::remove_dir_all(&dir);
}
