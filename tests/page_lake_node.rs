//! A page's node on the core's lake (designs/page-p3.md, piece 4): a page asks its node for the tables it doesn't have, with
//! the rows of its own tables a query names, and a fill (CREATE TABLE … AS, INSERT … SELECT into its own table) takes the node's
//! rows and runs its write over them on the lake. The node is a leader in a process of its own, as `pondra serve --lake` starts
//! one. The steps are web_check's `nodeChecks`, and their answers are the same. `cargo test --test page_lake_node`.
#![cfg(feature = "server")]
#![recursion_limit = "256"] // (as the program's: a node's futures are deep)
use pondra::page::{Answer, Asked, Node, Options, Page, Sent};
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
    // (the page's planning needs more than a test thread's 2 MB, as page_lake's does: invariant 195)
    std::thread::Builder::new().stack_size(8 << 20).spawn(run).unwrap().join().unwrap();
}

fn run() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).thread_stack_size(8 << 20).thread_keep_alive(Duration::from_secs(1)).enable_all().build().unwrap();
    runtime.block_on(steps());
}

/// The steps, in order: each one's assert message names it.
async fn steps() {
    let dir = std::env::temp_dir().join(format!("pondra-page-node-{}", std::process::id()));
    let lake = dir.join("lake");
    std::fs::create_dir_all(&lake).unwrap();
    let addr = free_addr();
    let url = format!("http://{addr}");
    let mut leader = Leader::start(&lake.display().to_string(), &addr, &dir.join("leader.stderr"));
    let http = reqwest::Client::new();

    // The node's tables, made through its own /sql before any page asks: `sales` (4 rows), and `page_orders`, the copy of the
    // page's `orders` that the node's own answer is taken from. The node answers 200 once it is ready (up to 30 s).
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
    leader.kill();
    let _ = std::fs::remove_dir_all(&dir);
}
