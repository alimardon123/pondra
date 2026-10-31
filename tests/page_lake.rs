//! A page's member on the core's lake (designs/page-p3.md, pieces 1 and 5): its lake is kept in the store the page is given,
//! so its tables are there again after a reopen; `GET /objects` is the lake's listing; a page on no store starts fresh; one page
//! at a time in a process; and a kept lake's term is taken at once. `cargo test --test page_lake`.
#![cfg(feature = "server")]
#![recursion_limit = "256"] // (as the program's: a node's futures are deep)
use pondra::page::{Options, Page};
use std::sync::Arc;

/// `text` as SQL on `page` (`POST /sql?format=csv`): the status, the `x-pondra-sqlstate` header (`failed` puts the SQLSTATE
/// only there), and the body as text.
async fn sql(page: &Page, text: &str) -> (u16, Option<String>, String) {
    let answer = page.request("POST", "/sql?format=csv", &[], bytes::Bytes::copy_from_slice(text.as_bytes())).await;
    (answer.status, answer.header("x-pondra-sqlstate").map(str::to_string), String::from_utf8_lossy(&answer.body).into_owned())
}

#[test]
fn a_page_on_the_lake_keeps_its_tables_in_the_store_it_is_given() {
    // (the page's planning needs more than a test thread's 2 MB, as node_twice's does: invariant 195)
    std::thread::Builder::new().stack_size(8 << 20).spawn(run).unwrap().join().unwrap();
}

fn run() {
    let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(8 << 20).enable_all().build().unwrap();
    runtime.block_on(steps());
}

/// The steps, in order: each one's assert message names it.
async fn steps() {
    // 1. Kept: a table written on a given store is there when the page opens on that store again.
    let given: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut page = Page::open_lake(&Options { lake_store: Some(given.clone()), ..Default::default() }).await.expect("step 1: a page opens on the store it is given");
    for text in ["CREATE TABLE t (a INT)", "INSERT INTO t VALUES (1), (2)"] {
        let (status, _, body) = sql(&page, text).await;
        assert_eq!(status, 200, "step 1: {text}: {body}");
    }
    page.close().await;
    let mut page = Page::open_lake(&Options { lake_store: Some(given.clone()), ..Default::default() }).await.expect("step 1: the page opens again on the same store");
    let (status, _, body) = sql(&page, "SELECT sum(a) AS s FROM t").await;
    assert_eq!(status, 200, "step 1: the table is there again: {body}");
    assert_eq!(body.lines().nth(1), Some("3"), "step 1: the sum of its rows, as CSV: {body:?}");

    // 2. /objects: the lake's listing, with the table in it as a table.
    let (status, _, body) = sql(&page, "INSERT INTO t SELECT * FROM (VALUES (3)) v(a)").await;
    assert_eq!(status, 200, "step 2: an insert from a select: {body}");
    let answer = page.request("GET", "/objects", &[], bytes::Bytes::new()).await;
    assert_eq!(answer.status, 200, "step 2: GET /objects: {}", String::from_utf8_lossy(&answer.body));
    let listing: serde_json::Value = serde_json::from_slice(&answer.body).expect("step 2: GET /objects answers JSON");
    let objects = listing["objects"].as_array().expect("step 2: the listing has its objects");
    let t = objects.iter().find(|o| o["name"] == "t").expect("step 2: the table t is listed");
    assert_eq!(t["kind"], "table", "step 2: t is listed as a table: {t}");
    assert_eq!(t["rows"], 3, "step 2: its three rows are counted (an INSERT … SELECT writes its rows to files at once): {t}");
    page.close().await;

    // 3. A page on no store is in memory: a fresh lake, with no table t.
    let mut page = Page::open_lake(&Options::default()).await.expect("step 3: a page opens in memory");
    let (status, sqlstate, body) = sql(&page, "SELECT count(*) FROM t").await;
    assert!((400..600).contains(&status), "step 3: a fresh lake has no table t: {status}: {body}");
    assert_eq!(sqlstate.as_deref(), Some("42P01"), "step 3: t is not there, 42P01: {body}");
    page.close().await;

    // 4. One page at a time in a process: a second, opened while the first is open, is refused by name.
    let mut first = Page::open_lake(&Options::default()).await.expect("step 4: a page opens");
    let refused = Page::open_lake(&Options::default()).await.err().expect("step 4: a second page is refused while the first is open");
    let words = format!("{refused:#}");
    assert!(words.contains("close it first"), "step 4: the refusal says to close it first: {words}");
    first.close().await;

    // 5. A kept page's lake takes its term at once: its page reloaded without a close, so the term is still marked alive. The
    // next page on the same store opens at once, not after that mark goes stale (`members::Alone::lead` claims and marks it).
    let kept: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (_, marks) = pondra::members::Alone::lead(&kept).await.expect("step 5: a page's term is claimed and marked alive");
    marks.abort(); // (the page never closed: its term is not released, and its last mark is fresh)
    let started = std::time::Instant::now();
    let mut page = Page::open_lake(&Options { lake_store: Some(kept.clone()), ..Default::default() }).await.expect("step 5: a page opens on the store its old page left alive");
    assert!(started.elapsed() < std::time::Duration::from_secs(5), "step 5: it opens at once: {:?}", started.elapsed());
    let (status, _, body) = sql(&page, "SELECT 1").await;
    assert_eq!(status, 200, "step 5: SELECT 1 answers: {body}");
    page.close().await;
}
