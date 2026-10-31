//! A page's member on the core's lake (designs/page-p3.md, pieces 1, 5 and 6): its lake is kept in the store the page is given,
//! so its tables are there again after a reopen; `GET /objects` is the lake's listing; a page on no store starts fresh; a folder
//! kept by 0.36 is imported once on open; one page at a time in a process; and a kept lake's term is taken at once.
//! `cargo test --test page_lake`.
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
    // (the page's planning needs more than a test thread's 2 MB, as node_twice's does: invariant 195. A debug build keeps every future
    // `steps` awaits in its own frame, and a page's import is deep: 32 MB, where 8 MB overflowed in this file's debug build)
    std::thread::Builder::new().stack_size(32 << 20).spawn(run).unwrap().join().unwrap();
}

fn run() {
    let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(32 << 20).enable_all().build().unwrap();
    runtime.block_on(steps());
}

/// The steps, in order: each one's assert message names it.
async fn steps() {
    // 1. Kept: a table written on a given store is there when the page opens on that store again.
    let given: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut page = Page::open_lake(&Options { store: Some(given.clone()), ..Default::default() }).await.expect("step 1: a page opens on the store it is given");
    for text in ["CREATE TABLE t (a INT)", "INSERT INTO t VALUES (1), (2)"] {
        let (status, _, body) = sql(&page, text).await;
        assert_eq!(status, 200, "step 1: {text}: {body}");
    }
    page.close().await;
    let mut page = Page::open_lake(&Options { store: Some(given.clone()), ..Default::default() }).await.expect("step 1: the page opens again on the same store");
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

    // 4. A kept folder of 0.36 (tests/fixtures/page-0.36: the files the old engine wrote for `CREATE TYPE mood`, a table `old` with
    // two rows and an identity counter, and a view over it) is imported once on open: its type, its table with its rows and identity
    // counter, and its view come over through the core, and the files go. A crash that leaves a table's file behind imports nothing
    // again, and the rows are not written twice.
    use object_store::ObjectStoreExt as _;
    let lake_folder: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    fixture_into(lake_folder.as_ref()).await;
    let kept_table = lake_folder.get(&object_store::path::Path::from("tables/old.arrow")).await.expect("step 4: the old table's file").bytes().await.expect("step 4: its bytes");

    let mut page = Page::open_lake(&Options { store: Some(lake_folder.clone()), ..Default::default() }).await.expect("step 4: the kept folder imports on open");
    assert!(page.unread().is_empty(), "step 4: every kept file imports: {:?}", page.unread());
    let left = left_kept(lake_folder.as_ref()).await;
    assert!(left.is_empty(), "step 4: the kept files are gone once imported; still there: {left:?}");
    let (_, _, body) = sql(&page, "SELECT count(*) FROM old").await;
    assert_eq!(body.lines().nth(1), Some("2"), "step 4: the table's rows came over: {body}");
    let (_, _, body) = sql(&page, "SELECT name FROM old_names ORDER BY 1").await;
    assert_eq!(body.lines().skip(1).collect::<Vec<_>>(), ["a", "b"], "step 4: the view answers over the table: {body}");
    let (status, _, body) = sql(&page, "SELECT 'ok'::mood").await;
    assert_eq!((status, body.lines().nth(1)), (200, Some("ok")), "step 4: the type came over: {body}");
    let (_, sqlstate, body) = sql(&page, "INSERT INTO old VALUES (1, 'x')").await;
    assert_eq!(sqlstate.as_deref(), Some("23505"), "step 4: the key came over with the table: {body}");
    page.close().await;

    // A crash after the table was made and before its file went: the file is put back, and the next open finds the table there with
    // its rows, takes the file away, and writes no second copy of the rows.
    lake_folder.put(&object_store::path::Path::from("tables/old.arrow"), object_store::PutPayload::from(kept_table)).await.expect("step 4: the file is put back");
    let mut page = Page::open_lake(&Options { store: Some(lake_folder.clone()), ..Default::default() }).await.expect("step 4: the page opens after the crash");
    assert!(page.unread().is_empty(), "step 4: the file goes, as its table is there with its rows: {:?}", page.unread());
    assert!(left_kept(lake_folder.as_ref()).await.is_empty(), "step 4: the file is gone again");
    let (_, _, body) = sql(&page, "SELECT count(*) FROM old").await;
    assert_eq!(body.lines().nth(1), Some("2"), "step 4: still 2 rows, imported once: {body}");
    page.close().await;

    // The identity counter came over: the next id is the kept counter's, 3.
    let mut page = Page::open_lake(&Options { store: Some(lake_folder.clone()), ..Default::default() }).await.expect("step 4: the page opens again");
    let (status, _, body) = sql(&page, "INSERT INTO old (name) VALUES ('c')").await;
    assert_eq!(status, 200, "step 4: an insert with no id: {body}");
    let (_, _, body) = sql(&page, "SELECT id FROM old WHERE name = 'c'").await;
    assert_eq!(body.lines().nth(1), Some("3"), "step 4: the next id is 3: {body}");
    page.close().await;

    // 4b. A table whose CREATE was made, and a page closed before its rows went in: the next open finds the table with no rows, and
    // the file's rows go in, its identity counter with them, and the file goes.
    let made: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut page = Page::open_lake(&Options { store: Some(made.clone()), ..Default::default() }).await.expect("step 4b: a page opens on an empty store");
    let (status, _, body) = sql(&page, "CREATE TABLE old (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name TEXT)").await;
    assert_eq!(status, 200, "step 4b: the CREATE, with no rows: {body}");
    page.close().await;
    fixture_into(made.as_ref()).await;
    let mut page = Page::open_lake(&Options { store: Some(made.clone()), ..Default::default() }).await.expect("step 4b: the page opens with the file beside its empty table");
    assert!(page.unread().is_empty(), "step 4b: the table's rows come in: {:?}", page.unread());
    assert!(left_kept(made.as_ref()).await.is_empty(), "step 4b: the file goes once its rows are in");
    let (_, _, body) = sql(&page, "SELECT count(*) FROM old").await;
    assert_eq!(body.lines().nth(1), Some("2"), "step 4b: the rows came in once: {body}");
    let (status, _, body) = sql(&page, "INSERT INTO old (name) VALUES ('c')").await;
    assert_eq!(status, 200, "step 4b: an insert with no id: {body}");
    let (_, _, body) = sql(&page, "SELECT id FROM old WHERE name = 'c'").await;
    assert_eq!(body.lines().nth(1), Some("3"), "step 4b: the identity counter came over too, so the next id is 3: {body}");
    page.close().await;

    // 4c. A kept table with two rows under one key, which the old engine let in: the core refuses the key, so the table is imported
    // without its rules and listed by `unread` with the refusal's words, and its file goes. A page closed after that import and before
    // the file went finds the table with its rows, and imports nothing again. The dropped table keeps no copy for UNDROP to bring back.
    let dup_create = "CREATE TABLE dup (id INT PRIMARY KEY, name TEXT)";
    let dup: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    put_bytes(dup.as_ref(), "tables/dup.arrow", kept_table_file("dup", dup_create, &[(1, "a"), (1, "b")])).await;
    let mut page = Page::open_lake(&Options { store: Some(dup.clone()), ..Default::default() }).await.expect("step 4c: the kept table with a duplicate key opens");
    let unread = page.unread().to_vec();
    assert_eq!(unread.len(), 1, "step 4c: the table is listed once: {unread:?}");
    assert_eq!(unread[0].0, "tables/dup.arrow", "step 4c: the listed file is the table's: {unread:?}");
    assert!(unread[0].1.starts_with("imported without its rules: ") && unread[0].1.contains("duplicate key"), "step 4c: listed with the refusal's words: {unread:?}");
    assert!(left_kept(dup.as_ref()).await.is_empty(), "step 4c: the file goes once the table is imported");
    let (_, _, body) = sql(&page, "SELECT count(*) FROM dup").await;
    assert_eq!(body.lines().nth(1), Some("2"), "step 4c: its two rows came in: {body}");
    page.close().await;

    put_bytes(dup.as_ref(), "tables/dup.arrow", kept_table_file("dup", dup_create, &[(1, "a"), (1, "b")])).await;
    let mut page = Page::open_lake(&Options { store: Some(dup.clone()), ..Default::default() }).await.expect("step 4c: a page opens after a close that left the file");
    assert!(page.unread().is_empty(), "step 4c: the table is there with its rows, so the file goes unlisted: {:?}", page.unread());
    assert!(left_kept(dup.as_ref()).await.is_empty(), "step 4c: the file is gone again");
    let (_, _, body) = sql(&page, "SELECT count(*) FROM dup").await;
    assert_eq!(body.lines().nth(1), Some("2"), "step 4c: no rows came in again, so the table still has its 2: {body}");
    let (status, _, body) = sql(&page, "INSERT INTO dup VALUES (1, 'c')").await;
    assert_eq!(status, 200, "step 4c: the key did not come over, so a second 1 goes in: {body}");
    page.close().await;

    let mut page = Page::open_lake(&Options { store: Some(dup.clone()), ..Default::default() }).await.expect("step 4c: the page opens a third time");
    assert!(page.unread().is_empty(), "step 4c: nothing is left to import: {:?}", page.unread());
    let (_, _, body) = sql(&page, "SELECT count(*) FROM dup").await;
    assert_eq!(body.lines().nth(1), Some("3"), "step 4c: the table has its 3, as nothing came in again: {body}");
    let (status, _, body) = sql(&page, "DROP TABLE dup PURGE").await;
    assert_eq!(status, 200, "step 4c: the table is dropped for good: {body}");
    let (_, sqlstate, body) = sql(&page, "UNDROP TABLE dup").await;
    assert_eq!(sqlstate.as_deref(), Some("42P01"), "step 4c: the import kept no dropped copy to UNDROP: {body}");
    page.close().await;

    // The same refusal where the CREATE is made and the page closed before its INSERT: the next open finds the table with no rows, and
    // the refusal imports it without its rules.
    let crashed: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut page = Page::open_lake(&Options { store: Some(crashed.clone()), ..Default::default() }).await.expect("step 4c: a page opens on an empty store");
    let (status, _, body) = sql(&page, dup_create).await;
    assert_eq!(status, 200, "step 4c: the CREATE, with no rows: {body}");
    page.close().await;
    put_bytes(crashed.as_ref(), "tables/dup.arrow", kept_table_file("dup", dup_create, &[(1, "a"), (1, "b")])).await;
    let mut page = Page::open_lake(&Options { store: Some(crashed.clone()), ..Default::default() }).await.expect("step 4c: the page opens with the file beside its empty table");
    assert_eq!(page.unread().len(), 1, "step 4c: the refusal is listed: {:?}", page.unread());
    assert!(left_kept(crashed.as_ref()).await.is_empty(), "step 4c: the file goes");
    let (_, _, body) = sql(&page, "SELECT count(*) FROM dup").await;
    assert_eq!(body.lines().nth(1), Some("2"), "step 4c: the rows came in without the key: {body}");
    page.close().await;

    // 5. One page at a time in a process: a second, opened while the first is open, is refused by name. (Its SQLSTATE, 55006,
    // is checked where the code is visible: node.rs's `a_claimed_page_refuses_a_second_claim_by_55006`.)
    let mut first = Page::open_lake(&Options::default()).await.expect("step 5: a page opens");
    let refused = Page::open_lake(&Options::default()).await.err().expect("step 5: a second page is refused while the first is open");
    let words = format!("{refused:#}");
    assert!(words.contains("a page is open in this module already"), "step 5: the refusal names the page that is open: {words}");
    first.close().await;

    // 6. A kept page's lake takes its term at once: its page reloaded without a close, so the term is still marked alive. The
    // next page on the same store opens at once, not after that mark goes stale (`members::Alone::lead` claims and marks it).
    let kept: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let (_, marks) = pondra::members::Alone::lead(&kept).await.expect("step 6: a page's term is claimed and marked alive");
    marks.abort(); // (the page never closed: its term is not released, and its last mark is fresh)
    let started = std::time::Instant::now();
    let mut page = Page::open_lake(&Options { store: Some(kept.clone()), ..Default::default() }).await.expect("step 6: a page opens on the store its old page left alive");
    assert!(started.elapsed() < std::time::Duration::from_secs(5), "step 6: it opens at once: {:?}", started.elapsed());
    let (status, _, body) = sql(&page, "SELECT 1").await;
    assert_eq!(status, 200, "step 6: SELECT 1 answers: {body}");
    page.close().await;

    // 7. A claimed page: a second claim is refused while the first lives. The first answers on its first request, which opens its
    // member. Once the first is dropped (no close), a third claim succeeds, and its first request waits for the dropped member's
    // close, then answers.
    let one = Page::claim(&Options::default()).expect("step 7: a page is claimed");
    let refused = Page::claim(&Options::default()).err().expect("step 7: a second claim is refused while the first lives");
    let words = format!("{refused:#}");
    assert!(words.contains("a page is open in this module already"), "step 7: the refusal names the page that is open: {words}");
    let (status, _, body) = sql(&one, "SELECT 1").await;
    assert_eq!((status, body.lines().nth(1)), (200, Some("1")), "step 7: the first page answers SELECT 1: {body}");
    drop(one);
    let mut third = Page::claim(&Options::default()).expect("step 7: a claim is made once the first page is dropped");
    let (status, _, body) = sql(&third, "SELECT 1").await;
    assert_eq!((status, body.lines().nth(1)), (200, Some("1")), "step 7: the third page answers SELECT 1 once the dropped member has closed: {body}");

    // 8. A claimed page answers a route it doesn't have with 404.
    let answer = third.request("GET", "/nothing", &[], bytes::Bytes::new()).await;
    assert_eq!(answer.status, 404, "step 8: GET /nothing is 404: {}", String::from_utf8_lossy(&answer.body));

    // 9. A file's PUT answers its version as an ETag, as GET does, and the next PUT names it in If-Match; If-Match on a file
    // that isn't there is 412, as for a file changed since it was read.
    let put = |path: &'static str, if_match: Option<String>, body: &'static str| {
        let headers: Vec<(String, String)> = if_match.into_iter().map(|v| ("if-match".to_string(), v)).collect();
        let page = &third;
        async move { page.request("PUT", path, &headers, bytes::Bytes::from_static(body.as_bytes())).await }
    };
    let first = put("/files/e.csv", None, "a\n1\n").await;
    let etag = first.header("etag").map(str::to_string).expect("step 9: a PUT answers an ETag");
    let read = third.request("GET", "/files/e.csv", &[], bytes::Bytes::new()).await;
    assert_eq!(read.header("etag"), Some(etag.as_str()), "step 9: GET's ETag is the one PUT answered");
    let again = put("/files/e.csv", Some(etag), "a\n2\n").await;
    assert_eq!(again.status, 200, "step 9: a PUT with the ETag it read: {}", String::from_utf8_lossy(&again.body));
    let gone = put("/files/nothere.csv", Some("\"v\"".into()), "a\n").await;
    assert_eq!(gone.status, 412, "step 9: If-Match on a file that isn't there: {}", String::from_utf8_lossy(&gone.body));
    third.close().await;
}

/// The 0.36 fixture (`tests/fixtures/page-0.36`: the `types/`, `tables/` and `views/` files the old engine wrote), put into `to`,
/// the lake's store, under the same paths.
async fn fixture_into(to: &dyn object_store::ObjectStore) {
    fn walk(folder: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(folder).expect("the fixture folder reads") {
            let path = entry.expect("a fixture entry").path();
            if path.is_dir() {
                walk(&path, found);
            } else {
                found.push(path);
            }
        }
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/page-0.36");
    let mut files = vec![];
    walk(&root, &mut files);
    assert!(!files.is_empty(), "the fixture holds the kept files");
    for file in files {
        let at = file.strip_prefix(&root).expect("a fixture file under the fixture folder").to_string_lossy().replace('\\', "/");
        let bytes = std::fs::read(&file).expect("a fixture file reads");
        object_store::ObjectStoreExt::put(to, &object_store::path::Path::from(at.as_str()), object_store::PutPayload::from(bytes)).await.expect("the lake's folder takes it");
    }
}

/// A kept table's file as 0.36 wrote it, made here for a table the old engine would not have let in: an Arrow IPC file of `rows` (an
/// `id` and a `name`), with the notes `pondra.table` (its name) and `pondra.create` (its CREATE TABLE text).
fn kept_table_file(name: &str, create: &str, rows: &[(i32, &str)]) -> Vec<u8> {
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::writer::FileWriter;
    let notes = std::collections::HashMap::from([("pondra.table".to_string(), name.to_string()), ("pondra.create".to_string(), create.to_string())]);
    let schema = Arc::new(Schema::new_with_metadata(vec![Field::new("id", DataType::Int32, false), Field::new("name", DataType::Utf8, true)], notes));
    let ids = Int32Array::from(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>());
    let names = StringArray::from(rows.iter().map(|(_, name)| *name).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).expect("the rows fit the schema");
    let mut bytes = vec![];
    let mut writer = FileWriter::try_new(&mut bytes, &schema).expect("an Arrow file writer");
    writer.write(&batch).expect("the batch is written");
    writer.finish().expect("the file is finished");
    drop(writer);
    bytes
}

/// `bytes` put into `to` at `at`, where a kept file is put.
async fn put_bytes(to: &dyn object_store::ObjectStore, at: &str, bytes: Vec<u8>) {
    object_store::ObjectStoreExt::put(to, &object_store::path::Path::from(at), object_store::PutPayload::from(bytes)).await.expect("the lake's folder takes it");
}

/// What is left in the kept folders of a store (`tables/`, `types/`, `views/`): each object's path.
async fn left_kept(store: &dyn object_store::ObjectStore) -> Vec<String> {
    use futures::TryStreamExt as _;
    let mut left = vec![];
    for folder in ["tables", "types", "views"] {
        let listed = store.list(Some(&object_store::path::Path::from(folder))).try_collect::<Vec<_>>().await.expect("the folder lists");
        left.extend(listed.into_iter().map(|o| o.location.to_string()));
    }
    left
}
