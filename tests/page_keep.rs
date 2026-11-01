//! A `CREATE TABLE … AS FROM 'f.parquet'` on a page keeps the file's row groups (`keep_load`): the table's files are the file's
//! own column chunks, Snappy, with no Arrow schema and the source's row groups, and its answers are a written table's. A
//! `WHERE` makes the same copy as a written table (never kept), and DELETE works on a kept table as on a written one.
//! `cargo test --test page_keep`.
#![cfg(feature = "server")]
#![recursion_limit = "256"] // (as the program's: a node's futures are deep)
use datafusion::parquet::file::reader::FileReader;
use object_store::ObjectStoreExt as _;
use pondra::page::{Options, Page};
use std::sync::Arc;

/// `text` as SQL on `page` (`POST /sql?format=csv`): the status, the `x-pondra-sqlstate` header, and the body as text.
async fn sql(page: &Page, text: &str) -> (u16, Option<String>, String) {
    let answer = page.request("POST", "/sql?format=csv", &[], bytes::Bytes::copy_from_slice(text.as_bytes())).await;
    (answer.status, answer.header("x-pondra-sqlstate").map(str::to_string), String::from_utf8_lossy(&answer.body).into_owned())
}

#[test]
fn a_bare_scan_of_a_parquet_file_is_kept_and_answers_as_written() {
    // (a page's planning needs more than a test thread's 2 MB: as page_lake's)
    std::thread::Builder::new().stack_size(32 << 20).spawn(run).unwrap().join().unwrap();
}

fn run() {
    let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(32 << 20).enable_all().build().unwrap();
    runtime.block_on(steps());
}

/// A Parquet file of three row groups of 1,000 rows, Snappy: (i BIGINT with a NULL in a third of its rows, s TEXT unique,
/// f DOUBLE, d DATE).
fn parquet_file() -> Vec<u8> {
    use datafusion::arrow::array::{ArrayRef, Date32Array, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::properties::WriterProperties;
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("s", DataType::Utf8, false),
        Field::new("f", DataType::Float64, false),
        Field::new("d", DataType::Date32, false),
    ]));
    let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
    let mut w = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).expect("a writer");
    for k in 0..3i64 {
        let n: Vec<i64> = (k * 1000..k * 1000 + 1000).collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter(n.iter().map(|&v| (v % 3 != 1).then_some(v)))),
            Arc::new(StringArray::from_iter_values(n.iter().map(|v| format!("row{v}")))),
            Arc::new(Float64Array::from_iter_values(n.iter().map(|&v| v as f64 / 4.0))),
            Arc::new(Date32Array::from_iter_values(n.iter().map(|&v| (v % 20_000) as i32))),
        ];
        w.write(&RecordBatch::try_new(schema.clone(), columns).expect("a batch")).expect("a write");
        w.flush().expect("a row group"); // (a row group per batch)
    }
    w.finish().expect("the footer");
    std::mem::take(w.inner_mut())
}

/// The files under `prefix` in `store`, as bytes.
async fn files_of(store: &Arc<dyn object_store::ObjectStore>, prefix: &str) -> Vec<bytes::Bytes> {
    use futures::TryStreamExt;
    let listed: Vec<object_store::ObjectMeta> = store.list(Some(&object_store::path::Path::from(prefix))).try_collect().await.expect("the listing");
    let mut out = vec![];
    for m in listed {
        out.push(store.get(&m.location).await.expect("a file").bytes().await.expect("its bytes"));
    }
    out
}

/// The steps, in order: each one's assert message names it.
async fn steps() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut page = Page::open_lake(&Options { store: Some(store.clone()), ..Default::default() }).await.expect("a page opens on the store it is given");

    // 1. The file is the lake's own, and two tables are made from it: `k` (a bare scan, kept) and `w` (a WHERE, written as today).
    let put = page.request("PUT", "/files/f.parquet", &[], bytes::Bytes::from(parquet_file())).await;
    assert!((200..300).contains(&put.status), "step 1: the file is put: {}", String::from_utf8_lossy(&put.body));
    for text in ["CREATE TABLE k AS FROM 'f.parquet'", "CREATE TABLE w AS SELECT * FROM 'f.parquet' WHERE true"] {
        let (status, _, body) = sql(&page, text).await;
        assert_eq!(status, 200, "step 1: {text}: {body}");
    }

    // 2. The answers are the same: every row, its counts and sums, and the ranges and sketches a filter uses.
    let (_, _, k_rows) = sql(&page, "SELECT i, s, f, d FROM k ORDER BY s").await;
    let (_, _, w_rows) = sql(&page, "SELECT i, s, f, d FROM w ORDER BY s").await;
    assert_eq!(k_rows, w_rows, "step 2: k's rows are w's");
    assert_eq!(k_rows.lines().count(), 3001, "step 2: the header and 3000 rows");
    for q in [
        "SELECT count(*), count(i), sum(i), sum(f), min(d), max(d) FROM {}",
        "SELECT count(*) FROM {} WHERE s = 'row1500'",
        "SELECT count(*) FROM {} WHERE i BETWEEN 2500 AND 2600",
        "SELECT count(*) FROM {} WHERE i IS NULL",
        "SELECT count(*) FROM {} WHERE f > 2000.0",
    ] {
        // (the header names the table, as `count(k.i)` or `count(w.i)`: the answer rows are compared)
        let (_, _, k) = sql(&page, &q.replace("{}", "k")).await;
        let (_, _, w) = sql(&page, &q.replace("{}", "w")).await;
        assert_eq!(k.lines().skip(1).collect::<Vec<_>>(), w.lines().skip(1).collect::<Vec<_>>(), "step 2: {q}");
    }

    // 3. The kept files are the file's own column chunks: Snappy, the source's three row groups, no Arrow schema, and ids 1..4 by
    // place. The written table's are LZ4 (our codec) as it always was.
    let kept = files_of(&store, "data/k").await;
    let written = files_of(&store, "data/w").await;
    assert!(!kept.is_empty(), "step 3: k has files");
    let mut kept_rows = 0;
    for bytes in &kept {
        let reader = datafusion::parquet::file::reader::SerializedFileReader::new(bytes.clone()).expect("step 3: a kept file reads");
        let md = reader.metadata();
        kept_rows += md.file_metadata().num_rows();
        let keys: Vec<String> = md.file_metadata().key_value_metadata().map(|kv| kv.iter().map(|k| k.key.clone()).collect()).unwrap_or_default();
        assert!(!keys.iter().any(|k| k == "ARROW:schema"), "step 3: no Arrow schema in a kept footer: {keys:?}");
        let ids: Vec<i32> = md.file_metadata().schema_descr().root_schema().get_fields().iter().map(|f| f.get_basic_info().id()).collect();
        assert_eq!(ids, [1, 2, 3, 4], "step 3: our field ids by place");
        for rg in md.row_groups() {
            for c in rg.columns() {
                assert_eq!(c.compression(), datafusion::parquet::basic::Compression::SNAPPY, "step 3: the file's codec is kept");
            }
        }
    }
    assert_eq!(kept_rows, 3000, "step 3: the kept files hold the file's rows");
    let row_groups: usize = kept.iter().map(|b| datafusion::parquet::file::reader::SerializedFileReader::new(b.clone()).expect("a file").metadata().num_row_groups()).sum();
    assert_eq!(row_groups, 3, "step 3: the source's row groups, copied");
    for bytes in &written {
        let md = datafusion::parquet::file::reader::SerializedFileReader::new(bytes.clone()).expect("step 3: a written file reads");
        for c in md.metadata().row_groups().iter().flat_map(|rg| rg.columns()) {
            assert_eq!(c.compression(), datafusion::parquet::basic::Compression::LZ4_RAW, "step 3: a written file keeps its own codec");
        }
    }

    // 4. A kept table changes as a written one does: the same DELETE and UPDATE, the same answers after.
    for text in ["DELETE FROM {} WHERE s = 'row5'", "UPDATE {} SET f = -1.0 WHERE i BETWEEN 10 AND 20"] {
        for t in ["k", "w"] {
            let (status, _, body) = sql(&page, &text.replace("{}", t)).await;
            assert_eq!(status, 200, "step 4: {t}: {body}");
        }
    }
    let (_, _, k) = sql(&page, "SELECT count(*), sum(f) FROM k").await;
    let (_, _, w) = sql(&page, "SELECT count(*), sum(f) FROM w").await;
    assert_eq!(k.lines().skip(1).collect::<Vec<_>>(), w.lines().skip(1).collect::<Vec<_>>(), "step 4: after a DELETE and an UPDATE, the same answers");
    assert!(k.lines().nth(1).is_some_and(|row| row.starts_with("2999,")), "step 4: one row of the 3000 is deleted: {k}");

    page.close().await;
}
