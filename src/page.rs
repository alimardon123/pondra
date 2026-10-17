//! A lake-less engine: the tables its caller gives it, SQL through the node's own front, answers in the node's
//! own bytes, so a page and a node answer alike. The page form's core (designs/l2-b4-web.md); `web/` wraps it.

use crate::expander::Routine;
use crate::ext_names::Attached;
use crate::front::{Host, Own};
use datafusion::arrow::ipc::reader::{FileReader, StreamReader};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::arrow::{csv, json};
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::Statement;
use std::collections::{BTreeSet, HashMap};
use std::io::Cursor;
use std::sync::Arc;

const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json";
const FILES: &str = "/files/";

/// How a page's session is made: its target partitions (a page in a browser has one).
pub struct Options {
    pub partitions: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options { partitions: 1 }
    }
}

/// A page: a session of its own, whose tables are the files its caller put in it (in its memory, nowhere else).
pub struct Page {
    ctx: SessionContext,
}

/// What a request answers: its status, its content type, the SQLSTATE of an error (none for an answer), its body.
pub struct Answer {
    pub status: u16,
    pub content_type: &'static str,
    pub sqlstate: Option<&'static str>,
    pub body: bytes::Bytes,
}

impl Answer {
    fn ok(content_type: &'static str, body: impl Into<bytes::Bytes>) -> Answer {
        Answer { status: 200, content_type, sqlstate: None, body: body.into() }
    }
}

/// An error as an answer: its SQLSTATE, the status HTTP's door would give it (`codes::http`), its words as the body.
fn failed(e: anyhow::Error) -> Answer {
    let code = crate::codes::of(&e);
    Answer { status: crate::codes::http(code), content_type: TEXT, sqlstate: Some(code), body: crate::codes::words(&e).into() }
}

impl Page {
    /// A page with no tables, its session a node's (`engine::session`) with the partitions asked for.
    pub fn open(options: &Options) -> Page {
        let config = SessionConfig::new().with_target_partitions(options.partitions.max(1));
        Page { ctx: crate::engine::session(config, Arc::new(RuntimeEnv::default())) }
    }

    /// One request: `path`'s route, and its `format=` for an answer (`answer::render`). Every failure is an answer too,
    /// with its SQLSTATE, as a node's door answers it.
    pub async fn request(&self, method: &str, path: &str, body: bytes::Bytes) -> Answer {
        let (route, query) = path.split_once('?').unwrap_or((path, ""));
        let format = query.split('&').find_map(|kv| kv.split_once('=').filter(|(k, _)| *k == "format").map(|(_, v)| v));
        let answer = match (method, route) {
            ("GET", "/ready") => return Answer::ok(TEXT, "ready"),
            ("POST", "/sql") => match std::str::from_utf8(&body) {
                Ok(text) => self.sql(text, format).await,
                Err(e) => Err(crate::codes::coded("22P02", format!("the request's body can't be read: {e}"))),
            },
            ("GET", "/tables") => self.tables(),
            ("PUT", _) if route.starts_with(FILES) => self.put(&route[FILES.len()..], body).await,
            _ => {
                let mut answer = failed(crate::codes::coded("0A000", format!("a page has no route {method} {route}")));
                answer.status = 404;
                return answer;
            }
        };
        answer.unwrap_or_else(failed)
    }

    /// SQL as a node reads it: the front, then the node's planning (queries only), then the rows in `format`.
    async fn sql(&self, text: &str, format: Option<&str>) -> anyhow::Result<Answer> {
        let expanded = crate::front::expand(&PageHost(&self.ctx), text, &HashMap::new()).await?;
        let df = crate::engine::sql(&self.ctx, &expanded).await?;
        let schema = df.schema().inner().clone();
        let mut batches = df.collect().await?;
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(schema)); // (an answer with no rows keeps its columns: invariant 88)
        }
        Ok(Answer::ok(crate::answer::content_type(format), crate::answer::render(&batches, format, |_| None)?))
    }

    /// The names of the page's tables, sorted.
    fn tables(&self) -> anyhow::Result<Answer> {
        let mut names = self.ctx.catalog("datafusion").and_then(|c| c.schema("public")).map(|s| s.table_names()).unwrap_or_default();
        names.sort();
        Ok(Answer::ok(JSON, serde_json::to_vec(&names)?))
    }

    /// A file made a table, named by its stem and read by its extension. A table of the same name is replaced.
    async fn put(&self, name: &str, body: bytes::Bytes) -> anyhow::Result<Answer> {
        let (stem, ext) = name.rsplit_once('.').unwrap_or((name, ""));
        let (stem, ext) = (stem.to_lowercase(), ext.to_lowercase());
        let reader: Box<dyn RecordBatchReader + '_> = match ext.as_str() {
            "csv" => {
                let (schema, _) = csv::reader::Format::default().with_header(true).infer_schema(&body[..], Some(1000))?;
                Box::new(csv::ReaderBuilder::new(Arc::new(schema)).with_header(true).build(&body[..])?)
            }
            "json" | "ndjson" => {
                let (schema, _) = json::reader::infer_json_schema(&body[..], Some(1000))?;
                Box::new(json::ReaderBuilder::new(Arc::new(schema)).build(&body[..])?)
            }
            "parquet" => Box::new(ParquetRecordBatchReaderBuilder::try_new(body.clone())?.build()?),
            "arrow" => match FileReader::try_new(Cursor::new(body.clone()), None) {
                Ok(r) => Box::new(r),
                Err(_) => Box::new(StreamReader::try_new(&body[..], None)?), // (an IPC stream, not a file)
            },
            _ => return Err(crate::codes::coded("22023", format!("{name}: a page takes .csv, .json, .ndjson, .parquet and .arrow files"))),
        };
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // (a bare name, so a stem with a dot is one table's name and not a schema's)
        let table = TableReference::bare(stem.clone());
        self.ctx.deregister_table(table.clone())?;
        self.ctx.register_table(table, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
        Ok(Answer::ok(JSON, serde_json::to_vec(&serde_json::json!({"table": stem, "rows": rows}))?))
    }
}

/// What a page's SQL asks of where it runs. A page has no routines, no attached lakes, no views with measures and no
/// types or spellings of its own, so each of those answers "none"; the columns are its tables' own.
struct PageHost<'a>(&'a SessionContext);

impl Host for PageHost<'_> {
    fn name(&self) -> String {
        "page".to_string()
    }

    async fn own(&self, sql: &str) -> anyhow::Result<Own> {
        Ok(Own::Go(sql.to_string()))
    }

    async fn routines(&self) -> anyhow::Result<Arc<HashMap<String, Routine>>> {
        Ok(Arc::default())
    }

    async fn attached(&self) -> anyhow::Result<Vec<(String, Attached)>> {
        Ok(vec![])
    }

    async fn columns(&self, tables: BTreeSet<String>) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let mut out = HashMap::new();
        for name in tables {
            if let Ok(t) = self.0.table_provider(name.as_str()).await {
                out.insert(name, t.schema().fields().iter().map(|f| f.name().clone()).collect());
            }
        }
        Ok(out)
    }

    async fn measured(&self, _sql: &str) -> anyhow::Result<bool> {
        Ok(false) // (a page keeps no views, so none with measures)
    }

    async fn measures(&self, _sql: &str, _stmts: &mut [Statement]) -> anyhow::Result<bool> {
        Ok(false) // (nor measures)
    }

    async fn types(&self, _stmts: &mut [Statement]) -> anyhow::Result<bool> {
        Ok(false) // (nor types of its own)
    }

    async fn friendly(&self, _stmts: &mut [Statement]) -> anyhow::Result<bool> {
        Ok(false) // (its probes: piece 4d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::ipc::reader::StreamReader;

    const ORDERS: &str = "id,name,qty\n1,a,5\n2,b,7\n3,c,9\n";
    const NAMES: &str = "id,label\n1,x\n3,z\n";

    /// A page with `orders` and `names` put in it, as a client puts files.
    async fn page_with_tables() -> Page {
        let page = Page::open(&Options::default());
        for (name, csv) in [("orders.csv", ORDERS), ("names.csv", NAMES)] {
            assert_eq!(page.request("PUT", &format!("/files/{name}"), csv.into()).await.status, 200);
        }
        page
    }

    /// The bytes `answer::render` gives the rows of `sql`, run on the page's session alone (no front).
    async fn rendered(page: &Page, sql: &str, format: Option<&str>) -> bytes::Bytes {
        let batches = page.ctx.sql(sql).await.unwrap().collect().await.unwrap();
        crate::answer::render(&batches, format, |_| None).unwrap()
    }

    #[tokio::test]
    async fn a_file_is_a_table() {
        let page = Page::open(&Options::default());
        let put = page.request("PUT", "/files/orders.csv", ORDERS.into()).await;
        assert_eq!((put.status, put.content_type), (200, "application/json"));
        assert_eq!(put.body, r#"{"table":"orders","rows":3}"#);
    }

    #[tokio::test]
    async fn from_first_goes_through_the_front() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=csv", "FROM orders WHERE qty > 5".into()).await;
        assert_eq!(got.status, 200);
        assert_eq!(got.body, rendered(&page, "SELECT * FROM orders WHERE qty > 5", Some("csv")).await);
    }

    #[tokio::test]
    async fn a_join_is_answered_as_json() {
        let page = page_with_tables().await;
        let sql = "SELECT o.id, n.label FROM orders o JOIN names n ON o.id = n.id ORDER BY o.id";
        let got = page.request("POST", "/sql", sql.into()).await;
        assert_eq!(got.body, rendered(&page, sql, None).await);
    }

    #[tokio::test]
    async fn exclude_is_planned_as_a_node_plans_it() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql", "SELECT * EXCLUDE (name) FROM orders ORDER BY id".into()).await;
        assert_eq!(got.body, rendered(&page, "SELECT id, qty FROM orders ORDER BY id", None).await);
    }

    #[tokio::test]
    async fn a_json_union_is_planned_as_text() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=csv", r#"SELECT '{"a": 7}' -> 'a' AS v"#.into()).await;
        assert_eq!(got.body, "v\n7\n");
    }

    #[tokio::test]
    async fn an_answer_with_no_rows_keeps_its_columns() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=arrow", "FROM orders WHERE qty > 100".into()).await;
        assert_eq!(got.content_type, "application/vnd.apache.arrow.stream");
        let reader = StreamReader::try_new(&got.body[..], None).unwrap();
        assert_eq!(reader.schema().fields().len(), 3);
        assert_eq!(reader.map(|b| b.unwrap().num_rows()).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn errors_carry_their_sqlstate() {
        let page = page_with_tables().await;
        let syntax = page.request("POST", "/sql", "SELEC 1".into()).await;
        assert_eq!((syntax.status, syntax.sqlstate), (400, Some("42601")));
        let ddl = page.request("POST", "/sql", "CREATE TABLE t (a INT)".into()).await;
        assert_eq!(ddl.sqlstate, Some("0A000"));
        let route = page.request("GET", "/nowhere", bytes::Bytes::new()).await;
        assert_eq!((route.status, route.sqlstate), (404, Some("0A000")));
    }

    #[tokio::test]
    async fn tables_are_listed_sorted() {
        let page = page_with_tables().await;
        let got = page.request("GET", "/tables", bytes::Bytes::new()).await;
        assert_eq!(got.body, r#"["names","orders"]"#);
    }
}
