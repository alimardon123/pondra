//! A lake-less engine: the tables its caller gives it, SQL through the node's own front, answers in the node's
//! own bytes, so a page and a node answer alike. The page form's core (designs/l2-b4-web.md); `web/` wraps it.
//! The files it is given are also read by name (`FROM 'x.csv'`, `read_parquet('x.parquet')`). DuckDB's forms that
//! ask the data (`PIVOT`, `COLUMNS`, `SUMMARIZE`) and ASOF JOIN answer as on a node.

use crate::expander::Routine;
use crate::ext_names::Attached;
use crate::front::{Host, Own};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList, SchemaProvider, TableProvider};
use datafusion::arrow::ipc::reader::{FileReader, StreamReader};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::arrow::{csv, json};
use datafusion::common::TableReference;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::MemTable;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt, PutPayload};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::Statement;
use futures::TryStreamExt;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::io::Cursor;
use std::sync::{Arc, PoisonError};

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
    files: Arc<InMemory>, // (the files as they were put, by name: what `FROM 'x.csv'` reads)
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
        let runtime = RuntimeEnvBuilder::new()
            .with_cache_manager(CacheManagerConfig::default().with_list_files_cache_limit(0)) // (a file put again is listed again)
            .build_arc()
            .expect("a runtime");
        let ctx = crate::engine::session(config, runtime);
        let files = Arc::new(InMemory::new());
        let url = ObjectStoreUrl::parse("mem://").expect("a URL");
        ctx.register_object_store(url.as_ref(), files.clone());
        Page { ctx, files }
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
            ("GET", "/files") => self.files_given().await,
            ("PUT", _) if route.starts_with(FILES) => self.put(&route[FILES.len()..], body).await,
            _ => {
                let mut answer = failed(crate::codes::coded("0A000", format!("a page has no route {method} {route}")));
                answer.status = 404;
                return answer;
            }
        };
        answer.unwrap_or_else(failed)
    }

    /// SQL as a node reads it: the front, then the rows of the files it names by name, planned (`plan`), then the
    /// rows in `format` (an answer with no rows keeps its columns: invariant 88).
    async fn sql(&self, text: &str, format: Option<&str>) -> anyhow::Result<Answer> {
        let df = self.plan(text).await?;
        let schema = df.schema().inner().clone();
        let mut batches = df.collect().await?;
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(schema)); // (an answer with no rows keeps its columns: invariant 88)
        }
        Ok(Answer::ok(crate::answer::content_type(format), crate::answer::render(&batches, format, |_| None)?))
    }

    /// A statement read as a node reads it: the front, ASOF JOIN's marker, then planned on the statement's own copy of
    /// the page's session (`statement`), with the files it names given to that copy alone (`given`). Nothing a
    /// statement registers reaches the page or another statement, so requests at once share no table and none is
    /// taken away from one another.
    async fn plan(&self, text: &str) -> anyhow::Result<DataFrame> {
        let expanded = crate::front::expand(&PageHost(self), text, &HashMap::new()).await?;
        let expanded = crate::asof::as_of(&expanded)?.into_owned();
        let (ctx, overlay) = self.statement();
        self.given(&overlay, &expanded).await?;
        crate::engine::sql(&ctx, &expanded).await
    }

    /// A statement's session: a copy of the page's, whose default schema is an `Overlay` over the page's own, so the
    /// files the statement names are its alone. The copy's state is the page's, cloned: its functions, settings and
    /// object store come with it, so nothing is registered again.
    fn statement(&self) -> (SessionContext, Arc<Overlay>) {
        let mut state = self.ctx.state();
        let (catalog, schema) = (state.config().options().catalog.default_catalog.clone(), state.config().options().catalog.default_schema.clone());
        let page = self.ctx.catalog(&catalog).and_then(|c| c.schema(&schema)).expect("a page has its default schema");
        let overlay = Arc::new(Overlay { page, own: Default::default() });
        let default = MemoryCatalogProvider::new();
        default.register_schema(&schema, overlay.clone()).expect("a new catalog takes its schema");
        let catalogs = MemoryCatalogProviderList::new();
        CatalogProviderList::register_catalog(&catalogs, catalog, Arc::new(default));
        state.register_catalog_list(Arc::new(catalogs));
        (SessionContext::new_with_state(state), overlay)
    }

    /// The files the statement names (the front wrote them as `ext:` names), each a table of the statement's own copy
    /// (`overlay`), read as a node reads it. A file refused stops the statement, and its copy goes with it.
    async fn given(&self, overlay: &Overlay, sql: &str) -> anyhow::Result<()> {
        for name in crate::ext_names::names(sql) {
            let Some(spec) = crate::ext_names::spec(&name) else { continue }; // (not a file's name: the planner answers 42P01 for it)
            self.register(overlay, &name, &spec).await?;
        }
        Ok(())
    }

    /// One file a statement names, as a table of its own copy under its `ext:` name: refused unless a page reads it
    /// (its format, its options, its URL) and a file of that path was given, and given to the statement only once its
    /// columns are known.
    async fn register(&self, overlay: &Overlay, name: &str, spec: &crate::ext_names::Spec) -> anyhow::Result<()> {
        use crate::ext_names::CSV_OPTIONS;
        if !["parquet", "csv", "json", "arrow"].contains(&spec.format.as_str()) {
            return Err(crate::codes::coded("0A000", format!("a page reads Parquet, CSV, JSON and Arrow files given to it, not {} tables", spec.format)));
        }
        if let Some(k) = spec.options.keys().find(|k| !(spec.format == "csv" && CSV_OPTIONS.contains(&k.as_str()))) {
            return Err(crate::codes::coded("0A000", format!("a page doesn't take {k} yet")));
        }
        let mut urls = vec![];
        for url in &spec.urls {
            if url.contains("://") {
                return Err(crate::codes::coded("0A000", format!("{url}: a page reads the files given to it (PUT /files/{{name}}); URLs come later")));
            }
            let path = url.strip_prefix("./").unwrap_or(url);
            urls.push((path, crate::ext_names::listing_url(&format!("mem:///{path}"))?));
        }
        let state = self.ctx.state();
        for (path, url) in &urls {
            // (every URL must find a file before the table is made: its schema is read from the first, and a missing one is a plan error)
            if url.list_all_files(&state, self.files.as_ref(), "").await?.try_next().await?.is_none() {
                return Err(crate::codes::coded("42P01", format!("no file {path} was given to this page (PUT /files/{{name}})")));
            }
        }
        let urls: Vec<_> = urls.into_iter().map(|(_, url)| url).collect();
        let options = ListingOptions::new(crate::ext_names::file_format(spec)?).with_file_extension("");
        let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options).infer_schema(&state).await?;
        overlay.own(name.to_string(), Arc::new(ListingTable::try_new(config)?)); // (a name is one table: the statement's own map)
        Ok(())
    }

    /// The names of the page's tables, sorted.
    fn tables(&self) -> anyhow::Result<Answer> {
        let mut names = self.ctx.catalog("datafusion").and_then(|c| c.schema("public")).map(|s| s.table_names()).unwrap_or_default();
        names.sort();
        Ok(Answer::ok(JSON, serde_json::to_vec(&names)?))
    }

    /// The names of the files given, sorted: what `FROM 'x.csv'` can read.
    async fn files_given(&self) -> anyhow::Result<Answer> {
        let metas: Vec<_> = self.files.list(None).try_collect().await?;
        let mut names: Vec<String> = metas.iter().map(|m| m.location.to_string()).collect();
        names.sort();
        Ok(Answer::ok(JSON, serde_json::to_vec(&names)?))
    }

    /// A file made a table, named by its stem and read by its extension. A table of the same name is replaced.
    async fn put(&self, name: &str, body: bytes::Bytes) -> anyhow::Result<Answer> {
        let (stem, ext) = name.rsplit_once('.').unwrap_or((name, ""));
        let (stem, ext) = (stem.to_lowercase(), ext.to_lowercase());
        // (the rows come in the session's batch size, DataFusion's own, not the readers' 1,024: a query's batches are
        // then as big as a node's; an Arrow file's batches are the file's own)
        let size = self.ctx.state().config().batch_size();
        let reader: Box<dyn RecordBatchReader + '_> = match ext.as_str() {
            "csv" => {
                let (schema, _) = csv::reader::Format::default().with_header(true).infer_schema(&body[..], Some(1000))?;
                Box::new(csv::ReaderBuilder::new(Arc::new(schema)).with_header(true).with_batch_size(size).build(&body[..])?)
            }
            "json" | "ndjson" => {
                let (schema, _) = json::reader::infer_json_schema(&body[..], Some(1000))?;
                Box::new(json::ReaderBuilder::new(Arc::new(schema)).with_batch_size(size).build(&body[..])?)
            }
            "parquet" => Box::new(ParquetRecordBatchReaderBuilder::try_new(body.clone())?.with_batch_size(size).build()?),
            "arrow" => match FileReader::try_new(Cursor::new(body.clone()), None) {
                Ok(r) => Box::new(r),
                Err(_) => Box::new(StreamReader::try_new(&body[..], None)?), // (an IPC stream, not a file)
            },
            _ => return Err(crate::codes::coded("22023", format!("{name}: a page takes .csv, .json, .ndjson, .parquet and .arrow files"))),
        };
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        self.files.put(&Path::from(name), PutPayload::from(body.clone())).await?; // (kept as given, now it has been read)
        // (a bare name, so a stem with a dot is one table's name and not a schema's)
        let table = TableReference::bare(stem.clone());
        self.ctx.deregister_table(table.clone())?;
        self.ctx.register_table(table, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
        Ok(Answer::ok(JSON, serde_json::to_vec(&serde_json::json!({"table": stem, "rows": rows}))?))
    }
}

/// A statement's view of its page's default schema: the files the statement names (`own`), then the page's own tables.
/// The page's tables are everyone's, and a statement's files are its own, so requests at once never see or take each
/// other's. A table a statement makes or drops (`register_table`, `deregister_table`) is the page's, as a `PUT` makes it.
#[derive(Debug)]
struct Overlay {
    page: Arc<dyn SchemaProvider>,
    own: std::sync::RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl Overlay {
    /// Adds a table for this statement alone.
    fn own(&self, name: String, table: Arc<dyn TableProvider>) {
        self.own.write().unwrap_or_else(PoisonError::into_inner).insert(name, table);
    }
}

#[async_trait]
impl SchemaProvider for Overlay {
    fn table_names(&self) -> Vec<String> {
        let mut names: BTreeSet<String> = self.page.table_names().into_iter().collect();
        names.extend(self.own.read().unwrap_or_else(PoisonError::into_inner).keys().cloned());
        names.into_iter().collect()
    }

    async fn table(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let own = self.own.read().unwrap_or_else(PoisonError::into_inner).get(name).cloned();
        match own {
            Some(table) => Ok(Some(table)),
            None => self.page.table(name).await,
        }
    }

    fn register_table(&self, name: String, table: Arc<dyn TableProvider>) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        self.page.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        self.page.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.own.read().unwrap_or_else(PoisonError::into_inner).contains_key(name) || self.page.table_exist(name)
    }
}

/// What a page's SQL asks of where it runs. DuckDB's forms that ask the data (`PIVOT`, `COLUMNS`, `SUMMARIZE`) ask the
/// page itself, as a node asks its own. A page has no routines, no attached lakes and no views with measures, so each
/// of those answers "none"; the columns are its tables' own.
struct PageHost<'a>(&'a Page);

impl Host for PageHost<'_> {
    fn name(&self) -> String {
        "page".to_string()
    }

    async fn own(&self, sql: &str) -> anyhow::Result<Own> {
        match crate::friendly::summarize(self, sql).await? {
            Some(q) => Ok(Own::Answer(q)),
            None => Ok(Own::Go(sql.to_string())),
        }
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
            if let Ok(t) = self.0.ctx.table_provider(name.as_str()).await {
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

    async fn friendly(&self, stmts: &mut [Statement]) -> anyhow::Result<bool> {
        crate::friendly::rewrite(self, stmts).await
    }

    // (not an `async fn`: a loop back into the front goes through here, and this declared `Send` is where the
    // compiler's check of that loop stops)
    fn ask(&self, sql: &str) -> impl Future<Output = anyhow::Result<DataFrame>> + Send {
        self.0.plan(sql)
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

    #[tokio::test]
    async fn a_file_is_read_by_name() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=csv", "FROM 'orders.csv' WHERE qty > 5".into()).await;
        assert_eq!(got.status, 200);
        assert_eq!(got.body, rendered(&page, "SELECT * FROM orders WHERE qty > 5", Some("csv")).await);
    }

    #[tokio::test]
    async fn read_csv_takes_its_options_by_name() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=csv", "SELECT count(*) AS n FROM read_csv('orders.csv', header => true, delim => ',')".into()).await;
        assert_eq!(got.body, "n\n3\n");
    }

    #[tokio::test]
    async fn a_glob_reads_every_file_it_matches_and_lists_again() {
        let page = Page::open(&Options::default());
        for (name, csv) in [("part1.csv", "id,qty\n1,5\n"), ("part2.csv", "id,qty\n2,7\n")] {
            assert_eq!(page.request("PUT", &format!("/files/{name}"), csv.into()).await.status, 200);
        }
        let sum = "SELECT sum(qty) AS s FROM 'part*.csv'";
        assert_eq!(page.request("POST", "/sql?format=csv", sum.into()).await.body, "s\n12\n");
        assert_eq!(page.request("PUT", "/files/part3.csv", "id,qty\n3,9\n".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", sum.into()).await.body, "s\n21\n");
    }

    #[tokio::test]
    async fn parquet_is_read_by_name() {
        let page = page_with_tables().await;
        let parquet = page.request("POST", "/sql?format=parquet", "SELECT * FROM orders".into()).await;
        assert_eq!(parquet.status, 200);
        assert_eq!(page.request("PUT", "/files/o.parquet", parquet.body).await.status, 200);
        let got = page.request("POST", "/sql?format=csv", "SELECT sum(qty) AS s FROM read_parquet('o.parquet')".into()).await;
        assert_eq!(got.body, "s\n21\n");
    }

    /// A Parquet file of 20,000 rows (one Int64 column) put as a table is read in the session's batch size, and its
    /// answer is the file's: the batch size is not visible in an answer, so this guards the path.
    #[tokio::test]
    async fn a_parquet_file_of_many_rows_is_read_in_full() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let column = Int64Array::from_iter_values(1..=20_000);
        let mut body = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut body, schema.clone(), None).unwrap();
        writer.write(&RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap()).unwrap();
        writer.close().unwrap();
        let page = Page::open(&Options::default());
        assert_eq!(page.request("PUT", "/files/big.parquet", body.into()).await.status, 200);
        let got = page.request("POST", "/sql?format=csv", "SELECT count(*) AS n, sum(x) AS s FROM big".into()).await;
        assert_eq!(got.body, "n,s\n20000,200010000\n");
    }

    #[tokio::test]
    async fn a_file_put_again_is_read_afresh() {
        let page = Page::open(&Options::default());
        let sum = "SELECT sum(a) AS s FROM 'v.csv'";
        assert_eq!(page.request("PUT", "/files/v.csv", "a\n1\n".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", sum.into()).await.body, "s\n1\n");
        assert_eq!(page.request("PUT", "/files/v.csv", "a\n5\n".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", sum.into()).await.body, "s\n5\n");
    }

    #[tokio::test]
    async fn a_refused_file_name_leaves_nothing_behind() {
        let page = page_with_tables().await;
        let refused = [
            ("SELECT * FROM 'nope.csv'", "42P01"),
            ("SELECT * FROM 'https://example.com/x.csv'", "0A000"),
            ("SELECT * FROM read_csv('orders.csv', columns => {'id': 'INT'})", "0A000"),
            ("SELECT * FROM 'nope.csv' JOIN 'orders.csv' ON true", "42P01"),
        ];
        for (sql, code) in refused {
            let got = page.request("POST", "/sql", sql.into()).await;
            assert_eq!(got.sqlstate, Some(code), "{sql}");
            assert_eq!(page.request("GET", "/tables", bytes::Bytes::new()).await.body, r#"["names","orders"]"#, "{sql}");
        }
        let got = page.request("POST", "/sql?format=csv", "FROM 'orders.csv'".into()).await;
        assert_eq!(got.body, rendered(&page, "SELECT * FROM orders", Some("csv")).await);
    }

    /// Requests at once that name one file each read it as their own: none takes another's table of its name, so every
    /// one answers the file's rows. (On one thread they never interleave, so the next test runs them on threads too.)
    #[tokio::test]
    async fn requests_at_once_naming_one_file_each_answer_its_rows() {
        let page = page_with_tables().await;
        let count = "SELECT count(*) AS n FROM 'orders.csv'";
        let answers = futures::future::join_all((0..20).map(|_| page.request("POST", "/sql?format=csv", count.into()))).await;
        for got in answers {
            assert_eq!(got.status, 200, "{:?}", got.body);
            assert_eq!(got.body, "n\n3\n");
        }
    }

    /// The same, each on a thread of its own: requests run truly at once, so one statement can take a file away between
    /// another's registering it and planning. (On one thread they never interleave, as above: nothing yields.) Each
    /// request is blocked on by its own thread, since a page's request future is not `Send` (`put` holds a reader).
    #[test]
    fn requests_on_threads_naming_one_file_each_answer_its_rows() {
        let page = futures::executor::block_on(page_with_tables());
        for _ in 0..50 {
            std::thread::scope(|s| {
                let asks: Vec<_> = (0..20)
                    .map(|_| s.spawn(|| futures::executor::block_on(page.request("POST", "/sql?format=csv", "SELECT count(*) AS n FROM 'orders.csv'".into()))))
                    .collect();
                for ask in asks {
                    let got = ask.join().unwrap();
                    assert_eq!(got.status, 200, "{:?}", got.body);
                    assert_eq!(got.body, "n\n3\n");
                }
            });
        }
    }

    #[tokio::test]
    async fn a_statement_that_names_a_file_leaves_no_table_behind() {
        let page = page_with_tables().await;
        assert_eq!(page.request("POST", "/sql?format=csv", "SELECT count(*) AS n FROM 'orders.csv'".into()).await.body, "n\n3\n");
        assert_eq!(page.request("GET", "/tables", bytes::Bytes::new()).await.body, r#"["names","orders"]"#);
    }

    /// A table a statement makes is the page's: a later request reads it, and `GET /tables` lists it until a statement
    /// drops it. (A page's door refuses DDL (`read_only`), which the front's own `CREATE TABLE` will change in a later
    /// step, so this runs the statement the way `rendered` does: its front, its file given, then its own session.)
    #[tokio::test]
    async fn a_table_a_statement_makes_is_the_pages() {
        let page = page_with_tables().await;
        let made = "CREATE TABLE t AS SELECT * FROM 'orders.csv'";
        let expanded = crate::front::expand(&PageHost(&page), made, &HashMap::new()).await.unwrap();
        let (ctx, overlay) = page.statement();
        page.given(&overlay, &expanded).await.unwrap();
        ctx.sql(&expanded).await.unwrap().collect().await.unwrap();
        assert_eq!(page.request("POST", "/sql?format=csv", "SELECT count(*) AS n FROM t".into()).await.body, "n\n3\n");
        assert_eq!(page.request("GET", "/tables", bytes::Bytes::new()).await.body, r#"["names","orders","t"]"#);
        let (ctx, _) = page.statement();
        ctx.sql("DROP TABLE t").await.unwrap().collect().await.unwrap();
        assert_eq!(page.request("GET", "/tables", bytes::Bytes::new()).await.body, r#"["names","orders"]"#);
    }

    #[tokio::test]
    async fn the_files_given_are_listed_sorted_and_a_refused_one_is_not_among_them() {
        let page = page_with_tables().await;
        assert_eq!(page.request("PUT", "/files/x.tsv", "a\tb\n1\t2\n".into()).await.sqlstate, Some("22023"));
        let got = page.request("GET", "/files", bytes::Bytes::new()).await;
        assert_eq!(got.body, r#"["names.csv","orders.csv"]"#);
    }

    #[tokio::test]
    async fn duckdb_forms_that_ask_the_data() {
        let page = page_with_tables().await;
        // PIVOT asks the page for its values, then writes the pivot as SQL.
        let pivot = page.request("POST", "/sql?format=csv", "PIVOT (SELECT name, qty FROM orders) ON name USING sum(qty)".into()).await;
        assert_eq!(pivot.body, "a,b,c\n5,7,9\n");
        // The same over a file read by name.
        let by_name = page.request("POST", "/sql?format=csv", "PIVOT (SELECT name, qty FROM 'orders.csv') ON name USING sum(qty)".into()).await;
        assert_eq!(by_name.body, "a,b,c\n5,7,9\n");
        // COLUMNS asks the page for the columns the pattern matches.
        let columns = page.request("POST", "/sql?format=csv", "SELECT COLUMNS('q.*') FROM orders ORDER BY qty".into()).await;
        assert_eq!(columns.body, "qty\n5\n7\n9\n");
        // SUMMARIZE asks for the columns, then answers one row a column.
        let summary = page.request("POST", "/sql?format=csv", "SUMMARIZE orders".into()).await;
        assert_eq!(summary.status, 200);
        let lines: Vec<&str> = std::str::from_utf8(&summary.body).unwrap().lines().collect();
        assert!(lines[0].starts_with("column_name,column_type,min,max"), "{lines:?}");
        assert_eq!(lines.len(), 4, "{lines:?}");
        assert!(lines[1].starts_with("id,") && lines[2].starts_with("name,") && lines[3].starts_with("qty,"), "{lines:?}");
        // ASOF JOIN, over two files put by name.
        assert_eq!(page.request("PUT", "/files/trades.csv", "k,t\n1,3\n1,7\n".into()).await.status, 200);
        assert_eq!(page.request("PUT", "/files/quotes.csv", "k,t,p\n1,1,100\n1,5,200\n".into()).await.status, 200);
        let asof = "SELECT tr.t, q.p FROM trades tr ASOF JOIN quotes q ON tr.k = q.k AND tr.t >= q.t ORDER BY tr.t";
        assert_eq!(page.request("POST", "/sql?format=csv", asof.into()).await.body, "t,p\n3,100\n7,200\n");
        // Every ask leaves no table behind.
        assert_eq!(page.request("GET", "/tables", bytes::Bytes::new()).await.body, r#"["names","orders","quotes","trades"]"#);
    }
}
