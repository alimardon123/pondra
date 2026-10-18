//! A lake-less engine: a node over a lake in memory, on the machine that is the page (designs/l2-b4-web.md). Its
//! files are kept as a node keeps a lake's own (`lake_files`), `FROM 'x.csv'` reads them as a node's working folder
//! reads them, and its tables are made by SQL (`CREATE TABLE t AS FROM 'x.parquet'`). Its routes mean what a node's
//! mean, through ADR-061's request contract. DuckDB's forms that ask the data (`PIVOT`, `COLUMNS`, `SUMMARIZE`) and
//! ASOF JOIN answer as on a node. `web/` wraps it for a browser.

use crate::front::{Host, Own};
use crate::lake_files::{changed, content_type, not_found, there_already, under_files};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList, Session, SchemaProvider, TableProvider};
use datafusion::common::{Constraints, Statistics};
use datafusion::datasource::{MemTable, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{DdlStatement, Expr, LogicalPlan};
use datafusion::object_store::{memory::InMemory, path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::Statement;
use serde_json::json;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, OnceLock, PoisonError};

const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json";
const FILES: &str = "/files/";
/// The folder SQL reads a relative path's file from, as a node's owner reads its working folder (`/objects` names it).
const WORKING: &str = "mem:///files/";

/// How a page's session is made: its target partitions (a page in a browser has one).
pub struct Options {
    pub partitions: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options { partitions: 1 }
    }
}

/// A page: a session of its own, whose tables are the ones its SQL made, and whose files are the ones its caller put
/// (in its memory, nowhere else).
pub struct Page {
    ctx: SessionContext,
    files: Arc<InMemory>, // (the lake's own files, `files/…`, as they were put: what `FROM 'x.csv'` reads)
}

/// What a request answers, as ADR-061's contract says: a status, its headers (the node's names, in lower case) and its
/// body. Every answer has a `content-type`. An error has its `x-pondra-sqlstate`; a file refused (409, 412, 404) has
/// none, as the node sends none for those.
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: bytes::Bytes,
}

impl Answer {
    /// An answer of status 200 in `content_type`.
    fn ok(content_type: &'static str, body: impl Into<bytes::Bytes>) -> Answer {
        Answer { status: 200, headers: vec![("content-type", content_type.to_string())], body: body.into() }
    }

    /// The answer's header `name` (any case), or None.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }

    /// The answer with one more header.
    fn with(mut self, name: &'static str, value: String) -> Answer {
        self.headers.push((name, value));
        self
    }
}

/// An error as an answer: its status, its SQLSTATE and its words, as every door gives them (`codes::answer`).
fn failed(e: anyhow::Error) -> Answer {
    let (status, code, words) = crate::codes::answer(&e);
    Answer { status, headers: vec![("content-type", TEXT.to_string()), ("x-pondra-sqlstate", code.to_string())], body: words.into() }
}

/// A file refused (409, 412, 404): the node's status and words, with no SQLSTATE.
fn refused(status: u16, words: String) -> Answer {
    Answer { status, headers: vec![("content-type", TEXT.to_string())], body: words.into() }
}

/// An entity tag as HTTP sends it: quoted.
fn quoted(e_tag: &str) -> String {
    format!("\"{e_tag}\"")
}

/// A request's header by name, compared case-insensitively.
fn request_header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
}

/// The value of `name` in a request's query (`format=csv`), or None; a bare `name` (`?versions`) has the empty value.
fn param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| match kv.split_once('=') {
        Some((k, v)) => (k == name).then_some(v),
        None => (kv == name).then_some(""),
    })
}

impl Page {
    /// A page with no tables and no files, its session a node's (`engine::session`) with the partitions asked for.
    pub fn open(options: &Options) -> Page {
        let mut config = SessionConfig::new().with_target_partitions(options.partitions.max(1));
        // A page's Parquet strings are plain, as the files hold them: views were no faster in a page and held 230 MB
        // more at SF1 (measured in web-4g). The session's Parquet options are the ones a file read takes (`ext_names::file_format`).
        config.options_mut().execution.parquet.schema_force_view_types = false;
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

    /// One request, as a node answers it (ADR-061): `method` and `path` (its query for `format=`, and nothing else
    /// here), the request's `headers` (any case of name) and `body`. Every failure is an answer too, with its SQLSTATE.
    pub async fn request(&self, method: &str, path: &str, headers: &[(String, String)], body: bytes::Bytes) -> Answer {
        let (route, query) = path.split_once('?').unwrap_or((path, ""));
        let answer = match (method, route) {
            ("GET", "/ready") => return Answer::ok(TEXT, "ready"),
            ("POST", "/sql") => match std::str::from_utf8(&body) {
                Ok(text) => self.sql(text, param(query, "format")).await,
                Err(e) => Err(crate::codes::coded("22P02", format!("the request's body can't be read: {e}"))),
            },
            ("GET", "/objects") => self.objects().await,
            ("PUT", _) if route.starts_with(FILES) => self.put(&route[FILES.len()..], headers, body).await,
            ("GET", _) if route.starts_with(FILES) => self.get(&route[FILES.len()..], query).await,
            ("DELETE", _) if route.starts_with(FILES) => self.delete(&route[FILES.len()..]).await,
            _ => {
                let mut answer = failed(crate::codes::coded("0A000", format!("a page has no route {method} {route}")));
                answer.status = 404;
                return answer;
            }
        };
        answer.unwrap_or_else(failed)
    }

    /// SQL as a node reads it: the front, then the rows of the files it names by name, planned (`plan`), then the
    /// rows in `format` (an answer with no rows keeps its columns, as `engine::collect` gives them: invariant 88).
    async fn sql(&self, text: &str, format: Option<&str>) -> anyhow::Result<Answer> {
        let batches = crate::engine::collect(self.plan(text).await?).await?;
        Ok(Answer::ok(crate::answer::content_type(format), crate::answer::render(&batches, format, |_| None)?))
    }

    /// A statement read as a node reads it: the front, ASOF JOIN's marker, then planned on the statement's own copy of
    /// the page's session (`statement`), with the files it names given to that copy alone (`given`). What a statement
    /// makes (a table) is the page's; nothing else it registers reaches the page or another statement.
    async fn plan(&self, text: &str) -> anyhow::Result<DataFrame> {
        let expanded = crate::front::expand(&PageHost(self), text, &HashMap::new()).await?;
        let expanded = crate::asof::as_of(&expanded)?.into_owned();
        let (ctx, overlay) = self.statement()?;
        self.given(&overlay, &expanded).await?;
        crate::engine::sql_checked(&ctx, &expanded, may).await
    }

    /// The page's default catalog and schema, and the schema's provider: the tables a statement is planned over.
    fn default_schema(&self) -> anyhow::Result<(String, String, Arc<dyn SchemaProvider>)> {
        let state = self.ctx.state();
        let options = state.config().options();
        let (catalog, schema) = (options.catalog.default_catalog.clone(), options.catalog.default_schema.clone());
        let page = self.ctx.catalog(&catalog).and_then(|c| c.schema(&schema)).ok_or_else(|| anyhow::anyhow!("a page has its default schema"))?;
        Ok((catalog, schema, page))
    }

    /// A statement's session: a copy of the page's, whose default schema is an `Overlay` over the page's own, so the
    /// files the statement names are its alone. The copy's state is the page's, cloned: its functions, settings and
    /// object store come with it, so nothing is registered again.
    fn statement(&self) -> anyhow::Result<(SessionContext, Arc<Overlay>)> {
        let (catalog, schema, page) = self.default_schema()?;
        let mut state = self.ctx.state();
        let overlay = Arc::new(Overlay { page, own: Default::default() });
        let default = MemoryCatalogProvider::new();
        default.register_schema(&schema, overlay.clone())?;
        let catalogs = MemoryCatalogProviderList::new();
        CatalogProviderList::register_catalog(&catalogs, catalog, Arc::new(default));
        state.register_catalog_list(Arc::new(catalogs));
        Ok((SessionContext::new_with_state(state), overlay))
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

    /// One file a statement names, as a table of its own copy under its `ext:` name, read as a node reads it
    /// (`ext_names`): Hive-style folders are columns, `**` reaches into folders, the files' notes are merged and the types
    /// are a node's. Refused unless a page reads it (its format, its options, its place) and every URL has a file that
    /// was put; given to the statement only once its columns are known.
    async fn register(&self, overlay: &Overlay, name: &str, spec: &crate::ext_names::Spec) -> anyhow::Result<()> {
        use crate::ext_names::CSV_OPTIONS;
        if !["parquet", "csv", "json", "arrow"].contains(&spec.format.as_str()) {
            return Err(crate::codes::coded("0A000", format!("a page reads Parquet, CSV, JSON and Arrow files given to it, not {} tables", spec.format)));
        }
        let takes = |k: &str| k == "hive_partitioning" || (spec.format == "csv" && CSV_OPTIONS.contains(&k));
        if let Some(k) = spec.options.keys().find(|k| !takes(k)) {
            return Err(crate::codes::coded("0A000", format!("a page doesn't take {k} yet")));
        }
        let state = self.ctx.state();
        let urls = spec.urls.iter().map(|u| located(u)).collect::<anyhow::Result<Vec<_>>>()?;
        let given = crate::ext_names::Spec { urls, ..spec.clone() };
        // (a file not put is named as SQL wrote it: the URL at the same place in `spec.urls`, not the store's form)
        let written = |u: &str| given.urls.iter().position(|g| g == u).map_or(u.to_string(), |i| spec.urls[i].clone());
        let found = crate::ext_names::listed(&state, &given, |u| Some(crate::codes::coded("42P01", format!("no file {} was given to this page (PUT /files/{{path}})", written(u))))).await?;
        let files = crate::ext_names::schema_of(&state, &given, &found).await?;
        let columns: Vec<(String, String)> = files.fields().iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect();
        let keys = crate::ext_names::keys(&found, &given, &[], &columns)?;
        let schema = crate::answer::schema(&[columns, keys.clone()].concat())?;
        let pairs: Vec<(String, String)> = found.iter().map(|f| (f.full(), if keys.is_empty() { String::new() } else { f.part() })).collect();
        let refs: Vec<(&str, &str)> = pairs.iter().map(|(file, part)| (file.as_str(), part.as_str())).collect();
        let df = crate::ext_names::read_files(&self.ctx, &refs, &schema, &given).await?;
        overlay.own(name.to_string(), df.into_view()); // (a name is one table: the statement's own map)
        Ok(())
    }

    /// `PUT /files/{path}` (the node's rules, `server::put_file`): the bytes kept as given, under `files/`, and no table
    /// made. Without `If-Match` a path that is there is 409; with it, the file is replaced if it is still that version
    /// (quotes and all), and 412 if not.
    async fn put(&self, name: &str, headers: &[(String, String)], body: bytes::Bytes) -> anyhow::Result<Answer> {
        let path = under_files(name);
        let mode = match request_header(headers, "if-match") {
            Some(version) => PutMode::Update(UpdateVersion { e_tag: Some(version.trim_matches('"').to_string()), version: None }),
            None => PutMode::Create,
        };
        let (bytes, at) = (body.len(), Path::from(path.as_str()));
        let e_tag = match self.files.put_opts(&at, PutPayload::from(body), PutOptions { mode, ..Default::default() }).await {
            Ok(done) => done.e_tag.unwrap_or_default(),
            Err(StoreError::AlreadyExists { .. }) => return Ok(refused(409, there_already(&path))),
            Err(StoreError::Precondition { .. }) => return Ok(refused(412, changed(&path))),
            Err(e) => return Err(e.into()),
        };
        let answer = serde_json::to_vec(&json!({"path": path, "bytes": bytes, "version": e_tag}))?;
        Ok(Answer::ok(JSON, answer).with("etag", quoted(&e_tag)))
    }

    /// `GET /files/{path}`: the file's bytes, its `etag` and its content type by its name. A page keeps no older
    /// versions, so `?versions` and `?version=` are refused by name.
    async fn get(&self, name: &str, query: &str) -> anyhow::Result<Answer> {
        if param(query, "versions").is_some() || param(query, "version").is_some() {
            return Err(crate::codes::coded("0A000", "a page keeps no older versions: ?versions and ?version= are a node's"));
        }
        let path = under_files(name);
        match self.files.get(&Path::from(path.as_str())).await {
            Ok(got) => {
                let e_tag = got.meta.e_tag.clone().unwrap_or_default();
                let body = got.bytes().await?;
                Ok(Answer::ok(content_type(&path), body).with("etag", quoted(&e_tag)))
            }
            Err(StoreError::NotFound { .. }) => Ok(refused(404, not_found(&path))),
            Err(e) => Err(e.into()),
        }
    }

    /// `DELETE /files/{path}`: the file gone. A path no file is at is 404, as on a node.
    async fn delete(&self, name: &str) -> anyhow::Result<Answer> {
        let path = under_files(name);
        let at = Path::from(path.as_str());
        match self.files.head(&at).await {
            Ok(_) => {}
            Err(StoreError::NotFound { .. }) => return Ok(refused(404, not_found(&path))),
            Err(e) => return Err(e.into()),
        }
        self.files.delete(&at).await?;
        Ok(Answer::ok(JSON, serde_json::to_vec(&json!({"removed": path}))?))
    }

    /// `GET /objects`: the page's tables, sorted by name, each with its rows and bytes (a memory table's; another
    /// provider's are left out), and the folder SQL reads a relative file from. Listed from the page's own schema.
    async fn objects(&self) -> anyhow::Result<Answer> {
        let (_, _, page) = self.default_schema()?;
        let mut names = page.table_names();
        names.sort();
        let mut objects = vec![];
        for name in names {
            let Some(table) = page.table(&name).await? else { continue };
            let mut object = json!({"catalog": "page", "schema": "public", "name": name, "kind": "table"});
            if let Some((rows, bytes)) = sizes(&table).await {
                object["rows"] = json!(rows);
                object["bytes"] = json!(bytes);
            }
            objects.push(object);
        }
        Ok(Answer::ok(JSON, serde_json::to_vec(&json!({"objects": objects, "files": WORKING}))?))
    }
}

/// Where a file a statement names is, as a page's store has it: a URL beginning `mem:///files/` as written, a relative
/// path (`'x.csv'`, `'./d/*.parquet'`) in the working folder (`under_files`), and any other URL refused: a page reads the
/// files it was given, not the web (yet).
fn located(url: &str) -> anyhow::Result<String> {
    if url.starts_with(WORKING) {
        return Ok(url.to_string());
    }
    if url.contains("://") {
        return Err(crate::codes::coded("0A000", format!("{url}: a page reads the files given to it (PUT /files/{{path}}); URLs come later")));
    }
    Ok(format!("mem:///{}", under_files(url.strip_prefix("./").unwrap_or(url))))
}

/// A table's rows and bytes, as its batches hold them: a memory table's, which is what a page's tables are. None for
/// another provider, whose size is not known here.
async fn sizes(table: &Arc<dyn TableProvider>) -> Option<(usize, usize)> {
    let table = table.downcast_ref::<Counted>().map_or(table, |c| &c.rows);
    let memory = table.downcast_ref::<MemTable>()?;
    let mut sizes = (0, 0);
    for partition in &memory.batches {
        for batch in partition.read().await.iter() {
            sizes = (sizes.0 + batch.num_rows(), sizes.1 + batch.get_array_memory_size());
        }
    }
    Some(sizes)
}

/// A page's table: its rows a `MemTable`, and what the join order needs to know of them, worked out the first time it
/// asks (as `query::Pruned::stats` is). The rule asks only when three or more inputs are joined, so a table never joined
/// so costs its load nothing. Nothing else is passed on: a page refuses DML (`may`), and a table changed under them would
/// make the statistics stale.
#[derive(Debug)]
struct Counted {
    rows: Arc<dyn TableProvider>,
    stats: OnceLock<Statistics>,
}

impl Counted {
    /// `table` with its statistics when it is a memory table (a page's own tables are); anything else as it is.
    fn over(table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        match table.is::<MemTable>() {
            true => Arc::new(Counted { rows: table, stats: OnceLock::new() }),
            false => table,
        }
    }
}

#[async_trait]
impl TableProvider for Counted {
    fn schema(&self) -> SchemaRef {
        self.rows.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.rows.constraints()
    }

    fn table_type(&self) -> TableType {
        self.rows.table_type()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.rows.get_column_default(column)
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.rows.scan(state, projection, filters, limit).await
    }

    /// The statistics once worked out; else worked out now from the rows, if no write holds them (a page's tables are
    /// only ever read, so the lock is free). Anything that fails answers None and keeps nothing.
    fn statistics(&self) -> Option<Statistics> {
        if let Some(known) = self.stats.get() {
            return Some(known.clone());
        }
        let memory = self.rows.downcast_ref::<MemTable>()?;
        let mut batches = vec![];
        for partition in &memory.batches {
            batches.extend(partition.try_read().ok()?.iter().cloned());
        }
        Some(self.stats.get_or_init(|| crate::sketch::statistics(&self.schema(), &batches)).clone())
    }
}

/// What a page's SQL may do. A page's tables are its own, in memory: `CREATE TABLE` (with `AS`, `OR REPLACE` and `IF
/// NOT EXISTS`) and `DROP TABLE` are what it takes. `CREATE EXTERNAL TABLE` is refused by name: a native page has
/// `file://` registered, so it would read its machine's disk. Every other DDL, and DML (INSERT, UPDATE, DELETE, COPY)
/// and statements, is refused as a node's read-only SQL refuses it.
fn may(plan: &LogicalPlan) -> anyhow::Result<()> {
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(_) | DdlStatement::DropTable(_)) => Ok(()),
        LogicalPlan::Ddl(ddl) => Err(crate::codes::coded("0A000", format!("a page doesn't take {} yet", ddl.name()))),
        _ => {
            crate::engine::read_only().verify_plan(plan)?; // (no DDL is left here: a node's read-only SQL)
            Ok(())
        }
    }
}

/// A statement's view of its page's default schema: the files the statement names (`own`), then the page's own tables.
/// The page's tables are everyone's, and a statement's files are its own, so requests at once never see or take each
/// other's. A table a statement makes or drops (`register_table`, `deregister_table`) is the page's, as SQL makes it.
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
        self.page.register_table(name, Counted::over(table))
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

// (a page keeps no routines, attached catalogs, views with measures or types of its own: the trait's defaults say so)
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

    async fn columns(&self, tables: BTreeSet<String>) -> anyhow::Result<HashMap<String, Vec<String>>> {
        let mut out = HashMap::new();
        for name in tables {
            if let Ok(t) = self.0.ctx.table_provider(name.as_str()).await {
                out.insert(name, t.schema().fields().iter().map(|f| f.name().clone()).collect());
            }
        }
        Ok(out)
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
    use datafusion::arrow::record_batch::RecordBatch;

    const ORDERS: &str = "id,name,qty\n1,a,5\n2,b,7\n3,c,9\n";
    const NAMES: &str = "id,label\n1,x\n3,z\n";

    /// A file put as a client puts it, with no `If-Match`.
    async fn put(page: &Page, name: &str, body: impl Into<bytes::Bytes>) -> Answer {
        page.request("PUT", &format!("/files/{name}"), &[], body.into()).await
    }

    /// A page with `orders` and `names` made from their files by SQL, as a client loads one.
    async fn page_with_tables() -> Page {
        let page = Page::open(&Options::default());
        for (name, csv) in [("orders", ORDERS), ("names", NAMES)] {
            assert_eq!(put(&page, &format!("{name}.csv"), csv).await.status, 200);
            let made = format!("CREATE TABLE {name} AS FROM '{name}.csv'");
            assert_eq!(page.request("POST", "/sql", &[], made.into()).await.status, 200);
        }
        page
    }

    /// The bytes `answer::render` gives the rows of `sql`, run on the page's session alone (no front).
    async fn rendered(page: &Page, sql: &str, format: Option<&str>) -> bytes::Bytes {
        let batches = page.ctx.sql(sql).await.unwrap().collect().await.unwrap();
        crate::answer::render(&batches, format, |_| None).unwrap()
    }

    /// The names `GET /objects` lists, in its order.
    fn names_listed(answer: &Answer) -> Vec<String> {
        let v: serde_json::Value = serde_json::from_slice(&answer.body).unwrap();
        v["objects"].as_array().unwrap().iter().map(|o| o["name"].as_str().unwrap().to_string()).collect()
    }

    /// The `rows` `GET /objects` gives the table `name`.
    fn rows_listed(answer: &Answer, name: &str) -> Option<u64> {
        let v: serde_json::Value = serde_json::from_slice(&answer.body).unwrap();
        v["objects"].as_array().unwrap().iter().find(|o| o["name"] == name).and_then(|o| o["rows"].as_u64())
    }

    #[tokio::test]
    async fn a_file_is_a_file_and_a_table_is_made_by_sql() {
        let page = Page::open(&Options::default());
        let put = put(&page, "orders.csv", ORDERS).await;
        assert_eq!((put.status, put.header("content-type")), (200, Some("application/json")));
        let answer: serde_json::Value = serde_json::from_slice(&put.body).unwrap();
        assert_eq!((answer["path"].as_str(), answer["bytes"].as_u64()), (Some("files/orders.csv"), Some(ORDERS.len() as u64)));
        assert_eq!(names_listed(&page.request("GET", "/objects", &[], bytes::Bytes::new()).await), Vec::<String>::new());
        let made = page.request("POST", "/sql", &[], "CREATE TABLE orders AS FROM 'orders.csv'".into()).await;
        assert_eq!(made.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM orders".into()).await.body, "n\n3\n");
    }

    /// `EXPLAIN ANALYZE` answers in a page, which has no tokio runtime. DataFusion's own would spawn its input's tasks
    /// on one and panic, so this runs under `block_on` alone: a `#[tokio::test]` would have a runtime and hide that.
    #[test]
    fn explain_analyze_runs_where_the_page_is() {
        futures::executor::block_on(async {
            let page = page_with_tables().await;
            let plain = page.request("POST", "/sql?format=csv", &[], "EXPLAIN ANALYZE SELECT count(*) FROM orders".into()).await;
            assert_eq!(plain.status, 200, "{:?}", plain.body);
            let text = String::from_utf8_lossy(&plain.body).into_owned();
            assert!(text.contains("Plan with Metrics") && text.contains("output_rows="), "{text}");
            let verbose = page.request("POST", "/sql?format=csv", &[], "EXPLAIN ANALYZE VERBOSE SELECT count(*) FROM orders".into()).await;
            assert_eq!(verbose.status, 200, "{:?}", verbose.body);
            let text = String::from_utf8_lossy(&verbose.body).into_owned();
            assert!(text.contains("Output Rows") && text.contains("Duration"), "{text}");
            // The page's tables are still there after it.
            assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM orders".into()).await.body, "n\n3\n");
        });
    }

    #[tokio::test]
    async fn a_put_answers_the_nodes_json_and_an_etag() {
        let page = Page::open(&Options::default());
        let first = put(&page, "orders.csv", ORDERS).await;
        let etag = first.header("etag").unwrap().to_string();
        assert!(etag.len() > 2 && etag.starts_with('"') && etag.ends_with('"'), "{etag}");
        // A second PUT without If-Match is refused by name, with no SQLSTATE: the node sends none for a file refused.
        let again = put(&page, "orders.csv", "x\n").await;
        assert_eq!((again.status, again.header("x-pondra-sqlstate")), (409, None));
        assert_eq!(&again.body[..], there_already("files/orders.csv").as_bytes());
        // With the etag the first PUT gave, quotes and all, it is replaced: the version changes.
        let replaced = page.request("PUT", "/files/orders.csv", &[("If-Match".to_string(), etag.clone())], ORDERS.into()).await;
        assert_eq!(replaced.status, 200);
        let newer = replaced.header("etag").unwrap().to_string();
        assert_ne!(newer, etag);
        // The first etag is stale now: 412, in the node's words.
        let stale = page.request("PUT", "/files/orders.csv", &[("if-match".to_string(), etag)], "x\n".into()).await;
        assert_eq!((stale.status, stale.header("x-pondra-sqlstate")), (412, None));
        assert_eq!(&stale.body[..], changed("files/orders.csv").as_bytes());
    }

    #[tokio::test]
    async fn a_get_answers_the_bytes_as_put() {
        let page = Page::open(&Options::default());
        put(&page, "orders.csv", ORDERS).await;
        let got = page.request("GET", "/files/orders.csv", &[], bytes::Bytes::new()).await;
        assert_eq!((got.status, got.header("content-type")), (200, Some("text/csv; charset=utf-8")));
        assert_eq!(&got.body[..], ORDERS.as_bytes());
        assert!(got.header("etag").is_some_and(|e| e.starts_with('"')));
        let missing = page.request("GET", "/files/nope.csv", &[], bytes::Bytes::new()).await;
        assert_eq!((missing.status, missing.header("x-pondra-sqlstate")), (404, None));
        assert_eq!(&missing.body[..], not_found("files/nope.csv").as_bytes());
        // A page keeps no older versions: both forms are refused by name.
        for query in ["?versions", "?version=1"] {
            let refused = page.request("GET", &format!("/files/orders.csv{query}"), &[], bytes::Bytes::new()).await;
            assert_eq!((refused.status, refused.header("x-pondra-sqlstate")), (501, Some("0A000")), "{query}");
        }
    }

    #[tokio::test]
    async fn a_table_made_from_a_file_outlives_the_file() {
        let page = Page::open(&Options::default());
        put(&page, "orders.csv", ORDERS).await;
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t AS FROM 'orders.csv'".into()).await.status, 200);
        let listed = page.request("GET", "/objects", &[], bytes::Bytes::new()).await;
        assert_eq!((names_listed(&listed), rows_listed(&listed, "t")), (vec!["t".to_string()], Some(3)));
        let v: serde_json::Value = serde_json::from_slice(&listed.body).unwrap();
        assert_eq!(v["files"], WORKING);
        let gone = page.request("DELETE", "/files/orders.csv", &[], bytes::Bytes::new()).await;
        assert_eq!((gone.status, &gone.body[..]), (200, &br#"{"removed":"files/orders.csv"}"#[..]));
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM t".into()).await.body, "n\n3\n");
        let read = page.request("POST", "/sql?format=csv", &[], "FROM 'orders.csv'".into()).await;
        assert_eq!((read.status, read.header("x-pondra-sqlstate")), (404, Some("42P01")));
        assert_eq!(page.request("DELETE", "/files/orders.csv", &[], bytes::Bytes::new()).await.status, 404);
        assert_eq!(page.request("POST", "/sql", &[], "DROP TABLE t".into()).await.status, 200);
        assert_eq!(names_listed(&page.request("GET", "/objects", &[], bytes::Bytes::new()).await), Vec::<String>::new());
    }

    #[tokio::test]
    async fn a_table_made_by_sql_takes_no_insert() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t (a INT)".into()).await.status, 200);
        let insert = page.request("POST", "/sql", &[], "INSERT INTO t VALUES (1)".into()).await;
        assert_eq!((insert.status, insert.header("x-pondra-sqlstate")), (501, Some("0A000")));
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM t".into()).await.body, "n\n0\n");
    }

    #[tokio::test]
    async fn from_first_goes_through_the_front() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=csv", &[], "FROM orders WHERE qty > 5".into()).await;
        assert_eq!(got.status, 200);
        assert_eq!(got.body, rendered(&page, "SELECT * FROM orders WHERE qty > 5", Some("csv")).await);
    }

    #[tokio::test]
    async fn a_join_is_answered_as_json() {
        let page = page_with_tables().await;
        let sql = "SELECT o.id, n.label FROM orders o JOIN names n ON o.id = n.id ORDER BY o.id";
        let got = page.request("POST", "/sql", &[], sql.into()).await;
        assert_eq!(got.body, rendered(&page, sql, None).await);
    }

    #[tokio::test]
    async fn exclude_is_planned_as_a_node_plans_it() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql", &[], "SELECT * EXCLUDE (name) FROM orders ORDER BY id".into()).await;
        assert_eq!(got.body, rendered(&page, "SELECT id, qty FROM orders ORDER BY id", None).await);
    }

    #[tokio::test]
    async fn a_json_union_is_planned_as_text() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=csv", &[], r#"SELECT '{"a": 7}' -> 'a' AS v"#.into()).await;
        assert_eq!(got.body, "v\n7\n");
    }

    #[tokio::test]
    async fn an_answer_with_no_rows_keeps_its_columns() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=arrow", &[], "FROM orders WHERE qty > 100".into()).await;
        assert_eq!(got.header("content-type"), Some("application/vnd.apache.arrow.stream"));
        let reader = StreamReader::try_new(&got.body[..], None).unwrap();
        assert_eq!(reader.schema().fields().len(), 3);
        assert_eq!(reader.map(|b| b.unwrap().num_rows()).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn an_empty_answer_is_typed_as_its_plan_runs_it() {
        // (the planner types 1e15 as a decimal of negative scale, which Parquet can't hold: the plan's type is not that)
        use datafusion::arrow::datatypes::DataType;
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=arrow", &[], "SELECT 1e15 AS x WHERE false".into()).await;
        assert_eq!(got.status, 200);
        let batches: Vec<RecordBatch> = StreamReader::try_new(&got.body[..], None).unwrap().map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
        let x = batches[0].schema().field(0).data_type().clone();
        assert!(!matches!(x, DataType::Decimal128(_, s) | DataType::Decimal256(_, s) if s < 0), "{x:?}");
        let parquet = page.request("POST", "/sql?format=parquet", &[], "SELECT 1e15 AS x WHERE false".into()).await;
        assert_eq!(parquet.status, 200);
    }

    #[tokio::test]
    async fn an_error_names_a_file_as_sql_did() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql", &[], "SELECT nope FROM 'orders.csv'".into()).await;
        assert_eq!(got.header("x-pondra-sqlstate"), Some("42703"));
        let body = std::str::from_utf8(&got.body).unwrap();
        assert!(body.contains("'orders.csv'") && !body.contains("ext:"), "{body}");
    }

    #[tokio::test]
    async fn errors_carry_their_sqlstate() {
        let page = page_with_tables().await;
        let syntax = page.request("POST", "/sql", &[], "SELEC 1".into()).await;
        assert_eq!((syntax.status, syntax.header("x-pondra-sqlstate")), (400, Some("42601")));
        let external = page.request("POST", "/sql", &[], "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'orders.csv'".into()).await;
        assert_eq!(external.header("x-pondra-sqlstate"), Some("0A000"));
        let route = page.request("GET", "/nowhere", &[], bytes::Bytes::new()).await;
        assert_eq!((route.status, route.header("x-pondra-sqlstate")), (404, Some("0A000")));
    }

    #[tokio::test]
    async fn objects_lists_the_tables_sorted_with_their_rows() {
        let page = page_with_tables().await;
        let got = page.request("GET", "/objects", &[], bytes::Bytes::new()).await;
        assert_eq!(got.status, 200);
        assert_eq!(names_listed(&got), ["names", "orders"]);
        assert_eq!((rows_listed(&got, "names"), rows_listed(&got, "orders")), (Some(2), Some(3)));
        let v: serde_json::Value = serde_json::from_slice(&got.body).unwrap();
        assert_eq!(v["objects"][1]["catalog"], "page");
        assert_eq!(v["objects"][1]["schema"], "public");
        assert_eq!(v["objects"][1]["kind"], "table");
        assert!(v["objects"][1]["bytes"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn a_file_is_read_by_name() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=csv", &[], "FROM 'orders.csv' WHERE qty > 5".into()).await;
        assert_eq!(got.status, 200);
        assert_eq!(got.body, rendered(&page, "SELECT * FROM orders WHERE qty > 5", Some("csv")).await);
    }

    /// A name a file is read by is its working folder's, as a node reads it: `files/…`, `./…` and the folder's own URL
    /// all name the same file, and any other URL is refused.
    #[tokio::test]
    async fn a_file_is_named_as_a_node_names_it() {
        let page = page_with_tables().await;
        let count = "SELECT count(*) AS n FROM ";
        for name in ["'files/orders.csv'", "'./orders.csv'", "'mem:///files/orders.csv'"] {
            let got = page.request("POST", "/sql?format=csv", &[], format!("{count}{name}").into()).await;
            assert_eq!(got.body, "n\n3\n", "{name}");
        }
        let refused = page.request("POST", "/sql", &[], format!("{count}'s3://bucket/orders.csv'").into()).await;
        assert_eq!(refused.header("x-pondra-sqlstate"), Some("0A000"));
    }

    #[tokio::test]
    async fn read_csv_takes_its_options_by_name() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM read_csv('orders.csv', header => true, delim => ',')".into()).await;
        assert_eq!(got.body, "n\n3\n");
    }

    #[tokio::test]
    async fn a_glob_reads_every_file_it_matches_and_lists_again() {
        let page = Page::open(&Options::default());
        for (name, csv) in [("part1.csv", "id,qty\n1,5\n"), ("part2.csv", "id,qty\n2,7\n")] {
            assert_eq!(put(&page, name, csv).await.status, 200);
        }
        let sum = "SELECT sum(qty) AS s FROM 'part*.csv'";
        assert_eq!(page.request("POST", "/sql?format=csv", &[], sum.into()).await.body, "s\n12\n");
        assert_eq!(put(&page, "part3.csv", "id,qty\n3,9\n").await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], sum.into()).await.body, "s\n21\n");
    }

    #[tokio::test]
    async fn parquet_is_read_by_name() {
        let page = page_with_tables().await;
        let parquet = page.request("POST", "/sql?format=parquet", &[], "SELECT * FROM orders".into()).await;
        assert_eq!(parquet.status, 200);
        assert_eq!(put(&page, "o.parquet", parquet.body).await.status, 200);
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT sum(qty) AS s FROM read_parquet('o.parquet')".into()).await;
        assert_eq!(got.body, "s\n21\n");
    }

    /// A page's Parquet strings are plain, as the file holds them, whichever way a statement reads the file (views were
    /// no faster in a page: web-4g).
    #[tokio::test]
    async fn a_parquet_file_has_plain_strings() {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let mut body = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut body, schema.clone(), None).unwrap();
        writer.write(&RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))]).unwrap()).unwrap();
        writer.close().unwrap();
        let page = Page::open(&Options::default());
        assert_eq!(put(&page, "s.parquet", body).await.status, 200);
        let by_name = page.request("POST", "/sql?format=csv", &[], "SELECT arrow_typeof(s) AS t FROM 's.parquet' LIMIT 1".into()).await;
        assert_eq!(by_name.body, "t\nUtf8\n");
        let read = page.request("POST", "/sql?format=csv", &[], "SELECT arrow_typeof(s) AS t FROM read_parquet('s.parquet') LIMIT 1".into()).await;
        assert_eq!(read.body, "t\nUtf8\n");
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE s AS FROM 's.parquet'".into()).await.status, 200);
        let table = page.request("POST", "/sql?format=csv", &[], "SELECT arrow_typeof(s) AS t FROM s LIMIT 1".into()).await;
        assert_eq!(table.body, "t\nUtf8\n");
    }

    /// A Parquet file of 20,000 rows (one Int64 column) made into a table is read in full: the batch size is not visible
    /// in an answer, so this guards the path.
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
        assert_eq!(put(&page, "big.parquet", body).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE big AS FROM 'big.parquet'".into()).await.status, 200);
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n, sum(x) AS s FROM big".into()).await;
        assert_eq!(got.body, "n,s\n20000,200010000\n");
    }

    #[tokio::test]
    async fn a_file_put_again_is_read_afresh() {
        let page = Page::open(&Options::default());
        let sum = "SELECT sum(a) AS s FROM 'v.csv'";
        let first = put(&page, "v.csv", "a\n1\n").await;
        assert_eq!(page.request("POST", "/sql?format=csv", &[], sum.into()).await.body, "s\n1\n");
        let etag = first.header("etag").unwrap().to_string();
        let second = page.request("PUT", "/files/v.csv", &[("If-Match".to_string(), etag)], "a\n5\n".into()).await;
        assert_eq!(second.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], sum.into()).await.body, "s\n5\n");
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
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!(got.header("x-pondra-sqlstate"), Some(code), "{sql}");
            assert_eq!(names_listed(&page.request("GET", "/objects", &[], bytes::Bytes::new()).await), ["names", "orders"], "{sql}");
        }
        let got = page.request("POST", "/sql?format=csv", &[], "FROM 'orders.csv'".into()).await;
        assert_eq!(got.body, rendered(&page, "SELECT * FROM orders", Some("csv")).await);
    }

    /// Requests on threads at once, each naming one file: none takes another's table of its name, so every one answers
    /// the file's rows. (A request is blocked on by its own thread: a page's request future is not `Send` for every
    /// caller, as `put` and the planner hold what they hold.)
    #[test]
    fn requests_on_threads_naming_one_file_each_answer_its_rows() {
        let page = futures::executor::block_on(page_with_tables());
        for _ in 0..50 {
            std::thread::scope(|s| {
                let asks: Vec<_> = (0..20)
                    .map(|_| s.spawn(|| futures::executor::block_on(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM 'orders.csv'".into()))))
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
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM 'orders.csv'".into()).await.body, "n\n3\n");
        assert_eq!(names_listed(&page.request("GET", "/objects", &[], bytes::Bytes::new()).await), ["names", "orders"]);
    }

    /// A table a statement makes is the page's: a later request reads it, and `GET /objects` lists it until a statement
    /// drops it.
    #[tokio::test]
    async fn a_table_a_statement_makes_is_the_pages() {
        let page = page_with_tables().await;
        let made = page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT * FROM 'orders.csv'".into()).await;
        assert_eq!(made.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM t".into()).await.body, "n\n3\n");
        assert_eq!(names_listed(&page.request("GET", "/objects", &[], bytes::Bytes::new()).await), ["names", "orders", "t"]);
        assert_eq!(page.request("POST", "/sql", &[], "DROP TABLE t".into()).await.status, 200);
        assert_eq!(names_listed(&page.request("GET", "/objects", &[], bytes::Bytes::new()).await), ["names", "orders"]);
    }

    /// A `.tsv` is a file like any other, and SQL reads it tab-delimited, as a node's spec reads it.
    #[tokio::test]
    async fn a_tsv_file_is_read_tab_delimited() {
        let page = Page::open(&Options::default());
        assert_eq!(put(&page, "x.tsv", "a\tb\n1\t2\n").await.status, 200);
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT * FROM 'x.tsv'".into()).await;
        assert_eq!(got.body, "a,b\n1,2\n");
    }

    /// Hive-style folders are columns, as a node reads them: `d/day=1/a.csv` and `d/day=2/b.csv` are one table whose `day`
    /// is each folder's value, `**` reaches into folders, and `hive_partitioning => false` leaves the column out.
    #[tokio::test]
    async fn folders_are_columns_and_double_stars_reach_into_them() {
        let page = Page::open(&Options::default());
        assert_eq!(put(&page, "d/day=1/a.csv", "id,qty\n1,5\n2,7\n").await.status, 200);
        assert_eq!(put(&page, "d/day=2/b.csv", "id,qty\n3,9\n").await.status, 200);
        let deep = "SELECT day, count(*) AS n FROM 'd/**/*.csv' GROUP BY day ORDER BY day";
        assert_eq!(page.request("POST", "/sql?format=csv", &[], deep.into()).await.body, "day,n\n1,2\n2,1\n");
        let flat = "SELECT * FROM read_csv('d/**/*.csv', hive_partitioning => false) ORDER BY id";
        assert_eq!(page.request("POST", "/sql?format=csv", &[], flat.into()).await.body, "id,qty\n1,5\n2,7\n3,9\n");
    }

    /// Two Arrow files whose notes differ (each file's own) are one table, as a node reads them.
    #[tokio::test]
    async fn arrow_files_with_their_own_notes_are_one_table() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::ipc::writer::FileWriter;
        let page = Page::open(&Options::default());
        for i in 0..2i64 {
            let note = [("a".to_string(), i.to_string())];
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]).with_metadata(note.into()));
            let mut body = Vec::new();
            let mut writer = FileWriter::try_new(&mut body, &schema).unwrap();
            writer.write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![i * 10, i * 10 + 1]))]).unwrap()).unwrap();
            writer.finish().unwrap();
            assert_eq!(put(&page, &format!("p/{i}.arrow"), body).await.status, 200);
        }
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n, sum(x) AS s FROM 'p/*.arrow'".into()).await;
        assert_eq!(got.status, 200, "{}", String::from_utf8_lossy(&got.body));
        assert_eq!(got.body, "n,s\n4,22\n");
    }

    /// A file not put is 42P01, and its words name the file as SQL wrote it, never the store's own form.
    #[tokio::test]
    async fn a_file_not_put_is_named_as_sql_wrote_it() {
        let page = Page::open(&Options::default());
        let got = page.request("POST", "/sql", &[], "SELECT * FROM 'nope.csv'".into()).await;
        let body = std::str::from_utf8(&got.body).unwrap();
        assert_eq!(got.header("x-pondra-sqlstate"), Some("42P01"));
        assert!(body.contains("no file nope.csv was given to this page") && !body.contains("mem:///"), "{body}");
    }

    #[tokio::test]
    async fn duckdb_forms_that_ask_the_data() {
        let page = page_with_tables().await;
        // PIVOT asks the page for its values, then writes the pivot as SQL.
        let pivot = page.request("POST", "/sql?format=csv", &[], "PIVOT (SELECT name, qty FROM orders) ON name USING sum(qty)".into()).await;
        assert_eq!(pivot.body, "a,b,c\n5,7,9\n");
        // The same over a file read by name.
        let by_name = page.request("POST", "/sql?format=csv", &[], "PIVOT (SELECT name, qty FROM 'orders.csv') ON name USING sum(qty)".into()).await;
        assert_eq!(by_name.body, "a,b,c\n5,7,9\n");
        // COLUMNS asks the page for the columns the pattern matches.
        let columns = page.request("POST", "/sql?format=csv", &[], "SELECT COLUMNS('q.*') FROM orders ORDER BY qty".into()).await;
        assert_eq!(columns.body, "qty\n5\n7\n9\n");
        // SUMMARIZE asks for the columns, then answers one row a column.
        let summary = page.request("POST", "/sql?format=csv", &[], "SUMMARIZE orders".into()).await;
        assert_eq!(summary.status, 200);
        let lines: Vec<&str> = std::str::from_utf8(&summary.body).unwrap().lines().collect();
        assert!(lines[0].starts_with("column_name,column_type,min,max"), "{lines:?}");
        assert_eq!(lines.len(), 4, "{lines:?}");
        assert!(lines[1].starts_with("id,") && lines[2].starts_with("name,") && lines[3].starts_with("qty,"), "{lines:?}");
        // ASOF JOIN, over two files read by name.
        assert_eq!(put(&page, "trades.csv", "k,t\n1,3\n1,7\n").await.status, 200);
        assert_eq!(put(&page, "quotes.csv", "k,t,p\n1,1,100\n1,5,200\n").await.status, 200);
        let asof = "SELECT tr.t, q.p FROM 'trades.csv' tr ASOF JOIN 'quotes.csv' q ON tr.k = q.k AND tr.t >= q.t ORDER BY tr.t";
        assert_eq!(page.request("POST", "/sql?format=csv", &[], asof.into()).await.body, "t,p\n3,100\n7,200\n");
        // Asking and reading files make no table: only the two made by SQL are listed.
        assert_eq!(names_listed(&page.request("GET", "/objects", &[], bytes::Bytes::new()).await), ["names", "orders"]);
    }

    /// A page's table tells the join order what it holds, as a node's does: rows, each column's least and greatest value,
    /// NULLs and distinct values (`sketch::statistics`).
    #[tokio::test]
    async fn a_page_table_says_what_it_holds() {
        use datafusion::common::stats::Precision;
        use datafusion::common::ScalarValue;
        let page = Page::open(&Options::default());
        let made = page.request("POST", "/sql", &[], "CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'a'), (NULL, 'c')".into()).await;
        assert_eq!(made.status, 200);
        let (_, _, schema) = page.default_schema().unwrap();
        let table = schema.table("t").await.unwrap().unwrap();
        let stats = table.statistics().unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(4));
        let (ids, names) = (&stats.column_statistics[0], &stats.column_statistics[1]);
        assert_eq!(ids.min_value, Precision::Inexact(ScalarValue::Int64(Some(1))));
        assert_eq!(ids.max_value, Precision::Inexact(ScalarValue::Int64(Some(3))));
        assert_eq!((ids.null_count, ids.distinct_count), (Precision::Inexact(1), Precision::Inexact(3)));
        assert_eq!(names.distinct_count, Precision::Inexact(3));
    }

    /// Three tables written in a bad order: as written, the outermost join is `mid.c = tiny.column1`, after all of `big`
    /// met `mid`. With what the tables hold, the join order meets `tiny` first and `big` last, so the outermost join (the
    /// plan's first `Join` line) is on `big.k`.
    #[tokio::test]
    async fn a_page_joins_by_what_its_tables_hold() {
        let page = Page::open(&Options::default());
        for sql in [
            "CREATE TABLE big AS SELECT value AS k FROM generate_series(1, 200000)",
            "CREATE TABLE mid AS SELECT value AS k, value % 10 AS c FROM generate_series(1, 20000)",
            "CREATE TABLE tiny AS VALUES (3)",
        ] {
            assert_eq!(page.request("POST", "/sql", &[], sql.into()).await.status, 200, "{sql}");
        }
        let explained = "EXPLAIN SELECT count(*) FROM big JOIN mid ON big.k = mid.k JOIN tiny ON mid.c = tiny.column1";
        let answer = page.request("POST", "/sql", &[], explained.into()).await;
        let rows: serde_json::Value = serde_json::from_slice(&answer.body).unwrap();
        let logical = rows.as_array().unwrap().iter().find(|r| r["plan_type"] == "logical_plan").unwrap()["plan"].as_str().unwrap();
        let first_join = logical.lines().find(|l| l.contains("Join")).unwrap();
        assert!(first_join.contains("big.k"), "{logical}");
    }
}
