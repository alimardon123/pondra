//! A lake-less engine: a node over a lake in memory, on the machine that is the page (designs/l2-b4-web.md). Its
//! files are kept as a node keeps a lake's own (`lake_files`), `FROM 'x.csv'` reads them as a node's working folder
//! reads them, and its tables are made by SQL (`CREATE TABLE t AS FROM 'x.parquet'`). Its routes mean what a node's
//! mean, through ADR-061's request contract. DuckDB's forms that ask the data (`PIVOT`, `COLUMNS`, `SUMMARIZE`) and
//! ASOF JOIN answer as on a node. A page with a node (`Options::node`) asks it for the tables the page doesn't have, with
//! the rows of the page's own tables the query names (B5). `web/` wraps it for a browser.

use crate::expander::Routine;
use crate::ext_names::Attached;
use crate::front::{Host, Own, Relation};
use crate::measures::Definition;
use crate::request_body;
use crate::lake_files::{changed, content_type, not_found, there_already, under_files};
use anyhow::Context;
use async_trait::async_trait;
use futures::{TryFutureExt, TryStreamExt};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::ipc::{reader::{FileReader, StreamDecoder}, writer::{FileWriter, StreamWriter}};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, Session, SchemaProvider, TableProvider};
use datafusion::common::{Constraints, DFSchema, Statistics, TableReference};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{source_as_provider, MemTable, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::dml::{InsertOp, WriteOp};
use datafusion::logical_expr::{DdlStatement, Expr, LogicalPlan, TableSource};
use datafusion::object_store::{memory::InMemory, path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{CreateTable, Insert, ObjectName, Query, SetExpr, Statement as SqlStatement, TableObject};
use serde_json::json;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, OnceLock, PoisonError};
use std::task::Poll;

const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json";
const FILES: &str = "/files/";
/// The folder SQL reads a relative path's file from, as a node's owner reads its working folder (`/objects` names it).
const WORKING: &str = "mem:///files/";
/// The folder a page's tables are kept in, beside `files/` in its store. SQL never reads it (`located`).
const TABLES: &str = "tables";
/// The notes an Arrow file of a kept table carries: the table's name, and a column's default as SQL text.
const TABLE_KEY: &str = "pondra.table";
const DEFAULT_KEY: &str = "pondra.default";
/// The name a fill's node rows have while the fill is planned (`over_rows`): a table of the statement's own, which only
/// the statement sees. SQL writes it quoted (`ext_names::quoted`), and a fill may not make a table of this name.
const NODE_ROWS: &str = "the node's rows";
/// A page's memory line when `PONDRA_MEMORY_MAX_GB` isn't set: 3 GiB, a quarter below the 4 GiB a wasm32 memory ends at. The
/// live bytes are what the program asked for, so that quarter is the room for the allocator's own footprint and for a last
/// step that grows within one poll.
const LINE: usize = 3 << 30;
/// What a query the watch stopped answers (SQLSTATE 53200: `codes::of` reads "out of memory").
const PAGE_OOM: &str = "out of memory: the page was nearly out of memory and stopped this query, the one that grew it (memory_max_gb)";

/// How a page's session is made: its target partitions (a page in a browser has one), where it keeps its files and its
/// tables, how it reaches the web, and how it watches its memory.
pub struct Options {
    pub partitions: usize,
    /// The store a page keeps its files and its tables in: a browser gives its origin's private file system (OPFS), so
    /// what a page was given is there again after a reload (`kept`). None: the page's own memory, gone with the page.
    pub store: Option<Arc<dyn ObjectStore>>,
    /// The store for a web origin (`https://host[:port]`) a statement names, made the first time one does: a
    /// browser's `fetch` (`web/`). None: the page reads only the files given to it.
    pub web: Option<Web>,
    /// The page's node (B5): asked for the tables the page doesn't have, with the rows of its own tables a query names.
    /// None asks no node: a query over a table the page lacks is 42P01, as it is on a page with no node.
    pub node: Option<Node>,
    /// The page's live heap bytes, read by the program that runs the page (`web/` counts its allocator's). The watch reads
    /// it at each poll of a query and stops the query that grows the page past its line (`Watch`). None watches nothing.
    pub memory: Option<Memory>,
}

/// The page's live heap bytes, as the program that runs the page reads them (`Options::memory`).
pub type Memory = Arc<dyn Fn() -> usize + Send + Sync>;

/// A page's memory watch: what it reads its live bytes with, and the line past which the query that grew them is stopped
/// (`watched`). A page's line is set by `PONDRA_MEMORY_MAX_GB` (`memory_line`).
struct Watch {
    live: Memory,
    line: usize,
}

/// A page's way to the web: a store for an origin. The program that runs the page gives it, so the core holds no
/// HTTP client of its own.
pub type Web = Arc<dyn Fn(&str) -> Arc<dyn ObjectStore> + Send + Sync>;

/// A page's way to its node: one request to the node's `/sql?{query}` with `Sent`'s body, answered with the node's
/// status, `content-type`, `x-pondra-sqlstate` and body (`Answer`). The program that runs the page makes it, since it
/// holds the node's address and token, so the core holds no HTTP client of its own.
pub type Node = Arc<dyn Fn(Sent) -> Asked + Send + Sync>;

/// A node's answer to a page, on its way, or the words of why none came (a node down, a browser's refusal): the page
/// answers those as 08001 (`Page::ask`).
pub type Asked = futures::future::BoxFuture<'static, Result<Answer, String>>;

/// A request a page sends its node: its `query` (`format=csv`, or empty), the body's `content_type` (a request with
/// tables of its own, `request_body`) and the body.
pub struct Sent {
    pub query: String,
    pub content_type: &'static str,
    pub body: bytes::Bytes,
}

impl Default for Options {
    fn default() -> Self {
        Options { partitions: 1, store: None, web: None, node: None, memory: None }
    }
}

/// The words of a write the site's browser storage can't keep, because its quota is full (`web/` says so when the browser
/// refuses a write). A node's full disk is 53100 too, so this is the same answer at every door.
pub const FULL: &str = "this site's browser storage is full";

/// A page: a session of its own, whose tables are the ones its SQL makes and writes, and whose files are the ones its
/// caller put. Both are kept in the store the page was given, or in its memory when it was given none.
pub struct Page {
    ctx: SessionContext,
    files: Arc<dyn ObjectStore>, // (the store given, or memory: the files as put, `files/…`, and the tables kept, `tables/…`)
    keeps: bool, // (a store was given: its tables are kept there; a page in memory keeps no second copy of them)
    web: Option<Web>,
    // One statement is planned at a time, and a write is run and kept before the next statement is planned, so no
    // statement is planned against a table another is changing, and two writes never put their snapshots out of order.
    // A read lets go once planned. Only `sql` takes it: `PageHost::ask` plans inside `planned`, so a lock taken there
    // would wait on the statement that asked.
    keeping: futures::lock::Mutex<()>,
    node: Option<Node>,
    watch: Option<Watch>, // (the page's memory watch: none when the page was given no `memory`, or its line is 0)
    unread: Vec<(String, String)>, // (the kept files `kept` couldn't read, each as its path and the words of why)
}

/// What a write to a page table found there, for `undo` to put back: the table and its rows, partition by partition.
/// None when the name had no table, which the write makes.
type Before = Option<(Arc<dyn TableProvider>, Vec<Vec<RecordBatch>>)>;

/// What a page sends its node for a statement. A query goes as it is written. A fill (`CREATE TABLE … AS`, or `INSERT INTO`
/// one of the page's own tables) sends only its query, and `write` is the statement with that query put for the node's
/// rows, which the page runs once it has them (`filled`).
enum Outgoing {
    Query(Sent),
    Fill { write: String, sent: Sent },
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

/// An error as an answer: its status, its SQLSTATE and its words, as every door gives them (`codes::answer`; a page's
/// caller sees all the words, so no line is cut).
fn failed(e: anyhow::Error) -> Answer {
    let (status, code, words) = crate::codes::answer(&e, true);
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
    /// A page over `options`'s store, with the files and tables it kept: `open`, then every object the store lists under
    /// `tables/` that ends `.arrow` is read back as a table of the page's default schema (`restore`). A kept table has its
    /// rows, its columns and its column defaults again. Its tables stay in memory while the page runs; its files are read
    /// from the store as they are asked for. With no store given nothing is listed, so the page is `open`'s. A kept file
    /// that doesn't read is left out and listed by `unread`, so the page still opens with the tables it can read.
    pub async fn kept(options: &Options) -> anyhow::Result<Page> {
        let mut page = Page::open(options);
        let listed = page.files.list(Some(&Path::from(TABLES))).try_collect::<Vec<_>>().await?;
        for object in listed.iter().filter(|o| o.location.as_ref().ends_with(".arrow")) {
            if let Err(e) = page.restore(&object.location).await {
                page.unread.push((object.location.to_string(), crate::codes::words(&e)));
            }
        }
        Ok(page)
    }

    /// The kept files `kept` couldn't read, each as its path and the words of why. The file is left as it is, and its
    /// table's name can't be written (`unwritable`) while the file is there, so no write takes the file's rows away.
    pub fn unread(&self) -> &[(String, String)] {
        &self.unread
    }

    /// A page with no tables, its session a node's (`engine::session`) with the partitions asked for. Its files are the
    /// store's, as they are kept there; with no store given they are in memory. Its memory is watched when it was given a
    /// `memory`, against the line its settings give (`memory_line`).
    pub fn open(options: &Options) -> Page {
        let watch = options.memory.as_ref().zip(memory_line()).map(|(live, line)| Watch { live: live.clone(), line });
        Page::with_watch(options, watch)
    }

    /// `open`, with its memory watch given: `open` reads the line from the settings, and a test gives a small one.
    fn with_watch(options: &Options, watch: Option<Watch>) -> Page {
        let mut config = SessionConfig::new().with_target_partitions(options.partitions.max(1));
        // A page's Parquet strings are plain, as the files hold them: views were no faster in a page and held 230 MB
        // more at SF1 (measured in web-4g). The session's Parquet options are the ones a file read takes (`ext_names::file_format`).
        config.options_mut().execution.parquet.schema_force_view_types = false;
        // A greedy pool, not a fair one: with no disk, every sort and aggregate can spill, so a fair pool would give each of
        // them a fraction. Nothing spills, since a page has no disk to spill to: an operator past the pool is refused (53200).
        let runtime = RuntimeEnvBuilder::new()
            .with_cache_manager(CacheManagerConfig::default().with_list_files_cache_limit(0)) // (a file put again is listed again)
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(query_memory())))
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled))
            .build_arc()
            .expect("a runtime");
        // (added last: a plan's partitions run in the page's own task, as there is no runtime to spawn them on)
        let ctx = crate::in_task::planned(crate::engine::session(config, runtime));
        let files: Arc<dyn ObjectStore> = match &options.store {
            Some(store) => store.clone(),
            None => Arc::new(InMemory::new()),
        };
        let url = ObjectStoreUrl::parse("mem://").expect("a URL");
        ctx.register_object_store(url.as_ref(), files.clone());
        Page { ctx, files, keeps: options.store.is_some(), web: options.web.clone(), node: options.node.clone(), keeping: futures::lock::Mutex::new(()), watch, unread: vec![] }
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

    /// SQL as a node reads it: the front, then the rows of the files it names by name, planned (`planned`), then the
    /// rows in `format` (an answer with no rows keeps its columns, as `engine::collect` gives them: invariant 88). An
    /// EXPLAIN's rows are made readable as a node's are (`explain::answered`). A read lets the next statement plan once
    /// its own is planned; a write is run and kept (`write`) before the next one is planned.
    async fn sql(&self, text: &str, format: Option<&str>) -> anyhow::Result<Answer> {
        let turn = self.keeping.lock().await;
        let planned = match self.planned(text).await {
            Ok(planned) => planned,
            // (a node to ask: the node answers a statement the page can't plan, or can't expand over a node's table, and the page
            // gives its answer on)
            Err(e) if self.node.is_some() => {
                let missing = crate::codes::of(&e) == "42P01";
                drop(turn); // (asking the node changes nothing on the page: the next statement may plan meanwhile)
                return match self.outgoing(text, format, missing).await? {
                    Some(Outgoing::Query(sent)) => self.ask(sent).await,
                    Some(Outgoing::Fill { write, sent }) => self.filled(&write, sent, format).await,
                    None => Err(e),
                };
            }
            Err(e) => return Err(e),
        };
        self.run_planned(turn, planned, format, crate::explain::asked(text)).await
    }

    /// A planned statement run, and its answer in `format`, as `sql` gives one: a write is run and kept (`write`) while the
    /// turn is held, and a read lets the turn go first, so the next statement may plan while its rows are read. `explained`
    /// says the statement is an EXPLAIN, whose rows are made readable (`explain::answered`).
    async fn run_planned(&self, turn: futures::lock::MutexGuard<'_, ()>, planned: (SessionContext, LogicalPlan, Option<String>), format: Option<&str>, explained: bool) -> anyhow::Result<Answer> {
        let (ctx, plan, changed) = planned;
        let batches = match changed {
            Some(name) => self.write(&ctx, plan, &name).await?,
            None => {
                drop(turn); // (a read runs on its own: it changes nothing, so the next statement may plan)
                rows_of(&ctx, plan, self.watch.as_ref()).await?
            }
        };
        let batches = if explained { crate::explain::answered(batches)? } else { batches };
        Ok(Answer::ok(crate::answer::content_type(format), crate::answer::render(&batches, format, |_| None)?))
    }

    /// What the page would send its node for `text` (`sql` sends it when the page can't plan the statement, or the front can't
    /// expand it), or None when the statement names no table the node holds, so the planner's own error stands (a file not put
    /// is 42P01 still). `missing` says the planner found no table. A statement the front can't expand over a node's table (a
    /// whole row of one, or its enum) is sent as written, and a page's own refusal, or a statement over its files, stands.
    /// Kept apart from sending, so the snapshot path (4k-5) branches here later.
    async fn outgoing(&self, text: &str, format: Option<&str>, missing: bool) -> anyhow::Result<Option<Outgoing>> {
        let (expanded, raw) = match crate::front::expand(&PageHost(self), text, &HashMap::new()).await {
            Ok(_) if !missing => return Ok(None), // (the planner's or a rule's refusal over what the page has: the page's own answer)
            Ok(expanded) => (crate::asof::as_of(&expanded)?.into_owned(), false),
            // (a page's own refusal, and a statement over its files, keep the front's error: only the front makes a file's name the page's)
            Err(_) if not_taken(text).is_err() || crate::ext_names::mentions(text) => return Ok(None),
            // (the front failed on what only the node may know, a node table's whole row or its enum: the text as written says
            // whether a node table is named)
            Err(_) => (text.to_string(), true),
        };
        let statement = match self.parsed(&expanded) {
            Ok(statement) => statement,
            Err(_) if raw => return Ok(None),
            Err(e) => return Err(e),
        };
        match self.fill_of(&statement)? {
            Some((target, query)) => {
                // (the fill's query as the text writes it, when it parses: the front's rewrites were made without the node's column types)
                let written = match self.parsed(text) {
                    Ok(as_written) => self.fill_of(&as_written).ok().flatten().map(|(_, q)| q.to_string()),
                    Err(_) => None,
                };
                let sent = written.unwrap_or_else(|| query.to_string());
                self.fill_outgoing(&statement, &target, query, &sent).await
            }
            None => self.query_outgoing(text, format, &statement).await,
        }
    }

    /// The query a statement that is not a fill sends its node, as it is written (`text`), with the page's tables it names.
    /// A write that is not a query is refused by name, before anything is sent (its words say how to write it instead).
    async fn query_outgoing(&self, text: &str, format: Option<&str>, statement: &DfStatement) -> anyhow::Result<Option<Outgoing>> {
        let named = self.named(statement)?;
        if named.iter().all(|(home, _)| *home != Home::Node) {
            return Ok(None);
        }
        if query_of(statement).is_none() {
            return Err(crate::codes::coded("0A000", "a page sends its node queries, and the query of a CREATE TABLE … AS or an INSERT INTO one of its own tables (write INSERT INTO t WITH … SELECT, not WITH … INSERT): write into the node's tables through a client of the node"));
        }
        let names = page_names(named)?;
        let query = format.map(|f| format!("format={f}")).unwrap_or_default();
        Ok(Some(Outgoing::Query(self.sent_for(text, query, &names).await?)))
    }

    /// The fill a statement is, if it is one: the table it writes (as the planner reads its name) and the query whose rows
    /// it takes. A `CREATE TABLE … AS` is a fill whatever its target, since a table the page doesn't have yet is the
    /// node's by name. An `INSERT INTO` is one only when its target is one of the page's own tables.
    fn fill_of<'a>(&self, statement: &'a DfStatement) -> anyhow::Result<Option<(TableReference, &'a Query)>> {
        let DfStatement::Statement(s) = statement else { return Ok(None) };
        match &**s {
            SqlStatement::CreateTable(CreateTable { name, query: Some(query), .. }) => Ok(Some((self.table_ref(name)?, query.as_ref()))),
            SqlStatement::Insert(Insert { table: TableObject::TableName(name), source: Some(query), .. }) => {
                let target = self.table_ref(name)?;
                Ok((self.home(&target)? == Home::Page).then_some((target, query.as_ref())))
            }
            _ => Ok(None),
        }
    }

    /// A fill's send: the node is asked the fill's query alone (`fill_of`), as `sent` writes it, so a file the statement names
    /// beside the node's tables is refused, and a table named as the node's rows is refused too (the name `over_rows` gives the rows).
    async fn fill_outgoing(&self, statement: &DfStatement, target: &TableReference, query: &Query, sent: &str) -> anyhow::Result<Option<Outgoing>> {
        let named = self.named(&as_statement(query))?;
        if named.iter().all(|(home, _)| *home != Home::Node) {
            return Ok(None);
        }
        if target.table() == NODE_ROWS {
            return Err(crate::codes::coded("0A000", format!("{}: a page gives its node's rows this name while it fills a table, so no table it makes may have it", crate::ext_names::quoted(NODE_ROWS))));
        }
        let names = page_names(named)?;
        let write = self.over_rows(statement)?;
        let sent = self.sent_for(sent, "format=arrow".to_string(), &names).await?;
        Ok(Some(Outgoing::Fill { write, sent }))
    }

    /// The fill's statement with its query put for the node's rows (`rows_query`), as SQL text the page plans: a `CREATE
    /// TABLE t AS SELECT * FROM "the node's rows"`, or an `INSERT INTO t (cols) SELECT * FROM "the node's rows"`.
    fn over_rows(&self, statement: &DfStatement) -> anyhow::Result<String> {
        let mut write = statement.clone();
        if let DfStatement::Statement(s) = &mut write {
            match &mut **s {
                SqlStatement::CreateTable(c) => c.query = Some(self.rows_query()?),
                SqlStatement::Insert(i) => i.source = Some(self.rows_query()?),
                _ => {}
            }
        }
        Ok(write.to_string())
    }

    /// `SELECT * FROM` the node's rows, parsed as the page's session parses SQL: the query a fill's write puts in place of
    /// its own.
    fn rows_query(&self) -> anyhow::Result<Box<Query>> {
        let sql = format!("SELECT * FROM {}", crate::ext_names::quoted(NODE_ROWS));
        let DfStatement::Statement(s) = self.parsed(&sql)? else { anyhow::bail!("{sql} is not a statement a page's session parses") };
        let SqlStatement::Query(query) = *s else { anyhow::bail!("{sql} is not a query") };
        Ok(query)
    }

    /// The request a page sends its node: `sql` as its JSON's `sql`, `query` as its query string, and the page's tables that
    /// `names` name, each with its rows (`page_tables`), within `PONDRA_WEB_SEND_MB` (`within`).
    async fn sent_for(&self, sql: &str, query: String, names: &BTreeSet<String>) -> anyhow::Result<Sent> {
        let (_, page) = self.default_schema()?;
        let tables = page_tables(page.as_ref(), names).await?;
        let bytes: usize = tables.iter().flat_map(|(_, batches)| batches).map(|b| b.get_array_memory_size()).sum();
        within(bytes, crate::env::or("PONDRA_WEB_SEND_MB", 64))?;
        let streams = tables.iter().map(|(schema, batches)| stream_of(schema, batches)).collect::<anyhow::Result<Vec<_>>>()?;
        let quoted: Vec<String> = names.iter().map(|name| crate::ext_names::quoted(name)).collect();
        let head = serde_json::to_vec(&json!({"sql": sql, "tables": quoted}))?;
        Ok(Sent { query, content_type: request_body::CONTENT_TYPE, body: request_body::write(&head, &streams) })
    }

    /// The statement parsed as the page's session parses it (its dialect and identifier rules).
    fn parsed(&self, expanded: &str) -> anyhow::Result<DfStatement> {
        let state = self.ctx.state();
        Ok(state.sql_to_statement(expanded, &state.config().options().sql_parser.dialect)?)
    }

    /// The tables a statement names, each with its home (`Home`), as the page reads them. Each is named as SQL wrote it,
    /// with no default schema or catalog the page's own names carry.
    fn named(&self, statement: &DfStatement) -> anyhow::Result<Vec<(Home, String)>> {
        let mut named = vec![];
        for name in self.ctx.state().resolve_table_references(statement)? {
            named.push((self.home(&name)?, name.table().to_string()));
        }
        Ok(named)
    }

    /// Where the table `name` is (`home_of`), with the page's default schema and catalog.
    fn home(&self, name: &TableReference) -> anyhow::Result<Home> {
        let state = self.ctx.state();
        let options = state.config().options();
        let (_, page) = self.default_schema()?;
        Ok(home_of(name, page.as_ref(), &options.catalog.default_catalog, &options.catalog.default_schema))
    }

    /// A table's name as the planner reads it: its identifiers normalized as the page's session normalizes them.
    fn table_ref(&self, name: &ObjectName) -> anyhow::Result<TableReference> {
        let normalize = self.ctx.state().config().options().sql_parser.enable_ident_normalization;
        Ok(datafusion::sql::planner::object_name_to_table_reference(name.clone(), normalize)?)
    }

    /// One request to the page's node, and its answer as the node gave it. A node that doesn't answer is 08001, its words
    /// saying which (`node` names it, and a browser's refusal reads the same).
    async fn ask(&self, sent: Sent) -> anyhow::Result<Answer> {
        let Some(node) = &self.node else { return Err(crate::codes::coded("08001", "the page has no node to ask")) };
        node(sent).await.map_err(|words| crate::codes::coded("08001", format!("the page's node didn't answer: {words}")))
    }

    /// A fill (`outgoing`): the node answers its query first, holding no turn, since asking changes nothing on the page. An
    /// answer that isn't 200 is the page's answer as it is (the node's refusal, its code and words), and nothing is made.
    /// A 200 is the node's rows (`node_rows`), and `write` is planned over them with the turn held, then run and kept as
    /// any write is (`run_planned`). Known and left as is: a CTAS without OR REPLACE over a table the page already has
    /// (IF NOT EXISTS does nothing, and the plain form is 42P07) still asks the node first.
    async fn filled(&self, write: &str, sent: Sent, format: Option<&str>) -> anyhow::Result<Answer> {
        let answered = self.ask(sent).await?;
        if answered.status != 200 {
            return Ok(answered);
        }
        let rows = node_rows(&answered)?;
        let turn = self.keeping.lock().await;
        let planned = self.planned_expanded(write, Some(rows)).await?;
        self.run_planned(turn, planned, format, false).await
    }

    /// A statement read as a node reads it, planned and not run: the front, ASOF JOIN's marker, then planned on the
    /// statement's own copy of the page's session (`statement`), with the files it names given to that copy alone
    /// (`given`). What a statement makes or changes (a table) is the page's; nothing else it registers reaches the page or
    /// another statement. The name of the page table it changes, if any (`touched`), comes back with the plan: `sql`
    /// keeps that table once the plan has run.
    async fn planned(&self, text: &str) -> anyhow::Result<(SessionContext, LogicalPlan, Option<String>)> {
        let row = crate::extension::row(text).transpose()?; // (DuckDB's INSTALL and LOAD: a row naming what happened, `extension.rs`)
        let text = row.as_deref().unwrap_or(text);
        let expanded = crate::front::expand(&PageHost(self), text, &HashMap::new()).await?;
        // (a page keeps no table's past, so `AT (…)` is refused by name: left alone, it would fail as an unknown table "at:…")
        if crate::past_names::mentioned(&expanded) {
            return Err(crate::codes::coded("0A000", format!("{}: a page keeps no table's past (a node reads a table as it was)", crate::past_names::spelled(&expanded))));
        }
        let expanded = crate::asof::as_of(&expanded)?.into_owned();
        self.planned_expanded(&expanded, None).await
    }

    /// `expanded`, which the front has run already, planned as `planned` plans it. With `rows` (a fill's node rows), they are
    /// the table `NODE_ROWS` for this statement alone (`Overlay::own`).
    async fn planned_expanded(&self, expanded: &str, rows: Option<Arc<dyn TableProvider>>) -> anyhow::Result<(SessionContext, LogicalPlan, Option<String>)> {
        rules(expanded)?; // (a table rule a page can't keep is refused before anything is planned or read)
        let (ctx, overlay) = self.statement()?;
        if let Some(rows) = rows {
            overlay.own(NODE_ROWS.to_string(), rows);
        }
        crate::explain::prepare(&ctx, expanded); // (an EXPLAIN's operators' expected rows, as a node's)
        self.given(&overlay, expanded).await?;
        let plan = crate::engine::planned(&ctx, expanded).await?;
        may(&plan)?;
        let changed = touched(&plan);
        Ok((ctx, plan, changed))
    }

    /// A statement planned (`planned`) and run: its frame, and the name of the page table it changes, if any. The page's
    /// `ask` and the tests read a page this way.
    async fn plan(&self, text: &str) -> anyhow::Result<(DataFrame, Option<String>)> {
        let (ctx, plan, changed) = self.planned(text).await?;
        Ok((crate::engine::run(&ctx, plan).await?, changed))
    }

    /// A write to a page table, run and kept before the next statement is planned (`keeping`). Every page takes the table as
    /// the write found it, kept or not, so a write that fails is undone (`undone`): one the watch stops (53200), one that
    /// fails as it runs (its own words), and one the store refuses (`written`: a full store, say; that one says the page
    /// couldn't keep it). An INSERT appends its rows only once it has them all, and a DELETE or UPDATE rewrites the table in
    /// one step, so a stop leaves those as they were; but a CREATE OR REPLACE TABLE … AS takes the old table away before it
    /// collects, and taking `before` copies no rows, so it is taken for every write.
    async fn write(&self, ctx: &SessionContext, plan: LogicalPlan, name: &str) -> anyhow::Result<Vec<RecordBatch>> {
        self.unwritable(name).await?;
        let before = self.before(name).await?;
        let batches = match rows_of(ctx, plan, self.watch.as_ref()).await {
            Ok(batches) => batches,
            Err(e) => return Err(self.undone(name, before, e, "").await),
        };
        match self.written(name).await {
            Ok(()) => Ok(batches),
            Err(e) => Err(self.undone(name, before, e, ": the page couldn't keep it").await),
        }
    }

    /// Refuses a write to a table whose kept file `kept` couldn't read (`unread`), while that file is in the store: a write
    /// keeps the table it leaves, or forgets a dropped one, over that same file, so the rows the file holds would be gone.
    /// Once the file is removed, the name is the page's again.
    async fn unwritable(&self, name: &str) -> anyhow::Result<()> {
        let at = kept_at(name);
        let Some((path, why)) = self.unread.iter().find(|(path, _)| path.as_str() == at.as_ref()) else { return Ok(()) };
        match self.files.head(&at).await {
            Err(StoreError::NotFound { .. }) => Ok(()),
            _ => Err(crate::codes::coded("55000", format!("{name}: the page couldn't read its kept file {path} ({why}), so the file is left as it is and the table can't be written until the file is removed"))),
        }
    }

    /// A failed write's error, once the write is undone (`undo`) to what it found (`before`), with `why` after the words
    /// saying so.
    async fn undone(&self, name: &str, before: Before, e: anyhow::Error, why: &str) -> anyhow::Error {
        match self.undo(name, before).await {
            Ok(()) => e.context(format!("{name}: the write was undone{why}")),
            Err(u) => e.context(format!("{name}: the write couldn't be undone ({u:#})")),
        }
    }

    /// The table `name` is now, with its rows, for `undo` to put back if the write to it can't be kept. Taken before the
    /// write runs, on every page: its partitions are `Arc`s of batches, so no rows are copied.
    async fn before(&self, name: &str) -> anyhow::Result<Before> {
        let (_, page) = self.default_schema()?;
        let Some(table) = page.table(name).await? else { return Ok(None) };
        let rows = partitions_of(&table).await.ok_or_else(|| anyhow::anyhow!("{name}: a page's table holds its rows in memory"))?;
        Ok(Some((table, rows)))
    }

    /// Puts the page table `name` back as `before` found it, after a write the page couldn't keep: the table the write
    /// made or changed is taken away, and the table it was (if there was one) is registered again with its rows put
    /// back. INSERT, UPDATE and DELETE change a memory table in place, so its partitions are written back; a DROP, or a
    /// CREATE OR REPLACE that took the old table away before it collected, left the old table itself as it was, so
    /// registering it again is all it takes.
    async fn undo(&self, name: &str, before: Before) -> anyhow::Result<()> {
        let (_, page) = self.default_schema()?;
        page.deregister_table(name)?; // (a name with no table is no error; the register below refuses a name that is there)
        if let Some((table, rows)) = before {
            put_back(&table, rows).await?;
            register_page_table(page.as_ref(), name.to_string(), inner(&table).clone())?;
        }
        Ok(())
    }

    /// The page's table a statement changed, once it has run: registered again, so the statistics `Counted` holds are
    /// worked out afresh (a write leaves them out of date), and, when the page was given a store, kept as one Arrow file
    /// under `tables/` (`kept_at`) that `kept` reads back. A table the statement dropped has neither, so its file goes. A
    /// page in memory keeps nothing: a copy of each table beside it would double what it holds.
    async fn written(&self, name: &str) -> anyhow::Result<()> {
        let (_, page) = self.default_schema()?;
        let Some(table) = page.table(name).await? else {
            return if self.keeps { self.forget(name).await } else { Ok(()) };
        };
        page.deregister_table(name)?;
        register_page_table(page.as_ref(), name.to_string(), inner(&table).clone())?;
        if !self.keeps {
            return Ok(());
        }
        let batches = batches_of(&table).await.ok_or_else(|| anyhow::anyhow!("{name}: a page's table holds its rows in memory"))?;
        let file = table_file(name, table.as_ref(), &batches)?;
        self.files.put_opts(&kept_at(name), PutPayload::from(file), PutOptions { mode: PutMode::Overwrite, ..Default::default() }).await?;
        Ok(())
    }

    /// A table the statement dropped: its file goes. A table that was never kept has none, which is fine.
    async fn forget(&self, name: &str) -> anyhow::Result<()> {
        match self.files.delete(&kept_at(name)).await {
            Ok(()) | Err(StoreError::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Reads the kept table at `at` back into the page, as a table of its default schema. A file that doesn't read, or
    /// that is not a kept table, fails with the words of why and no path: `kept` lists the file by its path beside them.
    async fn restore(&self, at: &Path) -> anyhow::Result<()> {
        let file = self.files.get(at).await?.bytes().await?;
        let (name, table) = self.table_from_file(file)?;
        let (_, page) = self.default_schema()?;
        register_page_table(page.as_ref(), name, Arc::new(table))?;
        Ok(())
    }

    /// The table a kept file holds, read back: its name (`pondra.table`), its rows as a memory table over the table's own
    /// schema, and its column defaults again (`column_defaults`).
    fn table_from_file(&self, file: bytes::Bytes) -> anyhow::Result<(String, MemTable)> {
        let reader = FileReader::try_new(std::io::Cursor::new(file), None)?;
        let kept = reader.schema();
        let name = kept.metadata().get(TABLE_KEY).cloned().ok_or_else(|| anyhow::anyhow!("it has no {TABLE_KEY} note, so it is not a kept table"))?;
        let schema = table_schema(&kept);
        let defaults = self.column_defaults(&kept, &schema)?;
        let mut batches = vec![];
        for batch in reader {
            batches.push(over_schema(&schema, &batch?)?);
        }
        Ok((name, MemTable::try_new(schema, vec![batches])?.with_column_defaults(defaults)))
    }

    /// The column defaults a kept file's notes give (`pondra.default`), each read as SQL over the table's schema.
    fn column_defaults(&self, kept: &Schema, schema: &SchemaRef) -> anyhow::Result<HashMap<String, Expr>> {
        let (state, df) = (self.ctx.state(), DFSchema::try_from(schema.clone())?);
        let mut defaults = HashMap::new();
        for field in kept.fields() {
            if let Some(text) = field.metadata().get(DEFAULT_KEY) {
                defaults.insert(field.name().clone(), state.create_logical_expr(text, &df)?);
            }
        }
        Ok(defaults)
    }

    /// The name of the page's default schema, and its provider: the tables a statement is planned over.
    fn default_schema(&self) -> anyhow::Result<(String, Arc<dyn SchemaProvider>)> {
        let state = self.ctx.state();
        let options = state.config().options();
        let (catalog, schema) = (&options.catalog.default_catalog, options.catalog.default_schema.clone());
        let page = self.ctx.catalog(catalog).and_then(|c| c.schema(&schema)).ok_or_else(|| anyhow::anyhow!("a page has its default schema"))?;
        Ok((schema, page))
    }

    /// A statement's session: a copy of the page's, whose default schema is an `Overlay` over the page's own, so the
    /// files the statement names are its alone. The copy's state is the page's, cloned: its functions, settings and
    /// object store come with it, so nothing is registered again.
    fn statement(&self) -> anyhow::Result<(SessionContext, Arc<Overlay>)> {
        let (schema, page) = self.default_schema()?;
        let overlay = Arc::new(Overlay { page, own: Default::default() });
        let default = MemoryCatalogProvider::new();
        default.register_schema(&schema, overlay.clone())?;
        Ok((crate::engine::with_catalog(self.ctx.state(), Arc::new(default)), overlay))
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
        if !["parquet", "csv", "json", "arrow", "delta"].contains(&spec.format.as_str()) {
            return Err(crate::codes::coded("0A000", format!("a page reads Parquet, CSV, JSON and Arrow files and Delta tables, not {} tables", spec.format)));
        }
        // (the declared types of the formats whose files they type: a node's `columns` and `hive_types`, as a node reads them)
        let declares = ["csv", "json", "parquet"].contains(&spec.format.as_str());
        let takes = |k: &str| k == "hive_partitioning" || (spec.format == "csv" && CSV_OPTIONS.contains(&k)) || (spec.format == "delta" && k == "version") || (declares && (k == "columns" || k == "hive_types"));
        if let Some(k) = spec.options.keys().find(|k| !takes(k)) {
            return Err(crate::codes::coded("0A000", format!("a page doesn't take {k} yet")));
        }
        let state = self.ctx.state();
        let urls = spec.urls.iter().map(|u| located(u)).collect::<anyhow::Result<Vec<_>>>()?;
        let given = crate::ext_names::Spec { urls, ..spec.clone() };
        // (a file not put is named as SQL wrote it: the URL at the same place in `spec.urls`, not the store's form)
        let written = |u: &str| given.urls.iter().position(|g| g == u).map_or(u.to_string(), |i| spec.urls[i].clone());
        self.reach_web(&given, &written).await?;
        if spec.format == "delta" {
            return self.delta(overlay, name, &given).await;
        }
        // The declared types are read in a copy of the page's session, so their dialect is the page's, as a node's is its
        // own. (Its scratch table needs the default catalog, which the page's own session registers itself.)
        let session = || SessionContext::new_with_config(self.ctx.copied_config().with_create_default_catalog_and_schema(true));
        let declared = match given.options.get("columns") {
            Some(c) => Some(crate::ext_names::declared(session(), c).await?),
            None => None,
        };
        let hive_types = match given.options.get("hive_types") {
            Some(c) => crate::ext_names::declared(session(), c).await?,
            None => vec![],
        };
        let undeclared = declared.is_none();
        let (columns, data) = crate::ext_names::files_table(&state, &given, declared, &hive_types, |u| {
            undeclared.then(|| crate::codes::coded("42P01", format!("no file {} was given to this page (PUT /files/{{path}})", written(u))))
        })
        .await?;
        let schema = crate::answer::schema(&columns)?;
        let refs: Vec<_> = data.iter().collect();
        let table: Arc<dyn TableProvider> = if refs.is_empty() {
            Arc::new(MemTable::try_new(schema, vec![vec![]])?) // (columns declared and no file put yet: an empty table with them, as a node answers)
        } else {
            crate::ext_names::read(&self.ctx, &refs, &schema, &given).await?.into_view()
        };
        overlay.own(name.to_string(), table); // (a name is one table: the statement's own map)
        Ok(())
    }

    /// The store a URL is in, and the object's path there. A page's own files are in the `mem://` store it was opened
    /// with. A web address's origin has no store until a statement names it: the program's `web` makes one, which is
    /// registered in the session, so the reads of that origin's files find it too.
    fn reached(&self, url: &str) -> anyhow::Result<(Arc<dyn ObjectStore>, Path)> {
        let at = ListingTableUrl::parse(url)?;
        match self.ctx.runtime_env().object_store(at.object_store()) {
            Ok(store) => return Ok((store, at.prefix().clone())),
            Err(e) if !is_web(url) => return Err(e.into()),
            Err(_) => {}
        }
        let Some(web) = &self.web else {
            return Err(crate::codes::coded("0A000", format!("{url}: this page reads no web addresses (its program gave it no way to fetch)")));
        };
        let origin = at.object_store();
        let store = web(origin.as_str().trim_end_matches('/'));
        self.ctx.register_object_store(origin.as_ref(), store.clone());
        Ok((store, at.prefix().clone()))
    }

    /// The web addresses a statement names, reached before anything reads them: each origin's store is in the session
    /// (`reached`), so the files' reads find it. A web server lists nothing, so a folder or a glob is refused by name,
    /// and a file is asked for by `head` first, so one that isn't there is 42P01 by the name SQL wrote. A Delta table
    /// is read from its log, not listed: only its origin is reached.
    async fn reach_web(&self, spec: &crate::ext_names::Spec, written: &impl Fn(&str) -> String) -> anyhow::Result<()> {
        for u in spec.urls.iter().filter(|u| is_web(u)) {
            if spec.format != "delta" && (u.ends_with('/') || crate::ext_names::glob_at(u).is_some()) {
                return Err(crate::codes::coded("0A000", format!("{}: a web server lists nothing, so a page reads the files it names (not a folder or a glob)", written(u))));
            }
            let (store, at) = self.reached(u)?;
            if spec.format == "delta" {
                continue;
            }
            match store.head(&at).await {
                Ok(_) => {}
                Err(StoreError::NotFound { .. }) => return Err(crate::codes::coded("42P01", format!("no file {} at that web address", written(u)))),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// A Delta table (`read_delta`) at the version asked, or its latest: read from its log as a node reads it
    /// (`read_delta::resolve`, over this page's own `Reach`), its columns as SQL names them (`delta_table`).
    async fn delta(&self, overlay: &Overlay, name: &str, given: &crate::ext_names::Spec) -> anyhow::Result<()> {
        let version = crate::ext_names::version(given)?;
        let found = crate::read_delta::resolve(&PageReach(self), given.urls[0].trim_end_matches('/'), version).await?;
        overlay.own(name.to_string(), delta_table(&self.ctx, found).await?);
        Ok(())
    }

    /// `PUT /files/{path}` (the node's rules, `server::put_file`): the bytes kept as given, under `files/`, and no table
    /// made. Without `If-Match` a path that is there is 409; with it, the file is replaced if it is still that version
    /// (quotes and all), and 412 if not.
    async fn put(&self, name: &str, headers: &[(String, String)], body: bytes::Bytes) -> anyhow::Result<Answer> {
        let path = under_files(name)?;
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
        let path = under_files(name)?;
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
        let path = under_files(name)?;
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
        let (_, page) = self.default_schema()?;
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

/// Where a file a statement names is, as a page's store has it: a URL beginning `mem:///files/` as written, a web address
/// (`http://`, `https://`) as written (`reached` finds its origin's store), a relative path (`'x.csv'`, `'./d/*.parquet'`)
/// in the working folder (`lake_files::relative`, as a node reads a client's), and any other URL refused by name: a page
/// reads the files it was given and the web. A `mem:` name that leaves `files/` (`..`, `%2E%2E`) is refused too: the
/// page's kept tables are not SQL's to read.
fn located(url: &str) -> anyhow::Result<String> {
    if is_web(url) {
        return Ok(url.to_string());
    }
    let made = if url.starts_with(WORKING) {
        url.to_string()
    } else {
        match crate::lake_files::relative("mem:///", url) {
            Some(r) => r?,
            None => return Err(files_or_web(url)), // (a URL with a scheme, or an absolute path: a page has no disk)
        }
    };
    // (`Url::parse` takes `..` out of a path, so the name is judged once parsed: `mem:///files/../tables/t` is `/tables/t`)
    if !url::Url::parse(&made)?.path().starts_with(FILES) {
        return Err(files_or_web(url));
    }
    Ok(made)
}

/// The refusal of a URL a page doesn't read: the same words for each such URL, whichever way it is found to be one.
fn files_or_web(url: &str) -> anyhow::Error {
    crate::codes::coded("0A000", format!("{url}: a page reads the files given to it (PUT /files/{{path}}) and the web (http, https)"))
}

/// The file a page's table is kept in: `tables/{name}.arrow`. The name is one part of the path, so object_store encodes
/// what a path can't hold (a `/` or a `?`): any name a table can have is one file.
fn kept_at(name: &str) -> Path {
    Path::from_iter([TABLES, format!("{name}.arrow").as_str()])
}

/// A planned statement run, and its rows collected, held to the page's memory watch (`watched`). A CREATE TABLE … AS
/// collects inside its run, so the run and the collection are one future.
async fn rows_of(ctx: &SessionContext, plan: LogicalPlan, watch: Option<&Watch>) -> anyhow::Result<Vec<RecordBatch>> {
    watched(watch, async move { crate::engine::collect(crate::engine::run(ctx, plan).await?).await }).await
}

/// `inner`, held to the page's memory watch: at each poll, before `inner` is polled, a page past its line and grown by a
/// tenth of the line since the first poll is stopped. The query is answered 53200 and its future dropped, so its memory goes
/// with it, while the page answers the rest. Growth is what the query did, not what the page holds: a query that starts past
/// the line and grows nothing is not stopped. A page's query yields between batches (DataFusion's cooperative streams, which
/// `web/`'s build turns on), so the watch gets to look between them. With no watch, `inner` runs as it is.
async fn watched<T>(watch: Option<&Watch>, inner: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    let Some(watch) = watch else { return inner.await };
    let margin = watch.line / 10; // (`oom::pick`'s margin, a node's)
    let mut inner = Some(Box::pin(inner));
    let mut started = None; // (the reading at the first poll: what the query grew from)
    std::future::poll_fn(move |cx| {
        let now = (watch.live)();
        let from = *started.get_or_insert(now);
        if now >= watch.line && now.saturating_sub(from) >= margin {
            inner = None; // (the query's memory goes with its future, before anything else is read)
            return Poll::Ready(Err(stopped()));
        }
        match inner.as_mut() {
            Some(future) => future.as_mut().poll(cx),
            None => Poll::Ready(Err(stopped())), // (never polled once it has answered: an answer is the stop's)
        }
    })
    .await
}

/// The answer to a query the watch stopped (`watched`).
fn stopped() -> anyhow::Error {
    crate::codes::coded("53200", PAGE_OOM)
}

/// The page's memory line in bytes: `PONDRA_MEMORY_MAX_GB`, 3 GiB by default (`LINE`), and None when it is 0 (the watch is off,
/// as on a node).
fn memory_line() -> Option<usize> {
    let bytes = crate::env::gib("PONDRA_MEMORY_MAX_GB").unwrap_or(LINE);
    (bytes > 0).then_some(bytes)
}

/// The most query memory a page's pool holds: `PONDRA_MEMORY_GB`, else two thirds of the line (of the default line when the
/// watch is off, so the pool is never empty). What DataFusion counts is the big hash tables and sort buffers, so the rest of
/// the line is for what it doesn't count.
fn query_memory() -> usize {
    crate::env::gib("PONDRA_MEMORY_GB").unwrap_or_else(|| memory_line().unwrap_or(LINE) / 3 * 2)
}

/// A table's batches, partition by partition, as its memory table holds them (`inner`); None for another provider's table.
async fn partitions_of(table: &Arc<dyn TableProvider>) -> Option<Vec<Vec<RecordBatch>>> {
    let memory = inner(table).downcast_ref::<MemTable>()?;
    let mut partitions = vec![];
    for partition in &memory.batches {
        partitions.push(partition.read().await.to_vec());
    }
    Some(partitions)
}

/// A table's batches, every partition's, as `partitions_of` gives them.
async fn batches_of(table: &Arc<dyn TableProvider>) -> Option<Vec<RecordBatch>> {
    Some(partitions_of(table).await?.concat())
}

/// Puts a page table's rows back, partition by partition, as `rows` had them. A write changes its memory table in place
/// (`inner`), so this undoes one (`undo`); a table that is not a memory table has no rows to put back, and is refused.
async fn put_back(table: &Arc<dyn TableProvider>, rows: Vec<Vec<RecordBatch>>) -> anyhow::Result<()> {
    let memory = inner(table).downcast_ref::<MemTable>().ok_or_else(|| anyhow::anyhow!("a page's table holds its rows in memory"))?;
    for (partition, batches) in memory.batches.iter().zip(rows) {
        *partition.write().await = batches;
    }
    Ok(())
}

/// A page table as one Arrow IPC file: its batches over the file's schema (`kept_schema`), so `kept` reads the table
/// back as it was.
fn table_file(name: &str, table: &dyn TableProvider, batches: &[RecordBatch]) -> anyhow::Result<bytes::Bytes> {
    let schema = kept_schema(name, table)?;
    let mut writer = FileWriter::try_new(Vec::new(), &schema)?;
    for batch in batches {
        writer.write(&over_schema(&schema, batch)?)?;
    }
    Ok(writer.into_inner()?.into())
}

/// The schema a table is kept under: its own, with `pondra.table` (its name) in the schema's notes, and `pondra.default`
/// (the column's default, as SQL text) in the notes of each column that has one.
fn kept_schema(name: &str, table: &dyn TableProvider) -> anyhow::Result<SchemaRef> {
    let mut fields = vec![];
    for field in table.schema().fields() {
        let mut notes = field.metadata().clone();
        if let Some(default) = table.get_column_default(field.name()) {
            notes.insert(DEFAULT_KEY.to_string(), datafusion::sql::unparser::expr_to_sql(default)?.to_string());
        }
        fields.push(field.as_ref().clone().with_metadata(notes));
    }
    let mut notes = table.schema().metadata().clone();
    notes.insert(TABLE_KEY.to_string(), name.to_string());
    Ok(Arc::new(Schema::new_with_metadata(fields, notes)))
}

/// The schema of a kept file without the two notes `kept_schema` added: the schema the table itself has.
fn table_schema(kept: &Schema) -> SchemaRef {
    let fields: Vec<Field> = kept.fields().iter().map(|f| f.as_ref().clone().with_metadata(without_notes(f.metadata()))).collect();
    Arc::new(Schema::new_with_metadata(fields, without_notes(kept.metadata())))
}

/// The notes without the two `kept_schema` adds, which a table's own schema does not have.
fn without_notes(notes: &HashMap<String, String>) -> HashMap<String, String> {
    notes.iter().filter(|(k, _)| ![TABLE_KEY, DEFAULT_KEY].contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// The rows a node answered a fill's query with, as a table the fill is planned over (`filled`): the rows of an Arrow stream,
/// with the schema it names even when it has no rows. Any other answer is 08P01, and so is a stream that doesn't read. There
/// is no size cap here: the page's memory line (a separate piece) stops a page that grows too far.
fn node_rows(answer: &Answer) -> anyhow::Result<Arc<dyn TableProvider>> {
    let said = answer.header("content-type");
    if said != Some(crate::answer::content_type(Some("arrow"))) {
        return Err(crate::codes::coded("08P01", format!("the page's node answered {}, not Arrow rows", said.unwrap_or("no content-type"))));
    }
    let (schema, batches) = stream_rows(answer.body.clone()).map_err(|e| crate::codes::coded("08P01", format!("the page's node answered Arrow rows that don't read: {e:#}")))?;
    Ok(Arc::new(MemTable::try_new(schema, vec![batches])?))
}

/// The schema and the batches of an Arrow IPC stream, read as they are: the body becomes the stream's buffer without a copy.
fn stream_rows(body: bytes::Bytes) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)> {
    let mut buffer = Buffer::from(body);
    let mut decoder = StreamDecoder::new();
    let mut batches = vec![];
    while !buffer.is_empty() {
        batches.extend(decoder.decode(&mut buffer)?);
    }
    decoder.finish()?;
    let schema = decoder.schema().context("an Arrow stream with no schema")?;
    Ok((schema, batches))
}

/// `batch` over `schema`: the same columns, under the schema's notes. A table's batches carry the table's schema exactly,
/// as `MemTable::try_new` refuses batches whose notes the table's lack.
fn over_schema(schema: &SchemaRef, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    Ok(RecordBatch::try_new(schema.clone(), batch.columns().to_vec())?)
}

/// Whether a URL is a web address, which a page reads through the store its program gives it (`Page::reached`).
fn is_web(url: &str) -> bool {
    matches!(crate::ext_names::scheme(url), Some("http" | "https"))
}

/// A page's way to read another engine's files: `read_delta` reads a log through it. The store a URL is in is
/// `Page::reached`'s, and the files are read by the page's session.
struct PageReach<'a>(&'a Page);

impl crate::reach::Reach for PageReach<'_> {
    fn store(&self, url: &str) -> impl Future<Output = anyhow::Result<(Arc<dyn ObjectStore>, Path)>> + Send {
        std::future::ready(self.0.reached(url))
    }

    fn session(&self) -> SessionContext {
        self.0.ctx.clone()
    }
}

/// A Delta table at the version read, as SQL names its columns. A column the table renames (column mapping) is
/// read under its physical name and renamed to its SQL name. A table with no files is empty, with its columns, as a
/// node reads one.
async fn delta_table(ctx: &SessionContext, found: crate::read_delta::Found) -> anyhow::Result<Arc<dyn TableProvider>> {
    if found.files.is_empty() {
        let columns: Vec<(String, String)> = found.columns.iter().map(|(c, t)| (found.names.get(c).unwrap_or(c).clone(), t.clone())).collect();
        return Ok(Arc::new(MemTable::try_new(crate::answer::schema(&columns)?, vec![vec![]])?));
    }
    let files: Vec<&crate::data_file::DataFile> = found.files.iter().collect();
    let df = crate::scan::read(ctx, &files, &crate::answer::schema(&found.columns)?, None).await?;
    Ok(crate::answer::renamed(df, |c| found.names.get(c).map_or(c, String::as_str).to_string(), |_| true)?.into_view())
}

/// A table's rows and bytes, as its batches hold them: a memory table's, which is what a page's tables are. None for
/// another provider, whose size is not known here.
async fn sizes(table: &Arc<dyn TableProvider>) -> Option<(usize, usize)> {
    let batches = batches_of(table).await?;
    Some(batches.iter().fold((0, 0), |(rows, bytes), b| (rows + b.num_rows(), bytes + b.get_array_memory_size())))
}

/// Where a table a statement names is: one of the page's own tables, one of its files (an `ext:` name, as the front wrote
/// it), or the node's.
#[derive(Debug, PartialEq)]
enum Home {
    Page,
    File,
    Node,
}

/// The home of the table `name` names. A name carries the page's default schema and catalog, or none, and is the page's
/// when its table is one of the page's; a file is any `ext:` name; every other table is the node's.
fn home_of(name: &TableReference, page: &dyn SchemaProvider, catalog: &str, schema: &str) -> Home {
    if crate::ext_names::is(name.table()) {
        return Home::File;
    }
    let here = name.schema().is_none_or(|s| s == schema) && name.catalog().is_none_or(|c| c == catalog);
    if here && page.table_exist(name.table()) {
        Home::Page
    } else {
        Home::Node
    }
}

/// The query a statement is, for a page to send its node: a `SELECT`, `WITH`, `VALUES` or `EXPLAIN` of one. A write is
/// not one (`writes`), and nor is any other statement (`CREATE`, `INSERT`, `COPY` …): a node writes its own tables.
fn query_of(statement: &DfStatement) -> Option<&Query> {
    match statement {
        DfStatement::Statement(s) => match &**s {
            SqlStatement::Query(q) if !writes(q) => Some(q),
            _ => None,
        },
        DfStatement::Explain(e) => query_of(&e.statement),
        _ => None,
    }
}

/// A query as a statement, for `named` to read the names it holds.
fn as_statement(query: &Query) -> DfStatement {
    DfStatement::Statement(Box::new(SqlStatement::Query(Box::new(query.clone()))))
}

/// The page's own tables among a query's names (`named`), sorted, each once. A file among them is refused by name: a query
/// a page sends names the page's tables and the node's, and a file is the page's to read, not the node's.
fn page_names(named: Vec<(Home, String)>) -> anyhow::Result<BTreeSet<String>> {
    if named.iter().any(|(home, _)| *home == Home::File) {
        return Err(crate::codes::coded("0A000", "a query a page sends its node names the page's tables and the node's, not the page's files: make the file a table first (CREATE TABLE t AS FROM 'file')"));
    }
    Ok(named.into_iter().filter(|(home, _)| *home == Home::Page).map(|(_, name)| name).collect())
}

/// Whether a query writes: its body is a write (`text::write_of`, a `WITH … INSERT` among them), or it is a `SELECT … INTO`
/// that makes a table, or one of its CTEs writes so.
fn writes(q: &Query) -> bool {
    crate::text::write_of(q).is_some()
        || matches!(&*q.body, SetExpr::Select(s) if s.into.is_some())
        || q.with.as_ref().is_some_and(|w| w.cte_tables.iter().any(|c| writes(&c.query)))
}

/// The page's tables `names` name, each with its schema and its batches: its rows as the page holds them. They are read
/// before anything is sent, so the bytes a query sends are counted first (`within`).
async fn page_tables(page: &dyn SchemaProvider, names: &BTreeSet<String>) -> anyhow::Result<Vec<(SchemaRef, Vec<RecordBatch>)>> {
    let mut tables = vec![];
    for name in names {
        let table = page.table(name).await?.ok_or_else(|| anyhow::anyhow!("{name}: a page's table is not there"))?;
        let batches = batches_of(&table).await.ok_or_else(|| anyhow::anyhow!("{name}: a page's table holds its rows in memory"))?;
        tables.push((table.schema(), batches));
    }
    Ok(tables)
}

/// A table's batches as one Arrow IPC stream over `schema`. A table with no rows sends one empty batch of its schema as
/// well: the node reads a table's columns from the batches it receives, and a stream with none has no columns to read.
fn stream_of(schema: &SchemaRef, batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema)?;
    if batches.is_empty() {
        writer.write(&RecordBatch::new_empty(schema.clone()))?;
    }
    for batch in batches {
        writer.write(&over_schema(schema, batch)?)?;
    }
    Ok(writer.into_inner()?)
}

/// The bytes of a page's tables a query sends its node, within `limit_mb` MiB (`PONDRA_WEB_SEND_MB`). The rows go in one
/// request, held whole in the page's memory, so a query past the limit is refused before anything is written.
fn within(bytes: usize, limit_mb: u64) -> anyhow::Result<()> {
    const MB: u64 = 1024 * 1024;
    let bytes = bytes as u64;
    if bytes <= limit_mb.saturating_mul(MB) {
        return Ok(());
    }
    Err(crate::codes::coded("54000", format!("this query names {} MB of the page's tables, past the {limit_mb} MB a page sends its node (web_send_mb)", bytes.div_ceil(MB))))
}

/// A page's table: its rows a `MemTable`, and what the join order needs to know of them, worked out the first time it
/// asks (as `query::Pruned::sized` is). The rule asks only when three or more inputs are joined, so a table never joined
/// so costs its load nothing. A write (INSERT, UPDATE, DELETE) goes to the rows; the statistics it leaves stale are
/// worked out again, because the table is registered again after the write (`written`).
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

    async fn insert_into(&self, state: &dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.rows.insert_into(state, input, insert_op).await
    }

    async fn delete_from(&self, state: &dyn Session, filters: Vec<Expr>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.rows.delete_from(state, filters).await
    }

    async fn update(&self, state: &dyn Session, assignments: Vec<(String, Expr)>, filters: Vec<Expr>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.rows.update(state, assignments, filters).await
    }

    /// The statistics once worked out; else worked out now from the rows, if no write holds them. A write may hold the
    /// rows while it runs; then the answer is None, as it is for anything that fails, and nothing is kept.
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

/// The table rules a page can't keep, refused by name before planning (ADR-072: a form that can't do something says
/// so). A page's tables are `MemTable`s, which keep only NOT NULL and DEFAULT: DataFusion would note a PRIMARY KEY and
/// take any rows, and drop an identity or a generated column without a word.
fn rules(sql: &str) -> anyhow::Result<()> {
    use datafusion::sql::parser::DFParser;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    // (the same cheap gate `asof.rs` uses: a statement with no CREATE in it makes no table to judge)
    if !sql.to_ascii_lowercase().contains("create") {
        return Ok(());
    }
    // (a parse error is the planner's to answer, as it always has)
    let Ok(statements) = DFParser::parse_sql_with_dialect(sql, &GenericDialect {}) else { return Ok(()) };
    statements.iter().try_for_each(statement_rules)
}

/// One statement's table rules: a CREATE TABLE's, and an EXPLAIN's (an EXPLAIN of a CREATE TABLE is refused as the
/// CREATE TABLE is).
fn statement_rules(statement: &datafusion::sql::parser::Statement) -> anyhow::Result<()> {
    use datafusion::sql::parser::Statement as DfStatement;
    use datafusion::sql::sqlparser::ast::Statement;
    match statement {
        DfStatement::Statement(s) => match s.as_ref() {
            Statement::CreateTable(c) => table_rules(c),
            _ => Ok(()),
        },
        DfStatement::Explain(e) => statement_rules(&e.statement),
        _ => Ok(()),
    }
}

/// A CREATE TABLE's rules: its first table constraint, else its first column option a page doesn't keep, each named as
/// written. Nothing is planned for it, so the table is not made.
fn table_rules(c: &datafusion::sql::sqlparser::ast::CreateTable) -> anyhow::Result<()> {
    if let Some(constraint) = c.constraints.first() {
        return Err(not_kept(&constraint.to_string()));
    }
    match c.columns.iter().flat_map(|column| &column.options).find(|o| !kept_option(&o.option)) {
        Some(o) => Err(not_kept(&o.option.to_string())),
        None => Ok(()),
    }
}

/// The column options a page keeps: NULL and NOT NULL (enforced), DEFAULT (filled in), and COMMENT (which changes no row,
/// so it is let by).
fn kept_option(option: &datafusion::sql::sqlparser::ast::ColumnOption) -> bool {
    use datafusion::sql::sqlparser::ast::ColumnOption;
    matches!(option, ColumnOption::Null | ColumnOption::NotNull | ColumnOption::Default(_) | ColumnOption::Comment(_))
}

/// The refusal for a table rule a page can't keep: 0A000, with the words that name it and say what to do.
fn not_kept(what: &str) -> anyhow::Error {
    crate::codes::coded("0A000", format!("a page doesn't keep {what} yet: its tables keep only NOT NULL and DEFAULT, so they would take any rows; make the table without it, or keep it on a node"))
}

/// The statements a page doesn't take yet, refused by name before anything is parsed (ADR-072: a form that can't do a thing
/// says so, and says what to write instead). A transaction word, DECLARE, MERGE and the type statements each name what to
/// write in its place; DataFusion's own words for them name nothing of the kind. Words are whole and read past comments, so
/// a table named `merged` or a string `'begin'` is not one of them.
fn not_taken(sql: &str) -> anyhow::Result<()> {
    if let Some(word) = crate::text::control(sql) {
        return Err(crate::codes::coded("0A000", format!("a page has no transactions yet ({word}): each statement it runs is its own, so run them one at a time, or in a transaction on a node")));
    }
    let words = crate::text::first_words(sql, 4);
    if begins(&words, &["DECLARE"]) {
        return Err(crate::codes::coded("0A000", "a page has no variables yet (DECLARE): write the value into the statement, or declare it on a node"));
    }
    if begins(&words, &["MERGE"]) {
        return Err(crate::codes::coded("0A000", "a page doesn't take MERGE yet: write its INSERT, UPDATE and DELETE into the page's table, or MERGE on a node"));
    }
    match TYPE_STATEMENTS.into_iter().find(|pattern| begins(&words, pattern)) {
        Some(pattern) => Err(crate::codes::coded("0A000", format!("a page makes no types yet ({}): make the column text, or keep the type on a node", pattern.join(" ")))),
        None => Ok(()),
    }
}

/// The words of the statements that make or drop a type, as `not_taken` reads them.
const TYPE_STATEMENTS: [&[&str]; 4] = [&["CREATE", "TYPE"], &["CREATE", "OR", "REPLACE", "TYPE"], &["DROP", "TYPE"], &["ALTER", "TYPE"]];

/// Whether `words` begin with `pattern`'s words (`text::first_words` gives them upper-cased, as a pattern is written).
fn begins(words: &[String], pattern: &[&str]) -> bool {
    words.len() >= pattern.len() && words.iter().zip(pattern).all(|(word, p)| word == p)
}

/// What a page's SQL may do. A page's tables are its own, in memory: `CREATE TABLE` (with `AS`, `OR REPLACE` and `IF
/// NOT EXISTS`), `DROP TABLE`, and INSERT, UPDATE and DELETE into a page's own table are what it takes. `CREATE EXTERNAL
/// TABLE` is refused by name: a native page has `file://` registered, so it would read its machine's disk. A write into
/// anything else (a file, a view), COPY, and every other DDL and statement, is refused as a node's read-only SQL refuses
/// it.
fn may(plan: &LogicalPlan) -> anyhow::Result<()> {
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(_) | DdlStatement::DropTable(_)) => Ok(()),
        LogicalPlan::Ddl(ddl) => Err(crate::codes::coded("0A000", format!("a page doesn't take {} yet", ddl.name()))),
        LogicalPlan::Dml(d) if writes_rows(&d.op) => own_table(&d.target),
        // (EXPLAIN ANALYZE runs what it explains, so the statement it explains is judged, as if it were run)
        LogicalPlan::Analyze(a) => may(&a.input),
        _ => {
            crate::engine::read_only().verify_plan(plan)?; // (no DDL is left here: a node's read-only SQL)
            Ok(())
        }
    }
}

/// Whether a DML statement writes rows: INSERT, UPDATE or DELETE, the three a page's own tables take.
fn writes_rows(op: &WriteOp) -> bool {
    matches!(op, WriteOp::Insert(_) | WriteOp::Update | WriteOp::Delete)
}

/// A write's target, when it is one of a page's own tables. Only a page's memory tables are `Counted`, so a file or a
/// view, which is some other provider, is refused by name (its name is left out: a file's is SQL's internal one).
fn own_table(target: &Arc<dyn TableSource>) -> anyhow::Result<()> {
    match source_as_provider(target) {
        Ok(table) if table.is::<Counted>() => Ok(()),
        _ => Err(crate::codes::coded("0A000", "a page writes only its own tables")),
    }
}

/// The name of the page table a plan makes, changes or drops, if it does: what `sql` keeps (`written`) once the plan has
/// run. An EXPLAIN ANALYZE changes what it runs, so it is its input's.
fn touched(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(c)) => Some(c.name.table().to_string()),
        LogicalPlan::Ddl(DdlStatement::DropTable(d)) => Some(d.name.table().to_string()),
        LogicalPlan::Dml(d) if writes_rows(&d.op) => Some(d.table_name.table().to_string()),
        LogicalPlan::Analyze(a) => touched(&a.input),
        _ => None,
    }
}

/// `table` registered as one of a page's own tables: as `Counted`, so its statistics are worked out once and held.
/// Both a table a statement makes and one `written` registers again go through here.
fn register_page_table(page: &dyn SchemaProvider, name: String, table: Arc<dyn TableProvider>) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
    page.register_table(name, Counted::over(table))
}

/// The table a page's table holds its rows in: a `Counted`'s memory table, or the table itself when it is another
/// provider's. Every reader of a page table's rows goes through here, so the downcast is written once.
fn inner(table: &Arc<dyn TableProvider>) -> &Arc<dyn TableProvider> {
    table.downcast_ref::<Counted>().map_or(table, |c| &c.rows)
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
        register_page_table(self.page.as_ref(), name, table)
    }

    fn deregister_table(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        self.page.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.own.read().unwrap_or_else(PoisonError::into_inner).contains_key(name) || self.page.table_exist(name)
    }
}

/// What a page's SQL asks of where it runs (`front::Host`). The front runs the passes itself, as it does for a node: DuckDB's
/// forms that ask the data (`PIVOT`, `COLUMNS`, `SUMMARIZE`) ask the page through `ask`, as a node asks its own. A page keeps
/// no statements, routines, attached lakes, views with measures or types of its own: their CREATEs are refused by name
/// (`may`), so it answers each of those lookups with none, as a node that has none would. The columns are its tables' own.
struct PageHost<'a>(&'a Page);

impl Host for PageHost<'_> {
    fn name(&self) -> String {
        "page".to_string()
    }

    // (the front asks the host first, before anything is parsed: a statement a page doesn't take is refused here, in the
    // words that say what to write instead, as a node refuses what it doesn't take, and not in DataFusion's)
    async fn own(&self, sql: &str) -> anyhow::Result<Own> {
        not_taken(sql)?;
        Ok(Own::Go(sql.to_string()))
    }

    async fn routines(&self) -> anyhow::Result<Arc<HashMap<String, Routine>>> {
        Ok(Arc::default())
    }

    async fn attached(&self) -> anyhow::Result<Vec<(String, Attached)>> {
        Ok(vec![])
    }

    async fn measure_views(&self, _sql: &str) -> anyhow::Result<Arc<HashMap<String, Arc<Definition>>>> {
        Ok(Arc::default())
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

    // (a page table's columns carry no Postgres type: a MemTable holds Arrow types, and the page keeps no view to trace)
    async fn relation(&self, name: &str) -> Option<Relation<Self>> {
        let table = self.0.ctx.table_provider(name).await.ok()?;
        Some(Relation::Columns(table.schema().fields().iter().map(|f| (f.name().clone(), None)).collect()))
    }

    // (a page makes no types, so no enum has labels here: an enum a page names is refused at its CREATE, by name)
    async fn enum_labels(&self, _name: &str) -> Option<Vec<String>> {
        None
    }

    // (not an `async fn`: a loop back into the front goes through here, and this declared `Send` is where the
    // compiler's check of that loop stops)
    fn ask(&self, sql: &str) -> impl Future<Output = anyhow::Result<DataFrame>> + Send {
        // (it only reads: the page table a statement would change is none of its business)
        self.0.plan(sql).map_ok(|(frame, _)| frame)
    }

    // (a PIVOT's values are collected here, so the page's watch holds them as it holds a query's rows)
    fn collect(&self, frame: DataFrame) -> impl Future<Output = anyhow::Result<Vec<RecordBatch>>> + Send {
        watched(self.0.watch.as_ref(), async move { Ok(frame.collect().await?) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use datafusion::arrow::ipc::reader::StreamReader;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    /// An EXPLAIN answers as a node's does (`explain::answered`): each operator's expected rows, with no DataFusion
    /// statistics, plain and ANALYZE alike; and a file it reads is named as SQL wrote it, not by its `ext:` name.
    #[test]
    fn explain_answers_as_a_node_does() {
        futures::executor::block_on(async {
            let page = page_with_tables().await;
            for sql in ["EXPLAIN SELECT count(*) FROM orders", "EXPLAIN ANALYZE SELECT count(*) FROM orders"] {
                let answer = page.request("POST", "/sql?format=csv", &[], sql.into()).await;
                let text = String::from_utf8_lossy(&answer.body).into_owned();
                assert_eq!(answer.status, 200, "{text}");
                assert!(text.contains("expected_rows=") && !text.contains("statistics=["), "{sql}\n{text}");
            }
            assert_eq!(put(&page, "x.csv", ORDERS).await.status, 200);
            let file = page.request("POST", "/sql?format=csv", &[], "EXPLAIN SELECT * FROM 'files/x.csv'".into()).await;
            let text = String::from_utf8_lossy(&file.body).into_owned();
            assert!(text.contains("'files/x.csv'") && !text.contains("ext:"), "{text}");
        });
    }

    #[tokio::test]
    async fn a_page_answers_an_extension_statement_with_a_row_naming_it() {
        let page = Page::open(&Options::default());
        let answered = page.request("POST", "/sql?format=csv", &[], "INSTALL httpfs".into()).await;
        assert_eq!(answered.status, 200, "{:?}", answered.body);
        assert_eq!(answered.body, "notice\nhttpfs is built into Pondra: nothing to install\n");
        let refused = page.request("POST", "/sql", &[], "INSTALL spatial".into()).await;
        assert_eq!(refused.header("x-pondra-sqlstate"), Some("0A000"));
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
    async fn a_pages_front_fills_default_values() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE d (a INT, b TEXT)".into()).await.status, 200);
        let front = crate::front::expand(&PageHost(&page), "INSERT INTO d DEFAULT VALUES", &HashMap::new()).await.unwrap();
        assert!(front.contains("VALUES (DEFAULT, DEFAULT)"), "{front}");
    }

    /// The Postgres passes run in a page's front as they run on a node: a JSON cast, `pg_typeof`, JSON's set functions, a lateral
    /// `generate_series`, an array literal, and the refusals a node gives, each answered as a node answers it.
    #[tokio::test]
    async fn a_pages_front_runs_the_postgres_passes() {
        let page = page_with_tables().await;
        let answers = [
            (r#"SELECT '{"b":1,"a":2}'::jsonb AS j"#, json!([{"j": r#"{"a": 2, "b": 1}"#}])),
            ("SELECT pg_typeof(1) AS t", json!([{"t": "bigint"}])),
            (r#"SELECT key, value FROM json_each_text('{"a":"x","b":"y"}') ORDER BY key"#, json!([{"key": "a", "value": "x"}, {"key": "b", "value": "y"}])),
            ("SELECT count(*) AS n FROM orders o, generate_series(1, o.qty) g", json!([{"n": 21}])),
            ("SELECT '{a,b}'::text[] AS a", json!([{"a": ["a", "b"]}])),
        ];
        for (sql, want) in answers {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            let body = String::from_utf8_lossy(&got.body).into_owned();
            assert_eq!(got.status, 200, "{sql}: {body}");
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&got.body).unwrap(), want, "{sql}: {body}");
        }
        for (sql, code, words) in [
            ("SELECT CAST('nope' AS UUID)", "22P02", r#"invalid input syntax for type uuid: "nope""#),
            ("SELECT enum_range(NULL::mood)", "42704", r#"type "mood" does not exist"#),
        ] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            let body = String::from_utf8_lossy(&got.body).into_owned();
            assert_eq!(got.header("x-pondra-sqlstate"), Some(code), "{sql}: {body}");
            assert!(body.contains(words), "{sql}: {body}");
        }
    }

    /// `MEASURE()` with no view with measures to read is refused on a page as on a node: the front runs the measure pass
    /// for both, so the answer is the same error, with the same status and SQLSTATE.
    #[tokio::test]
    async fn a_page_refuses_measure_as_a_node_does() {
        let page = page_with_tables().await;
        let sql = "SELECT MEASURE(qty) FROM orders";
        let got = page.request("POST", "/sql", &[], sql.into()).await;
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let want = failed(crate::measures::rewrite_with(&mut stmts, &|_| None, &|_| false).unwrap_err());
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (want.status, want.header("x-pondra-sqlstate")));
        assert_eq!(got.body, want.body);
    }

    /// A table made by SQL takes INSERT, UPDATE and DELETE, each answered with its count as a node answers it (`count`,
    /// as `engine::collect` gives it). The rows they leave are the ones SELECT reads, and the statistics the table holds
    /// say so: the write left them out of date, and the table was registered again (`written`).
    #[tokio::test]
    async fn a_table_made_by_sql_takes_insert_update_and_delete() {
        use datafusion::common::stats::Precision;
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t (id INT, name TEXT)".into()).await.status, 200);
        let writes = [("INSERT INTO t VALUES (1, 'a'), (2, 'b')", 2), ("UPDATE t SET name = 'c' WHERE id = 2", 1), ("DELETE FROM t WHERE id = 1", 1)];
        for (sql, count) in writes {
            let got = page.request("POST", "/sql?format=csv", &[], sql.into()).await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (200, None), "{sql}");
            assert_eq!(got.body, format!("count\n{count}\n"), "{sql}");
        }
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT id, name FROM t".into()).await.body, "id,name\n2,c\n");
        let (_, schema) = page.default_schema().unwrap();
        let table = schema.table("t").await.unwrap().unwrap();
        assert_eq!(table.statistics().unwrap().num_rows, Precision::Inexact(1));
        // A file read by name is no table of the page's, so no table is written: the name finds none (42P01).
        assert_eq!(put(&page, "x.csv", "a\n1\n").await.status, 200);
        let into_file = page.request("POST", "/sql", &[], "INSERT INTO 'x.csv' VALUES (2)".into()).await;
        assert_eq!(into_file.header("x-pondra-sqlstate"), Some("42P01"), "{:?}", into_file.body);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM t".into()).await.body, "n\n1\n");
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

    /// A page refuses by name the table rules it can't keep, before it plans anything (`rules`): each answers 0A000 with
    /// the words that name its clause, and makes no table, so SELECT finds none.
    #[tokio::test]
    async fn a_page_refuses_the_rules_it_cannot_keep() {
        let cases = [
            ("CREATE TABLE t (id INT PRIMARY KEY)", "PRIMARY KEY"),
            ("CREATE TABLE t (id INT, PRIMARY KEY (id))", "PRIMARY KEY"),
            ("CREATE TABLE t (id INT UNIQUE)", "UNIQUE"),
            ("CREATE TABLE t (id INT PRIMARY KEY) AS SELECT 1 AS id", "PRIMARY KEY"),
            ("CREATE TABLE t (id INT CHECK (id > 0))", "CHECK"),
            ("CREATE TABLE t (id INT REFERENCES u(id))", "REFERENCES"),
            ("CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY)", "GENERATED"),
            ("CREATE TABLE t (id INT, d INT GENERATED ALWAYS AS (id * 2) STORED)", "GENERATED"),
            ("EXPLAIN CREATE TABLE t (id INT PRIMARY KEY)", "PRIMARY KEY"),
        ];
        let page = Page::open(&Options::default());
        for (sql, words) in cases {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            let body = std::str::from_utf8(&got.body).unwrap();
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")), "{sql}: {body}");
            assert!(body.contains(words), "{sql}: {body}");
            let read = page.request("POST", "/sql", &[], "SELECT * FROM t".into()).await;
            assert_eq!((read.status, read.header("x-pondra-sqlstate")), (404, Some("42P01")), "{sql}");
        }
    }

    /// A page names the statements it doesn't take yet, before anything is parsed (`not_taken`): each answers 0A000 with words
    /// that say what to write instead. The words that only look like them (a string, a table's name, a column) run.
    #[tokio::test]
    async fn a_page_names_the_statements_it_doesnt_take() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS id".into()).await.status, 200);
        let refused = [
            "BEGIN", "begin transaction;", "START TRANSACTION", "COMMIT", "END", "ROLLBACK", "ROLLBACK TO SAVEPOINT a", "SAVEPOINT a",
            "RELEASE a", "/* {\"app\": \"dbt\"} */ commit;", "DECLARE $x = 1", "declare $mode = 'full'",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", "CREATE TYPE mood AS ENUM ('sad', 'ok')", "DROP TYPE mood",
        ];
        for sql in refused {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            let body = std::str::from_utf8(&got.body).unwrap();
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")), "{sql}: {body}");
            assert!(body.starts_with("a page"), "{sql}: {body}");
        }
        for (sql, words) in [
            ("BEGIN", "a page has no transactions yet (BEGIN): each statement it runs is its own, so run them one at a time, or in a transaction on a node"),
            ("DECLARE $x = 1", "a page has no variables yet (DECLARE): write the value into the statement, or declare it on a node"),
            ("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", "a page doesn't take MERGE yet: write its INSERT, UPDATE and DELETE into the page's table, or MERGE on a node"),
            ("CREATE TYPE mood AS ENUM ('sad', 'ok')", "a page makes no types yet (CREATE TYPE): make the column text, or keep the type on a node"),
        ] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!(std::str::from_utf8(&got.body).unwrap(), words, "{sql}");
        }
        for sql in [
            "SELECT 'begin' AS w",
            "CREATE TABLE merged AS SELECT 1 AS declared",
            "SELECT declared FROM merged",
            "CREATE TABLE types AS SELECT 1 AS id",
        ] {
            assert_eq!(page.request("POST", "/sql", &[], sql.into()).await.status, 200, "{sql}");
        }
    }

    /// A page keeps NOT NULL and DEFAULT: a NULL into a NOT NULL column, or an UPDATE that sets one, answers 23502 with the
    /// status a node gives it, and leaves the table as it was.
    #[tokio::test]
    async fn a_page_keeps_not_null_and_defaults() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t (id INT NOT NULL, v INT DEFAULT 7)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO t (id) VALUES (1)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT * FROM t".into()).await.body, "id,v\n1,7\n");
        for sql in ["INSERT INTO t VALUES (NULL, 1)", "UPDATE t SET id = NULL"] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (400, Some("23502")), "{sql}: {:?}", got.body);
            assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT * FROM t".into()).await.body, "id,v\n1,7\n", "{sql}");
        }
        assert_eq!(crate::codes::http("23502"), 400);
    }

    /// What a page refused before it kept rules stays refused, with the code it always gave.
    #[tokio::test]
    async fn what_a_page_refused_already_stays_refused() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t (id INT, v INT)".into()).await.status, 200);
        for sql in [
            "INSERT INTO t VALUES (1, 1) ON CONFLICT DO NOTHING",
            "CREATE TEMP TABLE x (id INT)",
            "CREATE TABLE s (id SERIAL)",
            "ALTER TABLE t ADD COLUMN w INT",
        ] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")), "{sql}");
        }
    }

    /// A page keeps no table's past: `AT (…)` is refused by name, as a node would name the table it was asked of.
    #[tokio::test]
    async fn a_page_refuses_a_tables_past_by_name() {
        let page = page_with_tables().await;
        let got = page.request("POST", "/sql", &[], "FROM orders AT (VERSION => 1)".into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")));
        let body = std::str::from_utf8(&got.body).unwrap();
        assert!(body.contains("orders AT (VERSION => 1)"), "{body}");
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

    /// A page's Parquet file tells the planner what its footer says, as a node's does: its rows exactly, and its column's
    /// range as bounds (inexact: they prune, and no answer is read from them).
    #[tokio::test]
    async fn a_pages_parquet_tells_the_planner_its_footer() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::stats::Precision::{Exact, Inexact};
        use datafusion::common::ScalarValue;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut body = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut body, schema.clone(), None).unwrap();
        writer.write(&RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(1..=100))]).unwrap()).unwrap();
        writer.close().unwrap();
        let page = Page::open(&Options::default());
        assert_eq!(put(&page, "p.parquet", body).await.status, 200);
        let (frame, _)= page.plan("SELECT * FROM 'p.parquet'").await.unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        let stats = StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new()).unwrap();
        assert_eq!(stats.num_rows, Exact(100));
        assert_eq!(stats.column_statistics[0].min_value, Inexact(ScalarValue::Int64(Some(1))));
        assert_eq!(stats.column_statistics[0].max_value, Inexact(ScalarValue::Int64(Some(100))));
    }

    /// Hive-style folders of Parquet files are one table as a node reads them: `k` is each folder's value, and NULL in
    /// the `__HIVE_DEFAULT_PARTITION__` folder (the answers `tools/harness.py`'s "Hive-style folders" asks of a node).
    #[tokio::test]
    async fn a_page_reads_hive_folders_of_parquet_as_a_node_does() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let page = Page::open(&Options::default());
        for (path, rows) in [("h/k=x/a.parquet", 3), ("h/k=__HIVE_DEFAULT_PARTITION__/b.parquet", 2)] {
            let mut body = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut body, schema.clone(), None).unwrap();
            writer.write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from_iter_values(0..rows))]).unwrap()).unwrap();
            writer.close().unwrap();
            assert_eq!(put(&page, path, body).await.status, 200);
        }
        let read = |tail: &str| format!("SELECT {tail} FROM read_parquet('h/')");
        let counts = [
            (read("count(k) AS n, count(*) AS rows"), "n,rows\n3,5\n"),
            (read("count(*) AS n") + " WHERE k IS NULL", "n\n2\n"),
            (read("count(*) AS n") + " WHERE k = 'x'", "n\n3\n"),
        ];
        for (sql, body) in counts {
            let got = page.request("POST", "/sql?format=csv", &[], sql.clone().into()).await;
            assert_eq!(got.status, 200, "{sql}: {}", String::from_utf8_lossy(&got.body));
            assert_eq!(got.body, body, "{sql}");
        }
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

    /// `columns =>` is read as a node reads it: the CSV's columns by position, each typed as declared (`qty` an `INT`,
    /// so an `Int32`), and the rows are the file's.
    #[tokio::test]
    async fn read_csv_takes_declared_columns() {
        let page = page_with_tables().await;
        let read = "read_csv('orders.csv', columns => {'id': 'BIGINT', 'name': 'VARCHAR', 'qty': 'INT'}, header => true)";
        let typed = page.request("POST", "/sql?format=csv", &[], format!("SELECT arrow_typeof(qty) AS t FROM {read} LIMIT 1").into()).await;
        assert_eq!(typed.status, 200, "{}", String::from_utf8_lossy(&typed.body));
        assert_eq!(typed.body, "t\nInt32\n");
        let rows = page.request("POST", "/sql?format=csv", &[], format!("SELECT * FROM {read}").into()).await;
        assert_eq!(rows.body, "id,name,qty\n1,a,5\n2,b,7\n3,c,9\n");
    }

    /// `hive_types =>` types a folder's key as declared: `sales/day=…/a.csv` has `day` a `Date32`, and a row of that day.
    #[tokio::test]
    async fn a_folder_key_takes_its_declared_type() {
        let page = Page::open(&Options::default());
        assert_eq!(put(&page, "sales/day=2026-09-28/a.csv", "id,qty\n1,5\n").await.status, 200);
        let read = "read_csv('sales/**/*.csv', hive_types => {'day': 'DATE'})";
        let typed = page.request("POST", "/sql?format=csv", &[], format!("SELECT arrow_typeof(day) AS t FROM {read} LIMIT 1").into()).await;
        assert_eq!(typed.body, "t\nDate32\n");
        let on_day = page.request("POST", "/sql?format=csv", &[], format!("SELECT count(*) AS n FROM {read} WHERE day = DATE '2026-09-28'").into()).await;
        assert_eq!(on_day.body, "n\n1\n");
    }

    /// Declared columns with no file put are an empty table with those columns, as a node answers, not a 42P01.
    #[tokio::test]
    async fn declared_columns_with_no_file_are_an_empty_table() {
        let page = Page::open(&Options::default());
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT * FROM read_csv('none/*.csv', columns => {'id': 'INT'})".into()).await;
        assert_eq!(got.status, 200, "{}", String::from_utf8_lossy(&got.body));
        assert_eq!(got.body, "id\n");
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

    /// A page with `u` (rows 1 and 2) and the files `a.parquet` and `b.parquet`, each holding the rows of `u`.
    async fn union_page() -> Page {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE u (x INT)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO u VALUES (1), (2)".into()).await.status, 200);
        let parquet = page.request("POST", "/sql?format=parquet", &[], "SELECT x FROM u".into()).await.body;
        for name in ["a.parquet", "b.parquet"] {
            assert_eq!(put(&page, name, parquet.clone()).await.status, 200);
        }
        page
    }

    /// The CSV `sql` answers on `page`, which must answer it.
    async fn csv_of(page: &Page, sql: &str) -> String {
        let got = page.request("POST", "/sql?format=csv", &[], sql.to_string().into()).await;
        assert_eq!(got.status, 200, "{sql}: {}", String::from_utf8_lossy(&got.body));
        String::from_utf8(got.body.to_vec()).unwrap()
    }

    /// A plan's partitions run in the page's own task (`in_task`). A union under an aggregate, a root of two partitions,
    /// a scalar subquery of two and a table made from two would each start a task per partition, and a page has no
    /// runtime to start one on: it panics, and takes every table of the page with it. Each of them answers here.
    #[test]
    fn a_union_runs_in_the_page_task() {
        futures::executor::block_on(async {
            let page = union_page().await;
            let two = "(SELECT x FROM u WHERE x > 0 UNION ALL SELECT x FROM u WHERE x > 0)"; // (the filters keep a count from the tables' row counts)
            assert_eq!(csv_of(&page, &format!("SELECT sum(x) AS s FROM {two}")).await, "s\n6\n");
            assert_eq!(csv_of(&page, "SELECT x FROM u UNION ALL SELECT x + 10 FROM u ORDER BY x").await, "x\n1\n2\n11\n12\n");
            let root = csv_of(&page, "SELECT x FROM u WHERE x > 0 UNION ALL SELECT x FROM u WHERE x > 0").await;
            let mut lines: Vec<&str> = root.lines().collect();
            lines.sort_unstable(); // (the rows come in any order: the header and four rows)
            assert_eq!(lines, ["1", "1", "2", "2", "x"], "{root}");
            assert_eq!(csv_of(&page, &format!("SELECT count(*) AS n FROM (SELECT x FROM {two} LIMIT 3)")).await, "n\n3\n");
            assert_eq!(csv_of(&page, &format!("SELECT count(*) AS n FROM u JOIN {two} v ON u.x = v.x")).await, "n\n4\n");
            assert_eq!(csv_of(&page, "SELECT count(*) AS n FROM u JOIN (SELECT x FROM 'a.parquet' UNION ALL SELECT x FROM 'b.parquet') v ON u.x = v.x").await, "n\n4\n");
            assert_eq!(csv_of(&page, "SELECT (SELECT x FROM u WHERE x = 1 UNION ALL SELECT x FROM u WHERE x > 5) AS v").await, "v\n1\n");
            let made = "CREATE TABLE w AS SELECT x FROM u WHERE x > 0 UNION ALL SELECT x FROM u WHERE x > 0";
            assert_eq!(page.request("POST", "/sql", &[], made.into()).await.status, 200);
            assert_eq!(csv_of(&page, "SELECT count(*) AS n FROM w").await, "n\n4\n");
            for (name, body) in [("s/year=2024/a.csv", "item,amount\npie,4\n"), ("s/year=2025/b.csv", "item,amount\npie,6\ntea,3\n")] {
                assert_eq!(put(&page, name, body).await.status, 200);
            }
            assert_eq!(csv_of(&page, "SELECT year, sum(amount) AS a FROM 's/**/*.csv' GROUP BY year ORDER BY year").await, "year,a\n2024,4\n2025,9\n");
            assert_eq!(csv_of(&page, "SELECT count(*) AS n FROM u").await, "n\n2\n");
        });
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

    /// A page has no disk, so a name that starts at the disk's root is refused by name (0A000), as a URL with a scheme is:
    /// it is not read as the page's own file of that name.
    #[tokio::test]
    async fn a_page_refuses_an_absolute_name() {
        let page = Page::open(&Options::default());
        for sql in ["SELECT * FROM '/x.csv'", "SELECT * FROM 'C:/x.csv'"] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")), "{sql}");
        }
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
        let (_, schema) = page.default_schema().unwrap();
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

    /// A web server over memory: it reads every object, and lists nothing as a web server does, so a test passes only if
    /// no read lists. It keeps the names of the objects it was asked for.
    #[derive(Debug)]
    struct NoList(Arc<InMemory>, Arc<std::sync::Mutex<BTreeSet<String>>>);

    impl std::fmt::Display for NoList {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("NoList") }
    }

    #[async_trait]
    impl ObjectStore for NoList {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> datafusion::object_store::Result<datafusion::object_store::PutResult> { self.0.put_opts(l, p, o).await }
        async fn put_multipart_opts(&self, l: &Path, o: datafusion::object_store::PutMultipartOptions) -> datafusion::object_store::Result<Box<dyn datafusion::object_store::MultipartUpload>> { self.0.put_multipart_opts(l, o).await }
        async fn get_opts(&self, l: &Path, o: datafusion::object_store::GetOptions) -> datafusion::object_store::Result<datafusion::object_store::GetResult> {
            self.1.lock().unwrap().insert(l.to_string());
            self.0.get_opts(l, o).await
        }
        fn delete_stream(&self, l: futures::stream::BoxStream<'static, datafusion::object_store::Result<Path>>) -> futures::stream::BoxStream<'static, datafusion::object_store::Result<Path>> { self.0.delete_stream(l) }
        fn list(&self, _: Option<&Path>) -> futures::stream::BoxStream<'static, datafusion::object_store::Result<datafusion::object_store::ObjectMeta>> { futures::StreamExt::boxed(futures::stream::once(async { Err(StoreError::NotSupported { source: "a web server lists nothing".into() }) })) }
        async fn list_with_delimiter(&self, _: Option<&Path>) -> datafusion::object_store::Result<datafusion::object_store::ListResult> { Err(StoreError::NotSupported { source: "a web server lists nothing".into() }) }
        async fn copy_opts(&self, f: &Path, t: &Path, o: datafusion::object_store::CopyOptions) -> datafusion::object_store::Result<()> { self.0.copy_opts(f, t, o).await }
    }

    /// The root of the tests' Delta table, at the origin a page's program gives it.
    const DELTA_ROOT: &str = "https://lake.test/lake/orders";

    /// A Parquet file of `(id, label)` rows, its columns named as a column-mapped table's files name them (`col-2`).
    fn delta_file(rows: &[(i64, &str)]) -> Vec<u8> {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true), Field::new("col-2", DataType::Utf8, true)]));
        let ids = Int64Array::from_iter_values(rows.iter().map(|r| r.0));
        let labels = StringArray::from_iter_values(rows.iter().map(|r| r.1));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(labels)]).unwrap();
        let mut body = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut body, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        body
    }

    /// A `add` action of a file with `rows` rows, `size` bytes, its ids from `ids.0` to `ids.1`.
    fn add(path: &str, rows: u64, size: u64, ids: (i64, i64)) -> serde_json::Value {
        let stats = json!({"numRecords": rows, "minValues": {"id": ids.0}, "maxValues": {"id": ids.1}});
        json!({"add": {"path": path, "partitionValues": {}, "size": size, "modificationTime": 0, "dataChange": true, "stats": stats.to_string()}})
    }

    /// A Delta table's first actions, as a node writes them: its protocol (columns mapped by name, deletion vectors) and
    /// its schema, `fields` in Delta's names and types.
    fn table_start(id: &str, fields: serde_json::Value) -> Vec<serde_json::Value> {
        let protocol = json!({"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["columnMapping", "deletionVectors"], "writerFeatures": ["columnMapping", "deletionVectors"]}});
        let schema = json!({"type": "struct", "fields": fields});
        let metadata = json!({"metaData": {"id": id, "format": {"provider": "parquet", "options": {}}, "schemaString": schema.to_string(), "partitionColumns": [], "configuration": {"delta.columnMapping.mode": "name"}}});
        vec![protocol, metadata]
    }

    /// A Delta table's log under `folder` in `memory`: commit n's actions, one JSON line each, as version n.
    async fn put_log(memory: &InMemory, folder: &str, commits: &[Vec<serde_json::Value>]) {
        for (v, actions) in commits.iter().enumerate() {
            let body: String = actions.iter().map(|a| format!("{a}\n")).collect();
            memory.put(&Path::from(format!("{folder}/_delta_log/{v:020}.json")), body.into_bytes().into()).await.unwrap();
        }
    }

    /// The Delta table of the tests, put under `lake/orders` in `memory` as a node publishes one: three Parquet files and
    /// three commits. The first has the protocol, the schema (`label` is `col-2` in the files) and `a.parquet` and
    /// `b.parquet`. The second removes `a.parquet` and adds it again with row 1 deleted by an inline deletion vector.
    /// The third adds `c.parquet`.
    async fn put_delta_table(memory: &InMemory) {
        let files = [
            ("a.parquet", delta_file(&[(1, "a"), (2, "b")])),
            ("b.parquet", delta_file(&[(3, "c")])),
            ("c.parquet", delta_file(&[(4, "d")])),
        ];
        let (a, b, c) = (files[0].1.len() as u64, files[1].1.len() as u64, files[2].1.len() as u64);
        let fields = json!([
            {"name": "id", "type": "long", "nullable": true, "metadata": {"delta.columnMapping.id": 1, "delta.columnMapping.physicalName": "id"}},
            {"name": "label", "type": "string", "nullable": true, "metadata": {"delta.columnMapping.id": 2, "delta.columnMapping.physicalName": "col-2"}},
        ]);
        let bitmap = crate::scan::roaring_bytes(&[1]);
        let mut deleted = add("a.parquet", 2, a, (1, 2));
        deleted["add"]["deletionVector"] = json!({"storageType": "i", "pathOrInlineDv": crate::scan::z85_of(&bitmap), "sizeInBytes": bitmap.len(), "cardinality": 1});
        let mut first = table_start("orders", fields);
        first.extend([add("a.parquet", 2, a, (1, 2)), add("b.parquet", 1, b, (3, 3))]);
        let commits = [
            first,
            vec![json!({"remove": {"path": "a.parquet", "deletionTimestamp": 0, "dataChange": true}}), deleted],
            vec![add("c.parquet", 1, c, (4, 4))],
        ];
        put_log(memory, "lake/orders", &commits).await;
        for (name, body) in files {
            memory.put(&Path::from(format!("lake/orders/{name}")), body.into()).await.unwrap();
        }
    }

    /// A page whose program gives it the origin `https://lake.test`: a store over `memory` that lists nothing.
    fn web_page(memory: Arc<InMemory>) -> Page { web_page_asked(memory).0 }

    /// The same page, and the names of the objects its store is asked for.
    fn web_page_asked(memory: Arc<InMemory>) -> (Page, Arc<std::sync::Mutex<BTreeSet<String>>>) {
        let asked = Arc::new(std::sync::Mutex::new(BTreeSet::new()));
        let kept = asked.clone();
        let web: Web = Arc::new(move |origin: &str| {
            assert_eq!(origin, "https://lake.test");
            Arc::new(NoList(memory.clone(), kept.clone())) as Arc<dyn ObjectStore>
        });
        (Page::open(&Options { web: Some(web), ..Default::default() }), asked)
    }

    /// A Delta table over the web reads as a node reads it: its latest version without the deleted row, its renamed
    /// column under its SQL name, and an older version as it was.
    #[tokio::test]
    async fn a_delta_table_is_read_over_the_web_at_its_version() {
        let memory = Arc::new(InMemory::new());
        put_delta_table(&memory).await;
        let page = web_page(memory);
        let read = |version: &str| format!("SELECT id, label FROM read_delta('{DELTA_ROOT}'{version}) ORDER BY id");
        let latest = page.request("POST", "/sql?format=csv", &[], read("").into()).await;
        assert_eq!((latest.status, latest.header("x-pondra-sqlstate")), (200, None), "{:?}", latest.body);
        assert_eq!(latest.body, "id,label\n1,a\n3,c\n4,d\n");
        let zero = page.request("POST", "/sql?format=csv", &[], read(", version => 0").into()).await;
        assert_eq!(zero.body, "id,label\n1,a\n2,b\n3,c\n");
        let one = page.request("POST", "/sql?format=csv", &[], read(", version => 1").into()).await;
        assert_eq!(one.body, "id,label\n1,a\n3,c\n");
    }

    /// A filter on a column whose range the log gives for each file opens only the files that can hold its rows: over the
    /// web, every file opened is a request for its footer.
    #[tokio::test]
    async fn a_delta_table_over_the_web_opens_only_the_files_a_filter_can_match() {
        let memory = Arc::new(InMemory::new());
        put_delta_table(&memory).await;
        let (page, asked) = web_page_asked(memory);
        let got = page.request("POST", "/sql?format=csv", &[], format!("SELECT label FROM read_delta('{DELTA_ROOT}') WHERE id = 3").into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (200, None), "{:?}", got.body);
        assert_eq!(got.body, "label\nc\n");
        let opened: Vec<String> = asked.lock().unwrap().iter().filter(|p| p.ends_with(".parquet")).cloned().collect();
        assert_eq!(opened, ["lake/orders/b.parquet"]);
        let none = page.request("POST", "/sql?format=csv", &[], format!("SELECT label FROM read_delta('{DELTA_ROOT}') WHERE id > 9").into()).await;
        assert_eq!(none.body, "label\n");
        assert_eq!(asked.lock().unwrap().iter().filter(|p| p.ends_with(".parquet")).count(), 1);
    }

    /// A Parquet file of one `decimal(38,18)` column `d`, holding `values` as their unscaled integers (18 places each).
    fn decimal_file(values: &[i128]) -> Vec<u8> {
        use datafusion::arrow::array::Decimal128Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Decimal128(38, 18), true)]));
        let column = Decimal128Array::from(values.to_vec()).with_precision_and_scale(38, 18).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).unwrap();
        let mut body = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut body, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        body
    }

    /// The schema of `delta_file`'s table: `id`, and `label` in the file's column `col-2`.
    fn id_and_label() -> serde_json::Value {
        json!([{"name": "id", "type": "long", "nullable": true, "metadata": {}},
               {"name": "label", "type": "string", "nullable": true, "metadata": {"delta.columnMapping.physicalName": "col-2"}}])
    }

    /// A Delta table of one commit at `folder`, put in `memory` as a node publishes one: its schema as `fields` (Delta's
    /// names and types), and each file as its name, its Parquet body and its stats as the log writes them (a JSON text,
    /// or none). The stats are passed as text and never through `json!`, which would round a decimal literal itself.
    async fn put_one_commit(memory: &InMemory, folder: &str, fields: serde_json::Value, files: Vec<(&str, Vec<u8>, Option<&str>)>) {
        let mut actions = table_start(folder, fields);
        for (name, body, stats) in files {
            let mut add = json!({"path": name, "partitionValues": {}, "size": body.len(), "modificationTime": 0, "dataChange": true});
            if let Some(stats) = stats {
                add["stats"] = json!(stats);
            }
            actions.push(json!({"add": add}));
            memory.put(&Path::from(format!("{folder}/{name}")), body.into()).await.unwrap();
        }
        put_log(memory, folder, &[actions]).await;
    }

    /// A decimal bound is read at full precision, as Spark writes it: the file whose `d` is 0.123456789012345678 at its
    /// least is the one `WHERE d = 0.123456789012345678` asks for. Read through an f64, the least rounds up and the file
    /// is skipped, and the count is 0.
    #[tokio::test]
    async fn a_delta_decimal_bound_is_read_at_full_precision() {
        let memory = Arc::new(InMemory::new());
        let stats = r#"{"numRecords":2,"minValues":{"d":0.123456789012345678},"maxValues":{"d":0.5},"nullCount":{"d":0}}"#;
        let fields = json!([{"name": "d", "type": "decimal(38,18)", "nullable": true, "metadata": {}}]);
        let body = decimal_file(&[123_456_789_012_345_678, 500_000_000_000_000_000]); // (0.123456789012345678 and 0.5)
        put_one_commit(&memory, "lake/decimals", fields, vec![("d.parquet", body, Some(stats))]).await;
        let page = web_page(memory);
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM read_delta('https://lake.test/lake/decimals') WHERE d = 0.123456789012345678".into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (200, None), "{:?}", got.body);
        assert_eq!(got.body, "n\n1\n");
    }

    /// A file whose add gives no row count is counted by reading it, never from its size: `size / 100` is only a guess,
    /// and the planner is told so (an inexact count), so `count(*)` reads the file and answers 3.
    #[tokio::test]
    async fn a_delta_file_with_no_count_is_counted_by_reading_it() {
        use datafusion::common::stats::Precision::Inexact;
        use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
        let memory = Arc::new(InMemory::new());
        let body = delta_file(&[(1, "a"), (2, "b"), (3, "c")]);
        let size = body.len() as u64;
        assert_ne!(size / 100, 3, "the size must not give the right count by chance");
        put_one_commit(&memory, "lake/uncounted", id_and_label(), vec![("f.parquet", body, None)]).await;
        let root = "https://lake.test/lake/uncounted";
        let page = web_page(memory);
        let got = page.request("POST", "/sql?format=csv", &[], format!("SELECT count(*) AS n FROM read_delta('{root}')").into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (200, None), "{:?}", got.body);
        assert_eq!(got.body, "n\n3\n");
        let (frame, _) = page.plan(&format!("SELECT * FROM read_delta('{root}')")).await.unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        let stats = StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new()).unwrap();
        assert_eq!(stats.num_rows, Inexact((size / 100) as usize));
    }

    /// A file whose add gives its row count keeps that count exact, so the planner may answer `count(*)` from the log.
    #[tokio::test]
    async fn a_delta_file_whose_log_counts_its_rows_keeps_them_exact() {
        use datafusion::common::stats::Precision::Exact;
        use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
        let memory = Arc::new(InMemory::new());
        let body = delta_file(&[(1, "a"), (2, "b"), (3, "c")]);
        put_one_commit(&memory, "lake/counted", id_and_label(), vec![("f.parquet", body, Some(r#"{"numRecords":3}"#))]).await;
        let page = web_page(memory);
        let (frame, _) = page.plan("SELECT * FROM read_delta('https://lake.test/lake/counted')").await.unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        let stats = StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new()).unwrap();
        assert_eq!(stats.num_rows, Exact(3));
    }

    /// A file over the web is read by its name, as a file given to a page is; a file that isn't there is 42P01 by the
    /// name SQL wrote.
    #[tokio::test]
    async fn a_web_file_is_read_by_name_and_a_missing_one_is_not_found() {
        let memory = Arc::new(InMemory::new());
        put_delta_table(&memory).await;
        let page = web_page(memory);
        let got = page.request("POST", "/sql?format=csv", &[], "FROM 'https://lake.test/lake/orders/b.parquet'".into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (200, None), "{:?}", got.body);
        assert_eq!(got.body, "id,col-2\n3,c\n");
        let missing = page.request("POST", "/sql?format=csv", &[], "FROM 'https://lake.test/lake/orders/nope.parquet'".into()).await;
        assert_eq!((missing.status, missing.header("x-pondra-sqlstate")), (404, Some("42P01")));
        assert!(std::str::from_utf8(&missing.body).unwrap().contains("at that web address"), "{:?}", missing.body);
    }

    /// A web server lists nothing, so a folder or a glob over the web is refused by name, not listed.
    #[tokio::test]
    async fn a_web_folder_or_glob_is_refused_by_name() {
        let memory = Arc::new(InMemory::new());
        put_delta_table(&memory).await;
        let page = web_page(memory);
        for sql in ["FROM read_parquet('https://lake.test/lake/orders/')", "FROM 'https://lake.test/lake/orders/*.parquet'"] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")), "{sql}");
            assert!(std::str::from_utf8(&got.body).unwrap().contains("lists nothing"), "{sql}");
        }
    }

    /// A web address with no Delta table at it says so.
    #[tokio::test]
    async fn a_web_address_with_no_delta_table_says_so() {
        let memory = Arc::new(InMemory::new());
        put_delta_table(&memory).await;
        let page = web_page(memory);
        let got = page.request("POST", "/sql", &[], "FROM read_delta('https://lake.test/lake/none')".into()).await;
        assert_ne!(got.status, 200);
        assert!(std::str::from_utf8(&got.body).unwrap().contains("no Delta table there"), "{:?}", got.body);
    }

    /// A page whose program gives it no way to fetch reads no web address, and says so by name.
    #[tokio::test]
    async fn a_page_with_no_web_reads_no_web_address() {
        let page = Page::open(&Options::default());
        let got = page.request("POST", "/sql", &[], format!("FROM read_delta('{DELTA_ROOT}')").into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")));
        assert!(std::str::from_utf8(&got.body).unwrap().contains("reads no web addresses"), "{:?}", got.body);
    }

    /// Each object `GET /objects` lists, as its name and rows. Its bytes are left out: an array read back from a file is a
    /// slice of one buffer, so its size differs from the table's.
    fn names_and_rows(answer: &Answer) -> Vec<(String, Option<u64>)> {
        let v: serde_json::Value = serde_json::from_slice(&answer.body).unwrap();
        v["objects"].as_array().unwrap().iter().map(|o| (o["name"].as_str().unwrap().to_string(), o["rows"].as_u64())).collect()
    }

    /// A page's files and tables are kept in its store: a page kept over the same store has what the first was given and
    /// made, with its rows, its column defaults, and none of the tables it dropped.
    #[tokio::test]
    async fn a_kept_page_has_its_files_and_tables_again() {
        let store = Arc::new(InMemory::new());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        assert_eq!(put(&a, "t.csv", "id,name\n1,a\n2,b\n").await.status, 200);
        for sql in [
            "CREATE TABLE t AS FROM 't.csv'",
            "INSERT INTO t VALUES (3, 'c')",
            "CREATE TABLE gone AS SELECT 1 AS x",
            "DROP TABLE gone",
            "CREATE TABLE e (a INT, b INT DEFAULT 7)",
        ] {
            assert_eq!(a.request("POST", "/sql", &[], sql.into()).await.status, 200, "{sql}");
        }
        let b = Page::kept(&options).await.unwrap();
        assert_eq!(b.request("POST", "/sql?format=csv", &[], "SELECT * FROM t ORDER BY id".into()).await.body, "id,name\n1,a\n2,b\n3,c\n");
        let listed_a = a.request("GET", "/objects", &[], bytes::Bytes::new()).await;
        let listed_b = b.request("GET", "/objects", &[], bytes::Bytes::new()).await;
        assert_eq!(names_and_rows(&listed_b), names_and_rows(&listed_a));
        assert_eq!(names_and_rows(&listed_b), [("e".to_string(), Some(0)), ("t".to_string(), Some(3))]);
        assert_eq!(b.request("POST", "/sql", &[], "FROM gone".into()).await.header("x-pondra-sqlstate"), Some("42P01"));
        assert_eq!(b.request("POST", "/sql?format=csv", &[], "FROM e".into()).await.body, "a,b\n");
        assert_eq!(b.request("POST", "/sql", &[], "INSERT INTO e (a) VALUES (1)".into()).await.status, 200);
        assert_eq!(b.request("POST", "/sql?format=csv", &[], "SELECT b FROM e".into()).await.body, "b\n7\n");
        let file_a = a.request("GET", "/files/t.csv", &[], bytes::Bytes::new()).await;
        let file_b = b.request("GET", "/files/t.csv", &[], bytes::Bytes::new()).await;
        assert_eq!((file_a.status, file_b.status), (200, 200));
        assert_eq!((&file_b.body, file_b.header("etag")), (&file_a.body, file_a.header("etag")));
    }

    /// A table whose name needs quoting is kept under a name the store takes: a space is kept as it is, and a `/` or a `?`
    /// is encoded, so each table is one object, and a page kept over the same store answers each as the first did.
    #[tokio::test]
    async fn a_table_whose_name_needs_quoting_is_kept() {
        let store = Arc::new(InMemory::new());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        assert_eq!(a.request("POST", "/sql", &[], "CREATE TABLE \"My Table\" AS SELECT 1 AS x, 'a' AS y".into()).await.status, 200);
        assert_eq!(a.request("POST", "/sql", &[], "CREATE TABLE \"a/b?\" AS SELECT 2 AS x".into()).await.status, 200);
        let b = Page::kept(&options).await.unwrap();
        for sql in ["SELECT * FROM \"My Table\"", "SELECT * FROM \"a/b?\""] {
            let (from_a, from_b) = (a.request("POST", "/sql?format=csv", &[], sql.into()).await, b.request("POST", "/sql?format=csv", &[], sql.into()).await);
            assert_eq!((from_a.status, from_b.status), (200, 200), "{sql}");
            assert_eq!(from_b.body, from_a.body, "{sql}");
        }
        let mut kept: Vec<String> = store.list(Some(&Path::from(TABLES))).try_collect::<Vec<_>>().await.unwrap().iter().map(|o| o.location.to_string()).collect();
        kept.sort();
        assert_eq!(kept, ["tables/My Table.arrow", "tables/a%2Fb%3F.arrow"]);
    }

    /// A kept file that doesn't read is left out, not fatal: a page kept over a store with one good table and one bad file
    /// still opens, the good table answers its rows, and `unread` names the bad file.
    #[tokio::test]
    async fn a_kept_page_opens_past_a_file_it_cannot_read() {
        let store = Arc::new(InMemory::new());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        assert_eq!(a.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS id".into()).await.status, 200);
        assert_eq!(a.request("POST", "/sql", &[], "INSERT INTO t VALUES (2)".into()).await.status, 200);
        store.put(&Path::from("tables/bad.arrow"), bytes::Bytes::from_static(b"not arrow").into()).await.unwrap();
        let b = Page::kept(&options).await.unwrap();
        assert_eq!(b.request("POST", "/sql?format=csv", &[], "SELECT id FROM t ORDER BY id".into()).await.body, "id\n1\n2\n");
        let unread = b.unread();
        assert_eq!(unread.len(), 1, "{unread:?}");
        assert!(unread[0].0.ends_with("tables/bad.arrow"), "{unread:?}");
        // (a write to its name, which would keep or forget a table over that file, is refused while the file is there)
        for sql in ["CREATE TABLE IF NOT EXISTS bad AS SELECT 1 AS x", "CREATE OR REPLACE TABLE bad AS SELECT 1 AS x", "DROP TABLE IF EXISTS bad"] {
            let answer = b.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((answer.status, answer.header("x-pondra-sqlstate")), (409, Some("55000")), "{sql}");
            assert!(String::from_utf8_lossy(&answer.body).contains("tables/bad.arrow"), "{sql}: {:?}", answer.body);
        }
        assert_eq!(store.get(&Path::from("tables/bad.arrow")).await.unwrap().bytes().await.unwrap().as_ref(), b"not arrow");
        assert_eq!(names_listed(&b.request("GET", "/objects", &[], bytes::Bytes::new()).await), ["t"]);
        // (once the file is removed, the name is the page's again)
        store.delete(&Path::from("tables/bad.arrow")).await.unwrap();
        assert_eq!(b.request("POST", "/sql", &[], "CREATE TABLE bad AS SELECT 1 AS x".into()).await.status, 200);
        assert!(store.head(&Path::from("tables/bad.arrow")).await.is_ok());
    }

    /// A page with no store keeps nothing: a page kept over no store is an empty page, as `open` makes one, and a table
    /// made in it is not there for the next page.
    #[tokio::test]
    async fn a_page_without_a_store_keeps_nothing() {
        let a = Page::kept(&Options::default()).await.unwrap();
        assert_eq!(a.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS x".into()).await.status, 200);
        assert_eq!(names_listed(&a.request("GET", "/objects", &[], bytes::Bytes::new()).await), ["t"]);
        let b = Page::kept(&Options::default()).await.unwrap();
        assert_eq!(names_listed(&b.request("GET", "/objects", &[], bytes::Bytes::new()).await), Vec::<String>::new());
        // (and its memory holds no copy of the table beside it)
        assert_eq!(a.files.list(Some(&Path::from(TABLES))).try_collect::<Vec<_>>().await.unwrap().len(), 0);
    }

    /// SQL reads a page's files and never its kept tables: a name that reaches `tables/` is refused by name, however it
    /// is written.
    #[tokio::test]
    async fn sql_never_reads_the_kept_tables() {
        let store = Arc::new(InMemory::new());
        let page = Page::kept(&Options { store: Some(store.clone()), ..Default::default() }).await.unwrap();
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS x".into()).await.status, 200);
        assert_eq!(store.list(Some(&Path::from(TABLES))).try_collect::<Vec<_>>().await.unwrap().len(), 1);
        for sql in ["FROM 'mem:///tables/t.arrow'", "FROM 'mem:///files/../tables/t.arrow'", "FROM 'mem:///files/%2E%2E/tables/t.arrow'", "FROM '../tables/t.arrow'"] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")), "{sql}");
        }
    }

    /// A store over memory that can be made full, as a browser's storage is: while `full` is set, every put and every
    /// delete is refused as the browser refuses a write over its quota (`FULL`). Each put is counted, and put number n
    /// first waits 20 − n turns, so a put begun earlier lands after a later one unless the page orders its writes.
    #[derive(Debug, Default)]
    struct Full {
        memory: InMemory,
        full: AtomicBool,
        puts: AtomicUsize,
    }

    impl std::fmt::Display for Full {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("Full") }
    }

    /// The refusal a full store gives a put or a delete.
    fn full_store() -> StoreError {
        StoreError::Generic { store: "test", source: FULL.into() }
    }

    #[async_trait]
    impl ObjectStore for Full {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> datafusion::object_store::Result<datafusion::object_store::PutResult> {
            let n = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
            for _ in 0..20usize.saturating_sub(n) {
                tokio::task::yield_now().await;
            }
            if self.full.load(Ordering::SeqCst) {
                return Err(full_store());
            }
            self.memory.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(&self, l: &Path, o: datafusion::object_store::PutMultipartOptions) -> datafusion::object_store::Result<Box<dyn datafusion::object_store::MultipartUpload>> { self.memory.put_multipart_opts(l, o).await }
        async fn get_opts(&self, l: &Path, o: datafusion::object_store::GetOptions) -> datafusion::object_store::Result<datafusion::object_store::GetResult> { self.memory.get_opts(l, o).await }
        fn delete_stream(&self, l: futures::stream::BoxStream<'static, datafusion::object_store::Result<Path>>) -> futures::stream::BoxStream<'static, datafusion::object_store::Result<Path>> {
            if self.full.load(Ordering::SeqCst) {
                return futures::StreamExt::boxed(futures::stream::once(async { Err(full_store()) }));
            }
            self.memory.delete_stream(l)
        }
        fn list(&self, p: Option<&Path>) -> futures::stream::BoxStream<'static, datafusion::object_store::Result<datafusion::object_store::ObjectMeta>> { self.memory.list(p) }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> datafusion::object_store::Result<datafusion::object_store::ListResult> { self.memory.list_with_delimiter(p).await }
        async fn copy_opts(&self, f: &Path, t: &Path, o: datafusion::object_store::CopyOptions) -> datafusion::object_store::Result<()> { self.memory.copy_opts(f, t, o).await }
    }

    /// A write a kept page can't keep is undone: its answer is the write's own error (53100 for a full store), its words
    /// say it was undone, and the table is as it was, for an INSERT, an UPDATE, a DELETE and a DROP alike. A CREATE it
    /// refuses leaves no table. Once the store takes writes again a write is kept, and a page kept over the same store
    /// has exactly the rows that were kept.
    #[tokio::test]
    async fn a_write_that_cannot_be_kept_is_undone() {
        let store = Arc::new(Full::default());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let page = Page::kept(&options).await.unwrap();
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS id".into()).await.status, 200);
        store.full.store(true, Ordering::SeqCst);
        for sql in ["INSERT INTO t VALUES (2)", "UPDATE t SET id = 5", "DELETE FROM t", "CREATE TABLE u AS SELECT 1 AS id", "DROP TABLE t"] {
            let answer = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((answer.status, answer.header("x-pondra-sqlstate")), (503, Some("53100")), "{sql}");
            assert!(String::from_utf8_lossy(&answer.body).contains("was undone"), "{sql}: {}", String::from_utf8_lossy(&answer.body));
            assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT id FROM t".into()).await.body, "id\n1\n", "{sql}");
        }
        assert_eq!(page.request("POST", "/sql", &[], "FROM u".into()).await.header("x-pondra-sqlstate"), Some("42P01"));
        store.full.store(false, Ordering::SeqCst);
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO t VALUES (3)".into()).await.status, 200);
        let again = Page::kept(&options).await.unwrap();
        assert_eq!(again.request("POST", "/sql?format=csv", &[], "SELECT id FROM t ORDER BY id".into()).await.body, "id\n1\n3\n");
    }

    /// A write that fails as it runs is undone too, in its own words and with its own code: the store refused nothing, so
    /// the answer doesn't say the page couldn't keep it.
    #[tokio::test]
    async fn a_write_that_fails_as_it_runs_is_undone_in_its_own_words() {
        let page = Page::kept(&Options { store: Some(Arc::new(Full::default())), ..Default::default() }).await.unwrap();
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS id".into()).await.status, 200);
        let answer = page.request("POST", "/sql", &[], "INSERT INTO t SELECT id / 0 FROM t".into()).await;
        let words = String::from_utf8_lossy(&answer.body).to_string();
        assert_eq!(answer.header("x-pondra-sqlstate"), Some("22012"), "{words}");
        assert!(words.contains("was undone") && !words.contains("couldn't keep"), "{words}");
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT id FROM t".into()).await.body, "id\n1\n");
    }

    /// Writes sent at once are kept one after another: each is run and kept before the next is planned, so a page kept
    /// over the same store has every row. Without that order, the yields in `Full` put the snapshots out of order, and
    /// the file that lands last is the first write's, with one row.
    #[tokio::test]
    async fn writes_in_flight_are_kept_in_order() {
        let store = Arc::new(Full::default());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let page = Page::kept(&options).await.unwrap();
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t (id INT)".into()).await.status, 200);
        let writes = (1..=20).map(|n| page.request("POST", "/sql", &[], format!("INSERT INTO t VALUES ({n})").into()));
        for answer in futures::future::join_all(writes).await {
            assert_eq!(answer.status, 200);
        }
        let again = Page::kept(&options).await.unwrap();
        assert_eq!(again.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM t".into()).await.body, "n\n20\n");
    }

    /// A memory reading for a test: a look answers 0 while fewer than `calm` looks have been made, and `usize::MAX` (past any
    /// line) after. A test sets `calm` to the looks made so far plus one, so the query it runs looks 0 at its first poll,
    /// where the watch notes its start, and past the line at its next; `calm` at `usize::MAX` keeps every look at 0.
    fn stepped(looks: Arc<AtomicUsize>, calm: Arc<AtomicUsize>) -> Memory {
        Arc::new(move || match looks.fetch_add(1, Ordering::SeqCst) < calm.load(Ordering::SeqCst) {
            true => 0,
            false => usize::MAX,
        })
    }

    /// A page watched with `live` against a line of `line` (the test's own: `Page::open` would read the settings').
    fn watched_page(store: Option<Arc<dyn ObjectStore>>, live: Memory, line: usize) -> Page {
        Page::with_watch(&Options { store, ..Default::default() }, Some(Watch { live, line }))
    }

    /// A query that grows a page past its line is stopped at its next poll, after its first batches (`range` has more than
    /// 128 batches, so the query yields to the watch): 503 and 53200, the words naming the setting, and the next query answers
    /// with the table as it was.
    #[tokio::test]
    async fn a_query_that_grows_a_page_past_its_line_is_stopped() {
        let (looks, calm) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(usize::MAX)));
        let page = watched_page(None, stepped(looks.clone(), calm.clone()), 1 << 30);
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS id".into()).await.status, 200);
        calm.store(looks.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT count(*) FROM range(2000000)".into()).await;
        let words = String::from_utf8_lossy(&got.body).to_string();
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (503, Some("53200")), "{words}");
        assert!(words.contains("memory_max_gb"), "{words}");
        calm.store(usize::MAX, Ordering::SeqCst);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT id FROM t".into()).await.body, "id\n1\n");
    }

    /// A query that starts past its line and grows nothing is not stopped: with the page past its line from the start, a
    /// `SELECT 1`, a count and a DROP all answer 200, since the watch stops only a query that grew.
    #[tokio::test]
    async fn a_query_that_starts_past_its_line_is_stopped_only_if_it_grows() {
        let page = watched_page(None, Arc::new(|| usize::MAX), 1 << 30);
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS id".into()).await.status, 200);
        for sql in ["SELECT 1 AS n", "SELECT count(*) FROM range(2000000)", "DROP TABLE t"] {
            assert_eq!(page.request("POST", "/sql", &[], sql.into()).await.status, 200, "{sql}");
        }
    }

    /// A write the watch stops leaves its table as it was. An INSERT appends as it runs, and a CREATE OR REPLACE takes the old
    /// table away before it collects: both are undone, on a page in memory and on a kept page, whose old file is kept too.
    #[tokio::test]
    async fn a_write_the_watch_stops_leaves_its_table_as_it_was() {
        for sql in ["INSERT INTO t SELECT * FROM range(2000000)", "CREATE OR REPLACE TABLE t AS SELECT * FROM range(2000000)"] {
            for store in [None, Some(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)] {
                let (looks, calm) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(usize::MAX)));
                let page = watched_page(store.clone(), stepped(looks.clone(), calm.clone()), 1 << 30);
                let made = "CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3)) AS v(id)";
                assert_eq!(page.request("POST", "/sql", &[], made.into()).await.status, 200);
                calm.store(looks.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
                let got = page.request("POST", "/sql", &[], sql.into()).await;
                assert_eq!((got.status, got.header("x-pondra-sqlstate")), (503, Some("53200")), "{sql}");
                calm.store(usize::MAX, Ordering::SeqCst);
                let count = "SELECT count(*) AS n FROM t";
                assert_eq!(page.request("POST", "/sql?format=csv", &[], count.into()).await.body, "n\n3\n", "{sql}");
                if store.is_some() {
                    let again = Page::kept(&Options { store, ..Default::default() }).await.unwrap();
                    assert_eq!(again.request("POST", "/sql?format=csv", &[], count.into()).await.body, "n\n3\n", "{sql}");
                }
            }
        }
    }

    /// A PIVOT's values are collected by the front, and the page's watch holds that collection too: a PIVOT over two million
    /// rows, grown past the line while its values are read, answers 53200.
    #[tokio::test]
    async fn a_pivots_values_are_watched_too() {
        let (looks, calm) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(usize::MAX)));
        let page = watched_page(None, stepped(looks.clone(), calm.clone()), 1 << 30);
        calm.store(looks.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        let pivot = "PIVOT (SELECT x % 500 AS k, x FROM range(2000000) AS r(x)) ON k USING sum(x)";
        let got = page.request("POST", "/sql?format=csv", &[], pivot.into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (503, Some("53200")), "{}", String::from_utf8_lossy(&got.body));
    }

    /// A node for the tests: it keeps each request it is sent, and answers what `answer` gives, as a node would.
    fn node_keeping(kept: Arc<std::sync::Mutex<Vec<Sent>>>, answer: impl Fn() -> Answer + Send + Sync + 'static) -> Node {
        Arc::new(move |sent: Sent| -> Asked {
            kept.lock().unwrap().push(sent);
            let answered = answer();
            async { Ok(answered) }.boxed()
        })
    }

    /// The count `3` a node answers as CSV, as the tests' node does.
    fn count_csv() -> Answer {
        Answer { status: 200, headers: vec![("content-type", "text/csv".to_string())], body: "n\n3\n".into() }
    }

    /// The rows a node answers a fill's query with: ids 1 and 3 with their totals, as an Arrow stream (the node's own answer).
    fn rows_answer() -> Answer {
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 3]));
        let totals: ArrayRef = Arc::new(Int64Array::from(vec![15, 7]));
        let batch = RecordBatch::try_from_iter([("id", ids), ("total", totals)]).unwrap();
        let body = crate::answer::render(&[batch], Some("arrow"), |_| None).unwrap();
        Answer { status: 200, headers: vec![("content-type", crate::answer::content_type(Some("arrow")).to_string())], body }
    }

    /// A page with the table `mine` (three ids, made by SQL) and `node` for the tables it doesn't have.
    async fn page_with_node(node: Node) -> Page {
        let page = Page::open(&Options { node: Some(node), ..Default::default() });
        for sql in ["CREATE TABLE mine (id INT)", "INSERT INTO mine VALUES (1), (2), (3)"] {
            assert_eq!(page.request("POST", "/sql", &[], sql.into()).await.status, 200, "{sql}");
        }
        page
    }

    /// A request a page sent, read back as its JSON and each of its tables' batches (`request_body::read`, then each stream).
    fn read_back(sent: &Sent) -> (serde_json::Value, Vec<Vec<RecordBatch>>) {
        let (head, streams) = request_body::read(&sent.body).unwrap();
        let tables: Vec<Vec<RecordBatch>> = streams.iter().map(|s| StreamReader::try_new(*s, None).unwrap().map(|b| b.unwrap()).collect()).collect();
        (serde_json::from_slice(head).unwrap(), tables)
    }

    /// A page asks its node for the table it doesn't have, with the rows of its own table the query names, and the node's
    /// answer is the page's: in the format asked, as the node gave it.
    #[tokio::test]
    async fn a_page_asks_its_node_for_a_table_it_does_not_have() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        let sql = "SELECT count(*) AS n FROM mine JOIN lake_orders USING (id)";
        let got = page.request("POST", "/sql?format=csv", &[], sql.into()).await;
        assert_eq!((got.status, got.header("content-type")), (200, Some("text/csv")));
        assert_eq!(got.body, "n\n3\n");
        let sent = kept.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!((sent[0].query.as_str(), sent[0].content_type), ("format=csv", request_body::CONTENT_TYPE));
        let (head, tables) = read_back(&sent[0]);
        assert_eq!((head["sql"].as_str(), head["tables"].clone()), (Some(sql), json!(["\"mine\""])));
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    /// The node reads what a page sends as it reads its own requests: the statement, and the page's table as a table.
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn the_node_reads_what_a_page_sends() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        let sql = "SELECT count(*) AS n FROM mine JOIN lake_orders USING (id)";
        page.request("POST", "/sql?format=csv", &[], sql.into()).await;
        let sent = kept.lock().unwrap();
        let request = crate::routines::Request::read(sent[0].content_type, &sent[0].body).unwrap();
        assert_eq!(request.sql, sql);
        assert_eq!(request.tables.len(), 1);
        assert_eq!((request.tables[0].0.as_str(), request.tables[0].1.iter().map(|b| b.num_rows()).sum::<usize>()), ("\"mine\"", 3));
    }

    /// `EXPLAIN` of a query over the node's tables is sent as the query is: the node explains it.
    #[tokio::test]
    async fn explain_of_a_query_over_the_nodes_tables_is_sent() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        let got = page.request("POST", "/sql", &[], "EXPLAIN SELECT * FROM lake_orders".into()).await;
        assert_eq!(got.status, 200, "{:?}", got.body);
        assert_eq!(kept.lock().unwrap().len(), 1);
    }

    /// A query the page can answer by itself never leaves it, even with a node to ask.
    #[tokio::test]
    async fn a_query_the_page_can_answer_never_leaves_it() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM mine".into()).await.body, "n\n3\n");
        assert!(kept.lock().unwrap().is_empty());
    }

    /// A page with no node answers a table it lacks with 42P01, as it always has.
    #[tokio::test]
    async fn a_page_without_a_node_answers_42p01_as_before() {
        let page = Page::open(&Options::default());
        let got = page.request("POST", "/sql", &[], "SELECT count(*) FROM lake_orders".into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (404, Some("42P01")));
    }

    /// A file not put is still 42P01 with a node: the node has no file of the page's, and sends nothing.
    #[tokio::test]
    async fn a_file_not_put_is_still_42p01_with_a_node() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        let got = page.request("POST", "/sql", &[], "SELECT * FROM 'nope.csv'".into()).await;
        assert_eq!(got.header("x-pondra-sqlstate"), Some("42P01"));
        assert!(kept.lock().unwrap().is_empty());
    }

    /// The writes a page does not send its node are refused by name, each before anything is sent: a write into the node's
    /// table (its words say to write through a client of the node), a DELETE, a `WITH … INSERT`, a fill joined with a file,
    /// and a fill into a table named as the node's rows.
    #[tokio::test]
    async fn a_page_refuses_the_writes_it_does_not_send() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        assert_eq!(put(&page, "x.csv", "id,v\n1,2\n").await.status, 200);
        let words = page.request("POST", "/sql", &[], "INSERT INTO lake_orders SELECT * FROM mine".into()).await;
        assert!(String::from_utf8_lossy(&words.body).contains("a client of the node"), "{:?}", words.body);
        for sql in [
            "INSERT INTO lake_orders SELECT * FROM mine",
            "DELETE FROM mine WHERE id IN (SELECT id FROM lake_orders)",
            "WITH x AS (SELECT * FROM lake_orders) INSERT INTO mine SELECT * FROM x",
            "CREATE TABLE f AS SELECT * FROM 'x.csv' JOIN lake_orders USING (id)",
            "CREATE TABLE \"the node's rows\" AS SELECT * FROM lake_orders",
        ] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!(got.header("x-pondra-sqlstate"), Some("0A000"), "{sql}");
        }
        assert!(kept.lock().unwrap().is_empty());
    }

    /// A front error over a statement that names a node's table is the node's to answer, as written: a whole row of the node's
    /// table, or its enum, which only the node can see. The page's own tables, and its own refusals, stay the page's.
    #[tokio::test]
    async fn a_front_error_over_a_node_table_goes_to_the_node() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        for (i, sql) in ["SELECT json_agg(s) AS n FROM lake_orders s", "SELECT enum_range(NULL::mood) AS n FROM lake_orders"].into_iter().enumerate() {
            let got = page.request("POST", "/sql?format=csv", &[], sql.into()).await;
            assert_eq!(got.status, 200, "{sql}: {}", String::from_utf8_lossy(&got.body));
            assert_eq!(got.body, "n\n3\n", "{sql}");
            let sent = kept.lock().unwrap();
            assert_eq!(sent.len(), i + 1, "{sql}");
            let (head, _) = read_back(sent.last().unwrap());
            assert_eq!(head["sql"].as_str(), Some(sql), "{sql}");
        }
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT json_agg(s) AS n FROM mine s".into()).await;
        assert_eq!(got.status, 200, "{}", String::from_utf8_lossy(&got.body));
        assert_eq!(kept.lock().unwrap().len(), 2);
        let merged = "MERGE INTO mine USING lake_orders o ON mine.id = o.id WHEN MATCHED THEN DELETE";
        let got = page.request("POST", "/sql", &[], merged.into()).await;
        let body = String::from_utf8_lossy(&got.body).into_owned();
        assert_eq!(got.header("x-pondra-sqlstate"), Some("0A000"), "{body}");
        assert!(body.contains("MERGE"), "{body}");
        assert_eq!(kept.lock().unwrap().len(), 2);
    }

    /// A fill over a node's table sends the node its query as written: the front's rewrites of it were made without the node's
    /// column types, so `json_agg` of a node's column is not sent as the page's own maker.
    #[tokio::test]
    async fn a_fill_over_a_node_table_sends_its_query_as_written() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), rows_answer)).await;
        page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT json_agg(id) AS d FROM lake_orders".into()).await;
        let sent = kept.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let (head, _) = read_back(&sent[0]);
        let sql = head["sql"].as_str().unwrap();
        assert!(sql.contains("json_agg(") && !sql.contains("pondra_json_agg"), "{sql}");
    }

    /// A `CREATE TABLE … AS` from the node's rows: the node answers the query alone, the page keeps its rows as a table, and
    /// the table is the page's own, so a query over it is answered by the page with nothing sent.
    #[tokio::test]
    async fn a_table_is_made_from_its_nodes_rows() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), rows_answer)).await;
        let sql = "CREATE TABLE totals AS SELECT m.id, sum(s.amount) AS total FROM mine m JOIN sales s USING (id) GROUP BY m.id";
        let made = page.request("POST", "/sql", &[], sql.into()).await;
        assert_eq!(made.status, 200, "{:?}", made.body);
        {
            let sent = kept.lock().unwrap();
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].query, "format=arrow");
            let (head, tables) = read_back(&sent[0]);
            assert!(head["sql"].as_str().unwrap().contains("sales") && !head["sql"].as_str().unwrap().contains("CREATE"));
            assert_eq!(head["tables"], json!(["\"mine\""]));
            assert_eq!(tables[0].iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        }
        let got = page.request("POST", "/sql?format=csv", &[], "SELECT * FROM totals ORDER BY id".into()).await;
        assert_eq!(got.body, "id,total\n1,15\n3,7\n");
        assert_eq!(kept.lock().unwrap().len(), 1);
    }

    /// An `INSERT INTO` one of the page's tables from the node's rows: the node's rows go into the table, columns and all.
    #[tokio::test]
    async fn an_insert_fills_a_table_of_the_page() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), rows_answer)).await;
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t2 (id BIGINT, total BIGINT)".into()).await.status, 200);
        let inserted = page.request("POST", "/sql", &[], "INSERT INTO t2 SELECT id, amount FROM sales".into()).await;
        assert_eq!(inserted.status, 200, "{:?}", inserted.body);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM t2".into()).await.body, "n\n2\n");
    }

    /// A `CREATE OR REPLACE TABLE … AS` from the node's rows replaces the page's table of that name.
    #[tokio::test]
    async fn a_fill_replaces_a_page_table() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), rows_answer)).await;
        let replaced = page.request("POST", "/sql", &[], "CREATE OR REPLACE TABLE mine AS SELECT * FROM sales".into()).await;
        assert_eq!(replaced.status, 200, "{:?}", replaced.body);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT * FROM mine ORDER BY id".into()).await.body, "id,total\n1,15\n3,7\n");
    }

    /// A node's refusal of a fill's query is the page's answer as it is, and nothing is made. A node's 200 that is not an
    /// Arrow stream is 08P01, and nothing is made either.
    #[tokio::test]
    async fn a_fill_is_the_nodes_answer_when_it_is_not_rows() {
        let refusing = Arc::new(std::sync::Mutex::new(vec![]));
        let not_found = || Answer { status: 404, headers: vec![("content-type", TEXT.to_string()), ("x-pondra-sqlstate", "42P01".to_string())], body: "no table".into() };
        let page = page_with_node(node_keeping(refusing.clone(), not_found)).await;
        let got = page.request("POST", "/sql", &[], "CREATE TABLE totals AS SELECT * FROM sales".into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (404, Some("42P01")));
        assert!(!page.default_schema().unwrap().1.table_exist("totals"));
        let csv = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(csv.clone(), count_csv)).await;
        let got = page.request("POST", "/sql", &[], "CREATE TABLE totals AS SELECT * FROM sales".into()).await;
        assert_eq!(got.header("x-pondra-sqlstate"), Some("08P01"), "{:?}", got.body);
        assert!(!page.default_schema().unwrap().1.table_exist("totals"));
    }

    /// A fill's table is kept with the page's other tables: a page kept over the same store has the table made from the
    /// node's rows, with its rows, and asks the node for nothing to read it.
    #[tokio::test]
    async fn a_kept_page_keeps_a_table_made_from_its_nodes_rows() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let options = Options { store: Some(Arc::new(InMemory::new())), node: Some(node_keeping(kept.clone(), rows_answer)), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        assert_eq!(a.request("POST", "/sql", &[], "CREATE TABLE totals AS SELECT * FROM sales".into()).await.status, 200);
        let b = Page::kept(&options).await.unwrap();
        assert_eq!(b.request("POST", "/sql?format=csv", &[], "SELECT * FROM totals ORDER BY id".into()).await.body, "id,total\n1,15\n3,7\n");
        assert_eq!(kept.lock().unwrap().len(), 1);
    }

    /// A table with no rows is sent with its columns: a node joins it as it joins any table.
    #[tokio::test]
    async fn an_empty_table_is_sent_with_its_columns() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE e (id INT)".into()).await.status, 200);
        let sql = "SELECT count(*) AS n FROM e JOIN lake_orders USING (id)";
        assert_eq!(page.request("POST", "/sql?format=csv", &[], sql.into()).await.status, 200);
        let sent = kept.lock().unwrap();
        let (head, tables) = read_back(&sent[0]);
        assert_eq!(head["tables"], json!(["\"e\""]));
        assert_eq!(tables[0].iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert_eq!(tables[0][0].schema().field(0).name(), "id");
    }

    /// A table is sent under the name the page holds it by: quoted, as SQL writes it, and a name in the page's schema
    /// is its bare name.
    #[tokio::test]
    async fn names_are_sent_as_the_page_holds_them() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let page = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        for sql in ["CREATE TABLE \"Mine\" AS SELECT 1 AS id", "CREATE TABLE public.mine2 AS SELECT 1 AS id"] {
            assert_eq!(page.request("POST", "/sql", &[], sql.into()).await.status, 200, "{sql}");
        }
        let sql = "SELECT count(*) AS n FROM \"Mine\" JOIN public.mine2 USING (id) JOIN lake_orders USING (id)";
        assert_eq!(page.request("POST", "/sql?format=csv", &[], sql.into()).await.status, 200);
        let sent = kept.lock().unwrap();
        let (head, _) = read_back(&sent[0]);
        assert_eq!(head["tables"], json!(["\"Mine\"", "\"mine2\""]));
    }

    /// What a page sends its node is no more than `PONDRA_WEB_SEND_MB`: past the limit, the query is refused by name, 54000.
    #[test]
    fn a_page_sends_no_more_than_web_send_mb() {
        assert!(within(1024 * 1024, 1).is_ok());
        let over = within(3 * 1024 * 1024, 1).unwrap_err();
        assert_eq!(crate::codes::of(&over), "54000");
        assert!(crate::codes::words(&over).contains("web_send_mb"), "{over:#}");
    }

    /// A node that can't be reached is 08001, and its words say which node did not answer.
    #[tokio::test]
    async fn a_node_not_reached_is_08001() {
        let node: Node = Arc::new(|_sent: Sent| -> Asked {
            async { Err("no answer from http://x".to_string()) }.boxed()
        });
        let page = page_with_node(node).await;
        let got = page.request("POST", "/sql", &[], "SELECT count(*) FROM lake_orders".into()).await;
        assert_eq!((got.status, got.header("x-pondra-sqlstate")), (503, Some("08001")));
        assert_eq!(&got.body[..], "the page's node didn't answer: no answer from http://x".as_bytes());
    }
}
