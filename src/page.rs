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
use datafusion::arrow::array::ArrayRef;
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
use datafusion::sql::sqlparser::ast::{AssignmentTarget, ColumnOption, ColumnOptionDef, CreateTable, CreateView, Ident, Insert, ObjectName, ObjectNamePart, ObjectType, OnInsert, Query, SetExpr, SqliteOnConflict, Statement as SqlStatement, TableConstraint, TableFactor, TableObject, Update};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, OnceLock, PoisonError};
use std::task::Poll;

mod alter; // (ALTER TABLE, ALTER VIEW … RENAME TO and TRUNCATE: `alter_statement`, `truncated`)
mod counters; // (an identity column's counter, and `nextval` over the counters: `Counter`, `Nextval`)
mod txn; // (BEGIN … COMMIT or ROLLBACK, and savepoints: `control`, `gate`, `at_once`)

const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json";
const FILES: &str = "/files/";
/// The folder SQL reads a relative path's file from, as a node's owner reads its working folder (`/objects` names it).
const WORKING: &str = "mem:///files/";
/// The folder a page's tables are kept in, beside `files/` in its store. SQL never reads it (`located`).
const TABLES: &str = "tables";
/// Where a kept page keeps its views, one file each (`view_at`).
const VIEWS: &str = "views";
/// The notes an Arrow file of a kept table carries: the table's name, a column's default as SQL text, the CREATE TABLE
/// text its rules are made from (`CREATE_KEY`, when the table has rules), and the counters of its identity columns, as JSON
/// (`COUNTERS_KEY`, when it has any).
const TABLE_KEY: &str = "pondra.table";
const DEFAULT_KEY: &str = "pondra.default";
const CREATE_KEY: &str = "pondra.create";
const COUNTERS_KEY: &str = "pondra.counters";
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
    /// The memory line the program that runs the page found for its device, in bytes (`web/`: a phone's browser gives a
    /// page less than a desktop's), taken when `PONDRA_MEMORY_MAX_GB` isn't set (`memory_line`). None: `LINE`.
    pub line: Option<usize>,
}

/// The page's live heap bytes, as the program that runs the page reads them (`Options::memory`).
pub type Memory = Arc<dyn Fn() -> usize + Send + Sync>;

/// A page's memory watch: what it reads its live bytes with, and the line past which the query that grew them is stopped
/// (`watched`). A page's line is set by `PONDRA_MEMORY_MAX_GB`, else by its device (`memory_line`).
struct Watch {
    live: Memory,
    line: usize,
}

impl Watch {
    /// The bytes a query may grow the page by now: to its line, and at least the tenth of the line `watched` lets any
    /// query grow, so a page past its line still takes a small answer.
    fn room(&self) -> usize {
        self.line.saturating_sub((self.live)()).max(self.line / 10)
    }
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
    /// The most bytes of answer the page has room for: its memory line less what it holds as it asks (`Watch::room`), or
    /// None when its memory isn't watched. The program that runs the page reads no more of an answer than that, and gives
    /// `past_room` in its place, so a node's answer never grows the page past its line.
    pub room: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Options { partitions: 1, store: None, web: None, node: None, memory: None, line: None }
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
    ruled: Arc<std::sync::Mutex<HashMap<String, Ruled>>>, // (each page table's rules, by its name: `Ruled`, kept with the table's file; `nextval` reads the counters in them)
    views: std::sync::Mutex<BTreeMap<String, String>>, // (each view's query, by its name: `view_statement`; a statement reads them through the front, `views_for`)
    txn: std::sync::Mutex<Option<txn::Txn>>, // (the open transaction, if any: `page::txn`)
    notes: std::sync::Mutex<Vec<String>>, // (what the statement running now says beside its answer: `note`)
}

/// A page table's rules, made from its CREATE TABLE by the core (`rules::Table::of_create`), that statement's text, which a kept
/// page keeps in the table's file (`CREATE_KEY`) to make them again, and the counters of its identity columns (`counters`, by
/// sequence name): a table's counters are kept with its rules, so a rename moves them and a drop takes them away.
#[derive(Clone)]
struct Ruled {
    create: String,
    table: Arc<crate::rules::Table>,
    counters: counters::Counters,
}

/// A statement planned (`planned`): its session, its plan, the name of the page table it changes (`touched`), and `ruled`,
/// the rules a CREATE TABLE makes for `changed` (None for any other statement, and for a CREATE TABLE … AS).
struct Planned {
    ctx: SessionContext,
    plan: LogicalPlan,
    changed: Option<String>,
    ruled: Option<Ruled>,
}

/// What a planned write does to its table, as `write` reads it for the table's rules.
#[derive(Clone, Copy, PartialEq)]
enum Change {
    /// A CREATE TABLE (`if_not_exists`: with IF NOT EXISTS).
    Create { if_not_exists: bool },
    Drop,
    /// An INSERT: `appends` when it only appends rows (the new ones are those past the table's length before the write); an
    /// INSERT OVERWRITE or REPLACE rewrites the table's partitions, so the whole table is what is checked.
    Insert { appends: bool },
    Update,
    /// A DELETE, which breaks no rule.
    Other,
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

/// The answer a page gives in place of its node's answer past the room it had for it (`Sent::room`): 53200, as for a query
/// the watch stops, and the page answers the next one.
pub fn past_room(room: usize) -> Answer {
    failed(crate::codes::coded("53200", format!("out of memory: the node's answer is more than the {} MB the page has room for, so the page didn't read it (memory_max_gb)", room >> 20)))
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
        let listed = page.files.list(Some(&Path::from(VIEWS))).try_collect::<Vec<_>>().await?;
        for object in listed.iter().filter(|o| o.location.as_ref().ends_with(".json")) {
            if let Err(e) = page.view_back(&object.location).await {
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
    /// `memory`, against the line its settings or its device give (`memory_line`).
    pub fn open(options: &Options) -> Page {
        let watch = options.memory.as_ref().zip(memory_line(options.line)).map(|(live, line)| Watch { live: live.clone(), line });
        Page::with_watch(options, watch)
    }

    /// `open`, with its memory watch given: `open` reads the line from the settings, and a test gives a small one.
    fn with_watch(options: &Options, watch: Option<Watch>) -> Page {
        // (the page's catalog is `page`, as its host and `GET /objects` name it: an error then says `page.public.t`)
        let mut config = SessionConfig::new().with_target_partitions(options.partitions.max(1)).with_default_catalog_and_schema("page", "public");
        // A page's Parquet strings are plain, as the files hold them: views were no faster in a page and held 230 MB
        // more at SF1 (measured in web-4g). The session's Parquet options are the ones a file read takes (`ext_names::file_format`).
        config.options_mut().execution.parquet.schema_force_view_types = false;
        // A greedy pool, not a fair one: with no disk, every sort and aggregate can spill, so a fair pool would give each of
        // them a fraction. Nothing spills, since a page has no disk to spill to: an operator past the pool is refused (53200).
        let runtime = RuntimeEnvBuilder::new()
            .with_cache_manager(CacheManagerConfig::default().with_list_files_cache_limit(0)) // (a file put again is listed again)
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(query_memory(options.line))))
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
        // (`nextval` counts in the page's tables' counters, which are kept with their rules: the page's session copies it to each
        // statement's, so a statement's `nextval` reaches the same counters)
        let ruled: Arc<std::sync::Mutex<HashMap<String, Ruled>>> = Default::default();
        ctx.register_udf(datafusion::logical_expr::ScalarUDF::new_from_impl(counters::Nextval::new(ruled.clone())));
        Page { ctx, files, keeps: options.store.is_some(), web: options.web.clone(), node: options.node.clone(), keeping: futures::lock::Mutex::new(()), watch, unread: vec![], ruled, views: Default::default(), txn: Default::default(), notes: Default::default() }
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
    ///
    /// In an open transaction (`page::txn`), the statement meets its checks first (`gate`), and one that fails fails the
    /// transaction. A transaction word is the transaction's (`control`). What the statement says beside its answer (a
    /// transaction's warning, DDL's notice) comes as `x-pondra-notices`, as a node's HTTP sends it.
    async fn sql(&self, text: &str, format: Option<&str>) -> anyhow::Result<Answer> {
        self.notes();
        let answer = match crate::text::control(text) {
            Some(word) => {
                let _turn = self.keeping.lock().await;
                self.control(word, text).await
            }
            None => {
                let out = match self.gate(text) {
                    Ok(()) => self.statement_sql(text, format).await,
                    Err(e) => Err(e),
                };
                out.inspect_err(|e| self.failed(e))
            }
        }?;
        let notes = self.notes();
        Ok(match notes.is_empty() {
            true => answer,
            false => answer.with("x-pondra-notices", serde_json::to_string(&notes)?),
        })
    }

    /// `sql` of a statement that is not a transaction word.
    async fn statement_sql(&self, text: &str, format: Option<&str>) -> anyhow::Result<Answer> {
        let turn = self.keeping.lock().await;
        if let Some(answer) = self.view_statement(text).await? {
            return Ok(answer);
        }
        if let Some(answer) = self.alter_statement(text).await? {
            return Ok(answer);
        }
        // (a TRUNCATE is its table's DELETE: the DELETE it becomes is planned and run as any DELETE is)
        let deleted = self.truncated(text)?;
        let text = deleted.as_deref().unwrap_or(text);
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
    async fn run_planned(&self, turn: futures::lock::MutexGuard<'_, ()>, planned: Planned, format: Option<&str>, explained: bool) -> anyhow::Result<Answer> {
        let Planned { ctx, plan, changed, ruled } = planned;
        let batches = match changed {
            Some(name) => self.write(&ctx, plan, &name, ruled).await?,
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
        let (expanded, raw) = match crate::front::expand(&PageHost(self), text, &self.views_for(text, &[]).await?).await {
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
        self.query_of_text(&format!("SELECT * FROM {}", crate::ext_names::quoted(NODE_ROWS)))
    }

    /// `sql` parsed as a query, as the page's session parses SQL (`parsed`).
    fn query_of_text(&self, sql: &str) -> anyhow::Result<Box<Query>> {
        let DfStatement::Statement(s) = self.parsed(sql)? else { anyhow::bail!("{sql} is not a statement a page's session parses") };
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
        Ok(Sent { query, content_type: request_body::CONTENT_TYPE, body: request_body::write(&head, &streams), room: None })
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

    /// One request to the page's node, and its answer as the node gave it, within the room the page has for it
    /// (`Sent::room`). A node that doesn't answer is 08001, its words saying which (`node` names it, and a browser's refusal
    /// reads the same).
    async fn ask(&self, mut sent: Sent) -> anyhow::Result<Answer> {
        let Some(node) = &self.node else { return Err(crate::codes::coded("08001", "the page has no node to ask")) };
        sent.room = self.watch.as_ref().map(Watch::room);
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
    async fn planned(&self, text: &str) -> anyhow::Result<Planned> {
        self.planned_as(text, &[]).await
    }

    /// `planned`, with the views in `outer` left out of those the statement reads: a view's own query, planned as it is
    /// made, doesn't read the view (`view_plans`).
    async fn planned_as(&self, text: &str, outer: &[String]) -> anyhow::Result<Planned> {
        let row = crate::extension::row(text).transpose()?; // (DuckDB's INSTALL and LOAD: a row naming what happened, `extension.rs`)
        let text = row.as_deref().unwrap_or(text);
        let expanded = crate::front::expand(&PageHost(self), text, &self.views_for(text, outer).await?).await?;
        // (a page keeps no table's past, so `AT (…)` is refused by name: left alone, it would fail as an unknown table "at:…")
        if crate::past_names::mentioned(&expanded) {
            return Err(crate::codes::coded("0A000", format!("{}: a page keeps no table's past (a node reads a table as it was)", crate::past_names::spelled(&expanded))));
        }
        let expanded = crate::asof::as_of(&expanded)?.into_owned();
        self.planned_expanded(&expanded, None).await
    }

    /// `expanded`, which the front has run already, planned as `planned` plans it. With `rows` (a fill's node rows), they are
    /// the table `NODE_ROWS` for this statement alone (`Overlay::own`). A CREATE TABLE with columns is planned without its
    /// constraints (`plain_create`), and its rules are the core's, made over the columns the planner gave it (`ruled_made`).
    async fn planned_expanded(&self, expanded: &str, rows: Option<Arc<dyn TableProvider>>) -> anyhow::Result<Planned> {
        let ordered = crate::rules::create::in_order(expanded); // (an identity's options in the order the parser takes them, as a node puts them)
        let expanded = ordered.as_deref().unwrap_or(expanded);
        rules(expanded)?; // (a table rule a page can't keep is refused before anything is planned or read)
        self.key_set(expanded)?; // (an UPDATE of a key column of a ruled table, which a node refuses too)
        let whole = self.whole_rows(expanded).await?; // (an INSERT into one of the page's tables, as whole rows: all that follows reads it)
        let expanded = whole.as_deref().unwrap_or(expanded);
        let (ctx, overlay) = self.statement()?;
        if let Some(rows) = rows {
            overlay.own(NODE_ROWS.to_string(), rows);
        }
        crate::explain::prepare(&ctx, expanded); // (an EXPLAIN's operators' expected rows, as a node's)
        self.given(&overlay, expanded).await?;
        let created = self.created(expanded);
        // (each identity column's sequence and counter, named past the names the page's counters hold: the planned DEFAULT and
        // the counter are made from the same names)
        let identified = match &created {
            Some(c) => self.identified(c)?,
            None => vec![],
        };
        let sql = match &created {
            Some(c) => plain_create(c, &identified)?.to_string(),
            None => expanded.to_string(),
        };
        let plan = crate::engine::planned(&ctx, &sql).await?;
        let ruled = match &created {
            Some(c) => self.ruled_made(c, &plan, identified)?,
            None => None,
        };
        may(&plan)?;
        let changed = touched(&plan);
        Ok(Planned { ctx, plan, changed, ruled })
    }

    /// The CREATE TABLE with columns (no `AS`) that `expanded` is, if it is one. A text with no CREATE in it (the gate `rules`
    /// uses), one that doesn't parse, and one that isn't a single CREATE TABLE are None, so the planner's own answer stands.
    fn created(&self, expanded: &str) -> Option<CreateTable> {
        if !expanded.to_ascii_lowercase().contains("create") {
            return None;
        }
        match self.parsed(expanded).ok()? {
            DfStatement::Statement(s) => match *s {
                SqlStatement::CreateTable(c) if c.query.is_none() => Some(c),
                _ => None,
            },
            _ => None,
        }
    }

    /// An INSERT into one of the page's own tables, written as an INSERT of whole rows, as a node writes it (`rules::rows::rows_for`):
    /// each row gives every column the table has, in its order. The columns it names (or, naming none, the first its values give)
    /// take its values, and a column it leaves out, or says DEFAULT for, takes the column's default, or NULL. A row of the wrong
    /// width is refused with the node's words and code. None for any other statement, and for an INSERT into a table the page
    /// doesn't have, so the planner's own answer stands.
    async fn whole_rows(&self, expanded: &str) -> anyhow::Result<Option<String>> {
        if !expanded.to_ascii_lowercase().contains("insert") {
            return Ok(None);
        }
        let Ok(DfStatement::Statement(mut statement)) = self.parsed(expanded) else { return Ok(None) };
        let SqlStatement::Insert(Insert { table: TableObject::TableName(name), source: Some(query), on: None, columns: names, .. }) = statement.as_mut() else { return Ok(None) };
        let target = self.table_ref(name)?;
        if self.home(&target)? != Home::Page {
            return Ok(None);
        }
        let Some(table) = self.default_schema()?.1.table(target.table()).await? else { return Ok(None) };
        let columns: Vec<String> = table.schema().fields().iter().map(|f| f.name().clone()).collect();
        let named: Vec<String> = names.iter().map(crate::text::object).collect();
        // (an ALWAYS identity is given no value by an INSERT, which a node refuses as 428C9: the statement's own columns are judged)
        if let Some(ruled) = self.ruled_of(target.table()).filter(|r| r.table.columns.iter().any(|c| c.always)) {
            crate::rules::keys::inserted(&ruled.table, &name.to_string(), &named, &query.to_string())?;
        }
        let named = match named.is_empty() {
            true => crate::rules::fit::implicit(&columns, crate::rules::rows::rows_width(&query.to_string())).to_vec(),
            false => named,
        };
        let default = |c: &str| table.get_column_default(c).and_then(|e| datafusion::sql::unparser::expr_to_sql(e).ok()).map(|e| e.to_string());
        let source = query.to_string();
        let text = crate::rules::rows::rows_for(&name.to_string(), &columns, &named, &source, &default)?; // (its refusals are the node's)
        let quoted = |names: &[String]| names.iter().map(|c| ObjectName::from(vec![Ident::with_quote('"', c.as_str())])).collect();
        if crate::rules::rows::plain_values(&source).is_none() {
            // (DataFusion types every VALUES in an INSERT's query by the INSERT's columns, a subquery's too, so a query keeps the
            // columns its rows give, and the planner fills the others with their defaults, as `rows_for` would)
            *names = quoted(&named);
            return Ok(Some(statement.to_string()));
        }
        *query = self.query_of_text(&text)?;
        *names = quoted(&columns);
        Ok(Some(statement.to_string()))
    }

    /// The rules a CREATE TABLE `c` makes, over the table as `plan` plans it (`made_table`), with the statement's text kept
    /// for a kept page (`CREATE_KEY`). None when the table has nothing a write could break (`has_rules`): it costs nothing,
    /// and its kept file has no note.
    fn ruled_made(&self, c: &CreateTable, plan: &LogicalPlan, identified: Vec<counters::Identified>) -> anyhow::Result<Option<Ruled>> {
        let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(made)) = plan else { anyhow::bail!("{} is not planned as a table", c.name) };
        let table = self.made_table(c, made.input.schema().as_arrow())?;
        if !has_rules(&table) {
            return Ok(None);
        }
        let held = identified.into_iter().map(|i| (i.sequence, i.counter)).collect();
        Ok(Some(Ruled { create: SqlStatement::CreateTable(c.clone()).to_string(), table: Arc::new(table), counters: Arc::new(std::sync::Mutex::new(held)) }))
    }

    /// The identity columns of a CREATE TABLE `c` (`counters::identified`), each with its sequence named past the names the page's
    /// tables' counters hold: a table renamed away keeps its sequence, so a new one may find its name taken.
    fn identified(&self, c: &CreateTable) -> anyhow::Result<Vec<counters::Identified>> {
        let taken: BTreeSet<String> = {
            let all = self.ruled.lock().unwrap_or_else(PoisonError::into_inner);
            all.values().flat_map(|r| r.counters.lock().unwrap_or_else(PoisonError::into_inner).keys().cloned().collect::<Vec<_>>()).collect()
        };
        counters::identified(c, |name| taken.contains(name))
    }

    /// The rules a CREATE TABLE declares (`rules::Table::of_create`) over the columns `schema` gives it. Each CHECK is checked as
    /// a node checks it at CREATE (`defaults::definition`): planned over an empty table of those columns, so a CHECK that names
    /// no column, or isn't a condition, is refused now and not at each row.
    fn made_table(&self, c: &CreateTable, schema: &Schema) -> anyhow::Result<crate::rules::Table> {
        let table = crate::rules::Table::of_create(c, &Arc::new(schema.clone()))?;
        for (name, check) in &table.checks {
            crate::rules::fit::breaking(&self.ctx, &RecordBatch::new_empty(table.schema.clone()), check).with_context(|| format!("CONSTRAINT {name} CHECK ({check})")).map_err(crate::rules::create::definition)?;
        }
        Ok(table)
    }

    /// An UPDATE that sets a key column of a page table with rules is refused as a node refuses it (`txn::refuse_change`): the
    /// row would move to another key, so the key is changed by a DELETE and an INSERT. The text is parsed only when it has an
    /// UPDATE in it, and a parse that fails is the planner's to answer.
    fn key_set(&self, expanded: &str) -> anyhow::Result<()> {
        if !expanded.to_ascii_lowercase().contains("update") {
            return Ok(());
        }
        let statement = match self.parsed(expanded) {
            Ok(DfStatement::Statement(s)) => *s,
            Ok(DfStatement::Explain(e)) if e.options.analyze => match *e.statement {
                DfStatement::Statement(s) => *s,
                _ => return Ok(()),
            },
            _ => return Ok(()),
        };
        let SqlStatement::Update(Update { table: target, assignments, .. }) = statement else { return Ok(()) };
        let TableFactor::Table { name, .. } = &target.relation else { return Ok(()) };
        let table = self.table_ref(name)?.table().to_string();
        let Some(ruled) = self.ruled_of(&table) else { return Ok(()) };
        // (an ALWAYS identity is set only to DEFAULT, as a node sets it: 428C9)
        crate::rules::keys::set(&ruled.table, &set_columns(&assignments))?;
        for assignment in &assignments {
            let columns: Vec<&ObjectName> = match &assignment.target {
                AssignmentTarget::ColumnName(n) => vec![n],
                AssignmentTarget::Tuple(ns) => ns.iter().collect(),
            };
            for column in columns {
                let Some(ObjectNamePart::Identifier(ident)) = column.0.last() else { continue };
                let column = crate::text::ident(ident);
                if ruled.table.key.contains(&column) {
                    return Err(crate::codes::coded("0A000", format!("UPDATE of {table}'s key column {column} isn't taken here: delete the row, then insert it with its new key")));
                }
            }
        }
        Ok(())
    }

    /// The rules a page table keeps, if it has any (`Ruled`).
    fn ruled_of(&self, name: &str) -> Option<Ruled> {
        self.ruled.lock().unwrap_or_else(PoisonError::into_inner).get(name).cloned()
    }

    /// Makes `name`'s rules `ruled` (None takes them away): what a CREATE or a DROP leaves its table with.
    fn set_ruled(&self, name: &str, ruled: Option<Ruled>) {
        let mut all = self.ruled.lock().unwrap_or_else(PoisonError::into_inner);
        match ruled {
            Some(ruled) => {
                all.insert(name.to_string(), ruled);
            }
            None => {
                all.remove(name);
            }
        }
    }

    /// A statement planned (`planned`) and run: its frame, and the name of the page table it changes, if any. The page's
    /// `ask` and the tests read a page this way.
    async fn plan(&self, text: &str) -> anyhow::Result<(DataFrame, Option<String>)> {
        let planned = self.planned(text).await?;
        Ok((crate::engine::run(&planned.ctx, planned.plan).await?, planned.changed))
    }

    /// A write to a page table, run and kept before the next statement is planned (`keeping`). Every page takes the table as
    /// the write found it, kept or not, so a write that fails is undone (`undone`): one the watch stops (53200), one that
    /// fails as it runs (its own words), one whose rows break the table's rules (`kept_rules`), and one the store refuses
    /// (`written`: a full store, say; that one says the page couldn't keep it). An INSERT appends its rows only once it has
    /// them all, and a DELETE or UPDATE rewrites the table in one step, so a stop leaves those as they were; but a CREATE OR
    /// REPLACE TABLE … AS takes the old table away before it collects, and taking `before` copies no rows, so it is taken for
    /// every write. The rules are put back with the table (`undo`).
    ///
    /// In an open transaction (`page::txn`), an INSERT, UPDATE or DELETE is the transaction's: the table is kept at its COMMIT,
    /// not now (`in_txn`). A CREATE or DROP is DDL, which runs at once and is kept now (`at_once`).
    async fn write(&self, ctx: &SessionContext, plan: LogicalPlan, name: &str, ruled: Option<Ruled>) -> anyhow::Result<Vec<RecordBatch>> {
        self.unwritable(name).await?;
        let change = change_of(&plan);
        let note = match change {
            Change::Create { .. } => self.at_once("CREATE TABLE", &[name])?,
            Change::Drop => self.at_once("DROP TABLE", &[name])?,
            _ => None,
        };
        let before = self.before(name).await?;
        let had = self.ruled_of(name);
        let held = note.is_none() && self.in_txn(name, &before, &had);
        let batches = match rows_of(ctx, plan, self.watch.as_ref()).await {
            Ok(batches) => batches,
            Err(e) => return Err(self.undone(name, before, had, e, "").await),
        };
        if let Err(e) = self.kept_rules(name, &before, ruled, change).await {
            return Err(self.refused(name, before, had, e).await);
        }
        let kept = match held {
            true => self.registered(name).await.map(|_| ()),
            false => self.written(name).await,
        };
        if let Err(e) = kept {
            return Err(self.undone(name, before, had, e, ": the page couldn't keep it").await);
        }
        // (a CREATE IF NOT EXISTS that found its table ran nothing, so it says nothing)
        if let Some(note) = note.filter(|_| !matches!(change, Change::Create { if_not_exists: true }) || before.is_none()) {
            self.note(note);
        }
        Ok(batches)
    }

    /// The rules of the page table `name` after its rows are in (`write`): a CREATE that made the table takes the rules it made
    /// (`ruled`, None for a CREATE … AS, so an OR REPLACE over a ruled table takes its rules away), a CREATE IF NOT EXISTS that
    /// found the table leaves them as they were, and a DROP takes them away. An INSERT or an UPDATE is checked against them
    /// (`holds`), after the rules are set and before the table is kept, so refused rows never reach the kept file.
    async fn kept_rules(&self, name: &str, before: &Before, ruled: Option<Ruled>, change: Change) -> anyhow::Result<()> {
        match change {
            Change::Create { if_not_exists } => {
                if !(if_not_exists && before.is_some()) {
                    self.set_ruled(name, ruled);
                }
                Ok(())
            }
            Change::Drop => {
                self.set_ruled(name, None);
                Ok(())
            }
            Change::Insert { .. } | Change::Update => self.holds(name, before, change).await,
            Change::Other => Ok(()),
        }
    }

    /// Whether page table `name` keeps its rules after a write, checked as a node checks a write: an INSERT's new rows meet the
    /// core's rules (`rules::checks`: CHECK, NOT NULL, held types and enum labels), give no key or UNIQUE value twice
    /// (`rules::keys`, which skips a NULL), and none a row held before gives (`held_again`). An INSERT that only appends checks
    /// just its new rows (the partitions past their length before); one that rewrites the table checks the whole table (no key
    /// or UNIQUE value twice, projected to those columns), as an UPDATE does, and its key is checked too. After an UPDATE the whole table is checked the same way, except for the
    /// key, which an UPDATE can't set (`key_set`). The rows that were there met the rules before, so a refusal is the write's.
    /// Known difference: a CHECK that isn't the same each time (`now()`, `random()`) is checked on an UPDATE over rows the UPDATE
    /// didn't touch.
    async fn holds(&self, name: &str, before: &Before, change: Change) -> anyhow::Result<()> {
        let Some(ruled) = self.ruled_of(name) else { return Ok(()) };
        let t = &ruled.table;
        // (a table whose key isn't checked here, with no UNIQUE, CHECK, held or enum column, has nothing to read)
        if !t.key_checked && t.unique.is_empty() && t.checks.is_empty() && !t.columns.iter().any(|c| c.held.is_some() || c.labels.is_some()) {
            return Ok(());
        }
        let insert = matches!(change, Change::Insert { .. });
        let appends = matches!(change, Change::Insert { appends: true });
        let (_, page) = self.default_schema()?;
        let Some(table) = page.table(name).await? else { return Ok(()) };
        let schema = table.schema();
        let parts = partitions_of(&table).await.ok_or_else(|| anyhow::anyhow!("{name}: a page's table holds its rows in memory"))?;
        let all: Vec<RecordBatch> = parts.iter().flatten().cloned().collect();
        let fresh: Vec<RecordBatch> = match before {
            // (MemSink appends each partition's rows, so the new ones are each partition's past the length it had)
            Some((_, held)) if appends => parts
                .iter()
                .enumerate()
                .flat_map(|(i, part)| {
                    let was = held.get(i).map_or(0, |h| h.len()).min(part.len());
                    part[was..].to_vec()
                })
                .collect(),
            _ => all.clone(),
        };
        let fresh = datafusion::arrow::compute::concat_batches(&schema, &fresh)?;
        if fresh.num_rows() == 0 {
            return Ok(());
        }
        crate::rules::checks(t, name, &fresh, true, &self.ctx)?;
        // (an appending INSERT's new rows against the rows held before it, which met the rules: probed, not hashed whole)
        let held: Vec<RecordBatch> = match before {
            Some((_, held)) if appends => held.iter().flatten().cloned().collect(),
            _ => vec![],
        };
        if insert && t.key_checked {
            let (pkey, key) = crate::rules::keys::once(name, &t.key, &fresh)?; // (a key given twice by the INSERT is named first, as the node names it)
            let found = match appends {
                true => held_again(&fresh, &held, &key)?,
                false => crate::rules::keys::twice(&projected(&schema, &all, &key)?, &key, &[])?,
            };
            if let Some(found) = found {
                return Err(crate::rules::keys::violated(&pkey, &key, found));
            }
        }
        for (constraint, columns) in &t.unique {
            let found = match appends {
                true => crate::rules::keys::twice(&fresh, columns, &[])?.map_or_else(|| held_again(&fresh, &held, columns), |f| Ok(Some(f)))?,
                false => crate::rules::keys::twice(&projected(&schema, &all, columns)?, columns, &[])?,
            };
            if let Some(found) = found {
                return Err(crate::rules::keys::violated(constraint, columns, found));
            }
        }
        Ok(())
    }

    /// Refuses a write to a table whose kept file `kept` couldn't read (`unread`), while that file is in the store: a write
    /// keeps the table it leaves, or forgets a dropped one, over that same file, so the rows the file holds would be gone.
    /// Once the file is removed, the name is the page's again.
    async fn unwritable(&self, name: &str) -> anyhow::Result<()> {
        self.unread_there(name, &kept_at(name), "table").await
    }

    /// `unwritable` of the kept file at `at`, which holds the `what` (a table or a view) named `name`.
    async fn unread_there(&self, name: &str, at: &Path, what: &str) -> anyhow::Result<()> {
        let Some((path, why)) = self.unread.iter().find(|(path, _)| path.as_str() == at.as_ref()) else { return Ok(()) };
        match self.files.head(at).await {
            Err(StoreError::NotFound { .. }) => Ok(()),
            _ => Err(crate::codes::coded("55000", format!("{name}: the page couldn't read its kept file {path} ({why}), so the file is left as it is and the {what} can't be written until the file is removed"))),
        }
    }

    /// A failed write's error, once the write is undone (`undo`) to what it found (`before`, and the rules it `had`), with
    /// `why` after the words saying so.
    async fn undone(&self, name: &str, before: Before, had: Option<Ruled>, e: anyhow::Error, why: &str) -> anyhow::Error {
        match self.undo(name, before, had).await {
            Ok(()) => e.context(format!("{name}: the write was undone{why}")),
            Err(u) => e.context(format!("{name}: the write couldn't be undone ({u:#})")),
        }
    }

    /// A write a table's rules refuse (`kept_rules`), once the write is undone (`undo`): the refusal itself, in the node's words,
    /// since a node checks before it writes and has nothing to undo to say of it. Only a failed undo is said, with its words.
    async fn refused(&self, name: &str, before: Before, had: Option<Ruled>, e: anyhow::Error) -> anyhow::Error {
        match self.undo(name, before, had).await {
            Ok(()) => e,
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

    /// Puts the page table `name` back as `before` found it, and its rules as `had` found them, after a write the page couldn't
    /// keep: the table the write made or changed is taken away, and the table it was (if there was one) is registered again
    /// with its rows put back. INSERT, UPDATE and DELETE change a memory table in place, so its partitions are written back; a
    /// DROP, or a CREATE OR REPLACE that took the old table away before it collected, left the old table itself as it was, so
    /// registering it again is all it takes.
    async fn undo(&self, name: &str, before: Before, had: Option<Ruled>) -> anyhow::Result<()> {
        self.set_ruled(name, had);
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
        let Some(table) = self.registered(name).await? else {
            return if self.keeps { self.forget(name).await } else { Ok(()) };
        };
        if !self.keeps {
            return Ok(());
        }
        let batches = batches_of(&table).await.ok_or_else(|| anyhow::anyhow!("{name}: a page's table holds its rows in memory"))?;
        // (the counters are kept with the rules, so the file holds each counter as this write left it)
        let file = table_file(name, table.as_ref(), &batches, self.ruled_of(name).as_ref())?;
        self.files.put_opts(&kept_at(name), PutPayload::from(file), PutOptions { mode: PutMode::Overwrite, ..Default::default() }).await?;
        Ok(())
    }

    /// The page's table `name`, registered again (`written`) so its statistics are worked out afresh, and not kept: a write in
    /// a transaction is kept at its COMMIT. None when the page has no table by that name.
    async fn registered(&self, name: &str) -> anyhow::Result<Option<Arc<dyn TableProvider>>> {
        let (_, page) = self.default_schema()?;
        let Some(table) = page.table(name).await? else { return Ok(None) };
        page.deregister_table(name)?;
        register_page_table(page.as_ref(), name.to_string(), inner(&table).clone())?;
        Ok(Some(table))
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
        let (name, table, ruled) = self.table_from_file(file)?;
        let (_, page) = self.default_schema()?;
        register_page_table(page.as_ref(), name.clone(), Arc::new(table))?;
        self.set_ruled(&name, ruled);
        Ok(())
    }

    /// The table a kept file holds, read back: its name (`pondra.table`), its rows as a memory table over the table's own
    /// schema, its column defaults again (`column_defaults`), and the rules its CREATE TABLE text makes (`ruled_from`), when
    /// the file has that text. A file without it, kept before the table had rules, has none.
    fn table_from_file(&self, file: bytes::Bytes) -> anyhow::Result<(String, MemTable, Option<Ruled>)> {
        let reader = FileReader::try_new(std::io::Cursor::new(file), None)?;
        let kept = reader.schema();
        let name = kept.metadata().get(TABLE_KEY).cloned().ok_or_else(|| anyhow::anyhow!("it has no {TABLE_KEY} note, so it is not a kept table"))?;
        let schema = table_schema(&kept);
        let defaults = self.column_defaults(&kept, &schema)?;
        let ruled = match kept.metadata().get(CREATE_KEY) {
            Some(text) => Some(self.ruled_from(text, &schema, kept.metadata().get(COUNTERS_KEY).map(String::as_str))?),
            None => None,
        };
        let mut batches = vec![];
        for batch in reader {
            batches.push(over_schema(&schema, &batch?)?);
        }
        Ok((name, MemTable::try_new(schema, vec![batches])?.with_column_defaults(defaults), ruled))
    }

    /// The rules a kept table's CREATE TABLE text makes (`made_table`), over the table's schema, with the text kept as it is, and
    /// its counters as its counters note has them (`COUNTERS_KEY`): a file with none starts each identity column at its start, as
    /// the CREATE made it. A note whose text is not a CREATE TABLE, or fails to parse or to build, makes the table unread, as a bad
    /// file does (`kept` lists it by its path and the words of why).
    fn ruled_from(&self, text: &str, schema: &Schema, counted: Option<&str>) -> anyhow::Result<Ruled> {
        let DfStatement::Statement(s) = self.parsed(text)? else { anyhow::bail!("its {CREATE_KEY} note is not a CREATE TABLE") };
        let SqlStatement::CreateTable(c) = *s else { anyhow::bail!("its {CREATE_KEY} note is not a CREATE TABLE") };
        let held: BTreeMap<String, counters::Counter> = match counted {
            Some(json) => serde_json::from_str(json).with_context(|| format!("its {COUNTERS_KEY} note is not a table's counters"))?,
            None => counters::starting(&c)?,
        };
        Ok(Ruled { create: text.to_string(), table: Arc::new(self.made_table(&c, schema)?), counters: Arc::new(std::sync::Mutex::new(held)) })
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
        let types = match given.options.get("types") {
            Some(t) => Some(crate::ext_names::declared(session(), t).await?.iter().map(|f| (f.name().clone(), f.data_type().clone())).collect::<Vec<_>>()),
            None => None,
        };
        let undeclared = declared.is_none();
        let (columns, data) = crate::ext_names::files_table(&state, &given, declared, &hive_types, types.as_deref(), |u| {
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
        for name in self.views.lock().unwrap_or_else(PoisonError::into_inner).keys() {
            objects.push(json!({"catalog": "page", "schema": "public", "name": name, "kind": "view"}));
        }
        Ok(Answer::ok(JSON, serde_json::to_vec(&json!({"objects": objects, "files": WORKING}))?))
    }
}

/// A page's views. DataFusion's own view keeps a plan made once, over the tables as they were then, and a page registers
/// a table again after each write (`written`), so a page keeps each view's query as text (`views`) and the front reads it
/// into each statement that names the view (`views_for`), as a node's front reads a request's own views.
impl Page {
    /// The page's view statements, answered here before anything is planned: CREATE VIEW and DROP VIEW, and the
    /// statements a view's name changes (a DROP TABLE of a table a view reads, a CREATE TABLE or INSERT naming a view), in
    /// a node's words. None for every other statement, and for one that doesn't parse, which the planner answers.
    async fn view_statement(&self, text: &str) -> anyhow::Result<Option<Answer>> {
        if !text.to_ascii_lowercase().contains("view") && self.views_held().is_empty() {
            return Ok(None);
        }
        let Some(statement) = self.statement_of(text) else { return Ok(None) };
        match statement {
            SqlStatement::CreateView(c) if !c.materialized => self.at_once_run("CREATE VIEW", &[], self.create_view(c)).await.map(Some),
            SqlStatement::Drop { object_type: ObjectType::View, if_exists, names, cascade, .. } => self.at_once_run("DROP VIEW", &[], self.drop_views(&names, if_exists, cascade)).await.map(Some),
            SqlStatement::Drop { object_type: ObjectType::Table, if_exists, names, cascade, .. } => {
                for name in &names {
                    if let Ok(table) = self.table_named(name) {
                        self.at_once("DROP TABLE", &[&table])?; // (before CASCADE drops a view: the DROP itself is `write`'s)
                    }
                    if !if_exists {
                        self.not_a_view(name, "no table ", "")?;
                    }
                    self.read_by(name, cascade).await?;
                }
                Ok(None)
            }
            SqlStatement::CreateTable(c) => self.not_a_view(&c.name, "", " is a view").map(|_| None),
            SqlStatement::Insert(Insert { table: TableObject::TableName(name), .. }) => self.not_a_view(&name, "", " is a view: write to the tables it reads").map(|_| None),
            _ => Ok(None),
        }
    }

    /// `text` as one SQL statement, as written or once DuckDB's forms are made SQL that parses (`friendly::text`), or None.
    fn statement_of(&self, text: &str) -> Option<SqlStatement> {
        let parsed = self.parsed(text).ok().or_else(|| self.parsed(&crate::friendly::text(text).ok()?).ok());
        match parsed? {
            DfStatement::Statement(s) => Some(*s),
            _ => None,
        }
    }

    /// CREATE VIEW: the view's query, planned now as a statement would read it (`view_plans`), so a view that can't be read
    /// is refused when it is made, then held and kept. A name a table or a view has is refused in a node's words; with IF
    /// NOT EXISTS, an existing view is a node's answer and nothing changes.
    async fn create_view(&self, c: CreateView) -> anyhow::Result<Answer> {
        let name = self.view_name(&c.name)?;
        if self.default_schema()?.1.table_exist(&name) {
            anyhow::bail!("{name} is a table");
        }
        let exists = self.views_held().contains_key(&name);
        if exists && c.if_not_exists {
            return answered(json!({"name": name, "exists": true}));
        }
        if exists && !c.or_replace {
            anyhow::bail!("view {name} already exists (CREATE OR REPLACE VIEW)");
        }
        let query = match c.columns.is_empty() {
            true => c.query.to_string(),
            false => {
                let columns = c.columns.iter().map(|column| column.name.to_string()).collect::<Vec<_>>().join(", ");
                format!("SELECT * FROM ({}) AS {}({columns})", c.query, crate::ext_names::quoted(&name))
            }
        };
        self.view_plans(&name, &query).await?;
        self.unread_there(&name, &view_at(&name), "view").await?;
        self.keep_view(&name, Some(&query)).await?;
        self.views.lock().unwrap_or_else(PoisonError::into_inner).insert(name.clone(), query);
        answered(json!({"view": name}))
    }

    /// Plans the query of the view `name` as a statement would read it, without the view itself. A page with a node refuses
    /// a view over the node's tables by name: a page's view reads the page's own tables and files.
    async fn view_plans(&self, name: &str, query: &str) -> anyhow::Result<()> {
        match self.planned_as(query, &[name.to_string()]).await {
            Ok(_) => Ok(()),
            Err(e) if self.node.is_some() && crate::codes::of(&e) == "42P01" => Err(crate::codes::coded("0A000", format!("{name}: a page's view reads the page's own tables and files, and this one names a table the page doesn't have: make a page table of the node's rows first (CREATE TABLE … AS SELECT …)"))),
            Err(e) => Err(e.context("the view's query")),
        }
    }

    /// DROP VIEW, of each view named: it goes, and so does its kept file. A name with no view is refused in a node's words,
    /// or with IF EXISTS answered as not dropped. A view another reads may go, as on a node; the other then fails when read,
    /// unless CASCADE drops it too.
    async fn drop_views(&self, names: &[ObjectName], if_exists: bool, cascade: bool) -> anyhow::Result<Answer> {
        let mut answer = json!({});
        for name in names {
            let name = self.view_name(name)?;
            if !self.views_held().contains_key(&name) {
                anyhow::ensure!(if_exists, "no view {name}");
                answer = json!({"view": name, "dropped": false});
                continue;
            }
            if cascade {
                self.cascade(&name, 0).await?;
            }
            self.forget_view(&name).await?;
            answer = json!({"view": name, "dropped": true});
        }
        answered(answer)
    }

    /// A view that goes: its kept file, then the view.
    async fn forget_view(&self, name: &str) -> anyhow::Result<()> {
        self.unread_there(name, &view_at(name), "view").await?;
        self.keep_view(name, None).await?;
        self.views.lock().unwrap_or_else(PoisonError::into_inner).remove(name);
        Ok(())
    }

    /// A DROP TABLE of `table` while views read it is refused in a node's words (`ddl::drop_table`); with CASCADE, those
    /// views go first, as a node drops them.
    async fn read_by(&self, table: &ObjectName, cascade: bool) -> anyhow::Result<()> {
        let table = self.table_ref(table)?.table().to_string();
        let readers = self.readers(&table)?;
        if readers.is_empty() {
            return Ok(());
        }
        if !cascade {
            let listed = readers.iter().map(|view| format!("view {view}")).collect::<Vec<_>>().join(", ");
            anyhow::bail!("{table} is read by {listed}: DROP TABLE {table} CASCADE drops them too, or drop them first");
        }
        self.cascade(&table, 0).await
    }

    /// CASCADE: the views that read `name` go, each after the views that read it, as a node drops them
    /// (`routines::views_reading`).
    async fn cascade(&self, name: &str, depth: usize) -> anyhow::Result<()> {
        anyhow::ensure!(depth < 16, "{name}: views that read views that read it, 16 deep: drop them by hand");
        for view in self.readers(name)? {
            if self.views_held().contains_key(&view) {
                Box::pin(self.cascade(&view, depth + 1)).await?;
                self.forget_view(&view).await?;
            }
        }
        Ok(())
    }

    /// The views whose queries name the page table or view `table`, in name order.
    fn readers(&self, table: &str) -> anyhow::Result<Vec<String>> {
        let state = self.ctx.state();
        let options = state.config().options();
        let (catalog, schema) = (&options.catalog.default_catalog, &options.catalog.default_schema);
        let mut readers = vec![];
        for (view, query) in self.views_held() {
            let Some(statement) = self.statement_of(&query) else { continue };
            let named = state.resolve_table_references(&DfStatement::Statement(Box::new(statement)))?;
            if named.iter().any(|n| n.table() == table && n.schema().is_none_or(|s| s == schema) && n.catalog().is_none_or(|c| c == catalog)) {
                readers.push(view);
            }
        }
        Ok(readers)
    }

    /// Refuses `name` when a view has it: a node's words, `before` and `after` the name.
    fn not_a_view(&self, name: &ObjectName, before: &str, after: &str) -> anyhow::Result<()> {
        let name = self.table_ref(name)?.table().to_string();
        match self.views_held().contains_key(&name) {
            true => anyhow::bail!("{before}{name}{after}"),
            false => Ok(()),
        }
    }

    /// A view's name as the planner reads it, in the page's one schema; a name in another schema or catalog is refused by
    /// name.
    fn view_name(&self, name: &ObjectName) -> anyhow::Result<String> {
        let reference = self.table_ref(name)?;
        let state = self.ctx.state();
        let options = state.config().options();
        let here = reference.schema().is_none_or(|s| s == options.catalog.default_schema) && reference.catalog().is_none_or(|c| c == options.catalog.default_catalog);
        match here {
            true => Ok(reference.table().to_string()),
            false => Err(crate::codes::coded("0A000", format!("{reference}: a page has one schema, public, so its views are made there"))),
        }
    }

    /// The page's views, each name with its query: a copy, so no lock is held past it.
    fn views_held(&self) -> BTreeMap<String, String> {
        self.views.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// The views `text` names, each as the front reads it (its own views read first, so a view may read another), by
    /// every name a statement may give it (`v`, `public.v`, `page.public.v`): what the front puts in a statement's place
    /// of each. `outer` are the views being read already, left out, so a view that names itself, or one that reads it,
    /// is a missing table rather than a loop.
    async fn views_for(&self, text: &str, outer: &[String]) -> anyhow::Result<HashMap<String, String>> {
        let mut views = HashMap::new();
        for (name, query) in self.views_held() {
            if outer.contains(&name) || !crate::text::mentions(text, &name) {
                continue;
            }
            let inner = [outer, std::slice::from_ref(&name)].concat();
            let read = Box::pin(self.views_for(&query, &inner)).await?;
            let query = crate::front::expand(&PageHost(self), &query, &read).await?;
            for key in [format!("page.public.{name}"), format!("public.{name}"), name] {
                views.insert(key, query.clone());
            }
        }
        Ok(views)
    }

    /// Keeps the view `name`'s query in a kept page's store (`view_at`), or with None forgets it. A page in memory keeps
    /// nothing.
    async fn keep_view(&self, name: &str, query: Option<&str>) -> anyhow::Result<()> {
        if !self.keeps {
            return Ok(());
        }
        let at = view_at(name);
        match query {
            Some(query) => {
                let file = serde_json::to_vec(&json!({"name": name, "query": query}))?;
                self.files.put_opts(&at, PutPayload::from(file), PutOptions { mode: PutMode::Overwrite, ..Default::default() }).await?;
            }
            None => match self.files.delete(&at).await {
                Ok(()) | Err(StoreError::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            },
        }
        Ok(())
    }

    /// Reads the kept view at `at` back into the page. Nothing is planned: a view whose tables are gone fails when read.
    async fn view_back(&self, at: &Path) -> anyhow::Result<()> {
        let file = self.files.get(at).await?.bytes().await?;
        let kept: serde_json::Value = serde_json::from_slice(&file).context("it is not a kept view")?;
        let (Some(name), Some(query)) = (kept["name"].as_str(), kept["query"].as_str()) else { anyhow::bail!("it has no name and query, so it is not a kept view") };
        self.views.lock().unwrap_or_else(PoisonError::into_inner).insert(name.to_string(), query.to_string());
        Ok(())
    }
}

/// An answer of one JSON object, as a node answers a statement that isn't a query.
fn answered(object: serde_json::Value) -> anyhow::Result<Answer> {
    Ok(Answer::ok(JSON, serde_json::to_vec(&object)?))
}

/// Where a kept page keeps the view `name`: its name and query, as JSON.
fn view_at(name: &str) -> Path {
    Path::from_iter([VIEWS, format!("{name}.json").as_str()])
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

/// `inner`, held to the page's memory watch: at each poll, before `inner` is polled, a page past its line whose query has
/// grown it by a tenth of the line is stopped. The query is answered 53200 and its future dropped, so its memory goes with
/// it, while the page answers the rest. A query's growth is what its own polls grew the page by (the live bytes after each
/// poll less before it), not what the page holds: a page runs one poll at a time, so two queries at once each count their
/// own, and a query that starts past the line and grows nothing is not stopped. A page's query yields between batches
/// (DataFusion's cooperative streams, which `web/`'s build turns on), so the watch gets to look between them. With no watch,
/// `inner` runs as it is.
async fn watched<T>(watch: Option<&Watch>, inner: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    let Some(watch) = watch else { return inner.await };
    let margin = watch.line / 10; // (`oom::pick`'s margin, a node's)
    let mut inner = Some(Box::pin(inner));
    let mut grown = 0usize; // (what this query's polls grew the page by, less what they let go)
    std::future::poll_fn(move |cx| {
        let now = (watch.live)();
        if now >= watch.line && grown >= margin {
            inner = None; // (the query's memory goes with its future, before anything else is read)
            return Poll::Ready(Err(stopped()));
        }
        let Some(future) = inner.as_mut() else { return Poll::Ready(Err(stopped())) }; // (never polled once it has answered: an answer is the stop's)
        let polled = future.as_mut().poll(cx);
        let after = (watch.live)();
        grown = grown.saturating_add(after.saturating_sub(now)).saturating_sub(now.saturating_sub(after));
        polled
    })
    .await
}

/// The answer to a query the watch stopped (`watched`).
fn stopped() -> anyhow::Error {
    crate::codes::coded("53200", PAGE_OOM)
}

/// The page's memory line in bytes: `PONDRA_MEMORY_MAX_GB`, else the line its `device` has (`Options::line`), else 3 GiB
/// (`LINE`), and None when it is 0 (the watch is off, as on a node).
fn memory_line(device: Option<usize>) -> Option<usize> {
    line_of(crate::env::gib("PONDRA_MEMORY_MAX_GB"), device)
}

/// `memory_line` of the line `set` and the device's line, as the setting and `Options::line` give them.
fn line_of(set: Option<usize>, device: Option<usize>) -> Option<usize> {
    let bytes = set.or(device).unwrap_or(LINE);
    (bytes > 0).then_some(bytes)
}

/// The most query memory a page's pool holds: `PONDRA_MEMORY_GB`, else two thirds of the line (of the default line when the
/// watch is off, so the pool is never empty). What DataFusion counts is the big hash tables and sort buffers, so the rest of
/// the line is for what it doesn't count.
fn query_memory(device: Option<usize>) -> usize {
    crate::env::gib("PONDRA_MEMORY_GB").unwrap_or_else(|| memory_line(device).unwrap_or(LINE) / 3 * 2)
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
/// back as it was, with its CREATE TABLE text and counters (`ruled`) when it has rules.
fn table_file(name: &str, table: &dyn TableProvider, batches: &[RecordBatch], ruled: Option<&Ruled>) -> anyhow::Result<bytes::Bytes> {
    let schema = kept_schema(name, table, ruled)?;
    let mut writer = FileWriter::try_new(Vec::new(), &schema)?;
    for batch in batches {
        writer.write(&over_schema(&schema, batch)?)?;
    }
    Ok(writer.into_inner()?.into())
}

/// The schema a table is kept under: its own, with `pondra.table` (its name) in the schema's notes, `pondra.create` (its CREATE
/// TABLE text, when it has rules) and `pondra.counters` (its identity counters, when it has any) beside it, and `pondra.default`
/// (the column's default, as SQL text) in the notes of each column that has one.
fn kept_schema(name: &str, table: &dyn TableProvider, ruled: Option<&Ruled>) -> anyhow::Result<SchemaRef> {
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
    if let Some(ruled) = ruled {
        notes.insert(CREATE_KEY.to_string(), ruled.create.clone());
        let counted = ruled.counters.lock().unwrap_or_else(PoisonError::into_inner);
        if !counted.is_empty() {
            notes.insert(COUNTERS_KEY.to_string(), serde_json::to_string(&*counted)?);
        }
    }
    Ok(Arc::new(Schema::new_with_metadata(fields, notes)))
}

/// The schema of a kept file without the two notes `kept_schema` added: the schema the table itself has.
fn table_schema(kept: &Schema) -> SchemaRef {
    let fields: Vec<Field> = kept.fields().iter().map(|f| f.as_ref().clone().with_metadata(without_notes(f.metadata()))).collect();
    Arc::new(Schema::new_with_metadata(fields, without_notes(kept.metadata())))
}

/// The notes without the ones `kept_schema` adds, which a table's own schema does not have.
fn without_notes(notes: &HashMap<String, String>) -> HashMap<String, String> {
    notes.iter().filter(|(k, _)| ![TABLE_KEY, DEFAULT_KEY, CREATE_KEY, COUNTERS_KEY].contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Whether a table has any rule a write could break: an enforced key, a UNIQUE, a CHECK, or a held type or enum column, or an
/// identity column (ALWAYS, or one whose DEFAULT counts), whose counter the table keeps. A table with none keeps no rules at all
/// (`ruled_made`).
fn has_rules(t: &crate::rules::Table) -> bool {
    !t.key.is_empty()
        || !t.unique.is_empty()
        || !t.checks.is_empty()
        || t.columns.iter().any(|c| c.held.is_some() || c.labels.is_some() || c.always || c.default.as_deref().is_some_and(|d| d.starts_with("nextval(")))
}

/// The first value of `columns` that a row of `new` gives and a row of `held` gives too, as Postgres's DETAIL writes it
/// (`rules::keys::shown`), or None; a NULL in any of the columns matches nothing, as UNIQUE takes it. A few new rows are
/// each compared with the held columns (`held_by_value`): a one-row INSERT into a table of a million keys compares one
/// column, where hashing the whole table into a map took 73 ms. More are hashed into a map, and each held batch is hashed
/// and probed against it, so no held batch is copied.
fn held_again(new: &RecordBatch, held: &[RecordBatch], columns: &[String]) -> anyhow::Result<Option<String>> {
    use datafusion::arrow::array::{make_comparator, DynComparator};
    use datafusion::common::hash_utils::{create_hashes, RandomState};
    if held.iter().all(|b| b.num_rows() == 0) {
        return Ok(None);
    }
    let mine = picked(new, columns)?;
    if new.num_rows() <= 16 {
        if let Ok(found) = held_by_value(&mine, held, columns) {
            return Ok(found); // (else a type the comparison kernels don't take, which goes the hashed way)
        }
    }
    let hashed = |c: &[ArrayRef], n: usize| -> anyhow::Result<Vec<u64>> {
        let mut hashes = vec![0u64; n];
        create_hashes(c, &RandomState::with_seed(0), &mut hashes)?;
        Ok(hashes)
    };
    // (each hash's first new row, and the rare other rows of a hash two new values share)
    let (mut first, mut more) = (HashMap::with_capacity(new.num_rows()), vec![]);
    for (i, h) in hashed(&mine, new.num_rows())?.into_iter().enumerate() {
        if mine.iter().any(|c| c.is_null(i)) {
            continue;
        }
        match first.entry(h) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(i);
            }
            std::collections::hash_map::Entry::Occupied(_) => more.push((h, i)),
        }
    }
    for batch in held {
        let theirs = picked(batch, columns)?;
        let same = mine.iter().zip(&theirs).map(|(m, t)| make_comparator(m.as_ref(), t.as_ref(), Default::default())).collect::<std::result::Result<Vec<DynComparator>, _>>()?;
        let equal = |i: usize, j: usize| same.iter().all(|c| c(i, j).is_eq());
        for (j, h) in hashed(&theirs, batch.num_rows())?.into_iter().enumerate() {
            let Some(&i) = first.get(&h) else { continue };
            if let Some(i) = std::iter::once(i).chain(more.iter().filter(|(m, _)| *m == h).map(|(_, i)| *i)).find(|&i| equal(i, j)) {
                return Ok(Some(crate::rules::keys::shown(&mine, i)));
            }
        }
    }
    Ok(None)
}

/// `held_again` for a few new rows: each row's values compared with each held batch's columns by Arrow's kernels. Errs on
/// a type the kernels don't compare (a list, a struct).
fn held_by_value(mine: &[ArrayRef], held: &[RecordBatch], columns: &[String]) -> anyhow::Result<Option<String>> {
    use datafusion::arrow::array::{BooleanArray, Scalar};
    use datafusion::arrow::compute::{and, kernels::cmp::eq};
    for i in 0..mine.first().map_or(0, |c| c.len()) {
        if mine.iter().any(|c| c.is_null(i)) {
            continue;
        }
        let values: Vec<Scalar<ArrayRef>> = mine.iter().map(|c| Scalar::new(c.slice(i, 1))).collect();
        for batch in held {
            let mut found: Option<BooleanArray> = None;
            for (theirs, value) in picked(batch, columns)?.iter().zip(&values) {
                let same = eq(theirs, value)?;
                found = Some(match found {
                    Some(f) => and(&f, &same)?,
                    None => same,
                });
            }
            if found.is_some_and(|f| f.true_count() > 0) {
                return Ok(Some(crate::rules::keys::shown(mine, i)));
            }
        }
    }
    Ok(None)
}

/// The columns `names` of `batch`, in that order.
fn picked(batch: &RecordBatch, names: &[String]) -> anyhow::Result<Vec<ArrayRef>> {
    names.iter().map(|c| batch.column_by_name(c).cloned().ok_or_else(|| anyhow::anyhow!("no column {c}"))).collect()
}

/// The columns `names` of a table's `batches` (under its `schema`), concatenated: a key's or a UNIQUE's values over the whole
/// table, without the other columns, which `rules::keys::twice` doesn't read.
fn projected(schema: &SchemaRef, batches: &[RecordBatch], names: &[String]) -> anyhow::Result<RecordBatch> {
    let indices = names.iter().map(|n| schema.index_of(n)).collect::<std::result::Result<Vec<_>, _>>()?;
    let projected = Arc::new(schema.project(&indices)?);
    let parts = batches.iter().map(|b| b.project(&indices)).collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(datafusion::arrow::compute::concat_batches(&projected, &parts)?)
}

/// A CREATE TABLE with columns, as the planner is given it (`planned_expanded`): its table constraints left out, and each
/// column's options kept only where DataFusion keeps them (`kept_option`), with NOT NULL added to each column of an enforced
/// PRIMARY KEY that lacks it, as Postgres makes them. An identity column is planned as the plain column it is: its integer type
/// (a SERIAL's, `planned_type`), NOT NULL, and a DEFAULT that counts in its sequence (`identified`), which the planner keeps as
/// any DEFAULT. Its rules are the core's (`Ruled`), made over the columns it plans.
fn plain_create(c: &CreateTable, identified: &[counters::Identified]) -> anyhow::Result<CreateTable> {
    let mut plain = c.clone();
    plain.constraints.clear();
    let keyed: Vec<String> = c
        .constraints
        .iter()
        .flat_map(|k| match k {
            TableConstraint::PrimaryKey(p) if !crate::rules::create::informational(&p.characteristics) => p
                .columns
                .iter()
                .map(|i| match &i.column.expr {
                    datafusion::sql::sqlparser::ast::Expr::Identifier(name) => crate::text::ident(name),
                    other => other.to_string(),
                })
                .collect(),
            _ => vec![],
        })
        .collect();
    for column in &mut plain.columns {
        let inline = column.options.iter().any(|o| matches!(&o.option, ColumnOption::PrimaryKey(p) if !crate::rules::create::informational(&p.characteristics)));
        column.options.retain(|o| kept_option(&o.option));
        let name = crate::text::ident(&column.name);
        let identity = identified.iter().find(|i| i.column == name);
        if let Some(identity) = identity {
            column.data_type = planned_type(&column.data_type)?;
            let default = parsed_expr(&crate::rules::create::identity_default(&identity.sequence))?;
            column.options.push(ColumnOptionDef { name: None, option: ColumnOption::Default(default) });
        }
        if (inline || keyed.contains(&name) || identity.is_some()) && !column.options.iter().any(|o| matches!(o.option, ColumnOption::NotNull)) {
            column.options.push(ColumnOptionDef { name: None, option: ColumnOption::NotNull });
        }
    }
    Ok(plain)
}

/// An SQL expression as the page's parser reads it (an identity column's DEFAULT, `nextval('…')`).
fn parsed_expr(text: &str) -> anyhow::Result<datafusion::sql::sqlparser::ast::Expr> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    Ok(Parser::new(&GenericDialect {}).try_with_sql(text)?.parse_expr()?)
}

/// The type a column is planned with: a SERIAL's integer type (`serial_type` names it), and any other type as it is written.
fn planned_type(t: &datafusion::sql::sqlparser::ast::DataType) -> anyhow::Result<datafusion::sql::sqlparser::ast::DataType> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    match crate::rules::create::serial_type(t) {
        Some(name) => Ok(Parser::new(&GenericDialect {}).try_with_sql(name)?.parse_data_type()?),
        None => Ok(t.clone()),
    }
}

/// What a planned write does to its table, for its rules (`kept_rules`).
fn change_of(plan: &LogicalPlan) -> Change {
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(c)) => Change::Create { if_not_exists: c.if_not_exists },
        LogicalPlan::Ddl(DdlStatement::DropTable(_)) => Change::Drop,
        LogicalPlan::Dml(d) => match &d.op {
            WriteOp::Insert(op) => Change::Insert { appends: matches!(op, InsertOp::Append) },
            WriteOp::Update => Change::Update,
            _ => Change::Other,
        },
        LogicalPlan::Analyze(a) => change_of(&a.input),
        _ => Change::Other,
    }
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

/// The table rules a page can't keep, refused by name before planning (ADR-072: a form that can't do something says so), and
/// the upserts a page doesn't take. A page's tables keep NOT NULL, DEFAULT, PRIMARY KEY, UNIQUE and CHECK (`Ruled`); what they
/// don't keep is refused here, and what the core refuses is refused by the core when the rules are made (`made_table`).
fn rules(sql: &str) -> anyhow::Result<()> {
    use datafusion::sql::parser::DFParser;
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    // (the same cheap gate `asof.rs` uses, for a CREATE: a statement with no CREATE in it makes no table to judge; and the words
    // an upsert has, so a statement with none of them is not parsed twice)
    let lower = sql.to_ascii_lowercase();
    if !["create", "conflict", "replace", "ignore", "duplicate"].iter().any(|word| lower.contains(word)) {
        return Ok(());
    }
    // (a parse error is the planner's to answer, as it always has)
    let Ok(statements) = DFParser::parse_sql_with_dialect(sql, &GenericDialect {}) else { return Ok(()) };
    statements.iter().try_for_each(statement_rules)
}

/// One statement's rules: a CREATE TABLE's (`table_rules`), an INSERT's upsert (`upsert_rules`), and an EXPLAIN's (an EXPLAIN of a
/// CREATE TABLE is refused for each constraint and option it has, `explained_rules`).
fn statement_rules(statement: &datafusion::sql::parser::Statement) -> anyhow::Result<()> {
    use datafusion::sql::parser::Statement as DfStatement;
    match statement {
        DfStatement::Statement(s) => match s.as_ref() {
            SqlStatement::CreateTable(c) => table_rules(c),
            SqlStatement::Insert(i) => upsert_rules(i),
            _ => Ok(()),
        },
        DfStatement::Explain(e) => match e.statement.as_ref() {
            DfStatement::Statement(s) => match s.as_ref() {
                SqlStatement::CreateTable(c) => explained_rules(c),
                _ => statement_rules(&e.statement),
            },
            other => statement_rules(other),
        },
        _ => Ok(()),
    }
}

/// A CREATE TABLE's rules. A CREATE TABLE … AS keeps NOT NULL, DEFAULT and COMMENT only, so its constraints and other options
/// are refused, each named as written. A CREATE TABLE with columns keeps PRIMARY KEY, UNIQUE, CHECK and REFERENCES (which
/// `made_table` refuses, or keeps as a fact, as a node does), NOT NULL, DEFAULT, COMMENT and an identity column (its counter,
/// `identified`), and a computed column (refused by `made_table`); any other option or table constraint is refused by name. A
/// column that is an identity has no DEFAULT of its own (`identity_of` refuses both, as Postgres does). Nothing is planned for a refused table, so it
/// is not made.
fn table_rules(c: &CreateTable) -> anyhow::Result<()> {
    if c.query.is_some() {
        let refused = c.constraints.first().map(|k| k.to_string()).or_else(|| c.columns.iter().flat_map(|column| &column.options).find(|o| !kept_option(&o.option)).map(|o| o.option.to_string()));
        return match refused {
            Some(what) => Err(not_kept_as(&what)),
            None => Ok(()),
        };
    }
    if let Some(k) = c.constraints.iter().find(|k| !kept_constraint(k)) {
        return Err(not_kept(&k.to_string()));
    }
    match c.columns.iter().flat_map(|column| &column.options).find(|o| !kept_column_option(&o.option) && !identity_option(&o.option)) {
        Some(o) => Err(not_kept(&o.option.to_string())),
        None => Ok(()),
    }
}

/// An EXPLAIN of a CREATE TABLE is refused for each constraint and option a page doesn't keep, as the CREATE is: one answer for
/// the EXPLAIN, which DataFusion would give two different ones for.
fn explained_rules(c: &CreateTable) -> anyhow::Result<()> {
    let refused = c.constraints.first().map(|k| k.to_string()).or_else(|| c.columns.iter().flat_map(|column| &column.options).find(|o| !kept_option(&o.option)).map(|o| o.option.to_string()));
    match refused {
        Some(what) => Err(crate::codes::coded("0A000", format!("a page doesn't keep {what} yet on an EXPLAIN of a CREATE TABLE: EXPLAIN the CREATE TABLE without it, or EXPLAIN it on a node"))),
        None => Ok(()),
    }
}

/// An INSERT that is an upsert, refused by name: a page's INSERT only adds rows (SQLSTATE 0A000). Its words name the form the
/// statement gave: ON CONFLICT, ON DUPLICATE KEY UPDATE, OR REPLACE and the other OR words, IGNORE, or REPLACE INTO.
fn upsert_rules(i: &Insert) -> anyhow::Result<()> {
    let form = if i.replace_into {
        "REPLACE INTO".to_string()
    } else if let Some(or) = &i.or {
        format!("INSERT OR {}", match or {
            SqliteOnConflict::Rollback => "ROLLBACK",
            SqliteOnConflict::Abort => "ABORT",
            SqliteOnConflict::Fail => "FAIL",
            SqliteOnConflict::Ignore => "IGNORE",
            SqliteOnConflict::Replace => "REPLACE",
        })
    } else if i.ignore {
        "INSERT IGNORE".to_string()
    } else {
        match &i.on {
            Some(OnInsert::OnConflict(_)) => "INSERT … ON CONFLICT".to_string(),
            Some(OnInsert::DuplicateKeyUpdate(_)) => "INSERT … ON DUPLICATE KEY UPDATE".to_string(),
            Some(_) => "INSERT … an upsert clause".to_string(),
            None => return Ok(()),
        }
    };
    Err(crate::codes::coded("0A000", format!("a page doesn't take {form} yet: its INSERT only adds rows; write an UPDATE, or upsert on a node")))
}

/// The column options a page keeps: NULL and NOT NULL (enforced), DEFAULT (filled in), and COMMENT (which changes no row,
/// so it is let by).
fn kept_option(option: &ColumnOption) -> bool {
    matches!(option, ColumnOption::Null | ColumnOption::NotNull | ColumnOption::Default(_) | ColumnOption::Comment(_))
}

/// The column options of a CREATE TABLE with columns that a page keeps or leaves to `made_table`: `kept_option`, a key, UNIQUE,
/// CHECK, REFERENCES, and a computed column (its refusal is `made_table`'s). An identity's options are `identity_option`'s.
fn kept_column_option(option: &ColumnOption) -> bool {
    kept_option(option) || matches!(option, ColumnOption::PrimaryKey(_) | ColumnOption::Unique(_) | ColumnOption::Check(_) | ColumnOption::ForeignKey(_) | ColumnOption::Generated { generation_expr: Some(_), .. })
}

/// The options an identity column says, which a page keeps as a counter does (`counters::identified`): GENERATED … AS IDENTITY,
/// the IDENTITY property and AUTO_INCREMENT, as `rules::create::identity_of` reads them.
fn identity_option(option: &ColumnOption) -> bool {
    match option {
        ColumnOption::Generated { generation_expr, .. } => generation_expr.is_none(),
        ColumnOption::Identity(_) => true,
        ColumnOption::DialectSpecific(t) => matches!(&t[..], [w] if ["AUTO_INCREMENT", "AUTOINCREMENT"].contains(&w.to_string().to_uppercase().as_str())),
        _ => false,
    }
}

/// The columns an UPDATE's assignments give a value other than DEFAULT, as SQL names them (a tuple's values pair with its names, as
/// written): an ALWAYS identity may be set to DEFAULT only (`keys::set`).
fn set_columns(assignments: &[datafusion::sql::sqlparser::ast::Assignment]) -> Vec<String> {
    use datafusion::sql::sqlparser::ast::{AssignmentTarget, Expr as SqlExpr};
    let mut set = vec![];
    for assignment in assignments {
        let names: Vec<&ObjectName> = match &assignment.target {
            AssignmentTarget::ColumnName(n) => vec![n],
            AssignmentTarget::Tuple(ns) => ns.iter().collect(),
        };
        let values: Vec<String> = match &assignment.value {
            SqlExpr::Tuple(vs) if vs.len() == names.len() => vs.iter().map(|v| v.to_string()).collect(),
            value => vec![value.to_string(); names.len()],
        };
        for (name, value) in names.into_iter().zip(values) {
            if let Some(ObjectNamePart::Identifier(ident)) = name.0.last() {
                if !crate::rules::rows::is_word(&value) {
                    set.push(crate::text::ident(ident));
                }
            }
        }
    }
    set
}

/// The table constraints a CREATE TABLE with columns keeps or leaves to `made_table`: a PRIMARY KEY, UNIQUE, CHECK and FOREIGN KEY.
fn kept_constraint(constraint: &TableConstraint) -> bool {
    matches!(constraint, TableConstraint::PrimaryKey(_) | TableConstraint::Unique(_) | TableConstraint::Check(_) | TableConstraint::ForeignKey(_))
}

/// The refusal for a table rule a page can't keep: 0A000, with the words that name it and say what its tables keep.
fn not_kept(what: &str) -> anyhow::Error {
    crate::codes::coded("0A000", format!("a page doesn't keep {what} yet: its tables keep NOT NULL, DEFAULT, identity, PRIMARY KEY, UNIQUE and CHECK, and no other rule; make the table without it, or keep it on a node"))
}

/// The refusal for a table rule a CREATE TABLE … AS can't keep (0A000): the table is made with its columns and the rule, and
/// the rows are written into it after.
fn not_kept_as(what: &str) -> anyhow::Error {
    crate::codes::coded("0A000", format!("a page doesn't keep {what} yet on a CREATE TABLE … AS: make the table with its columns and {what}, then INSERT … SELECT into it"))
}

/// The statements a page doesn't take yet, refused by name before anything is parsed (ADR-072: a form that can't do a thing
/// says so, and says what to write instead). DECLARE, MERGE, the type statements and a node's own
/// statements (users, tasks, shares, …) each name what to write in its place; DataFusion's own words for them name nothing
/// of the kind. Words are whole and read past comments, so a table named `merged` or a string `'begin'` is not one of them.
fn not_taken(sql: &str) -> anyhow::Result<()> {
    let words = crate::text::first_words(sql, 4);
    if begins(&words, &["DECLARE"]) {
        return Err(crate::codes::coded("0A000", "a page has no variables yet (DECLARE): write the value into the statement, or declare it on a node"));
    }
    if begins(&words, &["MERGE"]) {
        return Err(crate::codes::coded("0A000", "a page doesn't take MERGE yet: write its INSERT, UPDATE and DELETE into the page's table, or MERGE on a node"));
    }
    if let Some(pattern) = TYPE_STATEMENTS.into_iter().find(|pattern| begins(&words, pattern)) {
        return Err(crate::codes::coded("0A000", format!("a page makes no types yet ({}): make the column text, or keep the type on a node", pattern.join(" "))));
    }
    match NODE_STATEMENTS.into_iter().find(|(pattern, _)| begins(&words, pattern)) {
        Some((pattern, what)) => Err(crate::codes::coded("0A000", what.replace("{}", &pattern.join(" ")))),
        None => Ok(()),
    }
}

/// The words of the statements that make or drop a type, as `not_taken` reads them.
const TYPE_STATEMENTS: [&[&str]; 4] = [&["CREATE", "TYPE"], &["CREATE", "OR", "REPLACE", "TYPE"], &["DROP", "TYPE"], &["ALTER", "TYPE"]];

const NO_USERS: &str = "a page has no users or grants ({}): a page is one person's, so sign in to a node for users and grants";
const NO_TASKS: &str = "a page runs no tasks ({}): tasks run on a node";
const NO_SHARES: &str = "a page shares nothing ({}): share a table from a node";
const NO_BACKUPS: &str = "a page has no backups ({}): a kept page's tables are in its folder; back up a node's database";
const NO_LAKES: &str = "a page attaches no lakes ({}): read a lake's published tables with read_delta('https://…'), or attach the lake on a node";
const NO_SECRETS: &str = "a page keeps no secrets ({}): whoever runs the page can read what it holds, so keep secrets on a node";
const NO_SEQUENCES: &str = "a page has no sequences yet ({}): number rows with row_number() OVER (…), or keep the sequence on a node";
const NO_FUNCTIONS: &str = "a page has no functions of its own yet ({}): write the expression into the query, or make the function on a node";
const NO_PROCEDURES: &str = "a page has no procedures ({}): run the statements one at a time, or call the procedure on a node";
const NO_COPY: &str = "a page has no COPY: put a file with PUT /files/{name} and read it by name, and ask /sql?format=csv (or parquet) for a file of an answer";
const USE_SCHEMA: &str = "a page has one schema, public ({}): name the table itself, as page.public.t";
const NO_SCHEMAS: &str = "a page has one schema, public ({}): make the tables there, or make databases and schemas on a node";
const NO_COMMENTS: &str = "a page keeps no comments ({}): keep them on a node";
const NO_MATERIALIZED_VIEWS: &str = "a page keeps no materialized views ({}): make a table of the query (CREATE TABLE … AS), or keep the view current on a node";

/// A node's statements, which a page doesn't have: each pattern's words, and what the refusal says. `{}` is the statement's
/// words as the pattern spells them (`CREATE USER`).
const NODE_STATEMENTS: [(&[&str], &str); 44] = [
    (&["CREATE", "USER"], NO_USERS),
    (&["ALTER", "USER"], NO_USERS),
    (&["DROP", "USER"], NO_USERS),
    (&["CREATE", "ROLE"], NO_USERS),
    (&["DROP", "ROLE"], NO_USERS),
    (&["GRANT"], NO_USERS),
    (&["REVOKE"], NO_USERS),
    (&["CREATE", "TASK"], NO_TASKS),
    (&["CREATE", "OR", "REPLACE", "TASK"], NO_TASKS),
    (&["ALTER", "TASK"], NO_TASKS),
    (&["DROP", "TASK"], NO_TASKS),
    (&["EXECUTE", "TASK"], NO_TASKS),
    (&["CREATE", "SHARE"], NO_SHARES),
    (&["ALTER", "SHARE"], NO_SHARES),
    (&["DROP", "SHARE"], NO_SHARES),
    (&["CREATE", "RECIPIENT"], NO_SHARES),
    (&["DROP", "RECIPIENT"], NO_SHARES),
    (&["BACKUP"], NO_BACKUPS),
    (&["RESTORE"], NO_BACKUPS),
    (&["ATTACH"], NO_LAKES),
    (&["DETACH"], NO_LAKES),
    (&["CREATE", "SECRET"], NO_SECRETS),
    (&["CREATE", "OR", "REPLACE", "SECRET"], NO_SECRETS),
    (&["CREATE", "TEMPORARY", "SECRET"], NO_SECRETS),
    (&["DROP", "SECRET"], NO_SECRETS),
    (&["CREATE", "SEQUENCE"], NO_SEQUENCES),
    (&["DROP", "SEQUENCE"], NO_SEQUENCES),
    (&["CREATE", "FUNCTION"], NO_FUNCTIONS),
    (&["CREATE", "OR", "REPLACE", "FUNCTION"], NO_FUNCTIONS),
    (&["DROP", "FUNCTION"], NO_FUNCTIONS),
    (&["CREATE", "PROCEDURE"], NO_PROCEDURES),
    (&["CREATE", "OR", "REPLACE", "PROCEDURE"], NO_PROCEDURES),
    (&["DROP", "PROCEDURE"], NO_PROCEDURES),
    (&["CALL"], NO_PROCEDURES),
    (&["COPY"], NO_COPY),
    (&["USE"], USE_SCHEMA),
    (&["CREATE", "DATABASE"], NO_SCHEMAS),
    (&["DROP", "DATABASE"], NO_SCHEMAS),
    (&["CREATE", "SCHEMA"], NO_SCHEMAS),
    (&["DROP", "SCHEMA"], NO_SCHEMAS),
    (&["COMMENT", "ON"], NO_COMMENTS),
    (&["CREATE", "MATERIALIZED", "VIEW"], NO_MATERIALIZED_VIEWS),
    (&["CREATE", "OR", "REPLACE", "MATERIALIZED"], NO_MATERIALIZED_VIEWS),
    (&["DROP", "MATERIALIZED", "VIEW"], NO_MATERIALIZED_VIEWS),
];

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
    /// the words that name its clause, and makes no table, so SELECT finds none. A CREATE TABLE … AS keeps NOT NULL, DEFAULT
    /// and COMMENT only, so an identity is refused there too. A REFERENCES and a computed column are refused by the core's rules
    /// when the table is made (`made_table`), in the core's words. The upserts are refused in their own words.
    #[tokio::test]
    async fn a_page_refuses_the_rules_it_cannot_keep() {
        let cases = [
            ("CREATE TABLE t (id INT PRIMARY KEY) AS SELECT 1 AS id", "a page doesn't keep PRIMARY KEY yet on a CREATE TABLE … AS: make the table with its columns and PRIMARY KEY, then INSERT … SELECT into it"),
            ("CREATE TABLE t (id INT UNIQUE) AS SELECT 1 AS id", "a page doesn't keep UNIQUE yet on a CREATE TABLE … AS"),
            ("CREATE TABLE t (id INT CHECK (id > 0)) AS SELECT 1 AS id", "a page doesn't keep CHECK (id > 0) yet on a CREATE TABLE … AS"),
            ("CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY) AS SELECT 1 AS id", "a page doesn't keep GENERATED ALWAYS AS IDENTITY yet on a CREATE TABLE … AS"),
            ("CREATE TABLE t (id INT, u TEXT COLLATE \"C\")", "a page doesn't keep COLLATE"),
            ("EXPLAIN CREATE TABLE t (id INT PRIMARY KEY)", "a page doesn't keep PRIMARY KEY yet on an EXPLAIN of a CREATE TABLE"),
            ("EXPLAIN CREATE TABLE t (id INT CHECK (id > 0))", "a page doesn't keep CHECK (id > 0) yet on an EXPLAIN of a CREATE TABLE"),
            ("CREATE TABLE t (id INT, d INT GENERATED ALWAYS AS (id * 2) STORED)", "d … GENERATED ALWAYS AS (id * 2): computed columns aren't taken yet (a view can compute it)"),
            ("CREATE TABLE t (id INT REFERENCES u(id))", "id REFERENCES u: a reference isn't checked yet"),
            ("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING", "a page doesn't take INSERT … ON CONFLICT yet: its INSERT only adds rows; write an UPDATE, or upsert on a node"),
            ("INSERT OR REPLACE INTO t VALUES (1)", "a page doesn't take INSERT OR REPLACE yet"),
            ("INSERT OR IGNORE INTO t VALUES (1)", "a page doesn't take INSERT OR IGNORE yet"),
            ("REPLACE INTO t VALUES (1)", "a page doesn't take REPLACE INTO yet"),
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

    /// A page keeps PRIMARY KEY, UNIQUE and CHECK as a node does (`Ruled`, `holds`): each row that breaks one is refused with
    /// the node's code and words, and the table is as it was (its count and its keys' sum, then the rows themselves). A key
    /// given twice in one INSERT is named before a key already held (the node's order), and an UPDATE that sets a key column
    /// is refused with the node's words, as an UPDATE of a UNIQUE column is checked over the whole table.
    #[tokio::test]
    async fn a_page_keeps_keys_unique_and_check_as_a_node_does() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE k (id INT PRIMARY KEY, v TEXT, n INT CHECK (n >= 0), e TEXT UNIQUE)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO k VALUES (1, 'a', 1, 'x'), (2, 'b', 2, NULL), (3, 'c', 3, NULL)".into()).await.status, 200);
        let rows = "SELECT count(*) AS n, sum(id) AS s FROM k";
        assert_eq!(page.request("POST", "/sql?format=csv", &[], rows.into()).await.body, "n,s\n3,6\n");
        let refused = [
            ("INSERT INTO k VALUES (1, 'z', 1, 'z')", "23505", "duplicate key value violates unique constraint \"k_pkey\": Key (id)=(1) already exists"),
            ("INSERT INTO k VALUES (4, 'd', 4, 'd'), (4, 'e', 4, 'f')", "23505", "duplicate key value violates unique constraint \"k_pkey\": Key (id)=(4) already exists"),
            ("INSERT INTO k VALUES (1, 'q', 9, 'q'), (6, 'r', 9, 'r'), (6, 's', 9, 's')", "23505", "duplicate key value violates unique constraint \"k_pkey\": Key (id)=(6) already exists"),
            ("INSERT INTO k VALUES (7, 'f', 7, 'x')", "23505", "duplicate key value violates unique constraint \"k_e_key\": Key (e)=(x) already exists"),
            ("INSERT INTO k VALUES (8, 'g', -1, NULL)", "23514", "new row for relation \"k\" violates check constraint \"k_n_check\": CHECK (n >= 0)"),
            ("UPDATE k SET id = 9 WHERE id = 1", "0A000", "UPDATE of k's key column id isn't taken here: delete the row, then insert it with its new key"),
            ("UPDATE k SET e = 'x' WHERE id = 2", "23505", "duplicate key value violates unique constraint \"k_e_key\": Key (e)=(x) already exists"),
            ("UPDATE k SET n = -1", "23514", "new row for relation \"k\" violates check constraint \"k_n_check\": CHECK (n >= 0)"),
        ];
        for (sql, code, words) in refused {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            let body = std::str::from_utf8(&got.body).unwrap();
            assert_eq!(got.header("x-pondra-sqlstate"), Some(code), "{sql}: {body}");
            assert_eq!(body, words, "{sql}");
            assert_eq!(page.request("POST", "/sql?format=csv", &[], rows.into()).await.body, "n,s\n3,6\n", "{sql}");
        }
        // (a NULL key is refused by the planned schema, which doesn't take a NULL in a NOT NULL column: 23502 only)
        let null_key = page.request("POST", "/sql", &[], "INSERT INTO k (v) VALUES ('nokey')".into()).await;
        assert_eq!(null_key.header("x-pondra-sqlstate"), Some("23502"), "{:?}", null_key.body);
        // (two NULLs in a UNIQUE column are taken, as they are above)
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO k VALUES (5, 'e', 5, NULL), (6, 'f', 6, NULL)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT id, e FROM k WHERE e IS NULL ORDER BY id".into()).await.body, "id,e\n2,\n3,\n5,\n6,\n");
        // (the upserts a page doesn't take, refused in their own words, and nothing written)
        let upsert = page.request("POST", "/sql", &[], "INSERT INTO k VALUES (9, 'h', 9, 'h') ON CONFLICT (id) DO NOTHING".into()).await;
        assert_eq!(upsert.header("x-pondra-sqlstate"), Some("0A000"));
        assert!(String::from_utf8_lossy(&upsert.body).contains("a page doesn't take INSERT … ON CONFLICT yet"));
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM k".into()).await.body, "n\n5\n");
    }

    /// A key column written unquoted in capitals is the lower-case column SQL means, inline or in `PRIMARY KEY (…)` in other
    /// capitals, as on a node (`text::ident`): a key given twice is refused 23505, and a NULL in one 23502.
    #[tokio::test]
    async fn a_page_keeps_a_mixed_case_key() {
        let page = Page::open(&Options::default());
        for sql in ["CREATE TABLE m (Id INT PRIMARY KEY)", "INSERT INTO m VALUES (1)", "CREATE TABLE n (Ka INT, kb INT, PRIMARY KEY (KA, Kb))", "INSERT INTO n VALUES (1, 1)"] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!(got.status, 200, "{sql}: {}", String::from_utf8_lossy(&got.body));
        }
        for (sql, code) in [("INSERT INTO m VALUES (1)", "23505"), ("INSERT INTO n VALUES (1, 1)", "23505"), ("INSERT INTO n VALUES (NULL, 2)", "23502")] {
            let again = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!(again.header("x-pondra-sqlstate"), Some(code), "{sql}: {:?}", again.body);
        }
    }

    /// A CHECK is validated when its table is made, as a node validates it (`defaults::definition`): one that names no column, or
    /// isn't a condition, is refused at CREATE, and no table is made.
    #[tokio::test]
    async fn a_page_refuses_a_check_it_cannot_plan_at_create() {
        let page = Page::open(&Options::default());
        let cases = [
            ("CREATE TABLE c1 (n INT CHECK (nope > 0))", "c1", "42703", "CONSTRAINT c1_n_check CHECK (nope > 0): "),
            ("CREATE TABLE c2 (n INT, CHECK (n + 1))", "c2", "42P16", "CONSTRAINT c2_check CHECK (n + 1): CHECK (n + 1) is not a condition: it gives Int64, not true or false"),
        ];
        for (sql, table, code, words) in cases {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            let body = std::str::from_utf8(&got.body).unwrap();
            assert_eq!(got.header("x-pondra-sqlstate"), Some(code), "{sql}: {body}");
            assert!(body.starts_with(words), "{sql}: {body}");
            let read = page.request("POST", "/sql", &[], format!("SELECT * FROM {table}").into()).await;
            assert_eq!(read.header("x-pondra-sqlstate"), Some("42P01"), "{sql}");
        }
    }

    /// A page's rules change as its table does (`kept_rules`): CREATE TABLE IF NOT EXISTS over a keyed table keeps its key; CREATE
    /// OR REPLACE takes it away (and its rows); a DROP takes the rules too, so a table made again without a key takes any rows.
    #[tokio::test]
    async fn a_page_keeps_its_rules_as_its_tables_change() {
        let page = Page::open(&Options::default());
        for sql in [
            "CREATE TABLE k (id INT PRIMARY KEY, v TEXT)",
            "INSERT INTO k VALUES (1, 'a')",
            "CREATE TABLE IF NOT EXISTS k (id INT, v TEXT)",
        ] {
            assert_eq!(page.request("POST", "/sql", &[], sql.into()).await.status, 200, "{sql}");
        }
        let twice = page.request("POST", "/sql", &[], "INSERT INTO k VALUES (1, 'b')".into()).await;
        assert_eq!(twice.header("x-pondra-sqlstate"), Some("23505"), "the key is kept: {:?}", twice.body);
        assert_eq!(page.request("POST", "/sql", &[], "CREATE OR REPLACE TABLE k (id INT)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO k VALUES (1), (1)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "DROP TABLE k".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE k (id INT PRIMARY KEY)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "DROP TABLE k".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE k (id INT)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO k VALUES (1), (1)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM k".into()).await.body, "n\n2\n");
    }

    /// VARCHAR(n) is text of at most n characters: a value of more is refused 22001, as a node refuses it, and one of n is taken.
    #[tokio::test]
    async fn a_varchar_is_refused_past_its_length() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE s (v VARCHAR(3))".into()).await.status, 200);
        let long = page.request("POST", "/sql", &[], "INSERT INTO s VALUES ('abcd')".into()).await;
        assert_eq!(long.header("x-pondra-sqlstate"), Some("22001"), "{:?}", long.body);
        assert_eq!(String::from_utf8_lossy(&long.body), "value too long for type character varying(3) (column v: a value of 4 characters)");
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO s VALUES ('abc')".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT v FROM s".into()).await.body, "v\nabc\n");
    }

    /// An INSERT's new rows are checked against the rows held before it (`held_again`): many by hashing, a few by value.
    /// Either way a key or UNIQUE value already held is named in the node's words, and the table is as it was.
    #[tokio::test]
    async fn a_page_checks_new_rows_against_the_held_ones() {
        let page = Page::open(&Options::default());
        for sql in ["CREATE TABLE h (id BIGINT PRIMARY KEY, e BIGINT UNIQUE)", "INSERT INTO h SELECT value, value + 1000 FROM generate_series(1, 100)"] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!(got.status, 200, "{sql}: {}", String::from_utf8_lossy(&got.body));
        }
        let cases = [
            ("INSERT INTO h SELECT value, value + 5000 FROM generate_series(100, 140)", "duplicate key value violates unique constraint \"h_pkey\": Key (id)=(100) already exists"),
            ("INSERT INTO h SELECT value, value + 900 FROM generate_series(200, 240)", "duplicate key value violates unique constraint \"h_e_key\": Key (e)=(1100) already exists"),
            ("INSERT INTO h VALUES (300, 1050)", "duplicate key value violates unique constraint \"h_e_key\": Key (e)=(1050) already exists"),
        ];
        for (sql, words) in cases {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((got.header("x-pondra-sqlstate"), String::from_utf8_lossy(&got.body).as_ref()), (Some("23505"), words), "{sql}");
        }
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM h".into()).await.body, "n\n100\n");
    }

    /// The types a page makes are the types DataFusion plans (as before the rules), with a key: each takes a row.
    #[tokio::test]
    async fn a_page_makes_the_types_it_plans_with_a_key() {
        let page = Page::open(&Options::default());
        let made = "CREATE TABLE ty (a INT[], c DECIMAL(10, 2), d TIMESTAMPTZ, e INTERVAL, id INT PRIMARY KEY)";
        let got = page.request("POST", "/sql", &[], made.into()).await;
        assert_eq!(got.status, 200, "{made}: {}", String::from_utf8_lossy(&got.body));
        let row = "INSERT INTO ty (id, a, c, d, e) VALUES (1, [1, 2], 2.50, '2026-01-01T00:00:00Z', INTERVAL '1 day')";
        assert_eq!(page.request("POST", "/sql", &[], row.into()).await.status, 200, "{row}");
        assert_eq!(page.request("POST", "/sql", &[], row.into()).await.header("x-pondra-sqlstate"), Some("23505"));
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT id, c FROM ty".into()).await.body, "id,c\n1,2.50\n");
    }

    /// A page names the statements it doesn't take yet, before anything is parsed (`not_taken`): each answers 0A000 with words
    /// that say what to write instead. The words that only look like them (a string, a table's name, a column) run.
    #[tokio::test]
    async fn a_page_names_the_statements_it_doesnt_take() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t AS SELECT 1 AS id".into()).await.status, 200);
        let refused = [
            "DECLARE $x = 1", "declare $mode = 'full'",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", "CREATE TYPE mood AS ENUM ('sad', 'ok')", "DROP TYPE mood",
            "CREATE USER ann WITH PASSWORD 'x'", "grant select on t to ann", "CREATE TASK k SCHEDULE '1 minute' AS SELECT 1",
            "CREATE SHARE s", "BACKUP DATABASE db TO 'x'", "ATTACH 'x' AS y", "CREATE SECRET s (TYPE s3)", "CREATE SEQUENCE q",
            "CREATE FUNCTION g(x INT) RETURNS INT AS $$ SELECT x $$", "CALL p()", "COPY t TO 'x.csv'", "USE crm", "CREATE SCHEMA s",
            "COMMENT ON TABLE t IS 'x'", "CREATE MATERIALIZED VIEW mv AS SELECT 1",
        ];
        for sql in refused {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            let body = std::str::from_utf8(&got.body).unwrap();
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (501, Some("0A000")), "{sql}: {body}");
            assert!(body.starts_with("a page"), "{sql}: {body}");
        }
        for (sql, words) in [
            ("DECLARE $x = 1", "a page has no variables yet (DECLARE): write the value into the statement, or declare it on a node"),
            ("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", "a page doesn't take MERGE yet: write its INSERT, UPDATE and DELETE into the page's table, or MERGE on a node"),
            ("CREATE TYPE mood AS ENUM ('sad', 'ok')", "a page makes no types yet (CREATE TYPE): make the column text, or keep the type on a node"),
            ("CREATE USER ann WITH PASSWORD 'x'", "a page has no users or grants (CREATE USER): a page is one person's, so sign in to a node for users and grants"),
            ("COPY t TO 'x.csv'", "a page has no COPY: put a file with PUT /files/{name} and read it by name, and ask /sql?format=csv (or parquet) for a file of an answer"),
        ] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!(std::str::from_utf8(&got.body).unwrap(), words, "{sql}");
        }
        for sql in [
            "SELECT 'begin' AS w",
            "CREATE TABLE merged AS SELECT 1 AS declared",
            "SELECT declared FROM merged",
            "CREATE TABLE types AS SELECT 1 AS id",
            "CREATE TABLE grants AS SELECT 1 AS copy",
            "SELECT copy FROM grants",
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

    /// A statement's answer as `POST /sql` gives it, with its SQLSTATE in the header (`ask`), and its CSV rows (`csv`).
    async fn ask(page: &Page, sql: &str) -> Answer {
        page.request("POST", "/sql", &[], sql.to_string().into()).await
    }

    /// A statement that must run (200): its words when it doesn't.
    async fn ran(page: &Page, sql: &str) {
        let got = ask(page, sql).await;
        assert_eq!(got.status, 200, "{sql}: {}", body(&got));
    }

    async fn csv(page: &Page, sql: &str) -> String {
        let got = page.request("POST", "/sql?format=csv", &[], sql.to_string().into()).await;
        assert_eq!(got.status, 200, "{sql}: {}", body(&got));
        body(&got)
    }

    /// A page counts its identity columns as a node does: each table counts its own. An INSERT that leaves the column out, or says
    /// DEFAULT, takes the next number, whichever form it takes (VALUES, or a SELECT of several rows). An ALWAYS column given a value
    /// is refused 428C9 in a named INSERT, a positional one and an UPDATE, and the table is as it was; a BY DEFAULT column takes a
    /// value given without counting; SERIAL counts; and a count runs out (2200H) as a sequence does, and a table made after a rename
    /// of one of the same name counts its own.
    #[tokio::test]
    async fn a_page_counts_its_identity_columns_as_a_node_does() {
        let page = Page::open(&Options::default());
        for text in [
            "CREATE TABLE i (id BIGINT GENERATED ALWAYS AS IDENTITY, v TEXT)",
            "CREATE TABLE b (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 5), v TEXT)",
            "CREATE TABLE s (id SERIAL, v TEXT)",
            "CREATE TABLE m (id INT GENERATED ALWAYS AS IDENTITY (MAXVALUE 2), v TEXT)",
        ] {
            ran(&page, text).await;
        }
        ran(&page, "INSERT INTO i (v) VALUES ('a'), ('b')").await;
        ran(&page, "INSERT INTO i (id, v) VALUES (DEFAULT, 'c')").await;
        ran(&page, "INSERT INTO i (v) SELECT v FROM (VALUES ('d'), ('e')) AS x(v)").await;
        assert_eq!(csv(&page, "SELECT id, v FROM i ORDER BY id").await, "id,v\n1,a\n2,b\n3,c\n4,d\n5,e\n");
        for sql in ["INSERT INTO i (id, v) VALUES (10, 'c')", "INSERT INTO i VALUES (5, 'x')", "UPDATE i SET id = 3", "INSERT INTO i (id, v) SELECT 9, 'y'"] {
            let got = ask(&page, sql).await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (400, Some("428C9")), "{sql}: {}", body(&got));
        }
        assert_eq!(csv(&page, "SELECT id, v FROM i ORDER BY id").await, "id,v\n1,a\n2,b\n3,c\n4,d\n5,e\n", "the refused writes left the table as it was");
        ran(&page, "INSERT INTO b (v) VALUES ('x'), ('y')").await;
        ran(&page, "INSERT INTO b (id, v) VALUES (7, 'z')").await;
        ran(&page, "INSERT INTO b (v) VALUES ('w')").await;
        assert_eq!(csv(&page, "SELECT id, v FROM b ORDER BY id").await, "id,v\n7,z\n10,x\n15,y\n20,w\n", "BY DEFAULT counts from its start, and a value given does not count");
        ran(&page, "INSERT INTO s (v) VALUES ('p'), ('q')").await;
        assert_eq!(csv(&page, "SELECT id, v FROM s ORDER BY id").await, "id,v\n1,p\n2,q\n", "SERIAL counts from 1");
        // MAXVALUE 2: the third number is past the sequence's end, and the write is undone, so the count is not used
        ran(&page, "INSERT INTO m (v) VALUES ('a'), ('b')").await;
        for _ in 0..2 {
            let got = ask(&page, "INSERT INTO m (v) VALUES ('c'), ('d')").await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (400, Some("2200H")), "{}", body(&got));
        }
        assert_eq!(csv(&page, "SELECT id, v FROM m ORDER BY id").await, "id,v\n1,a\n2,b\n");
        let unknown = ask(&page, "SELECT nextval('nope')").await;
        assert_eq!((unknown.status, unknown.header("x-pondra-sqlstate")), (404, Some("42P01")), "{}", body(&unknown));
        // a rename moves the counter with the table, and a table made after it takes the name the renamed one's sequence holds
        ran(&page, "ALTER TABLE i RENAME TO j").await;
        ran(&page, "INSERT INTO j (v) VALUES ('f')").await;
        ran(&page, "CREATE TABLE i (id BIGINT GENERATED ALWAYS AS IDENTITY, v TEXT)").await;
        ran(&page, "INSERT INTO i (v) VALUES ('g')").await;
        assert_eq!(csv(&page, "SELECT id, v FROM j ORDER BY id").await, "id,v\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n", "j went on from 5");
        assert_eq!(csv(&page, "SELECT id, v FROM i").await, "id,v\n1,g\n", "a new i counts from 1");
        // a DROP takes the count with its table: a table made again starts over
        ran(&page, "DROP TABLE j").await;
        ran(&page, "CREATE TABLE j (id BIGINT GENERATED ALWAYS AS IDENTITY, v TEXT)").await;
        ran(&page, "INSERT INTO j (v) VALUES ('h')").await;
        assert_eq!(csv(&page, "SELECT id, v FROM j").await, "id,v\n1,h\n");
    }

    /// A kept page keeps its identity counts: a table reopened over the same store counts on from where its last write left it,
    /// SERIAL and BY DEFAULT alike, and a rename's table keeps its count in its new file.
    #[tokio::test]
    async fn a_kept_page_keeps_its_identity_counts() {
        let store = Arc::new(InMemory::new());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        for text in [
            "CREATE TABLE i (id BIGINT GENERATED ALWAYS AS IDENTITY, v TEXT)",
            "CREATE TABLE s (id SERIAL, v TEXT)",
            "CREATE TABLE b (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 5), v TEXT)",
        ] {
            ran(&a, text).await;
        }
        ran(&a, "INSERT INTO i (v) VALUES ('a'), ('b')").await;
        ran(&a, "INSERT INTO s (v) VALUES ('p')").await;
        ran(&a, "INSERT INTO b (v) VALUES ('x')").await;
        ran(&a, "ALTER TABLE i RENAME TO k").await;
        ran(&a, "INSERT INTO k (v) VALUES ('c')").await;
        drop(a);
        let b = Page::kept(&options).await.unwrap();
        assert!(b.unread().is_empty(), "{:?}", b.unread());
        ran(&b, "INSERT INTO k (v) VALUES ('d')").await;
        ran(&b, "INSERT INTO s (v) VALUES ('q')").await;
        ran(&b, "INSERT INTO b (v) VALUES ('y')").await;
        assert_eq!(csv(&b, "SELECT id, v FROM k ORDER BY id").await, "id,v\n1,a\n2,b\n3,c\n4,d\n");
        assert_eq!(csv(&b, "SELECT id, v FROM s ORDER BY id").await, "id,v\n1,p\n2,q\n");
        assert_eq!(csv(&b, "SELECT id, v FROM b ORDER BY id").await, "id,v\n10,x\n15,y\n");
        let refused = ask(&b, "INSERT INTO k (id, v) VALUES (9, 'z')").await;
        assert_eq!(refused.header("x-pondra-sqlstate"), Some("428C9"), "{}", body(&refused));
    }

    /// The code a refused statement answers.
    async fn code(page: &Page, sql: &str) -> String {
        let got = ask(page, sql).await;
        assert_ne!(got.status, 200, "{sql}: {}", body(&got));
        got.header("x-pondra-sqlstate").unwrap_or_default().to_string()
    }

    /// A page's transactions, as a node's: COMMIT keeps the writes, ROLLBACK puts every table back (a DELETE's, an UPDATE's and
    /// an INSERT's), and a savepoint is gone back to with ROLLBACK TO and forgotten with RELEASE. A failed statement fails the
    /// transaction: the ones after it answer 25P02 and its COMMIT is a ROLLBACK. READ ONLY refuses a write (25006), SERIALIZABLE
    /// is refused by name, and a savepoint word outside a transaction is 25P01. DDL runs at once: ROLLBACK leaves it, with a
    /// notice that says so, and DDL on a table the transaction wrote is refused.
    #[tokio::test]
    async fn a_page_keeps_transactions_as_a_node_does() {
        let page = Page::open(&Options::default());
        ran(&page, "CREATE TABLE a (id INT PRIMARY KEY, v TEXT)").await;
        ran(&page, "INSERT INTO a VALUES (1, 'x'), (2, 'y')").await;
        let begun = ask(&page, "BEGIN").await;
        assert_eq!(body(&begun), r#"{"transaction":"begin"}"#);
        for sql in ["INSERT INTO a VALUES (3, 'z')", "UPDATE a SET v = 'w' WHERE id = 1", "DELETE FROM a WHERE id = 2"] {
            ran(&page, sql).await;
        }
        assert_eq!(csv(&page, "SELECT id, v FROM a ORDER BY id").await, "id,v\n1,w\n3,z\n");
        ran(&page, "ROLLBACK").await;
        assert_eq!(csv(&page, "SELECT id, v FROM a ORDER BY id").await, "id,v\n1,x\n2,y\n");

        ran(&page, "BEGIN").await;
        ran(&page, "INSERT INTO a VALUES (3, 'kept')").await;
        ran(&page, "SAVEPOINT s").await;
        ran(&page, "UPDATE a SET v = 'gone' WHERE id = 3").await;
        ran(&page, "INSERT INTO a VALUES (4, 'gone')").await;
        ran(&page, "CREATE TABLE b (n INT)").await;
        ran(&page, "INSERT INTO b VALUES (1)").await;
        ran(&page, "ROLLBACK TO SAVEPOINT s").await;
        ran(&page, "RELEASE SAVEPOINT s").await;
        assert_eq!(code(&page, "ROLLBACK TO SAVEPOINT s").await, "3B001");
        assert_eq!(code(&page, "SELECT 1").await, "25P02");
        let ended = ask(&page, "COMMIT").await;
        assert_eq!(body(&ended), r#"{"transaction":"rollback"}"#);
        assert_eq!(csv(&page, "SELECT id, v FROM a ORDER BY id").await, "id,v\n1,x\n2,y\n");
        assert_eq!(csv(&page, "SELECT count(*) AS n FROM b").await, "n\n0\n"); // (its CREATE ran at once; its INSERT was rolled back)

        ran(&page, "BEGIN").await;
        ran(&page, "INSERT INTO a VALUES (3, 'kept')").await;
        assert_eq!(code(&page, "INSERT INTO a VALUES (3, 'twice')").await, "23505");
        assert_eq!(code(&page, "INSERT INTO a VALUES (5, 'after')").await, "25P02");
        ran(&page, "ROLLBACK").await;
        ran(&page, "BEGIN").await;
        ran(&page, "INSERT INTO a VALUES (3, 'kept')").await;
        assert_eq!(code(&page, "ALTER TABLE a ADD COLUMN w INT").await, "0A000");
        let made = ask(&page, "CREATE TABLE c (n INT)").await;
        assert_eq!(made.header("x-pondra-notices"), Some(r#"["CREATE TABLE ran at once: DDL isn't part of a transaction here, and ROLLBACK won't undo it"]"#));
        ran(&page, "COMMIT").await;
        assert_eq!(csv(&page, "SELECT id, v FROM a ORDER BY id").await, "id,v\n1,x\n2,y\n3,kept\n");

        ran(&page, "BEGIN READ ONLY").await;
        assert_eq!(code(&page, "INSERT INTO a VALUES (6, 'no')").await, "25006");
        ran(&page, "ROLLBACK").await;
        assert_eq!(code(&page, "BEGIN ISOLATION LEVEL SERIALIZABLE").await, "0A000");
        assert_eq!(code(&page, "SAVEPOINT s").await, "25P01");
        let none = ask(&page, "COMMIT").await;
        assert_eq!(none.header("x-pondra-notices"), Some(r#"["WARNING: there is no transaction in progress"]"#));
        ran(&page, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
        assert_eq!(code(&page, "CREATE TABLE d (n INT)").await, "0A000");
        ran(&page, "ROLLBACK").await;
    }

    /// A kept page keeps a transaction's writes at its COMMIT, and none of a ROLLBACK's: a page kept over the same store reads
    /// the tables as the last COMMIT left them.
    #[tokio::test]
    async fn a_kept_page_keeps_what_a_transaction_commits() {
        let store = Arc::new(InMemory::new());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        ran(&a, "CREATE TABLE t (id INT, v TEXT)").await;
        for sql in ["BEGIN", "INSERT INTO t VALUES (1, 'kept')", "COMMIT", "BEGIN", "INSERT INTO t VALUES (2, 'gone')", "ROLLBACK", "BEGIN", "INSERT INTO t VALUES (3, 'open')"] {
            ran(&a, sql).await;
        }
        drop(a);
        let b = Page::kept(&options).await.unwrap();
        assert_eq!(csv(&b, "SELECT id, v FROM t ORDER BY id").await, "id,v\n1,kept\n");
    }

    /// A page fills a narrow row, and a DEFAULT in VALUES, as a node does: a row that names fewer columns, or says DEFAULT, takes
    /// the column's default; a row wider than the columns it names is refused with 42601, and nothing is written.
    #[tokio::test]
    async fn a_page_fills_a_narrow_row_and_a_default_as_a_node_does() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t (a INT, b INT DEFAULT 5)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO t VALUES (1)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO t (a, b) VALUES (2, DEFAULT)".into()).await.status, 200);
        // (a query's rows too: a narrow one takes the defaults, and a VALUES inside it is its own width)
        assert_eq!(page.request("POST", "/sql", &[], "INSERT INTO t SELECT x FROM (VALUES (3)) AS v(x)".into()).await.status, 200);
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT a, b FROM t ORDER BY a".into()).await.body, "a,b\n1,5\n2,5\n3,5\n");
        for sql in ["INSERT INTO t VALUES (1, 2, 3)", "INSERT INTO t (a) VALUES (1, 2)"] {
            let got = page.request("POST", "/sql", &[], sql.into()).await;
            assert_eq!((got.status, got.header("x-pondra-sqlstate")), (400, Some("42601")), "{sql}: {:?}", got.body);
        }
        assert_eq!(page.request("POST", "/sql?format=csv", &[], "SELECT a, b FROM t ORDER BY a".into()).await.body, "a,b\n1,5\n2,5\n3,5\n");
    }

    /// What a page refused before it kept rules stays refused, with the code it always gave.
    #[tokio::test]
    async fn what_a_page_refused_already_stays_refused() {
        let page = Page::open(&Options::default());
        assert_eq!(page.request("POST", "/sql", &[], "CREATE TABLE t (id INT, v INT)".into()).await.status, 200);
        for sql in [
            "INSERT INTO t VALUES (1, 1) ON CONFLICT DO NOTHING",
            "CREATE TEMP TABLE x (id INT)",
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

    /// The text of an answer's body.
    fn body(answer: &Answer) -> String {
        String::from_utf8_lossy(&answer.body).into_owned()
    }

    /// A page keeps views as a node does: a view reads its tables as they are now (after an INSERT, and after CREATE OR
    /// REPLACE TABLE), a view reads another, CREATE OR REPLACE changes it, and the refusals are a node's words. DROP TABLE of
    /// a table a view reads is refused unless CASCADE, which drops the views that read it, and the views that read those.
    #[tokio::test]
    async fn a_page_keeps_views_over_its_tables_as_they_are_now() {
        let page = Page::open(&Options::default());
        let sql = |text: &str| page.request("POST", "/sql?format=csv", &[], text.to_string().into());
        assert_eq!(sql("CREATE TABLE t (a INT, b VARCHAR)").await.status, 200);
        assert_eq!(sql("INSERT INTO t VALUES (1, 'x'), (2, 'y')").await.status, 200);
        let made = sql("CREATE VIEW v AS SELECT a FROM t WHERE a > 1").await;
        assert_eq!((made.status, body(&made).as_str()), (200, r#"{"view":"v"}"#));
        assert_eq!(body(&sql("SELECT count(*) AS n FROM v").await), "n\n1\n");
        assert_eq!(sql("INSERT INTO t VALUES (3, 'z')").await.status, 200);
        assert_eq!(body(&sql("SELECT count(*) AS n FROM v").await), "n\n2\n", "the view reads the rows the INSERT added");
        assert_eq!(sql("CREATE VIEW w (n) AS SELECT count(*) FROM v").await.status, 200);
        assert_eq!(body(&sql("SELECT n FROM w").await), "n\n2\n");
        assert_eq!(body(&sql("SELECT n FROM page.public.w").await), "n\n2\n");
        assert_eq!(sql("CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES (5, 'q'), (6, 'r'), (7, 's')) AS x(a, b)").await.status, 200);
        assert_eq!(body(&sql("SELECT n FROM w").await), "n\n3\n", "the view reads the table made again");
        assert_eq!(sql("CREATE OR REPLACE VIEW v AS SELECT a FROM t WHERE a > 6").await.status, 200);
        assert_eq!(body(&sql("SELECT n FROM w").await), "n\n1\n", "w reads v as it is now");
        for (text, words) in [
            ("CREATE VIEW v AS SELECT 1", "view v already exists (CREATE OR REPLACE VIEW)"),
            ("CREATE VIEW t AS SELECT 1", "t is a table"),
            ("CREATE TABLE v (a INT)", "v is a view"),
            ("INSERT INTO v VALUES (1)", "v is a view: write to the tables it reads"),
            ("DROP VIEW nov", "no view nov"),
            ("DROP TABLE v", "no table v"),
            ("DROP TABLE t", "t is read by view v: DROP TABLE t CASCADE drops them too, or drop them first"),
        ] {
            let got = sql(text).await;
            assert_ne!(got.status, 200, "{text}");
            assert_eq!(body(&got), words, "{text}");
        }
        assert!(body(&sql("CREATE VIEW bad AS SELECT * FROM nothere").await).starts_with("the view's query: "));
        assert_eq!(body(&sql("CREATE VIEW IF NOT EXISTS v AS SELECT 2").await), r#"{"name":"v","exists":true}"#);
        assert_eq!(body(&sql("DROP VIEW IF EXISTS nov").await), r#"{"view":"nov","dropped":false}"#);
        let listed = page.request("GET", "/objects", &[], bytes::Bytes::new()).await;
        assert_eq!(names_listed(&listed), ["t", "v", "w"]);
        assert_eq!(sql("DROP VIEW w").await.status, 200);
        assert_eq!(sql("CREATE VIEW w AS SELECT a FROM v").await.status, 200);
        assert_eq!(sql("DROP TABLE t CASCADE").await.status, 200);
        assert!(names_listed(&page.request("GET", "/objects", &[], bytes::Bytes::new()).await).is_empty(), "CASCADE dropped v, which read t, and w, which read v");
        assert_eq!(sql("SELECT a FROM w").await.status, 404);
    }

    /// A kept page keeps its views, and a page opened on the same store reads them again.
    #[tokio::test]
    async fn a_kept_page_keeps_its_views() {
        let store = Arc::new(InMemory::new());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        for text in ["CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3)) AS x(id)", "CREATE VIEW big AS SELECT id FROM t WHERE id > 1", "CREATE VIEW gone AS SELECT 1 AS one"] {
            assert_eq!(a.request("POST", "/sql", &[], text.into()).await.status, 200, "{text}");
        }
        assert_eq!(a.request("POST", "/sql", &[], "DROP VIEW gone".into()).await.status, 200);
        drop(a);
        let b = Page::kept(&options).await.unwrap();
        assert!(b.unread().is_empty(), "{:?}", b.unread());
        assert_eq!(body(&b.request("POST", "/sql?format=csv", &[], "SELECT id FROM big ORDER BY id".into()).await), "id\n2\n3\n");
        assert_eq!(names_listed(&b.request("GET", "/objects", &[], bytes::Bytes::new()).await), ["t", "big"]);
    }

    /// The page's catalog is `page`, as `GET /objects` names it: a missing table's words say `page.public.…` (not
    /// DataFusion's `datafusion`), and a three-part name in it reads the page's table.
    #[tokio::test]
    async fn a_pages_catalog_is_page() {
        let page = page_with_tables().await;
        let missing = page.request("POST", "/sql", &[], "SELECT * FROM nope".into()).await;
        assert_eq!(missing.status, 404);
        assert!(String::from_utf8_lossy(&missing.body).contains("'page.public.nope'"), "{}", String::from_utf8_lossy(&missing.body));
        let whole = page.request("POST", "/sql?format=csv", &[], "SELECT count(*) AS n FROM page.public.orders".into()).await;
        assert_eq!((whole.status, String::from_utf8_lossy(&whole.body).as_ref()), (200, "n\n3\n"));
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

    /// A kept page keeps its tables' rules (`CREATE_KEY`): a page opened over the same store refuses a key there already and a
    /// CHECK that breaks, and a refused INSERT leaves the kept file without any of its rows.
    #[tokio::test]
    async fn a_kept_page_keeps_its_keys() {
        let store = Arc::new(InMemory::new());
        let options = Options { store: Some(store.clone()), ..Default::default() };
        let a = Page::kept(&options).await.unwrap();
        assert_eq!(a.request("POST", "/sql", &[], "CREATE TABLE k (id INT PRIMARY KEY, v TEXT, e TEXT UNIQUE, n INT CHECK (n >= 0))".into()).await.status, 200);
        assert_eq!(a.request("POST", "/sql", &[], "INSERT INTO k VALUES (1, 'a', 'x', 1), (2, 'b', NULL, 2)".into()).await.status, 200);
        let b = Page::kept(&options).await.unwrap();
        let again = b.request("POST", "/sql", &[], "INSERT INTO k VALUES (1, 'again', NULL, 1)".into()).await;
        assert_eq!(again.header("x-pondra-sqlstate"), Some("23505"), "{:?}", again.body);
        assert_eq!(&again.body[..], &b"duplicate key value violates unique constraint \"k_pkey\": Key (id)=(1) already exists"[..]);
        // (a refused INSERT of three rows, one of them a key held: none of the three is kept)
        let refused = a.request("POST", "/sql", &[], "INSERT INTO k VALUES (3, 'c', NULL, 3), (4, 'd', 'x', 4)".into()).await;
        assert_eq!(refused.header("x-pondra-sqlstate"), Some("23505"), "{:?}", refused.body);
        let checked = b.request("POST", "/sql", &[], "INSERT INTO k VALUES (9, 'z', NULL, -1)".into()).await;
        assert_eq!(checked.header("x-pondra-sqlstate"), Some("23514"), "{:?}", checked.body);
        let c = Page::kept(&options).await.unwrap();
        assert_eq!(c.request("POST", "/sql?format=csv", &[], "SELECT id, v FROM k ORDER BY id".into()).await.body, "id,v\n1,a\n2,b\n");
        assert_eq!(c.request("POST", "/sql", &[], "INSERT INTO k VALUES (1, 'x', NULL, 1)".into()).await.header("x-pondra-sqlstate"), Some("23505"));
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

    /// The watch stops the query that grew the page, not one beside it: a query that grows nothing finishes while another
    /// grows the page past its line between its polls, and one whose own polls grow the page is stopped, 53200.
    #[tokio::test]
    async fn the_watch_stops_the_query_that_grew_the_page() {
        let held = Arc::new(AtomicUsize::new(0));
        let live = held.clone();
        let watch = Watch { live: Arc::new(move || live.load(Ordering::SeqCst)), line: 1000 };
        let other = held.clone();
        let beside = tokio::spawn(async move {
            for _ in 0..4 {
                other.fetch_add(400, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        });
        let quiet = watched(Some(&watch), async {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            anyhow::Ok(())
        });
        assert!(quiet.await.is_ok());
        beside.await.unwrap();
        assert_eq!(held.load(Ordering::SeqCst), 1600);
        let mine = held.clone();
        let greedy = watched(Some(&watch), async move {
            for _ in 0..100 {
                mine.fetch_add(200, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
            anyhow::Ok(())
        });
        assert_eq!(crate::codes::of(&greedy.await.unwrap_err()), "53200");
        assert!(held.load(Ordering::SeqCst) < 2000, "stopped at its first poll past its tenth");
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
    /// A page tells its node the room it has for the answer (`Sent::room`): its line less what it holds, at least a tenth of
    /// the line, and none when its memory isn't watched. What the page answers in place of an answer past it (`past_room`)
    /// is 53200, as a query the watch stops, naming the room and the setting.
    #[tokio::test]
    async fn a_page_asks_its_node_within_the_room_it_has() {
        let kept = Arc::new(std::sync::Mutex::new(vec![]));
        let held = Arc::new(AtomicUsize::new(300));
        let live = held.clone();
        let options = Options { node: Some(node_keeping(kept.clone(), count_csv)), ..Default::default() };
        let page = Page::with_watch(&options, Some(Watch { live: Arc::new(move || live.load(Ordering::SeqCst)), line: 1000 }));
        let sql = "SELECT count(*) AS n FROM lake_orders";
        for holds in [300, 950, 5000] {
            held.store(holds, Ordering::SeqCst);
            assert_eq!(page.request("POST", "/sql?format=csv", &[], sql.into()).await.body, "n\n3\n");
        }
        let unwatched = page_with_node(node_keeping(kept.clone(), count_csv)).await;
        assert_eq!(unwatched.request("POST", "/sql?format=csv", &[], sql.into()).await.body, "n\n3\n");
        let rooms: Vec<Option<usize>> = kept.lock().unwrap().iter().map(|sent| sent.room).collect();
        assert_eq!(rooms, [Some(700), Some(100), Some(100), None]);
        let past = past_room(3 << 20);
        assert_eq!((past.status, past.header("x-pondra-sqlstate")), (503, Some("53200")));
        let words = String::from_utf8_lossy(&past.body);
        assert!(words.contains("3 MB") && words.contains("memory_max_gb"), "{words}");
    }

    /// A page's memory line is its setting's, else its device's (`Options::line`), else 3 GiB; 0 turns the watch off.
    #[test]
    fn a_pages_line_is_its_settings_then_its_devices() {
        assert_eq!((line_of(None, None), line_of(None, Some(1 << 30))), (Some(LINE), Some(1 << 30)));
        assert_eq!((line_of(Some(2 << 30), Some(1 << 30)), line_of(Some(0), Some(1 << 30))), (Some(2 << 30), None));
    }

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
