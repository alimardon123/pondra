//! A page: a member on the core's lake (designs/page-p3.md). A claimed page (`claim`, one per module) answers its requests with
//! its member, which opens at the first request: `/sql` is the core's statement path on the page's lake (`lake_sql`), `/objects`
//! is its listing, and `/files/…` its files (`lake_files`). A folder kept by 0.35/0.36 is imported once on open (`import_kept`).
//! A statement the lake can't answer because a table it names is a node's goes to the page's node (`lake_outgoing`, B5).
//! `web/` wraps it for a browser.

#[cfg(target_arch = "wasm32")]
pub use crate::ext::{web_with, Web}; // (a page gives its store of a web origin: its browser's fetch)
use crate::request_body;
use anyhow::Context;
use futures::TryStreamExt;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::ipc::{reader::FileReader, writer::StreamWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::execution::SessionState;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{ColumnOption, CreateTable, Ident, Insert, ObjectName, Query, SetExpr, Statement as SqlStatement, TableObject};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
#[cfg(target_arch = "wasm32")]
use std::task::Poll; // (`polled`'s)

mod counters; // (a kept table's identity counter, as its note has it: `Counter`, which an import restarts a sequence at)

const TEXT: &str = "text/plain; charset=utf-8";
const JSON: &str = "application/json";
const FILES: &str = "/files/";
/// The folder a page's tables are kept in, beside `files/` in its store. SQL never reads it.
const TABLES: &str = "tables";
/// Where a kept page keeps its views, one file each, with its name and query (`import_view`).
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
/// The 0A000 words of the refusals a page makes of a statement a node may not answer (`lake_outgoing`): a write that isn't a
/// query (`query_of`), and a file named beside a node's table (`page_names`). A fill may not name its target `NODE_ROWS`
/// (`node_rows_taken`).
const WRITE_TO_NODE: &str = "a page sends its node queries, and the query of a CREATE TABLE … AS or an INSERT INTO one of its own tables (write INSERT INTO t WITH … SELECT, not WITH … INSERT): write into the node's tables through a client of the node";
const FILE_BESIDE_NODE: &str = "a query a page sends its node names the page's tables and the node's, not the page's files: make the file a table first (CREATE TABLE t AS FROM 'file')";
/// The name a kept table's rows have in the requests that import it (`import_table`): a request's table, as `imported_said` names it.
const KEPT_ROWS: &str = "the kept rows";
/// What a query the watch stopped answers (SQLSTATE 53200: `codes::of` reads "out of memory").
pub(crate) const PAGE_OOM: &str = "out of memory: the page was nearly out of memory and stopped this query, the one that grew it (memory_max_gb)";

/// How a page is made: the store its member keeps its lake in, how it asks its node, how it watches its memory, and the line
/// its device has.
pub struct Options {
    /// The store a page's member on the core's lake keeps its lake in (`open_lake`): a browser gives its private folder (OPFS), so
    /// the page's tables are there again after a reload. It must be the page's alone (a kept page's folder, under its Web Lock): the
    /// page takes its term at once, since no other writer holds the term its old page left (`members::Alone::lead_owned`). None:
    /// memory.
    pub store: Option<crate::store::Store>,
    /// The page's node (B5): asked for the tables the page doesn't have, with the rows of its own tables a query names.
    /// None asks no node: a query over a table the page lacks is 42P01, as it is on a page with no node.
    pub node: Option<Node>,
    /// The page's live heap bytes, read by the program that runs the page (`web/` counts its allocator's). The watch reads
    /// it at each poll of a query and stops the query that grows the page past its line (`Watch`). None watches nothing.
    pub memory: Option<Memory>,
    /// The memory line the program that runs the page found for its device, in bytes (`web/`: a phone's browser gives a
    /// page less than a desktop's), taken when `PONDRA_MEMORY_MAX_GB` isn't set (`memory_line`). None: `env::PAGE_LINE`.
    pub line: Option<usize>,
}

/// The page's live heap bytes, as the program that runs the page reads them (`Options::memory`).
pub type Memory = Arc<dyn Fn() -> usize + Send + Sync>;

/// A page's memory watch: what it reads its live bytes with, and the line past which the query that grew them is stopped
/// (`polled`). A page's line is set by `PONDRA_MEMORY_MAX_GB`, else by its device (`memory_line`).
struct Watch {
    live: Memory,
    line: usize,
}

impl Watch {
    /// The bytes a query may grow the page by now: to its line, and at least the tenth of the line `polled` lets any
    /// query grow, so a page past its line still takes a small answer.
    fn room(&self) -> usize {
        self.line.saturating_sub((self.live)()).max(self.line / 10)
    }
}

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
        Options { store: None, node: None, memory: None, line: None }
    }
}

/// The words of a write the site's browser storage can't keep, because its quota is full (`web/` says so when the browser
/// refuses a write). A node's full disk is 53100 too, so this is the same answer at every door.
pub const FULL: &str = "this site's browser storage is full";

/// A page: a member on the core's lake, claimed once in the module (`claim`). Its requests are answered by its member (`request`).
pub struct Page {
    holds_claim: bool, // (the module's page is this page's: `close` and `ending` clear it, and give the module's page back)
    member: tokio::sync::OnceCell<crate::node::Paged>, // (the page's member on the core's lake: opened by its first request, or by `open_lake`)
    store: Option<crate::store::Store>, // (the store the member keeps its lake in, as `Options::store` gave it)
    line: Option<usize>, // (the device's line the member's queries hold, as `Options::line` gave it)
    node: Option<Node>,
    watch: Option<Watch>, // (the page's memory watch: none when the page was given no `memory`, or its line is 0)
    unread: Vec<(String, String)>, // (the kept files `import_kept` couldn't read, each as its path and the words of why)
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

/// A `files` answer (`lake_files::Filed`) as the page's answer: the node's status and content type, and when it is a
/// file's bytes, its `etag` (quoted) and `cache-control: no-store`, as the node's HTTP sends them.
fn filed_answer(f: crate::lake_files::Filed) -> Answer {
    let mut headers = vec![("content-type", f.content_type.to_string())];
    if let Some(version) = f.etag {
        headers.push(("etag", quoted(&version)));
        headers.push(("cache-control", "no-store".to_string()));
    }
    Answer { status: f.status, headers, body: f.body }
}

/// The principal a page's member runs its requests as: this process's admin, as the page's own door (`lake_sql`, `lake_files`).
fn lake_principal() -> crate::auth::Principal {
    crate::auth::Principal::token(crate::auth::Role::Admin).at("page", None)
}

/// An answer `statement::sql` handed in pieces (`Rendered::Pieces`), as one body: its first piece, then each piece the rest
/// sends, in order. A piece that fails, or a channel that closes before the answer's end, fails the answer.
async fn collected(mut h: crate::query::Handed, content: &'static str) -> Answer {
    let mut body = std::mem::take(&mut h.first);
    let mut cut = false;
    loop {
        match h.rest.recv().await {
            Some(Ok(piece)) => body.extend_from_slice(&piece),
            Some(Err(_)) => {
                cut = true;
                break;
            }
            None => break,
        }
    }
    if cut || !h.ended.load(std::sync::atomic::Ordering::Acquire) {
        return failed(anyhow::anyhow!("the answer stopped before its end"));
    }
    Answer::ok(content, body)
}

/// The kinds of kept file `import_kept` imports after the types: a table, or a view.
#[derive(Clone, Copy)]
enum Kept {
    Table,
    View,
}

/// The kept files under `folder` whose names end `ext`, as the lake's store lists them (`import_kept`).
async fn kept_files(store: &crate::store::Store, folder: &str, ext: &str) -> anyhow::Result<Vec<object_store::path::Path>> {
    let listed = store.list(Some(&object_store::path::Path::from(folder))).try_collect::<Vec<_>>().await?;
    Ok(listed.into_iter().map(|o| o.location).filter(|at| at.as_ref().ends_with(ext)).collect())
}

/// A kept file's bytes, read from the lake's store (object_store 0.14, whose `get` is on its ext trait).
async fn kept_bytes(store: &crate::store::Store, at: &object_store::path::Path) -> anyhow::Result<bytes::Bytes> {
    use object_store::ObjectStoreExt as _;
    Ok(store.get(at).await?.bytes().await?)
}

/// Deletes a kept file the import has imported (object_store 0.14, as `kept_bytes`); a file already gone is deleted.
async fn forgotten(store: &crate::store::Store, at: &object_store::path::Path) -> anyhow::Result<()> {
    use object_store::ObjectStoreExt as _;
    match store.delete(at).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The words of a statement's answer that failed, as a file that doesn't import is listed with them (its body as text).
fn answered_words(answer: &Answer) -> anyhow::Error {
    anyhow::anyhow!("{}", String::from_utf8_lossy(&answer.body))
}

/// Whether a statement's answer is 42P07: the name it makes is there already.
fn already_there(answer: &Answer) -> bool {
    answer.header("x-pondra-sqlstate") == Some("42P07")
}

/// Whether a statement's answer is a constraint's refusal: SQLSTATE class 23 (a duplicate key, a NULL in a NOT NULL column, a CHECK).
fn refused_by_constraint(answer: &Answer) -> bool {
    answer.header("x-pondra-sqlstate").is_some_and(|code| code.starts_with("23"))
}

/// The sequence a DEFAULT `nextval('s')` counts in, as `rules::create::identity_default` writes it; None for any other DEFAULT.
fn sequence_of(default: &str) -> Option<String> {
    Some(default.strip_prefix("nextval('")?.strip_suffix("')")?.replace("''", "'"))
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

/// The routes a claimed page answers in its member (`request`): `/sql`, `/objects`, and `/files/…` by the four methods.
fn lake_route(method: &str, route: &str) -> bool {
    match (method, route) {
        ("POST", "/sql") | ("GET", "/objects") => true,
        ("PUT" | "GET" | "DELETE" | "POST", _) => route.starts_with(FILES),
        _ => false,
    }
}

impl Page {
    /// The kept files `import_kept` couldn't read, each as its path and the words of why. The file is left as it is, so the
    /// next open tries it again.
    pub fn unread(&self) -> &[(String, String)] {
        &self.unread
    }

    /// A page on the core's lake: the module's one page, claimed now (`node::claim_page`, refused 55006 while a live page holds
    /// it). Its member on the core's lake opens at its first request (`member`), and keeps its lake in `store`. Its memory is
    /// watched when `memory` is given. `open_lake` also opens the member now, and imports a folder kept by 0.35/0.36.
    pub fn claim(options: &Options) -> anyhow::Result<Page> {
        crate::node::claim_page()?;
        #[cfg(target_arch = "wasm32")]
        if let Some(live) = &options.memory {
            crate::store::read_resident_with(live.clone()); // (the core's watch reads the page's memory as this page's program does)
        }
        let watch = options.memory.as_ref().zip(memory_line(options.line)).map(|(live, line)| Watch { live: live.clone(), line });
        Ok(Page { holds_claim: true, member: Default::default(), store: options.store.clone(), line: options.line, node: options.node.clone(), watch, unread: vec![] })
    }

    /// `claim`, with the member opened now (`open_now`), so an open's error is this call's. A folder kept by 0.35/0.36 in
    /// `store` is imported into the lake once (`import_kept`). A page that fails to open is closed, and the module's page is
    /// free again.
    pub async fn open_lake(options: &Options) -> anyhow::Result<Page> {
        let mut page = Page::claim(options)?;
        if let Err(e) = page.open_now().await {
            page.close().await; // (a page that fails to open closes: the module's one page is free again, not 55006)
            return Err(e);
        }
        Ok(page)
    }

    /// The member opened (`member`), and the kept folder imported into it (`import_kept`, which runs its statements through
    /// the member, so it is set first): what `open_lake` does at once.
    async fn open_now(&mut self) -> anyhow::Result<()> {
        self.member().await?;
        match self.store.clone() {
            Some(store) => self.import_kept(&store).await,
            None => Ok(()),
        }
    }

    /// The page's member on the core's lake, opened if it isn't yet (`node::open_page`, with the store and the line the page
    /// was claimed with). A closed page has none, and its requests are refused. An open that fails is tried again by the next
    /// request, as the cell keeps no error.
    async fn member(&self) -> anyhow::Result<&crate::node::Paged> {
        anyhow::ensure!(self.holds_claim, "this page is closed");
        self.member.get_or_try_init(|| crate::node::open_page(self.store.clone(), self.line)).await
    }

    /// `open_lake`, refused by name: this build has no core's lake (build `web/` with `--features lake`).
    /// Ends the page's member on the core's lake, if one opened: its loops, its catalog, its term and its lake in memory
    /// (`node::Paged::close`), and then gives the module's page back. A page closed twice, or opened another way, does nothing
    /// the second time.
    pub async fn close(&mut self) {
        if let Some(paged) = self.member.take() {
            paged.close().await;
        }
        if std::mem::take(&mut self.holds_claim) {
            crate::node::unclaim_page();
        }
    }

    /// The page's end, as `Drop` gives it: the module's page is given back at once, and the member's close is returned for the
    /// caller to run (None when no member opened). The next claim can be made while that close runs, and the next member's open
    /// waits for it (`node::open_page`).
    pub fn ending(&mut self) -> Option<impl Future<Output = ()> + 'static> {
        if std::mem::take(&mut self.holds_claim) {
            crate::node::unclaim_page();
        }
        self.member.take().map(|paged| paged.close())
    }

    /// `close`, for a build with no core's lake: nothing was opened, so nothing ends.
    /// One request, as a node answers it (ADR-061): `method` and `path` (its query for `format=`, and nothing else
    /// here), the request's `headers` (any case of name) and `body`. Every failure is an answer too, with its SQLSTATE. The
    /// routes are the lake's, on the page's member (which opens at this request, and whose error is the answer): `/sql`,
    /// `/objects` and `/files/…`. Any other route is 404, as the node's router answers it.
    pub async fn request(&self, method: &str, path: &str, headers: &[(String, String)], body: bytes::Bytes) -> Answer {
        let (route, query) = path.split_once('?').unwrap_or((path, ""));
        if (method, route) == ("GET", "/ready") {
            return Answer::ok(TEXT, "ready");
        }
        if !lake_route(method, route) {
            let mut answer = failed(crate::codes::coded("0A000", format!("a page has no route {method} {route}")));
            answer.status = 404;
            return answer;
        }
        if let Err(e) = self.member().await {
            return failed(e);
        }
        match (method, route) {
            ("POST", "/sql") => self.lake_sql(query, headers, body).await,
            ("GET", "/objects") => self.lake_objects().await,
            _ => self.lake_files(method, &route[FILES.len()..], query, headers, body).await,
        }
    }

    /// `POST /sql` on the page's member (`open_lake`): the statement is `statement::sql`'s, run as the owner (`WHO` for an
    /// admin principal of this process, as the HTTP door's `guard` gives an owner's request), and its answer is the HTTP
    /// door's: the same body, status and content type, with `x-pondra-session` and `x-pondra-notices` as HTTP adds them.
    async fn lake_sql(&self, query: &str, headers: &[(String, String)], body: bytes::Bytes) -> Answer {
        // (a plain-SQL body, as `routines::Request` reads one: its content type is absent or text. Only such a body may go to the node)
        let plain = request_header(headers, "content-type").is_none_or(|kind| kind.starts_with("text/"));
        let text = body.clone(); // (a `Bytes` clone is a count, not a copy: the statement takes the body)
        let answer = self.lake_said(query, headers, body).await;
        if !plain || self.node.is_none() || answer.header("x-pondra-sqlstate") != Some("42P01") {
            return answer;
        }
        let Ok(text) = std::str::from_utf8(&text) else { return answer };
        match self.lake_outgoing(text, param(query, "format")).await {
            Ok(Some(instead)) => instead,
            Ok(None) => answer, // (the node has no part in it: the lake's 42P01 stands)
            Err(e) => failed(e),
        }
    }

    /// What a node answers for a statement the lake can't plan because a table it names is not the page's (`lake_sql`'s 42P01),
    /// as a node answers it: a query goes to the node with the rows of the page's tables it names, and
    /// a fill (CTAS, or INSERT … SELECT into one of the page's tables) takes the node's rows for its query and runs its write over
    /// them on the lake. The names are read over the text as the core expands it (`plain_of`); the node is asked the text as it
    /// is written. None when no table it names is the node's, when a fill names none, or when the text doesn't parse: the 42P01 stands.
    async fn lake_outgoing(&self, text: &str, format: Option<&str>) -> anyhow::Result<Option<Answer>> {
        let Some(paged) = self.member.get() else { return Ok(None) };
        let lake = &paged.app.lake;
        let expanded = crate::routines::expand(lake, &crate::vars::bound(text)?).await?;
        let state = lake.session().state();
        let Ok(statement) = parsed(&state, &expanded) else { return Ok(None) };
        // (a name is the page's when the lake holds a table of it: a file's name is never one, and the names are the lake's own)
        let mut have = BTreeSet::new();
        for name in state.resolve_table_references(&statement)? {
            if !crate::ext_names::is(name.table()) && lake.table_meta_if(name.table()).await?.is_some() {
                have.insert(name.table().to_string());
            }
        }
        let held = |name: &str| have.contains(name);
        let tables = named(&state, &statement, &held)?;
        let format_query = format.map(|f| format!("format={f}")).unwrap_or_default();
        match fill_of(&state, &statement)?.filter(|(target, _, insert)| !insert || home(&state, target, &held) == Home::Page) {
            Some((target, fill, _)) => {
                // (the fill's query as the text writes it, when it parses: the node is asked what was written)
                let written = parsed(&state, text).ok().and_then(|as_written| fill_of(&state, &as_written).ok().flatten().map(|(_, q, _)| q.to_string()));
                let sent = written.unwrap_or_else(|| fill.to_string());
                let fill_tables = named(&state, &as_statement(fill), &held)?;
                if fill_tables.iter().all(|(h, _)| *h != Home::Node) {
                    return Ok(None);
                }
                if target.table() == NODE_ROWS {
                    return Err(node_rows_taken());
                }
                let names = page_names(fill_tables)?;
                let write = over_rows(&state, &statement)?;
                let asked = self.lake_request(&sent, &names, "format=arrow").await?;
                let answered = self.ask(asked).await?;
                if answered.status != 200 {
                    return Ok(Some(answered)); // (the node's refusal is the page's answer, as it is)
                }
                node_arrow(&answered)?;
                let body = request_of(&write, &BTreeSet::from([NODE_ROWS.to_string()]), &[answered.body])?;
                let headers = [("content-type".to_string(), request_body::CONTENT_TYPE.to_string())];
                Ok(Some(self.lake_said(&format_query, &headers, body).await))
            }
            None => {
                if tables.iter().all(|(h, _)| *h != Home::Node) {
                    return Ok(None);
                }
                if query_of(&statement).is_none() {
                    return Err(crate::codes::coded("0A000", WRITE_TO_NODE));
                }
                let names = page_names(tables)?;
                let asked = self.lake_request(text, &names, &format_query).await?;
                Ok(Some(self.ask(asked).await?))
            }
        }
    }

    /// The request a page sends its node (`Sent`): `sql`, with the rows of the page's tables `names`, each read on the lake as
    /// `SELECT *` (an Arrow stream), within `PONDRA_WEB_SEND_MB` (`sending`). A page table the lake can't read is the statement's
    /// own answer, sent as it is.
    async fn lake_request(&self, sql: &str, names: &BTreeSet<String>, query: &str) -> anyhow::Result<Sent> {
        let mut streams = vec![];
        for name in names {
            let read = format!("SELECT * FROM {}", crate::ext_names::quoted(name));
            let answer = self.lake_said("format=arrow", &[], bytes::Bytes::from(read)).await;
            if answer.status != 200 {
                return Err(anyhow::anyhow!(String::from_utf8_lossy(&answer.body).into_owned()));
            }
            streams.push(answer.body);
        }
        sending(streams.iter().map(|s| s.len()).sum())?;
        Ok(Sent { query: query.to_string(), content_type: request_body::CONTENT_TYPE, body: request_of(sql, names, &streams)?, room: None })
    }

    /// One `/sql` request on the page's member, as `lake_sql` answers it: the page's own requests (a kept page's import, the
    /// rows it sends its node) go through it too, so each is the core's statement path.
    async fn lake_said(&self, query: &str, headers: &[(String, String)], body: bytes::Bytes) -> Answer {
        let Some(paged) = self.member.get() else { return failed(anyhow::anyhow!("the page's member on the core's lake is not open")) };
        // (the query read as the HTTP door's `Query` reads it: decoded, and a number that doesn't parse refused 400)
        let q: std::collections::HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes()).into_owned().collect();
        let text = |name: &str| q.get(name).cloned();
        fn number<T: std::str::FromStr>(q: &std::collections::HashMap<String, String>, name: &str) -> Result<Option<T>, String>
        where T::Err: std::fmt::Display {
            q.get(name).map(|v| v.parse::<T>().map_err(|e| format!("Failed to deserialize query string: {e}"))).transpose()
        }
        let (after, stale_ms, rows) = match (number(&q, "after"), number(&q, "stale_ms"), number(&q, "rows")) {
            (Ok(a), Ok(s), Ok(r)) => (a, s, r),
            (Err(e), ..) | (_, Err(e), _) | (.., Err(e)) => return refused(400, e),
        };
        let p = crate::statement::SqlParams { format: text("format"), job: text("job"), spread: text("spread"), catalog: text("catalog"), after, stale_ms, rows };
        let mut map = http::HeaderMap::new();
        for (name, value) in headers {
            if let (Ok(name), Ok(value)) = (http::HeaderName::from_bytes(name.as_bytes()), http::HeaderValue::from_str(value)) {
                map.append(name, value);
            }
        }
        let who = lake_principal();
        let format = p.format.clone();
        let asked = map.clone(); // (a stop fails the session the statement ran in: the headers name it, `failed_for_memory`)
        let said = match crate::panics::work(true, crate::auth::WHO.scope(who, Box::pin(crate::statement::sql(paged.app.clone(), p, crate::auth::Role::Admin, map, body)))).await {
            Ok(said) => said,
            Err(words) => {
                if words == crate::oom::OOM || words == PAGE_OOM {
                    crate::statement::failed_for_memory(lake_principal(), &asked);
                }
                return failed(anyhow::anyhow!(words));
            }
        };
        let content = crate::answer::content_type(format.as_deref());
        let mut answer = match said.reply {
            Ok(crate::statement::Reply::Rendered(crate::statement::Rendered::Whole(b))) => Answer::ok(content, b),
            Ok(crate::statement::Reply::Rendered(crate::statement::Rendered::Pieces(h))) => collected(h, content).await,
            Ok(crate::statement::Reply::Rendered(crate::statement::Rendered::Sent)) => Answer { status: 204, headers: vec![], body: bytes::Bytes::new() },
            Ok(crate::statement::Reply::Done(v)) => match serde_json::to_vec(&v) {
                Ok(b) => Answer::ok(JSON, b),
                Err(e) => failed(e.into()),
            },
            Err(e) => failed(e),
        };
        if said.session.as_deref().is_some_and(crate::temp::holds) {
            let (name, value) = crate::statement::HELD_SESSION;
            answer = answer.with(name, value.to_string());
        }
        if let Some(notices) = crate::statement::notices(&said.heard) {
            answer = answer.with("x-pondra-notices", notices);
        }
        answer
    }

    /// `PUT`, `GET`, `DELETE` and `POST` `/files/{path}` on the page's member (`open_lake`): the node's four requests as
    /// `files.rs` answers them on the core's lake (`files::put`, `get`, `restore`, `remove`), run as the owner, so SQL on
    /// the lake reads a file the page was given. Its answer has the node's status and headers (`filed_answer`).
    async fn lake_files(&self, method: &str, name: &str, query: &str, headers: &[(String, String)], body: bytes::Bytes) -> Answer {
        let Some(paged) = self.member.get() else { return failed(anyhow::anyhow!("the page's member on the core's lake is not open")) };
        let q: std::collections::HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes()).into_owned().collect();
        let if_match = request_header(headers, "if-match");
        let lake = &paged.app.lake;
        let done = crate::auth::WHO.scope(lake_principal(), Box::pin(async move {
            match method {
                "PUT" => crate::files::put(lake, name, if_match, body).await,
                "GET" => crate::files::get(lake, name, &q).await,
                "POST" => crate::files::restore(lake, name, &q).await,
                _ => crate::files::remove(lake, name).await, // (DELETE)
            }
        })).await;
        match done {
            Ok(f) => filed_answer(f),
            Err(e) => failed(e),
        }
    }

    /// `GET /objects` on the page's member (`open_lake`): the lake's listing as the HTTP door answers it (`objects::listed`),
    /// run as the owner, as `lake_files` runs its requests. Its answer is JSON, or the listing's error.
    async fn lake_objects(&self) -> Answer {
        let Some(paged) = self.member.get() else { return failed(anyhow::anyhow!("the page's member on the core's lake is not open")) };
        let listed = crate::auth::WHO.scope(lake_principal(), Box::pin(crate::objects::listed(&paged.app.lake))).await;
        match listed {
            Ok(v) => match serde_json::to_vec(&v) {
                Ok(b) => Answer::ok(JSON, b),
                Err(e) => failed(e.into()),
            },
            Err(e) => failed(e),
        }
    }

    /// A kept folder of 0.35/0.36 (`types/`, `tables/`, `views/`), imported once into the page's lake through the core's own
    /// statements (`imported_said`): types first, then tables and views in passes until a pass imports nothing, since a table may
    /// name a table or a view listed later, and a view another view. A file goes from the store once its statements answered 200.
    /// A file that doesn't import stays where it is, listed by `unread` with its words, so the next open tries it again. Only a
    /// listing that fails is the open's error (`open_lake` closes the page then).
    async fn import_kept(&mut self, store: &crate::store::Store) -> anyhow::Result<()> {
        for at in kept_files(store, "types", ".json").await? {
            if let Err(e) = self.import_type(store, &at).await {
                self.unread.push((at.to_string(), crate::codes::words(&e)));
            }
        }
        let mut pending: Vec<(object_store::path::Path, Kept)> = kept_files(store, TABLES, ".arrow").await?.into_iter().map(|at| (at, Kept::Table)).collect();
        pending.extend(kept_files(store, VIEWS, ".json").await?.into_iter().map(|at| (at, Kept::View)));
        loop {
            let count = pending.len();
            let mut failed = vec![];
            for (at, kind) in std::mem::take(&mut pending) {
                let done = match kind {
                    Kept::Table => self.import_table(store, &at).await,
                    Kept::View => self.import_view(store, &at).await,
                };
                if let Err(e) = done {
                    failed.push((at, kind, crate::codes::words(&e)));
                }
            }
            // (a pass that imported nothing is the last: what it left is unread)
            if failed.is_empty() || failed.len() == count {
                for (at, _, words) in failed {
                    self.unread.push((at.to_string(), words));
                }
                return Ok(());
            }
            pending = failed.into_iter().map(|(at, kind, _)| (at, kind)).collect();
        }
    }

    /// A kept type, made again as `CREATE TYPE … AS ENUM` with its labels (`make_kept`).
    async fn import_type(&self, store: &crate::store::Store, at: &object_store::path::Path) -> anyhow::Result<()> {
        let kept: serde_json::Value = serde_json::from_slice(&kept_bytes(store, at).await?).context("it is not a kept type")?;
        let (Some(name), Some(labels)) = (kept["name"].as_str(), kept["labels"].as_array()) else { anyhow::bail!("it has no name and labels, so it is not a kept type") };
        let labels = labels.iter().map(|l| l.as_str().map(crate::pg_text::quote_literal)).collect::<Option<Vec<String>>>().context("its labels are not text")?;
        self.make_kept(&format!("CREATE TYPE {} AS ENUM ({})", crate::ext_names::quoted(name), labels.join(", ")), store, at).await
    }

    /// A kept view, made again as `CREATE VIEW` with its query (`make_kept`).
    async fn import_view(&self, store: &crate::store::Store, at: &object_store::path::Path) -> anyhow::Result<()> {
        let kept: serde_json::Value = serde_json::from_slice(&kept_bytes(store, at).await?).context("it is not a kept view")?;
        let (Some(name), Some(query)) = (kept["name"].as_str(), kept["query"].as_str()) else { anyhow::bail!("it has no name and query, so it is not a kept view") };
        self.make_kept(&format!("CREATE VIEW {} AS {query}", crate::ext_names::quoted(name)), store, at).await
    }

    /// A kept type or view, made by its statement, and then its file goes. A statement that finds the name there (42P07) was
    /// answered by an open that made it from this file before that open stopped, so the file goes too.
    async fn make_kept(&self, sql: &str, store: &crate::store::Store, at: &object_store::path::Path) -> anyhow::Result<()> {
        match self.imported_said(sql, None).await {
            Ok(()) => {}
            Err(answer) if already_there(&answer) => {}
            Err(answer) => return Err(answered_words(&answer)),
        }
        forgotten(store, at).await
    }

    /// A kept table, imported: made by its CREATE text as the core takes it (`imported_create`), or, with no CREATE text (no column
    /// of it had rules), as a `CREATE TABLE … AS` of its rows (`made_plain`); then filled and set (`filled`). A CREATE that finds a
    /// table of the name there is `table_there`'s.
    async fn import_table(&mut self, store: &crate::store::Store, at: &object_store::path::Path) -> anyhow::Result<()> {
        let reader = FileReader::try_new(std::io::Cursor::new(kept_bytes(store, at).await?), None)?;
        let kept = reader.schema();
        let name = kept.metadata().get(TABLE_KEY).cloned().ok_or_else(|| anyhow::anyhow!("it has no {TABLE_KEY} note, so it is not a kept table"))?;
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let counted: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let rows = stream_of(&table_schema(&kept), &batches)?;
        let create = match kept.metadata().get(CREATE_KEY) {
            Some(text) => Some(self.imported_create(text, &name)?),
            None => None,
        };
        let made = match &create {
            Some(create) => self.imported_said(create, None).await,
            None => self.made_plain(&name, &rows).await,
        };
        match made {
            Ok(()) => {}
            Err(answer) if already_there(&answer) => return self.table_there(store, at, &name, &kept, counted, &rows).await,
            Err(answer) => return Err(answered_words(&answer)),
        }
        self.filled(store, at, &name, &kept, &rows, create.is_some()).await
    }

    /// A kept table whose CREATE found a table of its name there (42P07): an open that made the table from this file stopped before
    /// the file went. A table with no rows is this import's own, and gets the file's rows (`filled`). A table of the file's count holds
    /// them already: its counters and defaults are set again, and the file goes. A table of any other count is someone else's, and is
    /// listed with the words of why.
    async fn table_there(&mut self, store: &crate::store::Store, at: &object_store::path::Path, name: &str, kept: &Schema, counted: usize, rows: &[u8]) -> anyhow::Result<()> {
        let answer = self.lake_said("format=csv", &[], bytes::Bytes::from(format!("SELECT count(*) FROM {}", crate::ext_names::quoted(name)))).await;
        if answer.status != 200 {
            return Err(answered_words(&answer));
        }
        let holds: usize = String::from_utf8_lossy(&answer.body).lines().nth(1).and_then(|n| n.parse().ok()).context("its row count is not a number")?;
        if holds == 0 {
            return self.filled(store, at, name, kept, rows, true).await;
        }
        anyhow::ensure!(holds == counted, "{name}: a table of this name is there already, with {holds} rows where its file has {counted}");
        // (a table with no identity was imported without its rules, as `imported_without_rules` leaves one: it has no counter to set)
        let lake = &self.member.get().context("the page's member on the core's lake is not open")?.app.lake;
        let identity = lake.table_meta_if(name).await?.is_some_and(|m| !m.identity.is_empty());
        self.restored(at, name, kept, !identity || kept.metadata().get(CREATE_KEY).is_none()).await?;
        forgotten(store, at).await
    }

    /// A kept table once made gets its rows, then its counters and defaults: its rows go in by an INSERT when `insert` (a CREATE made
    /// the table with none), `restored` sets the rest, and the file goes. A table made as a `CREATE TABLE … AS` has its rows in already.
    /// An INSERT a constraint refuses is `imported_without_rules`'s; any other failure drops the table, kept to be undropped as a drop
    /// keeps it, and the file stays for the next open.
    async fn filled(&mut self, store: &crate::store::Store, at: &object_store::path::Path, name: &str, kept: &Schema, rows: &[u8], insert: bool) -> anyhow::Result<()> {
        let table = crate::ext_names::quoted(name);
        if insert {
            match self.imported_said(&format!("INSERT INTO {table} SELECT * FROM {}", crate::ext_names::quoted(KEPT_ROWS)), Some(rows)).await {
                Ok(()) => {}
                Err(refusal) if refused_by_constraint(&refusal) => return self.imported_without_rules(store, at, name, kept, rows, &refusal).await,
                Err(answer) => {
                    let _ = self.imported_said(&format!("DROP TABLE {table}"), None).await;
                    return Err(answered_words(&answer));
                }
            }
        }
        if let Err(e) = self.restored(at, name, kept, kept.metadata().get(CREATE_KEY).is_none()).await {
            let _ = self.imported_said(&format!("DROP TABLE {table}"), None).await;
            return Err(e);
        }
        forgotten(store, at).await
    }

    /// A kept table whose INSERT a constraint refused (`refusal`: a duplicate key, a NULL in a NOT NULL column, a CHECK): the table its
    /// CREATE made is dropped for good (PURGE, which keeps nothing to UNDROP), and made again without its rules (`made_plain`), with its
    /// other defaults (`restored`). It is listed by `unread` with the refusal's words, and its file goes, so it imports once.
    async fn imported_without_rules(&mut self, store: &crate::store::Store, at: &object_store::path::Path, name: &str, kept: &Schema, rows: &[u8], refusal: &Answer) -> anyhow::Result<()> {
        self.imported_said(&format!("DROP TABLE {} PURGE", crate::ext_names::quoted(name)), None).await.map_err(|answer| answered_words(&answer))?;
        self.made_plain(name, rows).await.map_err(|answer| answered_words(&answer))?;
        self.restored(at, name, kept, true).await?;
        self.unread.push((at.to_string(), format!("imported without its rules: {}", String::from_utf8_lossy(&refusal.body))));
        forgotten(store, at).await
    }

    /// A kept table's rows as a table with no rules: a `CREATE TABLE … AS` of them, as an import with no CREATE text makes it.
    async fn made_plain(&self, name: &str, rows: &[u8]) -> Result<(), Answer> {
        self.imported_said(&format!("CREATE TABLE {} AS SELECT * FROM {}", crate::ext_names::quoted(name), crate::ext_names::quoted(KEPT_ROWS)), Some(rows)).await
    }

    /// A kept table's counters and defaults, set once its rows are in. A column whose DEFAULT is `nextval('s')` counts in `s`, the
    /// sequence its counter is kept under (`pondra.counters`), so the lake's sequence of that column is restarted at the counter's
    /// next value. A table with no rules (`plain`: no CREATE text, or one imported without its rules) has no identity to count, and
    /// gets its other DEFAULTs back (`ALTER TABLE … SET DEFAULT`); one the core refuses is listed by `unread`, and the table is
    /// imported all the same.
    async fn restored(&mut self, at: &object_store::path::Path, name: &str, kept: &Schema, plain: bool) -> anyhow::Result<()> {
        let counted: BTreeMap<String, counters::Counter> = match kept.metadata().get(COUNTERS_KEY) {
            Some(json) => serde_json::from_str(json).with_context(|| format!("its {COUNTERS_KEY} note is not a table's counters"))?,
            None => BTreeMap::new(),
        };
        for field in kept.fields() {
            let Some(default) = field.metadata().get(DEFAULT_KEY) else { continue };
            if let Some(sequence) = sequence_of(default) {
                if plain {
                    continue; // (an identity's counter: a table with no rules has no identity)
                }
                let Some(next) = counted.get(&sequence).and_then(counters::Counter::next) else { continue };
                let lake = &self.member.get().context("the page's member on the core's lake is not open")?.app.lake;
                let lake_sequence = lake.table_meta_if(name).await?.and_then(|m| m.identity.get(field.name()).map(|i| i.sequence.clone()));
                let lake_sequence = lake_sequence.with_context(|| format!("{name}: its column {} is no identity column", field.name()))?;
                self.imported_said(&format!("ALTER SEQUENCE {} RESTART WITH {next}", crate::ext_names::quoted(&lake_sequence)), None).await.map_err(|answer| answered_words(&answer))?;
            } else if plain {
                let set = format!("ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {default}", crate::ext_names::quoted(name), crate::ext_names::quoted(field.name()));
                if let Err(answer) = self.imported_said(&set, None).await {
                    self.unread.push((at.to_string(), format!("its column defaults were not kept (DEFAULT of {}: {})", field.name(), String::from_utf8_lossy(&answer.body))));
                }
            }
        }
        Ok(())
    }

    /// A kept table's CREATE TABLE text, as the core takes it: named as the file names the table (`pondra.table`: a RENAME keeps
    /// the name its text had), without OR REPLACE or IF NOT EXISTS, and an ALWAYS identity as BY DEFAULT. The kept rows carry the
    /// values the identity counted, which ALWAYS refuses (428C9), and the core has no OVERRIDING SYSTEM VALUE to give them.
    fn imported_create(&self, text: &str, name: &str) -> anyhow::Result<String> {
        use datafusion::sql::sqlparser::ast::GeneratedAs;
        let DfStatement::Statement(s) = parsed(&self.lake_state()?, text)? else { anyhow::bail!("its {CREATE_KEY} note is not a CREATE TABLE") };
        let SqlStatement::CreateTable(mut c) = *s else { anyhow::bail!("its {CREATE_KEY} note is not a CREATE TABLE") };
        c.name = ObjectName::from(vec![Ident::with_quote('"', name)]);
        c.or_replace = false;
        c.if_not_exists = false;
        for option in c.columns.iter_mut().flat_map(|column| column.options.iter_mut()) {
            if let ColumnOption::Generated { generated_as, .. } = &mut option.option {
                if matches!(generated_as, GeneratedAs::Always) {
                    *generated_as = GeneratedAs::ByDefault;
                }
            }
        }
        Ok(SqlStatement::CreateTable(c).to_string())
    }

    /// The session state of the member's lake, which an import parses its kept text with (`imported_create`).
    fn lake_state(&self) -> anyhow::Result<SessionState> {
        let paged = self.member.get().context("the page's member on the core's lake is not open")?;
        Ok(paged.app.lake.session().state())
    }

    /// One statement of an import, through the core's statement path (`lake_said`): `sql` alone, or with `rows` (an Arrow stream
    /// of the kept rows) as the request's table `KEPT_ROWS`, which the statement reads by name. Its answer, when it isn't 200, is
    /// the `Err` for the caller to read: the SQLSTATE it failed with, and its words.
    async fn imported_said(&self, sql: &str, rows: Option<&[u8]>) -> Result<(), Answer> {
        let answer = match rows {
            Some(rows) => {
                let head = json!({"sql": sql, "tables": [crate::ext_names::quoted(KEPT_ROWS)]}).to_string();
                let body = request_body::write(head.as_bytes(), &[rows]);
                self.lake_said("", &[("content-type".to_string(), request_body::CONTENT_TYPE.to_string())], body).await
            }
            None => self.lake_said("", &[], bytes::Bytes::copy_from_slice(sql.as_bytes())).await,
        };
        if answer.status == 200 { Ok(()) } else { Err(answer) }
    }

    /// One request to the page's node, and its answer as the node gave it, within the room the page has for it
    /// (`Sent::room`). A node that doesn't answer is 08001, its words saying which (`node` names it, and a browser's refusal
    /// reads the same).
    async fn ask(&self, mut sent: Sent) -> anyhow::Result<Answer> {
        let Some(node) = &self.node else { return Err(crate::codes::coded("08001", "the page has no node to ask")) };
        sent.room = self.watch.as_ref().map(Watch::room);
        node(sent).await.map_err(|words| crate::codes::coded("08001", format!("the page's node didn't answer: {words}")))
    }

}

/// A page dropped with its member open ends the member in the background, and gives the module's page back at once (`ending`).
/// Where this thread has no runtime to run the close on, the member is only dropped, and PAGED stays set.
impl Drop for Page {
    fn drop(&mut self) {
        let Some(closing) = self.ending() else { return };
        #[cfg(target_arch = "wasm32")]
        let runtime = true;
        #[cfg(not(target_arch = "wasm32"))]
        let runtime = tokio::runtime::Handle::try_current().is_ok();
        if runtime {
            crate::panics::untracked(closing); // (untracked: `close` calls `abort_all`, which would cut a tracked task)
        }
    }
}

/// A request watched at each of its polls, where no watch loop can run beside it (a page's one thread): before each poll,
/// past `line` by `reading` and grown by its own polls by a tenth of the line (`oom::pick`'s margin), it is stopped: its
/// future dropped, its memory let go, and `Err(())` answered (the caller says why).
#[cfg(target_arch = "wasm32")] // (its one caller, `panics::work`, is a page's: a node has a watch loop)
pub(crate) async fn polled<T>(reading: impl Fn() -> usize, line: usize, f: impl Future<Output = T>) -> Result<T, ()> {
    let margin = line / 10; // (`oom::pick`'s margin, a node's)
    let mut inner = Some(Box::pin(f));
    let mut grown = 0usize; // (what this query's polls grew the page by, less what they let go)
    std::future::poll_fn(move |cx| {
        let now = reading();
        if now >= line && grown >= margin {
            inner = None; // (the query's memory goes with its future, before anything else is read)
            return Poll::Ready(Err(()));
        }
        let Some(future) = inner.as_mut() else { return Poll::Ready(Err(())) }; // (never polled once it has answered: an answer is the stop's)
        let out = future.as_mut().poll(cx);
        let after = reading();
        grown = grown.saturating_add(after.saturating_sub(now)).saturating_sub(now.saturating_sub(after));
        out.map(Ok)
    })
    .await
}

/// The page's memory line (`env::memory_line`): its own is the line its `device` has (`Options::line`), else
/// `env::PAGE_LINE`.
pub(crate) fn memory_line(device: Option<usize>) -> Option<usize> {
    crate::env::memory_line(Some(device.unwrap_or(crate::env::PAGE_LINE)))
}

/// The schema of a kept file without the notes a kept file adds (`TABLE_KEY` and the rest): the schema the table itself has.
fn table_schema(kept: &Schema) -> SchemaRef {
    let fields: Vec<Field> = kept.fields().iter().map(|f| f.as_ref().clone().with_metadata(without_notes(f.metadata()))).collect();
    Arc::new(Schema::new_with_metadata(fields, without_notes(kept.metadata())))
}

/// The notes without the ones a kept file adds, which a table's own schema does not have.
fn without_notes(notes: &HashMap<String, String>) -> HashMap<String, String> {
    notes.iter().filter(|(k, _)| ![TABLE_KEY, DEFAULT_KEY, CREATE_KEY, COUNTERS_KEY].contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// A node's answer to a fill's query must be Arrow rows: any other `content-type` is 08P01, its words saying what it was.
fn node_arrow(answer: &Answer) -> anyhow::Result<()> {
    let said = answer.header("content-type");
    if said != Some(crate::answer::content_type(Some("arrow"))) {
        return Err(crate::codes::coded("08P01", format!("the page's node answered {}, not Arrow rows", said.unwrap_or("no content-type"))));
    }
    Ok(())
}

/// `batch` over `schema`: the same columns, under the schema's notes. A table's batches carry the table's schema exactly,
/// as `MemTable::try_new` refuses batches whose notes the table's lack.
fn over_schema(schema: &SchemaRef, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    Ok(RecordBatch::try_new(schema.clone(), batch.columns().to_vec())?)
}

/// Where a table a statement names is: one of the page's own tables, one of its files (an `ext:` name, as the front wrote
/// it), or the node's.
#[derive(Debug, PartialEq)]
enum Home {
    Page,
    File,
    Node,
}

/// The home of the table `name` names, given whether the page holds a table of its name (`held`). A name carries the default
/// schema and catalog, or none, and is the page's when it is held; a file is any `ext:` name; every other table is the node's.
fn home_of(name: &TableReference, held: bool, catalog: &str, schema: &str) -> Home {
    if crate::ext_names::is(name.table()) {
        return Home::File;
    }
    let here = name.schema().is_none_or(|s| s == schema) && name.catalog().is_none_or(|c| c == catalog);
    if here && held {
        Home::Page
    } else {
        Home::Node
    }
}

/// The home of `name` in `state`'s session, with `held` saying whether the page (or the lake) holds a table of a name.
fn home(state: &SessionState, name: &TableReference, held: &dyn Fn(&str) -> bool) -> Home {
    let options = state.config().options();
    home_of(name, held(name.table()), &options.catalog.default_catalog, &options.catalog.default_schema)
}

/// The tables a statement names, each with its home (`home`), as the page reads them. Each is named as SQL wrote it, with no
/// default schema or catalog the page's own names carry.
fn named(state: &SessionState, statement: &DfStatement, held: &dyn Fn(&str) -> bool) -> anyhow::Result<Vec<(Home, String)>> {
    let mut tables = vec![];
    for name in state.resolve_table_references(statement)? {
        tables.push((home(state, &name, held), name.table().to_string()));
    }
    Ok(tables)
}

/// The statement parsed as the session `state` parses it (its dialect and identifier rules).
fn parsed(state: &SessionState, expanded: &str) -> anyhow::Result<DfStatement> {
    Ok(state.sql_to_statement(expanded, &state.config().options().sql_parser.dialect)?)
}

/// A table's name as the planner reads it: its identifiers normalized as the session normalizes them.
fn table_ref(state: &SessionState, name: &ObjectName) -> anyhow::Result<TableReference> {
    let normalize = state.config().options().sql_parser.enable_ident_normalization;
    Ok(datafusion::sql::planner::object_name_to_table_reference(name.clone(), normalize)?)
}

/// The fill a statement is, if it is one: the table it writes (as the planner reads its name), the query whose rows it takes,
/// and whether the statement is an INSERT. A `CREATE TABLE … AS` is a fill whatever its target, since a table the page doesn't
/// have yet is the node's by name. An `INSERT INTO` is one only when its target is one of the page's own tables, which the
/// caller checks (`home`).
fn fill_of<'a>(state: &SessionState, statement: &'a DfStatement) -> anyhow::Result<Option<(TableReference, &'a Query, bool)>> {
    let DfStatement::Statement(s) = statement else { return Ok(None) };
    match &**s {
        SqlStatement::CreateTable(CreateTable { name, query: Some(query), .. }) => Ok(Some((table_ref(state, name)?, query.as_ref(), false))),
        SqlStatement::Insert(Insert { table: TableObject::TableName(name), source: Some(query), .. }) => Ok(Some((table_ref(state, name)?, query.as_ref(), true))),
        _ => Ok(None),
    }
}

/// The fill's statement with its query put for the node's rows (`rows_query`), as SQL text the session plans: a `CREATE TABLE t
/// AS SELECT * FROM "the node's rows"`, or an `INSERT INTO t (cols) SELECT * FROM "the node's rows"`.
fn over_rows(state: &SessionState, statement: &DfStatement) -> anyhow::Result<String> {
    let mut write = statement.clone();
    if let DfStatement::Statement(s) = &mut write {
        match &mut **s {
            SqlStatement::CreateTable(c) => c.query = Some(rows_query(state)?),
            SqlStatement::Insert(i) => i.source = Some(rows_query(state)?),
            _ => {}
        }
    }
    Ok(write.to_string())
}

/// `SELECT * FROM` the node's rows, parsed as the session parses SQL: the query a fill's write puts in place of its own.
fn rows_query(state: &SessionState) -> anyhow::Result<Box<Query>> {
    query_of_text(state, &format!("SELECT * FROM {}", crate::ext_names::quoted(NODE_ROWS)))
}

/// `sql` parsed as a query, as the session parses SQL (`parsed`).
fn query_of_text(state: &SessionState, sql: &str) -> anyhow::Result<Box<Query>> {
    let DfStatement::Statement(s) = parsed(state, sql)? else { anyhow::bail!("{sql} is not a statement a page's session parses") };
    let SqlStatement::Query(query) = *s else { anyhow::bail!("{sql} is not a query") };
    Ok(query)
}

/// The request a page sends its node (`request_body`): the JSON head with `sql` and the names `names` quoted as its tables, then
/// one stream per name, in the names' order.
fn request_of(sql: &str, names: &BTreeSet<String>, streams: &[impl AsRef<[u8]>]) -> anyhow::Result<bytes::Bytes> {
    let quoted: Vec<String> = names.iter().map(|name| crate::ext_names::quoted(name)).collect();
    let head = serde_json::to_vec(&json!({"sql": sql, "tables": quoted}))?;
    Ok(request_body::write(&head, streams))
}

/// The refusal of a fill that names its target `NODE_ROWS`: the name the rows have while the fill is planned is the page's.
fn node_rows_taken() -> anyhow::Error {
    crate::codes::coded("0A000", format!("{}: a page gives its node's rows this name while it fills a table, so no table it makes may have it", crate::ext_names::quoted(NODE_ROWS)))
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
        return Err(crate::codes::coded("0A000", FILE_BESIDE_NODE));
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

/// The bytes of a page's tables a query sends its node, within `PONDRA_WEB_SEND_MB` (`sending`), both paths' limit.
fn sending(bytes: usize) -> anyhow::Result<()> {
    within(bytes, crate::env::or("PONDRA_WEB_SEND_MB", 64))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_sends_no_more_than_web_send_mb() {
        assert!(within(1024 * 1024, 1).is_ok());
        let over = within(3 * 1024 * 1024, 1).unwrap_err();
        assert_eq!(crate::codes::of(&over), "54000");
        assert!(crate::codes::words(&over).contains("web_send_mb"), "{over:#}");
    }
}
