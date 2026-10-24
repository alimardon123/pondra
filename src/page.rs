//! A lake-less engine: a node over a lake in memory, on the machine that is the page (designs/l2-b4-web.md). Its
//! files are kept as a node keeps a lake's own (`lake_files`), `FROM 'x.csv'` reads them as a node's working folder
//! reads them, and its tables are made by SQL (`CREATE TABLE t AS FROM 'x.parquet'`). Its routes mean what a node's
//! mean, through ADR-061's request contract. DuckDB's forms that ask the data (`PIVOT`, `COLUMNS`, `SUMMARIZE`) and
//! ASOF JOIN answer as on a node. `web/` wraps it for a browser.

use crate::front::Host;
use crate::lake_files::{changed, content_type, not_found, there_already, under_files};
use anyhow::Context;
use async_trait::async_trait;
use futures::{TryFutureExt, TryStreamExt};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::ipc::{reader::FileReader, writer::FileWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, Session, SchemaProvider, TableProvider};
use datafusion::common::{Constraints, DFSchema, Statistics};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{source_as_provider, MemTable, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::dml::{InsertOp, WriteOp};
use datafusion::logical_expr::{DdlStatement, Expr, LogicalPlan, TableSource};
use datafusion::object_store::{memory::InMemory, path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use serde_json::json;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, OnceLock, PoisonError};

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

/// How a page's session is made: its target partitions (a page in a browser has one), where it keeps its files and its
/// tables, and how it reaches the web.
pub struct Options {
    pub partitions: usize,
    /// The store a page keeps its files and its tables in: a browser gives its origin's private file system (OPFS), so
    /// what a page was given is there again after a reload (`kept`). None: the page's own memory, gone with the page.
    pub store: Option<Arc<dyn ObjectStore>>,
    /// The store for a web origin (`https://host[:port]`) a statement names, made the first time one does: a
    /// browser's `fetch` (`web/`). None: the page reads only the files given to it.
    pub web: Option<Web>,
}

/// A page's way to the web: a store for an origin. The program that runs the page gives it, so the core holds no
/// HTTP client of its own.
pub type Web = Arc<dyn Fn(&str) -> Arc<dyn ObjectStore> + Send + Sync>;

impl Default for Options {
    fn default() -> Self {
        Options { partitions: 1, store: None, web: None }
    }
}

/// A page: a session of its own, whose tables are the ones its SQL makes and writes, and whose files are the ones its
/// caller put. Both are kept in the store the page was given, or in its memory when it was given none.
pub struct Page {
    ctx: SessionContext,
    files: Arc<dyn ObjectStore>, // (the store given, or memory: the files as put, `files/…`, and the tables kept, `tables/…`)
    keeps: bool, // (a store was given: its tables are kept there; a page in memory keeps no second copy of them)
    web: Option<Web>,
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
    /// from the store as they are asked for. With no store given nothing is listed, so the page is `open`'s.
    pub async fn kept(options: &Options) -> anyhow::Result<Page> {
        let page = Page::open(options);
        let listed = page.files.list(Some(&Path::from(TABLES))).try_collect::<Vec<_>>().await?;
        for object in listed.iter().filter(|o| o.location.as_ref().ends_with(".arrow")) {
            page.restore(&object.location).await?;
        }
        Ok(page)
    }

    /// A page with no tables, its session a node's (`engine::session`) with the partitions asked for. Its files are the
    /// store's, as they are kept there; with no store given they are in memory.
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
        let files: Arc<dyn ObjectStore> = match &options.store {
            Some(store) => store.clone(),
            None => Arc::new(InMemory::new()),
        };
        let url = ObjectStoreUrl::parse("mem://").expect("a URL");
        ctx.register_object_store(url.as_ref(), files.clone());
        Page { ctx, files, keeps: options.store.is_some(), web: options.web.clone() }
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
    /// rows in `format` (an answer with no rows keeps its columns, as `engine::collect` gives them: invariant 88). An
    /// EXPLAIN's rows are made readable as a node's are (`explain::answered`).
    async fn sql(&self, text: &str, format: Option<&str>) -> anyhow::Result<Answer> {
        let (frame, changed) = self.plan(text).await?;
        let batches = crate::engine::collect(frame).await?;
        if let Some(name) = changed {
            self.written(&name).await?;
        }
        let batches = if crate::explain::asked(text) { crate::explain::answered(batches)? } else { batches };
        Ok(Answer::ok(crate::answer::content_type(format), crate::answer::render(&batches, format, |_| None)?))
    }

    /// A statement read as a node reads it: the front, ASOF JOIN's marker, then planned on the statement's own copy of
    /// the page's session (`statement`), with the files it names given to that copy alone (`given`). What a statement
    /// makes or changes (a table) is the page's; nothing else it registers reaches the page or another statement. The
    /// name of the page table it changes, if any (`touched`), comes back with the frame: `sql` keeps that table after
    /// the frame runs.
    async fn plan(&self, text: &str) -> anyhow::Result<(DataFrame, Option<String>)> {
        let row = crate::extension::row(text).transpose()?; // (DuckDB's INSTALL and LOAD: a row naming what happened, `extension.rs`)
        let text = row.as_deref().unwrap_or(text);
        let expanded = crate::front::expand(&PageHost(self), text, &HashMap::new()).await?;
        // (a page keeps no table's past, so `AT (…)` is refused by name: left alone, it would fail as an unknown table "at:…")
        if crate::past_names::mentioned(&expanded) {
            return Err(crate::codes::coded("0A000", format!("{}: a page keeps no table's past (a node reads a table as it was)", crate::past_names::spelled(&expanded))));
        }
        let expanded = crate::asof::as_of(&expanded)?.into_owned();
        rules(&expanded)?; // (a table rule a page can't keep is refused before anything is planned or read)
        let (ctx, overlay) = self.statement()?;
        crate::explain::prepare(&ctx, &expanded); // (an EXPLAIN's operators' expected rows, as a node's)
        self.given(&overlay, &expanded).await?;
        // (a `Mutex`, not a `Cell`: this future must stay `Send`, and the guard is never held over an `.await`)
        let changed = std::sync::Mutex::new(None);
        let frame = crate::engine::sql_checked(&ctx, &expanded, |plan| {
            may(plan)?;
            *changed.lock().unwrap_or_else(PoisonError::into_inner) = touched(plan);
            Ok(())
        })
        .await?;
        Ok((frame, changed.into_inner().unwrap_or_else(PoisonError::into_inner)))
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
    /// that is not a kept table, fails with its path in the error.
    async fn restore(&self, at: &Path) -> anyhow::Result<()> {
        let file = self.files.get(at).await?.bytes().await?;
        let (name, table) = self.table_from_file(file).map_err(|e| anyhow::anyhow!("{at}: {e:#}"))?;
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
        let takes = |k: &str| k == "hive_partitioning" || (spec.format == "csv" && CSV_OPTIONS.contains(&k)) || (spec.format == "delta" && k == "version");
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
        let found = crate::ext_names::listed(&state, &given, |u| Some(crate::codes::coded("42P01", format!("no file {} was given to this page (PUT /files/{{path}})", written(u))))).await?;
        let files = crate::ext_names::schema_of(&state, &given, &found).await?;
        let columns: Vec<(String, String)> = files.fields().iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect();
        let keys = crate::ext_names::keys(&found, &given, &[], &columns)?;
        let schema = crate::answer::schema(&[columns, keys.clone()].concat())?;
        // (the files as a node lists and reads them: a Parquet footer's rows and ranges for the planner, every column kept)
        let stored: Vec<String> = files.fields().iter().map(|f| f.name().clone()).collect();
        let format = crate::ext_names::file_format(&given, &state)?;
        let data = crate::ext_names::data_files(&state, &format, &files, found, !keys.is_empty(), &stored, false).await?;
        let refs: Vec<_> = data.iter().collect();
        let df = crate::ext_names::read(&self.ctx, &refs, &schema, &given).await?;
        overlay.own(name.to_string(), df.into_view()); // (a name is one table: the statement's own map)
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
        let version = given.options.get("version").map(|v| v.parse::<i64>().with_context(|| format!("version is a number, not {v}"))).transpose()?;
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
/// in the working folder (`under_files`), and any other URL refused by name: a page reads the files it was given and the web.
/// A `mem:` name that leaves `files/` (`..`, `%2E%2E`) is refused too: the page's kept tables are not SQL's to read.
fn located(url: &str) -> anyhow::Result<String> {
    if is_web(url) {
        return Ok(url.to_string());
    }
    let made = if url.starts_with(WORKING) {
        url.to_string()
    } else if url.contains("://") {
        return Err(files_or_web(url));
    } else {
        format!("mem:///{}", under_files(url.strip_prefix("./").unwrap_or(url))?)
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

/// A table's batches, every partition's, as its memory table holds them (`inner`); None for another provider's table.
async fn batches_of(table: &Arc<dyn TableProvider>) -> Option<Vec<RecordBatch>> {
    let memory = inner(table).downcast_ref::<MemTable>()?;
    let mut batches = vec![];
    for partition in &memory.batches {
        batches.extend(partition.read().await.iter().cloned());
    }
    Some(batches)
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
    use datafusion::prelude::ident;
    if found.files.is_empty() {
        let columns: Vec<(String, String)> = found.columns.iter().map(|(c, t)| (found.names.get(c).unwrap_or(c).clone(), t.clone())).collect();
        return Ok(Arc::new(MemTable::try_new(crate::answer::schema(&columns)?, vec![vec![]])?));
    }
    let files: Vec<&crate::data_file::DataFile> = found.files.iter().collect();
    let df = crate::scan::read(ctx, &files, &crate::answer::schema(&found.columns)?, None).await?;
    let select = found.columns.iter().map(|(c, _)| {
        let name = found.names.get(c).map_or(c.as_str(), String::as_str);
        // (a column named as it is is not aliased to itself: `query.rs` says why)
        if name == c { ident(c) } else { ident(c).alias(name) }
    }).collect::<Vec<_>>();
    Ok(df.select(select)?.into_view())
}

/// A table's rows and bytes, as its batches hold them: a memory table's, which is what a page's tables are. None for
/// another provider, whose size is not known here.
async fn sizes(table: &Arc<dyn TableProvider>) -> Option<(usize, usize)> {
    let batches = batches_of(table).await?;
    Some(batches.iter().fold((0, 0), |(rows, bytes), b| (rows + b.num_rows(), bytes + b.get_array_memory_size())))
}

/// A page's table: its rows a `MemTable`, and what the join order needs to know of them, worked out the first time it
/// asks (as `query::Pruned::stats` is). The rule asks only when three or more inputs are joined, so a table never joined
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
/// no routines, attached lakes, views with measures or types, so the trait's defaults answer "none" for those; the columns
/// are its tables' own.
struct PageHost<'a>(&'a Page);

// (a page keeps no routines, attached catalogs, views with measures or types of its own: the trait's defaults say so)
impl Host for PageHost<'_> {
    fn name(&self) -> String {
        "page".to_string()
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

    // (not an `async fn`: a loop back into the front goes through here, and this declared `Send` is where the
    // compiler's check of that loop stops)
    fn ask(&self, sql: &str) -> impl Future<Output = anyhow::Result<DataFrame>> + Send {
        // (it only reads: the page table a statement would change is none of its business)
        self.0.plan(sql).map_ok(|(frame, _)| frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::ipc::reader::StreamReader;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

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
        let protocol = json!({"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["columnMapping", "deletionVectors"], "writerFeatures": ["columnMapping", "deletionVectors"]}});
        let schema = json!({"type": "struct", "fields": [
            {"name": "id", "type": "long", "nullable": true, "metadata": {"delta.columnMapping.id": 1, "delta.columnMapping.physicalName": "id"}},
            {"name": "label", "type": "string", "nullable": true, "metadata": {"delta.columnMapping.id": 2, "delta.columnMapping.physicalName": "col-2"}},
        ]});
        let metadata = json!({"metaData": {"id": "orders", "format": {"provider": "parquet", "options": {}}, "schemaString": schema.to_string(), "partitionColumns": [], "configuration": {"delta.columnMapping.mode": "name"}}});
        let bitmap = crate::scan::roaring_bytes(&[1]);
        let mut deleted = add("a.parquet", 2, a, (1, 2));
        deleted["add"]["deletionVector"] = json!({"storageType": "i", "pathOrInlineDv": crate::scan::z85_of(&bitmap), "sizeInBytes": bitmap.len(), "cardinality": 1});
        let commits = [
            vec![protocol, metadata, add("a.parquet", 2, a, (1, 2)), add("b.parquet", 1, b, (3, 3))],
            vec![json!({"remove": {"path": "a.parquet", "deletionTimestamp": 0, "dataChange": true}}), deleted],
            vec![add("c.parquet", 1, c, (4, 4))],
        ];
        for (v, actions) in commits.iter().enumerate() {
            let body: String = actions.iter().map(|a| format!("{a}\n")).collect();
            memory.put(&Path::from(format!("lake/orders/_delta_log/{v:020}.json")), body.into_bytes().into()).await.unwrap();
        }
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
}
