//! Temporary tables and views (ADR-028): `CREATE TEMP TABLE` and `CREATE TEMP VIEW` are a
//! session's own, as in Postgres. Nobody else sees them, they shadow a lake table or view of the
//! same name, and they end with the session.
//!
//! A temporary table lives in the memory of the node the session talks to, within
//! `PONDRA_TEMP_MB` (1024) for all of them, with system columns of its own: INSERT, UPDATE, DELETE
//! and MERGE work on it as on a lake table (`change::rows_of` works out what changes). A query
//! reading one runs on this node (`server::App::query_as`), and isn't answered from the result
//! cache.
//!
//! A session is a Postgres connection, or a client's `x-pondra-session` header (the Python and
//! JavaScript clients send one, and end it on `close()`); a procedure has its caller's. An idle one
//! ends after `PONDRA_SESSION_IDLE_SECS` (3600).
use crate::ddl::{mentions, Ddl};
use crate::server::App;
use crate::store::TableMeta;
use crate::write::Stmt;
use anyhow::{ensure, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast;
use serde_json::{json as j, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

tokio::task_local! {
    /// The session a request belongs to, if any.
    pub static SESSION: Option<String>;
}

/// The session a request names (`x-pondra-session`), or that of the caller whose procedure sent it.
pub fn of(headers: &axum::http::HeaderMap) -> Option<String> {
    let named = headers.get("x-pondra-session").and_then(|v| v.to_str().ok());
    let named = named.filter(|s| (8..=128).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    crate::auth::lent_session(token).or(named.map(id)) // (a procedure's own client names one: its caller's wins)
}

/// The session a client's id names: its user's (another who names the same id has a session of its own).
pub fn id(id: &str) -> String { format!("{}~{id}", crate::auth::current().map(|p| p.name).unwrap_or_default()) }

pub fn current() -> Option<String> { SESSION.try_with(|s| s.clone()).ok().flatten() }

/// The current session's temporary tables (name, columns) and views (name, SQL), as Postgres's
/// catalog lists them (`pg_catalog.rs`).
pub fn listed() -> (Vec<(String, Vec<(String, String)>)>, Vec<(String, String)>) {
    let Some(s) = current() else { return Default::default() };
    let all = SESSIONS.lock().unwrap();
    let Some(x) = all.get(&s) else { return Default::default() };
    (x.tables.iter().map(|(n, t)| (n.clone(), t.columns.clone())).collect(), x.views.iter().map(|(n, v)| (n.clone(), v.clone())).collect())
}

const NO_SESSION: &str = "a temporary table or view is a session's: a Postgres connection's, or the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>)";

struct Table {
    columns: Vec<(String, String)>, // (name, Arrow type), as a lake table's
    rows: Vec<RecordBatch>,         // its columns, then its system columns (`sys.rs`)
    next: i64,                      // the next row id
    version: i64,                   // the last change's number (its rows' `_version`)
}

impl Table {
    fn schema(&self) -> Result<datafusion::arrow::datatypes::SchemaRef> { crate::query::schema(&[self.columns.clone(), crate::sys::columns()].concat()) }
    fn bytes(&self) -> usize { self.rows.iter().map(|b| b.get_array_memory_size()).sum() }
}

#[derive(Default)]
struct Session {
    tables: HashMap<String, Table>,
    views: HashMap<String, String>,
    secrets: std::collections::BTreeMap<String, crate::ext::Secret>, // (CREATE TEMPORARY SECRET: in memory only)
    used: Option<Instant>,
    version: u64, // changes so far (`live.rs` watches them)
}

static SESSIONS: LazyLock<Mutex<HashMap<String, Session>>> = LazyLock::new(Default::default);
static CHANGES: LazyLock<tokio::sync::watch::Sender<u64>> = LazyLock::new(|| tokio::sync::watch::channel(0).0);

/// Any session's temporary tables or views changed (`live.rs` waits on it).
pub fn changes() -> tokio::sync::watch::Receiver<u64> { CHANGES.subscribe() }

/// How many changes a session's temporary tables and views have had.
pub fn version(session: Option<&str>) -> u64 { session.and_then(|s| SESSIONS.lock().unwrap().get(s).map(|x| x.version)).unwrap_or(0) }

/// A session's tables and views, made on first use; its changes counted.
fn with<T>(session: &str, change: bool, f: impl FnOnce(&mut Session) -> Result<T>) -> Result<T> {
    let mut all = SESSIONS.lock().unwrap();
    let first = all.is_empty();
    let x = all.entry(session.to_string()).or_default();
    x.used = Some(Instant::now());
    let out = f(x)?;
    if change {
        x.version += 1;
        CHANGES.send_modify(|v| *v += 1);
    }
    if first {
        crate::panics::spawn(reap()); // (only while there are sessions)
    }
    Ok(out)
}

/// Sessions idle for `PONDRA_SESSION_IDLE_SECS` end; once there are none, this stops.
async fn reap() {
    let idle = Duration::from_secs(std::env::var("PONDRA_SESSION_IDLE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(3600));
    loop {
        tokio::time::sleep(idle.min(Duration::from_secs(60))).await;
        let mut all = SESSIONS.lock().unwrap();
        all.retain(|_, x| x.used.is_some_and(|u| u.elapsed() < idle));
        if all.is_empty() {
            return;
        }
    }
}

/// The current session's temporary secrets (`CREATE TEMPORARY SECRET`), by name.
pub fn secrets() -> Vec<(String, crate::ext::Secret)> {
    let Some(s) = current() else { return vec![] };
    SESSIONS.lock().unwrap().get(&s).map(|x| x.secrets.iter().map(|(n, v)| (n.clone(), v.clone())).collect()).unwrap_or_default()
}

/// End a session: its temporary tables and views are gone, and its Python (`python::ask_session`).
pub fn end(session: &str) -> bool {
    crate::txn::end(session); // (its transaction, if open: rolled back)
    let python = crate::python::end_session(session); // (its Python's variables too)
    let gone = SESSIONS.lock().unwrap().remove(session).is_some() || python;
    if gone {
        CHANGES.send_modify(|v| *v += 1);
    }
    gone
}

/// Does `sql` read one of this session's temporary tables or views?
pub fn mentioned(sql: &str) -> bool {
    let Some(s) = current() else { return false };
    SESSIONS.lock().unwrap().get(&s).is_some_and(|x| x.tables.keys().chain(x.views.keys()).any(|n| mentions(sql, n)))
}

fn has(session: &str, name: &str, table: bool) -> bool {
    SESSIONS.lock().unwrap().get(session).is_some_and(|x| if table { x.tables.contains_key(name) } else { x.views.contains_key(name) })
}

/// Is this statement about the session's own tables and views? (Any role may keep those.)
pub fn own(stmt: &Stmt) -> bool {
    let Some(s) = current() else { return false };
    match stmt {
        Stmt::Create(c) => c.temporary,
        Stmt::TempView(..) | Stmt::TempSecret(..) => true,
        Stmt::Ddl(d) => !d.is_empty() && d.iter().all(|d| match d {
            Ddl::DropTable { name, .. } => has(&s, name, true),
            Ddl::DropView { name, .. } => has(&s, name, false),
            Ddl::DropSecret { name, .. } => SESSIONS.lock().unwrap().get(&s).is_some_and(|x| x.secrets.contains_key(name)),
            _ => false,
        }),
        Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) | Stmt::Update(t, ..) | Stmt::Delete(t, _) => has(&s, t, true),
        Stmt::Merge(m) => has(&s, &m.target, true),
        _ => false,
    }
}

/// The session's temporary tables `text` names, as tables of `ctx` (over the lake's of the same
/// name), with their system columns if `text` names one.
pub fn register(ctx: &SessionContext, text: &str) -> Result<()> {
    let Some(s) = current() else { return Ok(()) };
    let sys = crate::sys::mentioned(text);
    let mut all = SESSIONS.lock().unwrap();
    let Some(x) = all.get_mut(&s) else { return Ok(()) };
    x.used = Some(Instant::now());
    for (name, t) in x.tables.iter().filter(|(n, _)| mentions(text, n)) {
        let keep: Vec<usize> = (0..t.columns.len() + if sys { crate::sys::NAMES.len() } else { 0 }).collect();
        let rows = t.rows.iter().map(|b| b.project(&keep)).collect::<Result<Vec<_>, _>>()?;
        ctx.deregister_table(name.as_str())?;
        ctx.register_table(name.as_str(), Arc::new(MemTable::try_new(Arc::new(t.schema()?.project(&keep)?), vec![rows])?))?;
    }
    Ok(())
}

/// The session's temporary views `sql` names, and those they name.
pub fn views(sql: &str) -> Vec<(String, String)> {
    let Some(s) = current() else { return vec![] };
    let all = SESSIONS.lock().unwrap();
    let Some(x) = all.get(&s) else { return vec![] };
    let (mut text, mut out) = (sql.to_string(), Vec::<(String, String)>::new());
    loop {
        let more: Vec<(String, String)> = x.views.iter().filter(|(n, _)| !out.iter().any(|(m, _)| m == *n) && mentions(&text, n)).map(|(n, v)| (n.clone(), v.clone())).collect();
        if more.is_empty() {
            return out;
        }
        more.iter().for_each(|(_, v)| text = format!("{text} {v}"));
        out.extend(more);
    }
}

/// A statement about the session's temporary tables or views, carried out here (None: not one).
pub async fn statement(app: &App, stmt: &Stmt, files: bool) -> Result<Option<Value>> {
    let session = current();
    match stmt {
        Stmt::Create(c) if c.temporary => return create(app, &session.context(NO_SESSION)?, c, files).await.map(Some),
        Stmt::TempView(name, sql, replace) => {
            let s = session.context(NO_SESSION)?;
            ensure!(!name.contains('.'), "{name}: a temporary view's name has no schema (it is the session's)");
            ensure!(*replace || !has(&s, name, false), "temporary view {name} exists (CREATE OR REPLACE TEMP VIEW replaces it)");
            crate::query::session(&app.lake, sql, "").await?.sql(&crate::asof::rewrite(sql)?).await.with_context(|| format!("temporary view {name}"))?; // (it plans)
            with(&s, true, |x| Ok(x.views.insert(name.clone(), sql.clone())))?;
            return Ok(Some(j!({"view": name, "temporary": true})));
        }
        Stmt::TempSecret(name, params, replace) => {
            let s = session.context("a temporary secret is a session's: a Postgres connection's, or the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>)")?;
            let secret = crate::ext::temporary(name, params.clone())?;
            with(&s, true, |x| {
                ensure!(*replace || !x.secrets.contains_key(name), "temporary secret {name} exists (CREATE OR REPLACE TEMPORARY SECRET replaces it)");
                Ok(x.secrets.insert(name.clone(), secret))
            })?;
            return Ok(Some(j!({"secret": name, "temporary": true})));
        }
        Stmt::Ddl(d) => {
            for d in d {
                if let Ddl::CreateView { name, sql, .. } | Ddl::CreateMaterialized { name, sql, .. } = d {
                    ensure!(!mentioned(sql), "view {name} reads a temporary table or view, which ends with the session: CREATE TEMP VIEW {name} …");
                }
            }
            if !own(stmt) {
                return Ok(None);
            }
            let s = session.context(NO_SESSION)?;
            let dropped = with(&s, true, |x| Ok(d.iter().map(|d| match d {
                Ddl::DropTable { name, .. } => x.tables.remove(name).map(|_| name.clone()),
                Ddl::DropView { name, .. } => x.views.remove(name).map(|_| name.clone()),
                Ddl::DropSecret { name, .. } => x.secrets.remove(name).map(|_| name.clone()),
                _ => None,
            }).collect::<Vec<_>>()))?;
            return Ok(Some(j!({"dropped": dropped, "temporary": true})));
        }
        _ => {}
    }
    if !own(stmt) {
        return Ok(None);
    }
    let s = session.context(NO_SESSION)?;
    Ok(Some(match stmt {
        Stmt::Insert(t, q) => insert(app, &s, t, None, q, files).await?,
        Stmt::InsertInto(t, names, q) => insert(app, &s, t, Some(names), q, files).await?,
        _ => change(app, &s, &stmt.table(), stmt).await?,
    }))
}

/// `CREATE [OR REPLACE] TEMP TABLE name (columns) | AS query`.
async fn create(app: &App, s: &str, c: &ast::CreateTable, files: bool) -> Result<Value> {
    let name = crate::write::object(&c.name);
    ensure!(!name.contains('.'), "{name}: a temporary table's name has no schema (it is the session's)");
    if has(s, &name, true) && !c.or_replace {
        ensure!(c.if_not_exists, "temporary table {name} exists");
        return Ok(j!({"table": name, "temporary": true, "unchanged": true}));
    }
    let spec: Value = serde_json::from_str(&crate::write::create_spec(c, &app.lake, files).await?)?;
    let keyed = spec["key"].as_array().is_some_and(|k| !k.is_empty()) || spec["merge"].as_object().is_some_and(|m| !m.is_empty());
    ensure!(!keyed, "{name}: a temporary table keeps its rows as they come (PRIMARY KEY and merge are a lake table's)");
    let columns: Vec<(String, String)> = serde_json::from_value(spec["columns"].clone())?;
    let columns = columns.iter().map(|(n, t)| Ok((n.clone(), crate::query::type_name(&crate::write::stored(&crate::query::dtype(t)?))))).collect::<Result<Vec<_>>>()?;
    ensure!(columns.iter().all(|(c, _)| !crate::sys::NAMES.contains(&c.as_str())), "{}: system columns (every table has them)", crate::sys::NAMES.join(", "));
    with(s, true, |x| Ok(x.tables.insert(name.clone(), Table { columns, rows: vec![], next: 1, version: 0 })))?;
    let rows = match &c.query {
        Some(q) => insert(app, s, &name, None, &q.to_string(), files).await?["rows"].clone(),
        None => j!(0),
    };
    Ok(j!({"table": name, "temporary": true, "rows": rows}))
}

/// `INSERT INTO name [(columns)] query`.
async fn insert(app: &App, s: &str, name: &str, names: Option<&[String]>, query: &str, files: bool) -> Result<Value> {
    let columns = SESSIONS.lock().unwrap().get(s).and_then(|x| x.tables.get(name)).map(|t| t.columns.clone()).with_context(|| format!("no temporary table {name}"))?;
    let query = match names {
        Some(n) => {
            let all: Vec<String> = columns.iter().map(|(c, _)| c.clone()).collect();
            crate::write::rows_for(name, &all, if n.is_empty() { &all } else { n }, query, &|_| None)? // (no names: `VALUES (1, DEFAULT)`)
        }
        None => query.to_string(),
    };
    let ctx = crate::query::session(&app.lake, &query, "").await?;
    let ctx = if files { ctx.enable_url_table() } else { ctx };
    let batch = crate::write::rows(&ctx, &TableMeta { columns, ..Default::default() }, &query).await?;
    let n = batch.num_rows();
    apply(s, name, HashSet::new(), vec![batch])?;
    Ok(j!({"rows": n}))
}

/// UPDATE, DELETE or MERGE: what changes, worked out as for a lake table, applied here.
async fn change(app: &App, s: &str, name: &str, stmt: &Stmt) -> Result<Value> {
    let columns = SESSIONS.lock().unwrap().get(s).and_then(|x| x.tables.get(name)).map(|t| t.columns.clone()).with_context(|| format!("no temporary table {name}"))?;
    let meta = TableMeta { columns, ids: true, ..Default::default() };
    let (old, new) = crate::change::rows_of(&app.lake, name, &meta, stmt, app.lake.visible()).await?;
    let count = |b: &[RecordBatch]| b.iter().map(|b| b.num_rows()).sum::<usize>();
    let inserted: usize = new.iter().map(|b| b.column_by_name(crate::sys::ROW_ID).map_or(b.num_rows(), |c| c.null_count())).sum();
    let (replaced, added) = (count(&old), count(&new));
    let gone: HashSet<i64> = old.iter().filter_map(|b| b.column_by_name(crate::sys::ROW_ID)).flat_map(|c| c.as_primitive::<Int64Type>().iter().flatten().collect::<Vec<_>>()).collect();
    apply(s, name, gone, new)?;
    Ok(j!({"rows": replaced + inserted, "updated": added - inserted, "deleted": replaced + inserted - added, "inserted": inserted}))
}

/// The table without the rows `gone` names, and with `new` (the table's columns, and the ids a
/// changed row keeps): one change, within the memory temporary tables may take.
fn apply(s: &str, name: &str, gone: HashSet<i64>, new: Vec<RecordBatch>) -> Result<()> {
    static BUDGET: LazyLock<usize> = LazyLock::new(|| std::env::var("PONDRA_TEMP_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(1024usize) << 20);
    let others: usize = SESSIONS.lock().unwrap().iter().flat_map(|(k, x)| x.tables.iter().filter(move |(n, _)| k != s || *n != name)).map(|(_, t)| t.bytes()).sum();
    with(s, true, |x| {
        let t = x.tables.get_mut(name).with_context(|| format!("no temporary table {name}"))?;
        let now = crate::log::now_ms() as i64 * 1000;
        let mut rows = vec![];
        for b in &t.rows {
            let ids = b.column_by_name(crate::sys::ROW_ID).expect("a temporary table's row ids").as_primitive::<Int64Type>();
            let keep = datafusion::arrow::array::BooleanArray::from(ids.iter().map(|id| Some(!id.is_some_and(|i| gone.contains(&i)))).collect::<Vec<_>>());
            rows.push(datafusion::arrow::compute::filter_record_batch(b, &keep)?);
        }
        let schema = t.schema()?;
        let (version, n) = (t.version + 1, t.columns.len());
        for b in new.iter().filter(|b| b.num_rows() > 0) {
            let len = b.num_rows();
            let fresh = |c: Option<&ArrayRef>, next: &mut i64| -> Vec<i64> {
                let given = c.map(|c| c.as_primitive::<Int64Type>().clone());
                (0..len).map(|i| match &given {
                    Some(g) if g.is_valid(i) => g.value(i),
                    _ => (*next, *next += 1).0,
                }).collect()
            };
            let ids = fresh(b.column_by_name(crate::sys::ROW_ID), &mut t.next);
            let created = b.column_by_name(crate::sys::CREATED).map(|c| datafusion::arrow::compute::cast(c, &DataType::Timestamp(TimeUnit::Microsecond, None))).transpose()?;
            let created: Vec<i64> = (0..len).map(|i| created.as_ref().filter(|c| c.is_valid(i)).map_or(now, |c| c.as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>().value(i))).collect();
            let time = |v: Vec<i64>| -> ArrayRef { Arc::new(TimestampMicrosecondArray::from(v).with_timezone("UTC")) };
            let mut columns: Vec<ArrayRef> = b.columns()[..n].to_vec();
            columns.extend([Arc::new(Int64Array::from(ids)) as ArrayRef, Arc::new(Int64Array::from(vec![version; len])), time(created), time(vec![now; len])]);
            let columns = columns.iter().zip(schema.fields()).map(|(c, f)| datafusion::arrow::compute::cast(c, f.data_type())).collect::<Result<_, _>>()?; // (as the table's types)
            rows.push(RecordBatch::try_new(schema.clone(), columns)?);
        }
        let bytes: usize = rows.iter().map(|b| b.get_array_memory_size()).sum();
        ensure!(others + bytes <= *BUDGET, "temporary tables on this node would hold {} MB, past PONDRA_TEMP_MB ({} MB): a lake table holds more", (others + bytes) >> 20, *BUDGET >> 20);
        (t.rows, t.version) = (rows, version);
        Ok(())
    })
}
