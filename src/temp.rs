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
use crate::ddl::Ddl;
use crate::text::mentions;
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

/// The session a request names (`x-pondra-session`), or that of the caller whose procedure sent it. A name
/// that isn't 8 to 128 letters, digits, - or _ is refused and says so: it is never ignored, which would leave
/// the request in no session at all.
pub fn of(headers: &axum::http::HeaderMap) -> anyhow::Result<Option<String>> {
    let named = match headers.get("x-pondra-session") {
        None => None,
        Some(v) => Some(name(v)?),
    };
    let token = headers.get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    Ok(crate::auth::lent_session(token).or(named.map(|n| id(&n)))) // (a procedure's own client names one: its caller's wins)
}

/// A session name `x-pondra-session` carries (see `of`): the name, or the reason it is refused (22023).
fn name(v: &axum::http::HeaderValue) -> anyhow::Result<String> {
    const SAYS: &str = "x-pondra-session: 8 to 128 letters, digits, - or _";
    let Ok(s) = v.to_str() else {
        return Err(crate::codes::coded("22023", format!("{SAYS} (got a value that isn't text)")));
    };
    if !(8..=128).contains(&s.len()) {
        return Err(crate::codes::coded("22023", format!("{SAYS} (got {})", s.len())));
    }
    if !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        return Err(crate::codes::coded("22023", format!("{SAYS} (got a name with other characters)")));
    }
    Ok(s.to_string())
}

/// The session a client's id names: its user's (another who names the same id has a session of its own).
pub fn id(id: &str) -> String { format!("{}~{id}", crate::auth::current().map(|p| p.name).unwrap_or_default()) }

pub fn current() -> Option<String> { SESSION.try_with(|s| s.clone()).ok().flatten() }

/// A session for one script sent without one (`#`: no client names it so).
pub fn of_script() -> String { format!("{}#script-{}", crate::auth::current().map(|p| p.name).unwrap_or_default(), uuid::Uuid::new_v4()) }

/// The current session's temporary tables (name, definition) and views (name, SQL), as Postgres's
/// catalog lists them (`pg_catalog.rs`).
pub fn listed() -> (Vec<(String, TableMeta)>, Vec<(String, String)>) {
    let Some(s) = current() else { return Default::default() };
    let all = SESSIONS.lock().unwrap();
    let Some(x) = all.get(&s) else { return Default::default() };
    (x.tables.iter().map(|(n, t)| (n.clone(), t.meta.clone())).collect(), x.views.iter().map(|(n, v)| (n.clone(), v.clone())).collect())
}

/// The statement that makes one of the current session's temporary views or tables again, and its
/// kind (`SHOW CREATE`: the session's come first, as `pg_temp` is first on Postgres's path). A table's
/// is a lake table's, with its NOT NULL, DEFAULT and CHECK (`objects::table_sql`).
pub fn made(name: &str) -> Option<(&'static str, String)> {
    use crate::objects::{ident, table_sql};
    peek(&current()?, |x| match (x.views.get(name), x.tables.get(name)) {
        (Some(sql), _) => Some(("view", format!("CREATE TEMPORARY VIEW {} AS {sql}", ident(name)))),
        (None, Some(t)) => Some(("table", table_sql(&ident(name), &t.meta).replacen("CREATE TABLE", "CREATE TEMPORARY TABLE", 1))),
        _ => None,
    })
}

const NO_SESSION: &str = "a temporary table or view is a session's: a Postgres connection's, or the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>, 8 to 128 letters, digits, - or _)";

struct Table {
    meta: TableMeta, // its definition as a lake table's is (columns, PRIMARY KEY, NOT NULL, DEFAULT, CHECK, the typed columns): every write is checked as a lake table's is
    rules: Arc<crate::rules::Table>, // (its rules, made once at CREATE: every write's rows are shaped for them)
    rows: Vec<RecordBatch>, // its columns, then its system columns (`sys.rs`)
    next: i64,              // the next row id
    version: i64,           // the last change's number (its rows' `_version`)
}

/// A temporary table's rows' schema: its columns, then its system columns.
fn schema_of(meta: &TableMeta) -> Result<datafusion::arrow::datatypes::SchemaRef> { crate::answer::schema(&[meta.columns.clone(), crate::sys::columns()].concat()) }

impl Table {
    fn schema(&self) -> Result<datafusion::arrow::datatypes::SchemaRef> { schema_of(&self.meta) }
    fn bytes(&self) -> usize { self.rows.iter().map(|b| b.get_array_memory_size()).sum() }
}

#[derive(Default)]
pub struct Session {
    tables: HashMap<String, Table>,
    views: HashMap<String, String>,
    secrets: std::collections::BTreeMap<String, crate::ext::Secret>, // (CREATE TEMPORARY SECRET: in memory only)
    pub settings: std::collections::BTreeMap<String, String>,       // SET name = value (`settings.rs`)
    pub prepared: HashMap<String, String>,                          // PREPARE name AS …: its text
    pub variables: std::collections::BTreeMap<String, crate::vars::Var>, // DECLARE $name …, $name = … (`vars.rs`)
    pub currval: HashMap<String, i64>,                              // each sequence's last value its nextval gave (`seq.rs`)
    used: Option<Instant>,
    version: u64, // changes so far (`live.rs` watches them)
}

crate::per_node!(static SESSIONS: Mutex<HashMap<String, Session>> = Default::default());
static CHANGES: LazyLock<tokio::sync::watch::Sender<u64>> = LazyLock::new(|| tokio::sync::watch::channel(0).0);

/// Any session's temporary tables or views changed (`live.rs` waits on it).
pub fn changes() -> tokio::sync::watch::Receiver<u64> { CHANGES.subscribe() }

/// How many changes a session's temporary tables and views have had.
pub fn version(session: Option<&str>) -> u64 { session.and_then(|s| SESSIONS.lock().unwrap().get(s).map(|x| x.version)).unwrap_or(0) }

/// A session's tables, views and settings, made on first use; its changes counted.
pub fn with<T>(session: &str, change: bool, f: impl FnOnce(&mut Session) -> Result<T>) -> Result<T> {
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

/// Looks at a session's state without making it: a session that doesn't exist is None, and nothing is kept for it
/// (a read of a setting must not start a session, as `with` does).
pub fn peek<T>(session: &str, f: impl FnOnce(&Session) -> Option<T>) -> Option<T> {
    SESSIONS.lock().unwrap().get(session).and_then(f)
}

/// How long a session may sit unused (`PONDRA_SESSION_IDLE_SECS`, 3600).
fn idle() -> Duration {
    Duration::from_secs(crate::env::or("PONDRA_SESSION_IDLE_SECS", 3600))
}

/// Sessions idle for `PONDRA_SESSION_IDLE_SECS` end, as `end` ends them (a transaction rolled back,
/// its Python and temporary tables gone); once there are none, this stops.
async fn reap() {
    loop {
        tokio::time::sleep(idle().min(Duration::from_secs(60))).await;
        let stale: Vec<String> = SESSIONS.lock().unwrap().iter().filter(|(_, x)| !x.used.is_some_and(|u| u.elapsed() < idle())).map(|(s, _)| s.clone()).collect();
        for session in &stale {
            end(session);
        }
        if SESSIONS.lock().unwrap().is_empty() {
            return;
        }
    }
}

/// Does this node hold the session: made, used within the idle time, and not ended? One that has
/// gone idle ends here, as `reap` would end it (its transaction rolled back).
pub fn live(session: &str) -> bool {
    let fresh = SESSIONS.lock().unwrap().get(session).is_some_and(|x| x.used.is_some_and(|u| u.elapsed() < idle()));
    if !fresh {
        end(session);
    }
    fresh
}

/// The session was used just now: its idle time starts again.
pub fn touch(session: &str) {
    if let Some(x) = SESSIONS.lock().unwrap().get_mut(session) {
        x.used = Some(Instant::now());
    }
}

/// Does this node hold anything of the session's (temporary tables or views, settings,
/// variables, prepared statements, a transaction, its Python)? A client that fails over between
/// nodes keeps to this one while it does: `POST /sql` says so (`x-pondra-session: held`).
pub fn holds(session: &str) -> bool {
    let kept = SESSIONS.lock().unwrap().get(session).is_some_and(|x| {
        !(x.tables.is_empty() && x.views.is_empty() && x.secrets.is_empty() && x.settings.is_empty() && x.prepared.is_empty() && x.variables.is_empty())
    });
    kept || crate::txn::held(session) || crate::python::held(session)
}

/// The current session's temporary secrets (`CREATE TEMPORARY SECRET`), by name.
pub fn secrets() -> Vec<(String, crate::ext::Secret)> {
    let Some(s) = current() else { return vec![] };
    SESSIONS.lock().unwrap().get(&s).map(|x| x.secrets.iter().map(|(n, v)| (n.clone(), v.clone())).collect()).unwrap_or_default()
}

/// Does the session hold temporary secrets? Its queries then run on its node alone: the other nodes
/// don't have them, and would read its files with secrets of their own, or none (S6, S37).
pub fn holds_secrets() -> bool {
    let Some(s) = current() else { return false };
    SESSIONS.lock().unwrap().get(&s).is_some_and(|x| !x.secrets.is_empty())
}

/// End a session: its temporary tables and views are gone, and its Python (`python::ask_session`).
pub fn end(session: &str) -> bool {
    crate::txn::end(session); // (its transaction, if open: rolled back)
    crate::cursors::end(session); // (its cursors go with it: `cursors.rs`)
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
            Ddl::DropTable { name, made_by: None, .. } => has(&s, name, true), // (a failed CREATE TABLE … AS's drop is the lake's table, never a session's)
            Ddl::DropView { name, .. } => has(&s, name, false),
            Ddl::DropSecret { name, .. } => SESSIONS.lock().unwrap().get(&s).is_some_and(|x| x.secrets.contains_key(name)),
            _ => false,
        }),
        Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) | Stmt::Update(t, ..) | Stmt::Delete(t, _) => has(&s, t, true),
        Stmt::Merge(m) => has(&s, &m.target, true),
        _ => false,
    }
}

/// The columns of the current session's temporary table `name`, with the SQL type each holds as text, if there is one
/// (a node's `Host::relation`, in host.rs, reads them before a lake table of the name: a temporary table shadows it).
pub fn columns_of(name: &str) -> Option<Vec<(String, Option<String>)>> {
    peek(&current()?, |x| x.tables.get(name).map(|t| t.meta.columns.iter().map(|(c, _)| (c.clone(), t.meta.sql_types.get(c).cloned())).collect()))
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
        let keep: Vec<usize> = (0..t.meta.columns.len() + if sys { crate::text::NAMES.len() } else { 0 }).collect();
        let rows = t.rows.iter().map(|b| b.project(&keep)).collect::<Result<Vec<_>, _>>()?;
        ctx.deregister_table(name.as_str())?;
        ctx.register_table(name.as_str(), Arc::new(MemTable::try_new(Arc::new(t.schema()?.project(&keep)?), vec![rows])?))?;
    }
    Ok(())
}

/// The session's temporary views `sql` names, and those they name. They are copied out of `SESSIONS` and looked for after
/// it is let go, so one session's query never holds every other session's statements up.
pub fn views(sql: &str) -> Vec<(String, String)> {
    let Some(views) = current().and_then(|s| peek(&s, |x| (!x.views.is_empty()).then(|| x.views.clone()))) else { return vec![] };
    let (mut text, mut out) = (sql.to_string(), Vec::<(String, String)>::new());
    loop {
        let more: Vec<(String, String)> = views.iter().filter(|(n, _)| !out.iter().any(|(m, _)| m == *n) && mentions(&text, n)).map(|(n, v)| (n.clone(), v.clone())).collect();
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
        Stmt::Create(c) if c.temporary => {
            // (in an open transaction it runs at once too, as DDL does: `txn::temp_at_once` refuses REPEATABLE READ and says so)
            let note = crate::txn::temp_at_once()?;
            let out = create(app, &session.context(NO_SESSION)?, c, files).await?;
            if let (Some(n), false) = (note, out["unchanged"] == true) {
                crate::routines::heard(&n); // (IF NOT EXISTS left it as it was: nothing ran)
            }
            return Ok(Some(out));
        }
        Stmt::TempView(name, sql, replace, quiet) => {
            let s = session.context(NO_SESSION)?;
            ensure!(!name.contains('.'), "{name}: a temporary view's name has no schema (it is the session's)");
            if *quiet && has(&s, name, false) {
                return Ok(Some(j!({"view": name, "temporary": true, "exists": true}))); // (IF NOT EXISTS)
            }
            ensure!(*replace || !has(&s, name, false), "temporary view {name} exists (CREATE OR REPLACE TEMP VIEW replaces it)");
            crate::engine::sql(&crate::query::session(&app.lake, sql, "").await?, &crate::sys::planned(sql)?).await.with_context(|| format!("temporary view {name}"))?; // (it plans)
            with(&s, true, |x| Ok(x.views.insert(name.clone(), sql.clone())))?;
            return Ok(Some(j!({"view": name, "temporary": true})));
        }
        Stmt::TempSecret(name, params, replace, quiet) => {
            let s = session.context("a temporary secret is a session's: a Postgres connection's, or the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>, 8 to 128 letters, digits, - or _)")?;
            let secret = crate::ext::temporary(name, params.clone())?;
            let kept = with(&s, true, |x| {
                if *quiet && x.secrets.contains_key(name) {
                    return Ok(false); // (IF NOT EXISTS)
                }
                ensure!(*replace || !x.secrets.contains_key(name), "temporary secret {name} exists (CREATE OR REPLACE TEMPORARY SECRET replaces it)");
                x.secrets.insert(name.clone(), secret);
                Ok(true)
            })?;
            return Ok(Some(if kept { j!({"secret": name, "temporary": true}) } else { j!({"secret": name, "temporary": true, "exists": true}) }));
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
                Ddl::DropTable { name, made_by: None, .. } => x.tables.remove(name).map(|_| name.clone()),
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
    let name = crate::text::object(&c.name);
    ensure!(!name.contains('.'), "{name}: a temporary table's name has no schema (it is the session's)");
    ensure!(!(c.or_replace && c.if_not_exists), "CREATE OR REPLACE TEMP TABLE … IF NOT EXISTS: one or the other");
    if has(s, &name, true) && !c.or_replace {
        ensure!(c.if_not_exists, "temporary table {name} exists");
        return Ok(j!({"table": name, "temporary": true, "unchanged": true}));
    }
    let spec: Value = serde_json::from_str(&crate::write::create_spec(c, &app.lake, files).await?)?;
    let key: Vec<String> = serde_json::from_value(spec["key"].clone()).unwrap_or_default(); // (PRIMARY KEY: checked as rows come, `check_key`)
    ensure!(spec["merge"].as_object().is_none_or(|m| m.is_empty()), "{name}: a merge table is a lake table's (a temporary table keeps its rows as they come)");
    ensure!(spec["enums"].as_object().is_none_or(|e| e.is_empty()), "{name}: a temporary table's columns are plain types (an enum column is a lake table's)");
    if spec["constraints"].as_array().is_some_and(|k| k.iter().any(|k| k["enforced"] == true)) {
        return Err(crate::codes::coded("0A000", format!("{name}: a temporary table keeps its rows as they come (UNIQUE is a lake table's; NOT ENFORCED keeps none)")));
    }
    let columns: Vec<(String, String)> = serde_json::from_value(spec["columns"].clone())?;
    let columns = columns.iter().map(|(n, t)| Ok((n.clone(), crate::answer::type_name(&crate::answer::stored(&crate::answer::dtype(t)?))))).collect::<Result<Vec<_>>>()?;
    ensure!(columns.iter().all(|(c, _)| !crate::text::NAMES.contains(&c.as_str())), "{}: system columns (every table has them)", crate::text::NAMES.join(", "));
    let sql_types: std::collections::BTreeMap<String, String> = serde_json::from_value(spec["sql_types"].clone()).unwrap_or_default(); // (a uuid, JSON or VARCHAR(n) column: its values checked as a lake table's are)
    let not_null: Vec<String> = serde_json::from_value(spec["not_null"].clone()).unwrap_or_default();
    let defaults: std::collections::BTreeMap<String, String> = serde_json::from_value(spec["defaults"].clone()).unwrap_or_default();
    let checks: Vec<(String, String)> = serde_json::from_value(spec["checks"].clone()).unwrap_or_default();
    // (a lake table numbers these rows through their sequences; a temporary table's writes don't yet, so they are refused by name)
    if spec["identity"].as_object().is_some_and(|i| !i.is_empty()) {
        return Err(crate::codes::coded("0A000", format!("{name}: a temporary table's identity column isn't taken yet (a lake table's is; or give its values)")));
    }
    if defaults.values().any(|d| crate::seq::calls(d)) {
        return Err(crate::codes::coded("0A000", format!("{name}: a temporary table's DEFAULT nextval(…) isn't taken yet")));
    }
    crate::defaults::definition(&columns, &defaults, &checks).await.map_err(crate::write::definition)?; // (checked as CREATE TABLE checks them: 42P16 when wrong)
    let meta = TableMeta { columns, key, sql_types, not_null, defaults, checks, ids: true, ..Default::default() };
    let rules = Arc::new(meta.to_rules()?);
    with(s, true, |x| Ok(x.tables.insert(name.clone(), Table { meta, rules, rows: vec![], next: 1, version: 0 })))?;
    let rows = match &c.query {
        Some(q) => insert(app, s, &name, None, &q.to_string(), files).await?["rows"].clone(),
        None => j!(0),
    };
    Ok(j!({"table": name, "temporary": true, "rows": rows}))
}

/// `INSERT INTO name [(columns)] query`.
async fn insert(app: &App, s: &str, name: &str, names: Option<&[String]>, query: &str, files: bool) -> Result<Value> {
    let (meta, rules) = SESSIONS.lock().unwrap().get(s).and_then(|x| x.tables.get(name)).map(|t| (t.meta.clone(), t.rules.clone())).with_context(|| format!("no temporary table {name}"))?;
    let query = match names {
        Some(n) => {
            let all = crate::write::row_columns(meta.columns.iter().map(|(c, _)| c.clone()), n);
            let names = if n.is_empty() { crate::write::implicit(&all, crate::write::rows_width(query)) } else { n }; // (no names: `VALUES (1, DEFAULT)`, the columns its rows give)
            crate::write::rows_for(name, &all, names, query, &|c: &str| crate::defaults::sql_of(&meta, c))? // (a column left out takes its DEFAULT)
        }
        None => query.to_string(),
    };
    let ctx = crate::query::session(&app.lake, &query, "").await?;
    let ctx = if files { ctx.enable_url_table() } else { ctx };
    let rows = crate::write::rows(&app.lake, &ctx, &rules, name, &query, crate::rules::How::Load).await?; // (Load: its key is checked over all its rows, `check_key`: R6)
    let n = rows.rows().num_rows();
    apply(s, name, HashSet::new(), vec![rows])?;
    Ok(j!({"rows": n}))
}

/// UPDATE, DELETE or MERGE: what changes, worked out as for a lake table, shaped for the table's rules, applied here.
async fn change(app: &App, s: &str, name: &str, stmt: &Stmt) -> Result<Value> {
    let (meta, rules) = SESSIONS.lock().unwrap().get(s).and_then(|x| x.tables.get(name)).map(|t| (t.meta.clone(), t.rules.clone())).with_context(|| format!("no temporary table {name}"))?; // (its key: an ON CONFLICT without a column list is on it)
    let (old, new) = crate::change::rows_of(&app.lake, name, &meta, stmt, app.lake.visible()).await?;
    let count = |b: &[RecordBatch]| b.iter().map(|b| b.num_rows()).sum::<usize>();
    let inserted: usize = new.iter().map(|b| b.column_by_name(crate::text::ROW_ID).map_or(b.num_rows(), |c| c.null_count())).sum();
    let (replaced, added) = (count(&old), count(&new));
    let gone: HashSet<i64> = old.iter().filter_map(|b| b.column_by_name(crate::text::ROW_ID)).flat_map(|c| c.as_primitive::<Int64Type>().iter().flatten().collect::<Vec<_>>()).collect();
    let host = crate::defaults::Node(&app.lake);
    let mut shaped = Vec::with_capacity(new.len());
    for b in new {
        shaped.push(crate::rules::shape(&rules, name, crate::rules::Rows::Whole(b), crate::rules::How::Change, &host).await?);
    }
    apply(s, name, gone, shaped)?;
    Ok(j!({"rows": replaced + inserted, "updated": added - inserted, "deleted": replaced + inserted - added, "inserted": inserted}))
}

/// The table without the rows `gone` names, and with `new` (the table's columns, and the ids a
/// changed row keeps): one change, within the memory temporary tables may take. The rows are made and
/// checked outside the sessions' lock, so a big change never stalls another session; they are put in
/// place only if the table is still as it was read, else made again from what it is now.
fn apply(s: &str, name: &str, gone: HashSet<i64>, new: Vec<crate::rules::Shaped>) -> Result<()> {
    static BUDGET: LazyLock<usize> = LazyLock::new(|| crate::env::or("PONDRA_TEMP_MB", 1024usize) << 20);
    loop {
        let (meta, rules, old, next, version) = with(s, false, |x| {
            let t = x.tables.get(name).with_context(|| format!("no temporary table {name}"))?;
            Ok((t.meta.clone(), t.rules.clone(), t.rows.clone(), t.next, t.version))
        })?;
        let (rows, next) = changed(&meta, &rules, name, &old, &gone, &new, next, version + 1)?;
        let others: usize = SESSIONS.lock().unwrap().iter().flat_map(|(k, x)| x.tables.iter().filter(move |(n, _)| k != s || *n != name)).map(|(_, t)| t.bytes()).sum();
        let bytes: usize = rows.iter().map(|b| b.get_array_memory_size()).sum();
        ensure!(others + bytes <= *BUDGET, "temporary tables on this node would hold {} MB, past PONDRA_TEMP_MB ({} MB): a lake table holds more", (others + bytes) >> 20, *BUDGET >> 20);
        let put = with(s, true, |x| {
            let t = x.tables.get_mut(name).with_context(|| format!("no temporary table {name}"))?;
            if t.version != version {
                return Ok(false);
            }
            (t.rows, t.next, t.version) = (rows, next, version + 1);
            Ok(true)
        })?;
        if put {
            return Ok(());
        }
    }
}

/// `old` without the rows `gone` names, and with `new` as change `version`, each new row given its id (from
/// `next` unless it keeps one) and cast to its table's types: the rows, and the next row id. `new` come shaped by `rules`
/// (`rules::shape` checked them), so they are not checked again unless the table's rules are not the ones they were shaped for.
#[allow(clippy::too_many_arguments)]
fn changed(meta: &TableMeta, rules: &Arc<crate::rules::Table>, name: &str, old: &[RecordBatch], gone: &HashSet<i64>, new: &[crate::rules::Shaped], mut next: i64, version: i64) -> Result<(Vec<RecordBatch>, i64)> {
    let now = crate::log::now_ms() as i64 * 1000;
    let mut rows = vec![];
    for b in old {
        let ids = b.column_by_name(crate::text::ROW_ID).expect("a temporary table's row ids").as_primitive::<Int64Type>();
        let keep = datafusion::arrow::array::BooleanArray::from(ids.iter().map(|id| Some(!id.is_some_and(|i| gone.contains(&i)))).collect::<Vec<_>>());
        rows.push(datafusion::arrow::compute::filter_record_batch(b, &keep)?);
    }
    let (schema, n) = (schema_of(meta)?, meta.columns.len());
    for s in new.iter().filter(|s| s.rows().num_rows() > 0) {
        // (shaped for the table as it is: `shape` checked them. Rows shaped for a definition it no longer has, made again
        // meanwhile, are checked again, as the log checks them: R9)
        let b = match crate::rules::Table::same(s.table(), rules) {
            true => s.rows().clone(),
            false => crate::rules::checked(rules, name, s.rows().clone(), true, crate::defaults::base())?,
        };
        let len = b.num_rows();
        let fresh = |c: Option<&ArrayRef>, next: &mut i64| -> Vec<i64> {
            let given = c.map(|c| c.as_primitive::<Int64Type>().clone());
            (0..len).map(|i| match &given {
                Some(g) if g.is_valid(i) => g.value(i),
                _ => (*next, *next += 1).0,
            }).collect()
        };
        let ids = fresh(b.column_by_name(crate::text::ROW_ID), &mut next);
        let created = b.column_by_name(crate::text::CREATED).map(|c| datafusion::arrow::compute::cast(c, &DataType::Timestamp(TimeUnit::Microsecond, None))).transpose()?;
        let created: Vec<i64> = (0..len).map(|i| created.as_ref().filter(|c| c.is_valid(i)).map_or(now, |c| c.as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>().value(i))).collect();
        let time = |v: Vec<i64>| -> ArrayRef { Arc::new(TimestampMicrosecondArray::from(v).with_timezone("UTC")) };
        let mut columns: Vec<ArrayRef> = b.columns()[..n].to_vec();
        columns.extend([Arc::new(Int64Array::from(ids)) as ArrayRef, Arc::new(Int64Array::from(vec![version; len])), time(created), time(vec![now; len])]);
        let columns = columns.iter().zip(schema.fields()).map(|(c, f)| datafusion::arrow::compute::cast(c, f.data_type())).collect::<Result<_, _>>()?; // (as the table's types)
        rows.push(RecordBatch::try_new(schema.clone(), columns)?);
    }
    check_key(name, &meta.columns, &meta.key, &rows)?;
    Ok((rows, next))
}

/// A temporary table's PRIMARY KEY, checked over every row as a change leaves them: no NULL in a key
/// column (23502), and no two rows with the same key, within one INSERT too (23505, named `{name}_pkey`
/// as Postgres names it).
fn check_key(name: &str, columns: &[(String, String)], key: &[String], rows: &[RecordBatch]) -> Result<()> {
    if key.is_empty() {
        return Ok(());
    }
    let at: Vec<usize> = key.iter().map(|k| columns.iter().position(|(c, _)| c == k).with_context(|| format!("key column {k}"))).collect::<Result<_>>()?;
    let mut seen = HashSet::new();
    for b in rows {
        for r in 0..b.num_rows() {
            let mut values = Vec::with_capacity(at.len());
            for (k, &i) in key.iter().zip(&at) {
                let c = b.column(i);
                if c.is_null(r) {
                    return Err(crate::codes::coded("23502", format!("null value in column \"{k}\" of relation \"{name}\" violates not-null constraint")));
                }
                values.push(datafusion::arrow::util::display::array_value_to_string(c, r)?);
            }
            if !seen.insert(values.clone()) {
                return Err(crate::codes::coded("23505", format!("duplicate key value violates unique constraint \"{name}_pkey\": Key ({})=({}) already exists", key.join(", "), values.join(", "))));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_temporary_table_keeps_its_types_for_its_writes_and_its_shape() {
        // (the session's state is set directly: `with` starts the reaper, which needs a runtime a plain test doesn't have)
        let s = "unit-temp-types";
        let meta = TableMeta {
            columns: vec![("u".into(), "Utf8".into()), ("v".into(), "Utf8".into()), ("n".into(), "Int64".into())],
            sql_types: std::collections::BTreeMap::from([("u".to_string(), "uuid".to_string()), ("v".to_string(), "varchar(3)".to_string())]),
            ids: true,
            ..Default::default()
        };
        let table = Table { rules: Arc::new(meta.to_rules().unwrap()), meta, rows: vec![], next: 1, version: 0 };
        SESSIONS.lock().unwrap().entry(s.to_string()).or_default().tables.insert("tt".to_string(), table);
        SESSION.sync_scope(Some(s.to_string()), || {
            assert_eq!(columns_of("tt"), Some(vec![("u".to_string(), Some("uuid".to_string())), ("v".to_string(), Some("varchar(3)".to_string())), ("n".to_string(), None)]));
            assert_eq!(columns_of("other"), None, "a name with no temporary table of the session's is left to the lake");
            assert_eq!(made("tt"), Some(("table", "CREATE TEMPORARY TABLE tt (\n  u UUID,\n  v VARCHAR(3),\n  n BIGINT\n)".to_string())), "SHOW CREATE gives each column's own type");
        });
        SESSIONS.lock().unwrap().remove(s);
    }

    /// The tests' host: CHECKs planned in the node's base session, and no DEFAULT to fill.
    struct Checks;

    impl crate::rules::Host for Checks {
        fn base(&self) -> &datafusion::prelude::SessionContext { crate::defaults::base() }
        async fn values(&self, _: &str, _: usize) -> Result<ArrayRef> { anyhow::bail!("no DEFAULT in these tests") }
    }

    /// Rows of two Int32 columns, `a` and `c`, as a write gives them.
    fn ac_batch(a: Vec<Option<i32>>, c: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("a", DataType::Int32, true),
            datafusion::arrow::datatypes::Field::new("c", DataType::Int32, true),
        ]));
        let columns: Vec<ArrayRef> = vec![Arc::new(datafusion::arrow::array::Int32Array::from(a)), Arc::new(datafusion::arrow::array::Int32Array::from(c))];
        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[tokio::test]
    async fn a_temporary_table_refuses_what_its_not_null_and_checks_refuse() {
        // (its writes are shaped as a lake table's are: `rules::shape` checks each batch before `apply` puts it in place)
        let s = "unit-temp-checks";
        let meta = TableMeta {
            columns: vec![("a".into(), "Int32".into()), ("c".into(), "Int32".into())],
            not_null: vec!["a".into()],
            checks: vec![("tt_c_check".into(), "c > 0".into())],
            ids: true,
            ..Default::default()
        };
        let rules = Arc::new(meta.to_rules().unwrap());
        SESSIONS.lock().unwrap().entry(s.to_string()).or_default().tables.insert("tt".to_string(), Table { rules: rules.clone(), meta, rows: vec![], next: 1, version: 0 });
        let null_a = crate::rules::shape(&rules, "tt", crate::rules::Rows::Whole(ac_batch(vec![None], vec![1])), crate::rules::How::Change, &Checks).await.err().expect("a NULL in NOT NULL");
        assert!(format!("{null_a:#}").contains("tt.a is NOT NULL"), "{null_a:#}");
        let bad_c = crate::rules::shape(&rules, "tt", crate::rules::Rows::Whole(ac_batch(vec![Some(1)], vec![-5])), crate::rules::How::Change, &Checks).await.err().expect("a CHECK");
        assert!(format!("{bad_c:#}").contains("violates check constraint \"tt_c_check\""), "{bad_c:#}");
        let good = crate::rules::shape(&rules, "tt", crate::rules::Rows::Whole(ac_batch(vec![Some(1), Some(2)], vec![5, 6])), crate::rules::How::Change, &Checks).await.unwrap();
        apply(s, "tt", HashSet::new(), vec![good]).unwrap();
        let kept: usize = SESSIONS.lock().unwrap()[s].tables["tt"].rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(kept, 2, "the refused writes left nothing; the good rows are there");
        SESSIONS.lock().unwrap().remove(s);
    }

    #[tokio::test]
    async fn rows_shaped_for_another_definition_are_checked_again() {
        // (rows shaped for a definition the table no longer has are checked again, as the log checks its rows: R9)
        let s = "unit-temp-redefined";
        let meta = TableMeta {
            columns: vec![("a".into(), "Int32".into()), ("c".into(), "Int32".into())],
            not_null: vec!["a".into()],
            checks: vec![("tt_c_check".into(), "c > 0".into())],
            ids: true,
            ..Default::default()
        };
        let mut loose = meta.clone();
        loose.checks.clear();
        let shaped = crate::rules::shape(&Arc::new(loose.to_rules().unwrap()), "tt", crate::rules::Rows::Whole(ac_batch(vec![Some(1)], vec![-5])), crate::rules::How::Change, &Checks).await.unwrap();
        SESSIONS.lock().unwrap().entry(s.to_string()).or_default().tables.insert("tt".to_string(), Table { rules: Arc::new(meta.to_rules().unwrap()), meta, rows: vec![], next: 1, version: 0 });
        let refused = apply(s, "tt", HashSet::new(), vec![shaped]).unwrap_err();
        assert!(format!("{refused:#}").contains("violates check constraint \"tt_c_check\""), "{refused:#}");
        let kept: usize = SESSIONS.lock().unwrap()[s].tables["tt"].rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(kept, 0, "nothing is kept");
        SESSIONS.lock().unwrap().remove(s);
    }
}
