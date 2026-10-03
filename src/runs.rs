//! What procedures did, and what runs on a schedule (ADR-027).
//!
//! - **The run log** (`pondra.runs`): a row per procedure call — its routine, caller, node, job,
//!   arguments, start and end, status, notices and error — in a keyed table of the lake's own
//!   (`pondra$runs`, hidden), so every node's calls are there, and outlive the nodes. A call's row
//!   is written as it starts (`running`) and again as it ends, by a writer on each node that sends
//!   them in batches: a call never waits for its log. Rows go after `PONDRA_RUNS_DAYS` (30).
//! - **Tasks**: `CREATE TASK nightly SCHEDULE 'cron 0 2 * * * UTC' AS CALL report(current_date -
//!   1)`, or `SCHEDULE '5 minutes'`. The leader runs each tick once: it commits the tick it is
//!   about to run (`jt/`), then runs it with the job `task:{name}:{tick}`, then marks it done. A
//!   new leader finding a tick claimed but not done runs it again with the same job, so its writes
//!   land once; ticks missed while no node led are run once, as the latest of them.
//! - **Task graphs** (ADR-045): `CREATE TASK load AFTER nightly WHEN … WITH (retries = 2) AS …`. A
//!   task with `AFTER` has no schedule: it runs once every task it follows has ended well in the
//!   same run of their graph (the first task's tick, which its tick is too), claimed and marked done
//!   as a scheduled one is. `WHEN` false is a run `skipped`, which counts as done; `RETURN`'s value
//!   is the run's result (`pondra.result('load')`); `EXECUTE TASK nightly (day => …)` claims a tick
//!   now, its values every task's in the graph; a suspended task takes no tick of its own.
//! - **`pondra.routines`, `pondra.tasks`**: the catalog's functions, procedures and tasks, as
//!   tables (`SHOW FUNCTIONS`, `SHOW PROCEDURES`, `SHOW TASKS` read them).
use crate::auth::Role;
use crate::routines::{Kind, Outcome, Routine, Who};
use crate::server::App;
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use chrono::{Datelike, TimeZone, Timelike};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::sql::sqlparser::{keywords::Keyword, parser::Parser, tokenizer::Token};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Duration;

pub const TABLE: &str = "pondra$runs";

fn now_ms() -> u64 { crate::log::now_ms() }

pub fn new_id() -> String { uuid::Uuid::new_v4().simple().to_string() }

/// The run log's columns (a keyed table: a run's newest row is its row).
fn columns() -> Vec<(String, String)> {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    let ts = crate::query::type_name(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
    let text = |n: &str| (n.to_string(), "Utf8".to_string());
    vec![text("id"), text("routine"), text("caller"), text("node"), text("job"), text("args"), ("started".into(), ts.clone()), ("ended".into(), ts), text("status"), text("notices"), text("error"), ("_deleted".into(), "Boolean".into())]
}

/// Leader: the run log's table, made the first time a node has a line for it (`Ddl::RunLog`).
pub async fn create_log(lake: &Lake) -> Result<serde_json::Value> {
    if lake.cat.get::<TableMeta>(&table_key(TABLE)).await?.is_none() {
        let days: u64 = std::env::var("PONDRA_RUNS_DAYS").ok().and_then(|d| d.parse().ok()).unwrap_or(30);
        let meta = TableMeta { columns: columns(), key: vec!["id".into()], ttl: Some(("started".into(), days * 86400)), tiered: lake.visible(), ids: true, ..Default::default() };
        lake.cat.commit(vec![(table_key(TABLE), json(&meta))], &[]).await?;
    }
    Ok(serde_json::json!({"table": "pondra.runs"}))
}

tokio::task_local! {
    /// Who a call is made for, as the run log says it, when not a token's role: `task:nightly`.
    pub static CALLER: String;
}

/// One call, as the run log has it.
#[derive(Clone)]
struct Line {
    id: String,
    routine: String,
    caller: String,
    node: String,
    job: Option<String>,
    args: String,
    started: u64,
    ended: Option<u64>,
    status: &'static str,
    notices: Option<String>,
    error: Option<String>,
}

/// A call being logged: its line, written as it starts and again as it ends.
pub struct Run(Line);

impl Run {
    pub fn start(app: &App, routine: &str, role: Role, job: Option<&str>, args: &RecordBatch, id: Option<String>) -> Run {
        Run::begin(app, routine, role, job, args_text(args), id)
    }

    /// A call whose arguments are said as they are: a DO block's `{"language": …, "code": …}`.
    pub fn begin(app: &App, routine: &str, role: Role, job: Option<&str>, args: String, id: Option<String>) -> Run {
        let caller = CALLER.try_with(|c| c.clone()).unwrap_or_else(|_| format!("{role:?}").to_lowercase());
        let line = Line { id: id.unwrap_or_else(new_id), routine: routine.into(), caller, node: app.cluster.addr.clone(), job: job.map(String::from), args: cut(args), started: now_ms(), ended: None, status: "running", notices: None, error: None };
        log(app, line.clone(), None);
        Run(line)
    }

    /// The call's line, ended; the answer says when it is in the log (a task waits for it before
    /// its tick is done, so a tick that wrote is never missing from the log).
    pub fn end(self, app: &App, out: &Result<Outcome>, notices: Vec<String>) -> tokio::sync::oneshot::Receiver<()> {
        self.ended(app, if out.is_ok() { "ok" } else { "failed" }, out.as_ref().err().map(|e| format!("{e:#}")), notices)
    }

    /// The call's line, ended as `status` says (a task's run may be `skipped`).
    fn ended(self, app: &App, status: &'static str, error: Option<String>, notices: Vec<String>) -> tokio::sync::oneshot::Receiver<()> {
        let mut line = self.0;
        (line.ended, line.status) = (Some(now_ms()), status);
        line.error = error.map(cut);
        line.notices = (!notices.is_empty()).then(|| cut(notices.join("\n")));
        let (done, written) = tokio::sync::oneshot::channel();
        log(app, line, Some(done));
        written
    }
}

/// At most 64 KB of it (the rest is in the node's log).
fn cut(mut s: String) -> String {
    if s.len() > 65536 {
        let mut at = 65536;
        while !s.is_char_boundary(at) {
            at -= 1;
        }
        s.truncate(at);
        s.push('…');
    }
    s
}

/// The arguments as JSON: `{"day": "2026-09-27", "to": ["ann@example.com"]}`.
fn args_text(args: &RecordBatch) -> String {
    if args.num_columns() == 0 {
        return "{}".into();
    }
    let mut w = datafusion::arrow::json::ArrayWriter::new(Vec::new());
    let text = w.write_batches(&[args]).and_then(|_| w.finish()).ok().map(|_| String::from_utf8_lossy(&w.into_inner()).to_string());
    text.map(|t| t.trim_start_matches('[').trim_end_matches(']').to_string()).unwrap_or_default()
}

type Sent = (Line, Option<tokio::sync::oneshot::Sender<()>>);

static WRITER: OnceLock<tokio::sync::mpsc::UnboundedSender<Sent>> = OnceLock::new();

fn log(app: &App, line: Line, done: Option<tokio::sync::oneshot::Sender<()>>) {
    if app.log.is_none() {
        return; // (a read-only node writes nothing)
    }
    let tx = WRITER.get_or_init(|| {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        crate::panics::spawn(write(app.clone(), rx));
        tx
    });
    let _ = tx.send((line, done));
}

/// This node's lines are in the log (`wait` looks again).
static WRITTEN: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Wait for run `id` to end (`AWAIT 'id'`, the id `pondra.start` gave): its error if it failed, or
/// if its node stopped under it. Looked for again as this node writes its lines, and every second
/// for another node's; a run not in the log after 10 s is no run.
pub async fn wait(app: &App, id: &str) -> Result<()> {
    let sql = format!("SELECT status, error FROM pondra.runs WHERE id = '{}'", id.replace('\'', "''"));
    let since = std::time::Instant::now();
    loop {
        let written = WRITTEN.notified(); // (woken by any write from here on)
        let rows = Box::pin(app.query(&sql, Some("0"))).await?;
        if let Some(b) = rows.iter().find(|b| b.num_rows() > 0) {
            let text = |i: usize| -> Result<Option<String>> {
                let c = datafusion::arrow::compute::cast(b.column(i), &datafusion::arrow::datatypes::DataType::Utf8)?;
                let c = c.as_any().downcast_ref::<StringArray>().context("text")?;
                Ok(c.is_valid(0).then(|| c.value(0).to_string()))
            };
            match (text(0)?.unwrap_or_default().as_str(), text(1)?) {
                ("ok", _) => return Ok(()),
                ("running", _) => {}
                ("stopped", _) => bail!("run {id} stopped: its node stopped under it"),
                (status, error) => bail!("run {id} {status}: {}", error.unwrap_or_default()),
            }
        } else if since.elapsed() > Duration::from_secs(10) {
            bail!("no run {id}: AWAIT takes a handle ($h = ASYNC …) or a run's id (pondra.start)");
        }
        tokio::select! {
            _ = written => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

/// This node's log writer: what came in a moment, one row per run (its newest), appended exactly
/// once (one producer, a seq per batch, retried as it was).
async fn write(app: App, mut rx: tokio::sync::mpsc::UnboundedReceiver<Sent>) {
    let producer = format!("runs-{}", new_id());
    let (mut seq, mut made) = (0, false);
    while let Some((first, done)) = rx.recv().await {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (mut lines, mut dones) = (vec![first], vec![done]);
        while let Ok((l, done)) = rx.try_recv() {
            lines.retain(|x: &Line| x.id != l.id);
            lines.push(l);
            dones.push(done);
        }
        seq += 1;
        for attempt in 0.. {
            match append(&app, &producer, seq, &lines, &mut made).await {
                Ok(()) => {
                    dones.into_iter().flatten().for_each(|d| { let _ = d.send(()); });
                    WRITTEN.notify_waiters();
                    break;
                }
                Err(e) if attempt >= 30 => {
                    eprintln!("the run log lost {} rows: {e:#}", lines.len());
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    }
}

async fn append(app: &App, producer: &str, seq: u64, lines: &[Line], made: &mut bool) -> Result<()> {
    if !*made && app.lake.cat.get::<TableMeta>(&table_key(TABLE)).await?.is_none() {
        crate::write::on_node_as(app, crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::RunLog]), None, false).await?;
    }
    *made = true;
    let schema = crate::query::schema(&columns())?;
    let text = |f: &dyn Fn(&Line) -> Option<String>| Arc::new(lines.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let time = |f: &dyn Fn(&Line) -> Option<u64>| Arc::new(lines.iter().map(|l| f(l).map(|ms| ms as i64 * 1000)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef;
    let columns = vec![
        text(&|l| Some(l.id.clone())),
        text(&|l| Some(l.routine.clone())),
        text(&|l| Some(l.caller.clone())),
        text(&|l| Some(l.node.clone())),
        text(&|l| l.job.clone()),
        text(&|l| Some(l.args.clone())),
        time(&|l| Some(l.started)),
        time(&|l| l.ended),
        text(&|l| Some(l.status.to_string())),
        text(&|l| l.notices.clone()),
        text(&|l| l.error.clone()),
        Arc::new(datafusion::arrow::array::BooleanArray::from(vec![None; lines.len()])) as ArrayRef,
    ];
    let batch = RecordBatch::try_new(schema, columns)?;
    app.log()?.append(TABLE.into(), crate::log::Src { producer: producer.into(), seq, prev: None }, batch).await?;
    Ok(())
}

/// Runs whose node stopped under them (killed, restarted, gone from the cluster) are marked
/// `stopped`, so none says `running` for good: each node marks its own address's from before it
/// started, once it serves; the leader, every 30 s, those of nodes that left the cluster a minute
/// ago or more. (A run that ends after all writes its row again: the newest row is its row.)
pub fn mark_stopped(app: App) {
    crate::panics::spawn(async move {
        let since = now_ms();
        for round in 0.. {
            if round == 0 || app.cluster.is_leader() {
                if let Err(e) = stopped(&app, since, round == 0).await {
                    eprintln!("runs whose node stopped: {e:#}");
                }
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });
}

async fn stopped(app: &App, since: u64, own_only: bool) -> Result<()> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::TimestampMicrosecondType;
    if app.log.is_none() {
        return Ok(()); // (a read-only node writes nothing)
    }
    let live = app.cluster.nodes();
    let batches = app.query("SELECT id, routine, caller, node, job, args, started FROM pondra.runs WHERE status = 'running'", None).await?;
    for b in &batches {
        let text = |i: usize| datafusion::arrow::compute::cast(b.column(i), &datafusion::arrow::datatypes::DataType::Utf8);
        let (id, routine, caller, node, job, args) = (text(0)?, text(1)?, text(2)?, text(3)?, text(4)?, text(5)?);
        let (id, routine, caller, node, job, args) = (id.as_string::<i32>(), routine.as_string::<i32>(), caller.as_string::<i32>(), node.as_string::<i32>(), job.as_string::<i32>(), args.as_string::<i32>());
        let started = b.column(6).as_primitive::<TimestampMicrosecondType>();
        for r in 0..b.num_rows() {
            let (at, by) = ((started.value(r) / 1000) as u64, node.value(r));
            let gone = match by == app.cluster.addr {
                true => at < since, // (this address's, from before this node started)
                false => !own_only && !live.iter().any(|n| n == by) && now_ms().saturating_sub(at) > 60_000,
            };
            if gone {
                let why = format!("its node, {by}, stopped while it ran");
                log(app, Line { id: id.value(r).into(), routine: routine.value(r).into(), caller: caller.value(r).into(), node: by.into(), job: job.is_valid(r).then(|| job.value(r).into()),
                    args: args.value(r).into(), started: at, ended: Some(now_ms()), status: "stopped", notices: None, error: Some(why) }, None);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- tasks

/// A statement run on a schedule, as the catalog keeps it (`j/`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Task {
    /// When it runs (empty for a task that runs `after` others).
    pub schedule: String,
    pub sql: String,
    #[serde(default)]
    pub created_ms: u64,
    /// The tasks it runs after, in the same run of their graph.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    /// Checked before it runs: false, and the run is `skipped`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    #[serde(default, skip_serializing_if = "TaskOptions::plain")]
    pub with: TaskOptions,
    /// `ALTER TASK … SUSPEND`: no tick of its own until `RESUME` (`EXECUTE TASK` still runs it).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspended: bool,
}

/// `WITH (retries = 2, retry_delay = '1 minute', timeout = '1 hour', on_failure = notify)`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct TaskOptions {
    /// Runs again after a failure, with the same job (what it wrote lands once).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub retries: u64,
    /// Seconds between tries (10 by default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_delay: Option<u64>,
    /// Seconds a try may take.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    /// A procedure called with the task's name and its error when it fails for good.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<String>,
}

fn is_zero(n: &u64) -> bool { *n == 0 }

impl TaskOptions {
    fn plain(&self) -> bool { *self == TaskOptions::default() }
}

/// The tick a task last took on (a scheduled task's own; a following task's, its graph's), whether
/// it finished, and how (`jt/`).
#[derive(Serialize, Deserialize, Clone, Default)]
struct Tick {
    at: u64,
    done: bool,
    /// ok, failed or skipped (None: ok, from before graphs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    /// `RETURN`'s value, as SQL (`pondra.result`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<String>,
    /// `EXECUTE TASK … (day => …)`'s values, the graph's.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    params: HashMap<String, serde_json::Value>,
}

impl Tick {
    /// Ended, and well enough for what follows it.
    fn passed(&self) -> bool { self.done && matches!(self.status.as_deref(), None | Some("ok" | "skipped")) }
}

pub fn task_key(name: &str) -> String { format!("j/{name}") }
fn tick_key(name: &str) -> String { format!("jt/{name}") }

pub const USAGE: &str = "CREATE TASK name SCHEDULE 'cron 0 2 * * * UTC' | '5 minutes' | AFTER task, … [WHEN condition] [WITH (retries = 2, retry_delay = '1 minute', timeout = '1 hour', on_failure = procedure)] AS statement";

/// The rest of `CREATE TASK` (`sql`: the whole statement): its name, `SCHEDULE [=] '…'` or `AFTER
/// a, b`, `WHEN cond`, `WITH (…)` and `AS` what it runs, as written (a script too).
pub fn task(p: &mut Parser, sql: &str) -> Result<(String, Task)> {
    let _ = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = crate::write::object(&p.parse_object_name(false)?);
    let mut task = Task::default();
    if crate::routines::word(p, "schedule") {
        let _ = p.consume_token(&Token::Eq);
        task.schedule = crate::routines::text_of(p)?.context("the schedule is a string: SCHEDULE '5 minutes'")?;
        every(&task.schedule)?;
    } else if crate::routines::word(p, "after") {
        loop {
            task.after.push(crate::write::object(&p.parse_object_name(false)?));
            if !p.consume_token(&Token::Comma) {
                break;
            }
        }
    } else {
        bail!("SCHEDULE '…' or AFTER task is missing");
    }
    if p.parse_keyword(Keyword::WHEN) {
        task.when = Some(p.parse_expr()?.to_string());
    }
    if p.parse_keyword(Keyword::WITH) {
        task.with = task_options(p)?;
    }
    ensure!(p.parse_keyword(Keyword::AS), "AS statement is missing");
    let body = sql[offset(sql, p.peek_token().span.start)..].trim();
    task.sql = body.strip_suffix(';').unwrap_or(body).trim_end().to_string();
    ensure!(!task.sql.is_empty(), "AS statement is missing");
    Ok((name, task))
}


/// Where a token starts in `sql` (sqlparser counts lines and characters from 1).
pub(crate) fn offset(sql: &str, at: datafusion::sql::sqlparser::tokenizer::Location) -> usize {
    let mut from = 0;
    for (n, line) in sql.split_inclusive('\n').enumerate() {
        if n + 1 == at.line as usize {
            return from + line.char_indices().nth((at.column as usize).saturating_sub(1)).map_or(line.len(), |(i, _)| i);
        }
        from += line.len();
    }
    sql.len()
}

/// A task's `WITH (…)`: each option by name; an unknown one refused.
fn task_options(p: &mut Parser) -> Result<TaskOptions> {
    let mut o = TaskOptions::default();
    p.expect_token(&Token::LParen)?;
    loop {
        let k = p.parse_identifier()?.value.to_lowercase();
        p.expect_token(&Token::Eq)?;
        let v = match p.next_token().token {
            Token::SingleQuotedString(s) | Token::Number(s, _) => s,
            Token::Word(w) => w.value,
            t => bail!("{k}: a value, not {t}"),
        };
        let secs = |v: &str| -> Result<u64> {
            match v.parse::<u64>() {
                Ok(n) => Ok(n),
                Err(_) => match every(v)? {
                    Every::Seconds(s) => Ok(s),
                    Every::Cron(..) => bail!("{k}: how long ('10 minutes', or seconds), not a schedule"),
                },
            }
        };
        match k.as_str() {
            "retries" => o.retries = v.parse().with_context(|| format!("retries: how many times, not {v:?}"))?,
            "retry_delay" => o.retry_delay = Some(secs(&v)?),
            "timeout" => o.timeout = Some(secs(&v)?.max(1)),
            "on_failure" => o.on_failure = Some(v),
            _ => bail!("WITH ({k} …): retries, retry_delay, timeout or on_failure"),
        }
        if p.consume_token(&Token::RParen) {
            return Ok(o);
        }
        p.expect_token(&Token::Comma)?;
    }
}

/// `EXECUTE TASK name [(day => value, …)]`: its name and the values' SQL, if `sql` is one.
pub fn execute_of(sql: &str) -> Option<(String, Vec<(String, String)>)> {
    static HEAD: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?is)^\s*execute\s+task\b").expect("a regex"));
    if !HEAD.is_match(crate::write::first_word(sql)) {
        return None;
    }
    let mut p = Parser::new(&datafusion::sql::sqlparser::dialect::GenericDialect {}).try_with_sql(crate::write::first_word(sql)).ok()?;
    p.next_token();
    p.next_token();
    let name = crate::write::object(&p.parse_object_name(false).ok()?);
    let mut args = vec![];
    if p.consume_token(&Token::LParen) && !p.consume_token(&Token::RParen) {
        loop {
            let k = p.parse_identifier().ok()?.value.to_lowercase();
            let _ = p.consume_token(&Token::RArrow) || p.consume_token(&Token::Assignment) || p.consume_token(&Token::Eq);
            args.push((k, p.parse_expr().ok()?.to_string()));
            if p.consume_token(&Token::RParen) {
                break;
            }
            p.expect_token(&Token::Comma).ok()?;
        }
    }
    let _ = p.consume_token(&Token::SemiColon);
    matches!(p.peek_token().token, Token::EOF).then_some((name, args))
}

/// `EXECUTE TASK name (…)`: its values worked out once, here, as the caller, then the leader
/// claims a tick for it now (`Ddl::ExecuteTask`).
pub async fn execute(app: &App, name: String, args: Vec<(String, String)>, who: Who) -> Result<Outcome> {
    let mut params = HashMap::new();
    if !args.is_empty() {
        let select = args.iter().map(|(k, v)| format!("({v}) AS \"{k}\"")).collect::<Vec<_>>().join(", ");
        let rows = Box::pin(app.query(&format!("SELECT {select}"), Some("0"))).await?;
        let row = datafusion::arrow::compute::concat_batches(&rows[0].schema(), &rows)?;
        params = crate::routines::values_of(&row)?;
    }
    let stmt = crate::write::Stmt::Ddl(vec![crate::ddl::Ddl::ExecuteTask { name, params }]);
    app.auth.allows(who.role, &stmt)?;
    Ok(Outcome::Done(crate::write::on_node_as(app, stmt, None, who.files).await?))
}

/// Leader: claim a tick now for `EXECUTE TASK` (the scheduler runs it, and what follows it).
pub async fn execute_task(lake: &Lake, name: &str, params: HashMap<String, serde_json::Value>) -> Result<serde_json::Value> {
    let name = crate::ddl::local(lake, name).with_context(|| format!("{name}: not this lake's"))?;
    ensure!(lake.cat.get::<Task>(&task_key(&name)).await?.is_some(), "no task {name}");
    let last = lake.cat.get::<Tick>(&tick_key(&name)).await?;
    if let Some(t) = last.as_ref().filter(|t| !t.done) {
        bail!("task {name} is running (run task-{name}-{}): EXECUTE TASK once it ends", t.at);
    }
    let at = now_ms().max(last.map_or(0, |t| t.at + 1));
    lake.cat.commit(vec![(tick_key(&name), json(&Tick { at, params, ..Default::default() }))], &[]).await?;
    WAKE.notify_one();
    Ok(serde_json::json!({"task": name, "run": format!("task-{name}-{at}")}))
}

/// Leader: `ALTER TASK name SUSPEND | RESUME`.
pub async fn alter_task(lake: &Lake, name: &str, suspended: bool) -> Result<serde_json::Value> {
    let name = crate::ddl::local(lake, name).with_context(|| format!("{name}: not this lake's"))?;
    let mut task = lake.cat.get::<Task>(&task_key(&name)).await?.with_context(|| format!("no task {name}"))?;
    task.suspended = suspended;
    lake.cat.commit(vec![(task_key(&name), json(&task))], &[]).await?;
    Ok(serde_json::json!({"task": name, "state": if suspended { "suspended" } else { "started" }}))
}

/// The scheduled tasks a task's graph starts from, through what it follows; refused if it would
/// follow itself, or what it follows starts from two (they would never run in one graph's run).
fn roots(all: &HashMap<String, Task>, name: &str, task: &Task, seen: &mut Vec<String>) -> Result<HashSet<String>> {
    if task.after.is_empty() {
        return Ok(HashSet::from([name.to_string()]));
    }
    ensure!(!seen.iter().any(|s| s == name), "AFTER: {} would follow itself", seen.join(" → "));
    seen.push(name.to_string());
    let mut out = HashSet::new();
    for a in &task.after {
        let t = all.get(a).with_context(|| format!("AFTER {a}: no task {a}"))?;
        out.extend(roots(all, a, t, seen)?);
    }
    seen.pop();
    Ok(out)
}

/// Leader: keep a task (`ddl::apply`); its first tick is the first after now.
pub async fn create_task(lake: &Lake, name: &str, mut task: Task, replace: bool) -> Result<serde_json::Value> {
    let name = crate::ddl::new_name(lake, name).await?;
    ensure!(replace || lake.cat.get::<Task>(&task_key(&name)).await?.is_none(), "task {name} already exists (CREATE OR REPLACE TASK)");
    let mut all: HashMap<String, Task> = tasks(lake).await?.iter().cloned().collect();
    for a in task.after.iter_mut() {
        *a = crate::ddl::local(lake, a).with_context(|| format!("AFTER {a}: not this lake's"))?;
    }
    all.insert(name.clone(), task.clone());
    let graphs = roots(&all, &name, &task, &mut vec![])?;
    ensure!(graphs.len() == 1, "AFTER: those tasks start from {} different schedules, so they never run in one graph's run", graphs.len());
    for (other, t) in all.iter().filter(|(n, t)| **n != name && t.after.contains(&name)) {
        roots(&all, other, t, &mut vec![])?; // (replaced: what follows it still has one start, no loop)
    }
    task.created_ms = now_ms();
    lake.cat.commit(vec![(task_key(&name), json(&task))], &[]).await?;
    let next = every(&task.schedule).ok().filter(|_| task.after.is_empty()).map(|e| next_after(&e, task.created_ms));
    Ok(serde_json::json!({"task": name, "next": next, "after": task.after}))
}

pub async fn drop_task(lake: &Lake, name: &str, if_exists: bool) -> Result<serde_json::Value> {
    let name = crate::ddl::local(lake, name).with_context(|| format!("{name}: not this lake's"))?;
    if lake.cat.get::<Task>(&task_key(&name)).await?.is_none() {
        ensure!(if_exists, "no task {name}");
        return Ok(serde_json::json!({"dropped": false}));
    }
    if let Some((f, _)) = tasks(lake).await?.iter().find(|(_, t)| t.after.contains(&name)) {
        bail!("task {f} runs after {name}: drop {f} first, or make it again without it");
    }
    lake.cat.commit(vec![], &[task_key(&name), tick_key(&name)]).await?;
    Ok(serde_json::json!({"task": name, "dropped": true}))
}

/// This lake's tasks, read again only after a commit.
async fn tasks(lake: &Lake) -> Result<Arc<Vec<(String, Task)>>> {
    static SEEN: OnceLock<Mutex<HashMap<String, (u64, Arc<Vec<(String, Task)>>)>>> = OnceLock::new();
    let seen = SEEN.get_or_init(Default::default);
    let version = lake.cat.version();
    if let (Some(v), Some((at, all))) = (version, seen.lock().unwrap().get(&lake.url)) {
        if *at == v {
            return Ok(all.clone());
        }
    }
    let all = Arc::new(lake.cat.scan::<Task>("j/", "j0").await?.into_iter().map(|(k, t)| (k[2..].to_string(), t)).collect::<Vec<_>>());
    if let Some(v) = version {
        seen.lock().unwrap().insert(lake.url.clone(), (v, all.clone()));
    }
    Ok(all)
}

/// Wakes the scheduler at once: a task ended (what follows it may run), or `EXECUTE TASK` claimed
/// a tick.
static WAKE: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Leader: run the tasks as their ticks come, for as long as this node leads (a new leader is a
/// new process). Nothing to do costs a look at the catalog's version twice a second.
pub fn schedule(app: App) {
    crate::panics::spawn(async move {
        let running: Arc<Mutex<HashSet<String>>> = Default::default();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                _ = WAKE.notified() => {}
            }
            let Ok(all) = tasks(&app.lake).await else { continue };
            for (name, task) in all.iter() {
                if running.lock().unwrap().contains(name) {
                    continue; // (a tick still running: the next waits for it)
                }
                let tick = match due(&app.lake, name, task).await {
                    Ok(Some(t)) => t,
                    Ok(None) => continue,
                    Err(e) => {
                        eprintln!("task {name}: {e:#}");
                        continue;
                    }
                };
                running.lock().unwrap().insert(name.clone());
                let (app, name, task, running) = (app.clone(), name.clone(), task.clone(), running.clone());
                tokio::spawn(async move {
                    run_tick(&app, &name, &task, tick).await;
                    running.lock().unwrap().remove(&name);
                    WAKE.notify_one(); // (what follows it)
                });
            }
        }
    });
}

/// Run a task's tick: `WHEN`, then what it runs, with its graph's values and the results of the
/// tasks before it, tried again as `retries` says (the same job: what a try wrote lands once), each
/// try within `timeout`; then its line in the run log, then the tick marked done, with how it went.
async fn run_tick(app: &App, name: &str, task: &Task, tick: Tick) {
    let job = format!("task:{name}:{}", tick.at);
    let who = Who { role: Role::Admin, files: false, depth: 0 };
    let none = RecordBatch::new_empty(Arc::new(datafusion::arrow::datatypes::Schema::empty()));
    let id = format!("task-{name}-{}", tick.at); // (a tick run again after a failover is the same run: its row, ended)
    let run = CALLER.sync_scope("schedule".into(), || Run::start(app, name, Role::Admin, Some(&job), &none, Some(id)));
    let (views, caller, mut heard) = (HashMap::new(), format!("task:{name}"), vec![]);
    let skip = match &task.when {
        None => Ok(false),
        Some(cond) => {
            let check = format!("SELECT 1 AS t WHERE ({})", results(&app.lake, cond, tick.at).await);
            let (out, said) = crate::routines::with_notices(CALLER.scope(caller.clone(), crate::routines::script(app, &check, &tick.params, &views, who, None))).await;
            heard.extend(said);
            out.map(|o| !matches!(o, Outcome::Rows(b) if b.iter().any(|b| b.num_rows() > 0))).map_err(|e| e.context("WHEN"))
        }
    };
    let out = match skip {
        Ok(true) => Ok(None),
        Err(e) => Err(e),
        Ok(false) => {
            let sql = results(&app.lake, &task.sql, tick.at).await;
            let mut tries = 0;
            loop {
                let go = CALLER.scope(caller.clone(), crate::routines::script(app, &sql, &tick.params, &views, who, Some(job.clone())));
                let (out, said) = crate::routines::with_notices(async {
                    match task.with.timeout {
                        Some(s) => tokio::time::timeout(Duration::from_secs(s), go).await.unwrap_or_else(|_| Err(anyhow::anyhow!("timed out after {s} s"))),
                        None => go.await,
                    }
                })
                .await;
                heard.extend(said);
                match out {
                    Err(e) if tries < task.with.retries => {
                        tries += 1;
                        let wait = task.with.retry_delay.unwrap_or(10);
                        heard.push(format!("try {tries} failed ({e:#}); again in {wait} s"));
                        tokio::time::sleep(Duration::from_secs(wait)).await;
                    }
                    other => break other.map(Some),
                }
            }
        }
    };
    if let Err(e) = &out {
        eprintln!("task {name}, tick {}: {e:#}", tick.at);
        if let Some(p) = &task.with.on_failure {
            let call = format!("CALL {p}('{}', '{}')", name.replace('\'', "''"), format!("{e:#}").replace('\'', "''"));
            let (said, more) = crate::routines::with_notices(CALLER.scope(caller.clone(), crate::routines::script(app, &call, &HashMap::new(), &views, who, None))).await;
            heard.extend(more);
            if let Err(f) = said {
                heard.push(format!("on_failure {p}: {f:#}"));
            }
        }
    }
    let (status, result) = match &out {
        Ok(None) => ("skipped", None),
        Ok(Some(Outcome::Rows(b))) => ("ok", b.iter().find(|b| b.num_rows() > 0 && b.num_columns() > 0).and_then(|b| crate::vars::of_column(b.column(0).slice(0, 1).as_ref()).ok()).map(|v| v.sql)),
        Ok(Some(_)) => ("ok", None),
        Err(_) => ("failed", None),
    };
    let error = out.as_ref().err().map(|e| format!("{e:#}"));
    let _ = tokio::time::timeout(Duration::from_secs(60), run.ended(app, status, error, heard)).await; // (in the log before the tick is done)
    let done = json(&Tick { at: tick.at, done: true, status: Some(status.into()), result, params: tick.params });
    if let Err(e) = app.lake.cat.commit(vec![(tick_key(name), done)], &[]).await {
        eprintln!("task {name}: tick {} ran, and couldn't be marked done ({e:#}): a new leader runs it again, with the same job", tick.at);
    }
}

/// `pondra.result('t')` in a task's SQL: what task `t` gave in the same run of the graph (NULL if
/// it gave nothing, or hasn't run in it), put in as its value.
async fn results(lake: &Lake, sql: &str, at: u64) -> String {
    static RESULT: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\bpondra\.result\s*\(\s*'((?:[^']|'')*)'\s*\)").expect("a regex"));
    let (mut out, mut from) = (String::new(), 0);
    for c in RESULT.captures_iter(sql) {
        let m = c.get(0).expect("a match");
        let name = c[1].replace("''", "'");
        let name = crate::ddl::local(lake, &name).unwrap_or(name);
        let tick = lake.cat.get::<Tick>(&tick_key(&name)).await.ok().flatten();
        let value = tick.filter(|t| t.at == at && t.done).and_then(|t| t.result).unwrap_or_else(|| "NULL".into());
        out.push_str(&sql[from..m.start()]);
        out.push_str(&format!("({value})"));
        from = m.end();
    }
    out.push_str(&sql[from..]);
    out
}

/// The tick to run now, claimed (committed before it runs), if one is due: one claimed and not
/// finished (a leader died in it, or `EXECUTE TASK` claimed it); for a scheduled task, the latest
/// that has come since the last; for one that follows others, the latest run of their graph that
/// every one of them ended well in, and it hasn't run in.
async fn due(lake: &Lake, name: &str, task: &Task) -> Result<Option<Tick>> {
    let last = lake.cat.get::<Tick>(&tick_key(name)).await?;
    if let Some(t) = last.as_ref().filter(|t| !t.done) {
        return Ok(Some(t.clone())); // (again, with its job: what it wrote lands once)
    }
    if task.suspended {
        return Ok(None);
    }
    let tick = match task.after.is_empty() {
        true => {
            let after = last.as_ref().map_or(task.created_ms, |t| t.at.max(task.created_ms));
            let Some(at) = latest(&every(&task.schedule)?, after, now_ms()) else { return Ok(None) };
            Tick { at, ..Default::default() }
        }
        false => {
            let mut before: Option<Tick> = None;
            for a in &task.after {
                let Some(t) = lake.cat.get::<Tick>(&tick_key(a)).await?.filter(Tick::passed) else { return Ok(None) };
                match &before {
                    Some(b) if b.at != t.at => return Ok(None), // (they are in different runs of the graph yet)
                    _ => before = Some(t),
                }
            }
            let Some(b) = before.filter(|b| b.at > task.created_ms && last.as_ref().is_none_or(|l| l.at < b.at)) else { return Ok(None) };
            Tick { at: b.at, params: b.params, ..Default::default() }
        }
    };
    lake.cat.commit(vec![(tick_key(name), json(&tick))], &[]).await?;
    Ok(Some(tick))
}

/// How often: every so many seconds (aligned to the epoch), or a cron expression in a time zone.
pub enum Every {
    Seconds(u64),
    Cron(Box<Cron>, chrono_tz::Tz),
}

/// `'5 minutes'`, `'every 30 seconds'`, `'1 HOUR'` (Snowflake's `'5 MINUTE'`), or `'cron 0 2 * *
/// * UTC'` (also `'USING CRON …'`, or the five fields alone), its time zone UTC unless named.
pub fn every(text: &str) -> Result<Every> {
    let t = text.trim().to_lowercase();
    let t = t.strip_prefix("using ").unwrap_or(&t);
    let t = t.strip_prefix("every ").unwrap_or(t);
    let fields: Vec<&str> = t.strip_prefix("cron ").unwrap_or(t).split_whitespace().collect();
    if t.starts_with("cron ") || (fields.len() >= 5 && fields[0].chars().all(|c| c.is_ascii_digit() || "*/,-".contains(c))) {
        ensure!((5..=6).contains(&fields.len()), "cron: five fields (minute hour day month weekday) and a time zone");
        let tz = match fields.get(5) {
            Some(z) => text.split_whitespace().last().unwrap_or(z).parse::<chrono_tz::Tz>().map_err(|e| anyhow::anyhow!("cron: time zone {z}: {e}"))?,
            None => chrono_tz::UTC,
        };
        return Ok(Every::Cron(Box::new(Cron::parse(&fields[..5])?), tz));
    }
    let n: String = t.chars().take_while(|c| c.is_ascii_digit()).collect();
    let unit = t[n.len()..].trim();
    let n: u64 = if n.is_empty() { 1 } else { n.parse()? };
    let secs = match unit.trim_end_matches('s') {
        "second" | "sec" | "" => 1,
        "minute" | "min" | "m" => 60,
        "hour" | "h" => 3600,
        "day" | "d" => 86400,
        u => bail!("a schedule is '5 minutes', '30 seconds', '1 hour' or 'cron 0 2 * * * UTC', not {u:?}"),
    };
    ensure!(n > 0, "a schedule's interval is at least a second");
    Ok(Every::Seconds(n * secs))
}

/// A cron expression's five fields, as the minutes, hours, days, months and weekdays it takes.
pub struct Cron {
    minutes: u64,
    hours: u32,
    days: u32,
    months: u16,
    weekdays: u8,
    any_day: bool,
    any_weekday: bool,
}

impl Cron {
    fn parse(f: &[&str]) -> Result<Cron> {
        const MONTHS: [&str; 12] = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
        const DAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
        let (days, any_day) = field(f[2], 1, 31, &[])?;
        let (weekdays, any_weekday) = field(f[4], 0, 7, &DAYS)?;
        let weekdays = (weekdays | (weekdays >> 7)) & 0x7f; // (7 is Sunday too)
        Ok(Cron { minutes: field(f[0], 0, 59, &[])?.0, hours: field(f[1], 0, 23, &[])?.0 as u32, days: days as u32, months: (field(f[3], 1, 12, &MONTHS)?.0) as u16, weekdays: weekdays as u8, any_day, any_weekday })
    }

    fn day(&self, d: &chrono::DateTime<chrono_tz::Tz>) -> bool {
        let (dom, dow) = (self.days >> d.day() & 1 == 1, self.weekdays >> d.weekday().num_days_from_sunday() & 1 == 1);
        self.months >> d.month() & 1 == 1 && match (self.any_day, self.any_weekday) {
            (false, false) => dom || dow, // (cron's rule: either, when both are given)
            _ => dom && dow,
        }
    }
}

/// One field: `*`, `5`, `1-5`, `*/15`, `1-30/2`, `mon,wed` … as a bit per value it takes; and
/// whether it is `*`.
fn field(text: &str, lo: u64, hi: u64, names: &[&str]) -> Result<(u64, bool)> {
    let value = |v: &str| -> Result<u64> {
        let v = match names.iter().position(|n| *n == v) {
            Some(i) => i as u64 + if names.len() == 12 { 1 } else { 0 },
            None => v.parse().with_context(|| format!("cron: {v:?} in {text:?}"))?,
        };
        ensure!((lo..=hi).contains(&v), "cron: {v} is outside {lo}-{hi} in {text:?}");
        Ok(v)
    };
    let mut bits = 0u64;
    for part in text.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, s.parse::<u64>().ok().filter(|s| *s > 0).with_context(|| format!("cron: step {s:?}"))?),
            None => (part, 1),
        };
        let (a, b) = match range {
            "*" => (lo, hi),
            r => match r.split_once('-') {
                Some((a, b)) => (value(a)?, value(b)?),
                None if step > 1 => (value(r)?, hi),
                None => (value(r)?, value(r)?),
            },
        };
        for v in (a..=b).step_by(step as usize) {
            bits |= 1 << v;
        }
    }
    Ok((bits, text == "*"))
}

/// The first tick after `t` (ms).
pub fn next_after(e: &Every, t: u64) -> u64 {
    match e {
        Every::Seconds(s) => (t / (s * 1000) + 1) * s * 1000,
        Every::Cron(c, tz) => {
            let mut at = (t / 60_000 + 1) * 60_000;
            for _ in 0..600_000 {
                let local = tz.timestamp_millis_opt(at as i64).single().expect("a moment is one local time");
                let (h, m) = (local.hour() as u64, local.minute() as u64);
                at += if !c.day(&local) {
                    (24 * 60 - (h * 60 + m)) * 60_000 // (to the next day)
                } else if c.hours >> h & 1 == 0 {
                    (60 - m) * 60_000
                } else if c.minutes >> m & 1 == 0 {
                    60_000
                } else {
                    return at;
                };
            }
            u64::MAX // (never: February 30th)
        }
    }
}

/// The latest tick in (after, now], if any.
fn latest(e: &Every, after: u64, now: u64) -> Option<u64> {
    if let Every::Seconds(s) = e {
        let t = now / (s * 1000) * s * 1000;
        return (t > after).then_some(t);
    }
    let from = after.max(now.saturating_sub(86_400_000)); // (the last day's ticks; before that only if none came in it)
    let mut t = next_after(e, from);
    if t > now {
        if from == after {
            return None;
        }
        t = next_after(e, after);
        if t > now {
            return None;
        }
    }
    loop {
        let n = next_after(e, t);
        if n > now {
            return Some(t);
        }
        t = n;
    }
}

// ---------------------------------------------------------------- as tables

/// Does `sql` read one of these tables?
pub fn mentioned(sql: &str) -> bool {
    let s = sql.to_lowercase();
    ["pondra.runs", "pondra.routines", "pondra.tasks", "pondra.tables", "pondra.users", "pondra.grants", "pondra.audit", "pondra.history", "pondra.flows", "pondra.expectations", "pondra.variables", "pondra.dropped"].iter().any(|t| s.contains(t))
}

/// `pondra.routines`, `pondra.tasks` and `pondra.tables`, as they are now.
pub async fn tables(lake: &Lake) -> Result<Vec<(&'static str, Arc<dyn datafusion::catalog::TableProvider>)>> {
    use datafusion::datasource::MemTable;
    let all = crate::routines::listed(lake).await?;
    let mut names: Vec<&String> = all.keys().collect();
    names.sort();
    let r = |f: &dyn Fn(&str, &Routine) -> Option<String>| Arc::new(names.iter().map(|n| f(n, &all[*n])).collect::<StringArray>()) as ArrayRef;
    let kind = |r: &Routine| match (r.kind, r.what()) {
        (Kind::Procedure, _) => "procedure",
        (Kind::Table, _) => "table function",
        (_, "macro") => "macro",
        _ => "function",
    };
    let arguments = |r: &Routine| r.params.iter().map(|p| [Some(p.name.as_str()).filter(|n| !n.chars().all(|c| c.is_ascii_digit())), p.ty.as_deref(), p.default.as_ref().map(|_| "DEFAULT"), p.default.as_deref()].into_iter().flatten().collect::<Vec<_>>().join(" ")).collect::<Vec<_>>().join(", ");
    let routines = RecordBatch::try_from_iter(vec![
        ("name", r(&|n, _| Some(n.to_string()))),
        ("kind", r(&|_, r| Some(kind(r).to_string()))),
        ("language", r(&|_, r| Some(if r.language.is_empty() { "sql".into() } else { r.language.clone() }))),
        ("arguments", r(&|_, r| Some(arguments(r)))),
        ("returns", r(&|_, r| r.returns.clone())),
        ("volatility", r(&|_, r| (r.kind != Kind::Procedure).then(|| r.with.volatility.clone().unwrap_or_else(|| if r.python() { "volatile" } else { "as its body" }.into())))),
        ("options", r(&|_, r| Some(serde_json::to_string(&r.with).unwrap_or_default()).filter(|o| o != "{}"))),
        ("body", r(&|_, r| Some(r.body.clone()))),
    ])?;
    let all = tasks(lake).await?;
    let mut ticks = vec![];
    for (name, _) in all.iter() {
        ticks.push(lake.cat.get::<Tick>(&tick_key(name)).await?);
    }
    let t = |f: &dyn Fn(usize, &(String, Task)) -> Option<String>| Arc::new(all.iter().enumerate().map(|(i, x)| f(i, x)).collect::<StringArray>()) as ArrayRef;
    let at = |f: &dyn Fn(usize, &Task) -> Option<u64>| Arc::new(all.iter().enumerate().map(|(i, (_, x))| f(i, x).map(|ms| ms as i64 * 1000)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef;
    let next = |i: usize, x: &Task| every(&x.schedule).ok().filter(|_| x.after.is_empty() && !x.suspended).map(|e| next_after(&e, ticks[i].as_ref().map_or(x.created_ms, |t| t.at.max(x.created_ms)).max(now_ms().saturating_sub(1))));
    let tasks = RecordBatch::try_from_iter(vec![
        ("name", t(&|_, (n, _)| Some(n.clone()))),
        ("schedule", t(&|_, (_, x)| Some(x.schedule.clone()).filter(|s| !s.is_empty()))),
        ("after", t(&|_, (_, x)| Some(x.after.join(", ")).filter(|s| !s.is_empty()))),
        ("when", t(&|_, (_, x)| x.when.clone())),
        ("statement", t(&|_, (_, x)| Some(x.sql.clone()))),
        ("options", t(&|_, (_, x)| (!x.with.plain()).then(|| serde_json::to_string(&x.with).unwrap_or_default()))),
        ("state", t(&|_, (_, x)| Some(if x.suspended { "suspended" } else { "started" }.into()))),
        ("last_tick", at(&|i, _| ticks[i].as_ref().map(|t| t.at))),
        ("last_status", t(&|i, _| ticks[i].as_ref().map(|t| if t.done { t.status.clone().unwrap_or_else(|| "ok".into()) } else { "running".into() }))),
        ("last_result", t(&|i, _| ticks[i].as_ref().and_then(|t| t.result.clone()))),
        ("next_tick", at(&|i, x| next(i, x))),
    ])?;
    let mem = |b: RecordBatch| -> Result<Arc<dyn datafusion::catalog::TableProvider>> { Ok(Arc::new(MemTable::try_new(b.schema(), vec![vec![b]])?)) };
    // (every table and view, of what kind: `information_schema.tables` knows only BASE TABLE and VIEW;
    // rows and bytes in its files, so rows still in the log count once written out)
    let mut all = crate::ddl::listed(lake).await?;
    if let Some(a) = crate::auth::limited() {
        all.retain(|o| a.may("select", &if o.lake == crate::ddl::lake_name(lake) { crate::ddl::join(&o.schema, &o.name) } else { format!("{}.{}", o.lake, crate::ddl::join(&o.schema, &o.name)) })); // (a user's: what it may read)
    }
    let l = |f: &dyn Fn(&crate::ddl::Listed) -> Option<String>| Arc::new(all.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let n = |f: &dyn Fn(&TableMeta) -> u64| Arc::new(all.iter().map(|o| o.meta.as_ref().map(|m| f(m) as i64)).collect::<Int64Array>()) as ArrayRef;
    let sealed = |m: &TableMeta| m.sealed.clone().unwrap_or_default();
    let listed = RecordBatch::try_from_iter(vec![
        ("lake", l(&|o| Some(o.lake.clone()))),
        ("schema", l(&|o| Some(o.schema.clone()))),
        ("name", l(&|o| Some(o.name.clone()))),
        ("kind", l(&|o| Some(o.kind.to_string()))),
        ("rows_in_files", n(&|m| m.files.iter().map(|f| f.rows).sum::<u64>() + sealed(m).rows)),
        ("bytes_in_files", n(&|m| m.files.iter().map(|f| f.bytes).sum::<u64>() + sealed(m).bytes)),
        ("key", l(&|o| o.meta.as_ref().filter(|m| !m.key.is_empty()).map(|m| m.key.join(", ")))),
        ("definition", l(&|o| o.sql.clone())),
    ])?;
    // (dropped tables that UNDROP TABLE can still bring back: ADR-043)
    let mut gone = crate::ddl::dropped(lake).await?;
    if let Some(a) = crate::auth::limited() {
        gone.retain(|(n, _)| a.may("select", n));
    }
    let g = |f: &dyn Fn(&crate::ddl::Dropped) -> u64| Arc::new(gone.iter().map(|(_, d)| Some(f(d) as i64)).collect::<Int64Array>()) as ArrayRef;
    let when = |f: &dyn Fn(&crate::ddl::Dropped) -> u64| Arc::new(gone.iter().map(|(_, d)| Some(f(d) as i64 * 1000)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef;
    let dropped = RecordBatch::try_from_iter(vec![
        ("name", Arc::new(gone.iter().map(|(n, _)| Some(n.clone())).collect::<StringArray>()) as ArrayRef),
        ("dropped_at", when(&|d| d.at_ms)),
        ("kept_until", when(&|d| d.at_ms + d.keep_ms)),
        ("rows_in_files", g(&|d| d.meta.files.iter().map(|f| f.rows).sum::<u64>() + d.meta.sealed.as_ref().map_or(0, |s| s.rows))),
        ("bytes_in_files", g(&|d| d.meta.files.iter().map(|f| f.bytes).sum::<u64>() + d.meta.sealed.as_ref().map_or(0, |s| s.bytes))),
    ])?;
    Ok(vec![("routines", mem(routines)?), ("tasks", mem(tasks)?), ("tables", mem(listed)?), ("dropped", mem(dropped)?)])
}

/// `pondra.runs` before any run: no rows, its columns.
pub fn no_runs() -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    let schema = crate::query::read_schema(&columns().into_iter().filter(|(c, _)| c != "_deleted").collect::<Vec<_>>())?;
    Ok(Arc::new(datafusion::datasource::MemTable::try_new(schema, vec![vec![]])?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_task() {
        assert_eq!(execute_of("EXECUTE TASK nightly"), Some(("nightly".into(), vec![])));
        assert_eq!(execute_of("execute task etl.nightly (day => DATE '2026-09-01', n = 2);"), Some(("etl.nightly".into(), vec![("day".into(), "DATE '2026-09-01'".into()), ("n".into(), "2".into())])));
        assert_eq!(execute_of("EXECUTE q(1)"), None); // (a prepared statement's)
        assert_eq!(execute_of("EXECUTE TASK t; SELECT 1"), None);
    }

    #[test]
    fn options() {
        let opts = |sql: &str| task_options(&mut Parser::new(&datafusion::sql::sqlparser::dialect::GenericDialect {}).try_with_sql(sql).unwrap());
        let o = opts("(retries = 2, retry_delay = '1 minute', timeout = 90, on_failure = notify)").unwrap();
        assert_eq!((o.retries, o.retry_delay, o.timeout, o.on_failure.as_deref()), (2, Some(60), Some(90), Some("notify")));
        assert!(opts("(tries = 2)").unwrap_err().to_string().contains("retries, retry_delay"));
        assert!(opts("(timeout = 'cron 0 2 * * *')").unwrap_err().to_string().contains("how long"));
    }
}
