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
//! - **`pondra.routines`, `pondra.tasks`**: the catalog's functions, procedures and tasks, as
//!   tables (`SHOW FUNCTIONS`, `SHOW PROCEDURES`, `SHOW TASKS` read them).
use crate::auth::Role;
use crate::routines::{Kind, Outcome, Routine, Who};
use crate::server::App;
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use chrono::{Datelike, TimeZone, Timelike};
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::sql::sqlparser::{keywords::Keyword, parser::Parser, tokenizer::Token};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
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
        let mut line = self.0;
        (line.ended, line.status) = (Some(now_ms()), if out.is_ok() { "ok" } else { "failed" });
        line.error = out.as_ref().err().map(|e| cut(format!("{e:#}")));
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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Task {
    pub schedule: String,
    pub sql: String,
    #[serde(default)]
    pub created_ms: u64,
}

/// The tick a leader last took on, and whether it finished (`jt/`).
#[derive(Serialize, Deserialize, Clone, Copy)]
struct Tick {
    at: u64,
    done: bool,
}

pub fn task_key(name: &str) -> String { format!("j/{name}") }
fn tick_key(name: &str) -> String { format!("jt/{name}") }

pub const USAGE: &str = "CREATE TASK name SCHEDULE 'cron 0 2 * * * UTC' | '5 minutes' AS CALL procedure(…)";

/// The rest of `CREATE TASK`: its name, `SCHEDULE [=] '…'` and `AS` the statement it runs.
pub fn task(p: &mut Parser) -> Result<(String, Task)> {
    let _ = p.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = crate::write::object(&p.parse_object_name(false)?);
    ensure!(crate::routines::word(p, "schedule"), "SCHEDULE '…' is missing");
    let _ = p.consume_token(&Token::Eq);
    let schedule = crate::routines::text_of(p)?.context("the schedule is a string: SCHEDULE '5 minutes'")?;
    every(&schedule)?;
    ensure!(p.parse_keyword(Keyword::AS), "AS statement is missing");
    let sql = p.parse_statement()?.to_string();
    let t = p.next_token().token;
    ensure!(matches!(t, Token::EOF | Token::SemiColon), "unexpected {t}");
    Ok((name, Task { schedule, sql, created_ms: 0 }))
}

/// Leader: keep a task (`ddl::apply`); its first tick is the first after now.
pub async fn create_task(lake: &Lake, name: &str, mut task: Task, replace: bool) -> Result<serde_json::Value> {
    let name = crate::ddl::new_name(lake, name).await?;
    ensure!(replace || lake.cat.get::<Task>(&task_key(&name)).await?.is_none(), "task {name} already exists (CREATE OR REPLACE TASK)");
    every(&task.schedule)?;
    task.created_ms = now_ms();
    lake.cat.commit(vec![(task_key(&name), json(&task))], &[]).await?;
    Ok(serde_json::json!({"task": name, "next": next_after(&every(&task.schedule)?, task.created_ms)}))
}

pub async fn drop_task(lake: &Lake, name: &str, if_exists: bool) -> Result<serde_json::Value> {
    let name = crate::ddl::local(lake, name).with_context(|| format!("{name}: not this lake's"))?;
    if lake.cat.get::<Task>(&task_key(&name)).await?.is_none() {
        ensure!(if_exists, "no task {name}");
        return Ok(serde_json::json!({"dropped": false}));
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

/// Leader: run the tasks as their ticks come, for as long as this node leads (a new leader is a
/// new process). Nothing to do costs a look at the catalog's version twice a second.
pub fn schedule(app: App) {
    crate::panics::spawn(async move {
        let running: Arc<Mutex<HashSet<String>>> = Default::default();
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
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
                    let job = format!("task:{name}:{tick}");
                    let who = Who { role: Role::Admin, files: false, depth: 0 };
                    let none = RecordBatch::new_empty(Arc::new(datafusion::arrow::datatypes::Schema::empty()));
                    let id = format!("task-{name}-{tick}"); // (a tick run again after a failover is the same run: its row, ended)
                    let run = CALLER.sync_scope("schedule".into(), || Run::start(&app, &name, Role::Admin, Some(&job), &none, Some(id)));
                    let (params, views) = (HashMap::new(), HashMap::new());
                    let script = crate::routines::script(&app, &task.sql, &params, &views, who, Some(job));
                    let (out, heard) = crate::routines::with_notices(CALLER.scope(format!("task:{name}"), script)).await;
                    if let Err(e) = &out {
                        eprintln!("task {name}, tick {tick}: {e:#}");
                    }
                    let _ = tokio::time::timeout(Duration::from_secs(60), run.end(&app, &out, heard)).await; // (in the log before the tick is done)
                    let done = json(&Tick { at: tick, done: true });
                    if let Err(e) = app.lake.cat.commit(vec![(tick_key(&name), done)], &[]).await {
                        eprintln!("task {name}: tick {tick} ran, and couldn't be marked done ({e:#}): a new leader runs it again, with the same job");
                    }
                    running.lock().unwrap().remove(&name);
                });
            }
        }
    });
}

/// The tick to run now, claimed (committed before it runs), if one is due: one a leader claimed
/// and didn't finish, or the latest that has come since the last.
async fn due(lake: &Lake, name: &str, task: &Task) -> Result<Option<u64>> {
    let last = lake.cat.get::<Tick>(&tick_key(name)).await?;
    if let Some(t) = last.filter(|t| !t.done) {
        return Ok(Some(t.at)); // (again, with its job: what it wrote lands once)
    }
    let after = last.map_or(task.created_ms, |t| t.at.max(task.created_ms));
    let Some(tick) = latest(&every(&task.schedule)?, after, now_ms()) else { return Ok(None) };
    lake.cat.commit(vec![(tick_key(name), json(&Tick { at: tick, done: false }))], &[]).await?;
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
    ["pondra.runs", "pondra.routines", "pondra.tasks", "pondra.tables", "pondra.users", "pondra.grants", "pondra.audit", "pondra.pipelines", "pondra.expectations"].iter().any(|t| s.contains(t))
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
    let next = |i: usize, x: &Task| every(&x.schedule).ok().map(|e| next_after(&e, ticks[i].map_or(x.created_ms, |t| t.at.max(x.created_ms)).max(now_ms().saturating_sub(1))));
    let tasks = RecordBatch::try_from_iter(vec![
        ("name", t(&|_, (n, _)| Some(n.clone()))),
        ("schedule", t(&|_, (_, x)| Some(x.schedule.clone()))),
        ("statement", t(&|_, (_, x)| Some(x.sql.clone()))),
        ("last_tick", at(&|i, _| ticks[i].map(|t| t.at))),
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
    Ok(vec![("routines", mem(routines)?), ("tasks", mem(tasks)?), ("tables", mem(listed)?)])
}

/// `pondra.runs` before any run: no rows, its columns.
pub fn no_runs() -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    let schema = crate::query::read_schema(&columns().into_iter().filter(|(c, _)| c != "_deleted").collect::<Vec<_>>())?;
    Ok(Arc::new(datafusion::datasource::MemTable::try_new(schema, vec![vec![]])?))
}
