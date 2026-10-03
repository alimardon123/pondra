//! The query history (`pondra.history`, ADR-046): every statement a door was sent, a row each:
//! who ran it, through which door and from where, on which node and in which session, how long it
//! took, how many rows it answered and how it ended. Kept `PONDRA_HISTORY_DAYS` (7) in a table of
//! the lake's own (`pondra$history`, hidden); an admin reads every row, anyone else their own.
//!
//! A statement slower than `PONDRA_SLOW_MS` (1000) also keeps its plan, each operator with its
//! rows and time, and its trace: the time each node took for its share, a step at a time when it
//! ran across the nodes. It writes one line to the node's log too (the slow-query log).
//!
//! Nothing waits for any of it. Rows go to a writer on each node, which appends what came in a
//! second. At most `PONDRA_HISTORY_RATE` (500) rows a second are written a node: past that, the
//! fast statements that went well are counted in one row, not written one by one. A slow or failed
//! statement is always written. `PONDRA_HISTORY=off` keeps none.
use crate::server::App;
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub const TABLE: &str = "pondra$history";
pub const KEY: &str = "t/pondra$history"; // (`table_key(TABLE)`: commits only to it are quiet, `store::quiet`)

/// What a statement's work noted while it ran: what it answered, where, and how.
#[derive(Default)]
pub struct Note {
    rows: Option<u64>,
    nodes: Option<u64>,
    plan: Option<String>,
    trace: Vec<Span>,
}

/// A part of a statement's work: what it was, on which node, from when and for how long (ms).
#[derive(serde::Serialize)]
struct Span {
    what: String,
    node: String,
    at: f64,
    ms: f64,
}

tokio::task_local! {
    static NOTE: (Instant, Arc<Mutex<Note>>);
}

/// Run `f` as one statement, noting what its work says (`rows`, `spread`, `planned`, `span`).
pub async fn noted<T>(f: impl std::future::Future<Output = T>) -> (T, Note) {
    let note = Arc::new(Mutex::new(Note::default()));
    let out = NOTE.scope((Instant::now(), note.clone()), f).await;
    let note = std::mem::take(&mut *note.lock().unwrap());
    (out, note)
}

fn with(f: impl FnOnce(Instant, &mut Note)) {
    let _ = NOTE.try_with(|(start, note)| f(*start, &mut note.lock().unwrap()));
}

/// The rows a query answered (its statement's last query, in a script or a procedure).
pub fn rows(n: u64) { with(|_, note| note.rows = Some(n)); }

/// It ran across `n` nodes.
pub fn spread(n: usize) { with(|_, note| note.nodes = Some(n as u64)); }

/// A part of its work on `node`, which began at `began`, has ended.
pub fn span(what: impl Into<String>, node: &str, began: Instant) {
    with(|start, note| {
        let (at, ms) = (began.saturating_duration_since(start), began.elapsed());
        note.trace.push(Span { what: what.into(), node: node.to_string(), at: at.as_secs_f64() * 1e3, ms: ms.as_secs_f64() * 1e3 });
    });
}

/// A plan it ran, kept with each operator's rows and time when the statement is slow by now.
pub fn planned(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    with(|start, note| {
        if start.elapsed() >= slow() {
            let text = datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(plan.as_ref()).indent(true).to_string();
            note.plan = Some(cut(text, 64 << 10));
        }
    });
}

fn slow() -> Duration {
    static S: OnceLock<Duration> = OnceLock::new();
    *S.get_or_init(|| Duration::from_millis(std::env::var("PONDRA_SLOW_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(1000)))
}

fn on() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PONDRA_HISTORY").map_or(true, |v| !v.eq_ignore_ascii_case("off")))
}

fn rate() -> u64 {
    static R: OnceLock<u64> = OnceLock::new();
    *R.get_or_init(|| std::env::var("PONDRA_HISTORY_RATE").ok().and_then(|r| r.parse().ok()).unwrap_or(500))
}

/// `text` at most `max` bytes, cut at a character's end.
pub fn cut(mut text: String, max: usize) -> String {
    if text.len() > max {
        let mut at = max;
        while !text.is_char_boundary(at) {
            at -= 1;
        }
        text.truncate(at);
        text.push('…');
    }
    text
}

/// One statement, ended.
struct Line {
    at: u64,
    id: String,
    user: String,
    door: &'static str,
    from: Option<String>,
    session: Option<String>,
    class: &'static str,
    statement: String,
    outcome: &'static str,
    error: Option<String>,
    ms: i64,
    note: Note,
}

/// A statement a door ran has ended (`audit::statement`): its row, and a line in the node's log
/// when it was slow.
pub fn ended(app: &App, class: &'static str, sql: &str, outcome: &'static str, error: Option<String>, took: Duration, note: Note) {
    if !on() || app.log.is_none() {
        return; // (a read-only node writes nothing)
    }
    let slow = took >= slow();
    if !slow && outcome == "ok" && skipped() {
        return; // (counted: nothing else to do, on the path of every statement)
    }
    let who = crate::auth::current();
    let (user, door, from) = who.map_or((String::new(), "node", None), |p| (p.name, p.door, p.from.map(|a| a.to_string())));
    let statement = crate::audit::redacted(sql, class);
    if slow {
        eprintln!("slow statement: {} ms, {user} by {door}{}: {}", took.as_millis(), note.nodes.map(|n| format!(" on {n} nodes")).unwrap_or_default(), cut(statement.replace('\n', " "), 300));
    }
    let line = Line { at: crate::log::now_ms(), id: crate::runs::new_id(), user, door, from, session: crate::temp::current(), class, statement, outcome, error, ms: took.as_millis() as i64, note };
    let tx = WRITER.get_or_init(|| writer(app, TABLE, crate::ddl::Ddl::HistoryLog, Duration::from_secs(1), batch));
    let _ = tx.send(line);
}

/// Past this second's rate: this statement is counted, not written. The first statement of a
/// second after one that skipped some writes a row saying how many.
fn skipped() -> bool {
    static SECOND: Mutex<(u64, u64, u64)> = Mutex::new((0, 0, 0)); // (the second, rows written in it, statements skipped in it)
    let now = crate::log::now_ms() / 1000;
    let mut s = SECOND.lock().unwrap();
    if s.0 != now {
        let skipped = std::mem::replace(&mut *s, (now, 0, 0)).2;
        if let Some(tx) = WRITER.get().filter(|_| skipped > 0) {
            let statement = format!("({skipped} more statements that second, not written: PONDRA_HISTORY_RATE)");
            let _ = tx.send(Line { at: crate::log::now_ms(), id: crate::runs::new_id(), user: String::new(), door: "node", from: None, session: None, class: "skipped", statement, outcome: "ok", error: None, ms: 0, note: Note { rows: Some(skipped), ..Default::default() } });
        }
    }
    let full = s.1 >= rate();
    match full {
        true => s.2 += 1,
        false => s.1 += 1,
    }
    full
}

static WRITER: OnceLock<tokio::sync::mpsc::UnboundedSender<Line>> = OnceLock::new();

fn columns() -> Vec<(String, String)> {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    let ts = crate::query::type_name(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
    let text = |n: &str| (n.to_string(), "Utf8".to_string());
    let int = |n: &str| (n.to_string(), "Int64".to_string());
    vec![("at".into(), ts), text("id"), text("user"), text("door"), text("from"), text("node"), text("session"), text("class"), text("statement"), text("outcome"), text("error"), int("ms"), int("rows"), int("nodes"), text("plan"), text("trace")]
}

/// Leader: the table, made the first time a node has a row for it (`Ddl::HistoryLog`).
pub async fn create_log(lake: &Lake) -> Result<serde_json::Value> {
    if lake.cat.get::<TableMeta>(&table_key(TABLE)).await?.is_none() {
        let days: u64 = std::env::var("PONDRA_HISTORY_DAYS").ok().and_then(|d| d.parse().ok()).unwrap_or(7);
        let meta = TableMeta { columns: columns(), ttl: Some(("at".into(), days * 86400)), tiered: lake.visible(), ..Default::default() };
        lake.cat.commit(vec![(table_key(TABLE), json(&meta))], &[]).await?;
    }
    Ok(serde_json::json!({"table": "pondra.history"}))
}

fn batch(app: &App, lines: &[Line]) -> Result<RecordBatch> {
    let text = |f: &dyn Fn(&Line) -> Option<String>| Arc::new(lines.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let int = |f: &dyn Fn(&Line) -> Option<i64>| Arc::new(lines.iter().map(f).collect::<Int64Array>()) as ArrayRef;
    let node = app.cluster.addr.clone();
    let arrays = vec![
        Arc::new(lines.iter().map(|l| Some(l.at as i64 * 1000)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef,
        text(&|l| Some(l.id.clone())),
        text(&|l| Some(l.user.clone())),
        text(&|l| Some(l.door.to_string())),
        text(&|l| l.from.clone()),
        text(&|_| Some(node.clone())),
        text(&|l| l.session.clone()),
        text(&|l| Some(l.class.to_string())),
        text(&|l| Some(l.statement.clone())),
        text(&|l| Some(l.outcome.to_string())),
        text(&|l| l.error.clone()),
        int(&|l| Some(l.ms)),
        int(&|l| l.note.rows.map(|r| r as i64)),
        int(&|l| l.note.nodes.map(|n| n as i64)),
        text(&|l| l.note.plan.clone()),
        text(&|l| (!l.note.trace.is_empty()).then(|| serde_json::to_string(&l.note.trace).unwrap_or_default())),
    ];
    Ok(RecordBatch::try_new(crate::query::schema(&columns())?, arrays)?)
}

/// A node's writer for a table of its own (the history, the audit log): what came in `every`,
/// appended exactly once (one producer, a seq a batch), the table made the first time.
pub fn writer<L: Send + Sync + 'static>(app: &App, table: &'static str, make: crate::ddl::Ddl, every: Duration, batch: fn(&App, &[L]) -> Result<RecordBatch>) -> tokio::sync::mpsc::UnboundedSender<L> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<L>();
    let app = app.clone();
    crate::panics::spawn(async move {
        let producer = format!("{}-{}", table.trim_start_matches("pondra$"), crate::runs::new_id());
        let (mut seq, mut made) = (0, false);
        while let Some(first) = rx.recv().await {
            tokio::time::sleep(every).await;
            let mut lines = vec![first];
            while let Ok(l) = rx.try_recv() {
                lines.push(l);
            }
            seq += 1;
            for attempt in 0.. {
                let append = async {
                    if !made && app.lake.cat.get::<TableMeta>(&table_key(table)).await?.is_none() {
                        crate::write::on_node_as(&app, crate::write::Stmt::Ddl(vec![make.clone()]), None, false).await?;
                    }
                    made = true;
                    app.log()?.append(table.into(), crate::log::Src { producer: producer.clone(), seq, prev: None }, batch(&app, &lines)?).await
                };
                match append.await {
                    Ok(_) => break,
                    Err(e) if attempt >= 30 => {
                        eprintln!("{table} lost {} rows: {e:#}", lines.len());
                        break;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
                }
            }
        }
    });
    tx
}

/// `pondra.history` as the caller may read it: every row for an admin, their own for anyone else.
pub fn visible(ctx: &datafusion::prelude::SessionContext, table: Arc<dyn datafusion::catalog::TableProvider>) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    use datafusion::prelude::{col, lit};
    match crate::auth::current().filter(|p| p.role < crate::auth::Role::Admin || p.access.is_some()) {
        None => Ok(table),
        Some(p) => Ok(ctx.read_table(table)?.filter(col("user").eq(lit(p.name)))?.into_view()),
    }
}

/// `pondra.history` before its first row: the columns, no rows.
pub fn empty() -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
    Ok(Arc::new(datafusion::datasource::MemTable::try_new(crate::query::read_schema(&columns())?, vec![vec![]])?))
}
