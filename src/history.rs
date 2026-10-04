//! The query history (`pondra.history`, ADR-048): every statement a door was sent, a row each:
//! who ran it, through which door and from where, on which node and in which session, how long it
//! took, how many rows it answered and how it ended. Kept `PONDRA_HISTORY_DAYS` (7) in a table of
//! the lake's own (`pondra$history`, hidden); an admin reads every row, anyone else their own.
//!
//! A statement slower than `PONDRA_SLOW_MS` (1000) also keeps its plan, each operator with the rows
//! it expected beside the rows it got and its time, and its trace: the time each node took for its
//! share, a step at a time when it ran across the nodes. It writes one line to the node's log too
//! (the slow-query log). So does a query whose joins came out ten times off what was expected.
//!
//! Every row says what it ran (ADR-050): its `fingerprint` (the statement with its literals taken
//! out, so one query's runs group together), its `plan_id` (the shape of the plan that ran), the
//! `version` it read at (its answer again: `t AT (VERSION => n)`), the tables and views it `reads`
//! and those it `writes`, and, for a query of `PONDRA_LEARN_MS` (100) or more, its `misestimate`: how
//! many times its joins' rows were off what the planner expected, at the worst of them.
//!
//! Nothing waits for any of it. Rows go to a writer on each node, which appends what came in a
//! second. At most `PONDRA_HISTORY_RATE` (500) rows a second are written a node: past that, the
//! fast statements that went well are counted in one row, not written one by one. A slow or failed
//! statement is always written. `PONDRA_HISTORY=off` keeps none.
use crate::server::App;
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, ListBuilder, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray};
use datafusion::physical_plan::ExecutionPlan;
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
    version: Option<u64>,
    plan_id: Option<String>,
    misestimate: Option<f64>,
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

/// The commit its query read at (its statement's last query, as `rows`).
pub fn read_at(version: u64) { with(|_, note| note.version = Some(version)); }

/// A plan it ran: its shape always; how far its joins were from what was expected when it took
/// `PONDRA_LEARN_MS` or more; the plan itself, each operator's rows beside the rows it expected, when
/// it is slow by now or that was ten times off.
pub fn planned(plan: &Arc<dyn ExecutionPlan>) {
    with(|start, note| {
        note.plan_id = Some(plan_id(plan));
        let took = start.elapsed();
        note.misestimate = (took >= learn()).then(|| misestimate(plan)).flatten();
        if took >= slow() || note.misestimate.is_some_and(|m| m >= 10.0) {
            let text = datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(plan.as_ref()).set_show_statistics(true).indent(true).to_string();
            note.plan = Some(cut(expected(&text), 64 << 10));
        }
    });
}

/// A plan's text with each operator's estimate as `expected_rows=n` before its metrics, in place of
/// DataFusion's `statistics=[…]` (its every column's range and bytes, too long to read): `EXPLAIN`'s
/// physical plans, `EXPLAIN ANALYZE`'s and the plans kept here. An operator nothing is known about
/// shows none.
pub fn expected(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let Some(at) = line.rfind(", statistics=[Rows=") else {
            out.push_str(line);
            continue;
        };
        let (head, stats) = (&line[..at], &line[at + ", statistics=[Rows=".len()..]);
        let rows = stats.split(',').next().unwrap_or("");
        let rows = rows.strip_prefix("Exact(").or_else(|| rows.strip_prefix("Inexact(")).and_then(|r| r.strip_suffix(')'));
        let (op, metrics) = head.find(", metrics=[").map_or((head, ""), |m| head.split_at(m));
        out.push_str(op);
        if let Some(rows) = rows {
            out.push_str(if op.contains(':') { ", " } else { ": " });
            out.push_str("expected_rows=");
            out.push_str(rows);
        }
        out.push_str(metrics);
        out.push_str(if line.ends_with('\n') { "\n" } else { "" });
    }
    out
}

/// `EXPLAIN`'s rows with each operator's estimate made readable (`expected`).
pub fn explained(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    batches.into_iter().map(|b| {
        let schema = b.schema();
        let columns = b.columns().iter().zip(schema.fields()).map(|(c, f)| match (f.name().as_str(), c.as_any().downcast_ref::<StringArray>()) {
            ("plan", Some(text)) => Arc::new(text.iter().map(|t| t.map(expected)).collect::<StringArray>()) as ArrayRef,
            _ => c.clone(),
        });
        Ok(RecordBatch::try_new(schema.clone(), columns.collect())?)
    }).collect()
}

/// The worst ratio between a join's expected and actual rows, where either is 1,000 or more: where a
/// join order or a build side went by a wrong guess. Joins only: a scan's rows also drop by the
/// filters a join or a top-N hands it as it runs (dynamic filters), which is no misestimate.
fn misestimate(plan: &Arc<dyn ExecutionPlan>) -> Option<f64> {
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    let (mut worst, mut todo) = (None::<f64>, vec![plan.clone()]);
    while let Some(p) = todo.pop() {
        todo.extend(p.children().into_iter().cloned());
        if !p.name().contains("Join") {
            continue;
        }
        let (Some(actual), Ok(s)) = (p.metrics().and_then(|m| m.output_rows()), StatisticsContext::new().compute(p.as_ref(), &StatisticsArgs::new())) else { continue };
        let Some(&expected) = s.num_rows.get_value() else { continue };
        let (most, least) = (actual.max(expected), actual.min(expected));
        if most >= 1000 {
            let off = most as f64 / least.max(1) as f64;
            worst = Some(worst.map_or(off, |w| w.max(off)));
        }
    }
    worst.map(|w| (w * 10.0).round() / 10.0)
}

/// A plan's shape, as a short hash: its operators in their places and the columns each leaf reads,
/// so a join order or a build side changed is another plan, while its literals, its files and the
/// gathers the hot columns add or take away (invariant 34) are not.
fn plan_id(plan: &Arc<dyn ExecutionPlan>) -> String {
    fn shape(p: &Arc<dyn ExecutionPlan>, out: &mut String) {
        let kids = p.children();
        if p.name() == "CoalescePartitionsExec" {
            return kids.into_iter().for_each(|k| shape(k, out));
        }
        out.push_str(p.name());
        out.push('(');
        match kids.is_empty() {
            true => p.schema().fields().iter().for_each(|f| out.extend([f.name().as_str(), ","])),
            false => kids.into_iter().for_each(|k| {
                shape(k, out);
                out.push(';');
            }),
        }
        out.push(')');
    }
    let mut text = String::new();
    shape(plan, &mut text);
    format!("{:016x}", fnv(text.as_bytes()))
}

/// A statement as the same statement asked again: its literals `?` (a list of them one `?`), its
/// words lower-cased, its comments and spacing left out; a short hash of that. One query's runs,
/// whatever values they were asked with (Postgres's queryid, Snowflake's query hash).
pub fn fingerprint(sql: &str) -> Option<String> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};
    let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?;
    let mut words: Vec<String> = vec![];
    for t in tokens {
        let word = match t {
            Token::Whitespace(_) => continue,
            Token::Number(..) | Token::SingleQuotedString(_) | Token::NationalStringLiteral(_) | Token::EscapedStringLiteral(_) | Token::HexStringLiteral(_) | Token::DollarQuotedString(_) | Token::SingleQuotedByteStringLiteral(_) | Token::DoubleQuotedByteStringLiteral(_) => "?".to_string(),
            Token::Word(w) if w.quote_style.is_none() => w.value.to_lowercase(),
            t => t.to_string(),
        };
        if word == "?" && words.len() >= 2 && words[words.len() - 1] == "," && words[words.len() - 2] == "?" {
            words.pop(); // (`IN (1, 2, 3)` and `IN (1, 2)` are one query)
            continue;
        }
        words.push(word);
    }
    Some(format!("{:016x}", fnv(words.join(" ").as_bytes())))
}

/// FNV-1a: a hash that is the same in every build and on every machine, so fingerprints and plan
/// ids group a lake's rows across releases.
fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ *b as u64).wrapping_mul(0x100_0000_01b3))
}

fn learn() -> Duration {
    static L: OnceLock<Duration> = OnceLock::new();
    *L.get_or_init(|| Duration::from_millis(std::env::var("PONDRA_LEARN_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(100)))
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
    sql: String, // (as sent: its fingerprint and the tables it reads are worked out by the writer, off its path)
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
    let line = Line { at: crate::log::now_ms(), id: crate::runs::new_id(), user, door, from, session: crate::temp::current(), class, statement, sql: sql.to_string(), outcome, error, ms: took.as_millis() as i64, note };
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
            let _ = tx.send(Line { at: crate::log::now_ms(), id: crate::runs::new_id(), user: String::new(), door: "node", from: None, session: None, class: "skipped", statement, sql: String::new(), outcome: "ok", error: None, ms: 0, note: Note { rows: Some(skipped), ..Default::default() } });
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
    // (stored columns only grow, at the end: a lake's history made before keeps its rows, `create_log`)
    vec![("at".into(), ts), text("id"), text("user"), text("door"), text("from"), text("node"), text("session"), text("class"), text("statement"), text("outcome"), text("error"), int("ms"), int("rows"), int("nodes"), text("plan"), text("trace"),
         text("fingerprint"), text("plan_id"), int("version"), ("reads".into(), "Utf8[]".into()), ("misestimate".into(), "Float64".into()), ("writes".into(), "Utf8[]".into())]
}

/// Leader: the table, made the first time a node has a row for it (`Ddl::HistoryLog`), or given
/// the columns added since its lake's history was made.
pub async fn create_log(lake: &Lake) -> Result<serde_json::Value> {
    match lake.cat.get::<TableMeta>(&table_key(TABLE)).await? {
        None => {
            let days: u64 = std::env::var("PONDRA_HISTORY_DAYS").ok().and_then(|d| d.parse().ok()).unwrap_or(7);
            let meta = TableMeta { columns: columns(), ttl: Some(("at".into(), days * 86400)), tiered: lake.visible(), ..Default::default() };
            lake.cat.commit(vec![(table_key(TABLE), json(&meta))], &[]).await?;
        }
        Some(mut meta) if meta.columns.len() < columns().len() && columns().starts_with(&meta.columns) => {
            meta.columns = columns();
            lake.cat.commit(vec![(table_key(TABLE), json(&meta))], &[]).await?;
        }
        Some(_) => {}
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
        text(&|l| (l.class != "skipped").then(|| fingerprint(&l.sql)).flatten()),
        text(&|l| l.note.plan_id.clone()),
        int(&|l| l.note.version.map(|v| v as i64)),
    ];
    let [reads, writes] = touched(app, lines);
    let arrays = [arrays, vec![reads, Arc::new(lines.iter().map(|l| l.note.misestimate).collect::<Float64Array>()) as ArrayRef, writes]].concat();
    Ok(RecordBatch::try_new(crate::query::schema(&columns())?, arrays)?)
}

/// The tables and views each statement read, and the tables it changed, as this lake names them
/// (another lake's as `lake.schema.t`): every relation it names but an INSERT's or a CREATE TABLE …
/// AS's own; and the table an INSERT, UPDATE, DELETE, MERGE or CREATE TABLE … AS writes.
fn touched(app: &App, lines: &[Line]) -> [ArrayRef; 2] {
    let (mut reads, mut writes) = (ListBuilder::new(StringBuilder::new()), ListBuilder::new(StringBuilder::new()));
    let local = |n: &String| crate::ddl::local(&app.lake, n).unwrap_or_else(|| n.clone());
    for l in lines {
        use crate::write::Stmt;
        let stmt = matches!(l.class, "write" | "ddl").then(|| crate::write::parse(&l.sql)).flatten();
        let target = match &stmt {
            Some(s @ (Stmt::Insert(..) | Stmt::InsertInto(..) | Stmt::Create(_) | Stmt::Define(..) | Stmt::Update(..) | Stmt::Delete(..) | Stmt::Merge(_))) => Some(local(&s.table())),
            _ => None,
        };
        let pure = matches!(stmt, Some(Stmt::Insert(..) | Stmt::InsertInto(..) | Stmt::Create(_) | Stmt::Define(..))); // (its target isn't read)
        match relations(&l.sql) {
            Some(names) => {
                names.iter().map(local).filter(|n| !(pure && Some(n) == target.as_ref())).for_each(|n| reads.values().append_value(n));
                reads.append(true);
            }
            None => reads.append(false),
        }
        match &target {
            Some(t) => {
                writes.values().append_value(t);
                writes.append(true);
            }
            _ => writes.append(false),
        }
    }
    [Arc::new(reads.finish()), Arc::new(writes.finish())]
}

/// Every relation one statement names, anywhere in it, once each, CTE names left out (as
/// `spmd::tables`, for any statement).
fn relations(sql: &str) -> Option<Vec<String>> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    use std::ops::ControlFlow;
    #[derive(Default)]
    struct Names {
        tables: Vec<String>,
        ctes: std::collections::HashSet<String>,
    }
    fn name(i: &Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }
    impl Visitor for Names {
        type Break = ();
        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
            self.ctes.extend(q.with.iter().flat_map(|w| &w.cte_tables).map(|c| name(&c.alias.name)));
            ControlFlow::Continue(())
        }
        fn pre_visit_relation(&mut self, r: &ObjectName) -> ControlFlow<()> {
            let parts: Option<Vec<String>> = r.0.iter().map(|p| p.as_ident().map(name)).collect();
            self.tables.extend(parts.map(|p| p.join(".")));
            ControlFlow::Continue(())
        }
    }
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [stmt] = &stmts[..] else { return None };
    let mut names = Names::default();
    let _ = stmt.visit(&mut names);
    let mut seen = std::collections::HashSet::new();
    let tables: Vec<String> = names.tables.into_iter().filter(|t| !names.ctes.contains(t) && seen.insert(t.clone())).collect();
    (!tables.is_empty()).then_some(tables)
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
                    let rows = batch(&app, &lines)?;
                    if !made && app.lake.cat.get::<TableMeta>(&table_key(table)).await?.is_none_or(|m| m.columns.len() < rows.num_columns()) {
                        crate::write::on_node_as(&app, crate::write::Stmt::Ddl(vec![make.clone()]), None, false).await?; // (made, or given the columns this build adds)
                    }
                    made = true;
                    app.log()?.append(table.into(), crate::log::Src { producer: producer.clone(), seq, prev: None }, rows).await
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
