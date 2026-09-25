//! Inline views: SQL over each flush of new rows, run by the node that received the rows, whose
//! output commits in the SAME catalog write as its input. No lag behind the source, no progress
//! to track, exactly-once for free, and the work spreads over every node that ingests.
//!
//! * A view without GROUP BY appends its rows to its table: filter, reshape, enrich (it may join
//!   any other table, as of each row's time too: `asof.rs`).
//! * A view with GROUP BY is a merge table: each flush adds partial aggregates per key, and reads
//!   combine them (sum, min, max; counts are summed); compaction folds them into one row per key.
//!   So any number of nodes add to the same keys at once, without coordinating.
//!
//! Two kinds of view also emit what is final, once, when the source's event time has moved past
//! it. The watermark is the newest event time the source holds, less the lateness allowed (rows
//! may arrive that much out of order): as Flink's bounded out-of-orderness, from the data itself.
//! Emission is exactly-once: its progress is a producer's seq, committed with what it emits.
//!
//! * A window view (GROUP BY a `date_bin(…)` window column, with `emit`) emits each window to
//!   `{view}_final` once the watermark passes its end. Rows arriving later still update the
//!   view, not what was emitted.
//! * A session view (`sessions`) holds each key's sessions, a session being its rows with no gap
//!   of `gap_secs` between them, each emitted once, when the watermark passes its last row plus
//!   the gap. A row that falls inside a session already emitted is late, and left out.
use crate::query::{first_table, over, session};
use crate::store::*;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{Expr, LogicalPlan};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone)]
pub struct View {
    pub source: String, // the first table in FROM: the stream the view follows
    pub sql: String,
    #[serde(default)]
    pub emit: Option<Emit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sessions: Option<Sessions>,
}

/// Emit-once windows: `window` is the view's window-start column (a key), cut from the source's
/// event-time column `time`; windows are `size_secs` long and take rows up to `lateness_secs` late.
#[derive(Serialize, Deserialize, Clone)]
pub struct Emit {
    pub window: String,
    pub size_secs: u64,
    pub lateness_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<String>, // (None in views made before round 16: found from the SQL)
}

/// Session windows over the source's event-time column `time`, per value of `keys` (the view's
/// GROUP BY), rows up to `lateness_secs` late.
#[derive(Serialize, Deserialize, Clone)]
pub struct Sessions {
    pub time: String,
    pub gap_secs: u64,
    pub lateness_secs: u64,
    #[serde(default)]
    pub keys: Vec<String>,
}

pub fn view_key(name: &str) -> String { format!("v/{name}") }

/// `CREATE MATERIALIZED VIEW … WITH (window = 'w', size_secs = 60, lateness_secs = 10)` or
/// `WITH (session = 'ts', gap_secs = 1800, lateness_secs = 5)`: what `POST /views/{v}?…` takes.
pub fn options(kv: &std::collections::BTreeMap<String, String>) -> Result<(Option<Emit>, Option<Sessions>)> {
    if let Some(k) = kv.keys().find(|k| !["window", "size_secs", "lateness_secs", "session", "gap_secs"].contains(&k.as_str())) {
        bail!("{k}: a materialized view's options are window, size_secs, lateness_secs, session and gap_secs");
    }
    let num = |k: &str, d: u64| kv.get(k).map_or(Ok(d), |v| v.parse::<u64>().map_err(|_| anyhow::anyhow!("{k} is a number of seconds")));
    let lateness_secs = num("lateness_secs", 0)?;
    let emit = kv.get("window").map(|w| Ok::<_, anyhow::Error>(Emit { window: w.clone(), size_secs: num("size_secs", 60)?, lateness_secs, time: None })).transpose()?;
    let sessions = kv.get("session").map(|t| Ok::<_, anyhow::Error>(Sessions { time: t.clone(), gap_secs: num("gap_secs", 1800)?, lateness_secs, keys: vec![] })).transpose()?;
    Ok((emit, sessions))
}
/// A session view's bound: no session still open starts before this (µs).
fn open_key(name: &str) -> String { format!("w/{name}") }

/// Register view `name` (leader only): its table gets the query's output columns; a GROUP BY
/// query makes it a merge table keyed by the group columns.
pub async fn create(lake: &Lake, name: &str, sql: &str, mut emit: Option<Emit>, sessions: Option<Sessions>) -> Result<()> {
    if let Some(v) = lake.cat.get::<View>(&view_key(name)).await? {
        let windows = |e: &Option<Emit>| e.as_ref().map(|e| (e.window.clone(), e.size_secs, e.lateness_secs));
        let gaps = |s: &Option<Sessions>| s.as_ref().map(|s| (s.time.clone(), s.gap_secs, s.lateness_secs));
        ensure!(v.sql == sql && windows(&v.emit) == windows(&emit) && gaps(&v.sessions) == gaps(&sessions), "view {name} already exists, with other SQL or options");
        return Ok(()); // (asked again, the same: a notebook cell run twice)
    }
    ensure!(lake.cat.get::<TableMeta>(&table_key(name)).await?.is_none(), "table {name} already exists");
    let (other, source) = crate::ddl::resolve(lake, &first_table(sql)?).await?;
    ensure!(other.is_none(), "a view follows a table of this lake");
    let src: TableMeta = lake.cat.get(&table_key(&source)).await?.with_context(|| format!("no table {source}"))?;
    if let Some(s) = sessions {
        ensure!(emit.is_none(), "a view emits windows or sessions, not both");
        return create_sessions(lake, name, sql, source, &src, s).await;
    }
    let planned = crate::asof::rewrite(sql)?;
    let plan = session(lake, &planned, "").await?.sql(&planned).await?.logical_plan().clone();
    let (key, merge) = merges(&plan)?;
    let columns = plan.schema().fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    let meta = TableMeta { columns, key, merge, publish: default_publish(), ..Default::default() };
    let mut puts = vec![(table_key(name), json(&meta))];
    if let Some(e) = &mut emit {
        let is_time = meta.columns.iter().any(|(c, t)| *c == e.window && t.starts_with("Timestamp"));
        ensure!(meta.key.contains(&e.window) && is_time, "emit: the window column must be a GROUP BY timestamp (date_bin(…) AS {})", e.window);
        e.time = event_time(&plan, &e.window).filter(|t| timestamp(&src, t));
        ensure!(e.time.is_some(), "emit: the window must be cut from a timestamp column of {source}: date_bin(INTERVAL '1 minute', ts) AS {}", e.window);
        let columns = meta.columns.iter().filter(|(c, _)| c != "_deleted").cloned().collect();
        puts.push((table_key(&format!("{name}_final")), json(&TableMeta { columns, publish: default_publish(), ..Default::default() })));
    }
    puts.push((view_key(name), json(&View { source, sql: sql.into(), emit, sessions: None })));
    lake.cat.commit(puts, &[]).await
}

/// A session view: the SQL runs over each closed session's rows, grouped by session too, so its
/// table gets the SQL's columns and `session_start`, `session_end`.
async fn create_sessions(lake: &Lake, name: &str, sql: &str, source: String, src: &TableMeta, mut s: Sessions) -> Result<()> {
    ensure!(timestamp(src, &s.time), "sessions: {} is not a timestamp column of {source}", s.time);
    ensure!(s.gap_secs > 0, "sessions: the gap must be at least a second");
    let with = sessionized(sql)?;
    let ctx = crate::query::over_ctx(lake, &source, extended(src, &s.time)?, vec![], &with).await?;
    let plan = ctx.sql(&with).await?.logical_plan().clone();
    let Some(LogicalPlan::Aggregate(agg)) = top_aggregate(&plan) else { bail!("a session view is SELECT … FROM {source} GROUP BY <its key columns>") };
    for g in &agg.group_expr {
        match g {
            Expr::Column(c) if c.name == "session_start" || c.name == "session_end" => {}
            Expr::Column(c) if src.columns.iter().any(|(n, _)| *n == c.name) => s.keys.push(c.name.clone()),
            other => bail!("a session view groups by columns of {source}, not {other}"),
        }
    }
    ensure!(!s.keys.is_empty(), "a session view groups by a key: GROUP BY user");
    let out = plan.schema();
    ensure!(s.keys.iter().all(|k| out.field_with_unqualified_name(k).is_ok()), "a session view SELECTs its GROUP BY columns, as they are named");
    let columns = out.fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    let meta = TableMeta { columns, publish: default_publish(), ..Default::default() };
    let view = View { source, sql: sql.into(), emit: None, sessions: Some(s) };
    lake.cat.commit(vec![(view_key(name), json(&view)), (table_key(name), json(&meta))], &[]).await
}

fn timestamp(meta: &TableMeta, column: &str) -> bool { meta.columns.iter().any(|(c, t)| c == column && t.starts_with("Timestamp")) }

/// The source's columns and the two a session view's SQL also sees: `session_start`, `session_end`.
fn extended(src: &TableMeta, time: &str) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    use datafusion::arrow::datatypes::{Field, Schema};
    let s = crate::query::schema(&src.columns)?;
    let t = s.field_with_name(time)?.data_type().clone();
    let mut fields = s.fields().to_vec();
    fields.extend(["session_start", "session_end"].map(|n| std::sync::Arc::new(Field::new(n, t.clone(), true))));
    Ok(std::sync::Arc::new(Schema::new(fields)))
}

/// A session view's SQL grouped by session too: `session_start` and `session_end` join its
/// SELECT and its GROUP BY (where it doesn't name them itself).
fn sessionized(sql: &str) -> Result<String> {
    use datafusion::sql::sqlparser::{ast, dialect::GenericDialect, parser::Parser};
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let plain = "a session view is one SELECT … GROUP BY <its key columns>";
    let [ast::Statement::Query(q)] = &mut stmts[..] else { bail!(plain) };
    let ast::SetExpr::Select(s) = q.body.as_mut() else { bail!(plain) };
    let ast::GroupByExpr::Expressions(by, _) = &mut s.group_by else { bail!(plain) };
    for c in ["session_start", "session_end"] {
        let e = ast::Expr::Identifier(ast::Ident::new(c));
        if !s.projection.iter().any(|p| p.to_string() == c) {
            s.projection.push(ast::SelectItem::UnnamedExpr(e.clone()));
        }
        if !by.iter().any(|b| b.to_string() == c) {
            by.push(e);
        }
    }
    Ok(stmts[0].to_string())
}

/// The source column a window column is cut from: the one column its GROUP BY expression reads.
fn event_time(plan: &LogicalPlan, window: &str) -> Option<String> {
    let LogicalPlan::Projection(p) = plan else { return None };
    let LogicalPlan::Aggregate(a) = p.input.as_ref() else { return None };
    let i = plan.schema().fields().iter().position(|f| f.name() == window)?;
    let Expr::Column(c) = p.expr[i].clone().unalias_nested().data else { return None };
    let columns = a.group_expr.get(a.schema.index_of_column(&c).ok()?)?.column_refs();
    let [c] = columns.into_iter().collect::<Vec<_>>()[..] else { return None };
    Some(c.name.clone())
}

/// Leader: emit what the watermark has passed, once, for every window and session view (one
/// that fails is tried again next round; the others go on).
pub async fn emit_all(lake: &Lake, log: &crate::log::Log) -> Result<()> {
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        let done = match (&v.emit, &v.sessions) {
            (Some(e), _) => emit(lake, log, &key[2..], &v, e).await,
            (_, Some(s)) => sessions(lake, log, &key[2..], &v, s).await,
            _ => Ok(()),
        };
        if let Err(e) = done {
            eprintln!("emission of {}: {e:#}", &key[2..]);
        }
    }
    Ok(())
}

/// The newest event time `table` holds in `time`, in µs: what its files' ranges say, then each
/// log row after them, read once (kept in memory per table; it only grows, as a watermark does).
async fn newest(lake: &Lake, table: &str, time: &str) -> Result<Option<i64>> {
    use datafusion::arrow::{array::AsArray, compute::{cast, max}, datatypes::{DataType, TimeUnit, TimestampMicrosecondType}};
    use datafusion::common::ScalarValue;
    use std::sync::{LazyLock, Mutex};
    static SEEN: LazyLock<Mutex<std::collections::HashMap<String, (u64, Option<i64>)>>> = LazyLock::new(Default::default);
    let key = format!("{}|{table}|{time}", lake.url);
    let meta: TableMeta = lake.cat.get(&table_key(table)).await?.with_context(|| format!("no table {table}"))?;
    let upto = lake.visible();
    let (mut seen, mut newest) = SEEN.lock().unwrap().get(&key).copied().unwrap_or((0, None));
    let us = DataType::Timestamp(TimeUnit::Microsecond, None);
    if seen < meta.tiered {
        // (rows went into files since: their ranges say how new they were)
        let s = crate::query::schema(&meta.columns)?;
        let ranges = crate::manifest::ranges(table, &crate::manifest::list(lake, &meta).await?, &meta.files, &s);
        if let Some((_, hi)) = ranges.get(time) {
            if let ScalarValue::TimestampMicrosecond(v, _) = ScalarValue::try_from_string(hi.clone(), s.field_with_name(time)?.data_type())?.cast_to(&us)? {
                newest = newest.max(v);
            }
        }
        seen = meta.tiered;
    }
    if upto > seen {
        for b in crate::query::tail(lake, table, seen, Some(upto), false).await? {
            if let Some(c) = b.column_by_name(time) {
                newest = newest.max(max(cast(c, &us)?.as_primitive::<TimestampMicrosecondType>()));
            }
        }
        seen = upto;
    }
    SEEN.lock().unwrap().insert(key, (seen, newest));
    Ok(newest)
}

/// A timestamp column's values in µs.
fn micros(b: &RecordBatch, column: &str) -> Result<datafusion::arrow::array::TimestampMicrosecondArray> {
    use datafusion::arrow::{array::AsArray, compute::cast, datatypes::{DataType, TimeUnit, TimestampMicrosecondType}};
    let c = b.column_by_name(column).with_context(|| format!("no column {column}"))?;
    Ok(cast(c, &DataType::Timestamp(TimeUnit::Microsecond, None))?.as_primitive::<TimestampMicrosecondType>().clone())
}

fn quoted(c: &str) -> String { format!("\"{}\"", c.replace('"', "\"\"")) }

/// The windows of `view` now past the watermark, appended to `{view}_final` with the watermark
/// as the producer's seq (`prev`: the last one), so each window is emitted exactly once.
async fn emit(lake: &Lake, log: &crate::log::Log, view: &str, v: &View, e: &Emit) -> Result<()> {
    let time = match &e.time {
        Some(t) => t.clone(),
        None => event_time(session(lake, &v.sql, "").await?.sql(&crate::asof::rewrite(&v.sql)?).await?.logical_plan(), &e.window).context("a window view whose window has no event-time column")?,
    };
    let Some(newest) = newest(lake, &v.source, &time).await? else { return Ok(()) };
    let upto = newest - ((e.size_secs + e.lateness_secs) * 1_000_000) as i64; // windows starting at or before this have ended
    let (producer, final_table) = (format!("emit:{view}"), format!("{view}_final"));
    let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    if upto <= done as i64 {
        return Ok(());
    }
    let w = quoted(&e.window);
    let sql = format!("SELECT * FROM {} WHERE {w} > to_timestamp_micros({done}) AND {w} <= to_timestamp_micros({upto}) ORDER BY {w}", quoted(view));
    let rows = session(lake, &sql, "").await?.sql(&sql).await?.collect().await?;
    append(lake, log, &final_table, crate::log::Src { producer, seq: upto as u64, prev: Some(done) }, rows).await
}

/// `rows` into `table` (as its columns), with `src`: output and progress commit together. (A
/// conflict: another leader emitted first; the next round catches up.)
async fn append(lake: &Lake, log: &crate::log::Log, table: &str, src: crate::log::Src, rows: Vec<RecordBatch>) -> Result<()> {
    let meta: TableMeta = lake.cat.get(&table_key(table)).await?.with_context(|| format!("no table {table}"))?;
    let s = crate::query::schema(&meta.columns)?;
    let rows = datafusion::arrow::compute::concat_batches(&s, &rows.iter().map(|b| crate::query::conform(b, &s)).collect::<Result<Vec<_>>>()?)?;
    log.append(table.to_string(), src, rows).await?;
    Ok(())
}

/// The sessions of `view` the watermark has passed: each key's rows cut where a gap of
/// `gap_secs` falls, over the rows that may still be in a session not yet emitted (from the
/// earliest start of those still open), leaving out rows inside a session already emitted. The
/// closed ones run through the view's SQL and are appended to its table, with the watermark as
/// the producer's seq, as windows are.
async fn sessions(lake: &Lake, log: &crate::log::Log, view: &str, v: &View, s: &Sessions) -> Result<()> {
    let Some(newest) = newest(lake, &v.source, &s.time).await? else { return Ok(()) };
    let wm = newest - (s.lateness_secs * 1_000_000) as i64; // (rows at or before it are late)
    let producer = format!("emit:{view}");
    let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    if wm <= done as i64 {
        return Ok(());
    }
    let from: Option<i64> = lake.cat.get(&open_key(view)).await?;
    let (t, by, at) = (quoted(&s.time), s.keys.iter().map(|k| quoted(k)).collect::<Vec<_>>().join(", "), |us: i64| format!("to_timestamp_micros({us})"));
    let on = s.keys.iter().map(|k| format!("e.{0} IS NOT DISTINCT FROM l.{0}", quoted(k))).collect::<Vec<_>>().join(" AND ");
    let gap = format!("INTERVAL '{} seconds'", s.gap_secs);
    let (recent, since) = from.map_or_else(Default::default, |f| (format!(" WHERE session_end > {}", at(f)), format!(" AND e.{t} >= {}", at(f))));
    let sql = format!(
        "WITH _last AS (SELECT {by}, max(session_end) AS _end FROM {view_}{recent} GROUP BY {by}), \
         _rows AS (SELECT e.* FROM {source} e LEFT JOIN _last l ON {on} WHERE e.{t} <= {wm}{since} AND (l._end IS NULL OR e.{t} >= l._end)), \
         _gaps AS (SELECT *, CASE WHEN lag({t}) OVER (PARTITION BY {by} ORDER BY {t}) > {t} - {gap} THEN 0 ELSE 1 END AS _new FROM _rows), \
         _ids AS (SELECT *, sum(_new) OVER (PARTITION BY {by} ORDER BY {t} ROWS UNBOUNDED PRECEDING) AS _sid FROM _gaps) \
         SELECT * EXCLUDE (_new, _sid), min({t}) OVER (PARTITION BY {by}, _sid) AS session_start, max({t}) OVER (PARTITION BY {by}, _sid) + {gap} AS session_end FROM _ids",
        view_ = quoted(view), source = quoted(&v.source), wm = at(wm),
    );
    let rows = session(lake, &sql, "").await?.sql(&sql).await?.collect().await?;
    let (mut closed, mut open) = (vec![], wm);
    for b in &rows {
        let (start, end) = (micros(b, "session_start")?, micros(b, "session_end")?);
        let keep: datafusion::arrow::array::BooleanArray = end.iter().map(|e| e.map(|e| e > done as i64 && e <= wm)).collect();
        open = start.iter().zip(end.iter()).filter_map(|(s, e)| s.filter(|_| e.is_some_and(|e| e > wm))).fold(open, i64::min);
        closed.push(datafusion::arrow::compute::filter_record_batch(b, &keep)?);
    }
    let src: TableMeta = lake.cat.get(&table_key(&v.source)).await?.context("a session view without its source")?;
    let with = sessionized(&v.sql)?;
    let out = crate::query::over_ctx(lake, &v.source, extended(&src, &s.time)?, closed, &with).await?.sql(&with).await?.collect().await?;
    append(lake, log, view, crate::log::Src { producer, seq: wm as u64, prev: Some(done) }, out).await?;
    lake.cat.commit(vec![(open_key(view), json(&open))], &[]).await // (a lower bound: stale is safe, only slower)
}

/// The rows every view derives from a flush's new rows, per view table.
pub async fn derive(lake: &Lake, new: &BTreeMap<String, Vec<RecordBatch>>) -> Result<Vec<(String, RecordBatch)>> {
    let mut out = vec![];
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        let Some(rows) = new.get(&v.source).filter(|_| v.sessions.is_none()) else { continue }; // (sessions are cut as they close)
        let target = key[2..].to_string();
        let meta: TableMeta = lake.cat.get(&table_key(&target)).await?.context("view without table")?;
        let batch = over(lake, &v.source, rows.clone(), &v.sql).await?;
        out.push((target, crate::query::cast_as(&batch, &crate::query::schema(&meta.columns)?)?));
    }
    Ok(out)
}

/// For a GROUP BY query: its key columns and how each aggregate column merges. Only aggregates
/// that combine from partial results qualify.
fn merges(plan: &LogicalPlan) -> Result<(Vec<String>, BTreeMap<String, String>)> {
    let (mut key, mut merge) = (vec![], BTreeMap::new());
    let Some(LogicalPlan::Aggregate(agg)) = top_aggregate(plan) else { return Ok((key, merge)) }; // no GROUP BY: rows are appended
    let plain = "an aggregating view must be a plain SELECT … GROUP BY (no HAVING, ORDER BY or LIMIT)";
    let LogicalPlan::Projection(p) = plan else { bail!(plain) };
    ensure!(matches!(p.input.as_ref(), LogicalPlan::Aggregate(_)), plain);
    for (e, f) in p.expr.iter().zip(plan.schema().fields()) {
        let Expr::Column(c) = e.clone().unalias_nested().data else { bail!("column {} must be a group key or one aggregate: {e}", f.name()) };
        let i = agg.schema.index_of_column(&c)?;
        if i < agg.group_expr.len() {
            key.push(f.name().clone());
            continue;
        }
        let Expr::AggregateFunction(a) = agg.aggr_expr[i - agg.group_expr.len()].clone().unalias() else { bail!("unexpected aggregate") };
        ensure!(!a.params.distinct, "DISTINCT aggregates can't be combined from partial results");
        let m = match a.func.name() {
            "sum" | "count" => "sum",
            "min" => "min",
            "max" => "max",
            other => bail!("{other}() can't be combined from partial results: use sum, count, min or max (e.g. avg = sum / count at query time)"),
        };
        merge.insert(f.name().clone(), m.to_string());
    }
    ensure!(!key.is_empty(), "an aggregating view needs GROUP BY columns in its SELECT");
    Ok((key, merge))
}

/// The query's own GROUP BY, if any: the first Aggregate below its top-level projection, sort,
/// limit and filter nodes (aggregates inside joined tables don't count).
fn top_aggregate(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate(_) => Some(plan),
        LogicalPlan::Projection(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::Filter(_) => top_aggregate(plan.inputs()[0]),
        _ => None,
    }
}
