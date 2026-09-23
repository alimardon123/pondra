//! Inline views: SQL over each flush of new rows, run by the node that received the rows, whose
//! output commits in the SAME catalog write as its input. No lag behind the source, no progress
//! to track, exactly-once for free, and the work spreads over every node that ingests.
//!
//! * A view without GROUP BY appends its rows to its table: filter, reshape, enrich (it may join
//!   any other table).
//! * A view with GROUP BY is a merge table: each flush adds partial aggregates per key, and reads
//!   combine them (sum, min, max; counts are summed); compaction folds them into one row per key.
//!   So any number of nodes add to the same keys at once, without coordinating.
//! * An event-time window view (GROUP BY a `date_bin(…)` window column, with `emit`) also emits
//!   each window once, final, to `{view}_final` when the watermark passes it: the newest window
//!   started, less the window's size and the allowed lateness. Rows arriving later still update
//!   the view, not what was emitted. Emission is exactly-once: its progress is a producer's seq.
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
}

/// Emit-once windows: `window` is the view's window-start column (a key), windows are
/// `size_secs` long and take rows up to `lateness_secs` late.
#[derive(Serialize, Deserialize, Clone)]
pub struct Emit {
    pub window: String,
    pub size_secs: u64,
    pub lateness_secs: u64,
}

pub fn view_key(name: &str) -> String { format!("v/{name}") }

/// Register view `name` (leader only): its table gets the query's output columns; a GROUP BY
/// query makes it a merge table keyed by the group columns.
pub async fn create(lake: &Lake, name: &str, sql: &str, emit: Option<Emit>) -> Result<()> {
    ensure!(lake.cat.get::<TableMeta>(&table_key(name)).await?.is_none(), "table {name} already exists");
    let source = first_table(sql)?;
    ensure!(lake.cat.get::<TableMeta>(&table_key(&source)).await?.is_some(), "no table {source}");
    let plan = session(lake, sql, "").await?.sql(sql).await?.logical_plan().clone();
    let (key, merge) = merges(&plan)?;
    let columns = plan.schema().fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    let meta = TableMeta { columns, key, merge, publish: default_publish(), ..Default::default() };
    let mut puts = vec![(view_key(name), json(&View { source, sql: sql.into(), emit: emit.clone() })), (table_key(name), json(&meta))];
    if let Some(e) = &emit {
        let is_time = meta.columns.iter().any(|(c, t)| *c == e.window && t.starts_with("Timestamp"));
        ensure!(meta.key.contains(&e.window) && is_time, "emit: the window column must be a GROUP BY timestamp (date_bin(…) AS {})", e.window);
        let columns = meta.columns.iter().filter(|(c, _)| c != "_deleted").cloned().collect();
        puts.push((table_key(&format!("{name}_final")), json(&TableMeta { columns, publish: default_publish(), ..Default::default() })));
    }
    lake.cat.commit(puts, &[]).await
}

/// Leader: emit every window view's windows that the watermark has passed, once each.
pub async fn emit_all(lake: &Lake, log: &crate::log::Log) -> Result<()> {
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        if let Some(e) = &v.emit {
            emit(lake, log, &key[2..], e).await?;
        }
    }
    Ok(())
}

/// The windows of `view` now past the watermark, appended to `{view}_final` with the watermark
/// as the producer's seq (`prev`: the last one), so each window is emitted exactly once.
async fn emit(lake: &Lake, log: &crate::log::Log, view: &str, e: &Emit) -> Result<()> {
    use datafusion::arrow::{array::AsArray, compute::cast, datatypes::{DataType, TimeUnit, TimestampMicrosecondType}};
    let micros = |b: &RecordBatch| -> Result<Option<i64>> {
        let c = cast(b.column(0), &DataType::Timestamp(TimeUnit::Microsecond, None))?;
        Ok(c.as_primitive::<TimestampMicrosecondType>().iter().next().flatten())
    };
    let w = format!("\"{}\"", e.window);
    let newest = crate::query::session(lake, view, "").await?.sql(&format!("SELECT max({w}) FROM \"{view}\"")).await?.collect().await?;
    let Some(newest) = newest.first().map(micros).transpose()?.flatten() else { return Ok(()) };
    let upto = newest - ((e.size_secs + e.lateness_secs) * 1_000_000) as i64; // windows starting at or before this are final
    let (producer, final_table) = (format!("emit:{view}"), format!("{view}_final"));
    let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
    if upto <= done as i64 {
        return Ok(());
    }
    let sql = format!("SELECT * FROM \"{view}\" WHERE {w} > to_timestamp_micros({done}) AND {w} <= to_timestamp_micros({upto}) ORDER BY {w}");
    let rows = crate::query::session(lake, &sql, "").await?.sql(&sql).await?.collect().await?;
    let meta: TableMeta = lake.cat.get(&table_key(&final_table)).await?.context("window view without its final table")?;
    let s = crate::query::schema(&meta.columns)?;
    let rows = datafusion::arrow::compute::concat_batches(&s, &rows.iter().map(|b| crate::query::conform(b, &s)).collect::<Result<Vec<_>>>()?)?;
    let src = crate::log::Src { producer, seq: upto as u64, prev: Some(done) };
    log.append(final_table, src, rows).await?; // (a conflict: another leader emitted first; the next round catches up)
    Ok(())
}

/// The rows every view derives from a flush's new rows, per view table.
pub async fn derive(lake: &Lake, new: &BTreeMap<String, Vec<RecordBatch>>) -> Result<Vec<(String, RecordBatch)>> {
    let mut out = vec![];
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        let Some(rows) = new.get(&v.source) else { continue };
        let target = key[2..].to_string();
        let meta: TableMeta = lake.cat.get(&table_key(&target)).await?.context("view without table")?;
        let batch = over(lake, &v.source, rows.clone(), &v.sql).await?;
        out.push((target, batch.with_schema(crate::query::schema(&meta.columns)?)?));
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
