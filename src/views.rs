//! Inline views: SQL over each flush of new rows, run by the node that received the rows, whose
//! output commits in the SAME catalog write as its input. No lag behind the source, no progress
//! to track, exactly-once for free, and the work spreads over every node that ingests.
//!
//! * A view without GROUP BY appends its rows to its table: filter, reshape, enrich (it may join
//!   any other table).
//! * A view with GROUP BY is a merge table: each flush adds partial aggregates per key, and reads
//!   combine them (sum, min, max; counts are summed); compaction folds them into one row per key.
//!   So any number of nodes add to the same keys at once, without coordinating.
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
}

pub fn view_key(name: &str) -> String { format!("v/{name}") }

/// Register view `name` (leader only): its table gets the query's output columns; a GROUP BY
/// query makes it a merge table keyed by the group columns.
pub async fn create(lake: &Lake, name: &str, sql: &str) -> Result<()> {
    ensure!(lake.cat.get::<TableMeta>(&table_key(name)).await?.is_none(), "table {name} already exists");
    let source = first_table(sql)?;
    ensure!(lake.cat.get::<TableMeta>(&table_key(&source)).await?.is_some(), "no table {source}");
    let plan = session(lake, sql, "").await?.sql(sql).await?.logical_plan().clone();
    let (key, merge) = merges(&plan)?;
    let columns = plan.schema().fields().iter().map(|f| (f.name().clone(), f.data_type().to_string())).collect();
    let meta = TableMeta { columns, key, merge, ..Default::default() };
    lake.cat.commit(vec![(view_key(name), json(&View { source, sql: sql.into() })), (table_key(name), json(&meta))], &[]).await
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
