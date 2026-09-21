//! Distributed queries, SPMD style. Every live node runs the same SQL over its own slice of the
//! query's main table (the first table in FROM), reading those files straight from the bucket,
//! up to the plan's first exchange; for an aggregation that is the partial aggregate. The node
//! that received the query merges everyone's partial results and finishes the plan. No shuffles
//! and no coordinator service: the only data crossing the network is the (small) partial results.
//! Every other table in the query is read whole by each node (broadcast), so star joins work.
//!
//! Which queries: one SELECT (no subqueries, CTEs or set operations), inner joins only, over an
//! append table. The cut is taken from DataFusion's own parallel plan, so it is correct for any
//! aggregate DataFusion can split (avg, count distinct, …); anything else runs on one node.
use crate::query::{first_table, raw, session};
use crate::store::*;
use anyhow::{ensure, Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::execution_plan::replace_children_if_necessary;
use datafusion::physical_plan::{collect_partitioned, ExecutionPlan, ExecutionPlanProperties, Partitioning};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Tables smaller than this aren't worth spreading.
const SPREAD_BYTES: u64 = 256 << 20;

/// One node's share of a query: files of the main table, or its log tail (segments after..=upto).
#[derive(Serialize, Deserialize, Clone)]
pub struct Slice {
    pub sql: String,
    pub table: String,
    pub files: Vec<DataFile>,
    pub tail: Option<(u64, u64)>,
}

/// Run `sql` across `nodes` (this node is `me`), or None if it should just run here.
pub async fn query(lake: &Lake, nodes: &[String], me: &str, sql: &str, force: bool) -> Result<Option<Vec<RecordBatch>>> {
    if nodes.len() < 2 || !spreadable(sql) {
        return Ok(None);
    }
    let table = first_table(sql)?;
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&table)).await? else { return Ok(None) };
    let bytes: u64 = meta.files.iter().map(|f| f.bytes).sum();
    if !meta.key.is_empty() || meta.files.len() < nodes.len() || (!force && bytes < SPREAD_BYTES) {
        return Ok(None);
    }
    // Deal the files round-robin; the log tail is one more slice, run here.
    let new = |files, tail| Slice { sql: sql.into(), table: table.clone(), files, tail };
    let mut slices: Vec<Slice> = nodes.iter().map(|_| new(vec![], None)).collect();
    for (i, f) in meta.files.iter().enumerate() {
        slices[i % nodes.len()].files.push(f.clone());
    }
    let tail = new(vec![], Some((meta.tiered, lake.visible())));
    let mine = nodes.iter().position(|n| n == me).context("not a member")?;
    let (ctx, plan) = plan(lake, &slices[mine]).await?;
    let Some(cut) = find_cut(&plan) else { return Ok(None) };
    // Every node computes its partial result at the same time (this one: its files and the tail).
    let runs = nodes.iter().zip(&slices).map(|(node, s)| async move {
        match node == me {
            true => Ok(vec![]), // below
            false => remote(node, s).await,
        }
    });
    let ours = async { Ok(collect_partitioned(cut.children()[0].clone(), ctx.task_ctx()).await?) };
    let tail = async {
        let (after, upto) = tail.tail.expect("tail slice");
        match upto > after { true => stage(lake, &tail).await, false => Ok(vec![]) } // no log tail: nothing to do
    };
    let (mut parts, ours, tail) = futures::future::try_join3(futures::future::try_join_all(runs), ours, tail).await?;
    parts.extend([ours, tail]);
    let schema = cut.children()[0].schema();
    let parts: Vec<Vec<RecordBatch>> = parts.into_iter().flatten().collect();
    ensure!(parts.iter().flatten().all(|b| b.schema().fields() == schema.fields()), "nodes planned the query differently");
    // Finish the plan over everyone's partial results.
    let input: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(&parts, schema, None)?;
    let plan = plan.transform_down(|p| Ok(if Arc::ptr_eq(&p, &cut) { Transformed::yes(replace_children_if_necessary(p, vec![input.clone()])?) } else { Transformed::no(p) }))?.data;
    Ok(Some(datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?))
}

/// This node's partial result for a slice: one list of batches per partition (order kept).
pub async fn stage(lake: &Lake, s: &Slice) -> Result<Vec<Vec<RecordBatch>>> {
    let (ctx, plan) = plan(lake, s).await?;
    let cut = find_cut(&plan).context("no single-stage plan")?;
    Ok(collect_partitioned(cut.children()[0].clone(), ctx.task_ctx()).await?)
}

async fn remote(node: &str, s: &Slice) -> Result<Vec<Vec<RecordBatch>>> {
    let res = crate::cluster::http().post(format!("http://{node}/cluster/stage")).json(s).send().await?.error_for_status()?;
    decode_parts(&res.bytes().await?)
}

/// The physical plan of the slice's query, with the main table standing for just the slice.
async fn plan(lake: &Lake, s: &Slice) -> Result<(SessionContext, Arc<dyn ExecutionPlan>)> {
    let ctx = session(lake, &s.sql, &s.table).await?;
    // Always aggregate before the exchange: the partial results cross the network, so passing
    // raw rows through (DataFusion's shortcut for high-cardinality groups) would ship the table.
    ctx.state_ref().write().config_mut().options_mut().execution.skip_partial_aggregation_probe_rows_threshold = usize::MAX;
    let meta: TableMeta = lake.cat.get(&table_key(&s.table)).await?.context("no table")?;
    let (after, upto) = s.tail.unwrap_or((0, 0)); // (0, 0): no tail
    let part = TableMeta { files: s.files.clone(), tiered: after, ..meta };
    ctx.register_table(s.table.as_str(), raw(lake, &ctx, &s.table, &part, Some(upto)).await?.into_view())?;
    let plan = ctx.sql_with_options(&s.sql, crate::query::read_only()).await?.create_physical_plan().await?;
    Ok((ctx, plan))
}

/// The exchange whose input is the per-partition part of the plan: the first exchange on the
/// plan's single path down, with no other exchange below it.
fn find_cut(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let exchange = |p: &Arc<dyn ExecutionPlan>| match p.name() {
        "CoalescePartitionsExec" | "SortPreservingMergeExec" => true,
        "RepartitionExec" => !matches!(p.output_partitioning(), Partitioning::RoundRobinBatch(_)), // just re-balances
        _ => false,
    };
    fn any(p: &Arc<dyn ExecutionPlan>, f: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool) -> bool { f(p) || p.children().into_iter().any(|c| any(c, f)) }
    match plan.children()[..] {
        [child] if exchange(plan) && !any(child, &exchange) => Some(plan.clone()),
        [child] => find_cut(child),
        _ => None,
    }
}

/// One plain SELECT with inner joins only (then per-slice results combine exactly).
fn spreadable(sql: &str) -> bool {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    let Ok(stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return false };
    let [Statement::Query(q)] = &stmts[..] else { return false };
    let SetExpr::Select(s) = q.body.as_ref() else { return false };
    let inner = s.from.iter().flat_map(|t| &t.joins).all(|j| matches!(j.join_operator, JoinOperator::Join(_) | JoinOperator::Inner(_)));
    q.with.is_none() && inner && sql.to_lowercase().matches("select").count() == 1
}

/// Partitions over the wire: per partition, a u32 length and an Arrow IPC stream (empty = none).
pub fn encode_parts(parts: &[Vec<RecordBatch>]) -> Result<Vec<u8>> {
    let mut out = vec![];
    for p in parts {
        let ipc = if p.is_empty() { vec![] } else { crate::log::encode_ipc(p)? };
        out.extend((ipc.len() as u32).to_le_bytes());
        out.extend(ipc);
    }
    Ok(out)
}

fn decode_parts(mut b: &[u8]) -> Result<Vec<Vec<RecordBatch>>> {
    let mut parts = vec![];
    while b.len() >= 4 {
        let n = u32::from_le_bytes(b[..4].try_into()?) as usize;
        parts.push(if n == 0 { vec![] } else { crate::log::decode(&b[4..4 + n])? });
        b = &b[4 + n..];
    }
    Ok(parts)
}
