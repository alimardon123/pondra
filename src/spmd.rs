//! Distributed queries, SPMD style. Every live node runs the same SQL over its own slice of the
//! data, reading its files straight from the bucket, and the node that received the query (the
//! coordinator) finishes the plan over everyone's results. No coordinator service and no
//! scheduler: every node plans the same query the same way, so the plans' exchanges line up.
//! Two ways, chosen from DataFusion's own parallel plan:
//!
//! - **Gather.** The query's main table (the first in FROM) is sliced; every other table is read
//!   whole by each node (broadcast, so star joins work). Each node runs the plan up to its first
//!   exchange — for an aggregation, the partial aggregate — and the coordinator merges.
//! - **Shuffle.** When the plan exchanges rows by hash — a GROUP BY with many groups, a join of
//!   two big tables — every table is sliced and each hash exchange becomes a shuffle between
//!   nodes: a node runs a stage over its inputs, splits the output by the exchange's hash into one
//!   bucket per node and keeps it; the next stage on node j fetches bucket j from every node. The
//!   last stage's results go to the coordinator. Only operators that stay correct when split this
//!   way are allowed; anything else runs the gather way, or on one node.
//!
//! Which queries: one SELECT (no subqueries, CTEs or set operations), inner joins only, over
//! append tables. The cuts are taken from DataFusion's own plan, so they are right for any
//! aggregate DataFusion can split (avg, count distinct, …).
use crate::manifest::Manifest;
use crate::query::{session, Pruned};
use crate::store::*;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::execution_plan::replace_children_if_necessary;
use datafusion::physical_plan::{collect_partitioned, displayable, ExecutionPlan, ExecutionPlanProperties, Partitioning};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

/// Tables smaller than this aren't worth spreading.
const SPREAD_BYTES: u64 = 256 << 20;

/// In a shuffle, tables smaller than this are read whole by every node (joins broadcast them).
const BROADCAST_BYTES: u64 = 64 << 20;

/// One table's share on one node: manifests and files, and maybe the log tail (after..=upto).
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Part {
    pub table: String,
    pub manifests: Vec<Manifest>,
    pub files: Vec<DataFile>,
    pub tail: Option<(u64, u64)>,
}

/// One node's share of a query: gathering, the main table's part; shuffling, every table's.
#[derive(Serialize, Deserialize, Clone)]
pub struct Slice {
    pub sql: String,
    pub parts: Vec<Part>,
    pub shuffle: Option<Shuffle>,
}

/// A shuffle, as one node takes part in it.
#[derive(Serialize, Deserialize, Clone)]
pub struct Shuffle {
    pub id: String,
    pub nodes: Vec<String>,
    pub me: usize,
    pub step: usize, // a hash exchange (bottom-up); the number of them: the last stage
    pub all: bool,   // every table sliced (else small ones are read whole: broadcast)
}

/// Run `sql` across `nodes` (this node is `me`), or None if it should just run here.
pub async fn query(lake: &Lake, nodes: &[String], me: &str, sql: &str, force: bool) -> Result<Option<Vec<RecordBatch>>> {
    if nodes.len() < 2 || !spreadable(sql) {
        return Ok(None);
    }
    let tables = tables(sql)?;
    let main = tables[0].clone();
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&main)).await? else { return Ok(None) };
    let sealed = meta.sealed.clone().unwrap_or_default();
    let bytes = sealed.bytes + meta.files.iter().map(|f| f.bytes).sum::<u64>();
    let files = sealed.files as usize + meta.files.len();
    if !meta.key.is_empty() || files == 0 || (!force && (files < nodes.len() || bytes < SPREAD_BYTES)) {
        return Ok(None); // (`force`: spread anyway, for tests)
    }
    let mine = nodes.iter().position(|n| n == me).context("not a member")?;
    let parts = deal(lake, &meta, &main, nodes.len()).await?;
    let slices: Vec<Slice> = parts.into_iter().map(|p| Slice { sql: sql.into(), parts: vec![p], shuffle: None }).collect();
    let (ctx, plan) = plan(lake, &slices[mine]).await?;
    // (a table sliced twice, in a self-join, would only meet its own slice)
    let cut = find_cut(&plan).filter(|_| tables.iter().filter(|t| **t == main).count() == 1);
    if cut.as_ref().is_none_or(hashed) {
        if let Some(rows) = shuffle(lake, nodes, mine, sql, &tables).await? {
            return Ok(Some(rows));
        }
    }
    let Some(cut) = cut else { return Ok(None) };
    // Gather: every node computes its partial result at the same time (this one: its files and,
    // as one more slice, the log tail).
    let tail = Slice { sql: sql.into(), parts: vec![Part { table: main, tail: Some((meta.tiered, lake.visible())), ..Default::default() }], shuffle: None };
    let runs = nodes.iter().zip(&slices).map(|(node, s)| async move {
        match node == me {
            true => Ok(vec![]), // below
            false => remote(node, s).await.map(|(_, parts)| parts),
        }
    });
    let ours = async { Ok(collect_partitioned(cut.children()[0].clone(), ctx.task_ctx()).await?) };
    let tail = async {
        let (after, upto) = tail.parts[0].tail.expect("tail");
        match upto > after { true => stage(lake, &tail).await.map(|(_, parts)| parts), false => Ok(vec![]) } // no log tail: nothing to do
    };
    let (mut parts, ours, tail) = futures::future::try_join3(futures::future::try_join_all(runs), ours, tail).await?;
    parts.extend([ours, tail]);
    finish(&ctx, &plan, Some(&cut), parts.into_iter().flatten().collect()).await.map(Some)
}

/// The coordinator's last step: the plan above `cut`, over everyone's results (no cut: they are
/// the results).
async fn finish(ctx: &SessionContext, plan: &Arc<dyn ExecutionPlan>, cut: Option<&Arc<dyn ExecutionPlan>>, parts: Vec<Vec<RecordBatch>>) -> Result<Vec<RecordBatch>> {
    let Some(cut) = cut else { return Ok(parts.into_iter().flatten().collect()) };
    let schema = cut.children()[0].schema();
    ensure!(parts.iter().flatten().all(|b| b.schema().fields() == schema.fields()), "nodes planned the query differently");
    let input: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(&parts, schema, None)?;
    let plan = plan.clone().transform_down(|p| Ok(if Arc::ptr_eq(&p, cut) { Transformed::yes(replace_children_if_necessary(p, vec![input.clone()])?) } else { Transformed::no(p) }))?.data;
    Ok(datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?)
}

/// A table's manifests and files dealt round-robin over `n` nodes, oldest first (so a time range
/// spreads over all of them). Too few manifests to go round: their files are dealt instead.
async fn deal(lake: &Lake, meta: &TableMeta, table: &str, n: usize) -> Result<Vec<Part>> {
    let mut manifests = crate::manifest::list(lake, meta).await?;
    let mut files = vec![];
    if manifests.len() < 4 * n {
        for m in manifests.drain(..) {
            files.extend(crate::manifest::files(lake, &m).await?);
        }
    }
    files.extend(meta.files.iter().cloned());
    let mut parts = vec![Part { table: table.into(), ..Default::default() }; n];
    manifests.into_iter().enumerate().for_each(|(i, m)| parts[i % n].manifests.push(m));
    files.into_iter().enumerate().for_each(|(i, f)| parts[i % n].files.push(f));
    Ok(parts)
}

/// This node's part of a query: a gather's partial result, or a shuffle step (with the plan's
/// shape, which every node must agree on). One list of batches per partition, order kept.
pub async fn stage(lake: &Lake, s: &Slice) -> Result<(String, Vec<Vec<RecordBatch>>)> {
    if let Some(sh) = &s.shuffle {
        return step(lake, s, sh).await;
    }
    let (ctx, plan) = plan(lake, s).await?;
    let cut = find_cut(&plan).context("no single-stage plan")?;
    Ok((String::new(), collect_partitioned(cut.children()[0].clone(), ctx.task_ctx()).await?))
}

async fn remote(node: &str, s: &Slice) -> Result<(String, Vec<Vec<RecordBatch>>)> {
    let res = crate::cluster::http().post(format!("http://{node}/cluster/stage")).json(s).send().await?;
    ensure!(res.status().is_success(), "{node}: {}", res.text().await?);
    decode_reply(&res.bytes().await?)
}

/// The physical plan of the slice's query, its tables standing for just their parts.
async fn plan(lake: &Lake, s: &Slice) -> Result<(SessionContext, Arc<dyn ExecutionPlan>)> {
    let ctx = session(lake, &s.sql, "").await?;
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        let o = state.config_mut().options_mut();
        // Always aggregate before the exchange: partial results cross the network, so passing
        // raw rows through (DataFusion's shortcut for high-cardinality groups) would ship the table.
        o.execution.skip_partial_aggregation_probe_rows_threshold = usize::MAX;
        if let Some(sh) = &s.shuffle {
            o.optimizer.enable_dynamic_filter_pushdown = false; // (a join's filter would reach a scan of an earlier step)
            if sh.all {
                // Every table sliced: a join must shuffle both sides (a broadcast side would be partial).
                (o.optimizer.hash_join_single_partition_threshold, o.optimizer.hash_join_single_partition_threshold_rows) = (0, 0);
            }
        }
    }
    for p in &s.parts {
        let meta: TableMeta = lake.cat.get(&table_key(&p.table)).await?.context("no table")?;
        let (after, upto) = p.tail.unwrap_or((0, 0)); // (0, 0): no tail
        let schema = crate::query::schema(&meta.columns)?;
        let sealed = meta.sealed.clone().unwrap_or_default();
        let share = Some((sealed.rows + meta.files.iter().map(|f| f.rows).sum::<u64>(), sealed.bytes + meta.files.iter().map(|f| f.bytes).sum::<u64>()));
        let meta = TableMeta { files: p.files.clone(), tiered: after, ..meta };
        let table = Pruned { lake: lake.arc(), name: p.table.clone(), meta, manifests: Some(p.manifests.clone()), upto: Some(upto), schema, share };
        ctx.deregister_table(p.table.as_str())?;
        ctx.register_table(p.table.as_str(), Arc::new(table))?;
    }
    let plan = ctx.sql_with_options(&s.sql, crate::query::read_only()).await?.create_physical_plan().await?;
    Ok((ctx, plan))
}

/// The exchange whose input is the per-partition part of the plan: the first exchange on the
/// plan's single path down, with no other exchange below it.
fn find_cut(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let exchange = |p: &Arc<dyn ExecutionPlan>| gathers(p) || p.name() == "RepartitionExec" && !matches!(p.output_partitioning(), Partitioning::RoundRobinBatch(_));
    fn any(p: &Arc<dyn ExecutionPlan>, f: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool) -> bool { f(p) || p.children().into_iter().any(|c| any(c, f)) }
    match plan.children()[..] {
        [child] if exchange(plan) && !any(child, &exchange) => Some(plan.clone()),
        [child] => find_cut(child),
        _ => None,
    }
}

fn gathers(p: &Arc<dyn ExecutionPlan>) -> bool { matches!(p.name(), "CoalescePartitionsExec" | "SortPreservingMergeExec") }

fn hashed(p: &Arc<dyn ExecutionPlan>) -> bool { p.name() == "RepartitionExec" && matches!(p.output_partitioning(), Partitioning::Hash(..)) }

// ---------------------------------------------------------------- shuffles

/// Run `sql` as a shuffle, or None if its plan can't be split that way. First with small tables
/// read whole by every node (broadcast joins), then with every table sliced.
async fn shuffle(lake: &Lake, nodes: &[String], mine: usize, sql: &str, tables: &[String]) -> Result<Option<Vec<RecordBatch>>> {
    for all in [false, true] {
        let id = uuid::Uuid::new_v4().to_string();
        let mut slices: Vec<Slice> = (0..nodes.len()).map(|me| Slice { sql: sql.into(), parts: vec![], shuffle: Some(Shuffle { id: id.clone(), nodes: nodes.to_vec(), me, step: 0, all }) }).collect();
        let mut seen = std::collections::HashSet::new();
        for t in tables.iter().filter(|t| seen.insert(*t)) {
            let meta = lake.cat.get::<TableMeta>(&table_key(t)).await?;
            let bytes = meta.as_ref().map_or(0, |m| m.sealed.as_ref().map_or(0, |s| s.bytes) + m.files.iter().map(|f| f.bytes).sum::<u64>());
            if !all && *t != tables[0] && bytes < BROADCAST_BYTES {
                continue; // (read whole)
            }
            let Some(meta) = meta.filter(|m| m.key.is_empty()) else { return Ok(None) }; // (append tables of this lake only)
            for (i, mut p) in deal(lake, &meta, t, nodes.len()).await?.into_iter().enumerate() {
                p.tail = (i == mine).then(|| (meta.tiered, lake.visible())); // (the log tails here)
                slices[i].parts.push(p);
            }
        }
        if let Some(job) = Job::open(lake, &slices[mine]).await? {
            return run(lake, nodes, mine, &slices, job).await.map(Some); // (step 0 here uses this plan)
        }
    }
    Ok(None)
}

/// Every step of a shuffle on every node, then the coordinator's finish.
async fn run(lake: &Lake, nodes: &[String], mine: usize, slices: &[Slice], job: Arc<Job>) -> Result<Vec<RecordBatch>> {
    let id = slices[mine].shuffle.as_ref().expect("a shuffle").id.clone();
    crate::metrics::add(&crate::metrics::SHUFFLED, !job.exchanges.is_empty() as u64);
    let run = async {
        let mut last = vec![];
        for step in 0..=job.exchanges.len() {
            let runs = nodes.iter().zip(slices).map(|(node, s)| {
                let mut s = s.clone();
                s.shuffle.as_mut().expect("a shuffle").step = step;
                async move { if s.shuffle.as_ref().is_some_and(|sh| sh.me == mine) { stage(lake, &s).await } else { remote(node, &s).await } }
            });
            let outs = futures::future::try_join_all(runs).await?;
            ensure!(outs.iter().all(|(shape, _)| *shape == job.shape), "nodes planned the query differently");
            last = outs.into_iter().flat_map(|(_, parts)| parts).collect();
        }
        finish(&job.ctx, &job.plan, job.cut.as_ref(), last).await
    };
    let out = run.await;
    JOBS.lock().unwrap().remove(&id); // (all steps done: nobody fetches from here any more)
    out
}

/// A shuffle on one node: its plan, and the buckets it keeps for the others.
struct Job {
    ctx: SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    exchanges: Vec<Arc<dyn ExecutionPlan>>, // the hash exchanges below the cut, bottom-up
    cut: Option<Arc<dyn ExecutionPlan>>,     // the first gather on the plan's path down (None: nodes run it all)
    shape: String,
    buckets: Mutex<HashMap<(usize, usize), Vec<RecordBatch>>>, // (exchange, node) -> its rows for that node
    at: std::time::Instant,
    done: std::sync::atomic::AtomicBool, // its last step ran here (others may still fetch)
}

static JOBS: LazyLock<Mutex<HashMap<String, Arc<Job>>>> = LazyLock::new(Default::default);

impl Job {
    /// Plan the slice's query; None if it can't be split into shuffle stages.
    async fn open(lake: &Lake, s: &Slice) -> Result<Option<Arc<Job>>> {
        let (ctx, plan) = plan(lake, s).await?;
        let mut cut = None;
        let mut p = plan.clone();
        while let [child] = p.children()[..] {
            if gathers(&p) {
                cut = Some(p.clone());
                break;
            }
            if hashed(&p) {
                break;
            }
            p = child.clone();
        }
        let region = cut.as_ref().map_or(plan.clone(), |c| c.children()[0].clone());
        let mut exchanges = vec![];
        // What reaches the coordinator must be split between the nodes (a whole copy from each
        // would count everything once per node).
        if !matches!(spread(&region, &mut exchanges), Some(Spread::Split | Spread::Keyed)) {
            return Ok(None); // (no exchanges is fine: every node runs its share, the coordinator merges)
        }
        let job = Arc::new(Job { ctx, plan, exchanges, cut, shape: shape(&region), buckets: Default::default(), at: std::time::Instant::now(), done: Default::default() });
        let mut jobs = JOBS.lock().unwrap();
        // (finished ones after a minute, abandoned ones after ten)
        jobs.retain(|_, j| j.at.elapsed().as_secs() < if j.done.load(std::sync::atomic::Ordering::Relaxed) { 60 } else { 600 });
        jobs.insert(s.shuffle.as_ref().expect("a shuffle").id.clone(), job.clone());
        Ok(Some(job))
    }
}

/// How a plan's rows are spread over the nodes when each node runs it on its share.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Spread {
    Whole, // every node has all of them (a table read whole)
    Split, // each node has its own (a table sliced)
    Keyed, // …and a key's rows are all on one node (after a hash exchange)
}

/// The spread of `p`'s output if every operator in it stays correct run this way, else None.
/// Collects the hash exchanges (shuffles), bottom-up.
fn spread(p: &Arc<dyn ExecutionPlan>, exchanges: &mut Vec<Arc<dyn ExecutionPlan>>) -> Option<Spread> {
    use Spread::*;
    if p.name() == "ShareExec" {
        return Some(Split);
    }
    let kids = p.children().into_iter().map(|c| spread(c, exchanges)).collect::<Option<Vec<_>>>()?;
    let line = displayable(p.as_ref()).one_line().to_string();
    let has = |s: &str| line.contains(s);
    let one = kids.first().copied();
    let single = p.children().first().is_some_and(|c| c.output_partitioning().partition_count() == 1);
    let join = |l: Spread, r: Spread, partitioned: bool| match (l, r, partitioned) {
        (Whole, Whole, _) => Some(Whole),
        (Keyed, Keyed, true) => Some(Keyed), // (both sides shuffled by the join key)
        (Whole, x, false) => Some(x),        // a broadcast build side
        (x, Whole, false) => Some(if x == Keyed { Split } else { x }),
        _ => None,
    };
    if hashed(p) && one? != Whole {
        exchanges.push(p.clone()); // (over a whole copy it stays within the node: shuffled, each row would arrive N times)
        return Some(Keyed);
    }
    if !kids.is_empty() && kids.iter().all(|k| *k == Whole) {
        return Some(Whole); // (every node computes the same)
    }
    match p.name() {
        "DataSourceExec" | "EmptyExec" | "PlaceholderRowExec" => Some(Whole),
        "ProjectionExec" | "FilterExec" | "CoalesceBatchesExec" | "CooperativeExec" | "LocalLimitExec" => one,
        "CoalescePartitionsExec" | "SortPreservingMergeExec" => one, // (within a node)
        "RepartitionExec" => one,
        "AggregateExec" if has("mode=Partial,") => one,
        "AggregateExec" if has("mode=FinalPartitioned,") || has("mode=SinglePartitioned,") => matches!(one?, Keyed | Whole).then_some(one?),
        "AggregateExec" => (one? == Whole).then_some(Whole), // (a final aggregate over all rows)
        "HashJoinExec" if has("join_type=Inner") => join(kids[0], kids[1], has("mode=Partitioned,")),
        "SortMergeJoinExec" | "SortMergeJoin" if has("join_type=Inner") => join(kids[0], kids[1], true),
        "CrossJoinExec" | "NestedLoopJoinExec" if !has("join_type=") || has("join_type=Inner") => join(kids[0], kids[1], false),
        "SortExec" | "SortExec(TopK)" if has("preserve_partitioning=[true]") => one,
        "SortExec" | "SortExec(TopK)" | "GlobalLimitExec" => (one? == Whole).then_some(Whole),
        "BoundedWindowAggExec" | "WindowAggExec" => match single {
            true => (one? == Whole).then_some(Whole), // (a window over all rows)
            false => matches!(one?, Keyed | Whole).then_some(one?),
        },
        _ => None,
    }
}

/// The plan's shape above its scans, which every node must plan alike.
fn shape(p: &Arc<dyn ExecutionPlan>) -> String {
    fn scan(p: &Arc<dyn ExecutionPlan>) -> bool {
        let leafish = matches!(p.name(), "DataSourceExec" | "EmptyExec" | "UnionExec" | "FilterExec" | "ProjectionExec" | "CoalesceBatchesExec" | "CooperativeExec");
        p.name() == "ShareExec" || (leafish || p.name() == "RepartitionExec" && !hashed(p)) && p.children().into_iter().all(scan)
    }
    if scan(p) {
        return format!("scan{:?}", p.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
    }
    let what = match p.name() {
        "RepartitionExec" => p.output_partitioning().to_string(),
        "AggregateExec" | "HashJoinExec" | "SortMergeJoinExec" | "SortExec" | "SortExec(TopK)" | "BoundedWindowAggExec" => displayable(p.as_ref()).one_line().to_string(),
        name => name.to_string(),
    };
    format!("{}[{}]", what.trim(), p.children().iter().map(|c| shape(c)).collect::<Vec<_>>().join(", "))
}

/// One step of a shuffle on this node: the stage below exchange `step` (or, after the last
/// exchange, the last stage), over the buckets for this node of the exchanges right below it.
async fn step(lake: &Lake, s: &Slice, sh: &Shuffle) -> Result<(String, Vec<Vec<RecordBatch>>)> {
    let open = JOBS.lock().unwrap().get(&sh.id).cloned();
    let job = match (open, sh.step) {
        (Some(job), _) => job,
        (None, 0) => Job::open(lake, s).await?.context("can't shuffle this plan here")?,
        (None, _) => bail!("shuffle expired"),
    };
    let last = sh.step == job.exchanges.len();
    let top = match last {
        true => job.cut.as_ref().map_or(job.plan.clone(), |c| c.children()[0].clone()),
        false => job.exchanges[sh.step].children()[0].clone(),
    };
    let mut inputs = vec![];
    collect_inputs(&top, &job.exchanges, &mut inputs);
    let mut replaced = vec![];
    for x in inputs {
        let k = job.exchanges.iter().position(|e| Arc::ptr_eq(e, &x)).context("an unknown exchange")?;
        let fetches = sh.nodes.iter().enumerate().map(|(i, node)| fetch(&job, &sh.id, node, i == sh.me, k, sh.me));
        let parts = futures::future::try_join_all(fetches).await?;
        replaced.push((x.clone(), MemorySourceConfig::try_new_exec(&parts, x.children()[0].schema(), None)? as Arc<dyn ExecutionPlan>));
    }
    let top = top.transform_down(|p| match replaced.iter().find(|(x, _)| Arc::ptr_eq(x, &p)) {
        Some((_, input)) => Ok(Transformed::yes(replace_children_if_necessary(p, vec![input.clone()])?)),
        None => Ok(Transformed::no(p)),
    })?.data;
    let out = collect_partitioned(top, job.ctx.task_ctx()).await?;
    if last {
        job.done.store(true, std::sync::atomic::Ordering::Relaxed);
        return Ok((job.shape.clone(), out));
    }
    // Split by the exchange's hash, one bucket per node (the same hash on every node).
    let Partitioning::Hash(exprs, _) = job.exchanges[sh.step].output_partitioning().clone() else { bail!("not a hash exchange") };
    let mut split = datafusion::physical_plan::repartition::BatchPartitioner::try_new(Partitioning::Hash(exprs, sh.nodes.len()), Default::default(), 0, 1)?;
    let mut buckets = job.buckets.lock().unwrap();
    for b in out.into_iter().flatten() {
        split.partition(b, |to, b| {
            buckets.entry((sh.step, to)).or_default().push(b);
            Ok(())
        })?;
    }
    Ok((job.shape.clone(), vec![]))
}

/// The shuffles right below `p` (not below another one).
fn collect_inputs(p: &Arc<dyn ExecutionPlan>, exchanges: &[Arc<dyn ExecutionPlan>], out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    for c in p.children() {
        match exchanges.iter().any(|x| Arc::ptr_eq(x, c)) {
            true => out.push(c.clone()),
            false => collect_inputs(c, exchanges, out),
        }
    }
}

/// Node `from`'s bucket of exchange `k` for node `to` (taken: each is read once).
async fn fetch(job: &Job, id: &str, from: &str, local: bool, k: usize, to: usize) -> Result<Vec<RecordBatch>> {
    if local {
        return Ok(job.buckets.lock().unwrap().remove(&(k, to)).unwrap_or_default());
    }
    let res = crate::cluster::http().get(format!("http://{from}/cluster/shuffle?id={id}&exchange={k}&to={to}")).send().await?;
    ensure!(res.status().is_success(), "{from}: {}", res.text().await?);
    Ok(decode_parts(&res.bytes().await?)?.into_iter().flatten().collect())
}

/// `GET /cluster/shuffle`: a bucket this node keeps for another.
pub fn bucket(id: &str, exchange: usize, to: usize) -> Result<Vec<u8>> {
    let job = JOBS.lock().unwrap().get(id).cloned().context("shuffle expired")?;
    let rows = job.buckets.lock().unwrap().remove(&(exchange, to)).unwrap_or_default();
    encode_parts(&[rows])
}

// ---------------------------------------------------------------- SQL

/// One plain SELECT with inner joins only (then per-slice results combine exactly).
fn spreadable(sql: &str) -> bool {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    let Ok(stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return false };
    let [Statement::Query(q)] = &stmts[..] else { return false };
    let SetExpr::Select(s) = q.body.as_ref() else { return false };
    let inner = s.from.iter().flat_map(|t| &t.joins).all(|j| matches!(j.join_operator, JoinOperator::Join(_) | JoinOperator::Inner(_)));
    q.with.is_none() && inner && sql.to_lowercase().matches("select").count() == 1
}

/// The tables a (spreadable) query reads, in order, as often as it reads them.
fn tables(sql: &str) -> Result<Vec<String>> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let [Statement::Query(q)] = &stmts[..] else { bail!("not a query") };
    let SetExpr::Select(s) = q.body.as_ref() else { bail!("not a SELECT") };
    let relations = s.from.iter().flat_map(|t| std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)));
    let names: Vec<String> = relations.map(|r| match r {
        TableFactor::Table { name, .. } => Ok(name.to_string().trim_matches('"').to_string()),
        _ => bail!("not a table"),
    }).collect::<Result<_>>()?;
    ensure!(!names.is_empty(), "no table");
    Ok(names)
}

// ---------------------------------------------------------------- over the wire

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

/// A stage's reply: the plan's shape (a u32 length, then text), then its partitions.
pub fn encode_reply(shape: &str, parts: &[Vec<RecordBatch>]) -> Result<Vec<u8>> {
    let mut out = (shape.len() as u32).to_le_bytes().to_vec();
    out.extend(shape.as_bytes());
    out.extend(encode_parts(parts)?);
    Ok(out)
}

fn decode_reply(b: &[u8]) -> Result<(String, Vec<Vec<RecordBatch>>)> {
    ensure!(b.len() >= 4, "a short reply");
    let n = u32::from_le_bytes(b[..4].try_into()?) as usize;
    Ok((String::from_utf8(b[4..4 + n].to_vec())?, decode_parts(&b[4 + n..])?))
}

// ---------------------------------------------------------------- a node's share of a table

/// A node's share of a sliced table: its scan, with 2+ partitions (so aggregations plan as
/// partial + final everywhere), reporting the whole table's size, so every node plans its query
/// (join order, build sides) as for the whole table, whatever its share holds.
#[derive(Debug)]
pub struct ShareExec {
    input: Arc<dyn ExecutionPlan>,
    table: String,
    stats: Arc<datafusion::common::Statistics>,
}

impl ShareExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table: &str, rows: u64, bytes: u64) -> datafusion::error::Result<ShareExec> {
        use datafusion::common::stats::Precision;
        let input = match input.output_partitioning().partition_count() < 2 {
            true => datafusion::physical_plan::union::UnionExec::try_new(vec![input.clone(), Arc::new(datafusion::physical_plan::empty::EmptyExec::new(input.schema()))])?,
            false => input,
        };
        let mut stats = datafusion::common::Statistics::new_unknown(&input.schema());
        (stats.num_rows, stats.total_byte_size) = (Precision::Inexact(rows as usize), Precision::Inexact(bytes as usize));
        Ok(ShareExec { input, table: table.into(), stats: Arc::new(stats) })
    }
}

impl datafusion::physical_plan::DisplayAs for ShareExec {
    fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "ShareExec: table={}", self.table) }
}

impl ExecutionPlan for ShareExec {
    fn name(&self) -> &str { "ShareExec" }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> { self.input.properties() }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn maintains_input_order(&self) -> Vec<bool> { vec![true] }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> { vec![false] }
    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion>) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ShareExec { input: children[0].clone(), table: self.table.clone(), stats: self.stats.clone() }))
    }
    fn execute(&self, partition: usize, ctx: Arc<datafusion::execution::TaskContext>) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> { self.input.execute(partition, ctx) }
    fn statistics_from_inputs(&self, _: &[Arc<datafusion::common::Statistics>], args: &datafusion::physical_plan::statistics::StatisticsArgs) -> datafusion::error::Result<Arc<datafusion::common::Statistics>> {
        Ok(match args.partition() {
            None => self.stats.clone(),
            Some(_) => Arc::new(datafusion::common::Statistics::new_unknown(&self.schema())),
        })
    }
    fn gather_filters_for_pushdown(&self, _: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase, parent_filters: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>, _: &datafusion::common::config::ConfigOptions) -> datafusion::error::Result<datafusion::physical_plan::filter_pushdown::FilterDescription> {
        datafusion::physical_plan::filter_pushdown::FilterDescription::from_children(parent_filters, &self.children())
    }
    fn handle_child_pushdown_result(&self, _: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase, result: datafusion::physical_plan::filter_pushdown::ChildPushdownResult, _: &datafusion::common::config::ConfigOptions) -> datafusion::error::Result<datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation::if_all(result))
    }
}
