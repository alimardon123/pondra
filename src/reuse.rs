//! A subplan that appears more than once, alike, runs once. A query that names one CTE more than once (TPC-DS's
//! `cross_items`, `v1`, `results`, TPC-H q15's `revenue`) is planned with a copy of it at each use, and each copy
//! reads and groups its tables again. Where the copies are still alike once the filters are pushed down, the first use
//! to run computes it and holds its rows, counted in the query's memory, and every use reads those (`ReuseExec`), as
//! DuckDB and Postgres hold a CTE named twice. Uses the filters reach differently (TPC-DS q39's two months, q31's
//! quarters, q4's sale types) aren't alike, and each keeps its own copy and its own filters. At SF10 in process: TPC-DS
//! 68.6 → 64.9 s (q14 3.75 → 2.01, q36 1.10 → 0.34, q47 1.81 → 0.67, q23 3.21 → 2.17), TPC-H q15 1.01 → 0.55.
//!
//! Only what does real work is held: a copy that groups, joins or windows, whose rows are a small part of what it reads
//! (an eighth at most, as its statistics say; a grouping at its top says so when they don't). A join that gives as many
//! rows as it reads (q95's `ws_wh`, web_sales joined to itself) is cheaper run twice, the joins above it filtering its
//! scans as they run, than held whole. A held copy takes no filter from above it (a filter the joins above learn as they
//! run is one use's own), so the copies are found before those are pushed down (`optimize::physical_rules`); and a use
//! whose join learns such a filter for its grouping keeps its own copy (`keyed`: q23's `frequent_ss_items`). What
//! doesn't fit in the query's memory isn't held: the use that ran it reads on from its own copy, and each other use runs
//! its own, as before. A spread query's slices (`optimize::spread`) aren't touched: every node plans a slice the same way.
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::buffer::BufferExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{replace_children_if_necessary, CardinalityEffect};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{displayable, ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties, StatisticsArgs};
use futures::{StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};

/// The most of what a held copy reads that its rows may be, as a fraction's denominator (`worth`).
const SHARE: usize = 8;

/// Holds each subplan that appears more than once alike, once it does real work (`worth`): see the module's doc.
#[derive(Debug)]
pub struct Reuse;

impl PhysicalOptimizerRule for Reuse {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        if crate::optimize::spread(&plan)? || plan.exists(|p| Ok(p.is::<ReuseExec>()))? {
            return Ok(plan);
        }
        // (most plans read no table twice alike: they pay for a look at their scans alone)
        let mut scans: HashMap<String, usize> = HashMap::new();
        for leaf in leaves(&plan) {
            *scans.entry(key(&leaf)).or_default() += 1;
        }
        if scans.values().all(|&n| n < 2) {
            return Ok(plan);
        }
        // How many times each subplan appears, by its text (`key`).
        let mut seen: HashMap<String, usize> = HashMap::new();
        plan.apply(|p| {
            *seen.entry(key(p)).or_default() += 1;
            Ok(TreeNodeRecursion::Continue)
        })?;
        if seen.values().all(|&n| n < 2) {
            return Ok(plan);
        }
        // From the top down, the widest alike subplans worth holding, one `Held` for the uses of each.
        let mut held: HashMap<String, Arc<Held>> = HashMap::new();
        let plan = plan
            .transform_down(|p| {
                let k = key(&p);
                if seen.get(&k).is_none_or(|&n| n < 2) || !worth(&p)? {
                    return Ok(Transformed::no(p));
                }
                let held = held.entry(k).or_default().clone();
                Ok(Transformed::new(Arc::new(ReuseExec { input: p, held }) as Arc<dyn ExecutionPlan>, true, TreeNodeRecursion::Jump))
            })
            .data()?;
        // A use a join above filters as the query runs keeps its own copy (`keyed`).
        let plan = match config.optimizer.enable_join_dynamic_filter_pushdown {
            true => plan.transform_up(|p| keyed(p)).data()?,
            false => plan,
        };
        // A copy held where it is used just once (its other uses were inside a wider one, or kept their own) runs as it did.
        let mut uses: HashMap<*const Held, usize> = HashMap::new();
        plan.apply(|p| {
            if let Some(r) = p.downcast_ref::<ReuseExec>() {
                *uses.entry(Arc::as_ptr(&r.held)).or_default() += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        plan.transform_up(|p| match p.downcast_ref::<ReuseExec>() {
            Some(r) => {
                let n = uses[&Arc::as_ptr(&r.held)];
                r.held.uses.store(n, Relaxed);
                Ok(if n < 2 { Transformed::yes(r.input.clone()) } else { Transformed::no(p) })
            }
            None => Ok(Transformed::no(p)),
        })
        .data()
    }

    fn name(&self) -> &str {
        "Reuse"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A subplan as text, every detail of each of its nodes: two subplans of one text read the same tables the same way
/// and give the same rows.
fn key(p: &Arc<dyn ExecutionPlan>) -> String {
    displayable(p.as_ref()).indent(true).to_string()
}

/// Is `p` worth holding: it groups, joins or windows, reads nothing that changes from one run of it to the next, and
/// groups at its top or gives a small part of what it reads, as its statistics say (`SHARE`)?
fn worth(p: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    let text = key(p);
    // (a filter learned as the query runs is one use's own; a random value is drawn at each run; a recursive query's
    // working table changes between its rounds)
    // (and a scalar subquery's answer is filled in where it is used)
    if ["DynamicFilter", "random(", "uuid(", "WorkTable", "ReuseExec", "scalar_subquery("].iter().any(|s| text.contains(s)) {
        return Ok(false);
    }
    if !p.exists(|n| Ok(["Aggregate", "Join", "Window"].iter().any(|s| n.name().contains(s))))? {
        return Ok(false);
    }
    if groups_at_top(p) {
        return Ok(true); // (a grouping's estimate is often its input's: the groups are what a CTE named twice holds)
    }
    let read = leaves(p).into_iter().map(|l| rows(&l)).try_fold(0usize, |most, n| n.map(|n| most.max(n)));
    Ok(matches!((rows(p), read), (Some(out), Some(read)) if read > 0 && out.saturating_mul(SHARE) <= read))
}

/// The rows `p` gives, as its statistics say.
fn rows(p: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let s = datafusion::physical_plan::statistics::StatisticsContext::new().compute(p.as_ref(), &StatisticsArgs::new()).ok()?;
    s.num_rows.get_value().copied()
}

/// The nodes `p` reads from: its scans.
fn leaves(p: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    match p.children() {
        kids if kids.is_empty() => vec![p.clone()],
        kids => kids.into_iter().flat_map(leaves).collect(),
    }
}

/// `p`, a hash join, with the use its probe side reaches unwrapped when the join filters it as the query runs: its build
/// side's keys come down to the use's grouping and drop the rows of the groups they can't match (`keys_below`), so that
/// use groups only what it needs. Held, the copy would group every row once for all its uses: TPC-DS q23's
/// `frequent_ss_items` is wanted for 4,200 items of 102,000, and holding it took the query from 3.2 to 6.3 s. Not when
/// the build side is a whole table, whose keys keep every row that can match, nor when it holds another use of the same
/// copy, whose keys keep the copy's own rows (TPC-H q15's `revenue` joined to `supplier`, TPC-DS q65's `sa` to itself).
fn keyed(p: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(j) = p.downcast_ref::<HashJoinExec>() else { return Ok(Transformed::no(p)) };
    let (build, probe) = (j.left().clone(), j.right().clone()); // (DataFusion's probe side is the right)
    if whole(&build) {
        return Ok(Transformed::no(p));
    }
    match unheld(&probe, &build)? {
        Some(probe) => Ok(Transformed::yes(replace_children_if_necessary(p, vec![build, probe])?)),
        None => Ok(Transformed::no(p)),
    }
}

/// `plan` with the use it reaches through what passes rows as they are unwrapped, when that use groups at its top as
/// `keys_below` finds a grouping (through the same operators), and `build` holds no other use of it; None otherwise.
fn unheld(plan: &Arc<dyn ExecutionPlan>, build: &Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(r) = plan.downcast_ref::<ReuseExec>() {
        let own = build.exists(|n| Ok(n.downcast_ref::<ReuseExec>().is_some_and(|b| Arc::ptr_eq(&b.held, &r.held))))?;
        return Ok((!own && grouped(&r.input)).then(|| r.input.clone()));
    }
    if !passes(plan) {
        return Ok(None);
    }
    let [child] = plan.children()[..] else { return Ok(None) };
    Ok(match unheld(child, build)? {
        Some(child) => Some(replace_children_if_necessary(plan.clone(), vec![child])?),
        None => None,
    })
}

/// Does a join's filter that comes down to `p` meet a grouping, through what passes rows as they are (`keys_below`)?
fn grouped(p: &Arc<dyn ExecutionPlan>) -> bool {
    match p.downcast_ref::<AggregateExec>() {
        Some(a) => !a.group_expr().expr().is_empty() && !matches!(a.mode(), AggregateMode::Partial),
        None => passes(p) && p.children().first().is_some_and(|c| grouped(c)),
    }
}

/// Does `p` pass its input's rows as they are, for a join's filter to come down through it (`keys_below`)?
fn passes(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.is::<ProjectionExec>() || p.is::<BufferExec>() || p.is::<CoalescePartitionsExec>() || p.is::<RepartitionExec>() || p.is::<FilterExec>()
}

/// Is `p` a whole table: a scan with no filter, through what passes rows as they are?
fn whole(p: &Arc<dyn ExecutionPlan>) -> bool {
    match p.downcast_ref::<DataSourceExec>() {
        Some(_) => !key(p).contains("predicate="),
        None => !p.is::<FilterExec>() && passes(p) && p.children().first().is_some_and(|c| whole(c)),
    }
}

/// Does `p` group at its top, through what keeps the groups' rows as they are (a window over them adds a column)?
fn groups_at_top(p: &Arc<dyn ExecutionPlan>) -> bool {
    match p.name() {
        "AggregateExec" => true,
        "ProjectionExec" | "CoalesceBatchesExec" | "CoalescePartitionsExec" | "RepartitionExec" | "SortExec" | "SortPreservingMergeExec" | "FilterExec"
        | "BoundedWindowAggExec" | "WindowAggExec" => {
            p.children().first().is_some_and(|c| groups_at_top(c))
        }
        _ => false,
    }
}

/// What the uses of one held subplan share: the copy that runs, each partition's rows once a use has run it, and how
/// many uses there are (`Reuse` counts them). One copy runs every partition, whichever use asks first: a copy's
/// exchanges run all of their input for any partition of it, so partitions run by two copies would run it twice.
#[derive(Debug, Default)]
struct Held {
    input: std::sync::OnceLock<Arc<dyn ExecutionPlan>>,
    parts: Mutex<HashMap<usize, Arc<tokio::sync::Mutex<Part>>>>,
    uses: AtomicUsize,
}

impl Held {
    fn part(&self, p: usize) -> Arc<tokio::sync::Mutex<Part>> {
        self.parts.lock().unwrap_or_else(PoisonError::into_inner).entry(p).or_default().clone()
    }
}

/// One partition of a held subplan.
#[derive(Debug, Default)]
enum Part {
    #[default]
    Unrun,
    Kept(Arc<Kept>, usize), // its rows, and how many uses have taken them
    Own,                    // too big to hold: each use runs its own copy
}

/// A partition's rows, counted in the query's memory until the last use has read them.
#[derive(Debug)]
struct Kept {
    batches: Vec<RecordBatch>,
    _reservation: MemoryReservation,
}

/// One use of a subplan that appears more than once alike (`Reuse`): the first use to run a partition runs it and
/// holds its rows, and every use reads those. Past the query's memory nothing is held, and each use runs its own copy.
#[derive(Debug)]
pub struct ReuseExec {
    input: Arc<dyn ExecutionPlan>,
    held: Arc<Held>,
}

impl DisplayAs for ReuseExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ReuseExec: uses={}", self.held.uses.load(Relaxed))
    }
}

impl ExecutionPlan for ReuseExec {
    fn name(&self) -> &str {
        "ReuseExec"
    }

    // (its rows are its input's, in the same partitions and order)
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(self: Arc<Self>, mut children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(ReuseExec { input: children.swap_remove(0), held: self.held.clone() })),
            n => Err(DataFusionError::Internal(format!("ReuseExec has one input, not {n}"))),
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(&self, input_stats: &[Arc<Statistics>], _: &StatisticsArgs) -> Result<Arc<Statistics>> {
        Ok(input_stats[0].clone())
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(&self, p: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let schema = self.input.schema();
        let (own, held) = (self.input.clone(), self.held.clone());
        let rows = futures::stream::once(take(own, held, p, ctx)).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, rows)))
    }
}

/// One use's rows of partition `p`: the held ones, once the first use to get here has run it; else its own copy's.
async fn take(own: Arc<dyn ExecutionPlan>, held: Arc<Held>, p: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    let schema = own.schema();
    let part = held.part(p);
    let mut slot = part.lock().await;
    if matches!(*slot, Part::Unrun) {
        let input = held.input.get_or_init(|| own.clone()).clone();
        let reservation = MemoryConsumer::new(format!("ReuseExec[{p}]")).register(ctx.memory_pool());
        let mut stream = input.execute(p, ctx.clone())?;
        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if reservation.try_grow(batch.get_array_memory_size()).is_err() {
                // Too big to hold: this use reads on from its own copy, and the others run theirs.
                *slot = Part::Own;
                let rest = futures::stream::iter(batches.into_iter().chain(std::iter::once(batch)).map(Ok)).chain(stream);
                return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, rest)));
            }
            batches.push(batch);
        }
        *slot = Part::Kept(Arc::new(Kept { batches, _reservation: reservation }), 0);
    }
    let uses = held.uses.load(Relaxed).max(1);
    let kept = match &mut *slot {
        Part::Kept(kept, taken) => {
            *taken += 1;
            let kept = kept.clone();
            if *taken >= uses {
                *slot = Part::Unrun; // (every use has its rows: they go once the last has read them, and a plan run again runs it again)
            }
            kept
        }
        Part::Own => {
            drop(slot);
            return own.execute(p, ctx);
        }
        Part::Unrun => return Err(DataFusionError::Internal("ReuseExec: a partition neither run nor held".into())),
    };
    let rows = (0..kept.batches.len()).map(move |i| Ok(kept.batches[i].clone()));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, futures::stream::iter(rows))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    async fn session(rt: Arc<RuntimeEnv>, reuse: bool) -> Result<SessionContext> {
        let ctx = crate::engine::session(SessionConfig::new().with_target_partitions(4), rt);
        let rules = crate::optimize::physical_rules().into_iter().filter(|r| reuse || r.name() != "Reuse").collect();
        let ctx = SessionContext::new_with_state(SessionStateBuilder::new_from_existing(ctx.state()).with_physical_optimizer_rules(rules).build());
        ctx.sql("CREATE TABLE sales AS SELECT v % 1000 AS item, v % 7 AS store, v * 1.5 AS amount FROM generate_series(1, 200000) AS t(v)").await?.collect().await?;
        Ok(ctx)
    }

    /// The answer as text, and the plan.
    async fn answer(ctx: &SessionContext, sql: &str) -> Result<(String, String)> {
        let df = ctx.sql(sql).await?;
        let plan = df.clone().create_physical_plan().await?;
        let shown = displayable(plan.as_ref()).indent(true).to_string();
        Ok((pretty_format_batches(&df.collect().await?)?.to_string(), shown))
    }

    const TWICE: &str = "WITH t AS (SELECT item, sum(amount) AS total FROM sales GROUP BY item) \
        SELECT a.item, a.total FROM t a JOIN t b ON a.item = b.item + 1 WHERE a.total > b.total ORDER BY a.item LIMIT 5";

    /// A CTE that groups, named twice, runs once and answers as it did run twice; run again, it runs again.
    #[tokio::test]
    async fn a_cte_named_twice_runs_once() -> Result<()> {
        let ctx = session(Arc::new(RuntimeEnv::default()), true).await?;
        let (once, plan) = answer(&ctx, TWICE).await?;
        assert_eq!(plan.matches("ReuseExec: uses=2").count(), 2, "{plan}");
        let (twice, plan) = answer(&session(Arc::new(RuntimeEnv::default()), false).await?, TWICE).await?;
        assert!(!plan.contains("ReuseExec"), "{plan}");
        assert_eq!(once, twice);
        assert_eq!(answer(&ctx, TWICE).await?.0, once, "a second run");
        Ok(())
    }

    /// Uses the filters reach differently aren't alike: each keeps its own copy, filtered as before.
    #[tokio::test]
    async fn uses_filtered_apart_keep_their_own() -> Result<()> {
        let ctx = session(Arc::new(RuntimeEnv::default()), true).await?;
        let sql = "WITH t AS (SELECT store, item, sum(amount) AS total FROM sales GROUP BY store, item) \
                   SELECT a.item, a.total, b.total FROM t a JOIN t b ON a.item = b.item WHERE a.store = 1 AND b.store = 2 ORDER BY a.item LIMIT 5";
        let (_, plan) = answer(&ctx, sql).await?;
        assert!(!plan.contains("ReuseExec"), "{plan}");
        Ok(())
    }

    /// A use that a join filters as it runs, by the keys of a filtered side it builds, groups only those keys' rows:
    /// held, it would group every row (TPC-DS q23), so each use keeps its own copy.
    #[tokio::test]
    async fn uses_a_join_filters_keep_their_own() -> Result<()> {
        let ctx = session(Arc::new(RuntimeEnv::default()), true).await?;
        ctx.sql("CREATE TABLE few AS SELECT v AS item, v % 3 AS kind FROM generate_series(1, 30) AS t(v)").await?.collect().await?;
        let sql = "WITH t AS (SELECT item, count(*) AS n FROM sales GROUP BY item) \
                   SELECT sum(t.n) FROM few JOIN t ON few.item = t.item WHERE few.kind = 1 \
                   UNION ALL SELECT sum(t.n) FROM few JOIN t ON few.item = t.item WHERE few.kind = 2";
        let (got, plan) = answer(&ctx, sql).await?;
        assert!(plan.contains("HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(item@0, item@0)]"), "few builds, t probes: {plan}");
        assert!(!plan.contains("ReuseExec"), "{plan}");
        let plain = session(Arc::new(RuntimeEnv::default()), false).await?;
        plain.sql("CREATE TABLE few AS SELECT v AS item, v % 3 AS kind FROM generate_series(1, 30) AS t(v)").await?.collect().await?;
        assert_eq!(got, answer(&plain, sql).await?.0);
        Ok(())
    }

    /// A join that gives as many rows as it reads isn't held: run twice, the joins above filter its scans as they run.
    #[tokio::test]
    async fn a_join_as_big_as_its_input_runs_at_each_use() -> Result<()> {
        let ctx = session(Arc::new(RuntimeEnv::default()), true).await?;
        let sql = "WITH t AS (SELECT a.item, a.amount FROM sales a JOIN sales b ON a.item = b.item AND a.store <> b.store) \
                   SELECT count(*) FROM sales WHERE item IN (SELECT item FROM t) AND item IN (SELECT item FROM t WHERE amount > 10)";
        let (_, plan) = answer(&ctx, sql).await?;
        assert!(!plan.contains("ReuseExec"), "{plan}");
        Ok(())
    }

    /// What doesn't fit in the query's memory isn't held: the use that ran it reads on from its own copy, each other use
    /// runs its own, and every use gets every row. With room, the second use reads the rows the first held.
    #[tokio::test]
    async fn too_big_to_hold_runs_each_copy() -> Result<()> {
        let ctx = session(Arc::new(RuntimeEnv::default()), true).await?;
        let input = ctx.sql("SELECT item, amount FROM sales").await?.create_physical_plan().await?; // (a scan: it reserves nothing)
        for limit in [1 << 10, 1 << 30] {
            let held = Arc::new(Held::default());
            held.uses.store(2, Relaxed);
            let rt = RuntimeEnvBuilder::new().with_memory_limit(limit, 1.0).build_arc()?;
            let task = Arc::new(TaskContext::default().with_runtime(rt));
            for _ in 0..2 {
                let one: Arc<dyn ExecutionPlan> = Arc::new(ReuseExec { input: input.clone(), held: held.clone() });
                let mut rows = 0;
                for p in 0..one.properties().partitioning.partition_count() {
                    rows += one.execute(p, task.clone())?.try_collect::<Vec<_>>().await?.iter().map(|b| b.num_rows()).sum::<usize>();
                }
                assert_eq!(rows, 200_000, "memory limit {limit}");
            }
            let own = held.parts.lock().unwrap().values().all(|p| matches!(*p.try_lock().unwrap(), Part::Own));
            assert_eq!(own, limit < 1 << 20, "memory limit {limit}");
        }
        Ok(())
    }
}
