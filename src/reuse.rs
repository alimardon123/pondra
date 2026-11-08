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
//! whose join learns such a filter for its grouping keeps its own copy (`keyed`: q23's `frequent_ss_items`). The rows
//! stay held until the plan goes, for each read of them: a read past the uses counted (a plan run again) gets the same
//! rows, never a second run of the copy. A partition being held is kept where every use finds it: a use dropped
//! halfway (an anti join whose build side is empty drops its probe side) leaves it to the next. What doesn't fit in the query's memory goes to disk, as a sort's rows do, and
//! each use reads it from there: one copy runs
//! every partition, so no use runs its own beside it (a scan's partitions share its files out as they run, and a
//! partition of a second copy, run alone, would read them all). A plan reset to run again (`fresh`'s kept plans, a
//! recursive query's rounds) runs each use's own copy, as without this. A spread query's slices (`optimize::spread`)
//! aren't touched: every node plans a slice the same way.
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::common::utils::memory::RecordBatchMemoryCounter;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::buffer::BufferExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::execution_plan::{replace_children_if_necessary, CardinalityEffect};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::spill::SpillManager;
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

/// A subplan as text, every detail of each of its nodes, with what each of its scans reads where the text doesn't say
/// (`source`): two subplans of one key read the same rows the same way and give the same rows.
fn key(p: &Arc<dyn ExecutionPlan>) -> String {
    let mut text = displayable(p.as_ref()).indent(true).to_string();
    for leaf in leaves(p) {
        text.push_str(&source(&leaf));
    }
    text
}

/// What a scan reads, where its text doesn't say: its columns, and where rows held in memory are. A scan of files names
/// them, with its columns and filters. Rows in memory (a table made in memory, a transaction's own writes, a VALUES
/// list) show only how many batches they are, so two such tables of one shape would read alike: their batches' arrays
/// tell them apart. A scan of one empty row or of none gives the same rows wherever it is; any other is alike only to itself.
fn source(leaf: &Arc<dyn ExecutionPlan>) -> String {
    let columns: Vec<String> = leaf.schema().fields().iter().map(|f| format!("{}: {}", f.name(), f.data_type())).collect();
    let at = match leaf.downcast_ref::<DataSourceExec>().map(|s| s.data_source()) {
        Some(s) if s.downcast_ref::<FileScanConfig>().is_some() => String::new(),
        Some(s) => match s.downcast_ref::<MemorySourceConfig>() {
            Some(m) => {
                let arrays = m.partitions().iter().flatten().map(|b| (b.num_rows(), b.columns().iter().map(|c| Arc::as_ptr(c) as *const () as usize).collect()));
                format!("{:?}", arrays.collect::<Vec<(usize, Vec<usize>)>>())
            }
            None => format!("{:p}", Arc::as_ptr(leaf) as *const ()),
        },
        None if leaf.is::<PlaceholderRowExec>() || leaf.is::<EmptyExec>() => String::new(),
        None => format!("{:p}", Arc::as_ptr(leaf) as *const ()),
    };
    format!("\n[{}] {at}", columns.join(", "))
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
/// many uses there are (`Reuse` counts them: shown in the plan). One copy runs every partition, whichever use asks first: a copy's
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
    Running(Box<Running>), // being held: a use that goes away (its join's build side empty) leaves it to the next
    Kept(Rows),            // its rows, for every use that reads it, until the plan goes
    Failed(String),        // its run failed, and why: it isn't run again
}

/// A partition being held, kept in its slot rather than in the use running it: a use's stream dropped halfway (a join
/// whose build side is empty drops its probe side's) leaves the copy's stream and the rows so far for the next use to go
/// on from, and the copy's partition is never run twice.
struct Running {
    stream: SendableRecordBatchStream,
    held: Option<Holding>,
}

/// What a partition being held has so far.
enum Holding {
    Memory { batches: Vec<RecordBatch>, counter: RecordBatchMemoryCounter, reservation: MemoryReservation },
    Disk(ToDisk),
}

/// A spill file being written (DataFusion's, which counts it against the query's disk): a batch appends it, and none
/// finishes the file. (DataFusion doesn't name its type outside its crate.)
type ToDisk = Box<dyn FnMut(Option<&RecordBatch>) -> Result<Option<Arc<dyn SpillFile>>> + Send>;

impl std::fmt::Debug for Running {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Running")
    }
}

impl Running {
    fn new(stream: SendableRecordBatchStream, p: usize, ctx: &Arc<TaskContext>) -> Running {
        let reservation = MemoryConsumer::new(format!("ReuseExec[{p}]")).register(ctx.memory_pool());
        Running { stream, held: Some(Holding::Memory { batches: vec![], counter: RecordBatchMemoryCounter::new(), reservation }) }
    }

    /// The rest of the partition's rows held: in the query's memory, each buffer counted once (an aggregate gives one
    /// batch's slices), or on disk once they don't fit. Each step is kept here as it is taken, so this may be dropped at
    /// any await and taken up again.
    async fn finish(&mut self, p: usize, ctx: &Arc<TaskContext>) -> Result<Rows> {
        while let Some(batch) = self.stream.next().await {
            let batch = batch?;
            let full = match self.held.as_mut() {
                Some(Holding::Memory { counter, reservation, .. }) => reservation.try_grow(counter.count_batch(&batch)).is_err(),
                _ => false,
            };
            match self.held.as_mut() {
                Some(Holding::Memory { batches, .. }) if full => {
                    let mut file = spill(ctx, self.stream.schema(), p).create_in_progress_file(&format!("ReuseExec[{p}]"))?;
                    let mut file: ToDisk = Box::new(move |b| match b {
                        Some(b) => file.append_batch(b).map(|_| None),
                        None => file.finish(),
                    });
                    for b in batches.iter().chain(std::iter::once(&batch)) {
                        file(Some(b))?;
                    }
                    self.held = Some(Holding::Disk(file)); // (the batches held, and their memory, go)
                }
                Some(Holding::Memory { batches, .. }) => batches.push(batch),
                Some(Holding::Disk(file)) => {
                    file(Some(&batch))?;
                }
                None => return Err(DataFusionError::Internal("ReuseExec: a partition held twice".into())),
            }
        }
        Ok(match self.held.take() {
            Some(Holding::Memory { batches, reservation, .. }) => Rows::Held(Arc::new(Kept { batches, _reservation: reservation })),
            Some(Holding::Disk(mut file)) => match file(None)? {
                Some(file) => Rows::Spilled(Arc::new(Spilled(file))),
                None => Rows::Held(Arc::new(Kept { batches: vec![], _reservation: MemoryConsumer::new("ReuseExec").register(ctx.memory_pool()) })),
            },
            None => return Err(DataFusionError::Internal("ReuseExec: a partition held twice".into())),
        })
    }
}

/// A partition's rows: in the query's memory, or on disk where they didn't fit.
#[derive(Debug, Clone)]
enum Rows {
    Held(Arc<Kept>),
    Spilled(Arc<Spilled>),
}

/// A partition's rows, counted in the query's memory while they are held.
#[derive(Debug)]
struct Kept {
    batches: Vec<RecordBatch>,
    _reservation: MemoryReservation,
}

/// A partition's rows on disk (gone with the plan, and the last read of them).
struct Spilled(Arc<dyn SpillFile>);

impl std::fmt::Debug for Spilled {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Spilled({:?} bytes)", self.0.size())
    }
}

/// One use of a subplan that appears more than once alike (`Reuse`): the first use to run a partition runs it and
/// holds its rows, and every use reads those; past the query's memory they are held on disk.
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

    // A plan reset to run again runs each use's own copy (its input, reset), as it would without this: the rows held
    // for the run before are another run's, and the uses' copies reset one at a time share nothing to say they are one run.
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.input.clone())
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

/// One use's rows of partition `p`: the held ones, once the first use to get here has run it.
async fn take(own: Arc<dyn ExecutionPlan>, held: Arc<Held>, p: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    let schema = own.schema();
    let part = held.part(p);
    let mut slot = part.lock().await;
    if matches!(*slot, Part::Unrun) {
        let input = held.input.get_or_init(|| own.clone()).clone();
        match input.execute(p, ctx.clone()) {
            Ok(stream) => *slot = Part::Running(Box::new(Running::new(stream, p, &ctx))),
            Err(e) => {
                *slot = Part::Failed(e.to_string());
                return Err(e);
            }
        }
    }
    if let Part::Running(run) = &mut *slot {
        match run.finish(p, &ctx).await {
            Ok(rows) => *slot = Part::Kept(rows),
            Err(e) => {
                *slot = Part::Failed(e.to_string()); // (every other use is told the same: the copy isn't run again)
                return Err(e);
            }
        }
    }
    // (every read of it gets the same rows: a plan run again reads them again, and isn't the run that held them)
    let rows = match &*slot {
        Part::Kept(rows) => rows.clone(),
        Part::Failed(why) => return Err(DataFusionError::Execution(format!("{why} (ReuseExec: partition {p}'s run, which its uses share)"))),
        _ => return Err(DataFusionError::Internal(format!("ReuseExec: partition {p} neither run nor held"))),
    };
    drop(slot);
    match rows {
        Rows::Held(kept) => {
            let rows = (0..kept.batches.len()).map(move |i| Ok(kept.batches[i].clone()));
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, futures::stream::iter(rows))))
        }
        Rows::Spilled(file) => spill(&ctx, schema, p).read_spill_as_stream(file.0.clone(), None),
    }
}

/// Where a partition's rows that don't fit in memory go: the query's disk, as a sort's.
fn spill(ctx: &Arc<TaskContext>, schema: datafusion::arrow::datatypes::SchemaRef, p: usize) -> SpillManager {
    SpillManager::new(ctx.runtime_env(), SpillMetrics::new(&ExecutionPlanMetricsSet::new(), p), schema)
        .with_compression_type(ctx.session_config().options().execution.spill_compression)
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

    /// Two tables in memory of one shape plan alike, their scans showing only how many batches they hold, and are two
    /// tables: each is grouped on its own (`source`). (A transaction's writes are such a table, and a VALUES list.)
    #[tokio::test]
    async fn tables_in_memory_alike_are_two() -> Result<()> {
        let sql = "SELECT 'a' AS t, item, count(*) AS n FROM a GROUP BY item UNION ALL SELECT 'b', item, count(*) FROM b GROUP BY item ORDER BY 1, 2";
        let mut answers = vec![];
        for reuse in [true, false] {
            let ctx = session(Arc::new(RuntimeEnv::default()), reuse).await?;
            ctx.sql("CREATE TABLE a AS SELECT v % 3 AS item FROM generate_series(1, 300) AS t(v)").await?.collect().await?;
            ctx.sql("CREATE TABLE b AS SELECT v % 5 AS item FROM generate_series(1, 300) AS t(v)").await?.collect().await?;
            let (got, plan) = answer(&ctx, sql).await?;
            assert!(!plan.contains("ReuseExec"), "{plan}");
            answers.push(got);
            // (the control: one table named twice is held)
            let (_, plan) = answer(&ctx, &sql.replace("FROM b", "FROM a")).await?;
            assert_eq!(plan.contains("ReuseExec: uses=2"), reuse, "{plan}");
        }
        assert_eq!(answers[0], answers[1]);
        assert_eq!(answers[0].lines().filter(|l| l.starts_with("| b ")).count(), 5, "b's five items: {}", answers[0]);
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

    /// What doesn't fit in the query's memory goes to disk, and every use reads every row from the one copy that ran:
    /// the second use's own copy (here an empty one) is never run, whichever use asks for a partition first. A read
    /// past the uses counted (a plan run again) gets the same rows, the copy not run again.
    #[tokio::test]
    async fn too_big_to_hold_goes_to_disk() -> Result<()> {
        let ctx = session(Arc::new(RuntimeEnv::default()), true).await?;
        let input = ctx.sql("SELECT item, amount FROM sales").await?.create_physical_plan().await?; // (a scan: it reserves nothing)
        let parts = input.properties().partitioning.partition_count();
        let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(input.schema()).with_partitions(parts));
        for limit in [1 << 10, 1 << 30] {
            let held = Arc::new(Held::default());
            held.uses.store(2, Relaxed);
            let rt = RuntimeEnvBuilder::new().with_memory_limit(limit, 1.0).build_arc()?;
            let task = Arc::new(TaskContext::default().with_runtime(rt));
            let one: Arc<dyn ExecutionPlan> = Arc::new(ReuseExec { input: input.clone(), held: held.clone() });
            let two: Arc<dyn ExecutionPlan> = Arc::new(ReuseExec { input: empty.clone(), held: held.clone() });
            let (mut first, mut second) = (0, 0);
            for p in 0..parts {
                // (the first use asks first for the first partition, the second for every other)
                let order = if p == 0 { [(&one, &mut first), (&two, &mut second)] } else { [(&two, &mut second), (&one, &mut first)] };
                for (i, (plan, rows)) in order.into_iter().enumerate() {
                    *rows += plan.execute(p, task.clone())?.try_collect::<Vec<_>>().await?.iter().map(|b| b.num_rows()).sum::<usize>();
                    if i == 0 {
                        let spilled = matches!(&*held.part(p).try_lock().unwrap(), Part::Kept(Rows::Spilled(_)));
                        assert_eq!(spilled, limit < 1 << 20, "memory limit {limit}, partition {p}");
                    }
                }
            }
            assert_eq!((first, second), (200_000, 200_000), "memory limit {limit}");
            let mut again = 0;
            for p in 0..parts {
                again += two.execute(p, task.clone())?.try_collect::<Vec<_>>().await?.iter().map(|b| b.num_rows()).sum::<usize>();
            }
            assert_eq!(again, 200_000, "a third read, memory limit {limit}");
        }
        Ok(())
    }

    /// A use that goes away while its partition is being held (a join whose build side is empty drops its probe side)
    /// leaves it to the next use, which goes on from where it was: the copy's partitions run once, and every other use
    /// reads every group.
    #[tokio::test]
    async fn a_use_that_goes_away_leaves_its_partition_to_the_next() -> Result<()> {
        let ctx = session(Arc::new(RuntimeEnv::default()), true).await?;
        let input = ctx.sql("SELECT item, sum(amount) AS total FROM sales GROUP BY item").await?.create_physical_plan().await?;
        let parts = input.properties().partitioning.partition_count();
        let held = Arc::new(Held::default());
        held.uses.store(3, Relaxed);
        let task = ctx.task_ctx();
        let uses: Vec<Arc<dyn ExecutionPlan>> = (0..3).map(|_| Arc::new(ReuseExec { input: input.clone(), held: held.clone() }) as _).collect();
        for p in 0..parts {
            let mut gone = uses[0].execute(p, task.clone())?;
            assert!(futures::poll!(gone.next()).is_pending(), "partition {p}: its exchange's rows not in yet");
        }
        assert!(held.parts.lock().unwrap().values().all(|p| matches!(*p.try_lock().unwrap(), Part::Running(_))), "each left halfway");
        for u in &uses[1..] {
            let mut groups = 0;
            for p in 0..parts {
                groups += u.execute(p, task.clone())?.try_collect::<Vec<_>>().await?.iter().map(|b| b.num_rows()).sum::<usize>();
            }
            assert_eq!(groups, 1000);
        }
        Ok(())
    }

    /// A plan reset to run again (as `fresh` runs its kept plans) runs each use's own copy, and answers as before.
    #[tokio::test]
    async fn a_plan_reset_runs_each_use_again() -> Result<()> {
        let ctx = session(Arc::new(RuntimeEnv::default()), true).await?;
        let df = ctx.sql(TWICE).await?;
        let plan = df.clone().create_physical_plan().await?;
        assert!(displayable(plan.as_ref()).indent(true).to_string().contains("ReuseExec: uses=2"));
        let first = pretty_format_batches(&datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx()).await?)?.to_string();
        for _ in 0..2 {
            let again = datafusion::physical_plan::execution_plan::reset_plan_states(plan.clone())?;
            assert!(!displayable(again.as_ref()).indent(true).to_string().contains("ReuseExec"));
            assert_eq!(pretty_format_batches(&datafusion::physical_plan::collect(again, ctx.task_ctx()).await?)?.to_string(), first);
        }
        Ok(())
    }
}
