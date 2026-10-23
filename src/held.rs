//! What a window holds counts in the query memory (invariant 414). DataFusion's windows keep the rows they still need
//! with no memory reservation, so `count(*) OVER ()` over 60 M rows grew a node until the OS killed it. Each window sits
//! between `HeldInExec` (counts each batch going in) and `HeldOutExec` (lets go of each batch as its rows come out), which
//! share one reservation per partition. A window that streams copies its string columns plain (`plain_below`), so the
//! strings its views point into are no longer uncounted; still uncounted: what DataFusion keeps inside a window beyond
//! those rows (a frame's rows behind those given out, `WindowAggExec`'s concatenation, the view columns a partition key
//! or a function reads, which stay views).
//! `LeanExec` is the other operator here: a sort's output of strings holds only its own rows' strings (`LeanSorts`).
use crate::store::memory_limit;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::{cast, Column};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{replace_children_if_necessary, CardinalityEffect};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties, StatisticsArgs};
use futures::StreamExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

/// One partition's bytes in a window: the reservation, and the (rows, bytes) of each batch that went in whose rows haven't
/// come out yet, oldest first.
#[derive(Debug)]
struct Held {
    reservation: MemoryReservation,
    batches: VecDeque<(usize, usize)>, // (rows, bytes) of each batch in, oldest first
}

/// Each partition's `Held`, from its `HeldOutExec`'s execute to its `HeldInExec`'s. Copies of a plan share it
/// (`with_new_children`), and each partition of a plan runs one execution at a time.
#[derive(Debug, Default)]
struct Slots(Mutex<HashMap<usize, Arc<Mutex<Held>>>>);

/// The window's output side: lets go of each batch's bytes as the window hands its rows out.
#[derive(Debug)]
pub struct HeldOutExec {
    input: Arc<dyn ExecutionPlan>,
    slots: Arc<Slots>,
    props: Arc<PlanProperties>,
}

/// The window's input side: counts each batch going into the window against the query memory. `whole`: the window is a
/// `WindowAggExec` over whole partitions, which the frugal retry answers (its refusal says so: `query::exhausted`).
#[derive(Debug)]
pub struct HeldInExec {
    input: Arc<dyn ExecutionPlan>,
    slots: Arc<Slots>,
    props: Arc<PlanProperties>,
    extra: usize,
    whole: bool,
}

/// The bytes a batch's rows hold in a window: their own slices (a slice of a bigger batch counts its own rows),
/// the strings their views point to past 12 bytes (a view's slice size leaves those out), and the window's own
/// columns for them.
fn bytes(b: &RecordBatch, extra: usize) -> usize {
    b.columns().iter().map(|c| c.to_data().get_slice_memory_size().unwrap_or_else(|_| c.get_array_memory_size()) + match c.data_type() {
        DataType::Utf8View => c.as_string_view().total_buffer_bytes_used(),
        DataType::BinaryView => c.as_binary_view().total_buffer_bytes_used(),
        _ => 0,
    }).sum::<usize>() + b.num_rows() * extra
}

/// Lets go of the bytes of the oldest batches whose rows have come out: `rows` rows of them, in order.
fn release(h: &mut Held, mut rows: usize) {
    let mut freed = 0;
    while rows > 0 {
        let Some(&(n, b)) = h.batches.front() else { break };
        if n <= rows {
            h.batches.pop_front();
            (rows, freed) = (rows - n, freed + b);
        } else {
            let part = b * rows / n;
            h.batches[0] = (n - rows, b - part);
            (rows, freed) = (0, freed + part);
        }
    }
    h.reservation.shrink(freed.min(h.reservation.size())); // (`shrink` panics past what it holds)
}

/// Plans every window between its counting pair. The last physical rule (`optimize::physical_rules`): nothing after it
/// moves a window.
#[derive(Debug)]
pub struct Windows;

impl PhysicalOptimizerRule for Windows {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let keys = key_columns(&plan)?;
        plan.transform_up(|node| {
            let window = node.is::<WindowAggExec>() || node.is::<BoundedWindowAggExec>();
            let counted = node.children().first().is_some_and(|c| c.is::<HeldInExec>()); // (the rule may meet a plan twice)
            if !window || node.children().len() != 1 || counted {
                return Ok(Transformed::no(node));
            }
            let child = node.children()[0].clone();
            let child_fields = child.schema().fields().len();
            let extra: usize = node.schema().fields()[child_fields..].iter().map(|f| f.data_type().primitive_width().unwrap_or(16)).sum(); // (the window's own columns, a value a row each)
            // (`WindowAggExec` concatenates once at the end and is counted whole: only a streaming window is changed)
            let streams = node.downcast_ref::<BoundedWindowAggExec>().is_some_and(|w| !reads_view(w, &child.schema()));
            let whole = node.is::<WindowAggExec>();
            let input = if streams { plain_below(&child, &keys)? } else { child.clone() };
            let uncast = !Arc::ptr_eq(&input, &child);
            let slots = Arc::new(Slots::default());
            let kid: Arc<dyn ExecutionPlan> = Arc::new(HeldInExec { props: input.properties().clone(), input, slots: slots.clone(), extra, whole });
            let w = replace_children_if_necessary(node, vec![kid])?; // (`with_new_children` is deprecated in DataFusion 55.1: the repo's own call, as in `optimize.rs`)
            let out: Arc<dyn ExecutionPlan> = Arc::new(HeldOutExec { props: w.properties().clone(), input: w, slots });
            Ok(Transformed::yes(if uncast { views_back(&out, &child.schema())? } else { out }))
        })
        .data()
    }

    fn name(&self) -> &str {
        "held_windows"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The plain type a view column is held as, or None for every other type.
fn plain(t: &DataType) -> Option<DataType> {
    match t {
        DataType::Utf8View => Some(DataType::Utf8),
        DataType::BinaryView => Some(DataType::Binary),
        _ => None,
    }
}

/// The names of the columns the windows of `plan` read as partition keys. A key stays a view: a projection that casts it
/// hides the ordering the window finds its partitions by (`get_partition_by_sort_exprs`), and the window fails. The other
/// columns of an ordering pass through a projection as they are, so an expression of them keeps its order.
fn key_columns(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<String>> {
    let mut names = HashSet::new();
    plan.apply(|node| {
        if let Some(w) = node.downcast_ref::<BoundedWindowAggExec>() {
            for window in w.window_expr() {
                for key in window.partition_by() {
                    names.extend(collect_columns(key).into_iter().map(|c| c.name().to_string()));
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(names)
}

/// Whether a window function reads a view column of its input as an argument. The function's output takes that column's
/// type, so such a window keeps its input as it is.
fn reads_view(window: &BoundedWindowAggExec, input: &Schema) -> bool {
    window.window_expr().iter().flat_map(|w| w.expressions()).any(|arg| {
        collect_columns(&arg).iter().any(|c| input.fields().get(c.index()).is_some_and(|f| plain(f.data_type()).is_some()))
    })
}

/// `input` with each column cast to the type `types` gives it (names and nullability kept), or `input` itself when no
/// type changes.
fn recast(input: &Arc<dyn ExecutionPlan>, types: impl Fn(usize, &Field) -> DataType) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let mut exprs = vec![];
    let mut changed = false;
    for (i, f) in schema.fields().iter().enumerate() {
        let to = types(i, f);
        changed |= to != *f.data_type();
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(f.name(), i));
        exprs.push((cast(column, &schema, to)?, f.name().clone()));
    }
    if !changed {
        return Ok(Arc::clone(input));
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, Arc::clone(input))?))
}

/// A streaming window's input with its string columns plain: each view column that no partition key reads is cast to its
/// plain type. The window holds the plain copies, so the view buffers of the rows it has let go are not kept (a window
/// concatenates its partition's batches, and concatenating views keeps every batch's buffers).
fn plain_below(input: &Arc<dyn ExecutionPlan>, keys: &HashSet<String>) -> Result<Arc<dyn ExecutionPlan>> {
    recast(input, |_, f| match plain(f.data_type()) {
        Some(to) if !keys.contains(f.name()) => to,
        _ => f.data_type().clone(),
    })
}

/// A window's output with its columns from the input back in the types they had, so everything above sees the schema it
/// was planned with.
fn views_back(out: &Arc<dyn ExecutionPlan>, input: &Schema) -> Result<Arc<dyn ExecutionPlan>> {
    recast(out, |i, f| if i < input.fields().len() { input.field(i).data_type().clone() } else { f.data_type().clone() })
}

impl DisplayAs for HeldOutExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HeldOutExec")
    }
}

impl DisplayAs for HeldInExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HeldInExec")
    }
}

fn one_input(mut children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
    match children.len() {
        1 => Ok(children.swap_remove(0)),
        n => Err(DataFusionError::Internal(format!("this operator has one input, not {n}"))),
    }
}

impl ExecutionPlan for HeldOutExec {
    fn name(&self) -> &str {
        "HeldOutExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let input = one_input(children)?;
        Ok(Arc::new(HeldOutExec { props: input.properties().clone(), input, slots: self.slots.clone() }))
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
        let held = Arc::new(Mutex::new(Held {
            // (A window never spills: its rows wait for the partition's end. It is spillable so the pool gives it a fair
            // share beside the sorts that feed it. `FairSpillPool` refuses an unspillable grow whenever the sorts' shares are
            // full, though the window holds little; a spillable one is checked against its own share, and refused past it.)
            reservation: MemoryConsumer::new(format!("Window[{p}]")).with_can_spill(true).register(ctx.memory_pool()),
            batches: VecDeque::new(),
        }));
        self.slots.0.lock().unwrap().insert(p, held.clone());
        let s = self.input.execute(p, ctx); // (the window executes its input at once: that is how HeldInExec finds `held`)
        self.slots.0.lock().unwrap().remove(&p);
        let s = s?;
        let schema = s.schema();
        let out = s.map(move |item| -> Result<RecordBatch> {
            let batch = item?;
            release(&mut held.lock().unwrap(), batch.num_rows());
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

impl ExecutionPlan for HeldInExec {
    fn name(&self) -> &str {
        "HeldInExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let input = one_input(children)?;
        Ok(Arc::new(HeldInExec { props: input.properties().clone(), input, slots: self.slots.clone(), extra: self.extra, whole: self.whole }))
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
        let held = self.slots.0.lock().unwrap().get(&p).cloned();
        let s = self.input.execute(p, ctx)?;
        let Some(held) = held else { return Ok(s) }; // (run without its HeldOutExec: the input as it is)
        let schema = s.schema();
        let (extra, whole) = (self.extra, self.whole);
        let out = s.map(move |item| -> Result<RecordBatch> {
            let batch = item?;
            if batch.num_rows() == 0 {
                return Ok(batch);
            }
            let b = bytes(&batch, extra);
            let mut h = held.lock().unwrap();
            h.reservation.try_grow(b).map_err(|_| {
                let message = format!("out of memory: a window holds more rows than this node's memory for queries ({} MB): partition it by more columns, or give the node more memory (--memory-gb)", memory_limit() >> 20);
                // ("Resources exhausted" is what starts the frugal retry (`query::exhausted`): a window over whole partitions is
                // answered there as a GROUP BY. A streaming window's retry would run out the same way, so its refusal is a plain
                // error. Both give 53200 on every door, by the message's words.)
                match whole {
                    true => DataFusionError::ResourcesExhausted(message),
                    false => DataFusionError::Execution(message),
                }
            })?;
            h.batches.push_back((batch.num_rows(), b));
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// Hands up each batch with only its own rows' strings (`answer::compact`). A sort reads a string column as views into
/// its input's buffers, and `take` and the merge's `interleave` copy the views but keep those buffers: a batch of
/// 8,192 rows then points into every batch of the runs it merges. The merge above counts each batch's buffers whole,
/// so 4 partitions of a 10 M-row sort of md5 strings counted 66 MB for about 2 MB of rows, and a 256 MB pool refused
/// it (`SortPreservingMergeExec`). Passing the batches through `compact` costs a copy only where they are wasteful.
#[derive(Debug)]
pub struct LeanExec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for LeanExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LeanExec")
    }
}

impl ExecutionPlan for LeanExec {
    fn name(&self) -> &str {
        "LeanExec"
    }

    // (no properties of its own: it hands up what its input does, in the same order and partitions)
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LeanExec { input: one_input(children)? }))
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

    // (the rows are the input's, so its statistics are too: `guard` and the history read them above a sort)
    fn statistics_from_inputs(&self, input_stats: &[Arc<Statistics>], _: &StatisticsArgs) -> Result<Arc<Statistics>> {
        Ok(input_stats[0].clone())
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(&self, p: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let s = self.input.execute(p, ctx)?;
        let schema = s.schema();
        let out = s.map(|item| item.map(crate::answer::compact));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// Puts a `LeanExec` above each sort that keeps all its rows and sorts strings (why: `LeanExec`). A top-N keeps a few
/// rows, so it is left alone. A second run wraps nothing again: a `LeanExec` over a `LeanExec` becomes the inner one.
#[derive(Debug)]
pub struct LeanSorts;

impl PhysicalOptimizerRule for LeanSorts {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if let Some(lean) = node.downcast_ref::<LeanExec>() {
                if lean.input.is::<LeanExec>() {
                    return Ok(Transformed::yes(lean.input.clone()));
                }
                return Ok(Transformed::no(node));
            }
            let all_rows = node.downcast_ref::<SortExec>().is_some_and(|s| s.fetch().is_none());
            let strings = node.schema().fields().iter().any(|f| matches!(f.data_type(), DataType::Utf8View | DataType::BinaryView));
            if all_rows && strings {
                let lean: Arc<dyn ExecutionPlan> = Arc::new(LeanExec { input: node });
                return Ok(Transformed::yes(lean));
            }
            Ok(Transformed::no(node))
        })
        .data()
    }

    fn name(&self) -> &str {
        "lean_sorts"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result as AnyResult;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringViewArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{Field, Int64Type, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn small_session() -> AnyResult<SessionContext> {
        let rt = RuntimeEnvBuilder::new().with_memory_pool(Arc::new(GreedyMemoryPool::new(4 << 20))).build_arc()?;
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().execution.sort_spill_reservation_bytes = 1 << 20; // (the default 10 MB is more than the pool)
        let mut rules = PhysicalOptimizer::new().rules;
        rules.push(Arc::new(Windows));
        Ok(SessionContext::new_with_state(SessionStateBuilder::new().with_config(config).with_runtime_env(rt).with_default_features().with_physical_optimizer_rules(rules).build()))
    }

    async fn answer(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        ctx.sql(sql).await?.collect().await
    }

    /// The one number a query answers, as an integer whatever its type.
    async fn number(ctx: &SessionContext, sql: &str) -> AnyResult<i64> {
        let batches = answer(ctx, sql).await?;
        let column = cast(batches[0].column(0), &DataType::Int64)?;
        Ok(column.as_primitive::<Int64Type>().value(0))
    }

    /// The error a query is refused with, or None when it answers.
    async fn refusal(ctx: &SessionContext, sql: &str) -> Option<anyhow::Error> {
        answer(ctx, sql).await.err().map(anyhow::Error::from)
    }

    /// Whether the query is refused with SQLSTATE 53200 (out of memory), found anywhere in its error's chain.
    async fn refused(ctx: &SessionContext, sql: &str) -> AnyResult<bool> {
        Ok(refusal(ctx, sql).await.is_some_and(|e| crate::codes::of(&e) == "53200"))
    }

    #[tokio::test]
    async fn a_window_over_every_row_is_refused_past_the_pool() -> AnyResult<()> {
        let ctx = small_session()?;
        assert!(refused(&ctx, "SELECT max(v + n) AS m FROM (SELECT value AS v, count(*) OVER () AS n FROM generate_series(1, 2000000))").await?);
        assert!(refused(&ctx, "SELECT max(n) AS n FROM (SELECT count(*) OVER () AS n FROM generate_series(1, 2000000))").await?); // (it reads no column: the window's own column counts)
        Ok(())
    }

    #[tokio::test]
    async fn a_streaming_window_refused_past_the_pool_is_not_retried_frugally() -> AnyResult<()> {
        let ctx = small_session()?;
        // Every row is a peer of `c`, so the window holds them all until its input ends: a streaming window, which a frugal
        // run would not help. Its refusal must not start that retry (`query::exhausted`); a window over whole partitions must.
        let streaming = "SELECT max(s) AS m FROM (SELECT sum(id) OVER (ORDER BY c) AS s FROM (SELECT value AS id, 'a' AS c FROM generate_series(1, 2000000)))";
        assert!(planned(&ctx, streaming).await?.contains("BoundedWindowAggExec"), "not a streaming window");
        let e = refusal(&ctx, streaming).await.expect("refused past the pool");
        assert!(!crate::query::exhausted(&e), "{e:#}");
        assert_eq!(crate::codes::of(&e), "53200", "{e:#}");
        let whole = refusal(&ctx, "SELECT max(n) AS n FROM (SELECT count(*) OVER () AS n FROM generate_series(1, 2000000))").await.expect("refused past the pool");
        assert!(crate::query::exhausted(&whole), "{whole:#}");
        Ok(())
    }

    #[tokio::test]
    async fn a_window_that_lets_rows_go_answers_past_the_pool() -> AnyResult<()> {
        let ctx = small_session()?;
        assert_eq!(number(&ctx, "SELECT max(r) AS r FROM (SELECT row_number() OVER (ORDER BY value DESC) AS r FROM generate_series(1, 2000000))").await?, 2000000); // (the sort spills; the window streams)
        assert_eq!(number(&ctx, "SELECT max(r + length(p)) AS m FROM (SELECT row_number() OVER () AS r, p FROM (SELECT CASE WHEN value > 1000000 THEN repeat('x', 100) ELSE 'x' END AS p FROM generate_series(1, 2000000)))").await?, 2000100); // (rows widen as they stream: each batch released as it goes out)
        Ok(())
    }

    #[tokio::test]
    async fn explain_puts_the_counting_pair_around_the_window() -> AnyResult<()> {
        let ctx = small_session()?;
        let batches = answer(&ctx, "EXPLAIN SELECT max(v + n) AS m FROM (SELECT value AS v, count(*) OVER () AS n FROM generate_series(1, 2000000))").await?;
        let plan = batches.iter().flat_map(|b| b.column(1).as_string::<i32>().iter().flatten().map(str::to_string)).collect::<Vec<_>>().join("\n");
        let out = plan.find("HeldOutExec").expect("HeldOutExec in the plan");
        let window = plan.find("WindowAggExec").expect("a window in the plan");
        let inside = plan.find("HeldInExec").expect("HeldInExec in the plan");
        assert!(out < window && window < inside, "{plan}");
        Ok(())
    }

    #[test]
    fn a_batch_of_strings_counts_their_buffers() -> AnyResult<()> {
        let strings: Vec<String> = (0..1000).map(|i| format!("{i:0>50}")).collect();
        let array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])?;
        assert!(bytes(&batch, 0) >= 50_000, "{}", bytes(&batch, 0));
        Ok(())
    }

    #[test]
    fn the_window_rule_is_the_last_physical_rule() {
        assert_eq!(crate::optimize::physical_rules().last().unwrap().name(), "held_windows");
    }

    /// DataFusion's own physical rules, plus `LeanSorts` when `lean`, and `Windows` last as `physical_rules` ends
    /// with it. No small pool: these tests are about what a sort hands up, not about refusals.
    fn session(partitions: usize, lean: bool) -> SessionContext {
        let mut rules = PhysicalOptimizer::new().rules;
        if lean {
            rules.push(Arc::new(LeanSorts));
        }
        rules.push(Arc::new(Windows));
        let config = SessionConfig::new().with_target_partitions(partitions).with_batch_size(1024);
        SessionContext::new_with_state(SessionStateBuilder::new().with_config(config).with_default_features().with_physical_optimizer_rules(rules).build())
    }

    /// `n` strings of 20 to 40 letters from a fixed seed, so every run sorts the same ones.
    fn letters(n: usize) -> Vec<String> {
        let mut x: u64 = 0x2545_F491_4F6C_DD1D; // (xorshift: the seed)
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        (0..n)
            .map(|_| {
                let len = 20 + (next() % 21) as usize;
                (0..len).map(|_| char::from(b'a' + (next() % 26) as u8)).collect()
            })
            .collect()
    }

    /// A table of `n` rows (`id`, `k`), `k` a string column of type `kind`, in batches of 1,000 dealt over `partitions`.
    fn table(partitions: usize, n: usize, kind: DataType) -> AnyResult<MemTable> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("k", kind.clone(), false)]));
        let mut parts = vec![vec![]; partitions];
        for (b, strings) in letters(n).chunks(1000).enumerate() {
            let ids = Int64Array::from_iter_values((b * 1000) as i64..(b * 1000 + strings.len()) as i64);
            let k = cast(&StringViewArray::from(strings.to_vec()), &kind)?;
            parts[b % partitions].push(RecordBatch::try_new(schema.clone(), vec![Arc::new(ids) as ArrayRef, k])?);
        }
        Ok(MemTable::try_new(schema, parts)?)
    }

    /// The (string, id) rows of an answer to `SELECT k, id`, in order.
    fn rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
        batches
            .iter()
            .flat_map(|b| {
                let (k, id) = (b.column(0).as_string_view(), b.column(1).as_primitive::<Int64Type>());
                (0..b.num_rows()).map(|i| (k.value(i).to_string(), id.value(i))).collect::<Vec<_>>()
            })
            .collect()
    }

    /// Whether a batch's string buffers hold no more than its views use, as `compact` judges (twice, plus 8 KB).
    fn lean_batch(b: &RecordBatch) -> bool {
        let k = b.column(0).as_string_view(); // (`SELECT k, id`: k first)
        let held: usize = k.data_buffers().iter().map(|buf| buf.len()).sum();
        held <= 2 * k.total_buffer_bytes_used() + (8 << 10)
    }

    /// The physical plan of `sql`, as indented text.
    async fn planned(ctx: &SessionContext, sql: &str) -> AnyResult<String> {
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let text = displayable(plan.as_ref()).indent(true).to_string();
        Ok(text)
    }

    #[tokio::test]
    async fn a_sort_of_strings_gets_a_lean_exec_above_it() -> AnyResult<()> {
        let ctx = session(1, true);
        ctx.register_table("t", Arc::new(table(1, 2000, DataType::Utf8View)?))?;
        ctx.register_table("u", Arc::new(table(1, 2000, DataType::Utf8)?))?;
        let sorted = planned(&ctx, "SELECT k FROM t ORDER BY k").await?;
        assert!(sorted.find("LeanExec").is_some_and(|l| sorted.find("SortExec").is_some_and(|s| l < s)), "{sorted}");
        let plain = planned(&ctx, "SELECT k FROM u ORDER BY k").await?;
        assert!(!plain.contains("LeanExec"), "{plain}"); // (strings as Utf8 hold no views)
        let top = planned(&ctx, "SELECT k FROM t ORDER BY k LIMIT 5").await?;
        assert!(!top.contains("LeanExec"), "{top}"); // (a top-N keeps few rows)
        Ok(())
    }

    #[tokio::test]
    async fn a_second_run_wraps_nothing_again() -> AnyResult<()> {
        let ctx = session(4, true);
        ctx.register_table("t", Arc::new(table(4, 20_000, DataType::Utf8View)?))?;
        let plan = ctx.sql("SELECT k FROM t ORDER BY k").await?.create_physical_plan().await?;
        let twice = LeanSorts.optimize(plan, &ConfigOptions::new())?;
        let text = displayable(twice.as_ref()).indent(true).to_string();
        assert_eq!(text.matches("LeanExec").count(), 1, "{text}");
        Ok(())
    }

    #[tokio::test]
    async fn a_sort_of_strings_answers_the_same_with_or_without_it() -> AnyResult<()> {
        let (lean, plain) = (session(4, true), session(4, false));
        for ctx in [&lean, &plain] {
            ctx.register_table("t", Arc::new(table(4, 20_000, DataType::Utf8View)?))?;
        }
        for (sql, descending) in [("SELECT k, id FROM t ORDER BY k", false), ("SELECT k, id FROM t ORDER BY k DESC, id", true)] {
            let (a, b) = (rows(&answer(&lean, sql).await?), rows(&answer(&plain, sql).await?));
            assert_eq!(a.len(), 20_000, "{sql}");
            assert_eq!(a, b, "{sql}");
            let in_order = a.windows(2).all(|w| if descending { w[0].0 >= w[1].0 } else { w[0].0 <= w[1].0 });
            assert!(in_order, "{sql}: sorted by k");
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_sort_on_one_partition_hands_up_its_own_strings() -> AnyResult<()> {
        let sql = "SELECT k, id FROM t ORDER BY k";
        let lean = session(1, true);
        lean.register_table("t", Arc::new(table(1, 20_000, DataType::Utf8View)?))?;
        let batches = answer(&lean, sql).await?;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 20_000);
        assert!(batches.iter().all(lean_batch), "a batch points into other batches' strings");
        let plain = session(1, false); // (without the rule, the test bites: some batch points into many)
        plain.register_table("t", Arc::new(table(1, 20_000, DataType::Utf8View)?))?;
        assert!(answer(&plain, sql).await?.iter().any(|b| !lean_batch(b)), "the test would not catch the buffers");
        Ok(())
    }

    /// `session` without the window rule: the answers the rule must give back.
    fn session_plain(partitions: usize) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(partitions).with_batch_size(1024);
        SessionContext::new_with_state(SessionStateBuilder::new().with_config(config).with_default_features().build())
    }

    /// A table of `n` rows in batches of 1,000 dealt over `partitions`: `id`, `k` (`id % 3`), and two string views of
    /// random letters, `s` and `v` (`v` the reverse of `s`), 20 to 40 letters each, so each has buffers of its own.
    fn wide(partitions: usize, n: usize) -> AnyResult<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
            Field::new("v", DataType::Utf8View, false),
        ]));
        let s = letters(n);
        let v: Vec<String> = s.iter().rev().cloned().collect();
        let mut parts = vec![vec![]; partitions];
        for (b, (ss, vv)) in s.chunks(1000).zip(v.chunks(1000)).enumerate() {
            let start = (b * 1000) as i64;
            let end = start + ss.len() as i64;
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(start..end)),
                Arc::new(Int64Array::from_iter_values((start..end).map(|i| i % 3))),
                Arc::new(StringViewArray::from(ss.to_vec())),
                Arc::new(StringViewArray::from(vv.to_vec())),
            ];
            parts[b % partitions].push(RecordBatch::try_new(schema.clone(), columns)?);
        }
        Ok(MemTable::try_new(schema, parts)?)
    }

    /// The (id, number) rows of an answer whose first column is `id` and last a number, in order.
    fn pairs(batches: &[RecordBatch]) -> AnyResult<Vec<(i64, i64)>> {
        let mut out = vec![];
        for b in batches {
            let id = b.column(0).as_primitive::<Int64Type>();
            let n = cast(b.column(b.num_columns() - 1), &DataType::Int64)?;
            let n = n.as_primitive::<Int64Type>();
            out.extend((0..b.num_rows()).map(|i| (id.value(i), n.value(i))));
        }
        Ok(out)
    }

    #[tokio::test]
    async fn a_streaming_window_gets_plain_strings_and_its_views_back() -> AnyResult<()> {
        let ctx = session(1, false);
        ctx.register_table("t", Arc::new(wide(1, 2000)?))?;
        let plan = planned(&ctx, "SELECT id, k, s, rank() OVER (PARTITION BY k ORDER BY s) AS r FROM t").await?;
        let below = plan.find("CAST(s@2 AS Utf8)").expect(&plan);
        let window = plan.find("BoundedWindowAggExec").expect(&plan);
        let back = plan.find("CAST(s@2 AS Utf8View)").expect(&plan);
        assert!(back < window && window < below, "{plan}"); // (the cast back above the window, the plain cast below it)
        assert!(plan.contains("mode=[Sorted]"), "{plan}"); // (no sort added: the key's ordering survives the projection)
        Ok(())
    }

    #[tokio::test]
    async fn a_window_reading_a_view_argument_keeps_its_views() -> AnyResult<()> {
        let ctx = session(1, false);
        ctx.register_table("t", Arc::new(wide(1, 2000)?))?;
        let plan = planned(&ctx, "SELECT id, first_value(s) OVER (PARTITION BY k ORDER BY s) AS f FROM t").await?;
        assert!(!plan.contains("CAST("), "{plan}");
        Ok(())
    }

    #[tokio::test]
    async fn a_view_key_stays_a_view_and_its_window_stays_sorted() -> AnyResult<()> {
        let sql = "SELECT id, v, rank() OVER (PARTITION BY s ORDER BY k) AS r FROM t ORDER BY id";
        let (on, off) = (session(1, false), session_plain(1));
        for ctx in [&on, &off] {
            ctx.register_table("t", Arc::new(wide(1, 4000)?))?;
        }
        let plan = planned(&on, sql).await?;
        assert!(plan.contains("CAST(v@") && !plan.contains("CAST(s@"), "{plan}"); // (`v` is cast, the key `s` is not)
        assert!(plan.contains("mode=[Sorted]"), "{plan}");
        assert_eq!(pairs(&answer(&on, sql).await?)?, pairs(&answer(&off, sql).await?)?);
        Ok(())
    }

    #[tokio::test]
    async fn stacked_windows_keep_the_keys_the_lower_one_orders() -> AnyResult<()> {
        // (the upper window partitions by `s`, which the lower one orders: `s` must stay a view, or the upper one fails)
        let sql = "SELECT id, count(*) OVER (PARTITION BY k, s) AS c FROM (SELECT id, k, s, rank() OVER (PARTITION BY k ORDER BY s) AS r FROM t) ORDER BY id";
        let (on, off) = (session(1, false), session_plain(1));
        for ctx in [&on, &off] {
            ctx.register_table("t", Arc::new(wide(1, 4000)?))?;
        }
        assert_eq!(pairs(&answer(&on, sql).await?)?, pairs(&answer(&off, sql).await?)?);
        Ok(())
    }

    #[tokio::test]
    async fn a_streaming_window_answers_as_it_did_without_the_rule() -> AnyResult<()> {
        let sql = "SELECT id, rank() OVER (PARTITION BY k ORDER BY s) AS r FROM t ORDER BY id";
        let (on, off) = (session(4, false), session_plain(4));
        for ctx in [&on, &off] {
            ctx.register_table("t", Arc::new(wide(4, 20_000)?))?; // (20 batches of 1,000 rows)
        }
        let (a, b) = (pairs(&answer(&on, sql).await?)?, pairs(&answer(&off, sql).await?)?);
        assert_eq!(a.len(), 20_000);
        assert_eq!(a, b);
        Ok(())
    }

    #[tokio::test]
    async fn a_second_run_wraps_a_window_once() -> AnyResult<()> {
        let ctx = session(1, false);
        ctx.register_table("t", Arc::new(wide(1, 2000)?))?;
        let plan = ctx.sql("SELECT id, rank() OVER (PARTITION BY k ORDER BY s) AS r FROM t").await?.create_physical_plan().await?;
        let twice = Windows.optimize(plan, &ConfigOptions::new())?;
        let text = displayable(twice.as_ref()).indent(true).to_string();
        assert_eq!(text.matches("HeldInExec").count(), 1, "{text}");
        assert_eq!(text.matches("HeldOutExec").count(), 1, "{text}");
        assert_eq!(text.matches("CAST(s@2 AS Utf8)").count(), 1, "{text}");
        Ok(())
    }
}
