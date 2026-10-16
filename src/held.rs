//! What a window holds counts in the query memory (invariant 414). DataFusion's windows keep the rows they still need
//! with no memory reservation, so `count(*) OVER ()` over 60 M rows grew a node until the OS killed it. Each window sits
//! between `HeldInExec` (counts each batch going in) and `HeldOutExec` (lets go of each batch as its rows come out), which
//! share one reservation per partition. Still uncounted: what DataFusion keeps inside a window beyond those rows (the
//! strings a long partition's views point into, a frame's rows behind those given out, `WindowAggExec`'s concatenation).
use crate::store::memory_limit;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{replace_children_if_necessary, CardinalityEffect};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties, StatisticsArgs};
use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
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

/// The window's input side: counts each batch going into the window against the query memory.
#[derive(Debug)]
pub struct HeldInExec {
    input: Arc<dyn ExecutionPlan>,
    slots: Arc<Slots>,
    props: Arc<PlanProperties>,
    extra: usize,
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
        plan.transform_up(|node| {
            let window = node.is::<WindowAggExec>() || node.is::<BoundedWindowAggExec>();
            let counted = node.children().first().is_some_and(|c| c.is::<HeldInExec>()); // (the rule may meet a plan twice)
            if !window || node.children().len() != 1 || counted {
                return Ok(Transformed::no(node));
            }
            let child = node.children()[0].clone();
            let child_fields = child.schema().fields().len();
            let extra: usize = node.schema().fields()[child_fields..].iter().map(|f| f.data_type().primitive_width().unwrap_or(16)).sum(); // (the window's own columns, a value a row each)
            let slots = Arc::new(Slots::default());
            let kid: Arc<dyn ExecutionPlan> = Arc::new(HeldInExec { props: child.properties().clone(), input: child, slots: slots.clone(), extra });
            let w = replace_children_if_necessary(node, vec![kid])?; // (`with_new_children` is deprecated in DataFusion 55.1: the repo's own call, as in `optimize.rs`)
            Ok(Transformed::yes(Arc::new(HeldOutExec { props: w.properties().clone(), input: w, slots }) as Arc<dyn ExecutionPlan>))
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
        n => Err(DataFusionError::Internal(format!("a window's counting operator has one input, not {n}"))),
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
            reservation: MemoryConsumer::new(format!("Window[{p}]")).register(ctx.memory_pool()),
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
        Ok(Arc::new(HeldInExec { props: input.properties().clone(), input, slots: self.slots.clone(), extra: self.extra }))
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
        let extra = self.extra;
        let out = s.map(move |item| -> Result<RecordBatch> {
            let batch = item?;
            if batch.num_rows() == 0 {
                return Ok(batch);
            }
            let b = bytes(&batch, extra);
            let mut h = held.lock().unwrap();
            h.reservation.try_grow(b).map_err(|_| {
                let message = format!("out of memory: a window holds more rows than this node's memory for queries ({} MB): partition it by more columns, or give the node more memory (--memory-gb)", memory_limit() >> 20);
                DataFusionError::External(Box::new(crate::codes::Coded("53200", message)))
            })?; // (the words "out of memory" give 53200 across nodes too; they don't start the frugal retry, which keys on "Resources exhausted")
            h.batches.push_back((batch.num_rows(), b));
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result as AnyResult;
    use datafusion::arrow::array::StringViewArray;
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{Field, Int64Type, Schema};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
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

    /// Whether the query is refused with SQLSTATE 53200 (out of memory), found anywhere in its error's chain.
    async fn refused(ctx: &SessionContext, sql: &str) -> AnyResult<bool> {
        match answer(ctx, sql).await {
            Ok(_) => Ok(false),
            Err(e) => Ok(anyhow::Error::from(e).chain().any(|c| c.downcast_ref::<crate::codes::Coded>().is_some_and(|c| c.0 == "53200"))),
        }
    }

    #[tokio::test]
    async fn a_window_over_every_row_is_refused_past_the_pool() -> AnyResult<()> {
        let ctx = small_session()?;
        assert!(refused(&ctx, "SELECT max(v + n) AS m FROM (SELECT value AS v, count(*) OVER () AS n FROM generate_series(1, 2000000))").await?);
        assert!(refused(&ctx, "SELECT max(n) AS n FROM (SELECT count(*) OVER () AS n FROM generate_series(1, 2000000))").await?); // (it reads no column: the window's own column counts)
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
}
