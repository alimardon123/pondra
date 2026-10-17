//! What a window holds counts in the query memory (invariant 414). DataFusion's windows keep the rows they still need
//! with no memory reservation, so `count(*) OVER ()` over 60 M rows grew a node until the OS killed it. Each window sits
//! between `HeldInExec` (counts each batch going in) and `HeldOutExec` (lets go of each batch as its rows come out), which
//! share one reservation per partition. Still uncounted: what DataFusion keeps inside a window beyond those rows (the
//! strings a long partition's views point into, a frame's rows behind those given out, `WindowAggExec`'s concatenation).
//! `LeanExec` is the other operator here: a sort's output of strings holds only its own rows' strings (`LeanSorts`).
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
use datafusion::physical_plan::sorts::sort::SortExec;
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
                DataFusionError::ResourcesExhausted(message)
            })?; // ("Resources exhausted" starts the frugal retry (`query::frugally`), where a window over whole partitions is answered as a GROUP BY; the words give 53200 on every door)
            h.batches.push_back((batch.num_rows(), b));
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// Hands up each batch with only its own rows' strings (`query::compact`). A sort reads a string column as views into
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
        let out = s.map(|item| item.map(crate::query::compact));
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

    /// Whether the query is refused with SQLSTATE 53200 (out of memory), found anywhere in its error's chain.
    async fn refused(ctx: &SessionContext, sql: &str) -> AnyResult<bool> {
        match answer(ctx, sql).await {
            Ok(_) => Ok(false),
            Err(e) => Ok(crate::codes::of(&anyhow::Error::from(e)) == "53200"),
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
}
