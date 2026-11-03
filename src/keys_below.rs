//! A hash join's keys cut the rows that an aggregate on its probe side groups. TPC-H q20 grouped 909,000 lineitems into
//! 543,000 groups (117 ms of its 150 ms of aggregating) for a semi join that kept the 5,830 whose keys its 8,510 rows
//! have. The join's dynamic filter (DataFusion's: its build side's key ranges and hash table) can pass down through the
//! aggregate, since a filter on grouping columns only drops whole groups. But below it, nothing took the filter: a scan
//! of hot columns skips batches by their ranges and takes no row filter (`hot::HotSource`), so the join never built it.
//! `KeysExec`, put between such an aggregate and its input, takes the dynamic filters that come down to it and that
//! nothing below filters rows by, and filters its input's rows by them; they still go on down, for a Parquet scan to
//! skip row groups by. In-process at SF1, q20 then groups 97,000 rows' groups instead of 543,000: 71 ms became 37 ms
//! (DuckDB 2.0: 31 ms), and no other TPC-H plan changes; TPC-DS q59 128 ms became 90 ms
//! (`logs/round36/keys-below-2026-11-02.txt`).
//!
//! The filter is DataFusion's, so it keeps what the join would: no row whose keys could match is dropped. Until the
//! join's build side is in, the filter is `true` and rows pass as they are: an aggregate that `optimize::EarlyProbes`
//! starts early meets the filter only once the build is done. A filter that keeps most rows costs its checks and saves
//! nothing, the more so where an aggregate groups few keys or a join below already dropped the rows. So each filter,
//! once it has checked `TRIAL` rows, stops checking if it kept more than `KEPT` of them; the others go on.
//!
//! `KeysBelow` puts one under each grouping aggregate reached from a hash join's probe side through operators that pass
//! rows as they are (projections, exchanges, filters), and runs just before DataFusion's `FilterPushdown(Post)`.
//! `KeysUnused`, just after it, takes away each `KeysExec` that no filter came down to, so other plans are as before.
//! Neither runs on a spread query's slices (as `EarlyProbes`): `KeysExec` is no operator their analysis knows.

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::DynamicFilterTracking;
use datafusion::physical_expr_common::physical_expr::snapshot_physical_expr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::buffer::BufferExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{replace_children_if_necessary, CardinalityEffect};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::filter_pushdown::{ChildFilterDescription, ChildFilterPushdownResult, ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::{ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties, StatisticsArgs};
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Rows each of a `KeysExec`'s filters checks before it is judged (`KEPT`).
const TRIAL: u64 = 1 << 15;

/// The share of the rows checked that, kept, makes a filter not worth its checks.
const KEPT: f64 = 0.5;

/// Filters its input's rows by the dynamic filters that come down to it (the module's doc).
#[derive(Debug)]
pub struct KeysExec {
    input: Arc<dyn ExecutionPlan>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    seen: Arc<[Tally]>, // (one a filter)
    metrics: ExecutionPlanMetricsSet,
}

/// The rows a filter of a `KeysExec` checked and kept, over its partitions, and whether it stopped checking.
#[derive(Debug, Default)]
struct Tally {
    checked: AtomicU64,
    kept: AtomicU64,
    off: AtomicBool,
}

impl KeysExec {
    fn new(input: Arc<dyn ExecutionPlan>, filters: Vec<Arc<dyn PhysicalExpr>>) -> KeysExec {
        let seen = filters.iter().map(|_| Tally::default()).collect();
        KeysExec { input, filters, seen, metrics: ExecutionPlanMetricsSet::new() }
    }
}

impl DisplayAs for KeysExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let filters: Vec<String> = self.filters.iter().map(|e| e.to_string()).collect();
        write!(f, "KeysExec: {}", filters.join(" AND "))
    }
}

impl ExecutionPlan for KeysExec {
    fn name(&self) -> &str {
        "KeysExec"
    }

    // (the input's partitions and order: rows are only dropped)
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    // (shown, so the join that makes a filter here builds it: DataFusion looks for it in the plan below the join)
    fn apply_expressions(&self, f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        for e in &self.filters {
            if f(e)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|c: Vec<_>| DataFusionError::Internal(format!("KeysExec has one input, not {}", c.len())))?;
        Ok(Arc::new(KeysExec::new(input, self.filters.clone())))
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

    // (the planner sized the plan before the filters came: what it was sized as stands)
    fn statistics_from_inputs(&self, input_stats: &[Arc<Statistics>], _: &StatisticsArgs) -> Result<Arc<Statistics>> {
        Ok(input_stats[0].clone())
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    // (they go on down too: a Parquet scan below skips row groups by them, or filters its rows by them itself)
    fn gather_filters_for_pushdown(&self, _: FilterPushdownPhase, parent_filters: Vec<Arc<dyn PhysicalExpr>>, _: &ConfigOptions) -> Result<FilterDescription> {
        Ok(FilterDescription::new().with_child(ChildFilterDescription::from_child(&parent_filters, &self.input)?))
    }

    // (takes the dynamic ones that nothing below filters rows by; any other stays with the operator above, which
    // already evaluates it)
    fn handle_child_pushdown_result(&self, phase: FilterPushdownPhase, child: ChildPushdownResult, _: &ConfigOptions) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if phase != FilterPushdownPhase::Post {
            return Ok(FilterPushdownPropagation::if_all(child));
        }
        let mine = |f: &&ChildFilterPushdownResult| matches!(f.all(), PushedDown::No) && DynamicFilterTracking::classify(&f.filter).contains_dynamic_filter();
        let taken: Vec<PushedDown> = child.parent_filters.iter().map(|f| if mine(&f) { PushedDown::Yes } else { f.all() }).collect();
        let new: Vec<Arc<dyn PhysicalExpr>> = child.parent_filters.iter().filter(mine).map(|f| f.filter.clone()).collect();
        if new.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(taken));
        }
        let filters = self.filters.iter().cloned().chain(new).collect();
        let node: Arc<dyn ExecutionPlan> = Arc::new(KeysExec::new(self.input.clone(), filters));
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(taken).with_updated_node(node))
    }

    fn execute(&self, p: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let s = self.input.execute(p, ctx)?;
        let schema = s.schema();
        let (filters, seen, metrics) = (self.filters.clone(), self.seen.clone(), BaselineMetrics::new(&self.metrics, p));
        let out = s.map(move |batch| {
            let _time = metrics.elapsed_compute().timer();
            batch.and_then(|b| keep(b, &filters, &seen)).map(|b| b.record_output(&metrics))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }

    // (EXPLAIN ANALYZE: the rows kept)
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// `batch`'s rows that `filters` keep as they stand now (a filter whose build isn't in yet is `true`), each filter
/// with its tally in `seen`.
fn keep(batch: RecordBatch, filters: &[Arc<dyn PhysicalExpr>], seen: &[Tally]) -> Result<RecordBatch> {
    let mut batch = batch;
    for (f, seen) in filters.iter().zip(seen) {
        if batch.num_rows() == 0 {
            break;
        }
        if seen.off.load(Ordering::Relaxed) {
            continue;
        }
        let now = snapshot_physical_expr(f.clone())?;
        if now.downcast_ref::<Literal>().is_some_and(|l| l.value() == &ScalarValue::Boolean(Some(true))) {
            continue; // (not built yet, or keeps every row)
        }
        let before = batch.num_rows() as u64;
        let mask = match now.evaluate(&batch)? {
            ColumnarValue::Array(a) => a.as_boolean().clone(),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => continue,
            ColumnarValue::Scalar(_) => datafusion::arrow::array::BooleanArray::from(vec![false; batch.num_rows()]),
        };
        batch = filter_record_batch(&batch, &mask)?;
        let checked = seen.checked.fetch_add(before, Ordering::Relaxed) + before;
        let kept = seen.kept.fetch_add(batch.num_rows() as u64, Ordering::Relaxed) + batch.num_rows() as u64;
        if checked >= TRIAL && kept as f64 > KEPT * checked as f64 {
            seen.off.store(true, Ordering::Relaxed); // (keeps most rows: not worth checking)
        }
    }
    Ok(batch)
}

/// Puts a `KeysExec` under each grouping aggregate on a hash join's probe side (the module's doc).
#[derive(Debug)]
pub struct KeysBelow;

impl PhysicalOptimizerRule for KeysBelow {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.enable_join_dynamic_filter_pushdown || crate::optimize::spread(&plan)? {
            return Ok(plan);
        }
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<HashJoinExec>() else { return Ok(Transformed::no(p)) };
            let (build, probe) = (j.left().clone(), j.right().clone()); // (DataFusion's probe side is the right)
            match under_aggregate(&probe)? {
                Some(probe) => Ok(Transformed::yes(replace_children_if_necessary(p, vec![build, probe])?)),
                None => Ok(Transformed::no(p)),
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "keys_below"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `plan` with a `KeysExec` under the first grouping aggregate reached through operators that pass rows as they
/// are; None when there is none.
fn under_aggregate(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(a) = plan.downcast_ref::<AggregateExec>() {
        if a.group_expr().expr().is_empty() || a.input().is::<KeysExec>() {
            return Ok(None);
        }
        match a.mode() {
            AggregateMode::Final | AggregateMode::FinalPartitioned => return under_partial(plan, a.input()),
            AggregateMode::Single | AggregateMode::SinglePartitioned => {}
            _ => return Ok(None),
        }
        let keys: Arc<dyn ExecutionPlan> = Arc::new(KeysExec::new(a.input().clone(), vec![]));
        return Ok(Some(replace_children_if_necessary(plan.clone(), vec![keys])?));
    }
    let passes = plan.is::<ProjectionExec>() || plan.is::<BufferExec>() || plan.is::<CoalescePartitionsExec>() || plan.is::<RepartitionExec>() || plan.is::<FilterExec>();
    if !passes {
        return Ok(None);
    }
    let [child] = plan.children()[..] else { return Ok(None) };
    Ok(match under_aggregate(child)? {
        Some(child) => Some(replace_children_if_necessary(plan.clone(), vec![child])?),
        None => None,
    })
}

/// A final aggregate `plan` with a `KeysExec` under its partial step, which `input` reaches through exchanges.
fn under_partial(plan: &Arc<dyn ExecutionPlan>, input: &Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let below = if let Some(a) = input.downcast_ref::<AggregateExec>() {
        if a.mode() != &AggregateMode::Partial || a.input().is::<KeysExec>() {
            return Ok(None);
        }
        let keys: Arc<dyn ExecutionPlan> = Arc::new(KeysExec::new(a.input().clone(), vec![]));
        replace_children_if_necessary(input.clone(), vec![keys])?
    } else if input.is::<RepartitionExec>() || input.is::<CoalescePartitionsExec>() {
        let [child] = input.children()[..] else { return Ok(None) };
        match under_partial(input, child)? {
            Some(child) => child,
            None => return Ok(None),
        }
    } else {
        return Ok(None);
    };
    Ok(Some(replace_children_if_necessary(plan.clone(), vec![below])?))
}

/// Takes away each `KeysExec` that no filter came down to (the module's doc).
#[derive(Debug)]
pub struct KeysUnused;

impl PhysicalOptimizerRule for KeysUnused {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| match p.downcast_ref::<KeysExec>() {
            Some(k) if k.filters.is_empty() => Ok(Transformed::yes(k.input.clone())),
            _ => Ok(Transformed::no(p)),
        })
        .data()
    }

    fn name(&self) -> &str {
        "keys_unused"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// A session with Pondra's rules, as the engine has them, without the physical ones named.
    fn session(without: &[&str]) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(crate::optimize::config(SessionConfig::new().with_target_partitions(4)))
            .with_default_features()
            .with_analyzer_rules(crate::optimize::analyzer_rules())
            .with_optimizer_rules(crate::optimize::rules())
            .with_physical_optimizer_rules(crate::optimize::physical_rules().into_iter().filter(|r| !without.contains(&r.name())).collect())
            .build();
        SessionContext::new_with_state(state)
    }

    /// `few`: 3 keys of the 1,000 that `many`'s 200,000 rows have (and a value each); `many`: (k, v).
    fn tables(ctx: &SessionContext) {
        let two = |a: Vec<i64>, b: Vec<i64>| {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false), Field::new("v", DataType::Int64, false)]));
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(a)) as ArrayRef, Arc::new(Int64Array::from(b))]).unwrap()
        };
        ctx.register_batch("few", two(vec![7, 300, 999], vec![1, 2, 3])).unwrap();
        ctx.register_batch("many", two((0..200_000).map(|i| (i * 7919) % 1000).collect(), (0..200_000).collect())).unwrap();
    }

    async fn plan_and_rows(ctx: &SessionContext, sql: &str) -> (String, String) {
        let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await.unwrap();
        (text, datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string())
    }

    /// A join on an aggregate's grouping keys filters the rows the aggregate groups, by its build side's keys, with
    /// the same answer as without; a join on an aggregated value, which can't, leaves the plan as it was.
    #[tokio::test]
    async fn a_join_on_grouping_keys_cuts_the_rows_grouped() {
        let (ctx, plain) = (session(&[]), session(&["keys_below", "keys_unused"]));
        tables(&ctx);
        tables(&plain);
        let on_keys = "SELECT few.k, s.total FROM few JOIN (SELECT k, sum(v) AS total FROM many GROUP BY k) s ON few.k = s.k ORDER BY few.k";
        let (text, rows) = plan_and_rows(&ctx, on_keys).await;
        let (plain_text, plain_rows) = plan_and_rows(&plain, on_keys).await;
        assert!(text.contains("KeysExec: DynamicFilter"), "{text}");
        assert!(!plain_text.contains("KeysExec"), "{plain_text}");
        assert_eq!(rows, plain_rows);
        assert_eq!(rows.lines().count(), 3 + 4, "three keys and the table's frame: {rows}");
        let semi = "SELECT k FROM few WHERE k IN (SELECT k FROM many GROUP BY k HAVING sum(v) > 0) ORDER BY k";
        assert_eq!(plan_and_rows(&ctx, semi).await.1, plan_and_rows(&plain, semi).await.1);
        let on_value = "SELECT few.k, s.k FROM few JOIN (SELECT k, sum(v) AS total FROM many GROUP BY k) s ON few.v = s.total ORDER BY 1";
        let ((text, rows), (_, plain_rows)) = (plan_and_rows(&ctx, on_value).await, plan_and_rows(&plain, on_value).await);
        assert!(!text.contains("KeysExec"), "no filter comes down through an aggregated value: {text}");
        assert_eq!(rows, plain_rows);
    }

    /// Rows pass as they are while a filter isn't built (`true`), and a filter whose checks keep most rows stops, while
    /// one that drops most goes on.
    #[test]
    fn an_unbuilt_or_unselective_filter_keeps_every_row() {
        use datafusion::physical_expr::expressions::{col, lit, BinaryExpr};
        use datafusion::logical_expr::Operator;
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>())) as ArrayRef]).unwrap();
        let one = [Tally::default()];
        assert_eq!(keep(batch.clone(), &[lit(true)], &one).unwrap().num_rows(), 1000);
        assert_eq!(one[0].checked.load(Ordering::Relaxed), 0, "an unbuilt filter checks nothing");
        let cmp = |op, v: i64| -> Arc<dyn PhysicalExpr> { Arc::new(BinaryExpr::new(col("k", &schema).unwrap(), op, lit(v))) };
        let (most, tenth) = (cmp(Operator::GtEq, 10), cmp(Operator::Lt, 100));
        let filters = [most, tenth];
        let two = [Tally::default(), Tally::default()];
        assert_eq!(keep(batch.clone(), &filters, &two).unwrap().num_rows(), 90);
        while !two[0].off.load(Ordering::Relaxed) {
            keep(batch.clone(), &filters, &two).unwrap();
        }
        assert!(two[0].checked.load(Ordering::Relaxed) >= TRIAL);
        assert!(!two[1].off.load(Ordering::Relaxed), "the filter that drops most rows goes on");
        assert_eq!(keep(batch.clone(), &filters, &two).unwrap().num_rows(), 100, "the first is off: its rows pass unchecked");
        assert_eq!(keep(batch, &filters[..1], &two[..1]).unwrap().num_rows(), 1000);
    }
}
