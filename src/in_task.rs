//! A plan's partitions run in the task that asks for them, so a page can run a plan of more than one partition.
//!
//! DataFusion runs the partitions of a plan on tasks of their own, where it has more than one: the
//! `CoalescePartitionsExec` the planner puts under an aggregate or a sort, `collect` and `collect_partitioned` (a
//! table made from a query) at the root of a plan, and `ScalarSubqueryExec`, which runs each subquery the same way. A
//! page has no tokio runtime to start those tasks on, so each of them panics there, and takes every table of the page
//! with it. Here every partition runs in the task that asks, and all of them run at once, interleaved: a hash join's
//! probe partitions wait for each other at a barrier, so a partition run alone would never end. `InTask` is the
//! optimizer rule a page adds last (`planned`). A node keeps DataFusion's own, which has a runtime.
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_err, Result};
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::replace_children_if_necessary;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::stream::select_all;
use std::fmt;
use std::sync::Arc;

/// Every partition of `input`, run in this task: each is started up front, and they are polled together as one stream
/// of the input's schema. (`asof::lookup` polls its partitions the same way.)
pub fn stream(input: Arc<dyn ExecutionPlan>, task: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    let partitions = (0..input.output_partitioning().partition_count()).map(|p| input.execute(p, task.clone())).collect::<Result<Vec<_>>>()?;
    Ok(Box::pin(RecordBatchStreamAdapter::new(input.schema(), select_all(partitions))))
}

/// A `CoalescePartitionsExec` whose input runs in the task that executes it (`stream`), where the coalesce would start
/// a task per partition. It stands for the coalesce it replaces: the same input, fetch and properties, so the plan above
/// it sees the same schema, ordering and partitioning.
#[derive(Debug)]
pub struct InTaskExec {
    input: Arc<dyn ExecutionPlan>,
    fetch: Option<usize>,
    props: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl InTaskExec {
    /// The coalesce `coalesce` stands for, run in this task.
    fn replacing(coalesce: &CoalescePartitionsExec) -> InTaskExec {
        InTaskExec {
            input: coalesce.input().clone(),
            fetch: coalesce.fetch(),
            props: coalesce.properties().clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The partitions of `input` merged into one, in this task: what DataFusion would merge on tasks of its own.
    fn merging(input: Arc<dyn ExecutionPlan>) -> InTaskExec {
        InTaskExec::replacing(&CoalescePartitionsExec::new(input))
    }
}

impl DisplayAs for InTaskExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match self.fetch {
            Some(fetch) => write!(f, "InTaskExec: fetch={fetch}"),
            None => write!(f, "InTaskExec"),
        }
    }
}

impl ExecutionPlan for InTaskExec {
    fn name(&self) -> &str { "InTaskExec" }
    fn properties(&self) -> &Arc<PlanProperties> { &self.props }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn fetch(&self) -> Option<usize> { self.fetch }
    fn metrics(&self) -> Option<MetricsSet> { Some(self.metrics.clone_inner()) }

    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue) // (no expressions of its own, as the coalesce it stands for)
    }

    // (the coalesce's own rebuild: a new coalesce of the new child, with the same fetch, is what a child change means)
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else { return internal_err!("InTaskExec takes one child, not {}", children.len()) };
        Ok(Arc::new(InTaskExec::replacing(&CoalescePartitionsExec::new(input.clone()).with_fetch(self.fetch))))
    }

    fn execute(&self, partition: usize, task: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("InTaskExec has one partition, not {partition}");
        }
        // (counted, and cut at the fetch, as the coalesce counts and cuts its rows)
        Ok(Box::pin(LimitStream::new(stream(self.input.clone(), task)?, 0, self.fetch, BaselineMetrics::new(&self.metrics, partition))))
    }
}

/// The rule a page adds last (`planned`): every coalesce, every subquery and the root of more than one partition runs
/// in the task that asks for it (`stream`).
#[derive(Debug)]
pub struct InTask;

impl PhysicalOptimizerRule for InTask {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan.transform_up(in_task_node)?.data;
        // (the root is merged by `collect` and `collect_partitioned`, so it must have one partition by the time they run)
        Ok(match plan.output_partitioning().partition_count() > 1 {
            true => Arc::new(InTaskExec::merging(plan)),
            false => plan,
        })
    }

    fn name(&self) -> &str { "in_task" }
    fn schema_check(&self) -> bool { true }
}

/// One node of a plan: a coalesce runs in this task, and so does each subquery of a scalar subquery that has more than
/// one partition (the first child is the main input, which the plan above passes through as it is).
fn in_task_node(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return Ok(Transformed::yes(Arc::new(InTaskExec::replacing(coalesce))));
    }
    if plan.downcast_ref::<ScalarSubqueryExec>().is_none() {
        return Ok(Transformed::no(plan));
    }
    let children = plan
        .children()
        .into_iter()
        .enumerate()
        .map(|(i, child)| match i > 0 && child.output_partitioning().partition_count() > 1 {
            true => Arc::new(InTaskExec::merging(child.clone())) as Arc<dyn ExecutionPlan>,
            false => child.clone(),
        })
        .collect::<Vec<_>>();
    Ok(Transformed::yes(replace_children_if_necessary(plan, children)?))
}

/// `ctx`, with `InTask` added after its rules: every plan a page makes runs its partitions in the page's task.
pub fn planned(ctx: SessionContext) -> SessionContext {
    let state = SessionStateBuilder::new_from_existing(ctx.state()).with_physical_optimizer_rule(Arc::new(InTask)).build();
    SessionContext::new_with_state(state)
}
