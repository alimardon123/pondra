//! A view's plan over a flush's new rows, made once (`run`). Every flush runs each inline view over
//! the rows it brings, and planning that query was most of what a view cost (a GROUP BY of a
//! flush's 4,000 rows: 0.9 ms planning, 0.4 ms running). So a view that reads nothing but those
//! rows keeps its physical plan, and each flush puts its rows in it (invariant 200).
use crate::store::Lake;
use anyhow::Result;
use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::{MemTable, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, Volatility};
use datafusion::physical_plan::{execution_plan::*, memory::MemoryStream, ExecutionPlan, Partitioning, PlanProperties, PhysicalExpr};
use datafusion::prelude::SessionContext;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type Kept = (Arc<dyn ExecutionPlan>, Arc<TaskContext>);
static PLANS: Mutex<BTreeMap<String, Kept>> = Mutex::new(BTreeMap::new());

/// `sql` with table `source` standing for just `rows` (as columns `s`), `prepare` given its logical
/// plan first (a row-by-row view carries its ids through it; `kind` names what it does). Another
/// table it reads, or a function that may answer otherwise next time (`now()`), plans it afresh.
pub async fn run(lake: &Lake, source: &str, s: SchemaRef, rows: &[RecordBatch], sql: &str, kind: &str, prepare: impl FnOnce(LogicalPlan) -> Result<LogicalPlan>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let rows = rows.iter().map(|b| crate::query::conform(b, &s)).collect::<Result<Vec<_>>>()?;
    let parts = if rows.iter().map(|b| b.num_rows()).sum::<usize>() < 1 << 17 { 1 } else { crate::store::partitions() }; // (a flush's rows: one; a view's filling: every core)
    let functions = lake.cat.scan_raw("f/", "f0").await?; // (a function replaced is planned again)
    let key = format!("{}\0{kind}\0{sql}\0{:?}\0{parts}\0{:x}", lake.url, s.fields(), fingerprint(&functions));
    let kept = PLANS.lock().unwrap().get(&key).cloned();
    if let Some((plan, task)) = kept {
        let plan = renew(&plan, &rows)?;
        return Ok((plan.schema(), datafusion::physical_plan::collect(plan, task).await?));
    }
    let ctx = crate::query::session(lake, sql, source).await?;
    let alone = registered_none(&ctx);
    let table: Arc<dyn TableProvider> = match alone {
        true => {
            let state = ctx.state_ref();
            let mut state = state.write();
            state.config_mut().options_mut().execution.target_partitions = parts;
            Arc::new(Fresh { schema: s.clone(), rows: rows.clone() })
        }
        false => Arc::new(MemTable::try_new(s.clone(), vec![rows])?),
    };
    ctx.register_table(crate::query::table_ref(source), table)?;
    let plan = prepare(ctx.sql(sql).await?.into_unoptimized_plan())?;
    let physical = ctx.state().create_physical_plan(&plan).await?;
    let task = ctx.task_ctx();
    if alone && steady(&plan) {
        let mut plans = PLANS.lock().unwrap();
        if plans.len() >= 512 {
            plans.clear(); // (views dropped and fillings leave theirs: a bound, not a policy)
        }
        plans.insert(key, (renew(&physical, &[])?, task.clone())); // (holding none of these rows)
    }
    Ok((physical.schema(), datafusion::physical_plan::collect(physical, task).await?))
}

fn fingerprint(raw: &BTreeMap<String, bytes::Bytes>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    raw.hash(&mut h);
    h.finish()
}

/// The session registered no table of the lake, file or attached lake: the SQL reads the new rows
/// alone (a word that only looks like a table's name plans it afresh every time, which is merely
/// slower).
fn registered_none(ctx: &SessionContext) -> bool {
    ctx.catalog_names().iter().filter_map(|c| ctx.catalog(c)).all(|c| {
        c.schema_names().iter().filter(|s| *s != "information_schema").filter_map(|s| c.schema(s)).all(|s| s.table_names().is_empty())
    })
}

/// Every function in it gives the same answer for the same rows: `now()` is folded into the plan
/// as it is made, and a kept plan would keep that time.
fn steady(plan: &LogicalPlan) -> bool {
    let changing = |e: &Expr| Ok(matches!(e, Expr::ScalarFunction(f) if f.func.signature().volatility != Volatility::Immutable));
    let mut ok = true;
    let walked = plan.apply_with_subqueries(|p| {
        p.apply_expressions(|e| {
            ok &= !e.exists(changing)?;
            Ok(TreeNodeRecursion::Continue)
        })
    });
    ok && walked.is_ok()
}

/// `plan` over `rows`: its leaf holding them, every other operator's state reset, as DataFusion
/// runs a recursive query's plan again (`reset_plan_states`).
fn renew(plan: &Arc<dyn ExecutionPlan>, rows: &[RecordBatch]) -> Result<Arc<dyn ExecutionPlan>> {
    let renewed = plan.clone().transform_up(|p| {
        Ok(Transformed::yes(match p.downcast_ref::<FreshExec>() {
            Some(f) => Arc::new(FreshExec { rows: rows.to_vec(), ..f.clone() }),
            None => p.reset_state()?,
        }))
    })?;
    Ok(renewed.data)
}

/// The new rows, as a table that reports no statistics: a plan kept for the next flush must not
/// have been made from these rows' count (DataFusion answers `count(*)` from exact statistics).
#[derive(Debug)]
struct Fresh {
    schema: SchemaRef,
    rows: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl TableProvider for Fresh {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Temporary }
    async fn scan(&self, _: &dyn Session, projection: Option<&Vec<usize>>, _: &[Expr], _: Option<usize>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let all: Vec<usize> = (0..self.schema.fields().len()).collect();
        let projected = Arc::new(self.schema.project(projection.unwrap_or(&all))?);
        let props = PlanProperties::new(datafusion::physical_expr::EquivalenceProperties::new(projected), Partitioning::UnknownPartitioning(1), EmissionType::Incremental, Boundedness::Bounded);
        Ok(Arc::new(FreshExec { schema: self.schema.clone(), projection: projection.cloned(), rows: self.rows.clone(), props: Arc::new(props) }))
    }
}

#[derive(Debug, Clone)]
struct FreshExec {
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    rows: Vec<RecordBatch>,
    props: Arc<PlanProperties>,
}

impl datafusion::physical_plan::DisplayAs for FreshExec {
    fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "FreshExec") }
}

impl ExecutionPlan for FreshExec {
    fn name(&self) -> &str { "FreshExec" }
    fn properties(&self) -> &Arc<PlanProperties> { &self.props }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>) -> datafusion::error::Result<TreeNodeRecursion> { Ok(TreeNodeRecursion::Continue) }
    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> { Ok(self) }
    fn execute(&self, _: usize, _: Arc<TaskContext>) -> datafusion::error::Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(self.rows.clone(), self.schema.clone(), self.projection.clone())?))
    }
}
