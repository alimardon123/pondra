//! `EXCEPT ALL` and `INTERSECT ALL` as SQL means them: a row that is m times on the left and n times on the right is in
//! `EXCEPT ALL`'s answer m - n times (none when n ≥ m), and in `INTERSECT ALL`'s min(m, n) times, as Postgres and DuckDB
//! answer. DataFusion plans each as a join on every column that keeps the left's rows whole, so EXCEPT ALL dropped every
//! copy of a row the right holds at all, and INTERSECT ALL kept all m (apache/datafusion#12956): a silent wrong answer.
//!
//! `SetsAll`, an analyzer rule, numbers each side's copies of a row (`row_number() OVER (PARTITION BY` every column`)`) and
//! joins on the number too. The left's k-th copy then has a match only when the right has a k-th, so EXCEPT ALL keeps the
//! copies past the right's count and INTERSECT ALL the copies up to it. Which copy gets which number doesn't matter: the
//! copies are alike in every column.
//!
//! The join it numbers is the set operation's alone. That is a semi or anti join on each column of the two sides in order,
//! with NULLs equal and nothing else in its condition. DataFusion's planner makes one for INTERSECT and EXCEPT, and nothing
//! else does before the optimizer: a `[NOT] EXISTS` or `IN` is still a subquery here, and a join written in SQL takes NULLs
//! as unequal. INTERSECT and EXCEPT without ALL make the left distinct first, and over a distinct left the join is right
//! as it is, so a distinct left is left alone.
//!
//! The numbered join is what the plan means. What runs (`Counted`) counts instead: the right side's copies of each row in
//! a hash table, then each of the left's rows let through as its count says (`CountsExec`). DataFusion's plan of the
//! numbered join hashes both sides by every column, sorts each by every column for its window and joins on the numbers;
//! this hashes both sides and nothing more.
use datafusion::arrow::array::{BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, NullEquality, Result, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{Distinct, Expr, ExprFunctionExt, Join, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties, InputDistributionRequirements, PlanProperties};
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

/// The numbers' columns, the left's and the right's.
const LEFT: &str = "__set_all_l";
const RIGHT: &str = "__set_all_r";

#[derive(Debug)]
pub struct SetsAll;

impl AnalyzerRule for SetsAll {
    fn name(&self) -> &str { "pondra_sets_all" }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| match plan {
            LogicalPlan::Join(j) if counted(&j) => numbered(j).map(Transformed::yes),
            plan => Ok(Transformed::no(plan)),
        })
        .data()
    }
}

/// Whether `j` is an INTERSECT ALL's or an EXCEPT ALL's join, which must count copies: see the module's words.
fn counted(j: &Join) -> bool {
    let (left, right) = (j.left.schema(), j.right.schema());
    let named = |e: &Expr, name: &str| matches!(e, Expr::Column(c) if c.name == name);
    matches!(j.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        && j.null_equality == NullEquality::NullEqualsNull
        && j.filter.is_none()
        && j.on.len() == left.fields().len()
        && j.on.len() == right.fields().len()
        && j.on.iter().zip(left.fields().iter().zip(right.fields().iter())).all(|((l, r), (lf, rf))| named(l, lf.name()) && named(r, rf.name()))
        && !matches!(j.left.as_ref(), LogicalPlan::Distinct(Distinct::All(_)))
        && !j.on.last().is_some_and(|(l, _)| named(l, LEFT)) // (numbered already: a plan analyzed again)
}

/// `j` on each side's copies numbered, and the left's columns of it, as `j` gave them.
fn numbered(j: Join) -> Result<LogicalPlan> {
    let number = |side: &LogicalPlan, name: &str| -> Result<LogicalPlan> {
        let every = side.schema().columns().into_iter().map(Expr::Column).collect();
        LogicalPlanBuilder::from(side.clone()).window(vec![row_number().partition_by(every).build()?.alias(name)])?.build()
    };
    let (left, right) = (number(&j.left, LEFT)?, number(&j.right, RIGHT)?);
    let (keys_l, keys_r) = (left.schema().columns(), right.schema().columns());
    LogicalPlanBuilder::from(left)
        .join_detailed(right, j.join_type, (keys_l, keys_r), None, NullEquality::NullEqualsNull)?
        .project(j.left.schema().columns().into_iter().map(Expr::Column))?
        .build()
}

// ---------------------------------------------------------------- counting

/// Each numbered join (`numbered`) as DataFusion plans it, a hash join over two windows, run as a `CountsExec` of the
/// windows' inputs. Before `JoinSelection`, which would turn the join's sides. A frugal session's plan is left as it is:
/// its sorts and its sort-merge join spill (`frugal.rs`), and a count's table doesn't.
#[derive(Debug)]
pub struct Counted;

impl PhysicalOptimizerRule for Counted {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        if crate::frugal::is_on(config) {
            return Ok(plan);
        }
        let partitioned = config.execution.target_partitions > 1;
        plan.transform_up(|p| Ok(match counting(&p, partitioned)? {
            Some(c) => Transformed::yes(c),
            None => Transformed::no(p),
        }))
        .data()
    }

    fn name(&self) -> &str { "counted_sets" }

    fn schema_check(&self) -> bool { true }
}

type Exprs = Vec<Arc<dyn PhysicalExpr>>;

/// One side of a numbered join, as the join reads it.
struct Side {
    /// What the side's window numbers.
    input: Arc<dyn ExecutionPlan>,
    /// The join's keys but the number, over `input`: its i-th column alone, or that column cast to the other side's type.
    keys: Exprs,
    /// The side's columns as the join is given them, over `input`, with the number as 0 (nothing above the join reads it).
    columns: Vec<(Arc<dyn PhysicalExpr>, String)>,
}

/// `p`, a side of a join on `keys`, as a numbered join's side: a window that numbers the copies of `number`'s side and
/// maybe a projection above it (where the planner casts a key). None for any other plan.
fn side(p: &Arc<dyn ExecutionPlan>, number: &str, keys: &[&Arc<dyn PhysicalExpr>]) -> Option<Side> {
    let (cast, window) = match p.downcast_ref::<ProjectionExec>() {
        Some(c) => (Some(c), c.input()),
        None => (None, p),
    };
    let (exprs, input) = match (window.downcast_ref::<BoundedWindowAggExec>(), window.downcast_ref::<WindowAggExec>()) {
        (Some(w), _) => (w.window_expr(), w.input()),
        (_, Some(w)) => (w.window_expr(), w.input()),
        _ => return None,
    };
    let n = input.schema().fields().len(); // (the window's number, after the side's columns)
    if exprs.len() != 1 || exprs[0].name() != number || window.schema().fields().len() != n + 1 {
        return None;
    }
    // (each of `p`'s columns, over the window's)
    let over_window: Vec<(Arc<dyn PhysicalExpr>, String)> = match cast {
        Some(c) => c.expr().iter().map(|e| (e.expr.clone(), e.alias.clone())).collect(),
        None => window.schema().fields().iter().enumerate().map(|(i, f)| (Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>, f.name().clone())).collect(),
    };
    let reads = |e: &Arc<dyn PhysicalExpr>| datafusion::physical_expr::utils::collect_columns(e).into_iter().map(|c| c.index()).collect::<Vec<_>>();
    let keyed: Exprs = keys.iter().map(|k| k.downcast_ref::<Column>().and_then(|c| over_window.get(c.index())).map(|(e, _)| e.clone())).collect::<Option<_>>()?;
    let (last, keyed) = keyed.split_last()?;
    if !at(last, n) || keyed.len() != n || !keyed.iter().enumerate().all(|(i, k)| reads(k) == [i]) {
        return None;
    }
    let mut columns = vec![];
    for (e, name) in over_window {
        match (at(&e, n), reads(&e).contains(&n)) {
            (true, _) => columns.push((Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))) as Arc<dyn PhysicalExpr>, name)),
            (false, false) => columns.push((e, name)),
            (false, true) => return None,
        }
    }
    Some(Side { input: input.clone(), keys: keyed.to_vec(), columns })
}

/// Whether `e` is the column at `i`.
fn at(e: &Arc<dyn PhysicalExpr>, i: usize) -> bool { e.downcast_ref::<Column>().is_some_and(|c| c.index() == i) }

/// `p` as a `CountsExec`, when it is a numbered join: a semi or anti join, NULLs equal, on each column of two numbering
/// windows' sides and then their numbers, the keys of one type pair by pair. Its columns as the join gave them.
fn counting(p: &Arc<dyn ExecutionPlan>, partitioned: bool) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(j) = p.downcast_ref::<HashJoinExec>() else { return Ok(None) };
    let except = match j.join_type() {
        JoinType::LeftAnti => true,
        JoinType::LeftSemi => false,
        _ => return Ok(None),
    };
    if j.filter().is_some() || j.projection.is_some() || j.null_equality() != NullEquality::NullEqualsNull {
        return Ok(None);
    }
    let (on_l, on_r): (Vec<_>, Vec<_>) = j.on().iter().map(|(a, b)| (a, b)).unzip();
    let (Some(left), Some(right)) = (side(j.left(), LEFT, &on_l), side(j.right(), RIGHT, &on_r)) else { return Ok(None) };
    let (ls, rs) = (left.input.schema(), right.input.schema());
    for (a, b) in left.keys.iter().zip(&right.keys) {
        if a.data_type(&ls)? != b.data_type(&rs)? {
            return Ok(None);
        }
    }
    let counts: Arc<dyn ExecutionPlan> = Arc::new(CountsExec::new(left.input, right.input, (left.keys, right.keys), except, partitioned));
    Ok(Some(Arc::new(ProjectionExec::try_new(left.columns, counts)?)))
}

/// EXCEPT ALL or INTERSECT ALL by counting: the right side's copies of each row are counted (`Counts`), and then each
/// of the left's rows takes one off its row's count, EXCEPT ALL keeping the row when there is none left to take and
/// INTERSECT ALL when there is. A row is its keys: its columns, each maybe cast to the other side's type. Over sides
/// hashed by their keys, a table per partition; over one partition, one.
#[derive(Debug)]
pub struct CountsExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    keys: Arc<(Exprs, Exprs)>,
    except: bool,
    partitioned: bool,
    props: Arc<PlanProperties>,
}

impl CountsExec {
    fn new(left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>, keys: (Exprs, Exprs), except: bool, partitioned: bool) -> CountsExec {
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
        // (a count over an unbounded right side never ends, so nothing is let through)
        let bounded = match right.boundedness() {
            Boundedness::Bounded => left.boundedness(),
            Boundedness::Unbounded { .. } => Boundedness::Unbounded { requires_infinite_memory: true },
        };
        let props = PlanProperties::new(left.equivalence_properties().clone(), left.output_partitioning().clone(), EmissionType::Incremental, bounded);
        CountsExec { left, right, keys: Arc::new(keys), except, partitioned, props: Arc::new(props) }
    }
}

impl CountsExec {
    /// The keys a row is counted by: the left's and the right's, pair by pair.
    pub(crate) fn keys(&self) -> (&[Arc<dyn PhysicalExpr>], &[Arc<dyn PhysicalExpr>]) { (&self.keys.0, &self.keys.1) }
}

impl DisplayAs for CountsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CountsExec: {}", if self.except { "EXCEPT ALL" } else { "INTERSECT ALL" })
    }
}

impl ExecutionPlan for CountsExec {
    fn name(&self) -> &str { "CountsExec" }
    fn properties(&self) -> &Arc<PlanProperties> { &self.props }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.left, &self.right] }
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        if !self.partitioned {
            return InputDistributionRequirements::new(vec![Distribution::SinglePartition; 2]);
        }
        InputDistributionRequirements::co_partitioned(vec![Distribution::KeyPartitioned(self.keys.0.clone()), Distribution::KeyPartitioned(self.keys.1.clone())])
    }
    fn required_input_distribution(&self) -> Vec<Distribution> { self.input_distribution_requirements().into_per_child() }
    fn maintains_input_order(&self) -> Vec<bool> { vec![true, false] }
    fn apply_expressions(&self, f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        for e in self.keys.0.iter().chain(&self.keys.1) {
            if f(e)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CountsExec::new(children[0].clone(), children[1].clone(), (*self.keys).clone(), self.except, self.partitioned)))
    }
    fn execute(&self, partition: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let (left, mut right, except, schema) = (self.left.execute(partition, ctx.clone())?, self.right.execute(partition, ctx.clone())?, self.except, self.schema());
        let memory = MemoryConsumer::new(format!("CountsExec[{partition}]")).register(ctx.memory_pool());
        let ls = self.left.schema();
        let fields = self.keys.0.iter().map(|k| Ok(SortField::new(k.data_type(&ls)?))).collect::<Result<Vec<_>>>()?;
        let keys = self.keys.clone();
        let out = futures::stream::once(async move {
            let mut counts = Counts { rows: RowConverter::new(fields)?, at: Default::default(), memory };
            while let Some(b) = right.next().await {
                counts.add(&b?, &keys.1)?;
            }
            Ok::<_, DataFusionError>(left.map(move |b| counts.take(b?, &keys.0, except)))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// The right side's rows, each as the row encoding of its keys, and how many copies of each are left to take.
struct Counts {
    rows: RowConverter,
    at: datafusion::common::HashMap<Box<[u8]>, u64>,
    memory: MemoryReservation, // (each row counted against the query's budget while the table lives)
}

impl Counts {
    fn encoded(&self, b: &RecordBatch, keys: &Exprs) -> Result<Rows> {
        let columns = keys.iter().map(|k| k.evaluate(b)?.into_array(b.num_rows())).collect::<Result<Vec<_>>>()?;
        Ok(self.rows.convert_columns(&columns)?)
    }

    fn add(&mut self, b: &RecordBatch, keys: &Exprs) -> Result<()> {
        for row in self.encoded(b, keys)?.iter() {
            match self.at.get_mut(row.as_ref()) {
                Some(n) => *n += 1,
                None => {
                    self.memory.try_grow(row.as_ref().len() + std::mem::size_of::<(Box<[u8]>, u64)>() + 1)?;
                    self.at.insert(row.as_ref().into(), 1);
                }
            }
        }
        Ok(())
    }

    /// The rows of `b` kept: EXCEPT ALL's where no copy is left to take, INTERSECT ALL's where one is.
    fn take(&mut self, b: RecordBatch, keys: &Exprs, except: bool) -> Result<RecordBatch> {
        if self.at.is_empty() {
            return Ok(if except { b } else { b.slice(0, 0) });
        }
        let rows = self.encoded(&b, keys)?;
        let kept: BooleanArray = rows
            .iter()
            .map(|row| {
                Some(match self.at.get_mut(row.as_ref()) {
                    Some(n) if *n > 0 => {
                        *n -= 1;
                        !except
                    }
                    _ => except,
                })
            })
            .collect();
        Ok(filter_record_batch(&b, &kept)?)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;

    /// Each query's rows as text, sorted (NULL as `NULL`), as the session with Pondra's analyzer answers it: counted
    /// (`Counted`, over several partitions and over one), and as the numbered join's plan answers it, all alike.
    async fn rows(sql: &str) -> Vec<String> {
        let counted = answer(sql, true, 4).await;
        assert_eq!(counted, answer(sql, true, 1).await, "over one partition: {sql}");
        assert_eq!(counted, answer(sql, false, 4).await, "the numbered join's plan: {sql}");
        counted
    }

    fn session(counted: bool, partitions: usize) -> SessionContext {
        let config = datafusion::prelude::SessionConfig::new().with_target_partitions(partitions);
        let state = datafusion::execution::SessionStateBuilder::new().with_config(config).with_default_features().with_analyzer_rules(crate::optimize::analyzer_rules());
        let state = if counted { state.with_physical_optimizer_rules(crate::optimize::physical_rules()) } else { state };
        SessionContext::new_with_state(state.build())
    }

    async fn answer(sql: &str, counted: bool, partitions: usize) -> Vec<String> {
        let ctx = session(counted, partitions);
        let batches = ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")).collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut out: Vec<String> = batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).map(move |i| b.columns().iter().map(|c| if c.is_null(i) { "NULL".into() } else { array_value_to_string(c, i).unwrap() }).collect::<Vec<_>>().join(",")))
            .collect();
        out.sort();
        out
    }

    const L: &str = "(VALUES (1), (1), (1), (2), (2), (3), (NULL), (NULL)) AS l(a)";
    const R: &str = "(VALUES (1), (2), (2), (2), (4), (NULL)) AS r(a)";

    #[tokio::test]
    async fn except_all_and_intersect_all_count_the_copies() {
        // (each answer is DuckDB's and Postgres's: 1 three times on the left and once on the right, 2 twice and three
        // times, NULL twice and once)
        assert_eq!(rows(&format!("SELECT a FROM {L} EXCEPT ALL SELECT a FROM {R}")).await, ["1", "1", "3", "NULL"]);
        assert_eq!(rows(&format!("SELECT a FROM {L} INTERSECT ALL SELECT a FROM {R}")).await, ["1", "2", "2", "NULL"]);
        assert_eq!(rows(&format!("SELECT a FROM {R} EXCEPT ALL SELECT a FROM {L}")).await, ["2", "4"]);
        // (without ALL, as before: each row once)
        assert_eq!(rows(&format!("SELECT a FROM {L} EXCEPT SELECT a FROM {R}")).await, ["3"]);
        assert_eq!(rows(&format!("SELECT a FROM {L} INTERSECT SELECT a FROM {R}")).await, ["1", "2", "NULL"]);
        // (a distinct left, where the plain join was right already)
        assert_eq!(rows(&format!("SELECT DISTINCT a FROM {L} EXCEPT ALL SELECT a FROM {R}")).await, ["3"]);
        assert_eq!(rows(&format!("SELECT DISTINCT a FROM {L} INTERSECT ALL SELECT a FROM {R}")).await, ["1", "2", "NULL"]);
    }

    #[tokio::test]
    async fn every_column_is_counted_in_every_place() {
        let l = "(VALUES (1, 'x'), (1, 'x'), (1, 'y'), (2, NULL), (2, NULL)) AS l(a, b)";
        let r = "(VALUES (1, 'x'), (1, 'y'), (1, 'y'), (2, NULL)) AS r(a, b)";
        assert_eq!(rows(&format!("SELECT a, b FROM {l} EXCEPT ALL SELECT a, b FROM {r}")).await, ["1,x", "2,NULL"]);
        assert_eq!(rows(&format!("SELECT a, b FROM {l} INTERSECT ALL SELECT a, b FROM {r}")).await, ["1,x", "1,y", "2,NULL"]);
        // (sides of two types, one table on both sides, a chain, and inside a subquery, an IN and a CTE)
        assert_eq!(rows(&format!("SELECT a FROM {L} EXCEPT ALL SELECT CAST(a AS INT) FROM {R}")).await, ["1", "1", "3", "NULL"]);
        assert_eq!(rows(&format!("SELECT a FROM {L} EXCEPT ALL SELECT a FROM {L} WHERE a = 1")).await, ["2", "2", "3", "NULL", "NULL"]);
        assert_eq!(rows(&format!("(SELECT a FROM {L} EXCEPT ALL SELECT a FROM {R}) EXCEPT ALL SELECT 1")).await, ["1", "3", "NULL"]);
        assert_eq!(rows(&format!("SELECT count(*) FROM (SELECT a FROM {L} INTERSECT ALL SELECT a FROM {R})")).await, ["4"]);
        assert_eq!(rows(&format!("SELECT x FROM (VALUES (1), (3), (5)) AS v(x) WHERE x IN (SELECT a FROM {L} EXCEPT ALL SELECT a FROM {R})")).await, ["1", "3"]);
        assert_eq!(rows(&format!("WITH s AS (SELECT a FROM {L} EXCEPT ALL SELECT a FROM {R}) SELECT a, count(*) FROM s GROUP BY a")).await, ["1,2", "3,1", "NULL,1"]);
    }

    #[tokio::test]
    async fn the_copies_are_counted_with_no_sort_and_no_window() {
        for partitions in [1, 4] {
            // (and sides of two types, one cast to the other's above its numbers)
            for right in ["a", "CAST(a AS INT)"] {
                let sql = format!("EXPLAIN SELECT a FROM {L} EXCEPT ALL SELECT {right} FROM {R}");
                let plan = answer(&sql, true, partitions).await.join("\n");
                assert!(plan.contains("CountsExec: EXCEPT ALL") && !plan.contains("WindowAggExec") && !plan.contains("SortExec"), "{plan}");
            }
        }
        // (a frugal session's plan spills: it is left as the numbered join's)
        let config = datafusion::prelude::SessionConfig::new().with_target_partitions(1).with_option_extension(crate::frugal::Frugal { on: true });
        let state = datafusion::execution::SessionStateBuilder::new().with_config(config).with_default_features().with_analyzer_rules(crate::optimize::analyzer_rules());
        let ctx = SessionContext::new_with_state(state.with_physical_optimizer_rules(crate::optimize::physical_rules()).build());
        let sql = format!("EXPLAIN SELECT a FROM {L} INTERSECT ALL SELECT a FROM {R}");
        let plan = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let plan = datafusion::arrow::util::pretty::pretty_format_batches(&plan).unwrap().to_string();
        assert!(!plan.contains("CountsExec") && plan.contains("WindowAggExec"), "{plan}");
    }

    #[tokio::test]
    async fn a_join_written_in_sql_is_not_a_set_operation() {
        // (NOT EXISTS keeps every copy of a left row the right lacks, and a semi join every copy it has: neither counts)
        assert_eq!(rows(&format!("SELECT a FROM {L} WHERE NOT EXISTS (SELECT 1 FROM {R} WHERE r.a = l.a)")).await, ["3", "NULL", "NULL"]);
        assert_eq!(rows(&format!("SELECT a FROM {L} WHERE a IN (SELECT a FROM {R})")).await, ["1", "1", "1", "2", "2"]);
        assert_eq!(rows(&format!("SELECT l.a FROM {L} LEFT SEMI JOIN {R} ON l.a = r.a")).await, ["1", "1", "1", "2", "2"]);
    }
}
