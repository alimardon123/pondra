//! Point-in-time joins: each row joined to the other table's row as it was at that moment.
//!
//! `SELECT … FROM trades t ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym`
//! gives every trade the latest quote of its symbol at or before the trade (Snowflake's syntax,
//! whose left operand in MATCH_CONDITION is the left table's; `>` is strictly before, `<=` the
//! first at or after, `<` strictly after). A trade with no such quote keeps NULLs. Over a stream
//! (an inline view or a task), each event gets the table as it was at the event's own time, however
//! late the event arrives and whatever changed since.
//!
//! DataFusion has no such join, so the SQL is rewritten into a LEFT JOIN on the keys whose
//! condition carries a marker, `pondra_asof(t.ts >= q.ts)`, and the join DataFusion plans for it
//! is replaced (`Rule`) by one that finds the one row itself (`AsOfJoinExec`): the rows it looks up
//! in, per key, in time order, and a binary search per row: all of them in one table, one table
//! per partition where DataFusion would have hashed both sides by the key, or, where the kept side
//! is the small one (a stream's new rows), only its keys' rows (`Mode`). The marker fails if it
//! is ever run, so nothing can quietly return every earlier row instead of the latest.
use datafusion::arrow::array::{make_comparator, Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array};
use datafusion::arrow::compute::{cast, concat_batches, sort_to_indices, take, SortOptions};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{config::ConfigOptions, exec_err, plan_err, JoinSide, JoinType, NullEquality, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{ColumnarValue, Operator, Volatility};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::{split_conjunction, EquivalenceProperties, Partitioning, PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties, InputDistributionRequirements, PlanProperties};
use futures::{StreamExt, TryStreamExt};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

const MARKER: &str = "pondra_asof";
type Expr = Arc<dyn PhysicalExpr>;

// ---------------------------------------------------------------- SQL

/// The SQL with each `ASOF JOIN … MATCH_CONDITION (…) [ON …]` turned into a LEFT JOIN on its
/// keys whose condition carries the marker; as it was if it has none.
pub fn rewrite(sql: &str) -> anyhow::Result<Cow<'_, str>> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    use std::ops::ControlFlow;
    if !sql.to_ascii_lowercase().contains("asof") {
        return Ok(Cow::Borrowed(sql));
    }
    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return Ok(Cow::Borrowed(sql)) };
    fn name(t: &TableFactor) -> Option<String> {
        match t {
            TableFactor::Table { alias: Some(a), .. } | TableFactor::Derived { alias: Some(a), .. } => Some(a.name.value.to_lowercase()),
            TableFactor::Table { name, .. } => name.0.last().and_then(|p| p.as_ident()).map(|i| i.value.to_lowercase()),
            _ => None,
        }
    }
    fn joins(t: &mut TableWithJoins, found: &mut bool) -> anyhow::Result<()> {
        for j in &mut t.joins {
            let JoinOperator::AsOf { match_condition, constraint } = &j.join_operator else { continue };
            let mut cond = match_condition;
            while let Expr::Nested(e) = cond {
                cond = e;
            }
            let Expr::BinaryOp { left, op, right } = cond else { anyhow::bail!("ASOF JOIN: MATCH_CONDITION compares a column of each table with >=, >, <= or <") };
            let flipped = match op {
                BinaryOperator::GtEq => BinaryOperator::LtEq,
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::LtEq => BinaryOperator::GtEq,
                BinaryOperator::Lt => BinaryOperator::Gt,
                _ => anyhow::bail!("ASOF JOIN: MATCH_CONDITION compares with >=, >, <= or <, not {op}"),
            };
            // (its left operand is the left table's: written the other way round, it is turned)
            let theirs = |e: &Expr| matches!(e, Expr::CompoundIdentifier(p) if p.len() > 1 && Some(p[0].value.to_lowercase()) == name(&j.relation));
            let marker = match theirs(left) {
                true => format!("{MARKER}({right} {flipped} {left})"),
                false => format!("{MARKER}({left} {op} {right})"),
            };
            let on = match constraint {
                JoinConstraint::On(e) => format!("({e}) AND {marker}"),
                JoinConstraint::None => marker,
                _ => anyhow::bail!("ASOF JOIN: its keys go in ON a.k = b.k (not USING or NATURAL)"),
            };
            j.join_operator = JoinOperator::Left(JoinConstraint::On(Parser::new(&GenericDialect {}).try_with_sql(&on)?.parse_expr()?));
            *found = true;
        }
        Ok(())
    }
    struct Joins(bool);
    impl VisitorMut for Joins {
        type Break = anyhow::Error;
        fn post_visit_select(&mut self, s: &mut Select) -> ControlFlow<anyhow::Error> {
            s.from.iter_mut().try_for_each(|t| joins(t, &mut self.0)).map_or_else(ControlFlow::Break, ControlFlow::Continue)
        }
        fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<anyhow::Error> {
            match t {
                TableFactor::NestedJoin { table_with_joins, .. } => joins(table_with_joins, &mut self.0).map_or_else(ControlFlow::Break, ControlFlow::Continue),
                _ => ControlFlow::Continue(()),
            }
        }
    }
    let mut v = Joins(false);
    if let ControlFlow::Break(e) = VisitMut::visit(&mut stmts, &mut v) {
        return Err(e);
    }
    Ok(match v.0 {
        true => Cow::Owned(stmts.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(";\n")),
        false => Cow::Borrowed(sql),
    })
}

/// The marker: it only tells the planner which join is an as-of one.
pub fn register(ctx: &datafusion::prelude::SessionContext) {
    let never = |_: &[ColumnarValue]| exec_err!("ASOF JOIN: this query's plan has no place for it (its MATCH_CONDITION must compare a column of each table)");
    ctx.register_udf(datafusion::logical_expr::create_udf(MARKER, vec![DataType::Boolean], DataType::Boolean, Volatility::Immutable, Arc::new(never)));
}

// ---------------------------------------------------------------- planning

/// Whether a (logical) join is an as-of one.
fn marked(j: &datafusion::logical_expr::Join) -> bool {
    use datafusion::logical_expr::Expr;
    j.filter.as_ref().is_some_and(|f| f.exists(|e| Ok(matches!(e, Expr::ScalarFunction(s) if s.name() == MARKER))).unwrap_or(false))
}

/// DataFusion's rule that makes an outer join inner where a WHERE drops its rows padded with
/// NULLs — except over an as-of join: inner, the WHERE would be pushed into the side it looks
/// rows up in, and give each trade the latest quote that passes the filter instead of the latest
/// quote, filtered.
#[derive(Debug)]
pub struct KeepOuter(pub Arc<dyn datafusion::optimizer::OptimizerRule + Send + Sync>);

impl datafusion::optimizer::OptimizerRule for KeepOuter {
    fn name(&self) -> &str { self.0.name() }
    fn apply_order(&self) -> Option<datafusion::optimizer::optimizer::ApplyOrder> { self.0.apply_order() }
    fn supports_rewrite(&self) -> bool { true }
    fn rewrite(&self, plan: datafusion::logical_expr::LogicalPlan, config: &dyn datafusion::optimizer::OptimizerConfig) -> Result<Transformed<datafusion::logical_expr::LogicalPlan>> {
        use datafusion::logical_expr::LogicalPlan::{self, Filter, Join, Projection};
        fn asof(p: &LogicalPlan) -> bool {
            match p {
                Projection(p) => asof(&p.input), // (as the rule looks through them)
                Join(j) => marked(j),
                _ => false,
            }
        }
        match &plan {
            Filter(f) if asof(&f.input) => Ok(Transformed::no(plan)),
            _ => self.0.rewrite(plan, config),
        }
    }
}

/// Where DataFusion planned a join carrying the marker: one that finds the one row instead.
#[derive(Debug)]
pub struct Rule;

impl PhysicalOptimizerRule for Rule {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan.transform_up(|p| Ok(match AsOfJoinExec::replacing(&p)? {
            Some(x) => Transformed::yes(Arc::new(x) as Arc<dyn ExecutionPlan>),
            None => Transformed::no(p),
        }))?.data)
    }
    fn name(&self) -> &str { "asof_join" }
    fn schema_check(&self) -> bool { true }
}

/// What a point-in-time join looks up with. `keep` is the side every row of which is kept (the
/// left table), `asof` the side it looks rows up in.
#[derive(Debug)]
struct Spec {
    keys: Vec<(Expr, Expr)>, // (keep, asof)
    time: (Expr, Expr),
    op: Operator, // keep's time `op` asof's time
    outer: bool,  // a row with no match is kept, with NULLs
    keep_left: bool, // the kept side's columns come first
    projection: Option<Vec<usize>>,
    nulls_match: bool,
}

/// Where the rows a row is looked up in come from.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    Collected,   // all of the looked-up side, one table every partition of the kept side shares
    Partitioned, // both sides hashed by the key: a table per partition
    Keys,        // the kept side is small: all of it first, then only its keys' rows of the other
}

#[derive(Debug)]
pub struct AsOfJoinExec {
    keep: Arc<dyn ExecutionPlan>,
    asof: Arc<dyn ExecutionPlan>,
    spec: Arc<Spec>,
    mode: Mode,
    built: Arc<tokio::sync::OnceCell<Arc<Lookup>>>,
    props: Arc<PlanProperties>,
}

impl AsOfJoinExec {
    /// The as-of join `p` stands for, if it carries the marker.
    fn replacing(p: &Arc<dyn ExecutionPlan>) -> Result<Option<AsOfJoinExec>> {
        let (l, r, on, filter, kind, projection, collect_left, nulls_match) = if let Some(j) = p.downcast_ref::<HashJoinExec>() {
            let collect = *j.partition_mode() == PartitionMode::CollectLeft;
            (j.left(), j.right(), j.on().to_vec(), j.filter(), *j.join_type(), j.projection.as_ref().map(|p| p.to_vec()), collect, j.null_equality() == NullEquality::NullEqualsNull)
        } else if let Some(j) = p.downcast_ref::<SortMergeJoinExec>() {
            (j.left(), j.right(), j.on().to_vec(), j.filter().as_ref(), j.join_type(), None, false, j.null_equality() == NullEquality::NullEqualsNull) // (hash joins turned off)
        } else if let Some(j) = p.downcast_ref::<NestedLoopJoinExec>() {
            (j.left(), j.right(), vec![], j.filter(), *j.join_type(), j.projection().as_ref().map(|p| p.to_vec()), true, false)
        } else {
            return Ok(None);
        };
        let Some(filter) = filter else { return Ok(None) };
        let parts = split_conjunction(filter.expression());
        let Some(marker) = parts.iter().find_map(|e| e.downcast_ref::<ScalarFunctionExpr>().filter(|f| f.name() == MARKER)) else { return Ok(None) };
        if parts.len() > 1 {
            return plan_err!("ASOF JOIN: its ON takes only equalities of a column of each table");
        }
        let Some(cmp) = marker.args().first().and_then(|a| a.downcast_ref::<BinaryExpr>()) else { return plan_err!("ASOF JOIN: MATCH_CONDITION compares a column of each table") };
        // Each operand as an expression over its own side's rows.
        let side = |e: &Expr| -> Result<(JoinSide, Expr)> {
            let mut sides = vec![];
            let e = e.clone().transform(|x| {
                let Some(c) = x.downcast_ref::<Column>() else { return Ok(Transformed::no(x)) };
                let at = &filter.column_indices()[c.index()];
                sides.push(at.side);
                Ok(Transformed::yes(Arc::new(Column::new(c.name(), at.index)) as Expr))
            })?;
            sides.dedup();
            match sides[..] {
                [s] => Ok((s, e.data)),
                _ => plan_err!("ASOF JOIN: each side of MATCH_CONDITION reads one table"),
            }
        };
        let ((ls, lt), (rs, rt)) = (side(cmp.left())?, side(cmp.right())?);
        if ls == rs {
            return plan_err!("ASOF JOIN: MATCH_CONDITION compares a column of each table");
        }
        let keep = match kind {
            JoinType::Left => JoinSide::Left,
            JoinType::Right => JoinSide::Right,
            JoinType::Inner => ls, // (a filter on the looked-up side's columns made it inner)
            other => return plan_err!("ASOF JOIN can't run as a {other} join"),
        };
        let flip = |op: Operator| match op {
            Operator::GtEq => Operator::LtEq,
            Operator::Gt => Operator::Lt,
            Operator::LtEq => Operator::GtEq,
            Operator::Lt => Operator::Gt,
            op => op,
        };
        let (time, op) = if ls == keep { ((lt, rt), *cmp.op()) } else { ((rt, lt), flip(*cmp.op())) };
        let keep_left = keep == JoinSide::Left;
        let (kp, ap) = if keep_left { (l, r) } else { (r, l) };
        let keys = on.into_iter().map(|(a, b)| if keep_left { (a, b) } else { (b, a) }).collect::<Vec<_>>();
        // As DataFusion sized the sides: the side it would have collected, collected (the kept
        // side first, when that's the small one: a stream's new rows, say); else both hashed.
        let mode = match (keys.is_empty(), collect_left) {
            (true, _) => Mode::Collected,
            (false, true) if keep_left => Mode::Keys,
            (false, true) => Mode::Collected,
            (false, false) => Mode::Partitioned,
        };
        let spec = Spec { keys, time, op, outer: kind != JoinType::Inner, keep_left, projection, nulls_match };
        Ok(Some(AsOfJoinExec::new(kp.clone(), ap.clone(), Arc::new(spec), mode, p.schema())))
    }

    fn new(keep: Arc<dyn ExecutionPlan>, asof: Arc<dyn ExecutionPlan>, spec: Arc<Spec>, mode: Mode, schema: SchemaRef) -> AsOfJoinExec {
        let parts = Partitioning::UnknownPartitioning(if mode == Mode::Keys { 1 } else { keep.output_partitioning().partition_count() });
        let props = PlanProperties::new(EquivalenceProperties::new(schema), parts, EmissionType::Incremental, Boundedness::Bounded);
        AsOfJoinExec { keep, asof, spec, mode, built: Default::default(), props: Arc::new(props) }
    }
}

// ---------------------------------------------------------------- running

/// The looked-up side: its rows, its time values, and its rows per key (as the row encoding of
/// its key columns), in time order.
struct Lookup {
    rows: RecordBatch,
    time: ArrayRef,
    keys: Option<RowConverter>,
    at: HashMap<Box<[u8]>, Vec<u32>>,
    _memory: MemoryReservation, // (counted against the query's budget while it lives)
}

impl std::fmt::Debug for Lookup {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Lookup({} rows)", self.rows.num_rows()) }
}

/// The key columns' values, and which rows can match (no NULL in them, unless NULLs match).
fn keys_of(exprs: &[Expr], b: &RecordBatch, nulls_match: bool) -> Result<(Vec<ArrayRef>, Vec<bool>)> {
    let cols = exprs.iter().map(|e| e.evaluate(b)?.into_array(b.num_rows())).collect::<Result<Vec<_>>>()?;
    let ok = (0..b.num_rows()).map(|i| nulls_match || cols.iter().all(|c| c.is_valid(i))).collect();
    Ok((cols, ok))
}

/// The looked-up side's rows in `parts`; with `only`, just the rows of the keys it holds.
async fn lookup(spec: &Spec, asof: &Arc<dyn ExecutionPlan>, parts: Vec<usize>, ctx: &Arc<TaskContext>, only: Option<&RecordBatch>) -> Result<Lookup> {
    let schema = asof.schema();
    let fields = spec.keys.iter().map(|k| Ok(SortField::new(k.1.data_type(&schema)?))).collect::<Result<Vec<_>>>()?;
    let keys = (!fields.is_empty()).then(|| RowConverter::new(fields)).transpose()?;
    let wanted: Option<std::collections::HashSet<Box<[u8]>>> = match (only, &keys) {
        (Some(kept), Some(k)) => {
            let (cols, ok) = keys_of(&spec.keys.iter().map(|k| k.0.clone()).collect::<Vec<_>>(), kept, spec.nulls_match)?;
            let encoded = k.convert_columns(&cols)?;
            Some((0..kept.num_rows()).filter(|&i| ok[i]).map(|i| encoded.row(i).data().into()).collect())
        }
        _ => None,
    };
    let streams = parts.into_iter().map(|p| asof.execute(p, ctx.clone())).collect::<Result<Vec<_>>>()?;
    let batches: Vec<RecordBatch> = futures::stream::select_all(streams)
        .map(|b| -> Result<RecordBatch> {
            let b = b?;
            let (Some(wanted), Some(k)) = (&wanted, &keys) else { return Ok(b) };
            let encoded = k.convert_columns(&keys_of(&spec.keys.iter().map(|k| k.1.clone()).collect::<Vec<_>>(), &b, true)?.0)?;
            let mask: datafusion::arrow::array::BooleanArray = (0..b.num_rows()).map(|i| Some(wanted.contains(encoded.row(i).data()))).collect();
            Ok(datafusion::arrow::compute::filter_record_batch(&b, &mask)?)
        })
        .try_collect()
        .await?;
    let rows = concat_batches(&schema, &batches)?;
    // Past the query's memory budget this fails, and the query runs again with sort-merge joins
    // (`App::query`), where this join builds a table per partition instead.
    let memory = MemoryConsumer::new("AsOfJoinExec").register(ctx.memory_pool());
    memory.try_grow(rows.get_array_memory_size() + rows.num_rows() * 4)?;
    let time = spec.time.1.evaluate(&rows)?.into_array(rows.num_rows())?;
    let (cols, ok) = keys_of(&spec.keys.iter().map(|k| k.1.clone()).collect::<Vec<_>>(), &rows, spec.nulls_match)?;
    let encoded = keys.as_ref().map(|k| k.convert_columns(&cols)).transpose()?;
    let mut at: HashMap<Box<[u8]>, Vec<u32>> = HashMap::new();
    for i in sort_to_indices(&time, None, None)?.values().iter().map(|&i| i as usize) {
        if time.is_valid(i) && ok[i] {
            at.entry(encoded.as_ref().map_or(&[][..], |e| e.row(i).data()).into()).or_default().push(i as u32);
        }
    }
    memory.try_grow(at.keys().map(|k| k.len() + 64).sum())?;
    Ok(Lookup { rows, time, keys, at, _memory: memory })
}

/// A batch of the kept side, each row with the one row it matches.
fn join(spec: &Spec, l: &Lookup, b: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let n = b.num_rows();
    let time = cast(&spec.time.0.evaluate(&b)?.into_array(n)?, l.time.data_type())?;
    let (cols, ok) = keys_of(&spec.keys.iter().map(|k| k.0.clone()).collect::<Vec<_>>(), &b, spec.nulls_match)?;
    let encoded = l.keys.as_ref().map(|k| k.convert_columns(&cols)).transpose()?;
    let cmp = make_comparator(&time, &l.time, SortOptions::default())?;
    let (mut from, mut to) = (Vec::with_capacity(n), Vec::with_capacity(n));
    for i in 0..n {
        let rows = (time.is_valid(i) && ok[i]).then(|| l.at.get(encoded.as_ref().map_or(&[][..], |e| e.row(i).data()))).flatten();
        let hit = rows.and_then(|rows| {
            // (rows in time order: those before this row's time come first)
            let upto = |before: fn(Ordering) -> bool| rows.partition_point(|&j| before(cmp(i, j as usize)));
            match spec.op {
                Operator::GtEq => upto(|o| o != Ordering::Less).checked_sub(1).map(|k| rows[k]),
                Operator::Gt => upto(|o| o == Ordering::Greater).checked_sub(1).map(|k| rows[k]),
                Operator::LtEq => rows.get(upto(|o| o == Ordering::Greater)).copied(),
                Operator::Lt => rows.get(upto(|o| o != Ordering::Less)).copied(),
                _ => None,
            }
        });
        if hit.is_some() || spec.outer {
            from.push(i as u32);
            to.push(hit);
        }
    }
    let (from, to) = (UInt32Array::from(from), UInt32Array::from(to));
    let kept: Vec<ArrayRef> = match from.len() == n {
        true => b.columns().to_vec(),
        false => b.columns().iter().map(|c| take(c, &from, None)).collect::<std::result::Result<_, _>>()?,
    };
    let found: Vec<ArrayRef> = l.rows.columns().iter().map(|c| take(c, &to, None)).collect::<std::result::Result<_, _>>()?;
    let all: Vec<ArrayRef> = if spec.keep_left { [kept, found].concat() } else { [found, kept].concat() };
    let out = match &spec.projection {
        Some(p) => p.iter().map(|&i| all[i].clone()).collect(),
        None => all,
    };
    Ok(RecordBatch::try_new_with_options(schema.clone(), out, &RecordBatchOptions::new().with_row_count(Some(from.len())))?)
}

impl DisplayAs for AsOfJoinExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let on = self.spec.keys.iter().map(|(a, b)| format!("({a}, {b})")).collect::<Vec<_>>().join(", ");
        let mode = self.mode;
        let kind = if self.spec.outer { "Left" } else { "Inner" };
        write!(f, "AsOfJoinExec: mode={:?}, join_type={kind}, on=[{on}], match={} {} {}", mode, self.spec.time.0, self.spec.op, self.spec.time.1)
    }
}

impl ExecutionPlan for AsOfJoinExec {
    fn name(&self) -> &str { "AsOfJoinExec" }
    fn properties(&self) -> &Arc<PlanProperties> { &self.props }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.keep, &self.asof] }
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        match self.mode {
            Mode::Partitioned => InputDistributionRequirements::co_partitioned(vec![Distribution::KeyPartitioned(self.spec.keys.iter().map(|k| k.0.clone()).collect()), Distribution::KeyPartitioned(self.spec.keys.iter().map(|k| k.1.clone()).collect())]),
            Mode::Keys => InputDistributionRequirements::new(vec![Distribution::SinglePartition, Distribution::UnspecifiedDistribution]),
            Mode::Collected => InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution; 2]),
        }
    }
    fn required_input_distribution(&self) -> Vec<Distribution> { self.input_distribution_requirements().into_per_child() }
    fn apply_expressions(&self, f: &mut dyn FnMut(&Expr) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        let s = &self.spec;
        for e in s.keys.iter().flat_map(|(a, b)| [a, b]).chain([&s.time.0, &s.time.1]) {
            if f(e)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AsOfJoinExec::new(children[0].clone(), children[1].clone(), self.spec.clone(), self.mode, self.schema())))
    }
    fn execute(&self, partition: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let (rows, spec, asof, schema, built, mode) = (self.keep.execute(partition, ctx.clone())?, self.spec.clone(), self.asof.clone(), self.schema(), self.built.clone(), self.mode);
        let (out_schema, keep_schema) = (schema.clone(), self.keep.schema());
        let out = futures::stream::once(async move {
            let all = (0..asof.output_partitioning().partition_count()).collect();
            let (l, rows) = match mode {
                Mode::Partitioned => (Arc::new(lookup(&spec, &asof, vec![partition], &ctx, None).await?), rows.boxed()),
                Mode::Collected => {
                    let l = built.get_or_try_init(|| async { Ok::<_, datafusion::error::DataFusionError>(Arc::new(lookup(&spec, &asof, all, &ctx, None).await?)) }).await?;
                    (l.clone(), rows.boxed())
                }
                Mode::Keys => {
                    let kept = concat_batches(&keep_schema, &rows.try_collect::<Vec<_>>().await?)?;
                    (Arc::new(lookup(&spec, &asof, all, &ctx, Some(&kept)).await?), futures::stream::iter([Ok(kept)]).boxed())
                }
            };
            Ok::<_, datafusion::error::DataFusionError>(rows.map(move |b| join(&spec, &l, b?, &out_schema)))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}
