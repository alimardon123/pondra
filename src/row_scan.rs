//! A table read under a grant's filter on its rows (step 6 piece 5b): `query::Guarded`'s scan for a reader whose grant
//! names one (`rights::Scope::rows`). Only the rows the filter keeps leave the scan, and nothing the query asks of a row
//! it hides is worked out where its reader could tell: the scan is one leaf of the plan (`GuardedExec`), so no rule of
//! the planner moves the query's own conditions or limits under the filter, and the query's conditions reach the files
//! beneath only when they can't fail on any row (`leakproof`). L1's review of the plan (l3/rights-piece5-review-l1.md)
//! holds the reasons.

use datafusion::arrow::array::AsArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_err, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning, PlanProperties, RecordBatchStream};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A grant's filter on a table's rows, as its reader's scope gives it (`Scope::rows`: the reader's words filled in), and
/// whether EXPLAIN may show its text (`Scope::shows_code`; otherwise it says only that there is one).
#[derive(Debug, Clone)]
pub struct Rows {
    pub sql: String,
    pub told: bool,
}

/// `inner` (a table its reader may read these columns of: `Guarded::checked` asked) scanned for `projection` under `rows`:
/// the scan of those columns and the filter's (whichever its reader may read), `filters` (the query's, every one of them
/// leakproof: `Guarded::supports_filters_pushdown`) and the filter itself where it is leakproof too, so files are pruned by
/// both; then the filter, the columns asked for and `limit`, in `GuardedExec`. A filter that plans no more (a column it
/// names dropped since) shows no row, as one that parses no more does (`row_filter::filled`).
pub async fn scan(state: &dyn Session, inner: &Arc<dyn TableProvider>, table: &str, rows: &Rows, projection: Option<Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
    let all = inner.schema();
    let want: Vec<usize> = projection.unwrap_or_else(|| (0..all.fields().len()).collect());
    let filter = planned(state, &all, &rows.sql).unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(false)), None));
    match &filter {
        Expr::Literal(ScalarValue::Boolean(Some(true)), _) => return inner.scan(state, Some(&want), filters, limit).await, // (it hides no row)
        Expr::Literal(ScalarValue::Boolean(Some(false) | None) | ScalarValue::Null, _) => {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(all.project(&want)?)))); // (it shows none)
        }
        _ => {}
    }
    let mut read = want.clone();
    for c in filter.column_refs() {
        let i = all.index_of(&c.name)?;
        if !read.contains(&i) {
            read.push(i);
        }
    }
    let mut pushed = filters.to_vec();
    if leakproof(&filter, &all) && !matches!(inner.supports_filters_pushdown(&[&filter])?.as_slice(), [TableProviderFilterPushDown::Unsupported]) {
        pushed.push(filter.clone());
    }
    let scan = inner.scan(state, Some(&read), &pushed, None).await?; // (the limit goes above the filter)
    let schema = scan.schema();
    let kept = want.iter().map(|&i| schema.index_of(all.field(i).name())).collect::<std::result::Result<Vec<_>, _>>()?;
    let physical = state.create_physical_expr(filter, &DFSchema::try_from(schema.as_ref().clone())?)?;
    Ok(Arc::new(GuardedExec::new(table.to_string(), rows.told.then(|| rows.sql.clone()), scan, physical, kept, limit)?))
}

/// `sql` over `schema`'s columns, as this session plans it: coerced to their types and simplified (a filter on the
/// reader's name that names someone else is FALSE by now).
fn planned(state: &dyn Session, schema: &SchemaRef, sql: &str) -> Result<Expr> {
    let Some(session) = state.as_any().downcast_ref::<SessionState>() else { return internal_err!("a filter on rows is planned in a session") };
    let df = DFSchema::try_from(schema.as_ref().clone())?;
    let e = session.create_logical_expr(sql, &df)?;
    let simplifier = ExprSimplifier::new(SimplifyContext::builder().with_schema(Arc::new(df.clone())).build());
    simplifier.simplify(simplifier.coerce(e, &df)?)
}

/// Can `e`, a condition already coerced to its columns' types, be worked out on a row its reader's grant hides, and tell
/// nothing of it: it fails on no row, whatever the row holds, so how it went is never seen (L1's review, change 4). A
/// column, or one cast in a way that can't fail (`widens`), or TRY_CAST, compared with another such or with anything that
/// names no column; IS [NOT] NULL, IN, BETWEEN of those; LIKE of a text column itself with a pattern that names no column;
/// AND, OR and NOT of leakproof conditions. Anything else (arithmetic, a function, a cast that may fail, as `s = 5` on a
/// text column casts `s` and its error quotes the value) runs above the filter, on the rows it shows.
pub fn leakproof(e: &Expr, schema: &Schema) -> bool {
    use Operator::*;
    if constant(e) {
        return true;
    }
    let ok = |x: &Expr| leakproof(x, schema);
    match e {
        Expr::Column(_) => true,
        Expr::Cast(c) => type_of(&c.expr, schema).is_some_and(|from| widens(&from, c.field.data_type())) && ok(&c.expr),
        Expr::TryCast(c) => ok(&c.expr),
        Expr::BinaryExpr(b) => matches!(b.op, Eq | NotEq | Lt | LtEq | Gt | GtEq | IsDistinctFrom | IsNotDistinctFrom | And | Or) && ok(&b.left) && ok(&b.right),
        Expr::Not(x) | Expr::IsNull(x) | Expr::IsNotNull(x) | Expr::IsTrue(x) | Expr::IsFalse(x) | Expr::IsUnknown(x) | Expr::IsNotTrue(x) | Expr::IsNotFalse(x) | Expr::IsNotUnknown(x) => ok(x),
        Expr::Between(b) => ok(&b.expr) && ok(&b.low) && ok(&b.high),
        Expr::InList(l) => ok(&l.expr) && l.list.iter().all(ok),
        Expr::Like(l) => matches!(type_of(&l.expr, schema), Some(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)) && matches!(*l.expr, Expr::Column(_)) && constant(&l.pattern),
        _ => false,
    }
}

/// Does `e` name no column of any row, nor read a subquery: the same for every row, so it tells nothing of one.
fn constant(e: &Expr) -> bool {
    !e.exists(|x| Ok(matches!(x, Expr::Column(_) | Expr::OuterReferenceColumn(..) | Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) | Expr::SetComparison(_)))).unwrap_or(true)
}

/// A column's type, or a cast's.
fn type_of(e: &Expr, schema: &Schema) -> Option<DataType> {
    match e {
        Expr::Column(c) => Some(schema.field_with_name(&c.name).ok()?.data_type().clone()),
        Expr::Cast(c) => Some(c.field.data_type().clone()),
        Expr::TryCast(c) => Some(c.field.data_type().clone()),
        _ => None,
    }
}

/// Does a cast from `from` to `to` hold every value it is given, so it fails on none: an integer to a wider one (or a signed
/// one wider than an unsigned), to a float64 or to a decimal with room for its digits; a float to a float64; a decimal
/// to one with as many digits before the point and after; text to text with longer offsets; a dictionary to its values.
fn widens(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    // (an integer: signed, its bytes, its decimal digits)
    let int = |t: &DataType| match t {
        Int8 => Some((true, 1u8, 3i16)),
        Int16 => Some((true, 2, 5)),
        Int32 => Some((true, 4, 10)),
        Int64 => Some((true, 8, 19)),
        UInt8 => Some((false, 1, 3)),
        UInt16 => Some((false, 2, 5)),
        UInt32 => Some((false, 4, 10)),
        UInt64 => Some((false, 8, 20)),
        _ => None,
    };
    let decimal = |t: &DataType| match t {
        Decimal128(p, s) | Decimal256(p, s) => Some((*p as i16, *s as i16)),
        _ => None,
    };
    if from == to || from == &Null {
        return true;
    }
    match (from, to) {
        (Dictionary(_, values), to) => values.as_ref() == to,
        (Float16 | Float32, Float64) => true,
        (Utf8, LargeUtf8 | Utf8View) | (LargeUtf8 | Utf8View, LargeUtf8 | Utf8View) => true,
        (Date32, Date64) => true,
        _ => match (int(from), int(to), decimal(from), decimal(to)) {
            (Some((fs, fb, _)), Some((ts, tb, _)), ..) => (fs == ts && tb >= fb) || (!fs && ts && tb > fb),
            (Some(_), None, _, None) => to == &Float64,
            (Some((_, _, digits)), _, _, Some((p, s))) => p - s >= digits,
            (None, _, Some((p1, s1)), Some((p2, s2))) => s2 >= s1 && p2 - s2 >= p1 - s1,
            _ => false,
        },
    }
}

/// A table's scan under its reader's grant's filter on its rows (`scan`): the rows of `inner` that `filter` keeps, of its
/// columns at `kept`, at most `fetch` a partition. A leaf: `inner` is not its child, so the planner's rules reach neither
/// into it (a condition of the query pushed into the scan would be worked out on rows the filter hides, and could fail on
/// one, L1's change 1) nor its numbers (EXPLAIN ANALYZE shows this node's rows and time, never how many rows or files the
/// scan read or pruned; its statistics are unknown, so no count or bound is answered from the files'). EXPLAIN names the
/// table and says the grant filters it; it shows the filter's text only to a scope that sees code (`told`).
#[derive(Debug)]
pub struct GuardedExec {
    table: String,
    told: Option<String>,
    inner: Arc<dyn ExecutionPlan>,
    filter: Arc<dyn PhysicalExpr>,
    kept: Vec<usize>,
    fetch: Option<usize>,
    props: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl GuardedExec {
    fn new(table: String, told: Option<String>, inner: Arc<dyn ExecutionPlan>, filter: Arc<dyn PhysicalExpr>, kept: Vec<usize>, fetch: Option<usize>) -> Result<Self> {
        let schema = Arc::new(inner.schema().project(&kept)?);
        // (its partitions are the scan's, but nothing is said of their rows' order or keys: those are the scan's own)
        let partitions = Partitioning::UnknownPartitioning(inner.output_partitioning().partition_count());
        let props = Arc::new(PlanProperties::new(EquivalenceProperties::new(schema), partitions, inner.pipeline_behavior(), inner.boundedness()));
        Ok(GuardedExec { table, told, inner, filter, kept, fetch, props, metrics: ExecutionPlanMetricsSet::new() })
    }
}

impl DisplayAs for GuardedExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "GuardedExec: {}, row filter of your grant", self.table)?;
        if let Some(text) = &self.told {
            write!(f, ": {text}")?;
        }
        if let Some(fetch) = self.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for GuardedExec {
    fn name(&self) -> &str { "GuardedExec" }
    fn properties(&self) -> &Arc<PlanProperties> { &self.props }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
    fn fetch(&self) -> Option<usize> { self.fetch }
    fn metrics(&self) -> Option<MetricsSet> { Some(self.metrics.clone_inner()) }

    // (its filter is not offered to the planner's walks over a plan's expressions, as its scan is not)
    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> { Ok(TreeNodeRecursion::Continue) }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        match children.is_empty() {
            true => Ok(self),
            false => internal_err!("GuardedExec takes no child, not {}", children.len()),
        }
    }

    // (the scan's files spread over more partitions, as the planner asks of a scan, L1's change 7)
    fn repartitioned(&self, target: usize, config: &ConfigOptions) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(inner) = self.inner.repartitioned(target, config)? else { return Ok(None) };
        Ok(Some(Arc::new(GuardedExec::new(self.table.clone(), self.told.clone(), inner, self.filter.clone(), self.kept.clone(), self.fetch)?)))
    }

    fn execute(&self, partition: usize, task: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(Shown {
            input: self.inner.execute(partition, task)?,
            filter: self.filter.clone(),
            kept: self.kept.clone(),
            table: self.table.clone(),
            left: self.fetch,
            schema: self.schema(),
            rows: MetricBuilder::new(&self.metrics).output_rows(partition),
            time: MetricBuilder::new(&self.metrics).elapsed_compute(partition),
        }))
    }
}

/// One partition of a `GuardedExec`: its scan's batches, each cut to the rows its filter keeps and the columns asked for.
struct Shown {
    input: SendableRecordBatchStream,
    filter: Arc<dyn PhysicalExpr>,
    kept: Vec<usize>,
    table: String,
    left: Option<usize>,
    schema: SchemaRef,
    rows: Count,
    time: Time,
}

impl Shown {
    /// The rows of `batch` the filter keeps. Its error says nothing of the row it failed on (its words could quote a value
    /// the filter hides).
    fn keep(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let failed = || DataFusionError::Execution(format!("the filter on {}'s rows that your grant has could not be worked out for one of its rows", self.table));
        let mask = self.filter.evaluate(batch).and_then(|v| v.into_array(batch.num_rows())).map_err(|_| failed())?;
        let Some(mask) = mask.as_boolean_opt() else { return Err(failed()) };
        Ok(filter_record_batch(batch, mask)?.project(&self.kept)?)
    }
}

impl Stream for Shown {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.left == Some(0) {
                return Poll::Ready(None);
            }
            let batch = match futures::ready!(this.input.poll_next_unpin(cx)) {
                None => return Poll::Ready(None),
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                Some(Ok(b)) => b,
            };
            let timer = this.time.timer();
            let kept = this.keep(&batch);
            timer.done();
            let mut kept = match kept {
                Err(e) => return Poll::Ready(Some(Err(e))),
                Ok(b) if b.num_rows() == 0 => continue,
                Ok(b) => b,
            };
            if let Some(left) = &mut this.left {
                kept = kept.slice(0, kept.num_rows().min(*left));
                *left -= kept.num_rows();
            }
            this.rows.add(kept.num_rows());
            return Poll::Ready(Some(Ok(kept)));
        }
    }
}

impl RecordBatchStream for Shown {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use datafusion::prelude::{col, lit};

    #[test]
    fn a_condition_that_could_fail_on_a_hidden_row_is_not_leakproof() {
        let schema = Schema::new(vec![Field::new("n", DataType::Int32, true), Field::new("s", DataType::Utf8, true), Field::new("u", DataType::UInt32, true)]);
        let cast = |e: Expr, t: DataType| Expr::Cast(datafusion::logical_expr::Cast::new(Box::new(e), t));
        let try_cast = |e: Expr, t: DataType| Expr::TryCast(datafusion::logical_expr::TryCast::new(Box::new(e), t));
        let yes = [
            col("n").eq(lit(5)),
            col("n").gt(col("u")).and(col("s").is_null()),
            col("n").in_list(vec![lit(1), lit(2)], false).or(Expr::Not(Box::new(col("n").between(lit(1), lit(9))))),
            col("s").like(lit("ab%")),
            cast(col("n"), DataType::Int64).lt_eq(lit(7i64)),
            cast(col("u"), DataType::Int64).eq(lit(7i64)),
            cast(col("n"), DataType::Float64).eq(lit(7.5)),
            try_cast(col("s"), DataType::Int64).eq(lit(5i64)),
            cast(col("s"), DataType::Utf8View).eq(lit("x")),
            lit(1).eq(lit(1)),
        ];
        for e in &yes {
            assert!(leakproof(e, &schema), "{e} is leakproof");
        }
        let no = [
            (lit(1) / (col("n") - lit(5))).gt(lit(0)),
            cast(col("s"), DataType::Int64).eq(lit(5i64)),
            cast(col("n"), DataType::Int16).eq(lit(5i16)),
            cast(col("u"), DataType::Int32).eq(lit(5)),
            col("n").gt(lit(1)).and(cast(col("s"), DataType::Int32).eq(lit(1))),
            datafusion::functions::expr_fn::lower(col("s")).eq(lit("a")),
            datafusion::functions::expr_fn::lower(col("s")).like(lit("a%")),
            col("s").like(col("s")),
            cast(col("s"), DataType::Timestamp(TimeUnit::Microsecond, None)).is_null(),
            try_cast(lit(1) / col("n"), DataType::Int64).is_null(),
            (-col("n")).eq(lit(1)),
        ];
        for e in &no {
            assert!(!leakproof(e, &schema), "{e} could fail on a row");
        }
    }

    #[test]
    fn a_cast_widens_only_when_it_holds_every_value() {
        use DataType::*;
        for (from, to) in [(Int8, Int64), (UInt16, Int32), (UInt64, UInt64), (Int64, Float64), (Float32, Float64), (Int32, Decimal128(12, 2)), (Decimal128(5, 2), Decimal128(8, 3)), (Utf8, LargeUtf8), (Null, Int8), (Date32, Date64)] {
            assert!(widens(&from, &to), "{from} to {to} holds every value");
        }
        for (from, to) in [(Int64, Int32), (UInt32, Int32), (Int8, UInt64), (Float64, Float32), (Int64, Decimal128(12, 2)), (Decimal128(5, 2), Decimal128(5, 3)), (LargeUtf8, Utf8), (Utf8, Int64), (Int64, Utf8), (Float64, Int64)] {
            assert!(!widens(&from, &to), "{from} to {to} may fail on a value");
        }
    }
}
