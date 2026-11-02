//! An OR of ranges of one column is one range when they overlap or touch: `(q >= 1 AND q <= 11) OR (q >= 10 AND
//! q <= 20) OR (q >= 20 AND q <= 30)` is `q >= 1 AND q <= 30`, two comparisons a row instead of six, and no ORs.
//! DataFusion derives that OR for TPC-H q19's lineitem scan from the query's three ORed conditions, and works it out
//! on every row; one range takes q19 from 106 to 91 ms in-process at SF1, and TPC-DS q13, q48 and q85 have such ORs
//! too (`logs/round36/or-ranges-2026-11-02.txt`).
//!
//! A row's answer is the same: a value is in the ranges' union exactly when it is in the one range, and a NULL is
//! NULL either way. Only columns whose values order as the comparison kernels order them are merged (integers,
//! decimals, dates, times, durations and strings: not floats, whose -0 and NaN a kernel orders differently than a
//! value does), against bounds of the column's own type. `RangesOnce` rewrites the filters' predicates where they
//! will run, just before `optimize::AndsOnce`, which then ANDs the range's comparisons with the rest.

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::utils::{split_conjunction, split_disjunction};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use std::cmp::Ordering;
use std::sync::Arc;

/// Each filter's ORs of ranges of one column as one range, where they overlap or touch (the module's doc).
#[derive(Debug)]
pub struct RangesOnce;

impl PhysicalOptimizerRule for RangesOnce {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(f) = p.downcast_ref::<FilterExec>() else { return Ok(Transformed::no(p)) };
            let predicate = merged(f.predicate().clone(), &f.input().schema())?;
            if !predicate.transformed {
                return Ok(Transformed::no(p));
            }
            let new: Arc<dyn ExecutionPlan> = Arc::new(FilterExecBuilder::from(f).with_predicate(predicate.data).build()?);
            Ok(Transformed::yes(new))
        })
        .data()
    }

    fn name(&self) -> &str {
        "ranges_once"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `e` with each OR in it that is one range written as several, as that range.
fn merged(e: Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    e.transform_up(|e| {
        Ok(match one_range(&e, schema) {
            Some(range) => Transformed::yes(range),
            None => Transformed::no(e),
        })
    })
}

/// A range's bound: its value, and whether the value is in the range; None for no bound on that side.
type Bound = Option<(ScalarValue, bool)>;

/// `e`, an OR whose every part is a range of the same column, as one range, when the parts overlap or touch; None
/// for anything else.
fn one_range(e: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Option<Arc<dyn PhysicalExpr>> {
    if !e.downcast_ref::<BinaryExpr>().is_some_and(|b| *b.op() == Operator::Or) {
        return None;
    }
    let mut column: Option<Column> = None;
    let mut ranges: Vec<(Bound, Bound)> = vec![];
    for part in split_disjunction(e) {
        let (mut low, mut high): (Bound, Bound) = (None, None);
        for term in split_conjunction(part) {
            let (c, op, v) = compare(term)?;
            let t = c.data_type(schema).ok()?;
            if !ordered(&t) || v.data_type() != t || v.is_null() {
                return None;
            }
            match &column {
                Some(k) if k != &c => return None,
                Some(_) => {}
                None => column = Some(c),
            }
            match op {
                Operator::Gt | Operator::GtEq => low = tighter(low, (v, op == Operator::GtEq), Ordering::Greater)?,
                Operator::Lt | Operator::LtEq => high = tighter(high, (v, op == Operator::LtEq), Ordering::Less)?,
                Operator::Eq => {
                    low = tighter(low, (v.clone(), true), Ordering::Greater)?;
                    high = tighter(high, (v, true), Ordering::Less)?;
                }
                _ => return None,
            }
        }
        if let (Some(l), Some(h)) = (&low, &high) {
            match l.0.partial_cmp(&h.0)? {
                Ordering::Greater => return None, // (a part no value is in: left as written)
                Ordering::Equal if !(l.1 && h.1) => return None,
                _ => {}
            }
        }
        ranges.push((low, high));
    }
    ranges.sort_by(|a, b| lows(&a.0, &b.0));
    let mut parts = ranges.into_iter();
    let (low, mut high) = parts.next()?;
    for (l, h) in parts {
        if !reaches(&high, &l)? {
            return None; // (a gap: more than one range)
        }
        high = wider(high, h)?;
    }
    let column: Arc<dyn PhysicalExpr> = Arc::new(column?);
    let side = |(v, inside): (ScalarValue, bool), closed: Operator, open: Operator| -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(column.clone(), if inside { closed } else { open }, Arc::new(Literal::new(v))))
    };
    Some(match (low, high) {
        (None, None) => return None, // (every value: `IS NOT NULL`, which no one writes so)
        (Some(l), Some(h)) if l == h => side(l, Operator::Eq, Operator::Eq), // (both in: no part is empty)
        (Some(l), None) => side(l, Operator::GtEq, Operator::Gt),
        (None, Some(h)) => side(h, Operator::LtEq, Operator::Lt),
        (Some(l), Some(h)) => Arc::new(BinaryExpr::new(side(l, Operator::GtEq, Operator::Gt), Operator::And, side(h, Operator::LtEq, Operator::Lt))),
    })
}

/// `term` as a column compared with a value, the column on the left: `1 <= q` is `q >= 1`.
fn compare(term: &Arc<dyn PhysicalExpr>) -> Option<(Column, Operator, ScalarValue)> {
    let b = term.downcast_ref::<BinaryExpr>()?;
    if let (Some(c), Some(v)) = (b.left().downcast_ref::<Column>(), b.right().downcast_ref::<Literal>()) {
        return Some((c.clone(), *b.op(), v.value().clone()));
    }
    let (v, c) = (b.left().downcast_ref::<Literal>()?, b.right().downcast_ref::<Column>()?);
    let flipped = match b.op() {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        op => *op,
    };
    Some((c.clone(), flipped, v.value().clone()))
}

/// Whether a column's values order as the comparison kernels order them.
fn ordered(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..)
            | Date32 | Date64 | Time32(_) | Time64(_) | Timestamp(..) | Duration(_) | Utf8 | LargeUtf8 | Utf8View
    )
}

/// Of a bound and another on the same side, the tighter: the greater low (`side` Greater) or the lesser high.
fn tighter(bound: Bound, new: (ScalarValue, bool), side: Ordering) -> Option<Bound> {
    let Some(old) = bound else { return Some(Some(new)) };
    Some(Some(match new.0.partial_cmp(&old.0)? {
        Ordering::Equal => (old.0, old.1 && new.1),
        o if o == side => new,
        _ => old,
    }))
}

/// Two lows in order: none first, then by value, a low that is in its range before one that isn't.
fn lows(a: &Bound, b: &Bound) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal).then(b.1.cmp(&a.1)),
    }
}

/// Whether a range ending at `high` reaches one starting at `low`: they overlap, or touch at a value one of them has.
fn reaches(high: &Bound, low: &Bound) -> Option<bool> {
    let (Some(h), Some(l)) = (high, low) else { return Some(true) };
    Some(match l.0.partial_cmp(&h.0)? {
        Ordering::Less => true,
        Ordering::Equal => h.1 || l.1,
        Ordering::Greater => false,
    })
}

/// Of two highs, the wider: none (no bound) over any, else the greater, in if either has it.
fn wider(a: Bound, b: Bound) -> Option<Bound> {
    let (Some(x), Some(y)) = (&a, &b) else { return Some(None) };
    Some(match y.0.partial_cmp(&x.0)? {
        Ordering::Greater => b,
        Ordering::Less => a,
        Ordering::Equal => Some((x.0.clone(), x.1 || y.1)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::Field;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn schema() -> Schema {
        Schema::new(vec![Field::new("q", DataType::Int64, true), Field::new("r", DataType::Int64, true), Field::new("f", DataType::Float64, true)])
    }

    /// `c op v` for a column of `schema()`.
    fn cmp(c: &str, op: Operator, v: ScalarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(col(c, &schema()).unwrap(), op, lit(v)))
    }

    fn and(a: Arc<dyn PhysicalExpr>, b: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(a, Operator::And, b))
    }

    fn or(a: Arc<dyn PhysicalExpr>, b: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(a, Operator::Or, b))
    }

    /// `lo <= q <= hi`, written as q19 writes it.
    fn between(lo: i64, hi: i64) -> Arc<dyn PhysicalExpr> {
        and(cmp("q", Operator::GtEq, ScalarValue::Int64(Some(lo))), cmp("q", Operator::LtEq, ScalarValue::Int64(Some(hi))))
    }

    fn shown(e: Option<Arc<dyn PhysicalExpr>>) -> Option<String> {
        e.map(|e| e.to_string())
    }

    /// Overlapping and touching ranges, and a value in one, become one range; a gap, two columns, a float column, a
    /// NULL bound or a part no value is in leave the OR as written.
    #[test]
    fn ranges_that_meet_are_one() {
        let s = schema();
        let i = |v: i64| ScalarValue::Int64(Some(v));
        let q19 = or(or(between(1, 11), between(10, 20)), between(20, 30));
        assert_eq!(shown(one_range(&q19, &s)), Some("q@0 >= 1 AND q@0 <= 30".into()));
        let touching = or(and(cmp("q", Operator::GtEq, i(1)), cmp("q", Operator::Lt, i(5))), between(5, 9));
        assert_eq!(shown(one_range(&touching, &s)), Some("q@0 >= 1 AND q@0 <= 9".into()));
        let point = or(cmp("q", Operator::Eq, i(3)), between(1, 5));
        assert_eq!(shown(one_range(&point, &s)), Some("q@0 >= 1 AND q@0 <= 5".into()));
        let open = or(cmp("q", Operator::Gt, i(7)), between(1, 9));
        assert_eq!(shown(one_range(&open, &s)), Some("q@0 >= 1".into()));
        let flipped: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(lit(i(4)), Operator::GtEq, col("q", &s).unwrap()));
        assert_eq!(shown(one_range(&or(flipped, between(2, 6)), &s)), Some("q@0 <= 6".into()));
        let points = or(cmp("q", Operator::Eq, i(3)), cmp("q", Operator::Eq, i(3)));
        assert_eq!(shown(one_range(&points, &s)), Some("q@0 = 3".into()));
        assert_eq!(one_range(&or(between(1, 5), between(7, 9)), &s), None, "a gap");
        assert_eq!(one_range(&or(cmp("q", Operator::Lt, i(5)), cmp("q", Operator::Gt, i(5))), &s), None, "5 is in neither");
        assert_eq!(one_range(&or(cmp("q", Operator::LtEq, i(5)), cmp("q", Operator::Gt, i(5))), &s), None, "every value");
        let two_columns = or(between(1, 5), and(cmp("r", Operator::GtEq, i(3)), cmp("r", Operator::LtEq, i(9))));
        assert_eq!(one_range(&two_columns, &s), None);
        let f = |v: f64| ScalarValue::Float64(Some(v));
        assert_eq!(one_range(&or(cmp("f", Operator::LtEq, f(0.0)), cmp("f", Operator::GtEq, f(-0.0))), &s), None, "floats");
        assert_eq!(one_range(&or(between(5, 1), between(2, 3)), &s), None, "a part no value is in");
        assert_eq!(one_range(&or(cmp("q", Operator::GtEq, ScalarValue::Int64(None)), between(2, 3)), &s), None);
        assert_eq!(one_range(&between(1, 5), &s), None, "no OR");
    }

    /// In a query, the filter holds one range and keeps the rows the ORs did, NULLs and all.
    #[tokio::test]
    async fn a_query_keeps_its_rows() {
        let session = |rule: bool| {
            let rules = crate::optimize::physical_rules().into_iter().filter(|r| rule || r.name() != "ranges_once").collect();
            let state = SessionStateBuilder::new()
                .with_config(crate::optimize::config(SessionConfig::new().with_target_partitions(2)))
                .with_default_features()
                .with_analyzer_rules(crate::optimize::analyzer_rules())
                .with_optimizer_rules(crate::optimize::rules())
                .with_physical_optimizer_rules(rules)
                .build();
            let ctx = SessionContext::new_with_state(state);
            let q: Vec<Option<i64>> = (0..200).map(|i| if i % 17 == 0 { None } else { Some(i % 40) }).collect();
            let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(q)), Arc::new(Int64Array::from((0..200).collect::<Vec<i64>>())), Arc::new(Float64Array::from(vec![0.5; 200]))];
            ctx.register_batch("t", RecordBatch::try_new(Arc::new(schema()), columns).unwrap()).unwrap();
            ctx
        };
        let sql = "SELECT r FROM t WHERE r > 3 AND ((q >= 1 AND q <= 11) OR (q >= 10 AND q <= 20) OR (q >= 20 AND q <= 30)) ORDER BY r";
        let run = |ctx: SessionContext| async move {
            let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
            let text = displayable(plan.as_ref()).indent(true).to_string();
            let rows = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await.unwrap();
            (text, datafusion::arrow::util::pretty::pretty_format_batches(&rows).unwrap().to_string())
        };
        let ((text, rows), (plain_text, plain_rows)) = (run(session(true)).await, run(session(false)).await);
        assert!(text.contains("q@0 >= 1 AND q@0 <= 30"), "{text}");
        assert!(!text.contains(" OR "), "{text}");
        assert!(plain_text.contains(" OR "), "{plain_text}");
        assert_eq!(rows, plain_rows);
        assert!(rows.lines().count() > 100, "{rows}");
    }
}
