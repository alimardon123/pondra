//! A float's IN list answers as `=` does: `-0.0 IN (2.0, 0.0)` is true, as `-0.0 = 0.0` is, in Postgres and DuckDB.
//!
//! DataFusion's `=` and the other comparisons make the two zeros one before comparing (`datum::apply_cmp`), but the
//! set an IN list of floats is checked against compares values by their bits (`in_list::primitive_filter`,
//! `branchless_filter`), and there -0.0 and 0.0 differ. `random_sql.py --seed 20261102` found it:
//! `WHERE (-(t2.c0 + t2.c1)) IN (2.0, 0.0)` kept no row where `c0 + c1` was 0.0, while DuckDB kept 3. The simplifier
//! makes ORed `=`s of a column one IN list too (`a = 0.0 OR a = 2.0`), so those answered wrong as well.
//!
//! `ZerosIn` runs after the simplifier and takes the zero out of each float IN list as an `=` of its own:
//! `x IN (2.0, 0.0)` becomes `x IN (2.0) OR x = 0.0`, and `x NOT IN (2.0, 0.0)` becomes `x NOT IN (2.0) AND x <> 0.0`.
//! SQL's three values answer these alike, since an IN list is the OR of its `=`s, NULLs included. One `=` covers
//! both zeros. A list with no zero is left as it is: the only floats with different bits that `=` calls equal are
//! the two zeros. A list whose value is volatile (`random()`) is also left as it is, since the value would be worked
//! out twice.

use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

/// Takes a float IN list's zero out as an `=` (the module's doc).
#[derive(Debug)]
pub struct ZerosIn;

impl OptimizerRule for ZerosIn {
    fn name(&self) -> &str {
        "pondra_zeros_in"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        crate::optimize::rewrite_typed(plan, looks, split)
    }
}

/// A float literal 0.0 or -0.0 (the list's values have the list's type once types are coerced).
fn zero(e: &Expr) -> bool {
    match e {
        Expr::Literal(ScalarValue::Float64(Some(f)), _) => *f == 0.0,
        Expr::Literal(ScalarValue::Float32(Some(f)), _) => *f == 0.0,
        Expr::Literal(ScalarValue::Float16(Some(f)), _) => f.to_f32() == 0.0,
        _ => false,
    }
}

fn looks(e: &Expr) -> bool {
    matches!(e, Expr::InList(i) if i.list.iter().any(zero) && !i.expr.is_volatile())
}

/// `x IN (…, 0.0, …)` as `x IN (…) OR x = 0.0`; NOT IN as `x NOT IN (…) AND x <> 0.0`.
fn split(e: &Expr, _: &DFSchema) -> Option<Expr> {
    let Expr::InList(i) = e else { return None };
    if !looks(e) {
        return None;
    }
    let (zeros, rest): (Vec<&Expr>, Vec<&Expr>) = i.list.iter().partition(|l| zero(l));
    let x = (*i.expr).clone();
    let one = if i.negated { x.not_eq(zeros[0].clone()) } else { x.eq(zeros[0].clone()) };
    if rest.is_empty() {
        return Some(one);
    }
    let rest = Expr::InList(InList::new(i.expr.clone(), rest.into_iter().cloned().collect(), i.negated));
    Some(if i.negated { rest.and(one) } else { rest.or(one) })
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    fn session(with: bool) -> SessionContext {
        let rules = crate::optimize::rules().into_iter().filter(|r| with || r.name() != "pondra_zeros_in").collect();
        let state = SessionStateBuilder::new()
            .with_config(crate::optimize::config(SessionConfig::new()))
            .with_default_features()
            .with_analyzer_rules(crate::optimize::analyzer_rules())
            .with_optimizer_rules(rules)
            .with_physical_optimizer_rules(crate::optimize::physical_rules())
            .build();
        let ctx = SessionContext::new_with_state(state);
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false), Field::new("a", DataType::Float64, true)]));
        let a = Float64Array::from(vec![Some(-0.0), Some(0.0), Some(2.0), None, Some(1.5)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef, Arc::new(a)]).unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    async fn answers(ctx: &SessionContext, p: &str) -> (String, Vec<Option<bool>>) {
        let batches = ctx.sql(&format!("SELECT {p} FROM t ORDER BY i")).await.unwrap().collect().await.unwrap();
        let name = batches[0].schema().field(0).name().clone();
        let mut out = vec![];
        for b in &batches {
            let c = b.column(0).as_boolean();
            out.extend((0..c.len()).map(|r| c.is_valid(r).then(|| c.value(r))));
        }
        (name, out)
    }

    /// Each IN list answers as its `=`s do, Postgres's answers, over -0.0, 0.0, 2.0, NULL and 1.5; each keeps the
    /// column name it had.
    #[tokio::test]
    async fn a_zero_is_in_a_list_holding_the_other_zero() {
        let (ctx, plain) = (session(true), session(false));
        let (t, f, n) = (Some(true), Some(false), None);
        for (p, want) in [
            ("a IN (2.0, 0.0)", vec![t, t, t, n, f]),
            ("a NOT IN (2.0, 0.0)", vec![f, f, f, n, t]),
            ("-a IN (2.0, 0.0)", vec![t, t, f, n, f]),
            ("a IN (0.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0)", vec![t, t, f, n, f]),
            ("a = 0.0 OR a = 2.0", vec![t, t, t, n, f]),
            ("a IN (-0.0, 1.5)", vec![t, t, f, n, t]),
            ("a IN (0.0)", vec![t, t, f, n, f]),
            ("a NOT IN (0.0, NULL)", vec![f, f, n, n, n]),
            ("a IN (2.0, 1.5)", vec![f, f, t, n, t]),
        ] {
            let (name, got) = answers(&ctx, p).await;
            assert_eq!(got, want, "{p}");
            assert_eq!(name, answers(&plain, p).await.0, "{p}: the column's name");
        }
        let kept = ctx.sql("SELECT count(*) AS n FROM t WHERE -a IN (2.0, 0.0)").await.unwrap().collect().await.unwrap();
        assert_eq!(kept[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0), 2, "a filter keeps both zeros");
    }
}
