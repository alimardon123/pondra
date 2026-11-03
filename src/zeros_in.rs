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
//!
//! A float CASE's operand and NULLIF compared bits too (`random_sql.py`'s same run, L6): `CASE x WHEN 0.0 THEN …` missed
//! a -0.0, and NULLIF(x, 0) gave a -0.0 back where it meant NULL, so `y / NULLIF(x, 0)` divided by it. Over a float,
//! `CASE x WHEN a THEN r …` becomes `CASE WHEN x = a THEN r …`, what SQL defines it as, and NULLIF(x, y) becomes
//! `CASE WHEN x = y THEN NULL ELSE x END`: each `=` is DataFusion's, which makes the zeros one, and a value given back
//! keeps its sign. A volatile operand is left as it is, for the same reason.
//!
//! A MATCH_RECOGNIZE DEFINE that reads the match (FIRST, LAST, an aggregate) is an expression no plan holds, planned by
//! `matcher::glue_expr` without the optimizer's rules: it takes the same rewrite there (`alone`).

use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::expr::{Case, InList};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
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

/// `e` as `ZerosIn` writes it, its columns' types those of `schema`: for an expression no plan holds, a MATCH_RECOGNIZE
/// DEFINE that reads the match (`matcher::glue_expr`, 11-03 audit M1), after the simplifier as in a plan.
pub(crate) fn alone(e: Expr, schema: &DFSchema) -> Result<Expr> {
    use datafusion::common::tree_node::TreeNode;
    Ok(e.transform_up(|e| Ok(match split(&e, schema) {
        Some(new) => Transformed::yes(new),
        None => Transformed::no(e),
    }))?
    .data)
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
    match e {
        Expr::InList(i) => i.list.iter().any(zero) && !i.expr.is_volatile(),
        Expr::Case(Case { expr: Some(x), .. }) => !x.is_volatile(),
        Expr::ScalarFunction(f) => f.func.name() == "nullif" && f.args.len() == 2 && !f.args[0].is_volatile(),
        _ => false,
    }
}

fn float(e: &Expr, schema: &DFSchema) -> bool {
    e.get_type(schema).is_ok_and(|t| t.is_floating())
}

/// `x IN (…, 0.0, …)` as `x IN (…) OR x = 0.0`; NOT IN as `x NOT IN (…) AND x <> 0.0`. A float's `CASE x WHEN a` as `CASE
/// WHEN x = a`, and its NULLIF(x, y) as `CASE WHEN x = y THEN NULL ELSE x END`.
fn split(e: &Expr, schema: &DFSchema) -> Option<Expr> {
    if !looks(e) {
        return None;
    }
    let i = match e {
        Expr::InList(i) => i,
        Expr::Case(Case { expr: Some(x), when_then_expr, else_expr }) if float(x, schema) => {
            let whens = when_then_expr.iter().map(|(w, then)| (Box::new((**x).clone().eq((**w).clone())), then.clone())).collect();
            return Some(Expr::Case(Case::new(None, whens, else_expr.clone())));
        }
        Expr::ScalarFunction(f) if f.func.name() == "nullif" && float(&f.args[0], schema) => {
            let (x, y) = (f.args[0].clone(), f.args[1].clone());
            let t = x.get_type(schema).ok()?;
            let y = if y.get_type(schema).ok()? == t { y } else { datafusion::logical_expr::cast(y, t.clone()) };
            let null = Expr::Literal(ScalarValue::try_from(&t).ok()?, None); // (a NULL of x's type: the CASE's branches agree)
            return Some(Expr::Case(Case::new(None, vec![(Box::new(x.clone().eq(y)), Box::new(null))], Some(Box::new(x)))));
        }
        _ => return None,
    };
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

    /// A float CASE's operand and NULLIF take the two zeros as one, as `=` does (Postgres's answers, over -0.0, 0.0, 2.0,
    /// NULL and 1.5), keep the name they had, and give a value back with its sign.
    #[tokio::test]
    async fn a_case_and_nullif_take_the_zeros_as_one() {
        let (ctx, plain) = (session(true), session(false));
        let (t, f, n) = (Some(true), Some(false), None);
        let case = "CASE a WHEN 0.0 THEN true ELSE false END";
        assert_ne!(answers(&plain, case).await.1, vec![t, t, f, f, f], "DataFusion's answer, without the rule");
        for (p, want) in [
            (case, vec![t, t, f, f, f]),
            ("CASE -a WHEN 0.0 THEN true WHEN -2.0 THEN false END", vec![t, t, f, n, n]),
            ("CASE a WHEN CAST(i - i AS DOUBLE) THEN true ELSE false END", vec![t, t, f, f, f]),
            ("nullif(a, 0.0) IS NULL", vec![t, t, f, t, f]),
            ("nullif(0.0, a) IS NULL", vec![t, t, f, f, f]),
            ("nullif(a, 0) IS NULL", vec![t, t, f, t, f]),
            ("nullif(a, 2.0) IS NULL", vec![f, f, t, t, f]),
        ] {
            let (name, got) = answers(&ctx, p).await;
            assert_eq!(got, want, "{p}");
            assert_eq!(name, answers(&plain, p).await.0, "{p}: the column's name");
        }
        for sql in ["SELECT CAST(nullif(a, 2.0) AS VARCHAR) FROM t WHERE i = 1", "SELECT CAST(CASE a WHEN 0.0 THEN a END AS VARCHAR) FROM t WHERE i = 1"] {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let text = datafusion::arrow::util::display::array_value_to_string(batches[0].column(0), 0).unwrap();
            assert_eq!(text, "-0.0", "{sql}: the value as it was");
        }
    }
}
