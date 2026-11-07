//! A scalar subquery that reads the outer row and isn't aggregated: `(SELECT c.price FROM changes c WHERE c.id = p.id)`,
//! in a SELECT, a WHERE or an UPDATE's SET. Postgres and DuckDB take it, and raise 21000 only when a row's subquery gives
//! more than one row. DataFusion turns a scalar subquery that reads the outer row into a join on the columns it reads, which
//! takes one row for each of their values, so it refuses one it can't prove gives at most one ("must be aggregated"), and
//! can't run one with a LIMIT 1 ("not implemented").
//!
//! `OneRow` (an analyzer rule) makes each into one DataFusion runs:
//! - Not aggregated: the subquery gives, for each value it is joined on, its rows' count and the first one's value, as a
//!   struct, and `pondra_one_row` outside takes the value, or raises 21000 for a row whose value gave more than one. The
//!   check is outside, in the row that asks, because the join works out every value's count, and a value no outer row
//!   asks for (one the WHERE outside leaves out) must raise nothing.
//! - With `LIMIT 1` (and an ORDER BY or not): its first row in that order, `first_value(… ORDER BY …)`, which needs no
//!   check.
//!
//! A subquery that reads no outer row, or is aggregated, or gives at most one row by its shape, is DataFusion's as before.
//! So is one that compares the outer row by other than `=` (`c.day <= p.day`): DataFusion joins on equalities only, and
//! can't run such a subquery aggregated or not.
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::{is_null, nullif};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{exec_err, plan_err, Column, Result, ScalarValue};
use datafusion::functions_aggregate::expr_fn::{first_value, sum};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::logical_plan::{FetchType, SkipType};
use datafusion::logical_expr::{lit, ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Subquery, Volatility};
use datafusion::optimizer::analyzer::AnalyzerRule;
use std::collections::HashMap;
use std::sync::Arc;

/// What a row whose subquery gave more than one row says (21000 by these words, `codes::by_words`): Postgres's words, and
/// what to write instead.
pub const MANY: &str = "more than one row returned by a subquery used as an expression: its WHERE matched more than one row for one row outside it. Pick one: an aggregate (max(…)), or ORDER BY … LIMIT 1";

/// The analyzer rule. Runs before type coercion, so what it adds is typed as the planner's own is.
#[derive(Debug)]
pub struct OneRow;

impl AnalyzerRule for OneRow {
    fn name(&self) -> &str { "pondra_one_row" }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        // (most plans have no such subquery: they are handed back as they came, never rebuilt)
        let mut any = false;
        plan.apply_with_subqueries(|p| {
            p.apply_expressions(|e| {
                any = e.exists(|e| Ok(matches!(e, Expr::ScalarSubquery(s) if shape(s).is_some())))?;
                Ok(if any { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
            })?;
            Ok(if any { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
        })?;
        if !any {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(|plan| {
            let names = NamePreserver::new(&plan);
            plan.map_expressions(|e| {
                let name = names.save(&e);
                let e = e.transform_up(|e| match e {
                    Expr::ScalarSubquery(s) => match shape(&s) {
                        Some(Shape::Many) => Ok(Transformed::yes(counted(s)?)),
                        Some(Shape::First) => Ok(Transformed::yes(first(s)?)),
                        None => Ok(Transformed::no(Expr::ScalarSubquery(s))),
                    },
                    e => Ok(Transformed::no(e)),
                })?;
                Ok(e.update_data(|e| name.restore(e)))
            })
        })
        .data()
    }
}

/// What `OneRow` does with a scalar subquery.
enum Shape {
    /// It may give more than one row: counted, and checked outside.
    Many,
    /// It ends in `LIMIT 1` (under its projections): its first row, in its ORDER BY.
    First,
}

/// What `OneRow` does with `s`: None for one that reads no outer row, gives more than one column (DataFusion's error
/// says so), is aggregated (DataFusion's own, which takes it), or gives at most one row otherwise.
fn shape(s: &Subquery) -> Option<Shape> {
    if s.outer_ref_columns.is_empty() || s.subquery.schema().fields().len() != 1 {
        return None;
    }
    let mut p = s.subquery.as_ref();
    loop {
        p = match p {
            LogicalPlan::Projection(x) => x.input.as_ref(),
            LogicalPlan::SubqueryAlias(x) => x.input.as_ref(),
            LogicalPlan::Aggregate(_) => return None,
            LogicalPlan::Filter(f) if matches!(f.input.as_ref(), LogicalPlan::Aggregate(_)) => return None, // (a HAVING)
            LogicalPlan::Limit(l) if first_only(l) => return Some(Shape::First),
            _ if s.subquery.max_rows().is_some_and(|n| n <= 1) => return None,
            LogicalPlan::Limit(_) => return None, // (an OFFSET, or more rows than one: as DataFusion says)
            _ => return Some(Shape::Many),
        };
    }
}

/// Is `l` a `LIMIT 1` with no OFFSET.
fn first_only(l: &datafusion::logical_expr::Limit) -> bool {
    matches!(l.get_fetch_type(), Ok(FetchType::Literal(Some(1)))) && matches!(l.get_skip_type(), Ok(SkipType::Literal(0)))
}

/// What the count and the first value are called in the aggregate `counted` adds, and the struct of both over it.
const COUNT: &str = "__pondra_one_row_n";
const VALUE: &str = "__pondra_one_row_v";
const ONE: &str = "__pondra_one_row";

/// `s`, a subquery that may give more than one row, as one that gives its rows' count and first value, under
/// `pondra_one_row`: `(SELECT named_struct('n', sum(1), 'v', first_value(x)) FROM (s))`. `sum(1)` and not `count(*)`: no
/// row gives NULL, as the join gives a row outside nothing matched, and DataFusion's count of an empty group is a case
/// of its own. Either way `pondra_one_row` of it is NULL.
fn counted(s: Subquery) -> Result<Expr> {
    let inner = Arc::unwrap_or_clone(s.subquery);
    let (qualifier, field) = inner.schema().qualified_field(0);
    let value = Expr::Column(Column::from((qualifier, field)));
    let plan = LogicalPlanBuilder::from(inner)
        .aggregate(Vec::<Expr>::new(), vec![sum(lit(1i64)).alias(COUNT), first_value(value, vec![]).alias(VALUE)])?
        .project(vec![datafusion::functions::core::expr_fn::named_struct(vec![lit("n"), Expr::Column(Column::from_name(COUNT)), lit("v"), Expr::Column(Column::from_name(VALUE))]).alias(ONE)])? // (a name of its own, as `first`'s)
        .build()?;
    let counted = Expr::ScalarSubquery(Subquery { subquery: Arc::new(plan), outer_ref_columns: s.outer_ref_columns, spans: s.spans });
    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(udf(), vec![counted])))
}

/// `s`, a subquery whose projections stand over a `LIMIT 1`, as one row: `first_value(e ORDER BY …)` over what the
/// limit's ORDER BY sorts (or over what the limit takes, in no order, when there is none), where `e` is the subquery's
/// column written in that input's columns, through every projection and alias between. DataFusion plans `ORDER BY t
/// LIMIT 1` with the columns the order needs projected under the sort and the select list's over it. The one row's
/// column has a name of its own: the join DataFusion makes groups it by the columns it reads from the outer row, and a
/// column of the same name beside them would be one twice.
fn first(s: Subquery) -> Result<Expr> {
    let (qualifier, field) = s.subquery.schema().qualified_field(0);
    let mut e = Expr::Column(Column::from((qualifier, field)));
    let (mut p, mut limited) = (s.subquery.as_ref().clone(), false);
    let (below, order) = loop {
        p = match p {
            LogicalPlan::Projection(x) => {
                let by: HashMap<Column, Expr> = x.schema.columns().into_iter().zip(x.expr.iter().map(|e| e.clone().unalias())).collect();
                e = written_in(e, &by)?;
                Arc::unwrap_or_clone(x.input)
            }
            LogicalPlan::SubqueryAlias(x) => {
                let by: HashMap<Column, Expr> = x.schema.columns().into_iter().zip(x.input.schema().columns().into_iter().map(Expr::Column)).collect();
                e = written_in(e, &by)?;
                Arc::unwrap_or_clone(x.input)
            }
            LogicalPlan::Limit(l) if !limited && first_only(&l) => {
                limited = true;
                Arc::unwrap_or_clone(l.input)
            }
            LogicalPlan::Sort(sort) if limited && sort.fetch.is_none_or(|n| n >= 1) => break (Arc::unwrap_or_clone(sort.input), sort.expr),
            p if limited => break (p, vec![]),
            _ => return Ok(Expr::ScalarSubquery(s)), // (no LIMIT 1 under its projections: as `shape` found none, never)
        };
    };
    let one = LogicalPlanBuilder::from(below).aggregate(Vec::<Expr>::new(), vec![first_value(e, order).alias(FIRST)])?.build()?;
    Ok(Expr::ScalarSubquery(Subquery { subquery: Arc::new(one), outer_ref_columns: s.outer_ref_columns, spans: s.spans }))
}

/// What the one row of a `LIMIT 1` calls its column.
const FIRST: &str = "__pondra_one_row_first";

/// `e` with each column `by` names written as what `by` gives for it: an expression over a projection's (or an alias's)
/// output, written over its input. Bottom up, so what replaces a column is not read again.
fn written_in(e: Expr, by: &HashMap<Column, Expr>) -> Result<Expr> {
    e.transform_up(|e| match &e {
        Expr::Column(c) => Ok(by.get(c).map_or(Transformed::no(e.clone()), |w| Transformed::yes(w.clone()))),
        _ => Ok(Transformed::no(e)),
    })
    .data()
}

/// `pondra_one_row`, built for each plan that needs it (one per session as well, for plans that name it: `register`).
fn udf() -> Arc<ScalarUDF> { Arc::new(ScalarUDF::new_from_impl(Pick { signature: Signature::any(1, Volatility::Immutable) })) }

/// Registers `pondra_one_row` in a session (`engine.rs`), for plans that name it.
pub fn register(ctx: &datafusion::prelude::SessionContext) { ctx.register_udf(udf().as_ref().clone()); }

/// `pondra_one_row(s)`: `s`'s value, a struct of a subquery's count and first value (`counted`); NULL when it is NULL (no
/// row matched); 21000 when its count is more than one.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Pick {
    signature: Signature,
}

impl ScalarUDFImpl for Pick {
    fn name(&self) -> &str { "pondra_one_row" }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match &args[0] {
            DataType::Struct(fields) if fields.len() == 2 => Ok(fields[1].data_type().clone()),
            t => plan_err!("pondra_one_row takes a subquery's count and value, not {t}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = args.args[0].to_array(args.number_rows)?;
        let s = a.as_struct();
        let n = s.column(0).as_primitive::<Int64Type>();
        if (0..s.len()).any(|i| s.is_valid(i) && n.is_valid(i) && n.value(i) > 1) {
            return exec_err!("{MANY}");
        }
        let v = match s.null_count() {
            0 => s.column(1).clone(),
            _ => nullif(s.column(1), &is_null(&a)?)?,
        };
        Ok(match scalar {
            true => ColumnarValue::Scalar(ScalarValue::try_from_array(&v, 0)?),
            false => ColumnarValue::Array(v),
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    /// A session as a node's (`engine::session`), with `products` (1, 2, 3, 4) and `changes`: product 1 changed once, 2
    /// twice (20 on day 1, 21 on day 2), 3 never, and 4 three times (41, 42 and 43 on days 1, 3 and 2).
    async fn session() -> SessionContext {
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        for (name, sql) in [
            ("products", "SELECT * FROM (VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'a'), (4, 40, 'b')) AS x(product_id, list_price, cat)"),
            ("changes", "SELECT * FROM (VALUES (1, 11, 1), (2, 20, 1), (2, 21, 2), (4, 41, 1), (4, 42, 3), (4, 43, 2)) AS y(product_id, list_price, day)"),
        ] {
            ctx.sql(&format!("CREATE TABLE {name} AS {sql}")).await.unwrap().collect().await.unwrap(); // (a table scanned, as a node's are)
        }
        ctx
    }

    /// The rows of `sql`, each as its columns' text joined by commas, sorted; or the error's words.
    async fn rows(ctx: &SessionContext, sql: &str) -> Result<Vec<String>, String> {
        let batches = async { ctx.sql(sql).await?.collect().await }.await.map_err(|e| crate::codes::words(&e.into()))?;
        let mut out = vec![];
        for b in batches {
            for r in 0..b.num_rows() {
                out.push(b.columns().iter().map(|c| array_value_to_string(c, r).unwrap()).collect::<Vec<_>>().join(","));
            }
        }
        out.sort();
        Ok(out)
    }

    fn want(rows: &[&str]) -> Result<Vec<String>, String> { Ok(rows.iter().map(|s| s.to_string()).collect()) }

    /// A subquery that reads the outer row and isn't aggregated answers each row's one value, NULL where none matched,
    /// in a select list, a WHERE and inside an expression; a value the outer WHERE leaves out may match many and raises
    /// nothing.
    #[tokio::test]
    async fn a_subquery_of_the_outer_row_gives_its_one_row() {
        let ctx = session().await;
        let one = "(SELECT c.list_price FROM changes c WHERE c.product_id = p.product_id)";
        assert_eq!(rows(&ctx, &format!("SELECT p.product_id, {one} AS np FROM products p WHERE p.product_id IN (1, 3)")).await, want(&["1,11", "3,"]));
        assert_eq!(rows(&ctx, &format!("SELECT p.product_id FROM products p WHERE p.product_id IN (1, 3) AND {one} > 10")).await, want(&["1"]));
        assert_eq!(rows(&ctx, &format!("SELECT p.product_id, coalesce({one}, p.list_price) + 1 AS np FROM products p WHERE p.product_id < 2 OR p.product_id = 3")).await, want(&["1,12", "3,31"]));
        // (a second column of the outer row, in an expression, and the subquery's own expression)
        let two = "(SELECT c.list_price * 2 FROM changes c WHERE c.product_id = p.product_id AND c.list_price = p.list_price + 1)";
        assert_eq!(rows(&ctx, &format!("SELECT p.product_id, {two} AS d FROM products p")).await, want(&["1,22", "2,42", "3,", "4,82"]));
        assert_eq!(rows(&ctx, "SELECT p.product_id, (SELECT c.product_id FROM changes c WHERE c.product_id = p.product_id) AS v FROM products p WHERE p.product_id IN (1, 3)").await, want(&["1,1", "3,"]));
        // (DataFusion's own forms, as before: aggregated, and reading no outer row)
        assert_eq!(rows(&ctx, "SELECT p.product_id, (SELECT max(c.list_price) FROM changes c WHERE c.product_id = p.product_id) AS m FROM products p").await, want(&["1,11", "2,21", "3,", "4,43"]));
        assert_eq!(rows(&ctx, "SELECT (SELECT c.list_price FROM changes c WHERE c.product_id = 1) AS v").await, want(&["11"]));
    }

    /// A row whose subquery gives more than one row raises 21000 in Postgres's words, whether it reads the outer row or not.
    #[tokio::test]
    async fn more_than_one_row_is_21000() {
        let ctx = session().await;
        for sql in [
            "SELECT p.product_id, (SELECT c.list_price FROM changes c WHERE c.product_id = p.product_id) FROM products p",
            "SELECT p.product_id FROM products p WHERE (SELECT c.list_price FROM changes c WHERE c.product_id = p.product_id) > 0",
            "SELECT (SELECT c.list_price FROM changes c) AS v",
        ] {
            let e = rows(&ctx, sql).await.unwrap_err();
            assert!(e.starts_with("more than one row returned by a subquery used as an expression"), "{sql}: {e}");
            assert_eq!(crate::codes::of(&anyhow::anyhow!(e.clone())), "21000", "{sql}: {e}");
        }
    }

    /// `LIMIT 1` in a subquery of the outer row: its first row in the ORDER BY (on a column the select list has or not, in
    /// either direction), or its one row with no ORDER BY.
    #[tokio::test]
    async fn limit_1_gives_the_first_row_in_its_order() {
        let ctx = session().await;
        let latest = |order: &str| format!("SELECT p.product_id, (SELECT c.list_price FROM changes c WHERE c.product_id = p.product_id ORDER BY {order} LIMIT 1) AS v FROM products p");
        assert_eq!(rows(&ctx, &latest("c.day DESC")).await, want(&["1,11", "2,21", "3,", "4,42"]));
        assert_eq!(rows(&ctx, &latest("c.day")).await, want(&["1,11", "2,20", "3,", "4,41"]));
        assert_eq!(rows(&ctx, &latest("c.list_price DESC")).await, want(&["1,11", "2,21", "3,", "4,43"]));
        assert_eq!(rows(&ctx, "SELECT p.product_id, (SELECT c.list_price FROM changes c WHERE c.product_id = p.product_id LIMIT 1) AS v FROM products p WHERE p.product_id IN (1, 3)").await, want(&["1,11", "3,"]));
        assert_eq!(rows(&ctx, "SELECT p.product_id FROM products p WHERE (SELECT c.day FROM changes c WHERE c.product_id = p.product_id ORDER BY c.list_price DESC LIMIT 1) = 2").await, want(&["2", "4"]));
        // (the column it is joined on, given back, and an expression over two of its columns)
        assert_eq!(rows(&ctx, "SELECT p.product_id, (SELECT c.product_id FROM changes c WHERE c.product_id = p.product_id ORDER BY c.day LIMIT 1) AS v FROM products p").await, want(&["1,1", "2,2", "3,", "4,4"]));
        assert_eq!(rows(&ctx, "SELECT p.product_id, (SELECT c.list_price * 10 + c.day FROM changes c WHERE c.product_id = p.product_id ORDER BY c.day DESC LIMIT 1) AS v FROM products p").await, want(&["1,111", "2,212", "3,", "4,423"]));
    }
}
