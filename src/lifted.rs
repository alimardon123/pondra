//! A subquery that reads the outer row, inside an aggregate, a window function or an ORDER BY: `sum(coalesce((SELECT
//! c.price FROM c WHERE c.id = p.id), 0))`, a GROUP BY key, an aggregate's FILTER, a window's PARTITION BY or ORDER BY,
//! `ORDER BY (SELECT …)`. Postgres and DuckDB take each. DataFusion joins such a subquery in a projection or a filter, and
//! an EXISTS or IN one in an aggregate too, but a scalar one in an aggregate is left for the physical planner, which
//! can't run it ("does not support logical expression ScalarSubquery"), and one in a window or a sort is refused ("can
//! only be used in Projection, Filter, Aggregate plan nodes") or left too.
//!
//! `Lifted` (an analyzer rule) works each one out in a projection of its own under the node, which keeps every column of
//! what the node reads and adds the subquery's value as a column, and reads that column where the subquery was. What the
//! node gives is named as before, so nothing over it changes: an aggregate's expressions keep their names, and over a
//! window or a sort a projection gives its columns, without the ones added. DataFusion then joins the subquery in the
//! projection as it joins any other, and `one_row::OneRow` (the rule after this one) makes one that isn't aggregated
//! give its one row.
//!
//! The same holds for an EXISTS, an IN or an `ANY`/`ALL` subquery in a window or a sort, read or not the outer row
//! (`ORDER BY p.id IN (SELECT id FROM c)` is refused, and `PARTITION BY EXISTS (…)` left for the physical planner).
//!
//! A scalar subquery that reads no outer row is DataFusion's, as before, wherever it is: it runs once. So is one in a
//! join's ON, and one that compares the outer row by other than `=` (`c.day < p.day`), which DataFusion can't join
//! anywhere.
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, Result};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::logical_plan::{Aggregate, Projection, Sort, Window};
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr};
use datafusion::optimizer::analyzer::AnalyzerRule;
use std::sync::Arc;

/// The analyzer rule. Runs before `OneRow` and type coercion, so what it moves is typed and made one row as before.
#[derive(Debug)]
pub struct Lifted;

impl AnalyzerRule for Lifted {
    fn name(&self) -> &str { "pondra_lifted" }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        // (most plans have no such subquery: they are handed back as they came, never rebuilt)
        let mut any = false;
        plan.apply_with_subqueries(|p| {
            any = lifts(p)?;
            Ok(if any { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
        })?;
        if !any {
            return Ok(plan);
        }
        let mut n = 0;
        plan.transform_up_with_subqueries(|plan| {
            if !lifts(&plan)? {
                return Ok(Transformed::no(plan));
            }
            let (scalar_only, mut lifted) = (matches!(plan, LogicalPlan::Aggregate(_)), vec![]);
            let mut lift = |e: Expr| -> Result<Expr> {
                e.transform_up(|e| {
                    if !correlated(&e, scalar_only) {
                        return Ok(Transformed::no(e));
                    }
                    let column = format!("__pondra_lifted_{n}");
                    n += 1;
                    lifted.push(e.alias(&column));
                    Ok(Transformed::yes(Expr::Column(Column::from_name(column))))
                })
                .data()
            };
            let names = NamePreserver::new_for_projection();
            let mut named = |exprs: Vec<Expr>| -> Result<Vec<Expr>> {
                exprs.into_iter().map(|e| {
                    let name = names.save(&e);
                    Ok(name.restore(lift(e)?))
                })
                .collect()
            };
            Ok(Transformed::yes(match plan {
                LogicalPlan::Aggregate(a) => {
                    let (group, aggr) = (named(a.group_expr)?, named(a.aggr_expr)?);
                    LogicalPlan::Aggregate(Aggregate::try_new(below(&a.input, lifted)?, group, aggr)?)
                }
                LogicalPlan::Window(w) => {
                    let window = named(w.window_expr)?;
                    over(&w.schema, LogicalPlan::Window(Window::try_new(window, below(&w.input, lifted)?)?))?
                }
                LogicalPlan::Sort(s) => {
                    let exprs = s.expr.into_iter().map(|o| Ok(SortExpr { expr: lift(o.expr)?, ..o })).collect::<Result<Vec<_>>>()?;
                    over(s.input.schema(), LogicalPlan::Sort(Sort { expr: exprs, input: below(&s.input, lifted)?, fetch: s.fetch }))?
                }
                plan => plan, // (`lifts` says which)
            }))
        })
        .data()
    }
}

/// Does `plan` hold a subquery of the outer row where DataFusion can't join it: an aggregate (not one of GROUPING SETS,
/// CUBE or ROLLUP, whose keys are DataFusion's as before), a window or a sort.
fn lifts(plan: &LogicalPlan) -> Result<bool> {
    let (exprs, scalar_only) = match plan {
        LogicalPlan::Aggregate(a) if !a.group_expr.iter().any(|e| matches!(e, Expr::GroupingSet(_))) => (a.group_expr.iter().chain(&a.aggr_expr).collect::<Vec<_>>(), true),
        LogicalPlan::Window(w) => (w.window_expr.iter().collect(), false),
        LogicalPlan::Sort(s) => (s.expr.iter().map(|o| &o.expr).collect(), false),
        _ => return Ok(false),
    };
    for e in exprs {
        if e.exists(|e| Ok(correlated(e, scalar_only)))? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Is `e` one `Lifted` works out under its node: a scalar subquery that reads the outer row, and, in a window or a sort
/// (`!scalar_only`), any EXISTS, IN or `ANY`/`ALL`, which DataFusion runs in a projection, a filter and an aggregate only.
fn correlated(e: &Expr, scalar_only: bool) -> bool {
    match e {
        Expr::ScalarSubquery(s) => !s.outer_ref_columns.is_empty(),
        Expr::Exists(_) | Expr::InSubquery(_) | Expr::SetComparison(_) => !scalar_only,
        _ => false,
    }
}

/// What the node reads, with each lifted subquery as a column after its own: a projection of every column of `input`.
fn below(input: &Arc<LogicalPlan>, lifted: Vec<Expr>) -> Result<Arc<LogicalPlan>> {
    let exprs = input.schema().columns().into_iter().map(Expr::Column).chain(lifted).collect();
    Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(exprs, input.clone())?)))
}

/// `node` giving the columns of `schema`, the node's own before the lifted ones were added.
fn over(schema: &datafusion::common::DFSchemaRef, node: LogicalPlan) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Projection(Projection::try_new(schema.columns().into_iter().map(Expr::Column).collect(), Arc::new(node))?))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    /// A session as a node's (`engine::session`), with `p` (1, 10), (2, 20), (3, 30) and `c` (1, 11), (3, 33): each `p`
    /// row's price, and a changed price for 1 and 3; and `twice`, where 1 changed twice.
    async fn session() -> SessionContext {
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        for (name, sql) in [
            ("p", "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS x(id, price)"),
            ("c", "SELECT * FROM (VALUES (1, 11), (3, 33)) AS y(id, price)"),
            ("twice", "SELECT * FROM (VALUES (1, 11), (1, 12)) AS z(id, price)"),
        ] {
            ctx.sql(&format!("CREATE TABLE {name} AS {sql}")).await.unwrap().collect().await.unwrap(); // (a table scanned, as a node's are)
        }
        ctx
    }

    /// The rows of `sql`, each as its columns' text joined by commas, in its order; or the error's words.
    async fn rows(ctx: &SessionContext, sql: &str) -> Result<Vec<String>, String> {
        let batches = async { ctx.sql(sql).await?.collect().await }.await.map_err(|e| crate::codes::words(&e.into()))?;
        let mut out = vec![];
        for b in batches {
            for r in 0..b.num_rows() {
                out.push(b.columns().iter().map(|c| array_value_to_string(c, r).unwrap()).collect::<Vec<_>>().join(","));
            }
        }
        Ok(out)
    }

    fn want(rows: &[&str]) -> Result<Vec<String>, String> { Ok(rows.iter().map(|s| s.to_string()).collect()) }

    const CHANGED: &str = "(SELECT c.price FROM c WHERE c.id = p.id)";

    /// A subquery of the outer row in an aggregate's argument, its FILTER, a GROUP BY key and a window's ORDER BY answers
    /// as Postgres does, and each output column keeps its name.
    #[tokio::test]
    async fn a_subquery_of_the_outer_row_runs_in_an_aggregate_and_a_window() {
        let ctx = session().await;
        assert_eq!(rows(&ctx, &format!("SELECT sum(coalesce({CHANGED}, 0)) AS s FROM p")).await, want(&["44"]));
        assert_eq!(rows(&ctx, &format!("SELECT sum(coalesce({CHANGED}, p.price)) AS s, count({CHANGED}) AS n FROM p")).await, want(&["64,2"]));
        assert_eq!(rows(&ctx, &format!("SELECT p.id % 2 AS g, max({CHANGED}) AS m FROM p GROUP BY 1 ORDER BY 1")).await, want(&["0,", "1,33"]));
        assert_eq!(rows(&ctx, &format!("SELECT count(*) FILTER (WHERE {CHANGED} > 20) AS n FROM p")).await, want(&["1"]));
        assert_eq!(rows(&ctx, &format!("SELECT {CHANGED} AS k, count(*) AS n FROM p GROUP BY 1 ORDER BY 1 NULLS FIRST")).await, want(&[",1", "11,1", "33,1"]));
        assert_eq!(rows(&ctx, &format!("SELECT p.id, sum(p.price) OVER (ORDER BY {CHANGED} NULLS FIRST, p.id) AS w FROM p ORDER BY p.id")).await, want(&["1,30", "2,20", "3,60"]));
        assert_eq!(rows(&ctx, &format!("SELECT p.id, first_value(p.id) OVER (PARTITION BY {CHANGED} IS NULL ORDER BY p.id) AS f FROM p ORDER BY p.id")).await, want(&["1,1", "2,2", "3,1"]));
        // (an ORDER BY, and an EXISTS and an IN in a window, which DataFusion doesn't join there either)
        assert_eq!(rows(&ctx, &format!("SELECT p.id FROM p ORDER BY {CHANGED} NULLS FIRST, p.id")).await, want(&["2", "1", "3"]));
        assert_eq!(rows(&ctx, &format!("SELECT p.id FROM p ORDER BY {CHANGED} DESC NULLS LAST LIMIT 2")).await, want(&["3", "1"]));
        assert_eq!(rows(&ctx, "SELECT p.id, row_number() OVER (PARTITION BY EXISTS (SELECT 1 FROM c WHERE c.id = p.id) ORDER BY p.id) AS r FROM p ORDER BY 1").await, want(&["1,1", "2,1", "3,2"]));
        assert_eq!(rows(&ctx, "SELECT p.id, count(*) OVER (PARTITION BY p.id IN (SELECT c.id FROM c WHERE c.price > p.price)) AS n FROM p ORDER BY 1").await, want(&["1,2", "2,1", "3,2"]));
        // (and ones that read no outer row: an EXISTS in a window, an IN and an ANY in an ORDER BY)
        assert_eq!(rows(&ctx, "SELECT p.id, row_number() OVER (PARTITION BY EXISTS (SELECT 1 FROM c WHERE c.price > 30) ORDER BY p.id DESC) AS r FROM p ORDER BY 1").await, want(&["1,3", "2,2", "3,1"]));
        assert_eq!(rows(&ctx, "SELECT p.id FROM p ORDER BY p.id IN (SELECT id FROM c), p.id").await, want(&["2", "1", "3"]));
        assert_eq!(rows(&ctx, "SELECT p.id FROM p ORDER BY p.price > ANY (SELECT price FROM c) DESC, p.id").await, want(&["2", "3", "1"]));
        // (DISTINCT, and the subquery's value under HAVING)
        assert_eq!(rows(&ctx, &format!("SELECT count(DISTINCT coalesce({CHANGED}, 0) > 0) AS n FROM p")).await, want(&["2"]));
        assert_eq!(rows(&ctx, &format!("SELECT p.id % 2 AS g FROM p GROUP BY 1 HAVING sum(coalesce({CHANGED}, 0)) > 40 ORDER BY 1")).await, want(&["1"]));
        // (the names a client reads, with no alias: the ones DataFusion's planner gave the query, before the rule moved
        // anything, and none of the columns added)
        for sql in [format!("SELECT p.id, sum(p.price) OVER (ORDER BY {CHANGED}) FROM p"), format!("SELECT sum(coalesce({CHANGED}, 0)), p.id % 2 FROM p GROUP BY 2")] {
            let df = ctx.sql(&sql).await.unwrap();
            let planned = df.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>();
            let ran = df.collect().await.unwrap().first().map(|b| b.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()).unwrap_or_default();
            assert!(planned == ran && ran.len() == 2 && ran.iter().all(|n| !n.contains("__pondra")), "{sql}: planned {planned:?}, ran as {ran:?}");
        }
    }

    /// A subquery that gives more than one row for a row still raises 21000, and one that reads no outer row, or is in a
    /// projection, is DataFusion's as before.
    #[tokio::test]
    async fn the_rest_is_as_before() {
        let ctx = session().await;
        let e = rows(&ctx, "SELECT sum((SELECT t.price FROM twice t WHERE t.id = p.id)) AS s FROM p").await.unwrap_err();
        assert!(e.starts_with("more than one row returned by a subquery used as an expression"), "{e}");
        assert_eq!(rows(&ctx, "SELECT sum((SELECT max(c.price) FROM c)) AS s FROM p").await, want(&["99"]));
        assert_eq!(rows(&ctx, "SELECT sum(p.price) AS s FROM p HAVING sum(p.price) > (SELECT max(price) FROM c)").await, want(&["60"]));
        assert_eq!(rows(&ctx, &format!("SELECT p.id, {CHANGED} AS v FROM p ORDER BY 1")).await, want(&["1,11", "2,", "3,33"]));
    }
}
