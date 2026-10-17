//! LATERAL over a function's rows, in one pass. Postgres writes `FROM t, unnest(t.xs) AS u(x)`,
//! `CROSS JOIN LATERAL generate_series(1, t.n) AS g` and `LEFT JOIN LATERAL unnest(t.xs) AS u(x) ON
//! true`. DataFusion plans each as a join whose right side reads the left side's columns, but its
//! physical planner can't run an outer reference. `Lateral` (an analyzer rule) rewrites such a join
//! into one plan: each row of the left side is passed through the function's rows made from it.
//! `functions` writes the FROM items DataFusion can't plan with a column in them (`generate_series(1,
//! t.n)` takes literals only) as the LATERAL subqueries it can.
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{not_impl_err, plan_err, Column, DFSchema, DataFusionError, NullHandling, Result, ScalarValue, UnnestOptions};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, Filter, Join, JoinType, LogicalPlan, Projection, SubqueryAlias, Unnest};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::sql::sqlparser::ast::{self, FunctionArg, FunctionArgExpr, Statement, Visit, Visitor, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use std::ops::ControlFlow;
use std::sync::Arc;

/// What a left-side column is called inside a rewritten join: its position in the left side.
fn left_name(i: usize) -> String { format!("__lateral_l{i}") }

/// The left side's columns passed through a function's rows, under their positions' names.
fn passed(left: usize) -> impl Iterator<Item = Expr> { (0..left).map(|i| Expr::Column(Column::from_name(left_name(i)))) }

/// The analyzer rule. Runs before type coercion, so the rewritten plan is typed like the planner's
/// own (and invariant 295's optimizer-off answers, which the analyzer always runs, see it too).
#[derive(Debug)]
pub struct Lateral;

impl AnalyzerRule for Lateral {
    fn name(&self) -> &str { "pondra_lateral" }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        // (most plans have no lateral of this rule's: they are handed back as they came, never rebuilt)
        let mut ours = false;
        plan.apply_with_subqueries(|p| {
            ours = match p {
                LogicalPlan::Join(j) => takes(j)?,
                _ => false,
            };
            Ok(if ours { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
        })?;
        if !ours {
            stray_outer(&plan)?;
            return Ok(plan);
        }
        let plan = plan.transform_up_with_subqueries(|plan| {
            let ours = match &plan {
                LogicalPlan::Join(j) => takes(j)?,
                _ => false,
            };
            if !ours {
                return Ok(Transformed::no(plan)); // (every other lateral is DataFusion's, as before)
            }
            match plan {
                LogicalPlan::Join(j) => Ok(Transformed::yes(join(j)?)),
                plan => Ok(Transformed::no(plan)),
            }
        })
        .data()?;
        stray_outer(&plan)?;
        Ok(plan)
    }
}

/// Refuses a function's rows over a column of an outer query, as in `(SELECT count(*) FROM
/// jsonb_array_elements(t.doc))` or `EXISTS (SELECT 1 FROM unnest(t.xs) …)`. The rule above has already
/// turned the outer references of each join it takes into columns of the left side, so an outer
/// reference left under an `Unnest` is one DataFusion's physical planner can't run: it would fail with
/// its own words. Looks at subqueries too, since that is where these sit.
fn stray_outer(plan: &LogicalPlan) -> Result<()> {
    let mut outer: Vec<String> = vec![];
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Unnest(u) = node {
            u.input.apply(|under| {
                if let LogicalPlan::Projection(p) = under {
                    for e in &p.expr {
                        e.apply(|e| {
                            if let Expr::OuterReferenceColumn(_, c) = e {
                                let name = c.to_string();
                                if !outer.contains(&name) {
                                    outer.push(name);
                                }
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })?;
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if outer.is_empty() {
        return Ok(());
    }
    let message = format!(
        "a function's rows over a column of an outer query (in a subquery: {}) aren't taken yet: join them in that query's FROM, as FROM t, jsonb_array_elements(t.doc) AS e, then filter or group there",
        outer.join(", ")
    );
    Err(DataFusionError::External(Box::new(crate::codes::Coded("0A000", message))))
}

/// Whether a join is a lateral over a function's rows that this rule rewrites. A LEFT one that one
/// pass can't pad (Postgres keeps each left row with NULLs when its list is empty) is refused by name.
fn takes(j: &Join) -> Result<bool> {
    let mut chain = vec![];
    let subquery = match j.right.as_ref() {
        LogicalPlan::SubqueryAlias(a) => matches!(a.input.as_ref(), LogicalPlan::Subquery(_)),
        LogicalPlan::Subquery(_) => true,
        _ => false,
    };
    if !subquery || !j.on.is_empty() || !matches!(j.join_type, JoinType::Inner | JoinType::Left) || !chain_of(&j.right, &mut chain) {
        return Ok(false);
    }
    let left = j.left.schema();
    let mut outer = vec![];
    let mut nested = false;
    for e in chain.iter().flat_map(|p| p.expressions()) {
        e.apply(|e| {
            match e {
                Expr::OuterReferenceColumn(_, c) => outer.push(c.clone()),
                Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) => nested = true,
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    let unnests = chain.iter().any(|p| matches!(p, LogicalPlan::Unnest(_)));
    if !unnests || nested || outer.is_empty() || outer.iter().any(|c| left.index_of_column(c).is_err()) {
        return Ok(false);
    }
    if j.join_type == JoinType::Left && !(padded(j) && one_unnest(&chain)) {
        return not_impl_err!("LEFT JOIN LATERAL over {} is not supported yet: it takes ON true, and one unnest or set function whose columns are its own, unchanged", name_of(&j.right));
    }
    Ok(true)
}

/// The nodes of a function's rows, top first: wrappers, projections, unnests and filters, down to the
/// one row they start from. False for anything else (a scan, a join, an aggregate, a limit).
fn chain_of<'a>(plan: &'a LogicalPlan, nodes: &mut Vec<&'a LogicalPlan>) -> bool {
    nodes.push(plan);
    match plan {
        LogicalPlan::EmptyRelation(e) => e.produce_one_row,
        LogicalPlan::Projection(p) => chain_of(&p.input, nodes),
        LogicalPlan::Unnest(u) => chain_of(&u.input, nodes),
        LogicalPlan::Filter(f) => chain_of(&f.input, nodes),
        LogicalPlan::SubqueryAlias(a) => chain_of(&a.input, nodes),
        LogicalPlan::Subquery(s) => chain_of(&s.subquery, nodes),
        _ => false,
    }
}

/// `ON true`, or no condition: a LEFT JOIN LATERAL that keeps each left row.
fn padded(j: &Join) -> bool {
    match &j.filter {
        None => true,
        Some(f) => matches!(f, Expr::Literal(ScalarValue::Boolean(Some(true)), _)),
    }
}

/// One unnest, no filter, and every output of the right side one of the unnest's own (a column of
/// the left side, or a constant, would need a row of its own when the list is empty).
fn one_unnest(chain: &[&LogicalPlan]) -> bool {
    let unnests: Vec<&Unnest> = chain.iter().filter_map(|p| match p { LogicalPlan::Unnest(u) => Some(u), _ => None }).collect();
    if unnests.len() != 1 || chain.iter().any(|p| matches!(p, LogicalPlan::Filter(_))) {
        return false;
    }
    let u = unnests[0];
    let exec: Vec<usize> = u.exec_columns.iter().filter_map(|c| u.input.schema().index_of_column(c).ok()).collect();
    let made: Vec<&str> = u.schema.iter().enumerate().filter(|(i, _)| exec.contains(&u.dependency_indices[*i])).map(|(_, (_, f))| f.name().as_str()).collect();
    let Some(LogicalPlan::Projection(top)) = chain.iter().find(|p| matches!(p, LogicalPlan::Projection(_))) else { return false };
    top.expr.iter().all(|e| matches!(unaliased(e), Expr::Column(c) if made.contains(&c.name.as_str())))
}

fn unaliased(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unaliased(&a.expr),
        e => e,
    }
}

/// What the right side is called in an error: its alias.
fn name_of(right: &LogicalPlan) -> String {
    match right {
        LogicalPlan::SubqueryAlias(a) => a.alias.to_string(),
        _ => "a subquery".to_string(),
    }
}

/// The join as one plan: the left side's rows, each with the function's rows made from it. The left
/// side is renamed to position names at the bottom (`__lateral_l{i}`, unqualified), passed through the
/// chain, and the join's own columns are picked out at the top under their qualifiers and names.
fn join(j: Join) -> Result<LogicalPlan> {
    let left = j.left.schema().clone();
    let bottom = LogicalPlan::Projection(Projection::try_new(
        left.iter().enumerate().map(|(i, (q, f))| Expr::Column(Column::new(q.cloned(), f.name())).alias(left_name(i))).collect(),
        j.left.clone(),
    )?);
    // (a function's rows that are NULL or empty make no row of a CROSS join, one NULL row of a LEFT one)
    let nulls = if j.join_type == JoinType::Left { NullHandling::PreserveAndExpandEmpty } else { NullHandling::Drop };
    let right = rebuild(&j.right, &bottom, &left, nulls)?;
    let mut top: Vec<Expr> = left.iter().enumerate().map(|(i, (q, f))| Expr::Column(Column::from_name(left_name(i))).alias_qualified(q.cloned(), f.name())).collect();
    // The function's columns go up as they are: aliased to their own names, DataFusion's leaf pushdown (`e.value ->> 'k'`,
    // a struct's field) took the alias for a computed column, added the column beside it and failed on the duplicate.
    top.extend(j.right.schema().iter().map(|(q, f)| Expr::Column(Column::new(q.cloned(), f.name()))));
    let plan = LogicalPlan::Projection(Projection::try_new(top, Arc::new(right))?);
    Ok(match j.filter {
        Some(f) if j.join_type == JoinType::Inner => LogicalPlan::Filter(Filter::try_new(f, Arc::new(plan))?),
        _ => plan,
    })
}

/// The chain's nodes rebuilt over `bottom`: each keeps its names, and reads the left side's columns
/// from their position names (`left` says which column each outer reference is).
fn rebuild(plan: &LogicalPlan, bottom: &LogicalPlan, left: &DFSchema, nulls: NullHandling) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::EmptyRelation(_) => bottom.clone(),
        LogicalPlan::Projection(p) => {
            let input = rebuild(&p.input, bottom, left, nulls)?;
            let names = NamePreserver::new(plan); // (a column keeps the name its expression gave it)
            let mut exprs = p.expr.iter().map(|e| -> Result<Expr> { Ok(names.save(e).restore(unouter(e.clone(), left)?)) }).collect::<Result<Vec<_>>>()?;
            exprs.extend(passed(left.fields().len()));
            LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(input))?)
        }
        LogicalPlan::Filter(f) => {
            let input = rebuild(&f.input, bottom, left, nulls)?;
            LogicalPlan::Filter(Filter::try_new(unouter(f.predicate.clone(), left)?, Arc::new(input))?)
        }
        LogicalPlan::Unnest(u) => {
            let input = rebuild(&u.input, bottom, left, nulls)?;
            // (its list columns and dependencies are worked out again from the new input)
            let options = UnnestOptions { null_handling: nulls, recursions: u.options.recursions.clone() };
            LogicalPlan::Unnest(Unnest::try_new(Arc::new(input), u.exec_columns.clone(), options)?)
        }
        LogicalPlan::SubqueryAlias(a) => {
            let input = rebuild(&a.input, bottom, left, nulls)?;
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(input), a.alias.clone())?)
        }
        // (the wrapper goes: DataFusion's physical planner refuses an undecorrelated Subquery, and the
        // rewritten join reads the left side's columns directly, so nothing is correlated any more)
        LogicalPlan::Subquery(s) => rebuild(&s.subquery, bottom, left, nulls)?,
        _ => return plan_err!("LATERAL: a node of a function's rows that chain_of doesn't know"),
    })
}

/// An expression with each outer reference to the left side read from its position's column.
fn unouter(e: Expr, left: &DFSchema) -> Result<Expr> {
    e.transform(|e| match e {
        Expr::OuterReferenceColumn(_, c) => {
            let i = left.index_of_column(&c)?;
            Ok(Transformed::yes(Expr::Column(Column::from_name(left_name(i)))))
        }
        e => Ok(Transformed::no(e)),
    })
    .data()
}

/// The set functions a FROM item may call with a column (`generate_series(1, t.n)`, `range`, and
/// `unnest(t.xs)`, which takes its one argument).
const SET_FUNCTIONS: &[&str] = &["generate_series", "range", "unnest"];

/// Writes the FROM items DataFusion can't plan with a column in them as LATERAL subqueries it can:
/// `FROM t, generate_series(1, t.n) AS g` becomes `LATERAL (SELECT unnest(generate_series(1, t.n)) AS
/// "g") AS "g"`, and `unnest(t.xs) AS u(x)` becomes `LATERAL (SELECT unnest(t.xs) AS "x") AS "u"`. The
/// name of the column is as Postgres names it: the alias's column, else the alias's name, else the
/// function's. Whether anything changed. (A function with constant arguments stays DataFusion's.)
pub fn functions(stmt: &mut Statement) -> anyhow::Result<bool> {
    struct Functions {
        changed: bool,
        failed: Option<anyhow::Error>,
    }
    impl VisitorMut for Functions {
        type Break = ();

        fn pre_visit_table_factor(&mut self, f: &mut ast::TableFactor) -> ControlFlow<()> {
            match written(f) {
                Ok(Some(lateral)) => {
                    *f = lateral;
                    self.changed = true;
                    ControlFlow::Continue(())
                }
                Ok(None) => ControlFlow::Continue(()),
                Err(e) => {
                    self.failed = Some(e);
                    ControlFlow::Break(())
                }
            }
        }
    }
    let mut f = Functions { changed: false, failed: None };
    let _ = VisitMut::visit(stmt, &mut f);
    match f.failed {
        Some(e) => Err(e),
        None => Ok(f.changed),
    }
}

/// A FROM item that calls one of the set functions, as written.
struct Call {
    name: ast::Ident,
    args: Vec<ast::Expr>,
    alias: Option<ast::TableAlias>,
    ordinality: bool,
}

fn call_of(f: &ast::TableFactor) -> Option<Call> {
    let (name, args, alias, ordinality) = match f {
        ast::TableFactor::Table { name, args: Some(args), alias, with_ordinality, .. } => (last(name)?, plain(&args.args)?, alias, *with_ordinality),
        ast::TableFactor::Function { name, args, alias, with_ordinality, .. } => (last(name)?, plain(args)?, alias, *with_ordinality),
        ast::TableFactor::UNNEST { array_exprs, alias, with_ordinality, .. } => (ast::Ident::new("unnest"), array_exprs.clone(), alias, *with_ordinality),
        _ => return None,
    };
    Some(Call { name, args, alias: alias.clone(), ordinality })
}

/// The LATERAL subquery a FROM item becomes, when it's one of the set functions over a column.
fn written(f: &ast::TableFactor) -> anyhow::Result<Option<ast::TableFactor>> {
    let Some(call) = call_of(f) else { return Ok(None) };
    let lower = call.name.value.to_lowercase();
    if !SET_FUNCTIONS.contains(&lower.as_str()) || !call.args.iter().any(reads_column) {
        return Ok(None);
    }
    if call.ordinality {
        return Err(crate::codes::coded("0A000", "WITH ORDINALITY is not supported yet"));
    }
    let set = match (lower.as_str(), call.args.as_slice()) {
        ("unnest", [arg]) => arg.to_string(),
        ("unnest", _) => return Ok(None),
        _ => format!("{}({})", call.name, call.args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ")),
    };
    let col = match &call.alias {
        Some(a) => a.columns.first().map_or_else(|| a.name.clone(), |c| c.name.clone()),
        None => call.name.clone(),
    };
    let alias = match call.alias {
        Some(a) => ast::TableAlias { explicit: a.explicit, name: a.name, columns: vec![], at: None },
        None => ast::TableAlias { explicit: false, name: call.name.clone(), columns: vec![], at: None },
    };
    let mut stmts = Parser::parse_sql(&GenericDialect {}, &format!("SELECT unnest({set}) AS {col}"))?;
    let Some(Statement::Query(subquery)) = stmts.pop() else { return Ok(None) };
    Ok(Some(ast::TableFactor::Derived { lateral: true, subquery, alias: Some(alias), sample: None }))
}

fn last(name: &ast::ObjectName) -> Option<ast::Ident> { name.0.last().and_then(|p| p.as_ident()).cloned() }

/// The arguments, if each is a plain expression (not a name or a `*`).
fn plain(args: &[FunctionArg]) -> Option<Vec<ast::Expr>> {
    args.iter().map(|a| match a {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
        _ => None,
    }).collect()
}

/// Whether an argument reads a column: an identifier outside a subquery of its own.
fn reads_column(e: &ast::Expr) -> bool {
    struct Columns {
        depth: usize,
        found: bool,
    }
    impl Visitor for Columns {
        type Break = ();

        fn pre_visit_query(&mut self, _: &ast::Query) -> ControlFlow<()> {
            self.depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _: &ast::Query) -> ControlFlow<()> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, e: &ast::Expr) -> ControlFlow<()> {
            if self.depth == 0 && matches!(e, ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)) {
                self.found = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut c = Columns { depth: 0, found: false };
    let _ = Visit::visit(e, &mut c);
    c.found
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::sql::parser::DFParser;

    /// A session the way Pondra makes one (`store`'s, with `optimize`'s rules), and `t`: id, xs, n
    /// (a list, NULL, and an empty one; n 2, 1, 3).
    async fn session() -> Result<SessionContext> {
        let state = SessionStateBuilder::new()
            .with_config(crate::optimize::config(SessionConfig::new()))
            .with_default_features()
            .with_analyzer_rules(crate::optimize::analyzer_rules())
            .with_optimizer_rules(crate::optimize::rules())
            .build();
        let ctx = SessionContext::new_with_state(state);
        let t = ctx.sql("SELECT * FROM (VALUES (1, make_array(1, 2, 3), 2), (2, CAST(NULL AS INT[]), 1), (3, CAST(make_array() AS INT[]), 3)) AS t(id, xs, n)").await?;
        ctx.register_table("t", t.into_view())?;
        Ok(ctx)
    }

    /// The answer of one statement, with its functions written as the door would write them.
    async fn run(ctx: &SessionContext, sql: &str) -> anyhow::Result<Vec<String>> {
        let mut stmt = DFParser::parse_sql(sql)?.pop_front().expect("a statement");
        if let datafusion::sql::parser::Statement::Statement(s) = &mut stmt {
            functions(s)?;
        }
        let plan = ctx.state().statement_to_plan(stmt).await?;
        let df = ctx.execute_logical_plan(plan).await?;
        let mut rows = vec![];
        for b in df.collect().await? {
            for r in 0..b.num_rows() {
                let row: Vec<String> = b.columns().iter().map(|c| datafusion::arrow::util::display::array_value_to_string(c, r).unwrap_or_default()).collect();
                rows.push(row.join(","));
            }
        }
        rows.sort();
        Ok(rows)
    }

    fn rows(xs: &[&str]) -> Vec<String> { xs.iter().map(|s| s.to_string()).collect() }

    /// A field read, which DataFusion moves toward the scans, over either side of a lateral plans and answers.
    #[tokio::test]
    async fn a_field_read_over_either_side_answers() {
        let ctx = session().await.expect("a session");
        let sql = "SELECT get_field(s.r, 'a') AS a, get_field(u.x, 'b') AS b FROM (SELECT named_struct('a', 1) AS r, \
                   make_array(named_struct('b', 2), named_struct('b', 3)) AS xs) s, unnest(s.xs) AS u(x)";
        assert_eq!(run(&ctx, sql).await.expect("a field of each side"), rows(&["1,2", "1,3"]));
    }

    #[tokio::test]
    async fn implicit_and_explicit_unnest_are_one_pass() {
        let ctx = session().await.expect("a session");
        let want = rows(&["1,1", "1,2", "1,3"]);
        assert_eq!(run(&ctx, "SELECT t.id, u.x FROM t, unnest(t.xs) AS u(x)").await.expect("the implicit form"), want);
        assert_eq!(run(&ctx, "SELECT t.id, u.x FROM t CROSS JOIN LATERAL unnest(t.xs) AS u(x)").await.expect("the explicit form"), want);
    }

    #[tokio::test]
    async fn generate_series_takes_a_column() {
        let ctx = session().await.expect("a session");
        let got = run(&ctx, "SELECT t.id, g FROM t, generate_series(1, t.n) AS g").await.expect("generate_series");
        assert_eq!(got, rows(&["1,1", "1,2", "2,1", "3,1", "3,2", "3,3"]));
    }

    #[tokio::test]
    async fn left_keeps_each_row_once_padded() {
        let ctx = session().await.expect("a session");
        let got = run(&ctx, "SELECT t.id, u.x FROM t LEFT JOIN LATERAL unnest(t.xs) AS u(x) ON true").await.expect("LEFT ON true");
        assert_eq!(got, rows(&["1,1", "1,2", "1,3", "2,", "3,"]));
    }

    #[tokio::test]
    async fn chained_laterals() {
        let ctx = session().await.expect("a session");
        let got = run(&ctx, "SELECT t.id, u.x, v.y FROM t, LATERAL (SELECT unnest(t.xs) AS x) u, LATERAL (SELECT unnest(generate_series(1, u.x)) AS y) v").await.expect("chained");
        assert_eq!(got, rows(&["1,1,1", "1,2,1", "1,2,2", "1,3,1", "1,3,2", "1,3,3"]));
    }

    #[tokio::test]
    async fn a_lateral_the_rule_leaves_plans_as_before() {
        let ctx = session().await.expect("a session");
        let got = run(&ctx, "SELECT t.id, s.m FROM t, LATERAL (SELECT max(v) AS m FROM (SELECT 1 AS v) w WHERE w.v = t.n) s").await.expect("an aggregate lateral");
        assert_eq!(got, rows(&["1,", "2,1", "3,"]));
    }

    #[tokio::test]
    async fn left_outputs_refused_by_name() {
        let ctx = session().await.expect("a session");
        let err = run(&ctx, "SELECT t.id, u.x FROM t LEFT JOIN LATERAL unnest(t.xs) AS u(x) ON u.x > 1").await.expect_err("refused");
        let text = err.to_string();
        assert!(text.contains("LEFT JOIN LATERAL") && text.contains("not supported"), "{text}");
    }

    #[tokio::test]
    async fn a_set_function_over_an_outer_column_is_refused_by_name() {
        let ctx = session().await.expect("a session");
        for sql in ["SELECT t.id, (SELECT count(*) FROM unnest(t.xs)) FROM t", "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM unnest(t.xs) e WHERE e = 2)"] {
            let err = run(&ctx, sql).await.expect_err(sql);
            let words = crate::codes::words(&err);
            assert_eq!(crate::codes::of(&err), "0A000", "{sql}");
            assert!(words.contains("t.xs") && words.contains("aren't taken yet"), "{sql}: {words}");
        }
    }

    #[tokio::test]
    async fn a_plain_correlated_subquery_is_as_before() {
        let ctx = session().await.expect("a session");
        let got = run(&ctx, "SELECT t.id, (SELECT max(v) FROM (SELECT 1 AS v) w WHERE w.v = t.n) AS m FROM t").await.expect("a correlated subquery");
        assert_eq!(got, rows(&["1,", "2,1", "3,"]));
    }

    #[test]
    fn with_ordinality_is_refused() {
        let mut stmt = Parser::parse_sql(&GenericDialect {}, "SELECT * FROM t, generate_series(1, t.n) WITH ORDINALITY AS g(v, o)").expect("parses").pop().expect("one");
        let err = functions(&mut stmt).expect_err("refused");
        assert!(err.to_string().contains("WITH ORDINALITY is not supported yet"));
    }
}
