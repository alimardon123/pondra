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
use datafusion::sql::sqlparser::ast::{self, FunctionArg, FunctionArgExpr, Statement, Visitor, VisitorMut};
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
        return not_impl_err!("LEFT JOIN LATERAL over {} is not supported yet: it takes ON true, and one unnest or set function whose columns are its own, or read from them by a field, a JSON operator or a cast", name_of(&j.right));
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

/// One unnest, no filter, and every output of the right side NULL on a padded row: each projection above the
/// unnest reads only the unnest's own columns, through `nulls_with`. A column of the left side, a constant or any
/// other expression would need a row of its own when the list is empty.
fn one_unnest(chain: &[&LogicalPlan]) -> bool {
    let at: Vec<usize> = chain.iter().enumerate().filter(|(_, p)| matches!(p, LogicalPlan::Unnest(_))).map(|(i, _)| i).collect();
    if at.len() != 1 || chain.iter().any(|p| matches!(p, LogicalPlan::Filter(_))) {
        return false;
    }
    let LogicalPlan::Unnest(u) = chain[at[0]] else { return false };
    let exec: Vec<usize> = u.exec_columns.iter().filter_map(|c| u.input.schema().index_of_column(c).ok()).collect();
    let mut padded: Vec<String> = u.schema.iter().enumerate().filter(|(i, _)| exec.contains(&u.dependency_indices[*i])).map(|(_, (_, f))| f.name().clone()).collect();
    let mut above = false;
    // (the chain is top first: the projections over the unnest are read from it up, each naming what the next reads)
    for p in chain[..at[0]].iter().rev() {
        if let LogicalPlan::Projection(p) = p {
            if !p.expr.iter().all(|e| nulls_with(e, &padded)) {
                return false;
            }
            padded = p.schema.fields().iter().map(|f| f.name().clone()).collect();
            above = true;
        }
    }
    above
}

/// The functions that answer NULL for a NULL first argument when the others are constants: a struct's field, the
/// JSON marks of a set function's values (`pg_json_make.rs`), and the JSON operators (`->`, `#>`, `#>>` are
/// `pg_json.rs`'s; `->>` is datafusion-functions-json's).
const STRICT: [&str; 7] = ["get_field", "pondra_as_json", "pondra_as_jsonb", "json_object_field", "json_as_text", "pondra_json_path", "pondra_json_path_text"];

/// Whether `e` is NULL whenever the columns named `padded` are: one of them, a cast of such an expression, or a
/// `STRICT` call whose first argument is such and whose others are constants.
fn nulls_with(e: &Expr, padded: &[String]) -> bool {
    match e {
        Expr::Alias(a) => nulls_with(&a.expr, padded),
        Expr::Column(c) => padded.contains(&c.name),
        Expr::Cast(c) => nulls_with(&c.expr, padded),
        Expr::TryCast(c) => nulls_with(&c.expr, padded),
        Expr::ScalarFunction(f) => {
            STRICT.contains(&f.name()) && f.args.first().is_some_and(|a| nulls_with(a, padded)) && f.args[1..].iter().all(constant)
        }
        _ => false,
    }
}

/// A literal, or a cast of one (type coercion casts a key's literal).
fn constant(e: &Expr) -> bool {
    match e {
        Expr::Literal(..) => true,
        Expr::Cast(c) => constant(&c.expr),
        Expr::TryCast(c) => constant(&c.expr),
        Expr::Alias(a) => constant(&a.expr),
        // (a bound value, a `$name` or a procedure's argument, is written `arrow_cast('k', 'Utf8')`: routines::exact)
        Expr::ScalarFunction(f) if f.name() == "arrow_cast" => f.args.iter().all(constant),
        _ => false,
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
/// function's. Whether anything changed. (A function with constant arguments stays DataFusion's, unless it is
/// `WITH ORDINALITY`: then it is a derived table of its own, with the positions beside its elements.)
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
    let _ = crate::walk::visit_mut(stmt, &mut f);
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

/// Each element's position in its list, from 1 (`WITH ORDINALITY`): a second unnest zipped beside the list's own, over `generate_series` since `range` types the count DECIMAL.
pub fn positions(list: &str) -> String { format!("unnest(generate_series(1, CAST(cardinality({list}) AS BIGINT)))") }

/// The derived table a FROM item becomes, when it's one of the set functions: a LATERAL one when it reads a column, and
/// one of its own under `WITH ORDINALITY` (which numbers each row's position in the call's list). A call this doesn't take
/// is refused by name under `WITH ORDINALITY` (DataFusion would drop the flag); otherwise `None`, DataFusion's.
fn written(f: &ast::TableFactor) -> anyhow::Result<Option<ast::TableFactor>> {
    let Some(call) = call_of(f) else { return Ok(None) };
    let lower = call.name.value.to_lowercase();
    let set = match (lower.as_str(), call.args.as_slice()) {
        ("unnest", [arg]) => Some(arg.to_string()),
        ("unnest", _) => None,
        _ if SET_FUNCTIONS.contains(&lower.as_str()) => Some(format!("{}({})", call.name, call.args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", "))),
        _ => None,
    };
    let reads = call.args.iter().any(reads_column);
    let Some(set) = set.filter(|_| call.ordinality || reads) else {
        if call.ordinality {
            return Err(crate::codes::coded("0A000", format!("WITH ORDINALITY over {}(…) is not supported yet", call.name)));
        }
        return Ok(None);
    };
    let col = match &call.alias {
        Some(a) => a.columns.first().map_or_else(|| a.name.clone(), |c| c.name.clone()),
        None => call.name.clone(),
    };
    // The list is computed once, in the FROM of the body, so its elements and their positions come from the same list.
    let body = if call.ordinality {
        let ord = call.alias.as_ref().and_then(|a| a.columns.get(1)).map_or_else(|| ast::Ident::new("ordinality"), |c| c.name.clone());
        format!("SELECT unnest(l) AS {col}, {} AS {ord} FROM (SELECT {set} AS l) AS __l", positions("l"))
    } else {
        format!("SELECT unnest({set}) AS {col}")
    };
    let alias = match call.alias {
        Some(a) => ast::TableAlias { explicit: a.explicit, name: a.name, columns: vec![], at: None },
        None => ast::TableAlias { explicit: false, name: call.name.clone(), columns: vec![], at: None },
    };
    let mut stmts = Parser::parse_sql(&GenericDialect {}, &body)?;
    let Some(Statement::Query(subquery)) = stmts.pop() else { return Ok(None) };
    Ok(Some(ast::TableFactor::Derived { lateral: reads, subquery, alias: Some(alias), sample: None }))
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
    let _ = crate::walk::visit(e, &mut c);
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

    /// A cast of the rows is NULL with them: a row of its own for each element, and NULL for a NULL or empty list.
    #[tokio::test]
    async fn left_pads_a_cast_of_its_rows() {
        let ctx = session().await.expect("a session");
        let sql = "SELECT t.id, s.y FROM t LEFT JOIN LATERAL (SELECT CAST(u.x AS VARCHAR) AS y FROM unnest(t.xs) AS u(x)) s ON true";
        assert_eq!(run(&ctx, sql).await.expect("a cast of the rows"), rows(&["1,1", "1,2", "1,3", "2,", "3,"]));
    }

    /// A field of a struct is NULL when the struct is, so a padded row keeps its left row with NULL there.
    #[tokio::test]
    async fn left_pads_a_field_of_its_rows() {
        let ctx = session().await.expect("a session");
        let sql = "SELECT t.id, s.a FROM t LEFT JOIN LATERAL (SELECT get_field(u.r, 'a') AS a FROM unnest(CASE WHEN t.id = 1 THEN make_array(named_struct('a', 7)) END) AS u(r)) s ON true";
        assert_eq!(run(&ctx, sql).await.expect("a field of the rows"), rows(&["1,7", "2,", "3,"]));
    }

    /// Outputs a padded row can't make NULL (a constant, arithmetic, a column of the left side) are refused by name.
    #[tokio::test]
    async fn left_computed_outputs_refused_by_name() {
        let ctx = session().await.expect("a session");
        for sql in [
            "SELECT t.id, s.one FROM t LEFT JOIN LATERAL (SELECT 1 AS one FROM unnest(t.xs) AS u(x)) s ON true",
            "SELECT t.id, s.y FROM t LEFT JOIN LATERAL (SELECT u.x + 1 AS y FROM unnest(t.xs) AS u(x)) s ON true",
            "SELECT t.id, s.m, s.x FROM t LEFT JOIN LATERAL (SELECT t.n AS m, u.x FROM unnest(t.xs) AS u(x)) s ON true",
        ] {
            let text = run(&ctx, sql).await.expect_err(sql).to_string();
            assert!(text.contains("LEFT JOIN LATERAL") && text.contains("not supported"), "{sql}: {text}");
        }
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

    /// `WITH ORDINALITY` numbers each row's own list from 1: the positions restart for each row of the FROM.
    #[tokio::test]
    async fn with_ordinality_restarts_its_positions_for_each_row() {
        let ctx = session().await.expect("a session");
        let got = run(&ctx, "SELECT t.id, g.v, g.o FROM t, generate_series(t.n, t.n + 1) WITH ORDINALITY AS g(v, o)").await.expect("ordinality over a column");
        assert_eq!(got, rows(&["1,2,1", "1,3,2", "2,1,1", "2,2,2", "3,3,1", "3,4,2"]));
        let got = run(&ctx, "SELECT t.id, ordinality FROM t, unnest(t.xs) WITH ORDINALITY").await.expect("unnest with no alias");
        assert_eq!(got, rows(&["1,1", "1,2", "1,3"]));
    }

    /// A constant call is a table of its own; a NULL or an empty list gives no rows.
    #[tokio::test]
    async fn with_ordinality_over_a_constant_and_an_empty_list() {
        let ctx = session().await.expect("a session");
        let got = run(&ctx, "SELECT g.v, g.o FROM generate_series(5, 7) WITH ORDINALITY AS g(v, o)").await.expect("a constant");
        assert_eq!(got, rows(&["5,1", "6,2", "7,3"]));
        let got = run(&ctx, "SELECT u.x, u.o FROM unnest(make_array(9, 8)) WITH ORDINALITY AS u(x, o)").await.expect("unnest of a constant");
        assert_eq!(got, rows(&["8,2", "9,1"]));
        let got = run(&ctx, "SELECT t.id, u.x, u.o FROM t, unnest(t.xs) WITH ORDINALITY AS u(x, o)").await.expect("NULL and empty lists");
        assert_eq!(got, rows(&["1,1,1", "1,2,2", "1,3,3"]));
    }

    /// What this rewrite doesn't take under `WITH ORDINALITY` is refused by name, since DataFusion would drop the flag.
    #[test]
    fn with_ordinality_refused_by_name_where_not_taken() {
        for (sql, call) in [
            ("SELECT * FROM t, unnest(t.xs, t.xs) WITH ORDINALITY AS u(x, y, o)", "unnest(…)"),
            ("SELECT * FROM t, my_function(t.n) WITH ORDINALITY", "my_function(…)"),
        ] {
            let mut stmt = Parser::parse_sql(&GenericDialect {}, sql).expect("parses").pop().expect("one");
            let err = functions(&mut stmt).expect_err(sql);
            assert!(err.to_string().contains(&format!("WITH ORDINALITY over {call} is not supported yet")), "{sql}: {err}");
        }
    }
}
