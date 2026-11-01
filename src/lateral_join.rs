//! A LATERAL subquery correlated by equalities, as a join: Postgres's and DuckDB's top-N per group
//! (`LEFT JOIN LATERAL (SELECT … WHERE o.customer_id = c.id ORDER BY … LIMIT 1) top ON true`).
//! DataFusion can't run a subquery that reads the outer row, so each such subquery is rebuilt to run
//! once over all its rows, and each outer row joins to its own:
//!
//! - each equality `k = o` in the subquery's WHERE, where `o` is a column of the outer row, is a key:
//!   the subquery projects `k AS __lk1`, … and the join is `ON top.__lk1 = o`, and so on;
//! - with `LIMIT n`, the rows are numbered within each key (`row_number()`, in ORDER BY's order) and
//!   the first `n` of each key are kept: `n` rows per outer row, as the LIMIT gave.
//!
//! A subquery with anything else (an aggregate, a window, a set function, DISTINCT, OFFSET, a `*`, a correlation that
//! isn't an equality, an outer column outside those equalities) stays as written, and DataFusion's
//! error with it. `lateral.rs` keeps the LATERAL over a function's rows (`generate_series(1, t.n)`).
use crate::walk;
use anyhow::Result;
use datafusion::sql::sqlparser::ast::*;
use std::collections::HashSet;
use std::ops::ControlFlow;

/// The aggregates: a subquery that calls one answers one row, not one row per row it reads.
const AGGREGATES: &[&str] = &["count", "sum", "avg", "min", "max", "array_agg", "string_agg", "bool_and", "bool_or", "stddev", "variance", "median", "any_value"];

/// Rewrites each LATERAL subquery of `s` that the module's doc takes into a join. Whether any was.
pub fn rewrite(s: &mut Statement) -> Result<bool> {
    struct Joins {
        changed: bool,
        failed: Option<anyhow::Error>,
    }
    impl VisitorMut for Joins {
        type Break = ();

        fn post_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
            match lift_from(select) {
                Ok(changed) => {
                    self.changed |= changed;
                    ControlFlow::Continue(())
                }
                Err(e) => {
                    self.failed = Some(e);
                    ControlFlow::Break(())
                }
            }
        }
    }
    let mut joins = Joins { changed: false, failed: None };
    let _ = walk::visit_mut(s, &mut joins);
    match joins.failed {
        Some(e) => Err(e),
        None => Ok(joins.changed),
    }
}

/// The LATERAL subqueries of one select's FROM that become joins: a join's own item, and the comma form's
/// `FROM a, LATERAL (…) b` when `a` is the only item before it. Whether any did.
fn lift_from(select: &mut Select) -> Result<bool> {
    // (a `*` would show the columns the rewrite adds to the subquery)
    if select.projection.iter().any(|p| matches!(p, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..))) {
        return Ok(false);
    }
    let mut changed = false;
    if select.from.len() >= 2 && select.from[1].joins.is_empty() {
        if let Some(keys) = lift(&mut select.from[1].relation)? {
            let item = select.from.remove(1);
            let join = JoinOperator::Join(JoinConstraint::On(conjoin(keys, None)));
            select.from[0].joins.push(Join { relation: item.relation, global: false, join_operator: join });
            changed = true;
        }
    }
    for item in select.from.iter_mut() {
        for join in item.joins.iter_mut() {
            if join.global {
                continue;
            }
            let (left, extra) = match join_kind(&join.join_operator) {
                Some((left, extra)) => (left, extra.cloned()),
                None => continue,
            };
            let Some(keys) = lift(&mut join.relation)? else { continue };
            let on = conjoin(keys, extra.filter(|e| !is_true(e)));
            join.join_operator = match left {
                true => JoinOperator::Left(JoinConstraint::On(on)),
                false => JoinOperator::Join(JoinConstraint::On(on)),
            };
            changed = true;
        }
    }
    Ok(changed)
}

/// The joins a LATERAL subquery can become, as (LEFT?, the join's own condition): a JOIN or INNER JOIN ON it, a
/// CROSS JOIN (none), a LEFT [OUTER] JOIN ON it. Any other kind is None.
fn join_kind(op: &JoinOperator) -> Option<(bool, Option<&Expr>)> {
    match op {
        JoinOperator::Join(JoinConstraint::On(on)) | JoinOperator::Inner(JoinConstraint::On(on)) => Some((false, Some(on))),
        JoinOperator::CrossJoin(JoinConstraint::None) => Some((false, None)),
        JoinOperator::Left(JoinConstraint::On(on)) | JoinOperator::LeftOuter(JoinConstraint::On(on)) => Some((true, Some(on))),
        _ => None,
    }
}

fn is_true(e: &Expr) -> bool { matches!(e, Expr::Value(ValueWithSpan { value: Value::Boolean(true), .. })) }

/// The keys' equalities and the join's own condition, as one AND (`ON true` adds nothing).
fn conjoin(keys: Vec<Expr>, extra: Option<Expr>) -> Expr {
    let mut terms = keys;
    terms.extend(extra.map(|e| Expr::Nested(Box::new(e))));
    terms
        .into_iter()
        .reduce(|left, right| Expr::BinaryOp { left: Box::new(left), op: BinaryOperator::And, right: Box::new(right) })
        .unwrap_or_else(|| Expr::value(Value::Boolean(true)))
}

/// A LATERAL derived table that is rewritten: it becomes a plain one (its query is the rebuilt one, its alias gets
/// the key columns), and the equalities `alias.__lk1 = o1`, … are returned for the join's ON. None, and the table as
/// it was, when a check fails.
fn lift(f: &mut TableFactor) -> Result<Option<Vec<Expr>>> {
    let TableFactor::Derived { lateral, subquery, alias: Some(alias), sample: None } = f else { return Ok(None) };
    if !*lateral {
        return Ok(None);
    }
    let Some(p) = plan(&**subquery)? else { return Ok(None) };
    if !alias.columns.is_empty() {
        // (the alias's list names the answer's columns in order: the key columns come after the names it has)
        let Some(names) = &p.names else { return Ok(None) };
        let given = alias.columns.len();
        if given > names.len() {
            return Ok(None);
        }
        let mut columns = alias.columns.clone();
        columns.extend(names[given..].iter().map(|n| TableAliasColumnDef { name: n.clone(), data_type: None }));
        columns.extend((1..=p.outer.len()).map(|i| TableAliasColumnDef::from_name(format!("__lk{i}"))));
        alias.columns = columns;
    }
    let keys = p
        .outer
        .iter()
        .enumerate()
        .map(|(i, outer)| Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![alias.name.clone(), Ident::new(format!("__lk{}", i + 1))])),
            op: BinaryOperator::Eq,
            right: Box::new(outer.clone()),
        })
        .collect();
    *subquery = Box::new(p.query);
    *lateral = false;
    Ok(Some(keys))
}

/// A subquery rebuilt for its join: the new query, the outer side of each key (`o` in `k = o`), and the names of its
/// answer's columns (None when an item has no name).
struct Plan {
    query: Query,
    outer: Vec<Expr>,
    names: Option<Vec<Ident>>,
}

/// The rebuilt subquery of `q`, or None when a check fails. The text below carries the parts of a SELECT that are
/// kept, so every other clause is a check here: the ones that would change the answer, and the ones it would drop.
fn plan(q: &Query) -> Result<Option<Plan>> {
    if q.with.is_some() || q.fetch.is_some() || !q.locks.is_empty() || q.for_clause.is_some() || q.settings.is_some() || q.format_clause.is_some() || !q.pipe_operators.is_empty() {
        return Ok(None);
    }
    let SetExpr::Select(body) = &*q.body else { return Ok(None) };
    let select: &Select = body;
    let Select {
        select_token: _,
        optimizer_hints,
        distinct,
        select_modifiers,
        top,
        top_before_distinct: _,
        projection,
        exclude,
        into,
        from,
        lateral_views,
        prewhere,
        selection,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify: _,
        value_table_mode,
        flavor,
    } = select;
    let GroupByExpr::Expressions(group, modifiers) = group_by else { return Ok(None) };
    let plain_select = optimizer_hints.is_empty()
        && distinct.is_none()
        && select_modifiers.is_none()
        && top.is_none()
        && exclude.is_none()
        && into.is_none()
        && lateral_views.is_empty()
        && prewhere.is_none()
        && connect_by.is_empty()
        && group.is_empty()
        && modifiers.is_empty()
        && cluster_by.is_empty()
        && distribute_by.is_empty()
        && sort_by.is_empty()
        && having.is_none()
        && named_window.is_empty()
        && qualify.is_none()
        && value_table_mode.is_none()
        && *flavor == SelectFlavor::Standard;
    let starred = projection.iter().any(|p| matches!(p, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) | SelectItem::ExprWithAliases { .. }));
    if !plain_select || projection.is_empty() || starred || not_row_for_row(projection) {
        return Ok(None);
    }
    // the names the FROM is visible by: an outer column is one of them not
    let mut inner = Vec::new();
    for item in from.iter() {
        if names_of(item, &mut inner).is_none() {
            return Ok(None);
        }
    }
    if reads_outer(projection, &inner) || reads_outer(from, &inner) {
        return Ok(None);
    }
    // the WHERE's conjuncts: each outer column in one is a key's equality, and nothing else reads one
    let mut parts = Vec::new();
    if let Some(w) = selection {
        conjuncts(w, &mut parts);
    }
    let (mut keys, mut rest) = (Vec::new(), Vec::new());
    for c in parts {
        if !reads_outer(&c, &inner) {
            rest.push(c);
        } else {
            let Some(key) = equality(&c, &inner) else { return Ok(None) };
            keys.push(key);
        }
    }
    if keys.is_empty() {
        return Ok(None);
    }
    let order: Vec<OrderByExpr> = match &q.order_by {
        None => Vec::new(),
        Some(OrderBy { kind: OrderByKind::Expressions(list), interpolate: None }) => list.clone(),
        Some(_) => return Ok(None),
    };
    // an output name that is another item's expression: ORDER BY would read it inside the window as a column of FROM
    let aliased: Vec<String> = projection.iter().filter(|p| !plain(p)).filter_map(output).map(|n| n.value.to_lowercase()).collect();
    for o in &order {
        let number = matches!(o.expr, Expr::Value(ValueWithSpan { value: Value::Number(..), .. }));
        if number || o.with_fill.is_some() || reads_outer(&o.expr, &inner) || not_row_for_row(&o.expr) || names_alias(&o.expr, &aliased) {
            return Ok(None);
        }
    }
    let limit = match &q.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset { limit: None, offset: None, limit_by }) if limit_by.is_empty() => None,
        Some(LimitClause::LimitOffset { limit: Some(Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. })), offset: None, limit_by })
            if limit_by.is_empty() =>
        {
            match n.parse::<u64>() {
                Ok(n) => Some(n),
                Err(_) => return Ok(None),
            }
        }
        Some(_) => return Ok(None),
    };
    let names: Option<Vec<Ident>> = projection.iter().map(output).collect();
    let items = projection.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ");
    let from_text = from.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", ");
    let filter = match rest.is_empty() {
        true => String::new(),
        false => format!(" WHERE {}", rest.iter().map(|c| format!("({c})")).collect::<Vec<_>>().join(" AND ")),
    };
    let key_exprs: Vec<String> = keys.iter().map(|(k, _)| operand(k)).collect();
    let key_cols: Vec<String> = key_exprs.iter().enumerate().map(|(i, k)| format!("{k} AS __lk{}", i + 1)).collect();
    let (text, names) = match limit {
        None => (format!("SELECT {items}, {} FROM {from_text}{filter}", key_cols.join(", ")), names),
        Some(n) => {
            let Some(names) = names.filter(|v| !repeated(v)) else { return Ok(None) };
            let window = match order.is_empty() {
                true => format!("PARTITION BY {}", key_exprs.join(", ")),
                false => format!("PARTITION BY {} ORDER BY {}", key_exprs.join(", "), order.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(", ")),
            };
            let inner_query = format!("SELECT {items}, {}, row_number() OVER ({window}) AS __lrn FROM {from_text}{filter}", key_cols.join(", "));
            // (the answer's own columns, then the keys; __lrn is not one of them)
            let cols: Vec<String> = names.iter().map(|c| c.to_string()).chain((1..=keys.len()).map(|i| format!("__lk{i}"))).collect();
            (format!("SELECT {} FROM ({inner_query}) AS __l WHERE __lrn <= {n}", cols.join(", ")), Some(names))
        }
    };
    Ok(Some(Plan { query: crate::friendly::query(&text)?, outer: keys.into_iter().map(|(_, o)| o).collect(), names }))
}

/// A column named by an unquoted or quoted name, as the answer's name for it (`o.amount` is `amount`).
fn output(item: &SelectItem) -> Option<Ident> {
    match item {
        SelectItem::UnnamedExpr(Expr::Identifier(i)) => Some(i.clone()),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => parts.last().cloned(),
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.clone()),
        _ => None,
    }
}

/// A column written as itself (`o.amount`, not `o.amount AS x`): its name is the column's own.
fn plain(item: &SelectItem) -> bool { matches!(item, SelectItem::UnnamedExpr(Expr::Identifier(_) | Expr::CompoundIdentifier(_))) }

fn repeated(names: &[Ident]) -> bool {
    let mut seen = HashSet::new();
    names.iter().any(|n| !seen.insert(n.value.to_lowercase()))
}

/// A key's expression as text in a list: a name as it is, anything else in parentheses.
fn operand(e: &Expr) -> String {
    match e {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Nested(_) | Expr::Function(_) => e.to_string(),
        _ => format!("({e})"),
    }
}

/// The names a FROM item is visible by, lower-cased: its alias, else its table's last name part; a nested join's
/// own too. None for a kind of item this doesn't read (a PIVOT, a JSON table, …).
fn names_of(item: &TableWithJoins, out: &mut Vec<String>) -> Option<()> {
    factor_names(&item.relation, out)?;
    for join in &item.joins {
        factor_names(&join.relation, out)?;
    }
    Some(())
}

fn factor_names(f: &TableFactor, out: &mut Vec<String>) -> Option<()> {
    let (alias, table) = match f {
        TableFactor::Table { name, alias, .. } | TableFactor::Function { name, alias, .. } => (alias, name.0.last().and_then(|p| p.as_ident()).cloned()),
        TableFactor::Derived { alias, .. } | TableFactor::UNNEST { alias, .. } => (alias, None),
        TableFactor::NestedJoin { table_with_joins, alias } => {
            names_of(table_with_joins, out)?;
            (alias, None)
        }
        _ => return None,
    };
    if let Some(alias) = alias {
        out.push(alias.name.value.to_lowercase());
    } else if let Some(table) = table {
        out.push(table.value.to_lowercase());
    }
    Some(())
}

/// `e` split at its top-level ANDs (parentheses taken off, as they don't change what is ANDed).
fn conjuncts(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Nested(inner) => conjuncts(inner, out),
        _ => out.push(e.clone()),
    }
}

/// `k = o` or `o = k`, where `o` is an outer column and `k` reads none: (k, o).
fn equality(e: &Expr, inner: &[String]) -> Option<(Expr, Expr)> {
    let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = e else { return None };
    [(&**left, &**right), (&**right, &**left)]
        .into_iter()
        .find(|(o, k)| outer_ref(o, inner) && !reads_outer(*k, inner))
        .map(|(o, k)| (k.clone(), o.clone()))
}

/// A two-part name whose first part is no name the subquery's FROM gives: a column of the outer query.
fn outer_ref(e: &Expr, inner: &[String]) -> bool {
    matches!(e, Expr::CompoundIdentifier(parts) if parts.len() == 2 && !inner.contains(&parts[0].value.to_lowercase()))
}

fn reads_outer<V: Visit>(v: &V, inner: &[String]) -> bool {
    walk::visit_expressions(v, |e| match outer_ref(e, inner) {
        true => ControlFlow::Break(()),
        false => ControlFlow::Continue(()),
    })
    .is_break()
}

fn names_alias(e: &Expr, aliased: &[String]) -> bool {
    walk::visit_expressions(e, |x| match x {
        Expr::Identifier(i) if aliased.contains(&i.value.to_lowercase()) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// Whether a window function, an aggregate or a set function is called anywhere in `v`: an aggregate answers one row
/// for all it reads, and a set function's rows (`unnest(o.tags)`) come after the window numbers the rows it reads, so
/// `__lrn <= n` would keep every element of the first n rows, not n elements.
fn not_row_for_row<V: Visit>(v: &V) -> bool {
    walk::visit_expressions(v, |e| {
        let Expr::Function(f) = e else { return ControlFlow::Continue(()) };
        let name = crate::friendly::function_name(f);
        match f.over.is_some() || AGGREGATES.contains(&name.as_str()) || crate::lateral::SET_FUNCTIONS.contains(&name.as_str()) {
            true => ControlFlow::Break(()),
            false => ControlFlow::Continue(()),
        }
    })
    .is_break()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    /// Whether `sql` was rewritten, and the statement as it reads after.
    fn rewritten(sql: &str) -> (bool, String) {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let changed = rewrite(&mut stmts[0]).unwrap();
        (changed, stmts[0].to_string())
    }

    #[test]
    fn top_n_per_group_is_a_left_join_on_a_numbered_subquery() {
        let (changed, sql) = rewritten("SELECT c.name, top.amount FROM customers c LEFT JOIN LATERAL (SELECT o.amount FROM orders o WHERE o.customer_id = c.id ORDER BY o.amount DESC LIMIT 1) top ON true");
        assert!(changed);
        assert!(sql.contains("top.__lk1 = c.id"), "{sql}");
        assert!(sql.contains("row_number() OVER (PARTITION BY o.customer_id ORDER BY o.amount DESC)"), "{sql}");
        assert!(sql.contains("__lrn <= 1"), "{sql}");
        assert!(!sql.contains("LATERAL"), "{sql}");
    }

    #[test]
    fn a_cross_join_without_a_limit_joins_on_its_keys() {
        let (changed, sql) = rewritten("SELECT c.name, o.id FROM customers c CROSS JOIN LATERAL (SELECT id FROM orders o WHERE o.customer_id = c.id AND o.amount > 5) o");
        assert!(changed);
        assert!(sql.contains("ON o.__lk1 = c.id"), "{sql}");
        assert!(sql.contains("WHERE (o.amount > 5)"), "{sql}");
        assert!(!sql.contains("LATERAL"), "{sql}");
    }

    #[test]
    fn the_comma_form_is_an_inner_join_on_its_keys() {
        let (changed, sql) = rewritten("SELECT a.k, b.id FROM (SELECT DISTINCT k FROM t) a, LATERAL (SELECT t.id FROM t WHERE t.k = a.k ORDER BY t.id LIMIT 2) b");
        assert!(changed);
        assert!(sql.contains("ON b.__lk1 = a.k"), "{sql}");
        assert!(!sql.contains("LATERAL"), "{sql}");
    }

    #[test]
    fn what_the_rewrite_does_not_take_stays_as_written() {
        let outer = "SELECT c.name, top.amount FROM customers c LEFT JOIN LATERAL (";
        for inner in [
            "SELECT * FROM orders o WHERE o.customer_id = c.id LIMIT 1",
            "SELECT count(*) AS amount FROM orders o WHERE o.customer_id = c.id",
            "SELECT o.amount + c.id AS amount FROM orders o WHERE o.customer_id = c.id LIMIT 1",
            "SELECT o.amount FROM orders o WHERE o.customer_id > c.id ORDER BY o.amount LIMIT 1",
            "SELECT o.amount FROM orders o WHERE o.customer_id = c.id ORDER BY o.amount LIMIT 1 OFFSET 2",
            "SELECT o.amount FROM orders o WHERE o.customer_id = c.id ORDER BY 1 LIMIT 1",
            "SELECT DISTINCT o.amount FROM orders o WHERE o.customer_id = c.id LIMIT 1",
            "SELECT o.amount AS x FROM orders o WHERE o.customer_id = c.id ORDER BY x LIMIT 1",
            "SELECT unnest(o.tags) AS amount FROM orders o WHERE o.customer_id = c.id LIMIT 2",
        ] {
            let sql = format!("{outer}{inner}) top ON true");
            let (changed, text) = rewritten(&sql);
            assert!(!changed, "{sql}");
            assert_eq!(text, Parser::parse_sql(&GenericDialect {}, &sql).unwrap()[0].to_string());
        }
        // (a `*` in the outer query would show the columns the rewrite adds)
        let star = "SELECT * FROM customers c LEFT JOIN LATERAL (SELECT o.amount FROM orders o WHERE o.customer_id = c.id LIMIT 1) top ON true";
        assert!(!rewritten(star).0);
    }
}
