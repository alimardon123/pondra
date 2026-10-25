//! The rules of an INSERT's rows, as SQL text, in the core: which columns a row gives (`implicit`, `row_columns`), what a
//! DEFAULT or a column a row leaves out takes (`rows_for`), and the 42601 a row of the wrong width gets (`width_refused`).
//! A node (`write.rs`, `change.rs`, `temp.rs`) and a page (`page.rs`) write their rows by these, so they answer alike.
//! Nothing here reads files, storage or the catalog.
use anyhow::Result;
use datafusion::sql::sqlparser::ast;

/// Is this text the word DEFAULT (any case, spaces aside)? A quoted `"default"` is a column's name.
pub fn is_word(text: &str) -> bool { text.trim().eq_ignore_ascii_case("default") }

/// The rows of a query that is only `VALUES` (no ORDER BY), parsed.
pub(crate) fn plain_values(query: &str) -> Option<ast::Values> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let q = Parser::new(&GenericDialect {}).try_with_sql(query).ok()?.parse_query().ok()?;
    match (*q.body, &q.order_by) {
        (ast::SetExpr::Values(v), None) => Some(v),
        _ => None,
    }
}

/// A query that is one row of `VALUES`: its rows can't hold a key twice (`change::merge` asks nothing more of them).
pub(crate) fn one_row(query: &str) -> bool { plain_values(query).is_some_and(|v| v.rows.len() == 1) }

/// A query that is one row of `VALUES` whose values read nothing but themselves: no subquery, and no function but the
/// statement's time (`now()`, `current_date`, …), where a function may be a routine that reads a table. A one-row upsert's
/// row is worked out once, before its key's lock (`txn::upsert_point`), so a value that read a table would read it before
/// the lock, where the planned MERGE reads it after: such a row stays with the planned MERGE.
pub(crate) fn constant_row(query: &str) -> bool {
    const TIME: [&str; 6] = ["now", "current_timestamp", "current_date", "current_time", "localtimestamp", "localtime"];
    let Some(v) = plain_values(query).filter(|v| v.rows.len() == 1) else { return false };
    let reads = ast::visit_expressions(&v, |e| match e {
        ast::Expr::Subquery(_) | ast::Expr::Exists { .. } | ast::Expr::InSubquery { .. } => std::ops::ControlFlow::Break(()),
        ast::Expr::Function(f) if !TIME.iter().any(|t| f.name.to_string().eq_ignore_ascii_case(t)) => std::ops::ControlFlow::Break(()),
        _ => std::ops::ControlFlow::Continue(()),
    });
    reads.is_continue()
}

/// The width of a query's rows, when the query fixes it: a plain `VALUES` whose rows share one width, or a query whose
/// body is a SELECT (the leftmost of a set operation) with no `*`, `t.*` or `COLUMNS(…)` among its items. None for any
/// other query, which takes the table's columns as they come.
pub(crate) fn rows_width(query: &str) -> Option<usize> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let q = Parser::new(&GenericDialect {}).try_with_sql(query).ok()?.parse_query().ok()?;
    match q.body.as_ref() {
        ast::SetExpr::Values(_) if q.order_by.is_some() => None, // (`VALUES … ORDER BY` isn't plain: `plain_values` says so too)
        body => body_width(body),
    }
}

fn body_width(body: &ast::SetExpr) -> Option<usize> {
    match body {
        ast::SetExpr::Values(v) => {
            let first = v.rows.first()?.len();
            v.rows.iter().all(|r| r.len() == first).then_some(first)
        }
        ast::SetExpr::Select(s) => (!s.projection.iter().any(is_wildcard)).then_some(s.projection.len()),
        ast::SetExpr::Query(q) => body_width(&q.body),
        ast::SetExpr::SetOperation { left, .. } => body_width(left),
        _ => None,
    }
}

/// `*`, `t.*`, DuckDB's `COLUMNS(…)` and `unnest(…)` (a struct's fields, several columns): the items a query's width
/// isn't known from until it is planned.
fn is_wildcard(item: &ast::SelectItem) -> bool {
    let expr = match item {
        ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(..) => return true,
        ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => e,
        _ => return false,
    };
    matches!(expr, ast::Expr::Function(f) if ["columns", "unnest"].iter().any(|n| f.name.to_string().eq_ignore_ascii_case(n)))
}

/// The columns an INSERT's whole rows are built for (`rows_for`): the table's, its delete marker only when the column list
/// names it (`by_position`).
pub(crate) fn row_columns(columns: impl IntoIterator<Item = String>, names: &[String]) -> Vec<String> {
    columns.into_iter().filter(|c| c != "_deleted" || names.contains(c)).collect()
}

/// Does every row of this `VALUES` say DEFAULT at position `i`?
pub(crate) fn defaulted(query: &str, i: usize) -> bool {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let Ok(q) = Parser::new(&GenericDialect {}).try_with_sql(query).and_then(|mut p| p.parse_query()) else { return false };
    matches!(q.body.as_ref(), ast::SetExpr::Values(v) if v.rows.iter().all(|r| r.get(i).is_some_and(is_default)))
}

/// Is this `VALUES` item the word DEFAULT (the column's default, or NULL)?
fn is_default(e: &ast::Expr) -> bool { matches!(e, ast::Expr::Identifier(i) if i.quote_style.is_none() && is_word(&i.value)) }

/// `INSERT INTO t VALUES (…, DEFAULT, …)`: the query says DEFAULT somewhere.
pub(crate) fn says_default(q: &ast::Query) -> bool { matches!(q.body.as_ref(), ast::SetExpr::Values(v) if v.rows.iter().any(|r| r.iter().any(is_default))) }

/// Does this query's `VALUES` say DEFAULT somewhere (`says_default`), read from its text? False if it doesn't parse.
pub(crate) fn says_default_text(query: &str) -> bool {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let Ok(q) = Parser::new(&GenericDialect {}).try_with_sql(query).and_then(|mut p| p.parse_query()) else { return false };
    says_default(&q)
}

/// `INSERT INTO table (names) query` as a query of all its `columns`, in order (`whole_rows`; a
/// temporary table's too, `temp.rs`).
pub fn rows_for(table: &str, columns: &[String], names: &[String], query: &str, default: &dyn Fn(&str) -> Option<String>) -> Result<String> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    if let Some(n) = names.iter().find(|n| !columns.contains(n)) {
        return Err(crate::codes::coded("42703", format!("column \"{n}\" of relation \"{table}\" does not exist (its columns: {})", columns.join(", "))));
    }
    if let Some((_, n)) = names.iter().enumerate().find(|(i, n)| names[..*i].contains(n)) {
        return Err(crate::codes::coded("42701", format!("column \"{n}\" specified more than once (INSERT INTO {table})")));
    }
    let at = |c: &String| names.iter().position(|n| n == c);
    Parser::new(&GenericDialect {}).try_with_sql(query)?.parse_query()?; // (what's wrong with it, said)
    let q = |c: &str| format!("\"{}\"", c.replace('"', "\"\""));
    if let Some(v) = plain_values(query) {
        let mut rows = vec![];
        for r in &v.rows {
            if r.len() != names.len() {
                return Err(width_refused(table, names.len(), r.len()));
            }
            let value = |c: &String| match at(c) {
                Some(i) if !is_default(&r[i]) => r[i].to_string(),
                _ => default(c).unwrap_or_else(|| "NULL".into()),
            };
            rows.push(format!("({})", columns.iter().map(value).collect::<Vec<_>>().join(", ")));
        }
        let values = format!("VALUES {}", rows.join(", "));
        // (a list that names the delete marker names every column: `fit_with` takes the marker by name, where a plain
        // VALUES would fill the table's columns by position, the marker among them)
        return Ok(match columns.iter().any(|c| c == "_deleted") {
            true => format!("SELECT * FROM ({values}) AS __pondra_v({})", columns.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ")),
            false => values,
        });
    }
    // A query of another width is refused as Postgres words it, not as the alias list below says it (DataFusion's error).
    if let Some(w) = rows_width(query).filter(|&w| w != names.len()) {
        return Err(width_refused(table, names.len(), w));
    }
    let list = columns.iter().map(|c| match at(c) {
        Some(i) => format!("__pondra_q.c{i} AS {}", q(c)),
        None => format!("{} AS {}", default(c).unwrap_or_else(|| "NULL".into()), q(c)),
    });
    let aliases = (0..names.len()).map(|i| format!("c{i}")).collect::<Vec<_>>().join(", ");
    Ok(format!("SELECT {} FROM ({query}) AS __pondra_q({aliases})", list.collect::<Vec<_>>().join(", ")))
}

/// The 42601 for rows `given` wide written into `names` columns, in Postgres's words.
pub(crate) fn width_refused(table: &str, names: usize, given: usize) -> anyhow::Error {
    let what = if given > names { "INSERT has more expressions than target columns" } else { "INSERT has more target columns than expressions" };
    crate::codes::coded("42601", format!("{what} (INSERT INTO {table} names {names} columns, and the query gives {given})"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::fit::implicit;

    #[test]
    fn the_implicit_column_list_is_the_first_the_values_give() {
        assert_eq!(rows_width("VALUES (1, 2), (3, 4)"), Some(2));
        assert_eq!(rows_width("VALUES (1), (2, 3)"), None);
        assert_eq!(rows_width("SELECT 1, 2"), Some(2));
        assert_eq!(rows_width("SELECT * FROM t"), None);
        assert_eq!(rows_width("SELECT unnest(s) FROM t"), None);
        assert_eq!(rows_width("SELECT 1 UNION SELECT 2"), Some(1));
        assert_eq!(rows_width("VALUES (1) ORDER BY 1"), None);
        assert!(one_row("VALUES (1, 'a')") && !one_row("VALUES (1), (2)") && !one_row("SELECT 1") && !one_row("VALUES (1) ORDER BY 1"));
        assert!(constant_row("VALUES (1, 'a', -2.5, CAST('3' AS INT), DATE '2026-10-25', now(), CURRENT_DATE, $1)"));
        assert!(!constant_row("VALUES (1, (SELECT n FROM c WHERE id = 1) + 1)") && !constant_row("VALUES ((SELECT max(id) + 1 FROM t), 'x')"));
        assert!(!constant_row("VALUES (1, lookup(2))") && !constant_row("VALUES (1, EXISTS (SELECT 1))") && !constant_row("VALUES (1), (2)"));
        let names: Vec<String> = ["a", "b", "c"].iter().map(|n| n.to_string()).collect();
        assert_eq!(implicit(&names, Some(1)), ["a"]);
        assert_eq!(implicit(&names, Some(2)), ["a", "b"]);
        assert_eq!(implicit(&names, Some(3)), ["a", "b", "c"]);
        assert_eq!(implicit(&names, Some(5)), ["a", "b", "c"]);
        assert_eq!(implicit(&names, None), ["a", "b", "c"]);
    }

    /// A column the table hasn't, or one named twice, is refused as Postgres refuses it (42703, 42701), on every form.
    #[test]
    fn a_column_named_wrong_or_twice_is_refused_with_postgres_codes() {
        let columns: Vec<String> = ["a", "b"].iter().map(|n| n.to_string()).collect();
        let none = |_: &str| None;
        let unknown = rows_for("t", &columns, &["nope".to_string()], "VALUES (1)", &none).unwrap_err();
        assert_eq!((crate::codes::of(&unknown), unknown.to_string().starts_with("column \"nope\" of relation \"t\" does not exist")), ("42703", true));
        let twice = rows_for("t", &columns, &["a".to_string(), "a".to_string()], "VALUES (1, 2)", &none).unwrap_err();
        assert_eq!(crate::codes::of(&twice), "42701");
    }
}
