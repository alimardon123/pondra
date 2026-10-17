//! SQL as text: names as SQL resolves them, a table's name inside the lake, a statement written back.
use datafusion::sql::sqlparser::ast::{self, visit_expressions_mut, Expr, Statement};
use std::ops::ControlFlow;

/// A name as SQL resolves it: its parts, unquoted ones in lower case, joined by dots.
pub fn object(n: &ast::ObjectName) -> String {
    let part = |p: &ast::ObjectNamePart| p.as_ident().map(ident).unwrap_or_else(|| p.to_string());
    n.0.iter().map(part).collect::<Vec<_>>().join(".")
}

/// One part of a name: as written if quoted, else in lower case.
pub fn ident(i: &ast::Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }

/// `sql` from its first word on: past leading whitespace and comments (`-- …`, `/* … */`).
pub fn first_word(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.find('\n').map_or("", |i| &rest[i + 1..]).trim_start();
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = rest.find("*/").map_or("", |i| &rest[i + 2..]).trim_start();
        } else {
            return s;
        }
    }
}

#[cfg_attr(not(feature = "server"), allow(dead_code))]
pub(crate) fn text(stmts: &[Statement]) -> String { stmts.iter().map(sql).collect::<Vec<_>>().join(";\n") }

/// A statement as SQL text again. sqlparser writes `- -3` (or `- $x`, `$x` bound to -3) as `--3`,
/// which reads back as a comment, so a minus before anything written with a minus first gets
/// parentheses.
pub fn sql(s: &Statement) -> String {
    let twice = |e: &Expr| matches!(e, Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } if expr.to_string().starts_with('-'));
    let mut doubled = false;
    let _ = ast::visit_expressions(s, |e| {
        doubled |= twice(e);
        ControlFlow::<()>::Continue(())
    });
    if !doubled {
        return s.to_string();
    }
    let mut s = s.clone();
    let _ = visit_expressions_mut(&mut s, |e| {
        if let (true, Expr::UnaryOp { expr, .. }) = (twice(e), &mut *e) {
            **expr = Expr::Nested(Box::new(std::mem::replace(&mut **expr, Expr::Value(ast::Value::Null.into()))));
        }
        ControlFlow::<()>::Continue(())
    });
    s.to_string()
}

pub const PUBLIC: &str = "public";

/// A table's schema, and its name within it.
pub fn split(name: &str) -> (&str, &str) { name.split_once('.').unwrap_or((PUBLIC, name)) }

/// The name a table of `schema` has inside the lake.
pub fn join(schema: &str, table: &str) -> String {
    if schema == PUBLIC { table.to_string() } else { format!("{schema}.{table}") }
}

/// Might `sql` name table `t`? A word match on its last part (a session registers the tables that
/// might be read: one too many costs nothing).
pub fn mentions(sql: &str, t: &str) -> bool {
    let last = split(t).1.to_lowercase();
    sql.to_lowercase().split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '-')).any(|w| w == last)
}
