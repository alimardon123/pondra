//! SQL as text: names as SQL resolves them, a table's name inside the lake, a statement written back.
use anyhow::{bail, ensure, Result};
use datafusion::sql::sqlparser::ast::{self, visit_expressions_mut, Expr, FunctionArg, FunctionArgExpr, Statement};
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

/// The statement's first word, lowercased (`create` for `CREATE TABLE …`, past comments): what a
/// statement's own regexes are looked at for, so a query compiles none of them.
pub fn head(sql: &str) -> String {
    first_word(sql).split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or_default().to_ascii_lowercase()
}

/// `local` by the lake's name (the front has no `Lake`).
pub fn local_in(lake: &str, name: &str) -> Option<String> {
    match name.split('.').collect::<Vec<_>>()[..] {
        [t] => Some(t.to_string()),
        [s, t] => Some(join(s, t)),
        [l, s, t] if l == lake => Some(join(s, t)),
        _ => None,
    }
}

/// A call's arguments as written: those by position, then those by name (`day => …`, or `day := …`),
/// each name once. Every procedure and table function of Pondra's own reads its arguments through
/// this, so `=>` means one thing everywhere.
pub struct Args<'a> {
    pub given: Vec<&'a Expr>,
    pub named: Vec<(String, &'a Expr)>,
}

/// `what` names the call in errors (`run`, `read_csv`, …).
pub fn args<'a>(what: &str, args: &'a [FunctionArg]) -> Result<Args<'a>> {
    let mut out = Args { given: vec![], named: vec![] };
    for a in args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                ensure!(out.named.is_empty(), "{what}: arguments by position come before those by name");
                out.given.push(e);
            }
            FunctionArg::Named { name, arg: FunctionArgExpr::Expr(e), .. } | FunctionArg::ExprNamed { name: Expr::Identifier(name), arg: FunctionArgExpr::Expr(e), .. } => {
                let n = ident(name);
                ensure!(!out.named.iter().any(|(m, _)| *m == n), "{what}: {n} given twice");
                out.named.push((n, e));
            }
            _ => bail!("{what}: an argument is an expression, or name => expression"),
        }
    }
    Ok(out)
}

pub const ROW_ID: &str = "_row_id";
pub const VERSION: &str = "_version";
pub const CREATED: &str = "_created_at";
pub const UPDATED: &str = "_updated_at";
pub const NAMES: [&str; 4] = [ROW_ID, VERSION, CREATED, UPDATED];

/// VARIANT (JSON) values go to Python as what they hold, and come back as JSON.
pub fn is_json(sql_type: Option<&str>) -> bool { sql_type.is_some_and(|t| ["VARIANT", "JSON"].contains(&t.trim().to_uppercase().as_str())) }

/// A parameter that takes any value as it comes, not cast: none given, `ANY` (Postgres's
/// `anyelement`), or VARIANT / JSON (a JSON document's text, or any other value).
pub fn loose(sql_type: Option<&str>) -> bool { sql_type.is_none_or(|t| is_json(Some(t)) || ["ANY", "ANYELEMENT"].contains(&t.trim().to_uppercase().as_str())) }

/// `name(args…)` as SQL's tree has a call.
pub fn function(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Function(ast::Function {
        name: ast::ObjectName::from(vec![ast::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args.into_iter().map(|a| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(a))).collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}
