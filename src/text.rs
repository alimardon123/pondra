//! SQL as text: names as SQL resolves them, a table's name inside the lake, a statement written back.
use anyhow::{bail, ensure, Result};
use datafusion::sql::sqlparser::ast::{self, visit_expressions_mut, Expr, FunctionArg, FunctionArgExpr, Query, SelectItem, SetExpr, Statement, Value, VisitMut};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::LazyLock;

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
pub fn is_json_type(sql_type: Option<&str>) -> bool { sql_type.is_some_and(|t| ["VARIANT", "JSON"].contains(&t.trim().to_uppercase().as_str())) }

/// A parameter that takes any value as it comes, not cast: none given, `ANY` (Postgres's
/// `anyelement`), or VARIANT / JSON (a JSON document's text, or any other value).
pub fn loose(sql_type: Option<&str>) -> bool { sql_type.is_none_or(|t| is_json_type(Some(t)) || ["ANY", "ANYELEMENT"].contains(&t.trim().to_uppercase().as_str())) }

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

/// `s` is a uuid as Postgres writes one, the only way a uuid column holds it: 36 characters, lower
/// case, hyphens after the 8th, 12th, 16th and 20th digits.
pub fn is_uuid(s: &str) -> bool {
    s.len() == 36 && s.bytes().enumerate().all(|(i, b)| if matches!(i, 8 | 13 | 18 | 23) { b == b'-' } else { b.is_ascii_digit() || (b'a'..=b'f').contains(&b) })
}

/// The functions that make JSON: the Postgres port traces their arguments (`pg_types`), so a JSON or
/// whole-row argument is written as JSON.
pub const JSON_MAKERS: &[&str] = &["to_json", "to_jsonb", "row_to_json", "json_build_object", "jsonb_build_object", "json_build_array",
    "jsonb_build_array", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg"];

/// Whether `sql` calls a JSON maker (`JSON_MAKERS`), as a name followed by `(`. Only a statement with one has its arguments traced.
pub fn makes_json(sql: &str) -> bool {
    static CALL: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(&format!(r"(?i)\b(?:{})\s*\(", JSON_MAKERS.join("|"))).expect("a regex"));
    CALL.is_match(sql)
}

/// Could `sql` name a type of one's own, in a cast or one of the enum functions? (Only then is
/// it parsed for one: the built-in types' names are left alone.) A cast to uuid, json or jsonb,
/// an array type, a set function (which may read a column: `lateral::functions`), a JSON maker's
/// call and a `||` that may meet a jsonb document are parsed too.
pub fn names_a_type(sql: &str) -> bool {
    if sql.contains("[]") {
        return true; // (an array type: a string cast to one reads as a Postgres array literal, `pg_values::array_literals`)
    }
    // (a set function: one in a FROM that reads a column is LATERAL's, `lateral::functions`; JSON's give a document's
    // rows, `pg_json_sets::sets`)
    static SET_FUNCTION: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?i)\b(?:generate_series|range|unnest|jsonb?_(?:array_elements(?:_text)?|each(?:_text)?|object_keys|to_record(?:set)?))\s*\(").expect("a regex")
    });
    if SET_FUNCTION.is_match(sql) || makes_json(sql) {
        return true;
    }
    // (`||` merges two jsonb documents: a statement with one is read when it may name a table or JSON, since a
    // `SELECT 'n' || 5` has no document in it and is text's)
    static TABLE_OR_JSON: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\b(?:from|update|into)\b|json").expect("a regex"));
    if sql.contains("||") && TABLE_OR_JSON.is_match(sql) {
        return true;
    }
    static NAMED: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?i)(?:::|\bas)\s*"?([a-z_][a-z0-9_]*)"?\s*(?:[.]\s*"?([a-z_][a-z0-9_]*))?|\b(?:enum_(?:range|first|last)|pg_typeof)\s*\(|'(\{?[0-9a-fA-F]{8}-?(?:[0-9a-fA-F]{4}-?){3}[0-9a-fA-F]{12}\}?)'"#).expect("a regex"));
    const BUILT_IN: &[&str] = &["int", "integer", "bigint", "smallint", "tinyint", "int2", "int4", "int8", "hugeint", "ubigint", "uinteger", "usmallint", "utinyint", "float", "float4",
        "float8", "real", "double", "decimal", "numeric", "dec", "text", "varchar", "char", "character", "string", "bpchar", "bool", "boolean", "date", "time", "timestamp", "timestamptz",
        "datetime", "interval", "json", "jsonb", "uuid", "bytea", "blob", "binary", "varbinary", "bytes", "regclass", "regtype", "regproc", "regnamespace", "oid", "name", "variant", "bit"];
    NAMED.captures_iter(sql).any(|c| match (c.get(1), c.get(2)) {
        _ if c.get(3).is_some() => c.get(3).is_some_and(|s| !is_uuid(s.as_str())), // (`'A0EE…'`: read as a uuid where one is compared with it or written into one)
        (None, _) => true, // (enum_range(…), pg_typeof(…))
        (Some(_), Some(_)) => true,
        (Some(n), None) => ["uuid", "json", "jsonb"].iter().any(|t| n.as_str().eq_ignore_ascii_case(t)) || !BUILT_IN.contains(&n.as_str().to_lowercase().as_str()), // (a cast to uuid, json or jsonb: written as text)
    })
}

/// An expression's place in its statement, which tells it from another of the same text.
pub(crate) fn at(e: &Expr) -> usize { std::ptr::from_ref(e) as usize }

/// Runs `change` over `stmt`, then gives each item it changed that has no alias (in a SELECT, or a RETURNING)
/// its text as written for a name, so a change made where SQL comes in never renames a column:
/// `SELECT json_agg(t) FROM t` still answers `json_agg(t)`. Items are found by their places, so `change` must
/// change expressions where they are (moving a query is fine; a query it clones is new to it). Whether
/// `change` changed anything.
pub(crate) fn names_kept(stmt: &mut Statement, change: impl FnOnce(&mut Statement) -> bool) -> bool {
    let mut written = HashMap::new();
    let _ = stmt.visit(&mut Items(|item: &mut SelectItem| {
        if let SelectItem::UnnamedExpr(e) = item {
            written.insert(at(e), e.to_string());
        }
    }));
    if !change(stmt) {
        return false;
    }
    let _ = stmt.visit(&mut Items(|item: &mut SelectItem| {
        if let SelectItem::UnnamedExpr(e) = item {
            if let Some(text) = written.get(&at(e)).filter(|text| **text != e.to_string()) {
                let expr = std::mem::replace(e, Expr::value(Value::Null));
                *item = SelectItem::ExprWithAlias { expr, alias: ast::Ident::with_quote('"', text.clone()) };
            }
        }
    }));
    true
}

/// Calls its function with each item of every query in a statement (both sides of a UNION) and of every
/// RETURNING.
struct Items<F>(F);

impl<F: FnMut(&mut SelectItem)> ast::VisitorMut for Items<F> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        fn items(body: &mut SetExpr, f: &mut impl FnMut(&mut SelectItem)) {
            match body {
                SetExpr::Select(s) => s.projection.iter_mut().for_each(f),
                SetExpr::SetOperation { left, right, .. } => {
                    items(left, f);
                    items(right, f);
                }
                _ => {} // (a query in parentheses is a query of its own: this visits it too)
            }
        }
        items(&mut q.body, &mut self.0);
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<()> {
        let returning = match s {
            Statement::Insert(i) => i.returning.as_mut(),
            Statement::Update(u) => u.returning.as_mut(),
            Statement::Delete(d) => d.returning.as_mut(),
            _ => None,
        };
        returning.into_iter().flatten().for_each(&mut self.0);
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    /// `sql` as one statement, read by the generic dialect (the SQL these tests use is plain).
    fn one(sql: &str) -> Option<Statement> {
        let mut parsed = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
        (parsed.len() == 1).then(|| parsed.pop()).flatten()
    }

    #[test]
    fn a_concatenation_is_parsed_only_when_it_may_meet_a_document() {
        for sql in ["SELECT 'n' || 5", "SELECT 'a' || 'b' || 'c'", "SELECT name || 'x'"] {
            assert!(!names_a_type(sql), "{sql}");
        }
        for sql in ["SELECT 'n' || name FROM t", "UPDATE t SET doc = doc || '{}'", "INSERT INTO t SELECT 'a' || 'b'", "SELECT '{}' || '{}'::jsonb", "SELECT to_jsonb(1) || '{}'"] {
            assert!(names_a_type(sql), "{sql}");
        }
        assert!(names_a_type("SELECT json_agg(x) FROM t") && names_a_type("SELECT * FROM generate_series(1, t.n)") && names_a_type("SELECT * FROM jsonb_each(t.doc)") && names_a_type("SELECT '{a}'::text[]"));
    }

    #[test]
    fn names_kept_by_place() {
        // (every `upper(…)` becomes 'x': the items it changed keep their names, the others are as they were)
        let upper = |s: &mut Statement| {
            let mut changed = false;
            let _ = ast::visit_expressions_mut(s, |e| {
                if matches!(e, Expr::Function(f) if object(&f.name) == "upper") {
                    *e = Expr::value(Value::SingleQuotedString("x".into()));
                    changed = true;
                }
                ControlFlow::<()>::Continue(())
            });
            changed
        };
        let kept = |sql: &str| {
            let mut s = one(sql).unwrap();
            let changed = names_kept(&mut s, upper);
            (changed, s.to_string())
        };
        assert_eq!(
            kept("SELECT upper(a), upper(a) AS b, lower(a), (SELECT upper(a) FROM t) FROM t UNION ALL SELECT upper(a), 'y', 'z', 'w' FROM t"),
            (true, r#"SELECT 'x' AS "upper(a)", 'x' AS b, lower(a), (SELECT 'x' AS "upper(a)" FROM t) AS "(SELECT upper(a) FROM t)" FROM t UNION ALL SELECT 'x' AS "upper(a)", 'y', 'z', 'w' FROM t"#.to_string())
        );
        assert_eq!(kept("WITH c AS (SELECT upper(a) FROM t) SELECT * FROM c"), (true, r#"WITH c AS (SELECT 'x' AS "upper(a)" FROM t) SELECT * FROM c"#.to_string()));
        assert_eq!(kept("UPDATE t SET a = upper(b) RETURNING upper(a), a"), (true, r#"UPDATE t SET a = 'x' RETURNING 'x' AS "upper(a)", a"#.to_string()));
        assert_eq!(kept("INSERT INTO t SELECT upper(a) FROM s RETURNING upper(a)"), (true, r#"INSERT INTO t SELECT 'x' AS "upper(a)" FROM s RETURNING 'x' AS "upper(a)""#.to_string()));
        assert_eq!(kept("SELECT lower(a) FROM t"), (false, "SELECT lower(a) FROM t".to_string()));
        // (a name holding a double quote is written so it reads back as itself)
        let (_, text) = kept(r#"SELECT upper("A") FROM t"#);
        match &one(&text).unwrap() {
            Statement::Query(q) => match &*q.body {
                SetExpr::Select(s) => assert!(matches!(&s.projection[0], SelectItem::ExprWithAlias { alias, .. } if alias.value == r#"upper("A")"#), "{text}"),
                _ => panic!("{text}"),
            },
            _ => panic!("{text}"),
        }
    }
}
