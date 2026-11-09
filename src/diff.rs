//! `pondra.diff(a, b)` in a FROM (ADR-047, Trellis T1): the rows of b that differ from a's, in the change feed's words
//! (`_change_type`: insert, delete, update_preimage, update_postimage), and on an update's two rows the names of the
//! columns whose values differ (`_changed`, NULL on the others).
//!
//! Either side is a table's or a view's name, as a string (`'prod.sales.orders'`), or a query (`(SELECT …)`).
//! - Two tables match by their rows' `_row_id` and `_version` (ADR-020): a branch's rows keep both, so the rows both hold
//!   alike match, and a row whose version moved is an update. The answer keeps both columns.
//! - Any other two match whole, as multisets: a row of b's that a lacks, or has fewer copies of, is an insert (once for
//!   each copy more), and one of a's that b lacks a delete. They have no update rows.
//!
//! The front reads it on the parsed tree (`rewrite`): each call becomes the query that answers it, a subquery in its place,
//! so a view it names is expanded as any other. Each side's columns come from planning it (`Host::ask`, nothing run). The
//! two must have the same columns, each of one type on both (`42804` names the first that differs); the answer has b's,
//! in b's order.
use crate::front::Host;
use anyhow::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion::sql::sqlparser::ast::{self, Expr, Statement, TableFactor, Value, VisitorMut};
use std::ops::ControlFlow;

/// The front's pass of this module (`passes.rs`): a call to `pondra.diff` means the tree has something to rewrite.
pub(crate) fn pass() -> crate::passes::Pass {
    crate::passes::Pass { name: "diff.diffs", order: 40, words: vec![], marks: vec![&["pondra", ".", "diff"]], run: crate::passes::Run::Wants(mentioned) }
}

fn mentioned(sql: &str) -> bool {
    static CALL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\bpondra\s*\.\s*diff\b").expect("a regex"));
    CALL.is_match(sql)
}

/// One side of a diff: a table's or a view's name, or a query.
#[derive(Clone, Debug, PartialEq)]
enum Side {
    Name(String),
    Query(String),
}

impl Side {
    /// What a FROM reads it as.
    fn from(&self) -> String {
        match self {
            Side::Name(n) => n.clone(),
            Side::Query(q) => format!("({q})"),
        }
    }

    /// How an error names it.
    fn shown(&self) -> String {
        match self {
            Side::Name(n) => n.clone(),
            Side::Query(q) if q.chars().count() > 40 => format!("({}…)", q.chars().take(40).collect::<String>()),
            Side::Query(q) => format!("({q})"),
        }
    }
}

/// Each `pondra.diff(a, b)` in `stmts` as the query that answers it, innermost first (a side's query may diff too).
/// Whether there was one.
pub async fn rewrite<H: Host>(host: &H, stmts: &mut [Statement]) -> Result<bool> {
    let mut any = false;
    loop {
        let mut first = First { found: None };
        for s in stmts.iter_mut() {
            if let ControlFlow::Break(e) = crate::walk::visit_mut(s, &mut first) {
                return Err(e);
            }
        }
        let Some((a, b)) = first.found else { return Ok(any) };
        let answer = crate::expander::parse_query(&Box::pin(answer_of(host, &a, &b)).await?)?; // (its sides are asked through the front again)
        let mut put = Put { answer: Some(answer) };
        for s in stmts.iter_mut() {
            if let ControlFlow::Break(e) = crate::walk::visit_mut(s, &mut put) {
                return Err(e);
            }
        }
        any = true;
    }
}

/// Whether a table factor is a call to `pondra.diff`.
fn is_diff(t: &TableFactor) -> bool { matches!(t, TableFactor::Table { name, args: Some(_), .. } if crate::text::object(name).eq_ignore_ascii_case("pondra.diff")) }

/// The first call to `pondra.diff` in the walk's order (a call's sides before it), with its sides.
struct First {
    found: Option<(Side, Side)>,
}

impl VisitorMut for First {
    type Break = anyhow::Error;

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
        if self.found.is_some() || !is_diff(t) {
            return ControlFlow::Continue(());
        }
        let TableFactor::Table { args: Some(args), .. } = t else { return ControlFlow::Continue(()) };
        match sides(&args.args) {
            Ok(s) => {
                self.found = Some(s);
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(e),
        }
    }
}

/// Puts the answer in place of the first call `First` found.
struct Put {
    answer: Option<ast::Query>,
}

impl VisitorMut for Put {
    type Break = anyhow::Error;

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
        if !is_diff(t) {
            return ControlFlow::Continue(());
        }
        let Some(answer) = self.answer.take() else { return ControlFlow::Continue(()) };
        let TableFactor::Table { alias, .. } = t else { return ControlFlow::Continue(()) };
        *t = TableFactor::Derived { lateral: false, subquery: Box::new(answer), alias: alias.take(), sample: None };
        ControlFlow::Continue(())
    }
}

/// A call's two sides: each a string naming a table or a view, or a query.
fn sides(args: &[ast::FunctionArg]) -> Result<(Side, Side)> {
    static NAME: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"^[\w"$]+(\.[\w"$]+){0,2}$"#).expect("a regex"));
    const FORM: &str = "pondra.diff takes two sides, each a table's or a view's name or a query: pondra.diff('prod.sales.orders', 'dev.sales.orders')";
    let a = crate::text::args("pondra.diff", args)?;
    if let Some((n, _)) = a.named.first() {
        return Err(crate::codes::coded("42883", format!("pondra.diff has no argument {n}: {FORM}"))); // (as Postgres names a call it has no function for)
    }
    let side = |e: &Expr| -> Result<Side> {
        match e {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(s) if NAME.is_match(s.trim()) => Ok(Side::Name(s.trim().to_string())),
                _ => Err(crate::codes::coded("42883", format!("{FORM}, not {e}"))),
            },
            Expr::Subquery(q) => Ok(Side::Query(q.to_string())),
            _ => Err(crate::codes::coded("42883", format!("{FORM}, not {e}"))),
        }
    };
    match a.given[..] {
        [x, y] => Ok((side(x)?, side(y)?)),
        _ => Err(crate::codes::coded("42883", FORM)),
    }
}

/// A side's columns, with their types, as `SELECT *` gives them, and whether it is a table whose rows keep their
/// `_row_id` and `_version` (a name a query can read both of).
async fn columns_of<H: Host>(host: &H, side: &Side) -> Result<(Vec<(String, DataType)>, bool)> {
    let fields = |df: &datafusion::prelude::DataFrame| df.schema().fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect::<Vec<_>>();
    if let Side::Name(n) = side {
        if let Ok(df) = host.ask(&format!("SELECT *, _row_id, _version FROM {n} LIMIT 0")).await {
            let mut all = fields(&df);
            all.truncate(all.len().saturating_sub(2));
            return Ok((all, true));
        }
    }
    Ok((fields(&host.ask(&format!("SELECT * FROM {} LIMIT 0", side.from())).await?), false))
}

/// The query that answers `pondra.diff(a, b)`.
async fn answer_of<H: Host>(host: &H, a: &Side, b: &Side) -> Result<String> {
    let ((of_a, rows_a), (of_b, rows_b)) = (columns_of(host, a).await?, columns_of(host, b).await?);
    let cols = alike(a, &of_a, b, &of_b)?;
    Ok(answer(&a.from(), &b.from(), &cols, rows_a && rows_b))
}

/// b's columns, when a has the same ones of the same types: else `42804`, naming the first that differs, in b's order
/// and then a's. Text of each width is one type, and so are bytes: a view's text and its table's are alike.
fn alike(a: &Side, of_a: &[(String, DataType)], b: &Side, of_b: &[(String, DataType)]) -> Result<Vec<String>> {
    let kind = |t: &DataType| match t {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        t => t.clone(),
    };
    let differs = |m: String| Err(crate::codes::coded("42804", format!("pondra.diff compares two sides of the same columns: {m}")));
    for (c, t) in of_b {
        match of_a.iter().find(|(n, _)| n == c) {
            None => return differs(format!("{c} is a column of {} and not of {}", b.shown(), a.shown())),
            Some((_, u)) if kind(u) != kind(t) => return differs(format!("{c} is {u} in {} and {t} in {}", a.shown(), b.shown())),
            Some(_) => {}
        }
    }
    if let Some((c, _)) = of_a.iter().find(|(n, _)| !of_b.iter().any(|(m, _)| m == n)) {
        return differs(format!("{c} is a column of {} and not of {}", a.shown(), b.shown()));
    }
    Ok(of_b.iter().map(|(c, _)| c.clone()).collect())
}

/// The answer over `a` and `b` (what a FROM reads), of the columns `cols`: matched by `_row_id` and `_version` when
/// `by_row`, else whole, as multisets.
fn answer(a: &str, b: &str, cols: &[String], by_row: bool) -> String {
    let q = |c: &String| ast::Ident::with_quote('"', c).to_string();
    let list = |t: &str| cols.iter().map(|c| format!("{t}{}", q(c))).collect::<Vec<_>>().join(", ");
    let none = "CAST(NULL AS VARCHAR[])";
    if !by_row {
        let (l, ins, del) = (list(""), format!("SELECT {} FROM {b} y EXCEPT ALL SELECT {} FROM {a} x", list("y."), list("x.")), format!("SELECT {} FROM {a} x EXCEPT ALL SELECT {} FROM {b} y", list("x."), list("y.")));
        return format!("SELECT 'insert' AS _change_type, {l}, {none} AS _changed FROM ({ins}) d UNION ALL SELECT 'delete', {l}, {none} FROM ({del}) d");
    }
    // (each column's name in a one-name list when its two values differ, an empty one when not, all put together)
    let changed = {
        let each: Vec<String> = cols.iter().map(|c| format!("CASE WHEN x.{0} IS DISTINCT FROM y.{0} THEN [{1}] ELSE CAST([] AS VARCHAR[]) END", q(c), Value::SingleQuotedString(c.clone()))).collect();
        match each.len() {
            1 => each[0].clone(),
            _ => format!("array_concat({})", each.join(", ")),
        }
    };
    let (x, y) = (list("x."), list("y."));
    let moved = format!("FROM {a} x JOIN {b} y ON x._row_id = y._row_id AND x._version <> y._version");
    format!(
        "SELECT 'insert' AS _change_type, {y}, y._row_id, y._version, {none} AS _changed FROM {b} y WHERE NOT EXISTS (SELECT 1 FROM {a} x WHERE x._row_id = y._row_id) \
         UNION ALL SELECT 'update_preimage', {x}, x._row_id, x._version, {changed} {moved} \
         UNION ALL SELECT 'update_postimage', {y}, y._row_id, y._version, {changed} {moved} \
         UNION ALL SELECT 'delete', {x}, x._row_id, x._version, {none} FROM {a} x WHERE NOT EXISTS (SELECT 1 FROM {b} y WHERE y._row_id = x._row_id)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;

    /// A session holding `t`, `u` (a branch of t: two rows changed, one gone, one new) and `w` (t's rows, by value,
    /// with no row ids: a copy).
    async fn ctx() -> SessionContext {
        let ctx = SessionContext::new_with_state(datafusion::execution::SessionStateBuilder::new().with_default_features().with_analyzer_rules(crate::optimize::analyzer_rules()).build());
        for sql in [
            "CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a', 10, 1, 1), (2, 'b', 20, 2, 1), (3, 'c', 30, 3, 1), (4, 'd', 40, 4, 1)) AS v(id, name, n, _row_id, _version)",
            "CREATE TABLE u AS SELECT * FROM (VALUES (1, 'a', 10, 1, 1), (2, 'B', 20, 2, 2), (3, 'C', 31, 3, 2), (5, 'e', 50, 5, 1)) AS v(id, name, n, _row_id, _version)",
            "CREATE TABLE w AS SELECT * FROM (VALUES (1, 'a', 10), (2, 'b', 20), (2, 'b', 20), (3, 'c', 30)) AS v(id, name, n)",
        ] {
            ctx.sql(sql).await.unwrap().collect().await.unwrap();
        }
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")).collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut out: Vec<String> = batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).map(move |i| b.columns().iter().map(|c| if c.is_null(i) { "NULL".into() } else { array_value_to_string(c, i).unwrap() }).collect::<Vec<_>>().join(" ")))
            .collect();
        out.sort();
        out
    }

    fn cols() -> Vec<String> { ["id", "name", "n"].map(String::from).to_vec() }

    #[tokio::test]
    async fn two_tables_match_by_row_and_name_what_changed() {
        let ctx = ctx().await;
        let sql = format!("SELECT _change_type, id, name, n, _changed FROM ({}) d", answer("t", "u", &cols(), true));
        assert_eq!(
            rows(&ctx, &sql).await,
            [
                "delete 4 d 40 NULL",
                "insert 5 e 50 NULL",
                "update_postimage 2 B 20 [name]",
                "update_postimage 3 C 31 [name, n]",
                "update_preimage 2 b 20 [name]",
                "update_preimage 3 c 30 [name, n]",
            ]
        );
    }

    #[tokio::test]
    async fn other_sides_match_whole_counting_copies() {
        let ctx = ctx().await;
        // (w holds b twice and t once: one insert; t's d is not in w: a delete; no update rows)
        let sql = format!("SELECT _change_type, id, name, n, _changed FROM ({}) d", answer("(SELECT id, name, n FROM t)", "w", &cols(), false));
        assert_eq!(rows(&ctx, &sql).await, ["delete 4 d 40 NULL", "insert 2 b 20 NULL"]);
        // (one column, and a row whose version moved with it unchanged: an update that names no column)
        let one = ["n".to_string()];
        assert_eq!(
            rows(&ctx, &format!("SELECT _change_type, n, _changed FROM ({}) d", answer("t", "u", &one, true))).await,
            ["delete 40 NULL", "insert 50 NULL", "update_postimage 20 []", "update_postimage 31 [n]", "update_preimage 20 []", "update_preimage 30 [n]"]
        );
    }

    #[test]
    fn sides_are_names_or_queries_and_the_columns_must_agree() {
        let parsed = |sql: &str| -> Result<(Side, Side)> {
            let q = crate::expander::parse_query(sql).unwrap();
            let ast::SetExpr::Select(s) = *q.body else { panic!() };
            let TableFactor::Table { args: Some(args), .. } = &s.from[0].relation else { panic!() };
            sides(&args.args)
        };
        assert_eq!(parsed("SELECT * FROM pondra.diff('prod.sales.orders', (SELECT 1 AS a))").unwrap(), (Side::Name("prod.sales.orders".into()), Side::Query("SELECT 1 AS a".into())));
        assert!(parsed("SELECT * FROM pondra.diff('a', 'b', key => 'id')").unwrap_err().to_string().contains("no argument key"));
        assert!(parsed("SELECT * FROM pondra.diff('a; DROP TABLE t', 'b')").is_err());
        assert!(parsed("SELECT * FROM pondra.diff('a')").is_err());
        let (a, b) = (Side::Name("prod.v".into()), Side::Name("dev.v".into()));
        let int = |c: &str| (c.to_string(), DataType::Int64);
        let text = |c: &str, t: DataType| (c.to_string(), t);
        assert_eq!(alike(&a, &[int("x"), text("s", DataType::Utf8)], &b, &[text("s", DataType::Utf8View), int("x")]).unwrap(), ["s", "x"]);
        let e = alike(&a, &[int("x")], &b, &[int("x"), int("y")]).unwrap_err();
        assert_eq!(crate::codes::of(&e), "42804");
        assert!(e.to_string().contains("y is a column of dev.v and not of prod.v"), "{e}");
        let e = alike(&a, &[int("x"), int("z")], &b, &[int("x")]).unwrap_err();
        assert!(e.to_string().contains("z is a column of prod.v and not of dev.v"), "{e}");
        let e = alike(&a, &[(String::from("x"), DataType::Int32)], &b, &[int("x")]).unwrap_err();
        assert!(e.to_string().contains("x is Int32 in prod.v and Int64 in dev.v"), "{e}");
    }
}
