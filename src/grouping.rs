//! Postgres's grouping of what sqlparser's generic dialect groups otherwise, where SQL comes in (`path::door`) and where
//! a stored text is read again (a routine's body, a view's CHECK, `pg_trace`). Each part generic groups otherwise is
//! regrouped the way Postgres groups it and nested, so the text reads the same under the generic dialect in every later
//! stage, DataFusion's own parse too. Text with nothing to regroup is never parsed here.
//!
//! - **The JSON operators** (round 35). Generic ranks `->` and its kin below `=`, `IS`, `LIKE`, `IN`, `BETWEEN` and
//!   `||`. Postgres ranks them above all of those (its `Op`, 70, with `||`, `|`, `&`, `#`, `<<` and `>>`), and below
//!   `+`. So `doc ->> 'a' = 'x'` would read as `doc ->> ('a' = 'x')` and fail.
//! - **`IS [NOT] DISTINCT FROM`'s right side.** Generic reads the whole rest of the expression there, so
//!   `a IS DISTINCT FROM b OR c` would read as `a IS DISTINCT FROM (b OR c)`: an error, or with booleans a wrong answer
//!   and no error. Postgres and DuckDB end the right side at anything as loose as `IS` or looser (`AND`, `OR`,
//!   `IS NULL`, …), and read `(a IS DISTINCT FROM b) OR c`.
use datafusion::sql::sqlparser::ast::{self, BinaryOperator, VisitorMut};
use datafusion::sql::sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;
use std::borrow::Cow;
use std::ops::ControlFlow;

/// Postgres's rank of `Op`: the JSON operators and `||` share it, and it is left-associative.
const OP: u8 = 70;

/// The text grouped as Postgres groups it: `doc ->> 'a' = 'x'` reads as `(doc ->> 'a') = 'x'`, `doc ? 'a'` as
/// `jsonb_exists(doc, 'a')`, and `a IS DISTINCT FROM b OR c` as `(a IS DISTINCT FROM b) OR c`. Text that needs none of
/// it (and any text neither dialect reads) comes back borrowed, as it was.
pub fn text(sql: &str) -> Cow<'_, str> {
    if !wanted(sql) {
        return Cow::Borrowed(sql);
    }
    let grouped = match Parser::parse_sql(&GenericDialect {}, sql) {
        Ok(stmts) => regrouped(stmts),
        // (the generic dialect reads `?` as a placeholder, so text with one is read as Postgres's dialect reads it)
        Err(_) if sql.contains('?') => questions(sql),
        Err(_) => None,
    };
    grouped.map_or(Cow::Borrowed(sql), Cow::Owned)
}

/// One expression (a CHECK, an expectation, a SQL function's body) grouped as `text` groups a statement's: read as the
/// one value of a SELECT, in parentheses, so a bare `?` is no select item to be named.
pub fn expression(e: &str) -> Cow<'_, str> {
    let e = e.trim();
    match text(&format!("SELECT ({e})")) {
        Cow::Owned(s) => s.strip_prefix("SELECT (").and_then(|s| s.strip_suffix(')')).map_or(Cow::Borrowed(e), |s| Cow::Owned(s.to_string())),
        Cow::Borrowed(_) => Cow::Borrowed(e),
    }
}

/// Whether `sql` may hold something generic groups otherwise: a JSON operator, or `IS [NOT] DISTINCT FROM`. A look
/// before anything is parsed, here and for the front (`front::expand`, which groups every form's and dialect's tree).
pub(crate) fn wanted(sql: &str) -> bool {
    const OPERATORS: [&str; 6] = ["->", "#>", "@>", "<@", "?", "#-"];
    OPERATORS.iter().any(|op| sql.contains(op)) || distinct_from(sql)
}

/// The statements with each JSON operator and each `IS [NOT] DISTINCT FROM` regrouped where Postgres groups it
/// differently. None when none was.
fn regrouped(mut stmts: Vec<ast::Statement>) -> Option<String> { regroup(&mut stmts).then(|| written(&stmts)) }

/// `regrouped` in place, on a tree whichever dialect read (the front's: generic's, or the fallback's for FROM first), or on a
/// client's frame as it is inlined (`expander.rs`).
/// Whether any was. (The walk visits an expression after its operands, so an inner one is regrouped first.)
pub(crate) fn regroup<V: ast::VisitMut>(stmts: &mut V) -> bool {
    let mut moved = false;
    let _ = crate::walk::visit_expressions_mut(stmts, |e| {
        if is_json_op(e) {
            moved |= regroup_at(e);
        } else {
            moved |= distinct_at(e);
        }
        ControlFlow::<()>::Continue(())
    });
    moved
}

/// The front's look for `regroup` (`passes.rs`): a text that may hold what generic groups otherwise.
pub(crate) fn pass() -> crate::passes::Pass {
    use crate::passes::{Pass, Run};
    let marks: Vec<&'static [&'static str]> = vec![&["->"], &["#>"], &["@>"], &["<@"], &["?"], &["#-"]];
    Pass { name: "grouping.regroups", order: 85, words: vec!["distinct"], marks, run: Run::Wants(wanted) }
}

/// Text with a `?`, `?|` or `?&`, read as Postgres's dialect reads it (so its JSON operators are grouped right).
/// Each `?` becomes the call Postgres names it for, a top-level `?` in a SELECT list or a RETURNING is named `?column?`, and every
/// other JSON operator is nested, so the generic dialect reads the text the same way.
fn questions(sql: &str) -> Option<String> {
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let _ = crate::walk::visit_mut(&mut stmts, &mut Questions);
    Some(written(&stmts))
}

/// The rewrite `questions` makes, in one walk over the statements.
struct Questions;

impl VisitorMut for Questions {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut ast::Query) -> ControlFlow<()> {
        if let ast::SetExpr::Select(select) = q.body.as_mut() {
            name_questions(&mut select.projection);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, s: &mut ast::Statement) -> ControlFlow<()> {
        let returning = match s {
            ast::Statement::Insert(i) => i.returning.as_mut(),
            ast::Statement::Update(u) => u.returning.as_mut(),
            ast::Statement::Delete(d) => d.returning.as_mut(),
            _ => None,
        };
        if let Some(items) = returning {
            name_questions(items);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &mut ast::Expr) -> ControlFlow<()> {
        if is_question(e) {
            *e = unquestion(take(e));
        } else if is_json_op(e) {
            *e = ast::Expr::Nested(Box::new(take(e)));
        } else {
            distinct_at(e);
        }
        ControlFlow::Continue(())
    }
}

/// A select list's or a RETURNING's items with each bare `?`, `?|` or `?&` named as Postgres names it, `?column?` (the call it
/// becomes would otherwise name the column after itself).
fn name_questions(items: &mut Vec<ast::SelectItem>) {
    *items = std::mem::take(items)
        .into_iter()
        .map(|item| match item {
            ast::SelectItem::UnnamedExpr(e) if is_question(&e) => ast::SelectItem::ExprWithAlias { expr: unquestion(e), alias: ast::Ident::with_quote('"', "?column?") },
            other => other,
        })
        .collect();
}

fn written(stmts: &[ast::Statement]) -> String { stmts.iter().map(crate::text::sql).collect::<Vec<_>>().join(";\n") }

/// Regroups one JSON operator node in place. True when its grouping changed; the node is then nested where it was moved.
fn regroup_at(e: &mut ast::Expr) -> bool {
    let ast::Expr::BinaryOp { left, op, right } = e else { return false };
    let (l, r) = (take(&mut **left), take(&mut **right));
    let mut moved = false;
    let out = group(op, l, r, &mut moved);
    *e = if moved { out } else { unnest(out) };
    moved
}

/// `left op right` for a JSON operator, grouped as Postgres groups it. `moved` is set when the grouping changed. A JSON
/// operator built here is nested (`json`), so the generic dialect reads it as one operand.
fn group(op: &BinaryOperator, left: ast::Expr, right: ast::Expr, moved: &mut bool) -> ast::Expr {
    // Only `->` has a lambda form (`x -> …`), so only it is checked for one; the other JSON operators take any key.
    if matches!(op, BinaryOperator::Arrow) && lambda_like(&left, &right) {
        return json(op, left, right); // (a lambda whose body uses its name: left as it was)
    }
    // A left operand that binds looser than `op` gives it its own right-most operand: `x = doc -> 'a' || 'b'` is
    // `x = ((doc -> 'a') || 'b')`. Checked first, since the operator's key runs on through the right side.
    if rank(&left).is_some_and(|r| r < OP) && last(&left).is_some() {
        *moved = true;
        return map_last(left, |operand| group(op, operand, right, moved));
    }
    // A right operand that binds looser than `op` gives it its own left-most operand: `doc -> 'a' || 'b' = 'x'` is
    // `((doc -> 'a') || 'b') = 'x'`.
    if rank(&right).is_some_and(|r| r <= OP) && first(&right).is_some() {
        *moved = true;
        return map_first(right, |operand| group(op, left, operand, moved));
    }
    json(op, left, right)
}

/// A JSON operator as the text writes it: nested, so the generic dialect reads it as one operand.
fn json(op: &BinaryOperator, left: ast::Expr, right: ast::Expr) -> ast::Expr {
    ast::Expr::Nested(Box::new(ast::Expr::BinaryOp { left: Box::new(left), op: op.clone(), right: Box::new(right) }))
}

fn unnest(e: ast::Expr) -> ast::Expr {
    match e {
        ast::Expr::Nested(inner) => *inner,
        other => other,
    }
}

/// `x -> …` is a lambda when the left side is a bare name the body uses; such an arrow is never JSON's.
fn lambda_like(left: &ast::Expr, right: &ast::Expr) -> bool {
    let ast::Expr::Identifier(name) = left else { return false };
    let mut used = false;
    let _ = crate::walk::visit_expressions(right, |e| {
        used |= matches!(e, ast::Expr::Identifier(n) if n.value == name.value);
        ControlFlow::<()>::Continue(())
    });
    used
}

/// Postgres's rank of an operator or predicate (`gram.y`, as sqlparser's PostgreSQL dialect numbers it); None for an
/// operand.
fn rank(e: &ast::Expr) -> Option<u8> {
    use ast::Expr as X;
    Some(match e {
        X::BinaryOp { op, .. } => pg_rank(op),
        X::IsNull(_) | X::IsNotNull(_) | X::IsTrue(_) | X::IsFalse(_) | X::IsNotTrue(_) | X::IsNotFalse(_) | X::IsUnknown(_) | X::IsNotUnknown(_) | X::IsDistinctFrom(..) | X::IsNotDistinctFrom(..) => IS,
        X::Like { .. } | X::ILike { .. } | X::SimilarTo { .. } | X::InList { .. } | X::InSubquery { .. } | X::Between { .. } => 60,
        _ => return None,
    })
}

fn pg_rank(op: &BinaryOperator) -> u8 {
    match op {
        BinaryOperator::Or => 10,
        BinaryOperator::And => 20,
        BinaryOperator::Eq | BinaryOperator::NotEq | BinaryOperator::Lt | BinaryOperator::Gt | BinaryOperator::LtEq | BinaryOperator::GtEq => 50,
        BinaryOperator::Plus | BinaryOperator::Minus => 80,
        BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Modulo => 90,
        BinaryOperator::PGExp => 100,
        _ => OP, // (`||`, `|`, `&`, `#`, `<<`, `>>` and the JSON operators: Postgres's `Op`)
    }
}

/// The operand at the left end of an operator or predicate (for `IS NULL`, its one operand).
fn first(e: &ast::Expr) -> Option<&ast::Expr> {
    use ast::Expr as X;
    match e {
        X::BinaryOp { left: x, .. } | X::IsDistinctFrom(x, _) | X::IsNotDistinctFrom(x, _) | X::Between { expr: x, .. } | X::Like { expr: x, .. } | X::ILike { expr: x, .. } | X::SimilarTo { expr: x, .. } | X::InList { expr: x, .. } | X::InSubquery { expr: x, .. } => Some(&**x),
        X::IsNull(x) | X::IsNotNull(x) | X::IsTrue(x) | X::IsFalse(x) | X::IsNotTrue(x) | X::IsNotFalse(x) | X::IsUnknown(x) | X::IsNotUnknown(x) => Some(&**x),
        _ => None,
    }
}

/// The operand at the right end of an operator or predicate. A LIKE with an escape character has none (its escape is
/// a value, not an operand), nor does a list or an `IS` test.
fn last(e: &ast::Expr) -> Option<&ast::Expr> {
    use ast::Expr as X;
    match e {
        X::BinaryOp { right: x, .. } | X::IsDistinctFrom(_, x) | X::IsNotDistinctFrom(_, x) | X::Between { high: x, .. } => Some(&**x),
        X::Like { pattern: x, escape_char: None, .. } | X::ILike { pattern: x, escape_char: None, .. } | X::SimilarTo { pattern: x, escape_char: None, .. } => Some(&**x),
        _ => None,
    }
}

/// `e` with its left-most operand replaced by `f` of it. Called only where `first` finds one.
fn map_first(e: ast::Expr, f: impl FnOnce(ast::Expr) -> ast::Expr) -> ast::Expr {
    use ast::Expr as X;
    match e {
        X::BinaryOp { left, op, right } => X::BinaryOp { left: Box::new(f(*left)), op, right },
        X::IsDistinctFrom(l, r) => X::IsDistinctFrom(Box::new(f(*l)), r),
        X::IsNotDistinctFrom(l, r) => X::IsNotDistinctFrom(Box::new(f(*l)), r),
        X::Between { expr, negated, low, high } => X::Between { expr: Box::new(f(*expr)), negated, low, high },
        X::Like { negated, any, expr, pattern, escape_char } => X::Like { negated, any, expr: Box::new(f(*expr)), pattern, escape_char },
        X::ILike { negated, any, expr, pattern, escape_char } => X::ILike { negated, any, expr: Box::new(f(*expr)), pattern, escape_char },
        X::SimilarTo { negated, expr, pattern, escape_char } => X::SimilarTo { negated, expr: Box::new(f(*expr)), pattern, escape_char },
        X::InList { expr, list, negated } => X::InList { expr: Box::new(f(*expr)), list, negated },
        X::InSubquery { expr, subquery, negated } => X::InSubquery { expr: Box::new(f(*expr)), subquery, negated },
        X::IsNull(x) => X::IsNull(Box::new(f(*x))),
        X::IsNotNull(x) => X::IsNotNull(Box::new(f(*x))),
        X::IsTrue(x) => X::IsTrue(Box::new(f(*x))),
        X::IsFalse(x) => X::IsFalse(Box::new(f(*x))),
        X::IsNotTrue(x) => X::IsNotTrue(Box::new(f(*x))),
        X::IsNotFalse(x) => X::IsNotFalse(Box::new(f(*x))),
        X::IsUnknown(x) => X::IsUnknown(Box::new(f(*x))),
        X::IsNotUnknown(x) => X::IsNotUnknown(Box::new(f(*x))),
        other => other,
    }
}

/// `e` with its right-most operand replaced by `f` of it. Called only where `last` finds one.
fn map_last(e: ast::Expr, f: impl FnOnce(ast::Expr) -> ast::Expr) -> ast::Expr {
    use ast::Expr as X;
    match e {
        X::BinaryOp { left, op, right } => X::BinaryOp { left, op, right: Box::new(f(*right)) },
        X::IsDistinctFrom(l, r) => X::IsDistinctFrom(l, Box::new(f(*r))),
        X::IsNotDistinctFrom(l, r) => X::IsNotDistinctFrom(l, Box::new(f(*r))),
        X::Between { expr, negated, low, high } => X::Between { expr, negated, low, high: Box::new(f(*high)) },
        X::Like { negated, any, expr, pattern, escape_char } => X::Like { negated, any, expr, pattern: Box::new(f(*pattern)), escape_char },
        X::ILike { negated, any, expr, pattern, escape_char } => X::ILike { negated, any, expr, pattern: Box::new(f(*pattern)), escape_char },
        X::SimilarTo { negated, expr, pattern, escape_char } => X::SimilarTo { negated, expr, pattern: Box::new(f(*pattern)), escape_char },
        other => other,
    }
}

/// Postgres's rank of `IS` (with `ISNULL` and `NOTNULL`), which `IS [NOT] DISTINCT FROM` takes.
const IS: u8 = 40;

/// Whether the text has `IS [NOT] DISTINCT FROM`, in any case and with any space between: only such text is parsed for
/// it. (`SELECT DISTINCT * FROM t` has a DISTINCT before a FROM too, and isn't: 11-10 audit L8.)
fn distinct_from(sql: &str) -> bool {
    // (how far into `IS [NOT] DISTINCT FROM` the words so far reach: 1 after IS, 2 after NOT, 3 after DISTINCT)
    let mut reached = 0;
    sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).filter(|w| !w.is_empty()).any(|w| {
        let is = |word: &str| w.eq_ignore_ascii_case(word);
        reached = match reached {
            1 if is("not") => 2,
            1 | 2 if is("distinct") => 3,
            3 if is("from") => return true,
            _ if is("is") => 1,
            _ => 0,
        };
        false
    })
}

/// Regroups one `IS [NOT] DISTINCT FROM` in place where its right side runs on past where Postgres ends it. True when
/// it did: its right side's loose operator is then on top, with the comparison, nested, as its left-most operand.
fn distinct_at(e: &mut ast::Expr) -> bool {
    let ends_early = match e {
        ast::Expr::IsDistinctFrom(_, right) | ast::Expr::IsNotDistinctFrom(_, right) => loose(right),
        _ => false,
    };
    if !ends_early {
        return false;
    }
    *e = match take(e) {
        ast::Expr::IsDistinctFrom(l, r) => map_first(*r, |operand| distinct(false, *l, operand)),
        ast::Expr::IsNotDistinctFrom(l, r) => map_first(*r, |operand| distinct(true, *l, operand)),
        other => other,
    };
    true
}

/// `left IS [NOT] DISTINCT FROM right`, nested, with `right` ended where Postgres ends it.
fn distinct(not: bool, left: ast::Expr, right: ast::Expr) -> ast::Expr {
    if loose(&right) {
        return map_first(right, |operand| distinct(not, left, operand));
    }
    let (l, r) = (Box::new(left), Box::new(right));
    ast::Expr::Nested(Box::new(if not { ast::Expr::IsNotDistinctFrom(l, r) } else { ast::Expr::IsDistinctFrom(l, r) }))
}

/// A right side that Postgres ends before: as loose as `IS` or looser, with an operand on its left. Parentheses the
/// text has are kept, since a nested expression has no rank.
fn loose(e: &ast::Expr) -> bool { rank(e).is_some_and(|r| r <= IS) && first(e).is_some() }

/// The JSON operators Postgres groups as its `Op` (`->` and `->>`, `#>` and `#>>`, `@>`, `<@`, `?`, `?|` and `?&`).
fn is_json_op(e: &ast::Expr) -> bool {
    matches!(
        e,
        ast::Expr::BinaryOp {
            op: BinaryOperator::Arrow
                | BinaryOperator::LongArrow
                | BinaryOperator::HashArrow
                | BinaryOperator::HashLongArrow
                | BinaryOperator::AtArrow
                | BinaryOperator::ArrowAt
                | BinaryOperator::Question
                | BinaryOperator::QuestionPipe
                | BinaryOperator::QuestionAnd
                | BinaryOperator::HashMinus,
            ..
        }
    )
}

fn is_question(e: &ast::Expr) -> bool { matches!(e, ast::Expr::BinaryOp { op: BinaryOperator::Question | BinaryOperator::QuestionPipe | BinaryOperator::QuestionAnd, .. }) }

/// `?`, `?|` or `?&` as the call Postgres names for it; any other expression as it is.
fn unquestion(e: ast::Expr) -> ast::Expr {
    match e {
        ast::Expr::BinaryOp { left, op: BinaryOperator::Question, right } => crate::text::function("jsonb_exists", vec![*left, *right]),
        ast::Expr::BinaryOp { left, op: BinaryOperator::QuestionPipe, right } => crate::text::function("jsonb_exists_any", vec![*left, *right]),
        ast::Expr::BinaryOp { left, op: BinaryOperator::QuestionAnd, right } => crate::text::function("jsonb_exists_all", vec![*left, *right]),
        other => other,
    }
}

/// The expression at `e`, leaving a NULL behind (for a rewrite that moves it).
fn take(e: &mut ast::Expr) -> ast::Expr { std::mem::replace(e, ast::Expr::value(ast::Value::Null)) }

#[cfg(test)]
mod tests {
    use super::*;

    fn grouped(sql: &str) -> String { text(sql).into_owned() }

    #[test]
    fn text_groups_right_side_operators_as_postgres_does() {
        assert_eq!(grouped("SELECT doc ->> 'a' = 'x' FROM t"), "SELECT (doc ->> 'a') = 'x' FROM t");
        assert_eq!(grouped("SELECT * FROM t WHERE doc -> 'zz' IS NULL"), "SELECT * FROM t WHERE (doc -> 'zz') IS NULL");
        assert_eq!(grouped("SELECT doc #>> '{b,0}' = '1' FROM t"), "SELECT (doc #>> '{b,0}') = '1' FROM t");
        assert_eq!(grouped("SELECT doc ->> 'a' LIKE 'x%' FROM t"), "SELECT (doc ->> 'a') LIKE 'x%' FROM t");
        assert_eq!(grouped("SELECT doc ->> 'a' IN ('x', 'y') FROM t"), "SELECT (doc ->> 'a') IN ('x', 'y') FROM t");
        assert_eq!(grouped("SELECT doc ->> 'a' || 'y' FROM t"), "SELECT (doc ->> 'a') || 'y' FROM t");
        assert_eq!(grouped("SELECT doc -> 'a' || 'b' = 'x' FROM t"), "SELECT (doc -> 'a') || 'b' = 'x' FROM t");
    }

    #[test]
    fn text_groups_left_side_operators_as_postgres_does() {
        assert_eq!(grouped("SELECT * FROM t WHERE 'x' = doc ->> 'a'"), "SELECT * FROM t WHERE 'x' = (doc ->> 'a')");
        assert_eq!(grouped("SELECT x = doc -> 'a' || 'b' FROM t"), "SELECT x = (doc -> 'a') || 'b' FROM t");
        assert_eq!(grouped("SELECT doc -> 'a' -> 'b' = 'x' FROM t"), "SELECT (doc -> 'a' -> 'b') = 'x' FROM t");
        assert_eq!(grouped("SELECT 'z' || doc -> 'a' = 'x' FROM t"), "SELECT ('z' || doc -> 'a') = 'x' FROM t");
    }

    #[test]
    fn text_reads_question_operators_as_postgres_does() {
        // (`?` is a placeholder to the generic dialect, and so is the `?` of `?|` and `?&`: text with any of them is
        // read as Postgres's dialect reads it, each operator becoming the call Postgres names for it)
        assert_eq!(grouped("SELECT doc ? 'a' FROM t"), r#"SELECT jsonb_exists(doc, 'a') AS "?column?" FROM t"#);
        assert_eq!(grouped("SELECT * FROM t WHERE doc ? 'a' AND doc ->> 'a' = 'x'"), "SELECT * FROM t WHERE jsonb_exists(doc, 'a') AND (doc ->> 'a') = 'x'");
        assert_eq!(grouped("SELECT * FROM t WHERE doc ?| ARRAY['a', 'b']"), "SELECT * FROM t WHERE jsonb_exists_any(doc, ARRAY['a', 'b'])");
        assert_eq!(grouped("SELECT * FROM t WHERE doc ?& '{a}'"), "SELECT * FROM t WHERE jsonb_exists_all(doc, '{a}')");
        // (a RETURNING's items are named as a select list's: Postgres 16 names `RETURNING id, doc ? 'a'` id and ?column?)
        assert_eq!(grouped("DELETE FROM t WHERE doc ? 'a' RETURNING id, doc ? 'a'"), r#"DELETE FROM t WHERE jsonb_exists(doc, 'a') RETURNING id, jsonb_exists(doc, 'a') AS "?column?""#);
        assert_eq!(grouped("UPDATE t SET n = 1 RETURNING doc ?| ARRAY['a']"), r#"UPDATE t SET n = 1 RETURNING jsonb_exists_any(doc, ARRAY['a']) AS "?column?""#);
    }

    #[test]
    fn an_expression_is_grouped_as_postgres_groups_it() {
        // (a CHECK's, an expectation's or a SQL function's: Postgres 16 groups the same text this way, and names nothing)
        assert_eq!(expression("doc ? 'a'"), "jsonb_exists(doc, 'a')");
        assert_eq!(expression(" doc ->> 'a' = 'x' "), "(doc ->> 'a') = 'x'");
        assert_eq!(expression("doc ?| ARRAY['a'] AND n > 1"), "jsonb_exists_any(doc, ARRAY['a']) AND n > 1");
        assert!(matches!(expression("amount > 0"), Cow::Borrowed("amount > 0")), "nothing of JSON's: as written");
    }

    #[test]
    fn text_groups_column_keys_as_postgres_does() {
        // (no lambda form: `->>` and `#>>` take any key, a column too)
        assert_eq!(grouped("SELECT * FROM t WHERE doc ->> col = 'x'"), "SELECT * FROM t WHERE (doc ->> col) = 'x'");
        assert_eq!(grouped("SELECT * FROM t WHERE doc -> col = 'x'"), "SELECT * FROM t WHERE (doc -> col) = 'x'");
        assert_eq!(grouped("SELECT * FROM t WHERE doc #>> keys = 'x'"), "SELECT * FROM t WHERE (doc #>> keys) = 'x'");
    }

    #[test]
    fn lambdas_are_left_alone() {
        let lambda = "SELECT list_filter(l, y -> y > 0 AND y < 4) FROM t";
        assert!(matches!(text(lambda), Cow::Borrowed(_)), "a lambda's body is a column, never a key");
        assert!(matches!(text("SELECT list_transform(l, x -> 'p' || x) FROM t"), Cow::Borrowed(_)), "a lambda whose body uses its name");
        assert!(matches!(text("SELECT list_transform(l, x -> x * 2) FROM t"), Cow::Borrowed(_)));
        assert_eq!(grouped("SELECT list_transform(l, x -> col || x) FROM t"), "SELECT list_transform(l, x -> col || x) FROM t", "a lambda whose body uses its name, with a column in it: as it was");
    }

    #[test]
    fn text_without_a_rewrite_is_borrowed() {
        assert!(matches!(text("SELECT ARRAY[1,2] @> ARRAY[1]"), Cow::Borrowed(_)), "an array's containment is DataFusion's");
        assert!(matches!(text("SELECT doc -> 'a' FROM t"), Cow::Borrowed(_)), "a JSON operator already grouped the way Postgres groups it");
        assert!(matches!(text("SELECT 1 FROM t"), Cow::Borrowed(_)), "no JSON operator at all");
        assert!(matches!(text("SELECT doc ->> 'a' = 'x' FROM t WHERE doc"), Cow::Owned(_)));
    }

    #[test]
    fn text_groups_path_deletion_as_postgres_does() {
        assert_eq!(grouped("SELECT doc #- '{a}' = '{}' FROM t"), "SELECT (doc #- '{a}') = '{}' FROM t");
        assert_eq!(grouped("SELECT doc #- '{a}' FROM t"), "SELECT doc #- '{a}' FROM t", "alone, it reads as it is");
    }

    #[test]
    fn distinct_from_ends_where_postgres_ends_it() {
        // (DuckDB 2.0 answers each the way Postgres groups it: `true IS DISTINCT FROM 1 = 1 AND false` is false)
        assert_eq!(grouped("SELECT a IS DISTINCT FROM b OR c FROM t"), "SELECT (a IS DISTINCT FROM b) OR c FROM t");
        assert_eq!(grouped("SELECT * FROM t WHERE a IS NOT DISTINCT FROM b AND c = d"), "SELECT * FROM t WHERE (a IS NOT DISTINCT FROM b) AND c = d");
        assert_eq!(grouped("SELECT a IS DISTINCT FROM b AND c OR d FROM t"), "SELECT (a IS DISTINCT FROM b) AND c OR d FROM t");
        assert_eq!(grouped("SELECT a IS DISTINCT FROM b IS NULL FROM t"), "SELECT (a IS DISTINCT FROM b) IS NULL FROM t");
        assert_eq!(grouped("SELECT * FROM t JOIN u ON t.a IS NOT DISTINCT FROM u.a AND t.b IS NOT DISTINCT FROM u.b"), "SELECT * FROM t JOIN u ON (t.a IS NOT DISTINCT FROM u.a) AND t.b IS NOT DISTINCT FROM u.b");
        assert_eq!(grouped("SELECT a is distinct\n from b or c FROM t"), "SELECT (a IS DISTINCT FROM b) OR c FROM t", "any case, any space");
        // (what binds tighter than IS stays on the right side: a comparison, IN, BETWEEN and arithmetic)
        assert_eq!(grouped("SELECT a IS DISTINCT FROM b IN (1, 2) OR c FROM t"), "SELECT (a IS DISTINCT FROM b IN (1, 2)) OR c FROM t");
        assert_eq!(grouped("SELECT a IS DISTINCT FROM b BETWEEN 1 AND 2 AND c FROM t"), "SELECT (a IS DISTINCT FROM b BETWEEN 1 AND 2) AND c FROM t");
        assert_eq!(grouped("SELECT a IS DISTINCT FROM b OR c IS DISTINCT FROM d AND e FROM t"), "SELECT (a IS DISTINCT FROM b) OR (c IS DISTINCT FROM d) AND e FROM t");
        assert_eq!(expression("x IS DISTINCT FROM y OR z"), "(x IS DISTINCT FROM y) OR z");
    }

    #[test]
    fn distinct_from_already_ended_is_as_written() {
        assert!(matches!(text("SELECT a IS DISTINCT FROM b FROM t"), Cow::Borrowed(_)), "nothing after its right side");
        assert!(matches!(text("SELECT a IS DISTINCT FROM (b OR c) FROM t"), Cow::Borrowed(_)), "parentheses the text has");
        assert!(matches!(text("SELECT a IS DISTINCT FROM b + 1 = c FROM t"), Cow::Borrowed(_)), "tighter than IS: Postgres reads it so too");
        assert!(matches!(text("SELECT (a IS DISTINCT FROM b) OR c FROM t"), Cow::Borrowed(_)));
        assert!(matches!(text("SELECT DISTINCT a FROM t WHERE a OR b"), Cow::Borrowed(_)), "SELECT DISTINCT is no DISTINCT FROM");
        assert!(!distinct_from("SELECT DISTINCT from_date FROM t") && distinct_from("a IS NOT DISTINCT/**/FROM b"));
        assert!(!distinct_from("SELECT DISTINCT * FROM t") && !distinct_from("SELECT DISTINCT ON (a) a FROM t"), "no IS before it (11-10 audit L8)");
        assert!(!distinct_from("SELECT a IS NOT NULL, DISTINCT FROM t") && distinct_from("x is\n distinct from y"));
    }

    /// A tree only the fallback dialect reads (FROM first) is regrouped as generic's is: `text` hands such text back as it
    /// was, so the front groups the tree it read (`front::expand`), in every form (11-10 audit H3).
    #[test]
    fn a_tree_the_fallback_read_is_regrouped() {
        let sql = "SELECT [x FOR x IN l] AS d, a IS DISTINCT FROM b OR c FROM t";
        assert!(crate::parsed::generic(sql).is_err() && matches!(text(sql), Cow::Borrowed(_)) && wanted(sql), "generic can't read it; the front's look can");
        let mut stmts = crate::parsed::read(sql).unwrap();
        assert!(regroup(&mut stmts));
        assert!(written(&stmts).contains("(a IS DISTINCT FROM b) OR c"), "{}", written(&stmts));
        let mut again = crate::parsed::read(&written(&stmts)).unwrap();
        assert!(!regroup(&mut again), "its print reads grouped");
        assert!(!wanted("SELECT DISTINCT * FROM t"));
    }

    /// Each grouping answers as DuckDB 2.0 does, run: with booleans the old reading answered otherwise, with no error.
    #[tokio::test]
    async fn distinct_from_answers_as_postgres_does() {
        let ctx = datafusion::prelude::SessionContext::new();
        for (sql, want) in [
            ("SELECT true IS DISTINCT FROM false OR true", "true"),
            ("SELECT true IS DISTINCT FROM 1 = 1 AND false", "false"),
            ("SELECT true IS NOT DISTINCT FROM false AND false OR true", "true"),
            ("SELECT 1 IS DISTINCT FROM 2 IS NULL", "false"),
            ("SELECT NOT 1 IS DISTINCT FROM 1", "true"),
        ] {
            let batches = ctx.sql(&text(sql)).await.unwrap().collect().await.unwrap();
            let got = datafusion::arrow::util::display::array_value_to_string(batches[0].column(0), 0).unwrap();
            assert_eq!(got, want, "{sql}");
        }
    }
}
