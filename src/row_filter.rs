//! A grant's filter on its rows (step 6 piece 5, `users::Grant::rows`): checked when it is set (`check`), and filled in
//! for whoever reads (`filled`), as `rights::Scope::rows` gives it to planning. It may use the table's own columns,
//! literals, `current_user` (and `current_role` and `session_user`, the same name here, as in Postgres with no SET ROLE),
//! `current_claim('x')` (a claim of the token signed in with: NULL until sign-in carries claims, step 7, so it shows no
//! row) and immutable functions. So one reader's filter answers alike for every reading of a row, and planning may push
//! it into a scan.

use crate::codes::coded;
use anyhow::{ensure, Result};
use core::ops::ControlFlow;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{ExprSchemable, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;

/// The filter `text` on `table`'s rows (`schema`: its columns), if a grant may hold it: as it is kept (written again from
/// what was parsed), or why not. `ctx` names the functions it may call: the lake's own session's.
#[cfg_attr(not(test), allow(dead_code))] // (its caller is the grant's syntax, when it comes: L6 names it)
pub fn check(ctx: &SessionContext, table: &str, schema: &Schema, text: &str) -> Result<String> {
    let e = parsed(text)?;
    let state = ctx.state();
    let mut refused = None;
    let _ = crate::walk::visit_expressions(&e, |x| match why_not(x, table, schema, &state) {
        Some(why) => {
            refused = Some(why);
            ControlFlow::Break(())
        }
        None => ControlFlow::Continue(()),
    });
    if let Some(why) = refused {
        return Err(why);
    }
    // (its type, planned over the table's columns as no one's reading: a condition, or NULL)
    let df = DFSchema::try_from(schema.clone())?;
    let planned = ctx.parse_sql_expr(&filled_expr(e.clone(), None).to_string(), &df)?;
    let t = planned.get_type(&df)?;
    ensure!(matches!(t, DataType::Boolean | DataType::Null), coded("42804", format!("a filter on rows is a condition: {text} is a {t}")));
    Ok(e.to_string())
}

/// The filter `text` (as `check` keeps it) for `reader` (None: a scope made without a principal, which names no one): its
/// reader's words as literals. One that no longer parses shows no row (`FALSE`).
pub fn filled(text: &str, reader: Option<&str>) -> String {
    match parsed(text) {
        Ok(e) => filled_expr(e, reader).to_string(),
        Err(_) => "FALSE".into(),
    }
}

/// One condition, and nothing after it.
fn parsed(text: &str) -> Result<Expr> {
    let dialect = PostgreSqlDialect {};
    let said = |e: &dyn std::fmt::Display| coded("42601", format!("a filter on rows is one SQL condition: {e}"));
    let mut p = Parser::new(&dialect).try_with_sql(text).map_err(|e| said(&e))?;
    let e = p.parse_expr().map_err(|e| said(&e))?;
    ensure!(matches!(p.peek_token().token, Token::EOF), said(&format!("{text} goes on past {e}")));
    Ok(e)
}

/// Why `x`, a part of a filter on `table`'s rows, may not be in one (None: it may).
#[cfg_attr(not(test), allow(dead_code))] // (`check`'s)
fn why_not(x: &Expr, table: &str, schema: &Schema, state: &datafusion::execution::SessionState) -> Option<anyhow::Error> {
    match x {
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => Some(coded("0A000", "a filter on rows reads its own row only: no subquery")),
        Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) => Some(coded("0A000", format!("a filter on rows takes no parameter ({p})"))),
        _ if names_reader(x) => None,
        Expr::Identifier(i) => {
            let name = if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() };
            schema.field_with_name(&name).is_err().then(|| coded("42703", format!("{table} has no column {name}")))
        }
        Expr::CompoundIdentifier(_) => Some(coded("42601", format!("a filter on rows names {table}'s columns by themselves: {x}"))),
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            if f.over.is_some() || state.window_functions().contains_key(&name) {
                return Some(coded("42P20", format!("a filter on rows may not use a window function: {name}")));
            }
            if name == "current_claim" {
                return claim(x).is_none().then(|| coded("42601", "current_claim takes the claim's name, as current_claim('dept')"));
            }
            if state.aggregate_functions().contains_key(&name) {
                return Some(coded("42803", format!("a filter on rows may not use an aggregate: {name}")));
            }
            match state.scalar_functions().get(&name) {
                None => Some(coded("42883", format!("no function {name}"))),
                Some(u) if u.signature().volatility != Volatility::Immutable => {
                    Some(coded("42P17", format!("{name} may answer otherwise each time it is called, so a filter on rows may not use it: only immutable functions")))
                }
                Some(_) => None,
            }
        }
        _ => None,
    }
}

/// `current_user`, `current_role` or `session_user`: whoever reads.
fn names_reader(x: &Expr) -> bool {
    let reader = |w: &str| ["current_user", "current_role", "session_user"].iter().any(|r| w.eq_ignore_ascii_case(r));
    match x {
        Expr::Identifier(i) => i.quote_style.is_none() && reader(&i.value),
        Expr::Function(f) => matches!(f.args, FunctionArguments::None) && reader(&f.name.to_string()),
        _ => false,
    }
}

/// `current_claim('x')`'s claim's name.
#[cfg_attr(not(test), allow(dead_code))] // (`check`'s)
fn claim(x: &Expr) -> Option<&str> {
    let Expr::Function(f) = x else { return None };
    let FunctionArguments::List(l) = &f.args else { return None };
    match l.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(c), .. })))] if l.clauses.is_empty() => Some(c),
        _ => None,
    }
}

/// `e` with its reader's words as literals: the reader's name (NULL for no one's), and NULL for every claim.
fn filled_expr(mut e: Expr, reader: Option<&str>) -> Expr {
    let _ = crate::walk::visit_expressions_mut(&mut e, |x: &mut Expr| {
        if names_reader(x) {
            *x = Expr::Value(reader.map_or(Value::Null, |r| Value::SingleQuotedString(r.to_string())).into());
        } else if matches!(x, Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("current_claim")) {
            *x = Expr::Value(Value::Null.into());
        }
        ControlFlow::<()>::Continue(())
    });
    e
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;

    fn t() -> Schema { Schema::new(vec![Field::new("dept", DataType::Utf8, true), Field::new("owner", DataType::Utf8, true), Field::new("n", DataType::Int64, true)]) }

    fn said(text: &str) -> String {
        match check(&SessionContext::new(), "t", &t(), text) {
            Ok(kept) => format!("ok: {kept}"),
            Err(e) => format!("{}: {}", crate::codes::of(&e), crate::ext::said(&e)),
        }
    }

    #[test]
    fn a_filter_on_rows_reads_its_own_row_and_who_reads() {
        assert_eq!(said("dept = 'x'"), "ok: dept = 'x'");
        assert_eq!(said("owner = current_user OR n > 3"), "ok: owner = current_user OR n > 3");
        assert_eq!(said("owner = current_role"), "ok: owner = current_role");
        assert_eq!(said("dept = current_claim('dept')"), "ok: dept = current_claim('dept')");
        assert_eq!(said("lower(dept) IN ('a', 'b') AND n BETWEEN 1 AND 9"), "ok: lower(dept) IN ('a', 'b') AND n BETWEEN 1 AND 9");
        assert_eq!(said("\"dept\" IS NOT NULL"), "ok: \"dept\" IS NOT NULL");
    }

    #[test]
    fn a_filter_on_rows_refuses_what_could_answer_otherwise() {
        assert_eq!(said("n IN (SELECT n FROM u)"), "0A000: a filter on rows reads its own row only: no subquery");
        assert_eq!(said("EXISTS (SELECT 1)"), "0A000: a filter on rows reads its own row only: no subquery");
        assert_eq!(said("n > (SELECT 1)"), "0A000: a filter on rows reads its own row only: no subquery");
        assert_eq!(said("n > $1"), "0A000: a filter on rows takes no parameter ($1)");
        assert_eq!(said("sum(n) > 3"), "42803: a filter on rows may not use an aggregate: sum");
        assert_eq!(said("row_number() OVER () > 3"), "42P20: a filter on rows may not use a window function: row_number");
        assert!(said("random() > 0.5").starts_with("42P17: random may answer otherwise"), "{}", said("random() > 0.5"));
        assert!(said("now() > '2026-01-01'").starts_with("42P17: now may answer otherwise"));
        assert_eq!(said("salary > 3"), "42703: t has no column salary");
        assert_eq!(said("t.dept = 'x'"), "42601: a filter on rows names t's columns by themselves: t.dept");
        assert_eq!(said("no_such(n)"), "42883: no function no_such");
        assert_eq!(said("current_claim(dept)"), "42601: current_claim takes the claim's name, as current_claim('dept')");
        assert_eq!(said("n + 1"), "42804: a filter on rows is a condition: n + 1 is a Int64");
        assert!(said("dept = 'x'; DROP TABLE t").starts_with("42601: a filter on rows is one SQL condition"));
        assert!(said("").starts_with("42601: a filter on rows is one SQL condition"));
    }

    #[test]
    fn a_filter_is_filled_in_for_whoever_reads() {
        assert_eq!(filled("owner = CURRENT_USER OR owner = current_role", Some("ann")), "owner = 'ann' OR owner = 'ann'");
        assert_eq!(filled("owner = current_user", Some("o'brien")), "owner = 'o''brien'");
        assert_eq!(filled("owner = current_user", None), "owner = NULL", "no one's reading names no one");
        assert_eq!(filled("dept = current_claim('dept')", Some("ann")), "dept = NULL", "no claim is carried yet: no row");
        assert_eq!(filled("not sql (", Some("ann")), "FALSE", "a filter that no longer parses shows no row");
    }
}
