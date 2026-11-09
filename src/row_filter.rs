//! A grant's filter on its rows (step 6 piece 5, `users::Grant::rows`): checked when it is set (`check`), and filled in
//! for whoever reads (`filled`), as `rights::Scope::rows` gives it to planning. It may use the table's own columns,
//! literals, `current_user` (and `current_role` and `session_user`, the same name here, as in Postgres with no SET ROLE),
//! `current_claim('x')` (a claim of the token signed in with: NULL until sign-in carries claims, step 7, so it shows no
//! row) and immutable functions. So one reader's filter answers alike for every reading of a row, and planning may push
//! it into a scan.

use crate::codes::coded;
use anyhow::{ensure, Result};
use core::ops::ControlFlow;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{ExprSchemable, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectNamePart, Value, ValueWithSpan};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;

/// The filter `text` on `table`'s rows (`schema`: its columns), if a grant may hold it: as it is kept (written again from
/// what was parsed), or why not. `ctx` names the functions it may call: the lake's own session's.
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

/// Do a write's new versions of `table`'s rows (`new`, under SQL's names: a row with no `_row_id` is a new row, no version)
/// all meet `filter`, its writer's filter on them for `privilege` (as `Scope::rows` fills it in)? Else the write is refused,
/// 42501, naming no value: Postgres's WITH CHECK, which is a policy's USING when it gives none. NULL fails it, as in a WHERE.
/// `ctx` names the functions it may call: the lake's own session's, as `check`'s.
pub async fn holds(ctx: &SessionContext, table: &str, new: &[RecordBatch], filter: &str, privilege: &str) -> Result<()> {
    let versions: Vec<&RecordBatch> = new.iter().filter(|b| b.num_rows() > 0 && b.column_by_name(crate::text::ROW_ID).is_some()).collect();
    let Some(first) = versions.first() else { return Ok(()) };
    let schema = first.schema();
    let all = concat_batches(&schema, &versions.iter().map(|b| crate::query::conform(b, &schema)).collect::<Result<Vec<_>>>()?)?;
    ctx.register_batch("__pondra_versions", all)?;
    let sql = format!("SELECT 1 FROM __pondra_versions WHERE \"{}\" IS NOT NULL AND NOT coalesce(({filter}), false) LIMIT 1", crate::text::ROW_ID);
    let failed = async { anyhow::Ok(crate::engine::sql(ctx, &sql).await?.collect().await?.iter().any(|b| b.num_rows() > 0)) }.await;
    ctx.deregister_table("__pondra_versions")?; // (the lake's session is a copy, its tables its own: this one goes with the check)
    ensure!(!failed?, coded("42501", format!("new row violates the filter on {table}'s rows of your {privilege} grant")));
    Ok(())
}

/// The filter `text` as a grant keeps it (`check` gives the same): its parse printed, so spacing, keyword case and the
/// case of names unquoted never make two filters differ (`REVOKE … WHERE c` takes the grant whose filter is `c`'s).
pub fn kept(text: &str) -> Result<String> { Ok(parsed(text)?.to_string()) }

/// One condition, and nothing after it: names unquoted in lower case (as SQL reads them), and a minus before a minus
/// printed as one (`parsed::signed`: `- -1`'s print, `--1`, reads back as a comment).
fn parsed(text: &str) -> Result<Expr> {
    let dialect = PostgreSqlDialect {};
    let said = |e: &dyn std::fmt::Display| coded("42601", format!("a filter on rows is one SQL condition: {e}"));
    let mut p = Parser::new(&dialect).try_with_sql(text).map_err(|e| said(&e))?;
    let mut e = p.parse_expr().map_err(|e| said(&e))?;
    ensure!(matches!(p.peek_token().token, Token::EOF), said(&format!("{text} goes on past {e}")));
    let lower = |i: &mut Ident| {
        if i.quote_style.is_none() {
            i.value = i.value.to_lowercase();
        }
    };
    let _ = crate::walk::visit_expressions_mut(&mut e, |x: &mut Expr| {
        match x {
            Expr::Identifier(i) => lower(i),
            Expr::Function(f) => f.name.0.iter_mut().filter_map(|p| match p {
                ObjectNamePart::Identifier(i) => Some(i),
                _ => None,
            }).for_each(lower),
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    crate::parsed::signed_in(&mut e);
    Ok(e)
}

/// Why `x`, a part of a filter on `table`'s rows, may not be in one (None: it may).
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

    /// A write's new versions meet its filter or the write is refused, naming no value (step 6 piece 5c): NULL fails it, and
    /// a new row (no `_row_id`, or a NULL one where a MERGE's inserts are conformed to its updates) isn't a version.
    #[tokio::test]
    async fn a_new_version_meets_its_writers_filter_or_the_write_is_refused() {
        use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
        let batch = |depts: &[Option<&str>], ids: Option<&[Option<i64>]>| {
            let mut columns: Vec<(&str, ArrayRef)> = vec![("dept", std::sync::Arc::new(StringArray::from(depts.to_vec())))];
            if let Some(ids) = ids {
                columns.push((crate::text::ROW_ID, std::sync::Arc::new(Int64Array::from(ids.to_vec()))));
            }
            RecordBatch::try_from_iter(columns).unwrap()
        };
        let said = |r: Result<()>| r.err().map_or("ok".into(), |e| format!("{}: {}", crate::codes::of(&e), crate::ext::said(&e)));
        let ctx = SessionContext::new();
        let held = |b: Vec<RecordBatch>| { let ctx = ctx.clone(); async move { said(holds(&ctx, "t", &b, "dept = 'x'", "UPDATE").await) } };
        assert_eq!(held(vec![batch(&[Some("x"), Some("x")], Some(&[Some(1), Some(2)]))]).await, "ok");
        let refused = "42501: new row violates the filter on t's rows of your UPDATE grant";
        assert_eq!(held(vec![batch(&[Some("x")], Some(&[Some(1)])), batch(&[Some("y")], Some(&[Some(2)]))]).await, refused);
        assert_eq!(held(vec![batch(&[None], Some(&[Some(1)]))]).await, refused, "NULL fails it");
        assert_eq!(held(vec![batch(&[Some("y")], None), batch(&[Some("y")], Some(&[None]))]).await, "ok", "new rows are INSERT's to check");
        assert_eq!(held(vec![]).await, "ok");
        assert!(ctx.table_exist("__pondra_versions").is_ok_and(|e| !e), "the check's table goes with it");
    }

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
