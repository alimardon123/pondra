//! Postgres's JSON operators on JSON text (round 35): `#>` and `#>>` (a path), `@>` and `<@` (containment),
//! `?`, `?|` and `?&` (existence). datafusion-functions-json reads a path as one key and plans none of the
//! others, so they are planned here, ahead of its planner (`Lake::made`), when the left side is text. `->`,
//! `->>` and every operator on another type keep the planning they have. The answers follow Postgres 16:
//! a document's text is kept as written (`types.rs`), so `#>` answers the value's text as the document has it.
use crate::pg_catalog::{function, text_arg, texts_out};
use crate::types::{is_json, json_of};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::{Expr, ExprSchemable, ScalarUDF, Signature, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{BinaryOperator, Statement};
use datafusion::sql::sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;
use serde_json::value::RawValue;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

pub fn planner() -> Arc<dyn ExprPlanner> { Arc::new(Operators) }

#[derive(Debug)]
struct Operators;

impl ExprPlanner for Operators {
    fn plan_binary_op(&self, e: RawBinaryExpr, schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>> {
        let Some((name, both_text, swap)) = operator(&e.op) else { return Ok(PlannerResult::Original(e)) };
        let text = |x: &Expr| x.get_type(schema).is_ok_and(|t| matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8));
        if !text(&e.left) || (both_text && !text(&e.right)) {
            return Ok(PlannerResult::Original(e)); // (an array's @> stays DataFusion's)
        }
        // The column's title is what functions-json gives it (`doc #>> '{b,0}'`), so a default name doesn't change.
        let alias = format!("{} {} {}", as_title(&e.left), e.op, as_title(&e.right));
        let args = if swap { vec![e.right, e.left] } else { vec![e.left, e.right] };
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(named(name), args));
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(call, None::<&str>, alias))))
    }
}

/// The function an operator calls, whether both sides must be text, and whether its sides swap (`<@`).
fn operator(op: &BinaryOperator) -> Option<(&'static str, bool, bool)> {
    Some(match op {
        BinaryOperator::HashArrow => ("pg_json_path", false, false),
        BinaryOperator::HashLongArrow => ("pg_json_path_text", false, false),
        BinaryOperator::AtArrow => ("pg_jsonb_contains", true, false),
        BinaryOperator::ArrowAt => ("pg_jsonb_contains", true, true),
        BinaryOperator::Question => ("pg_jsonb_exists", false, false),
        BinaryOperator::QuestionPipe => ("pg_jsonb_exists_any", false, false),
        BinaryOperator::QuestionAnd => ("pg_jsonb_exists_all", false, false),
        _ => return None,
    })
}

/// An operand as functions-json writes it in a column's title: a column's name, a string in quotes.
fn as_title(e: &Expr) -> String {
    match e {
        Expr::Column(c) => c.name.clone(),
        Expr::Alias(a) => a.name.clone(),
        Expr::Cast(c) => as_title(&c.expr),
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => format!("'{s}'"),
        Expr::Literal(v, _) => v.to_string(),
        _ => e.to_string(),
    }
}

/// The six functions the operators call. `register` adds them to every session; the planner calls them by name.
static FUNCTIONS: LazyLock<Vec<Arc<ScalarUDF>>> = LazyLock::new(|| {
    let two = || Signature::any(2, Volatility::Immutable);
    vec![
        Arc::new(function("pg_json_path", two(), |_| DataType::Utf8, |a, n| path(a, n, false).map_err(fail))),
        Arc::new(function("pg_json_path_text", two(), |_| DataType::Utf8, |a, n| path(a, n, true).map_err(fail))),
        Arc::new(function("pg_jsonb_contains", two(), |_| DataType::Boolean, |a, n| contains_all(a, n).map_err(fail))),
        Arc::new(function("pg_jsonb_exists", two(), |_| DataType::Boolean, |a, n| exists(a, n).map_err(fail))),
        Arc::new(function("pg_jsonb_exists_any", two(), |_| DataType::Boolean, |a, n| any_all(a, n, false).map_err(fail))),
        Arc::new(function("pg_jsonb_exists_all", two(), |_| DataType::Boolean, |a, n| any_all(a, n, true).map_err(fail))),
    ]
});

/// The function called `name` (one of the six above).
fn named(name: &str) -> Arc<ScalarUDF> {
    FUNCTIONS.iter().find(|f| f.name() == name).cloned().expect("pg_json.rs calls only the functions it makes")
}

/// Adds the six functions to a session, so `pg_json_path(…)` and the rest can be called by name.
pub fn register(ctx: &SessionContext) {
    for f in FUNCTIONS.iter() {
        ctx.register_udf(f.as_ref().clone());
    }
}

/// A query whose text has a `?` operator (`?`, `?|`, `?&`). SQL is read with the generic dialect, which reads `?` as a
/// placeholder, so such a query is read as Postgres's dialect reads it and planned from that tree, as
/// `friendly::lambdas`' are (`query::sql`). None for any other text, and for text the generic dialect reads.
pub fn statement(sql: &str) -> Option<datafusion::sql::parser::Statement> {
    if !sql.contains('?') || Parser::parse_sql(&GenericDialect {}, sql).is_ok() {
        return None;
    }
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    if !matches!(stmts.as_slice(), [Statement::Query(_)]) {
        return None;
    }
    Some(datafusion::sql::parser::Statement::Statement(Box::new(stmts.remove(0))))
}

/// An error a function raises, carrying its SQLSTATE to the door (`codes::of`).
fn fail(e: anyhow::Error) -> DataFusionError { DataFusionError::External(e.into()) }

/// `s` as a JSON document, or Postgres's refusal (22P02) when it isn't one.
fn valid(s: &str) -> anyhow::Result<()> {
    if is_json(s) { Ok(()) } else { Err(json_of(s).unwrap_err()) }
}

/// `s` parsed, for containment and existence.
fn document(s: &str) -> anyhow::Result<Value> { serde_json::from_str(s).map_err(|_| json_of(s).unwrap_err()) }

/// A path or a list of keys, one per row: a text-array literal (`'{b,0}'`) or a list (`ARRAY['b', '0']`). None where
/// the argument is NULL, and a NULL element is None inside the list.
fn lists(a: &ArrayRef) -> anyhow::Result<Vec<Option<Vec<Option<String>>>>> {
    if !matches!(a.data_type(), DataType::List(_) | DataType::LargeList(_)) {
        return text_arg(a)?.into_iter().map(|t| t.map(|s| array_literal(&s)).transpose()).collect();
    }
    let items = cast(a, &DataType::new_list(DataType::Utf8, true))?;
    let items = items.as_list::<i32>();
    let rows = (0..items.len()).map(|i| {
        (!items.is_null(i)).then(|| items.value(i).as_string::<i32>().iter().map(|s| s.map(str::to_string)).collect::<Vec<_>>())
    });
    Ok(rows.collect())
}

/// Postgres's text array literal (`{a,"b,c",NULL}`): a bare NULL is a NULL element, a quoted one is text, a bare
/// element is trimmed of its spaces, and `{}` has no elements.
fn array_literal(s: &str) -> anyhow::Result<Vec<Option<String>>> {
    let malformed = || crate::codes::coded("22P02", format!("malformed array literal: \"{s}\""));
    let body = s.trim().strip_prefix('{').and_then(|b| b.strip_suffix('}')).ok_or_else(malformed)?;
    let mut rest = body.trim_start();
    let mut items = Vec::new();
    if rest.is_empty() {
        return Ok(items);
    }
    loop {
        let (item, after) = if rest.starts_with('"') { quoted(rest) } else { bare(rest) }.ok_or_else(malformed)?;
        items.push(item);
        let after = after.trim_start();
        if after.is_empty() {
            return Ok(items);
        }
        rest = after.strip_prefix(',').ok_or_else(malformed)?.trim_start();
    }
}

/// A quoted element at the start of `s`, its escapes undone, and the text after its closing quote.
fn quoted(s: &str) -> Option<(Option<String>, &str)> {
    let mut out = String::new();
    let mut chars = s[1..].char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Some((Some(out), &s[2 + i..])),
            '\\' => out.push(chars.next()?.1),
            c => out.push(c),
        }
    }
    None
}

/// A bare element at the start of `s`: up to the next comma, trimmed, with `\x` read as `x`. A bare NULL is NULL,
/// and an empty element (`{a,,b}`) is malformed.
fn bare(s: &str) -> Option<(Option<String>, &str)> {
    let mut end = s.len();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => {
                chars.next();
            }
            ',' => {
                end = i;
                break;
            }
            _ => {}
        }
    }
    let raw = s[..end].trim();
    if raw.is_empty() {
        return None;
    }
    let item = if raw.eq_ignore_ascii_case("NULL") { None } else { Some(unescape(raw)) };
    Some((item, &s[end..]))
}

/// `s` with each `\x` read as `x`.
fn unescape(s: &str) -> String {
    let mut out = String::new();
    let mut escaped = false;
    for c in s.chars() {
        if c == '\\' && !escaped {
            escaped = true;
        } else {
            out.push(c);
            escaped = false;
        }
    }
    out
}

const JSON_SPACE: &[char] = &[' ', '\t', '\n', '\r'];

/// `#>` (`text` false) and `#>>` (true): what the path reaches in each document, NULL where it reaches nothing.
fn path(a: &[ArrayRef], n: usize, text: bool) -> anyhow::Result<ArrayRef> {
    let (docs, paths) = (text_arg(&a[0])?, lists(&a[1])?);
    let out = (0..n).map(|i| -> anyhow::Result<Option<String>> {
        match (&docs[i], &paths[i]) {
            (Some(doc), Some(steps)) => {
                valid(doc)?;
                Ok(walk(doc, steps)?.and_then(|raw| if text { unquoted(raw) } else { Some(raw.to_string()) }))
            }
            _ => Ok(None),
        }
    });
    Ok(texts_out(out.collect::<anyhow::Result<Vec<_>>>()?))
}

/// The text of the value a path reaches from `doc`, as written: the whole document (trimmed of its spaces) for an
/// empty path, None where the path reaches nothing or a step is NULL. Each level is read by its own parse, so
/// the text returned is the document's own bytes, not a re-serialization.
fn walk<'a>(doc: &'a str, steps: &[Option<String>]) -> anyhow::Result<Option<&'a str>> {
    let Some((step, rest)) = steps.split_first() else { return Ok(Some(doc.trim_matches(JSON_SPACE))) };
    let Some(step) = step else { return Ok(None) };
    let found: Option<&'a RawValue> = match doc.trim_start_matches(JSON_SPACE).as_bytes().first().copied() {
        // (a duplicate key's last value wins, as the map keeps the last it reads)
        Some(b'{') => serde_json::from_str::<BTreeMap<String, &'a RawValue>>(doc)?.get(step).copied(),
        Some(b'[') => {
            let items: Vec<&'a RawValue> = serde_json::from_str(doc)?;
            index(step, items.len()).map(|i| items[i])
        }
        _ => None, // (a scalar has no steps)
    };
    match found {
        Some(raw) => walk(raw.get(), rest),
        None => Ok(None),
    }
}

/// An array index as Postgres reads one: an optional sign, then digits, where a negative one counts from the end
/// (`-1` is the last). None when it isn't a whole number or lies outside the array.
fn index(step: &str, len: usize) -> Option<usize> {
    let (negative, digits) = match step.as_bytes().first().copied() {
        Some(b'-') => (true, &step[1..]),
        Some(b'+') => (false, &step[1..]),
        _ => (false, step),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: i64 = digits.parse().ok()?;
    let value = if negative { -n } else { n };
    let at = if value < 0 { len as i64 + value } else { value };
    (0..len as i64).contains(&at).then_some(at as usize)
}

/// `#>>`'s text for a value's text: a JSON string's content, unescaped; a JSON null is NULL; anything else as written.
fn unquoted(raw: &str) -> Option<String> {
    match raw {
        "null" => None,
        s if s.starts_with('"') => serde_json::from_str(s).ok(),
        s => Some(s.to_string()),
    }
}

/// `@>` and `<@`: does the left document contain the right (`<@` swaps them before the call)?
fn contains_all(a: &[ArrayRef], n: usize) -> anyhow::Result<ArrayRef> {
    let (lefts, rights) = (text_arg(&a[0])?, text_arg(&a[1])?);
    let out = (0..n).map(|i| -> anyhow::Result<Option<bool>> {
        match (&lefts[i], &rights[i]) {
            (Some(left), Some(right)) => Ok(Some(contains(&document(left)?, &document(right)?, true))),
            _ => Ok(None),
        }
    });
    Ok(Arc::new(BooleanArray::from(out.collect::<anyhow::Result<Vec<_>>>()?)))
}

/// Does `have` contain `want`? Objects by their keys, arrays by each element of `want` lying in some element of
/// `have`. Only at the top does an array also contain a scalar equal to one of its elements.
fn contains(have: &Value, want: &Value, top: bool) -> bool {
    match (have, want) {
        (Value::Object(h), Value::Object(w)) => w.iter().all(|(k, wv)| h.get(k).is_some_and(|hv| contains(hv, wv, false))),
        (Value::Array(h), Value::Array(w)) => w.iter().all(|wv| h.iter().any(|hv| contains(hv, wv, false))),
        (Value::Array(h), w) if top && is_scalar(w) => h.iter().any(|hv| same_scalar(hv, w)),
        _ => same_scalar(have, want),
    }
}

fn is_scalar(v: &Value) -> bool { !matches!(v, Value::Array(_) | Value::Object(_)) }

/// Two scalars are the same: numbers as numbers (`1` is `1.0`), strings, booleans and null by value.
fn same_scalar(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

/// `?`: does the key exist? A key of an object, a string element of an array, or the document itself when it is a
/// string (Postgres treats a scalar string as an array of one element); any other scalar holds none.
fn has_key(doc: &Value, key: &str) -> bool {
    match doc {
        Value::Object(map) => map.contains_key(key),
        Value::Array(items) => items.iter().any(|v| v.as_str() == Some(key)),
        Value::String(s) => s == key,
        _ => false,
    }
}

/// `?`, one key.
fn exists(a: &[ArrayRef], n: usize) -> anyhow::Result<ArrayRef> {
    let (docs, keys) = (text_arg(&a[0])?, text_arg(&a[1])?);
    let out = (0..n).map(|i| -> anyhow::Result<Option<bool>> {
        match (&docs[i], &keys[i]) {
            (Some(doc), Some(key)) => Ok(Some(has_key(&document(doc)?, key))),
            _ => Ok(None),
        }
    });
    Ok(Arc::new(BooleanArray::from(out.collect::<anyhow::Result<Vec<_>>>()?)))
}

/// `?|` (`all` false) and `?&` (`all` true): any or all of a list of keys. A NULL key is left out, as Postgres
/// leaves it out: `?|` of no keys is false and `?&` of none is true.
fn any_all(a: &[ArrayRef], n: usize, all: bool) -> anyhow::Result<ArrayRef> {
    let (docs, keys) = (text_arg(&a[0])?, lists(&a[1])?);
    let out = (0..n).map(|i| -> anyhow::Result<Option<bool>> {
        match (&docs[i], &keys[i]) {
            (Some(doc), Some(keys)) => {
                let doc = document(doc)?;
                let mut found = keys.iter().flatten().map(|k| has_key(&doc, k));
                Ok(Some(if all { found.all(|f| f) } else { found.any(|f| f) }))
            }
            _ => Ok(None),
        }
    });
    Ok(Arc::new(BooleanArray::from(out.collect::<anyhow::Result<Vec<_>>>()?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn elements(v: &[&str]) -> Vec<Option<String>> { v.iter().map(|s| Some(s.to_string())).collect() }

    /// `#>` (or `#>>`) of a document at a path literal, as the functions answer it.
    fn at(doc: &str, path: &str, text: bool) -> Option<String> {
        walk(doc, &array_literal(path).unwrap()).unwrap().and_then(|raw| if text { unquoted(raw) } else { Some(raw.to_string()) })
    }

    #[test]
    fn path_literals_read_as_postgres_reads_them() {
        assert_eq!(array_literal("{b,0}").unwrap(), elements(&["b", "0"]));
        assert_eq!(array_literal("{}").unwrap(), Vec::<Option<String>>::new());
        assert_eq!(array_literal("{ a , b c }").unwrap(), elements(&["a", "b c"]));
        assert_eq!(array_literal(r#"{"a,b","c\"d","e\\f"}"#).unwrap(), elements(&["a,b", "c\"d", "e\\f"]));
        assert_eq!(array_literal(r#"{ "a" , b }"#).unwrap(), elements(&["a", "b"]));
        assert_eq!(array_literal("{NULL,\"NULL\"}").unwrap(), vec![None, Some("NULL".to_string())]);
        assert_eq!(array_literal(r"{a\,b}").unwrap(), elements(&["a,b"]));
        for bad in ["{a,}", "{,a}", "{a,,b}", "a,b", r#"{"a"b}"#, "{\"a"] {
            assert!(array_literal(bad).is_err(), "{bad} is a malformed literal");
        }
    }

    #[test]
    fn path_walks_objects_and_arrays_as_postgres_does() {
        let doc = r#"{"a":"x","b":[1,2,3],"c":{"d":null},"e,f":1,"g\"h":2}"#;
        assert_eq!(at(doc, "{b,-1}", false).as_deref(), Some("3"));
        assert_eq!(at(doc, "{b,-3}", false).as_deref(), Some("1"));
        assert_eq!(at(doc, "{b,-4}", false), None, "out of range from the end");
        assert_eq!(at(doc, "{b,3}", false), None, "out of range");
        assert_eq!(at(doc, "{b,x}", false), None, "not an integer on an array");
        assert_eq!(at(doc, "{b,1.0}", false), None, "a decimal on an array");
        assert_eq!(at(doc, "{b,a}", false), None, "a key on an array");
        assert_eq!(at(doc, "{a,b}", false), None, "a path into a scalar");
        assert_eq!(at(doc, "{c,d}", false).as_deref(), Some("null"), "a JSON null is found");
        assert_eq!(at(doc, "{a,NULL}", false), None, "a NULL step");
        assert_eq!(at(doc, "{\"e,f\"}", false).as_deref(), Some("1"), "a quoted step with a comma");
        assert_eq!(at(doc, r#"{"g\"h"}"#, false).as_deref(), Some("2"), "a quoted step with a quote");
        assert_eq!(at(r#"{"a":1,"a":2}"#, "{a}", false).as_deref(), Some("2"), "a duplicate key: the last wins");
        assert_eq!(at("[1,2,3]", "{+1}", false).as_deref(), Some("2"), "a plus sign is an index");
        assert_eq!(at("[1,2,3]", "{01}", false).as_deref(), Some("2"), "a leading zero is an index");
        assert_eq!(at("[1,2,3]", "{-0}", false).as_deref(), Some("1"), "minus zero is the first");
        assert_eq!(at("[1,2,3]", "{ 1 }", false).as_deref(), Some("2"), "an element's spaces are trimmed");
        assert_eq!(at(r#"{"1":"one"}"#, "{1}", false).as_deref(), Some(r#""one""#), "a number step names a key too");
        assert_eq!(at(r#"{"a":1}"#, "{5}", false), None, "an index on an object");
        assert_eq!(at("5", "{0}", false), None, "a scalar has no steps");
        assert_eq!(at("null", "{a}", false), None, "a null has no steps");
    }

    #[test]
    fn path_answers_are_the_text_as_written() {
        let doc = r#"  {"a": 1.0 , "b" :  {"x" : 1 }, "s": "a\"b", "u": "aéb", "n": null}  "#;
        assert_eq!(at(doc, "{}", false).as_deref(), Some(r#"{"a": 1.0 , "b" :  {"x" : 1 }, "s": "a\"b", "u": "aéb", "n": null}"#), "the empty path, spaces at the ends trimmed");
        assert_eq!(at(doc, "{a}", false).as_deref(), Some("1.0"), "a number keeps its digits");
        assert_eq!(at(doc, "{a}", true).as_deref(), Some("1.0"));
        assert_eq!(at(doc, "{b}", false).as_deref(), Some(r#"{"x" : 1 }"#), "an object keeps its spaces");
        assert_eq!(at(doc, "{b}", true).as_deref(), Some(r#"{"x" : 1 }"#));
        assert_eq!(at(doc, "{s}", false).as_deref(), Some(r#""a\"b""#), "#> keeps the escapes");
        assert_eq!(at(doc, "{s}", true).as_deref(), Some("a\"b"), "#>> unescapes a string");
        assert_eq!(at(doc, "{u}", true).as_deref(), Some("a\u{e9}b"), "#>> unescapes a unicode escape");
        assert_eq!(at(doc, "{n}", false).as_deref(), Some("null"), "#> answers a JSON null as null");
        assert_eq!(at(doc, "{n}", true), None, "#>> answers a JSON null as SQL NULL");
        assert_eq!(at(r#""a\"b""#, "{}", true).as_deref(), Some("a\"b"));
    }

    #[test]
    fn containment_as_postgres() {
        let c = |have: &str, want: &str| contains(&document(have).unwrap(), &document(want).unwrap(), true);
        let doc = r#"{"a":"x","b":[1,2],"c":{"d":null}}"#;
        assert!(c(doc, r#"{"a":"x"}"#));
        assert!(c(doc, r#"{"c":{"d":null}}"#));
        assert!(c(doc, r#"{"b":[2]}"#));
        assert!(!c(doc, r#"{"b":[2,3]}"#));
        assert!(c(r#"{"a":{"b":{"c":1}}}"#, r#"{"a":{"b":{}}}"#), "nested objects, the right one smaller");
        assert!(!c(r#"{"a":{"b":{}}}"#, r#"{"a":{"b":{"c":1}}}"#), "nested objects, the right one bigger");
        assert!(c(r#"[1,2,[3,4]]"#, "[[4]]"), "an array inside an array");
        assert!(c(r#"[1,2,[3,4]]"#, "[2,[3]]"));
        assert!(c(r#"["a", "b"]"#, r#""a""#), "an array contains a top-level scalar");
        assert!(!c(r#"[["a"]]"#, r#""a""#), "not one nested in an array");
        assert!(!c(r#"[["a"]]"#, r#"["a"]"#), "an element that is an array isn't a scalar");
        assert!(c(r#"[1, [2]]"#, "[[2]]"));
        assert!(!c(r#"{"a":[1]}"#, r#"{"a":1}"#), "an array never contains a scalar that isn't top-level");
        assert!(!c(r#"[{"a":1}]"#, r#"{"a":1}"#), "an array never contains an object");
        assert!(!c(r#"{"a":1}"#, r#"[{"a":1}]"#), "an object never contains an array");
        assert!(c("1", "1.0"), "1 is 1.0");
        assert!(c(r#"{"a": 1.0}"#, r#"{"a": 1}"#));
        assert!(c("{}", "{}"), "empty objects");
        assert!(c("[]", "[]"), "empty arrays");
        assert!(!c("{}", r#"{"a":1}"#));
        assert!(!c("[]", "[1]"));
        assert!(c("[1]", "[]"));
        assert!(c(r#"{"a":null}"#, r#"{"a":null}"#), "null is null");
        assert!(!c(r#"{"a":null}"#, r#"{"a":1}"#));
        assert!(!c(r#"{"a":"1"}"#, r#"{"a":1}"#), "a string is no number");
        assert!(c("[1,1]", "[1,1,1]"), "repeated elements");
        assert!(c(r#"{"a":1,"a":2}"#, r#"{"a":2}"#), "a duplicate key's last value");
        assert!(c(r#""x""#, r#""x""#));
        assert!(!c(r#""a""#, r#"["a"]"#), "a scalar contains no array");
    }

    #[test]
    fn existence_as_postgres() {
        let e = |doc: &str, key: &str| has_key(&document(doc).unwrap(), key);
        assert!(e(r#"["foo", "bar"]"#, "foo"));
        assert!(!e(r#"["foo", ["bar"]]"#, "bar"), "not in a nested array");
        assert!(e(r#"[1, "1"]"#, "1"), "a string element, not a number");
        assert!(!e("[1]", "1"));
        assert!(!e(r#"[{"a":1}]"#, "a"), "not a key of an object in an array");
        assert!(e(r#"{"a":1}"#, "a"));
        assert!(e(r#"{"a":null}"#, "a"), "a key whose value is null");
        assert!(!e(r#"{"a":1}"#, "b"));
        assert!(e(r#""foo""#, "foo"), "a scalar string is an array of one element");
        assert!(!e("5", "5"), "a scalar number holds no key");
        // ?| and ?& over lists: an empty list, and NULL keys, left out
        let any_all = |doc: &str, keys: &[Option<&str>], all: bool| {
            let doc = document(doc).unwrap();
            let keys: Vec<Option<String>> = keys.iter().map(|k| k.map(str::to_string)).collect();
            let mut found = keys.iter().flatten().map(|k| has_key(&doc, k));
            if all { found.all(|f| f) } else { found.any(|f| f) }
        };
        assert!(!any_all(r#"{"a":1}"#, &[], false), "?| of no keys is false");
        assert!(any_all(r#"{"a":1}"#, &[], true), "?& of no keys is true");
        assert!(any_all(r#"{"a":1}"#, &[Some("x"), Some("a")], false));
        assert!(!any_all(r#"{"a":1}"#, &[Some("x"), None], false), "a NULL key is left out");
        assert!(any_all(r#"{"a":1}"#, &[Some("a"), None], true), "a NULL key is left out of ?&");
        assert!(!any_all(r#"{"a":1}"#, &[Some("a"), Some("x")], true));
        assert!(!any_all(r#"{"a":1}"#, &[None], false));
        assert!(any_all(r#"{"a":1}"#, &[None], true));
    }

    #[test]
    fn index_takes_signs_and_digits() {
        assert_eq!(index("1", 3), Some(1));
        assert_eq!(index("-1", 3), Some(2));
        assert_eq!(index("-3", 3), Some(0));
        assert_eq!(index("-4", 3), None);
        assert_eq!(index("3", 3), None);
        assert_eq!(index("-0", 3), Some(0));
        assert_eq!(index("", 3), None);
        assert_eq!(index("-", 3), None);
        assert_eq!(index("1e2", 300), None);
        assert_eq!(index("99999999999999999999", 3), None, "too big for an index");
    }

    #[test]
    fn question_operators_are_read_as_postgres_reads_them() {
        assert!(statement("SELECT doc ?| ARRAY['a'] FROM t").is_some());
        assert!(statement("SELECT doc ? 'a' AS x, doc ?& '{a}' FROM t WHERE doc ? 'b'").is_some());
        assert!(statement("SELECT 'a?b' FROM t").is_none(), "a ? in a string is the generic dialect's, and it reads it");
        assert!(statement("SELECT 1 FROM t; SELECT doc ? 'a' FROM t").is_none(), "one query only");
        // (sqlparser's PostgreSQL dialect reads `doc ?| FROM t`, a missing right operand, as a query: Postgres
        // refuses it as a syntax error (42601), Pondra answers 42703 from planning. A known limit, not tested here.)
    }

    #[test]
    fn refuses_what_is_not_json() {
        let e = document("not json").unwrap_err();
        assert_eq!(crate::codes::of(&e), "22P02");
        assert!(e.to_string().starts_with("invalid input syntax for type json"));
        assert_eq!(crate::codes::of(&valid("{").unwrap_err()), "22P02");
    }
}
