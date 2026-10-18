//! Postgres's JSON operators on JSON text (round 35): `->` (a value as JSON text), `#>` and `#>>` (a path), `@>` and
//! `<@` (containment), `?`, `?|` and `?&` (existence). datafusion-functions-json reads a path as one key and plans none
//! of the others, so they are planned here, ahead of its planner (`engine::session`), when the left side is text. `->>` and
//! every operator on another type keep the planning they have. The answers follow Postgres 16: a document's text is
//! kept as written (`types.rs`), so `->` and `#>` answer the value's text as the document has it. Text that isn't JSON
//! answers NULL where a walk meets it; `@>`, `<@`, `?`, `?|` and `?&` read the whole document and refuse it (22P02).
//! `text` groups the operators the way Postgres does, where SQL comes in (`path::door`).
use crate::pg_functions::{follows_json, function, marked, text_arg, texts_out, MARK};
use crate::pg_json_make::{fail, jsonb_text, refuse};
use crate::pg_text::array_literal;
use datafusion::arrow::array::{Array, ArrayAccessor, ArrayRef, AsArray, BooleanArray, Int32Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::{Expr, ExprSchemable, ScalarUDF, Signature, Volatility};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, BinaryOperator, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;
use jiter::{Jiter, Peek};
use serde_json::Value;
use std::borrow::Cow;
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock};

/// The engine's session (`engine::session`) with Postgres's JSON in every session: its operators (`#>`, `#>>`,
/// `@>`, `?|`, …) planned ahead of the engine's planners, functions-json's among them; a text cast's JSON mark taken off
/// after type coercion (`pg_json_make::AsText`); and the functions they call.
pub fn planned(ctx: SessionContext) -> SessionContext {
    let mut state = SessionStateBuilder::new_from_existing(ctx.state());
    state.expr_planners().get_or_insert_with(Vec::new).insert(0, planner());
    let ctx = SessionContext::new_with_state(state.build());
    ctx.add_analyzer_rule(Arc::new(crate::pg_json_make::AsText));
    register(&ctx);
    ctx
}

pub fn planner() -> Arc<dyn ExprPlanner> { Arc::new(Operators) }

#[derive(Debug)]
struct Operators;

impl ExprPlanner for Operators {
    fn plan_binary_op(&self, e: RawBinaryExpr, schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>> {
        let Some((name, both_text)) = operator(&e.op) else { return Ok(PlannerResult::Original(e)) };
        let text = |x: &Expr| x.get_type(schema).is_ok_and(|t| is_text(&t));
        let planned = match e.op {
            BinaryOperator::Minus | BinaryOperator::HashMinus => deletes(&e, schema)?,
            _ => text(&e.left) && (!both_text || text(&e.right)), // (an array's @> stays DataFusion's, a JSON union's -> stays functions-json's)
        };
        if !planned {
            return Ok(PlannerResult::Original(e));
        }
        // The column's title is what functions-json gives it (`doc #>> '{b,0}'`), so a default name doesn't change.
        let alias = format!("{} {} {}", as_title(&e.left), e.op, as_title(&e.right));
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(named(name), vec![e.left, e.right]));
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(call, None::<&str>, alias))))
    }
}

/// The function an operator calls, and whether both sides must be text. `->` takes a text key or an integer index,
/// so only its left side must be text. `-` and `#-` are planned by `deletes`, which asks for the same two.
fn operator(op: &BinaryOperator) -> Option<(&'static str, bool)> {
    Some(match op {
        BinaryOperator::Arrow => ("json_object_field", false),
        BinaryOperator::HashArrow => ("pondra_json_path", false),
        BinaryOperator::HashLongArrow => ("pondra_json_path_text", false),
        BinaryOperator::AtArrow => ("jsonb_contains", true),
        BinaryOperator::ArrowAt => ("jsonb_contained", true),
        BinaryOperator::Question => ("jsonb_exists", false),
        BinaryOperator::QuestionPipe => ("jsonb_exists_any", false),
        BinaryOperator::QuestionAnd => ("jsonb_exists_all", false),
        BinaryOperator::Minus => ("jsonb_delete", false),
        BinaryOperator::HashMinus => ("jsonb_delete_path", false),
        _ => return None,
    })
}

/// Text in any of DataFusion's string types: what the JSON functions read as a document's text, from a column or a literal.
pub(crate) fn is_text(t: &DataType) -> bool { matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) }

/// `s` parses as JSON (`json` and `jsonb` alike): jiter reads its syntax, as the functions do, and nothing is built.
pub fn is_json(s: &str) -> bool { crate::pg_json_make::refused(s).is_none() }

/// Postgres's words for a value that isn't JSON, which it quotes as far as 60 characters (jsonb's
/// refusals say `type json` too).
pub(crate) fn json_refusal(s: &str) -> String {
    let head: String = s.chars().take(60).collect();
    format!("invalid input syntax for type json: \"{head}\"")
}

/// `s` as JSON text the lake keeps: itself when it parses as JSON, else Postgres's refusal (22P02).
pub fn json_of(s: &str) -> anyhow::Result<String> {
    match crate::pg_json_make::refused(s) {
        None => Ok(s.to_string()),
        Some("54001") => Err(crate::codes::coded("54001", "stack depth limit exceeded")),
        Some(code) => Err(crate::codes::coded(code, json_refusal(s))),
    }
}

fn is_list(t: &DataType) -> bool { matches!(t, DataType::List(_) | DataType::LargeList(_)) }

/// The JSON type a field is marked with (`json` or `jsonb`), if it is.
fn json_mark(f: &Field) -> Option<&str> { f.metadata().get(MARK).map(String::as_str).filter(|m| matches!(*m, "json" | "jsonb")) }

/// Whether `-` or `#-` deletes from a document here (`jsonb_delete`, `jsonb_delete_path`), else DataFusion keeps the
/// operator it has. A document takes `-` a key, an integer or a list of keys, and `#-` a path (text or a list): what
/// Postgres's jsonb operators take. A bare literal's `-` stays DataFusion's, since Postgres has no `unknown - unknown`.
/// json has neither operator, and a right side jsonb has none for is refused by name (42883), as Postgres refuses it.
fn deletes(e: &RawBinaryExpr, schema: &DFSchema) -> Result<bool> {
    let (left, right) = (e.left.to_field(schema)?.1, e.right.to_field(schema)?.1);
    let minus = matches!(e.op, BinaryOperator::Minus);
    if !is_text(left.data_type()) || (minus && matches!(e.left, Expr::Literal(..))) {
        return Ok(false);
    }
    // (a NULL on the right is a NULL text or path, as Postgres reads an unknown there)
    let keys = is_text(right.data_type()) || matches!(right.data_type(), DataType::Null) || is_list(right.data_type());
    let fits = keys || (minus && is_integer(right.data_type()));
    let refused = json_mark(&left) == Some("json") || json_mark(&right).is_some() || (!fits && (!minus || json_mark(&left).is_some()));
    if refused {
        let message = format!("operator does not exist: {} {} {}", type_name(&e.left, &left), e.op, type_name(&e.right, &right));
        return Err(refuse("42883", message));
    }
    Ok(fits)
}

/// The name Postgres gives an operand's type in its message about an operator (`jsonb - integer`).
fn type_name(e: &Expr, f: &Field) -> &'static str {
    if let Some(mark) = json_mark(f) {
        return if mark == "json" { "json" } else { "jsonb" };
    }
    match f.data_type() {
        DataType::Null => "unknown",
        t if is_text(t) => if matches!(e, Expr::Literal(..)) { "unknown" } else { "text" },
        t if is_list(t) => "text[]",
        t if is_integer(t) => "integer",
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Decimal128(..) | DataType::Decimal256(..) => "numeric",
        _ => "unknown",
    }
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

/// The functions the operators call, by Postgres's names where Postgres has them (`jsonb_contains`, `json_object_field`
/// and its aliases); the path ones keep `pondra_` names, as `pondra_json` does. `register` adds them to every session.
static FUNCTIONS: LazyLock<Vec<Arc<ScalarUDF>>> = LazyLock::new(|| {
    let two = || Signature::any(2, Volatility::Immutable);
    let one = || Signature::any(1, Volatility::Immutable);
    // (a path's keys are any number of text arguments: `json_extract_path(doc, 'a', 'b')`; zero keys are refused in `extract`)
    let any = || Signature::variadic_any(Volatility::Immutable);
    vec![
        Arc::new(follows_json("json_object_field", two(), |_| DataType::Utf8, |a, n| field(a, n).map_err(fail)).with_aliases(["jsonb_object_field", "json_array_element", "jsonb_array_element"])),
        Arc::new(follows_json("pondra_json_path", two(), |_| DataType::Utf8, |a, n| path(a, n, false).map_err(fail))),
        Arc::new(function("pondra_json_path_text", two(), |_| DataType::Utf8, |a, n| path(a, n, true).map_err(fail))),
        Arc::new(function("jsonb_contains", two(), |_| DataType::Boolean, |a, n| contains_all(a, n).map_err(fail))),
        Arc::new(function("jsonb_contained", two(), |_| DataType::Boolean, |a, n| contained(a, n).map_err(fail))),
        Arc::new(function("jsonb_exists", two(), |_| DataType::Boolean, |a, n| exists(a, n).map_err(fail))),
        Arc::new(function("jsonb_exists_any", two(), |_| DataType::Boolean, |a, n| any_all(a, n, false).map_err(fail))),
        Arc::new(function("jsonb_exists_all", two(), |_| DataType::Boolean, |a, n| any_all(a, n, true).map_err(fail))),
        Arc::new(function("json_typeof", one(), |_| DataType::Utf8, |a, n| type_of(a, n).map_err(fail))),
        Arc::new(function("jsonb_typeof", one(), |_| DataType::Utf8, |a, n| type_of(a, n).map_err(fail))),
        Arc::new(function("json_array_length", one(), |_| DataType::Int32, |a, n| array_length(a, n).map_err(fail))),
        Arc::new(function("jsonb_array_length", one(), |_| DataType::Int32, |a, n| array_length(a, n).map_err(fail))),
        Arc::new(marked("json_extract_path", any(), |_| DataType::Utf8, Some("json"), |a, n| extract("json_extract_path", a, n, false, false).map_err(fail))),
        Arc::new(marked("jsonb_extract_path", any(), |_| DataType::Utf8, Some("jsonb"), |a, n| extract("jsonb_extract_path", a, n, false, true).map_err(fail))),
        Arc::new(function("json_extract_path_text", any(), |_| DataType::Utf8, |a, n| extract("json_extract_path_text", a, n, true, false).map_err(fail))),
        Arc::new(function("jsonb_extract_path_text", any(), |_| DataType::Utf8, |a, n| extract("jsonb_extract_path_text", a, n, true, true).map_err(fail))),
    ]
});

/// The function called `name` (one of the ones above, or an alias of one).
fn named(name: &str) -> Arc<ScalarUDF> {
    FUNCTIONS.iter().chain(crate::pg_json_edit::FUNCTIONS.iter()).find(|f| f.name() == name).cloned().expect("pg_json.rs calls only the functions it makes")
}

/// Adds the functions to a session, so `json_object_field(…)`, `jsonb_contains(…)` and the rest can be called by name.
pub fn register(ctx: &SessionContext) {
    for f in FUNCTIONS.iter() {
        ctx.register_udf(f.as_ref().clone());
    }
    crate::pg_json_make::register(ctx);
    crate::pg_json_edit::register(ctx);
    crate::pg_json_sets::register(ctx);
}

/// `s` parsed, for containment and existence: a document that isn't JSON is refused (22P02, in `json_of`'s words).
fn document(s: &str) -> anyhow::Result<Value> {
    json_of(s)?;
    Ok(serde_json::from_str(s)?)
}

/// A path or a list of keys, one per row: a text-array literal (`'{b,0}'`) or a list (`ARRAY['b', '0']`). None where
/// the argument is NULL, and a NULL element is None inside the list.
pub(crate) fn lists(a: &ArrayRef) -> anyhow::Result<Vec<Option<Vec<Option<String>>>>> {
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

pub(crate) const JSON_SPACE: &[char] = &[' ', '\t', '\n', '\r'];

/// One step of a path, as an operator takes it. `->` takes a text key (`Key`, an object's member only) or an integer
/// (`Index`, an array's element, negative from the end); `#>` takes each element as text (`Path`): a key of an object,
/// or an index of an array when it spells one.
#[derive(Clone, Copy)]
enum Step<'a> {
    Key(&'a str),
    Index(i64),
    Path(&'a str),
}

/// `#>` (`text` false) and `#>>` (true): what the path reaches in each document, NULL where it reaches nothing.
fn path(a: &[ArrayRef], n: usize, text: bool) -> anyhow::Result<ArrayRef> {
    let (docs, paths) = (text_arg(&a[0])?, lists(&a[1])?);
    reached(&docs, paths, n, text, false)
}

/// What each row's path reaches in its document: `paths[i]` is the path's steps (None where it is NULL, and a NULL step
/// makes the answer NULL). `text` and `jsonb` answer as `answer` says.
fn reached(docs: &[Option<String>], paths: Vec<Option<Vec<Option<String>>>>, n: usize, text: bool, jsonb: bool) -> anyhow::Result<ArrayRef> {
    let out = (0..n).map(|i| -> anyhow::Result<Option<String>> {
        let (Some(doc), Some(elements)) = (&docs[i], &paths[i]) else { return Ok(None) };
        let Some(steps) = elements.iter().map(|e| e.as_deref().map(Step::Path)).collect::<Option<Vec<_>>>() else { return Ok(None) };
        match find(doc, &steps) {
            Some(raw) => answer(raw, text, jsonb),
            None => Ok(None),
        }
    });
    Ok(texts_out(out.collect::<anyhow::Result<Vec<_>>>()?))
}

/// A value a path reached, as the function answers it. As written, unless `text` (`#>>`'s text: `text_of`); `jsonb`
/// answers as jsonb prints it.
pub(crate) fn answer(raw: &str, text: bool, jsonb: bool) -> anyhow::Result<Option<String>> {
    if text {
        return text_of(raw, jsonb);
    }
    Ok(Some(if jsonb { jsonb_text(raw)? } else { raw.to_string() }))
}

/// `#>>`'s text for a value: a JSON string's content (unescaped), NULL for a JSON null, and anything else as written,
/// or as jsonb prints it (`jsonb`). Shared with the functions that expand a document (`pg_json_sets`).
pub(crate) fn text_of(raw: &str, jsonb: bool) -> anyhow::Result<Option<String>> {
    if raw == "null" || raw.starts_with('"') {
        return Ok(unquoted(raw));
    }
    Ok(Some(if jsonb { jsonb_text(raw)? } else { raw.to_string() }))
}

/// The JSON type of a document's one value, as Postgres names it (`object`, `array`, `string`, `number`, `boolean`,
/// `null`). Text that isn't one JSON value is 22P02, as Postgres refuses it.
pub(crate) fn kind(doc: &str) -> anyhow::Result<&'static str> {
    let refused = || crate::codes::coded("22P02", json_refusal(doc));
    let mut jiter = Jiter::new(doc.as_bytes());
    let peek = jiter.peek().map_err(|_| refused())?;
    jiter.known_skip(peek).map_err(|_| refused())?;
    jiter.finish().map_err(|_| refused())?;
    Ok(match peek {
        Peek::Null => "null",
        Peek::True | Peek::False => "boolean",
        Peek::String => "string",
        Peek::Array => "array",
        Peek::Object => "object",
        _ => "number",
    })
}

/// `json_typeof` and `jsonb_typeof`: the type of each document's value. NULL where the document is.
fn type_of(a: &[ArrayRef], n: usize) -> anyhow::Result<ArrayRef> {
    let docs = cast(&a[0], &DataType::Utf8)?;
    let docs = docs.as_string::<i32>();
    let kinds = (0..n).map(|i| docs.is_valid(i).then(|| kind(docs.value(i))).transpose()).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Arc::new(StringArray::from(kinds)))
}

/// `json_array_length` and `jsonb_array_length`: how many elements each document's array has. A document that isn't an
/// array is 22023, as Postgres words it; NULL where the document is.
fn array_length(a: &[ArrayRef], n: usize) -> anyhow::Result<ArrayRef> {
    let docs = cast(&a[0], &DataType::Utf8)?;
    let docs = docs.as_string::<i32>();
    let lengths = (0..n).map(|i| docs.is_valid(i).then(|| length_of(docs.value(i))).transpose()).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Arc::new(Int32Array::from(lengths)))
}

fn length_of(doc: &str) -> anyhow::Result<i32> {
    match kind(doc)? {
        "array" => array_len(doc).map(|n| n as i32).ok_or_else(|| crate::codes::coded("22P02", json_refusal(doc))),
        "object" => Err(crate::codes::coded("22023", "cannot get array length of a non-array")),
        _ => Err(crate::codes::coded("22023", "cannot get array length of a scalar")),
    }
}

/// `json_extract_path` and kin: `#>` (`#>>` when `text`) with each argument after the document as one step of the path.
/// `jsonb` answers as jsonb prints. Zero keys are refused as Postgres refuses them (42883).
fn extract(name: &str, a: &[ArrayRef], n: usize, text: bool, jsonb: bool) -> anyhow::Result<ArrayRef> {
    if a.len() < 2 {
        return Err(crate::codes::coded("42883", format!("function {name}(unknown) does not exist")));
    }
    let docs = text_arg(&a[0])?;
    let keys = a[1..].iter().map(text_arg).collect::<datafusion::common::Result<Vec<_>>>()?;
    let paths = (0..n).map(|i| Some(keys.iter().map(|key| key[i].clone()).collect())).collect();
    reached(&docs, paths, n, text, jsonb)
}

/// `->`, and `json_array_element` and its kin: the value at a text key of an object or an integer index of an array,
/// as JSON text. Each row reads its own document; NULL where a key is NULL or the document has nothing there.
fn field(a: &[ArrayRef], n: usize) -> anyhow::Result<ArrayRef> {
    // A table's text is a string view, and a batch of one is read where it lies: a copy of every document per batch
    // cost `->` half its speed on a million rows.
    if a[0].data_type() == &DataType::Utf8View {
        return field_of(a[0].as_string_view(), &a[1], n);
    }
    let docs = cast(&a[0], &DataType::Utf8)?;
    field_of(docs.as_string::<i32>(), &a[1], n)
}

/// `field` over a column of documents, whichever string array holds them.
fn field_of<'a, D>(docs: D, key: &ArrayRef, n: usize) -> anyhow::Result<ArrayRef>
where
    D: ArrayAccessor<Item = &'a str> + Array + Copy,
{
    if is_integer(key.data_type()) {
        let keys = cast(key, &DataType::Int64)?;
        let keys = keys.as_primitive::<Int64Type>();
        return Ok(each_row(docs, n, |i| keys.is_valid(i).then(|| Step::Index(keys.value(i)))));
    }
    let keys = cast(key, &DataType::Utf8)?;
    let keys = keys.as_string::<i32>();
    Ok(each_row(docs, n, |i| keys.is_valid(i).then(|| Step::Key(keys.value(i)))))
}

/// One step per row over a column of documents, as text. `step(i)` is row `i`'s step, None where its key is NULL.
fn each_row<'a, 's, D>(docs: D, n: usize, step: impl Fn(usize) -> Option<Step<'s>>) -> ArrayRef
where
    D: ArrayAccessor<Item = &'a str> + Array + Copy,
{
    let out = (0..n).map(|i| if docs.is_null(i) { None } else { step(i).and_then(|s| find(docs.value(i), &[s])) });
    Arc::new(StringArray::from_iter(out))
}

pub(crate) fn is_integer(d: &DataType) -> bool {
    matches!(d, DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
}

/// The value a path reaches in `doc`, as written. An empty path is the document itself, which must be JSON; any other
/// path is read a step at a time, and text that isn't JSON where a step looks answers None without being validated.
fn find<'a>(doc: &'a str, steps: &[Step]) -> Option<&'a str> {
    if steps.is_empty() {
        return is_json(doc).then(|| doc.trim_matches(JSON_SPACE));
    }
    walk(doc, steps)
}

/// The value at `steps` in `doc`. Each step's value is a slice of the document before it, so the answer is the
/// document's own text.
fn walk<'a>(doc: &'a str, steps: &[Step]) -> Option<&'a str> {
    match steps.split_first() {
        None => Some(doc.trim_matches(JSON_SPACE)),
        Some((step, rest)) => walk(child(doc, *step)?, rest),
    }
}

/// The value one step reaches in `doc`, as written, or None.
fn child<'a>(doc: &'a str, step: Step) -> Option<&'a str> {
    match step {
        Step::Key(key) => member(doc, key),
        Step::Index(at) => element(doc, at),
        Step::Path(text) => match doc.trim_start_matches(JSON_SPACE).as_bytes().first()? {
            b'{' => member(doc, text),
            b'[' => element(doc, index_of(text)?),
            _ => None,
        },
    }
}

/// The value of the object `doc`'s last member named `key`, as written: Postgres takes a duplicate key's last, so the
/// whole object is read. Values it skips are not built into anything.
fn member<'a>(doc: &'a str, key: &str) -> Option<&'a str> {
    let mut jiter = Jiter::new(doc.as_bytes());
    if jiter.peek().ok()? != Peek::Object {
        return None;
    }
    let mut found = None;
    // (each key is compared as it is read, so the borrow of jiter's text ends before the next read)
    let mut next = jiter.known_object().ok()?.map(|name| name == key);
    while let Some(is_key) = next {
        let peek = jiter.peek().ok()?;
        let start = jiter.current_index();
        jiter.known_skip(peek).ok()?;
        if is_key {
            found = Some((start, jiter.current_index()));
        }
        next = jiter.next_key().ok()?.map(|name| name == key);
    }
    let (start, end) = found?;
    doc.get(start..end).map(|value| value.trim_matches(JSON_SPACE))
}

/// The element `at` of the array `doc`, as written; a negative index counts from the end, and an index past the array
/// is None.
fn element<'a>(doc: &'a str, at: i64) -> Option<&'a str> {
    let mut jiter = Jiter::new(doc.as_bytes());
    if jiter.peek().ok()? != Peek::Array {
        return None;
    }
    let at = if at < 0 { array_len(doc)? as i64 + at } else { at };
    if at < 0 {
        return None;
    }
    let mut peek = jiter.known_array().ok()??;
    for _ in 0..at {
        jiter.known_skip(peek).ok()?;
        peek = jiter.array_step().ok()??;
    }
    let start = jiter.current_index();
    jiter.known_skip(peek).ok()?;
    doc.get(start..jiter.current_index()).map(|value| value.trim_matches(JSON_SPACE))
}

/// A reader of `doc` standing at its container, for the functions that take an array's or an object's items in order.
/// `known_array` and `known_object` read the bracket where the reader stands, so JSON's spaces in front of it (` [1,2]`)
/// are passed over first, by `peek`; the reader's offsets stay offsets into `doc`. None unless the document is what
/// `want` says (`Peek::Array` or `Peek::Object`).
pub(crate) fn opened(doc: &str, want: Peek) -> Option<Jiter<'_>> {
    let mut jiter = Jiter::new(doc.as_bytes());
    (jiter.peek().ok()? == want).then_some(jiter)
}

/// The number of elements of the array `doc`, read through once more (a negative index counts from the end, as
/// functions-json does). `Jiter` can't be copied, so the count reads its own reader.
fn array_len(doc: &str) -> Option<usize> {
    let mut jiter = opened(doc, Peek::Array)?;
    let mut peek = jiter.known_array().ok()?;
    let mut len = 0;
    while let Some(p) = peek {
        jiter.known_skip(p).ok()?;
        len += 1;
        peek = jiter.array_step().ok()?;
    }
    Some(len)
}

/// An array index as `#>` reads a path element: an optional sign, then digits (`+1`, `01`, `-0` and `-1` as Postgres
/// reads them). None when it isn't a whole number.
pub(crate) fn index_of(text: &str) -> Option<i64> {
    let (sign, digits) = match text.as_bytes().first()? {
        b'-' => (-1, &text[1..]),
        b'+' => (1, &text[1..]),
        _ => (1, text),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<i64>().ok().map(|n| sign * n)
}

/// `#>>`'s text for a value's text: a JSON string's content, unescaped by jiter; a JSON null is NULL; anything else
/// as written.
fn unquoted(raw: &str) -> Option<String> {
    if raw == "null" {
        return None;
    }
    if raw.starts_with('"') {
        let mut jiter = Jiter::new(raw.as_bytes());
        return jiter.known_str().ok().map(str::to_string);
    }
    Some(raw.to_string())
}

/// `@>` and `<@`: does the left document contain the right?
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

/// `<@`, which is `@>` with its sides swapped (`jsonb_contained(a, b)` is `jsonb_contains(b, a)`).
fn contained(a: &[ArrayRef], n: usize) -> anyhow::Result<ArrayRef> { contains_all(&[a[1].clone(), a[0].clone()], n) }

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
        (Value::Number(x), Value::Number(y)) => same_number(x, y),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

/// Integers are compared exactly, as Postgres compares them (`9007199254740993` is not `9007199254740992`); anything
/// else as a float.
fn same_number(x: &serde_json::Number, y: &serde_json::Number) -> bool {
    match (x.as_i64(), y.as_i64(), x.as_u64(), y.as_u64()) {
        (Some(a), Some(b), _, _) => a == b,
        (_, _, Some(a), Some(b)) => a == b,
        _ => x.as_f64() == y.as_f64(),
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

// ---- Postgres's grouping of the JSON operators, where SQL comes in (`path::door`) ----
//
// sqlparser's generic dialect ranks `->` and its kin below `=`, `IS`, `LIKE`, `IN`, `BETWEEN` and `||`. Postgres ranks
// them above all of those (its `Op`, 70, with `||`, `|`, `&`, `#`, `<<` and `>>`), and below `+`. So `doc ->> 'a' = 'x'`
// reads as `doc ->> ('a' = 'x')` and fails. Here each JSON operator's operands are regrouped the way Postgres groups
// them, and the operator is nested, so the text reads the same under the generic dialect in every later stage.

/// Postgres's rank of `Op`: the JSON operators and `||` share it, and it is left-associative.
const OP: u8 = 70;

/// The text with its JSON operators grouped as Postgres groups them: `doc ->> 'a' = 'x'` reads as
/// `(doc ->> 'a') = 'x'`, and `doc ? 'a'` as `jsonb_exists(doc, 'a')`. Text that needs neither (and any text neither
/// dialect reads) comes back borrowed, as it was.
pub fn text(sql: &str) -> Cow<'_, str> {
    const OPERATORS: [&str; 6] = ["->", "#>", "@>", "<@", "?", "#-"];
    if !OPERATORS.iter().any(|op| sql.contains(op)) {
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

/// One expression (a CHECK, an expectation, a SQL function's body) with its JSON operators grouped as `text` groups a
/// statement's: read as the one value of a SELECT, in parentheses, so a bare `?` is no select item to be named.
pub fn expression(e: &str) -> Cow<'_, str> {
    let e = e.trim();
    match text(&format!("SELECT ({e})")) {
        Cow::Owned(s) => s.strip_prefix("SELECT (").and_then(|s| s.strip_suffix(')')).map_or(Cow::Borrowed(e), |s| Cow::Owned(s.to_string())),
        Cow::Borrowed(_) => Cow::Borrowed(e),
    }
}

/// The statements with each JSON operator regrouped where Postgres groups it differently. None when none was.
fn regrouped(mut stmts: Vec<ast::Statement>) -> Option<String> {
    let mut moved = false;
    let _ = ast::visit_expressions_mut(&mut stmts, |e| {
        if is_json_op(e) {
            moved |= regroup_at(e);
        }
        ControlFlow::<()>::Continue(())
    });
    moved.then(|| written(&stmts))
}

/// Text with a `?`, `?|` or `?&`, read as Postgres's dialect reads it (so its JSON operators are grouped right).
/// Each `?` becomes the call Postgres names it for, a top-level `?` in a SELECT list or a RETURNING is named `?column?`, and every
/// other JSON operator is nested, so the generic dialect reads the text the same way.
fn questions(sql: &str) -> Option<String> {
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let _ = stmts.visit(&mut Questions);
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
    let _ = ast::visit_expressions(right, |e| {
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
        X::IsNull(_) | X::IsNotNull(_) | X::IsTrue(_) | X::IsFalse(_) | X::IsNotTrue(_) | X::IsNotFalse(_) | X::IsUnknown(_) | X::IsNotUnknown(_) | X::IsDistinctFrom(..) | X::IsNotDistinctFrom(..) => 40,
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
    use crate::codes::Coded;
    use datafusion::common::DataFusionError;

    fn elements(v: &[&str]) -> Vec<Option<String>> { v.iter().map(|s| Some(s.to_string())).collect() }

    /// The steps of a path literal (`{b,0}`), as `#>` takes them.
    fn steps_of(path: &str) -> Vec<Option<String>> { array_literal(path).unwrap() }

    /// `#>` (or `#>>`) of a document at a path literal, as the functions answer it.
    fn at(doc: &str, path: &str, text: bool) -> Option<String> {
        let elements = steps_of(path);
        let steps: Option<Vec<Step>> = elements.iter().map(|e| e.as_deref().map(Step::Path)).collect();
        let raw = find(doc, &steps?)?;
        if text { unquoted(raw) } else { Some(raw.to_string()) }
    }

    /// `->` with one step of the given kind.
    fn arrow(doc: &str, step: Step) -> Option<String> { find(doc, &[step]).map(str::to_string) }

    /// The statement `sql` reads as after `text`.
    fn grouped(sql: &str) -> String { text(sql).into_owned() }

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
    fn arrow_takes_a_duplicate_keys_last_value() {
        assert_eq!(arrow(r#"{"a":1,"b":2,"a":3}"#, Step::Key("a")).as_deref(), Some("3"));
        assert_eq!(arrow(r#"{"a": [1, {"x": 2}], "a": {"y": 3}}"#, Step::Key("a")).as_deref(), Some(r#"{"y": 3}"#));
        assert_eq!(arrow(r#"{ "a" : 1 }"#, Step::Key("a")).as_deref(), Some("1"), "a value after spaces starts at its first character");
    }

    #[test]
    fn arrow_text_key_on_an_array_is_null() {
        assert_eq!(arrow("[1,2]", Step::Key("0")), None, "a text key never reaches an array (Postgres: NULL even for '0')");
        assert_eq!(arrow("[1,2]", Step::Path("0")).as_deref(), Some("1"), "a path element does reach one");
        assert_eq!(arrow(r#"{"0":"zero"}"#, Step::Key("0")).as_deref(), Some(r#""zero""#), "a text key names an object's member");
    }

    #[test]
    fn arrow_integer_on_an_object_is_null() {
        assert_eq!(arrow(r#"{"1":"one"}"#, Step::Index(1)), None, "an integer never names an object's member");
        assert_eq!(arrow("[10,20,30]", Step::Index(1)).as_deref(), Some("20"));
        assert_eq!(arrow("[10,20,30]", Step::Index(3)), None, "past the end");
    }

    #[test]
    fn arrow_negative_index_counts_from_the_end() {
        let doc = r#"{"b":[1,{"k":"v"},[3]]}"#;
        assert_eq!(find(doc, &[Step::Key("b"), Step::Index(-1)]), Some("[3]"));
        assert_eq!(find(doc, &[Step::Key("b"), Step::Index(-2)]), Some(r#"{"k":"v"}"#));
        assert_eq!(find(doc, &[Step::Key("b"), Step::Index(-3)]), Some("1"));
        assert_eq!(find(doc, &[Step::Key("b"), Step::Index(-4)]), None);
        assert_eq!(find("[]", &[Step::Index(-1)]), None, "an empty array has no last element");
    }

    #[test]
    fn text_that_is_not_json_is_null() {
        assert_eq!(arrow("not json", Step::Key("a")), None);
        assert_eq!(arrow("{", Step::Key("a")), None);
        assert_eq!(arrow("[1,", Step::Index(0)).as_deref(), Some("1"), "a value read before the text goes wrong still answers");
        assert_eq!(at("not json", "{a}", false), None);
        assert_eq!(at("not json", "{a}", true), None);
        assert_eq!(find("not json", &[]), None, "the empty path refuses a document that isn't JSON");
    }

    #[test]
    fn containment_compares_integers_exactly() {
        let c = |have: &str, want: &str| contains(&document(have).unwrap(), &document(want).unwrap(), true);
        assert!(!c(r#"{"x": 9007199254740993}"#, r#"{"x": 9007199254740992}"#), "exact integers differ past 2^53");
        assert!(c(r#"{"x": 9007199254740993}"#, r#"{"x": 9007199254740993}"#));
        assert!(c("1", "1.0"), "an integer and a float with the same value");
        assert!(!c("-1", "18446744073709551615"), "a negative and a big positive integer");
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
    fn index_of_takes_signs_and_digits() {
        assert_eq!(index_of("1"), Some(1));
        assert_eq!(index_of("-1"), Some(-1));
        assert_eq!(index_of("+1"), Some(1));
        assert_eq!(index_of("01"), Some(1));
        assert_eq!(index_of("-0"), Some(0));
        assert_eq!(index_of(""), None);
        assert_eq!(index_of("-"), None);
        assert_eq!(index_of("1e2"), None);
        assert_eq!(index_of("1.0"), None);
        assert_eq!(index_of("99999999999999999999"), None, "too big for an index");
    }

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

    /// The SQLSTATE of a refusal a planner raised (`refuse`), if it raised one.
    fn refused_as(r: Result<bool>) -> Option<&'static str> {
        match r {
            Err(DataFusionError::External(e)) => e.downcast_ref::<Coded>().map(|c| c.0),
            _ => None,
        }
    }

    #[test]
    fn deletes_are_planned_where_postgres_has_the_operator() {
        use datafusion::arrow::datatypes::Schema;
        use datafusion::logical_expr::{col, lit};
        use std::collections::HashMap;
        let marked = |m: &str| HashMap::from([(MARK.to_string(), m.to_string())]);
        let schema = DFSchema::try_from(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true).with_metadata(marked("jsonb")),
            Field::new("js", DataType::Utf8, true).with_metadata(marked("json")),
            Field::new("name", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
            Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
        ]))
        .unwrap();
        let plan = |op: BinaryOperator, left: Expr, right: Expr| deletes(&RawBinaryExpr { op, left, right }, &schema);
        let (minus, hash) = (|| BinaryOperator::Minus, || BinaryOperator::HashMinus);
        let null = || Expr::Literal(ScalarValue::Null, None);
        // `-` and `#-` on a document: the operators Postgres has (a key, an index, a list of keys, a path)
        assert!(plan(minus(), col("doc"), lit("a")).unwrap(), "doc - 'a'");
        assert!(plan(minus(), col("doc"), lit(1i64)).unwrap(), "doc - 1");
        assert!(plan(minus(), col("doc"), col("tags")).unwrap(), "doc - a text[] column");
        assert!(plan(minus(), col("doc"), null()).unwrap(), "doc - NULL");
        assert!(plan(hash(), col("doc"), lit("{a}")).unwrap(), "doc #- a path literal");
        // Not these: a bare literal's `-`, a number, and text with no document in it
        assert!(!plan(minus(), lit("[]"), lit("a")).unwrap(), "a bare literal's minus stays DataFusion's");
        assert!(!plan(minus(), col("n"), lit(1i64)).unwrap(), "an integer column's `-` is arithmetic");
        assert!(!plan(minus(), col("name"), lit(1.5f64)).unwrap(), "text - a decimal is not a document's");
        // Refused by name, as Postgres refuses them: json has no `-`, jsonb has no `-` for a decimal or a jsonb right side
        assert_eq!(refused_as(plan(minus(), col("js"), lit("a"))), Some("42883"), "json - text");
        assert_eq!(refused_as(plan(minus(), col("doc"), lit(1.5f64))), Some("42883"), "jsonb - numeric");
        assert_eq!(refused_as(plan(minus(), col("doc"), col("doc"))), Some("42883"), "jsonb - jsonb");
        assert_eq!(refused_as(plan(hash(), col("doc"), lit(1i64))), Some("42883"), "jsonb #- integer");
    }

    /// The SQLSTATE of an error from a reading function (`codes::of`).
    fn code(e: &anyhow::Error) -> &'static str { crate::codes::of(e) }

    /// `json(b)_extract_path(_text)` over one document and its keys, as the SQL function gets them: one column per
    /// argument, one row.
    fn extract_of(name: &str, doc: Option<&str>, keys: &[Option<&str>], text: bool, jsonb: bool) -> anyhow::Result<Option<String>> {
        let mut args: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![doc]))];
        args.extend(keys.iter().map(|k| Arc::new(StringArray::from(vec![*k])) as ArrayRef));
        let out = extract(name, &args, 1, text, jsonb)?;
        Ok(out.as_string::<i32>().iter().next().flatten().map(str::to_string))
    }

    #[test]
    fn json_kinds_are_postgres_type_names() {
        assert_eq!(kind("{}").unwrap(), "object");
        assert_eq!(kind("[]").unwrap(), "array");
        assert_eq!(kind(r#""s""#).unwrap(), "string");
        assert_eq!(kind("1.5").unwrap(), "number");
        assert_eq!(kind("-2e3").unwrap(), "number");
        assert_eq!(kind("true").unwrap(), "boolean");
        assert_eq!(kind("false").unwrap(), "boolean");
        assert_eq!(kind("null").unwrap(), "null");
        assert_eq!(kind(" [1] ").unwrap(), "array", "spaces around the one value are fine (Postgres: array)");
        for bad in ["", "[1,", "1 2", "not json"] {
            assert_eq!(code(&kind(bad).unwrap_err()), "22P02", "{bad:?} is not one JSON value");
        }
    }

    #[test]
    fn array_length_counts_elements_and_refuses_the_rest_as_postgres_does() {
        assert_eq!(length_of("[]").unwrap(), 0);
        assert_eq!(length_of("[1,[2,3]]").unwrap(), 2, "a nested array is one element");
        assert_eq!(length_of(" [1, 2] ").unwrap(), 2);
        let object = length_of(r#"{"a":1}"#).unwrap_err();
        assert_eq!(code(&object), "22023");
        assert_eq!(object.to_string(), "cannot get array length of a non-array");
        let scalar = length_of("null").unwrap_err();
        assert_eq!(code(&scalar), "22023");
        assert_eq!(scalar.to_string(), "cannot get array length of a scalar", "a JSON null is a scalar");
        assert_eq!(code(&length_of("[").unwrap_err()), "22P02", "text that isn't JSON is refused as JSON text");
    }

    /// JSON's four spaces in front of a document, or after it, change no answer: the count, the kind and the walks
    /// are those of the document without them (`opened` passes over the spaces before the bracket).
    #[test]
    fn spaces_around_a_document_change_no_answer() {
        for doc in [" [1, 2]", "\t[1, 2]", "\n[1, 2]", "\r\n \t[1, 2]", "[1, 2] ", " [1, 2]\n\t\r "] {
            assert_eq!(length_of(doc).unwrap(), 2, "{doc:?}");
            assert_eq!(array_len(doc), Some(2), "{doc:?}");
            assert_eq!(kind(doc).unwrap(), "array", "{doc:?}");
        }
        assert_eq!(length_of(" []").unwrap(), 0);
        assert_eq!(length_of(" [[1], {\"a\": [2]}]").unwrap(), 2);
        assert_eq!(array_len(" {}"), None, "an object is no array");
        assert_eq!(kind(" \t{\"a\":1}\n").unwrap(), "object");
    }

    #[test]
    fn opened_stands_at_the_container_past_spaces_or_says_none() {
        assert!(opened(" [1]", Peek::Array).is_some());
        assert!(opened("\n\t{}", Peek::Object).is_some());
        assert!(opened(" {}", Peek::Array).is_none(), "an object is not the array asked for");
        assert!(opened("1", Peek::Array).is_none());
        assert!(opened("  ", Peek::Object).is_none(), "spaces are no document");
    }

    /// `->` and `#>` (and their `_text` forms, `->>` and `#>>`) walk a document with JSON's spaces around it, and answer
    /// each value as written, as they do without the spaces.
    #[test]
    fn the_walks_pass_over_spaces_around_the_document() {
        let bare = r#"{"a": {"b": [1, "x"]}}"#;
        for doc in [" {\"a\": {\"b\": [1, \"x\"]}}", "\t{\"a\": {\"b\": [1, \"x\"]}}\n", "\r\n{\"a\": {\"b\": [1, \"x\"]}} "] {
            assert_eq!(find(doc, &[Step::Key("a")]), find(bare, &[Step::Key("a")]), "{doc:?}");
            assert_eq!(find(doc, &[Step::Key("a"), Step::Key("b"), Step::Index(1)]), Some("\"x\""), "{doc:?}");
            assert_eq!(find(doc, &[Step::Key("a"), Step::Key("b"), Step::Index(-2)]), Some("1"), "{doc:?}");
            assert_eq!(find(doc, &[Step::Key("a"), Step::Path("b"), Step::Path("0")]), Some("1"), "{doc:?}");
        }
        assert_eq!(find(" [1, [2, 3]]", &[Step::Index(1), Step::Index(0)]), Some("2"));
        assert_eq!(find(" [1, [2, 3]]", &[Step::Index(7)]), None, "past the end is NULL");
        assert_eq!(extract_of("json_extract_path", Some("\t[1, [2, 3]]"), &[Some("1"), Some("0")], false, false).unwrap().as_deref(), Some("2"));
        assert_eq!(extract_of("json_extract_path_text", Some(" {\"a\": \"x\"}\n"), &[Some("a")], true, false).unwrap().as_deref(), Some("x"));
        assert_eq!(extract_of("json_extract_path", Some(" {\"a\": \"x\"}\n"), &[Some("a")], false, false).unwrap().as_deref(), Some("\"x\""));
        assert_eq!(extract_of("jsonb_extract_path", Some("\n{\"b\":1,\"a\":{\"y\":2,\"x\":1}}"), &[Some("a")], false, true).unwrap().as_deref(), Some(r#"{"x": 1, "y": 2}"#));
    }

    #[test]
    fn typeof_and_array_length_answer_null_for_a_null_document() {
        let docs: ArrayRef = Arc::new(StringArray::from(vec![Some("[1]"), None]));
        let kinds = type_of(&[docs.clone()], 2).unwrap();
        assert_eq!(kinds.as_string::<i32>().iter().collect::<Vec<_>>(), vec![Some("array"), None]);
        let lengths = array_length(&[docs], 2).unwrap();
        let lengths = lengths.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(lengths.iter().collect::<Vec<_>>(), vec![Some(1), None]);
    }

    #[test]
    fn extract_path_walks_keys_and_indexes_as_postgres_does() {
        let doc = r#"{"a":{"b":[1,2]}}"#;
        assert_eq!(extract_of("json_extract_path", Some(doc), &[Some("a"), Some("b")], false, false).unwrap().as_deref(), Some("[1,2]"));
        assert_eq!(extract_of("jsonb_extract_path", Some(doc), &[Some("a"), Some("b"), Some("1")], false, true).unwrap().as_deref(), Some("2"));
        assert_eq!(extract_of("jsonb_extract_path", Some(r#"{"a":[1,2]}"#), &[Some("a"), Some("-1")], false, true).unwrap().as_deref(), Some("2"), "a negative index counts from the end");
        assert_eq!(extract_of("json_extract_path", Some(r#"{"a":1}"#), &[Some("z")], false, false).unwrap(), None, "a missing path is NULL");
        assert_eq!(extract_of("json_extract_path", Some(r#"{"a":1}"#), &[Some("a"), Some("b")], false, false).unwrap(), None, "a path into a scalar is NULL");
        assert_eq!(extract_of("json_extract_path", Some("[\"a\"]"), &[Some("0")], false, false).unwrap().as_deref(), Some(r#""a""#));
        assert_eq!(extract_of("json_extract_path", Some(doc), &[Some("a"), None], false, false).unwrap(), None, "a NULL key makes the answer NULL");
        assert_eq!(extract_of("json_extract_path", None, &[Some("a")], false, false).unwrap(), None, "a NULL document is NULL");
    }

    #[test]
    fn extract_path_answers_json_as_written_and_jsonb_as_jsonb_prints_it() {
        let loose = r#"{"a": 1.0 , "b" :  {"x" : 1 }}"#;
        assert_eq!(extract_of("json_extract_path", Some(loose), &[Some("b")], false, false).unwrap().as_deref(), Some(r#"{"x" : 1 }"#));
        let docb = r#"{"b":1,"a":{"y":2,"x":1}}"#;
        assert_eq!(extract_of("jsonb_extract_path", Some(docb), &[Some("a")], false, true).unwrap().as_deref(), Some(r#"{"x": 1, "y": 2}"#));
        assert_eq!(extract_of("jsonb_extract_path", Some(r#"{"a":"é"}"#), &[Some("a")], false, true).unwrap().as_deref(), Some("\"é\""));
        assert_eq!(extract_of("json_extract_path", Some(r#"{"a":"é"}"#), &[Some("a")], false, false).unwrap().as_deref(), Some(r#""é""#));
        assert_eq!(extract_of("jsonb_extract_path", Some(r#"{"a":1,"a":2}"#), &[Some("a")], false, true).unwrap().as_deref(), Some("2"), "a duplicate key: the last wins");
    }

    #[test]
    fn extract_path_text_unquotes_strings_and_keeps_numbers_as_written() {
        assert_eq!(extract_of("json_extract_path_text", Some(r#"{"a":"x\"y"}"#), &[Some("a")], true, false).unwrap().as_deref(), Some("x\"y"));
        assert_eq!(extract_of("json_extract_path_text", Some(r#"{"a":null}"#), &[Some("a")], true, false).unwrap(), None, "a JSON null is NULL");
        assert_eq!(extract_of("json_extract_path_text", Some(r#"{"a":1.0}"#), &[Some("a")], true, false).unwrap().as_deref(), Some("1.0"));
        assert_eq!(extract_of("jsonb_extract_path_text", Some(r#"{"a":1.50}"#), &[Some("a")], true, true).unwrap().as_deref(), Some("1.50"));
        assert_eq!(extract_of("jsonb_extract_path_text", Some(r#"{"a":""}"#), &[Some("a")], true, true).unwrap().as_deref(), Some(""), "an empty string is not NULL");
        assert_eq!(extract_of("jsonb_extract_path_text", Some(r#"{"a":{"b":true}}"#), &[Some("a"), Some("b")], true, true).unwrap().as_deref(), Some("true"));
        assert_eq!(extract_of("jsonb_extract_path_text", Some(r#"{"a":{"b":[1]}}"#), &[Some("a")], true, true).unwrap().as_deref(), Some(r#"{"b": [1]}"#));
    }

    #[test]
    fn extract_path_with_no_keys_is_refused_as_postgres_refuses_it() {
        let e = extract_of("json_extract_path", Some("{}"), &[], false, false).unwrap_err();
        assert_eq!(code(&e), "42883");
        assert_eq!(e.to_string(), "function json_extract_path(unknown) does not exist");
        let t = extract_of("jsonb_extract_path_text", Some("{}"), &[], true, true).unwrap_err();
        assert_eq!(code(&t), "42883");
        assert_eq!(t.to_string(), "function jsonb_extract_path_text(unknown) does not exist");
    }

    #[test]
    fn text_of_a_value_is_unquoted_or_as_written_or_as_jsonb_prints_it() {
        assert_eq!(text_of("null", false).unwrap(), None);
        assert_eq!(text_of(r#""x\"y""#, false).unwrap(), Some("x\"y".to_string()));
        assert_eq!(text_of("1.0", false).unwrap(), Some("1.0".to_string()));
        assert_eq!(text_of(r#"{"b":1,"a":2}"#, true).unwrap(), Some(r#"{"a": 2, "b": 1}"#.to_string()));
        assert_eq!(text_of(r#"{"b":1,"a":2}"#, false).unwrap(), Some(r#"{"b":1,"a":2}"#.to_string()));
    }
}
