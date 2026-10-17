//! Postgres's functions that make JSON from values: `to_json`, `to_jsonb`, `row_to_json`, `json_build_object`,
//! `jsonb_build_object`, `json_build_array`, `jsonb_build_array`, and the scalar forms of the aggregates
//! (`pondra_json_agg(list, kind)` is what a later step makes `json_agg(x)` into: `array_agg(x)` and the kind of its
//! elements). Each answer is text marked as JSON (`pg_types::MARK`: "json", or "jsonb" for the `jsonb_*` ones),
//! so the port sends it as json, and a function given another's answer writes it as it is. Text without the mark is a
//! string and is quoted. The text is Postgres 16's, down to the spaces and the digits of a float (`json.c`, `jsonb.c`).
//! `jsonb_text` is jsonb's own output: keys sorted, numbers as `numeric` prints them, and every `jsonb_*` answer goes
//! through it. `pondra_as_jsonb` and `pondra_jsonb` (types.rs) answer the same way.
use crate::codes::{coded, Coded};
use crate::pg_types::MARK;
use crate::types::json_refusal;
use crate::pg_values::{interval_text, time_text};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Field, FieldRef, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{Cast, TryCast};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{ColumnarValue, Expr, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use jiter::{Jiter, Peek};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Write as _;
use std::sync::Arc;

/// Adds the functions to a session. `pg_json::register` calls it, so they are in every session.
pub fn register(ctx: &SessionContext) {
    let one = || Signature::any(1, Volatility::Immutable);
    let two = || Signature::any(2, Volatility::Immutable);
    let any = || Signature::one_of(vec![TypeSignature::Nullary, TypeSignature::VariadicAny], Volatility::Immutable);
    let functions = vec![
        make("to_json", one(), "json", |a, n| per_row(n, |i, out| json_of_row(&a[0], i, out))),
        make("row_to_json", one(), "json", |a, n| per_row(n, |i, out| json_of_row(&a[0], i, out))),
        make("to_jsonb", one(), "jsonb", |a, n| as_jsonb(per_row(n, |i, out| json_of_row(&a[0], i, out))?)),
        make("json_build_object", any(), "json", |a, n| pairs(a, n, " : ", ", ")),
        make("jsonb_build_object", any(), "jsonb", |a, n| as_jsonb(pairs(a, n, " : ", ", ")?)),
        make("json_build_array", any(), "json", array_of),
        make("jsonb_build_array", any(), "jsonb", |a, n| as_jsonb(array_of(a, n)?)),
        make("pondra_row", any(), "json", |a, n| pairs(a, n, ":", ",")),
        make("pondra_json_agg", two(), "json", json_agg),
        make("pondra_jsonb_agg", two(), "jsonb", |a, n| as_jsonb(json_agg(a, n)?)),
        make("pondra_json_object_agg", two(), "json", object_agg),
        make("pondra_jsonb_object_agg", two(), "jsonb", |a, n| as_jsonb(object_agg(a, n)?)),
    ];
    for f in functions {
        ctx.register_udf(f);
    }
    for (name, mark) in [("pondra_as_json", "json"), ("pondra_as_jsonb", "jsonb")] {
        ctx.register_udf(ScalarUDF::new_from_impl(AsJson { name, mark, signature: Signature::string(1, Volatility::Immutable) }));
    }
}

/// One argument of a function: its values, and whether they are JSON text (their field carries the mark).
struct Arg {
    values: ArrayRef,
    json: bool,
}

type Body = dyn Fn(&[Arg], usize) -> Result<ArrayRef> + Send + Sync;

/// A function of this file's: any arguments, text out, its field marked `mark` ("json" or "jsonb").
struct Make {
    name: &'static str,
    signature: Signature,
    mark: &'static str,
    body: Arc<Body>,
}

fn make(name: &'static str, signature: Signature, mark: &'static str, body: impl Fn(&[Arg], usize) -> Result<ArrayRef> + Send + Sync + 'static) -> ScalarUDF {
    ScalarUDF::new_from_impl(Make { name, signature, mark, body: Arc::new(body) })
}

impl std::fmt::Debug for Make {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Make({})", self.name) }
}

// (one of each name: its name says which, as `pg_catalog`'s functions do)
impl PartialEq for Make {
    fn eq(&self, o: &Self) -> bool { self.name == o.name }
}

impl Eq for Make {}

impl std::hash::Hash for Make {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.name.hash(h) }
}

impl ScalarUDFImpl for Make {
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Utf8) }
    fn return_field_from_args(&self, _: ReturnFieldArgs) -> Result<FieldRef> { Ok(Arc::new(field_marked(self.name, DataType::Utf8, self.mark))) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let items = args
            .args
            .iter()
            .zip(&args.arg_fields)
            .map(|(v, f)| -> Result<Arg> { Ok(Arg { values: v.to_array(n)?, json: is_json_field(f) }) })
            .collect::<Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array((self.body)(&items, n)?))
    }
}

/// `pondra_as_json(x)` and `pondra_as_jsonb(x)`: a JSON column's values, marked as JSON, so the functions here write
/// them as they are. `pondra_as_json` answers `x` itself (the same array, no copy). `pondra_as_jsonb` answers each
/// value as jsonb prints it (keys sorted, a refusal for text that isn't JSON), as `jsonb_*` answers do.
#[derive(Debug)]
struct AsJson {
    name: &'static str,
    mark: &'static str,
    signature: Signature,
}

impl PartialEq for AsJson {
    fn eq(&self, o: &Self) -> bool { self.name == o.name }
}

impl Eq for AsJson {}

impl std::hash::Hash for AsJson {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.name.hash(h) }
}

impl ScalarUDFImpl for AsJson {
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> { Ok(if self.mark == "jsonb" { DataType::Utf8 } else { args[0].clone() }) }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let t = self.return_type(&[args.arg_fields[0].data_type().clone()])?;
        Ok(Arc::new(field_marked(self.name, t, self.mark)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.mark == "json" {
            return Ok(args.args[0].clone());
        }
        Ok(ColumnarValue::Array(as_jsonb(args.args[0].to_array(args.number_rows)?)?))
    }
}

/// Takes the JSON mark off a cast to text, as an analyzer rule after type coercion (`optimize.rs`). `(doc -> 'a')::text`
/// is text, as Postgres says, so the port sends it as text and a function quotes it. DataFusion gives a cast its
/// source's metadata unless the cast's target field has a name or metadata of its own, so each text cast gets a named
/// target with none. The values are the same; only the mark goes.
#[derive(Debug)]
pub struct AsText;

impl AnalyzerRule for AsText {
    fn name(&self) -> &str {
        "pondra_json_text_casts"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        // (most plans cast nothing to text: they are handed back as they came, never rebuilt)
        let mut any = false;
        plan.apply_with_subqueries(|p| {
            p.apply_expressions(|e| {
                any = e.exists(|e| Ok(text_cast(e)))?;
                Ok(if any { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
            })?;
            Ok(if any { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
        })?;
        if !any {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(|plan| {
            // (most plans cast nothing to text, and are left as they are)
            let mut any = false;
            plan.apply_expressions(|e| {
                any = e.exists(|e| Ok(text_cast(e)))?;
                Ok(if any { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
            })?;
            if !any {
                return Ok(Transformed::no(plan));
            }
            let names = NamePreserver::new(&plan);
            plan.map_expressions(|e| {
                let name = names.save(&e);
                let e = e.transform_up(|e| Ok(unmarked(e)))?;
                Ok(e.update_data(|e| name.restore(e)))
            })
        })
        .data()
    }
}

/// Whether `e` casts to text (a CAST or a TRY_CAST).
fn text_cast(e: &Expr) -> bool {
    match e {
        Expr::Cast(c) => is_text(c.field.data_type()),
        Expr::TryCast(c) => is_text(c.field.data_type()),
        _ => false,
    }
}

fn is_text(t: &DataType) -> bool { matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) }

/// `e`, with a text cast's target named (so it takes no metadata from its source).
fn unmarked(e: Expr) -> Transformed<Expr> {
    match e {
        Expr::Cast(c) if is_text(c.field.data_type()) => {
            let field = Arc::new(Field::new("text", c.field.data_type().clone(), true));
            Transformed::yes(Expr::Cast(Cast::new_from_field(c.expr, field)))
        }
        Expr::TryCast(c) if is_text(c.field.data_type()) => {
            let field = Arc::new(Field::new("text", c.field.data_type().clone(), true));
            Transformed::yes(Expr::TryCast(TryCast::new_from_field(c.expr, field)))
        }
        e => Transformed::no(e),
    }
}

fn field_marked(name: &str, t: DataType, mark: &str) -> Field {
    Field::new(name, t, true).with_metadata([(MARK.to_string(), mark.to_string())].into())
}

/// Whether a field is JSON text, by its mark.
fn is_json_field(f: &Field) -> bool { matches!(f.metadata().get(MARK).map(String::as_str), Some("json" | "jsonb")) }

/// An error a function raises, carrying its SQLSTATE to the door (`codes::of`). anyhow's own box hides the `Coded` type
/// `codes::of` looks for, so a `Coded` is boxed as itself; any other error goes as anyhow boxes it.
pub(crate) fn fail(e: anyhow::Error) -> DataFusionError {
    match e.downcast::<Coded>() {
        Ok(c) => DataFusionError::External(Box::new(c)),
        Err(e) => DataFusionError::External(e.into()),
    }
}

/// A refusal with its SQLSTATE, as a function raises it (`fail` of a `Coded`).
pub(crate) fn refuse(code: &'static str, message: impl Into<String>) -> DataFusionError { fail(coded(code, message)) }

fn null_key() -> DataFusionError { fail(coded("22004", "null value not allowed for object key")) }

/// One text per row, for a whole batch: `row(i, out)` writes row `i` into `out` (cleared before each row, so one String
/// serves the batch) and says false where the row is NULL. Each text is appended to one `StringBuilder`, so no row
/// makes a String of its own.
pub(crate) fn per_row(n: usize, mut row: impl FnMut(usize, &mut String) -> Result<bool>) -> Result<ArrayRef> {
    let mut out = String::new();
    let mut answers = StringBuilder::new();
    for i in 0..n {
        out.clear();
        if row(i, &mut out)? {
            answers.append_value(&out);
        } else {
            answers.append_null();
        }
    }
    Ok(Arc::new(answers.finish()))
}

/// Row `i` of an argument as JSON text, written into `out`: false where it is NULL, which the strict functions answer as
/// NULL.
fn json_of_row(x: &Arg, i: usize, out: &mut String) -> Result<bool> {
    if is_null_at(&x.values, i) {
        return Ok(false);
    }
    value(out, &x.values, i, x.json)?;
    Ok(true)
}

/// Whether row `i` of `a` is NULL (a NULL-typed array is all NULL, though it has no validity bitmap).
pub(crate) fn is_null_at(a: &ArrayRef, i: usize) -> bool { matches!(a.data_type(), DataType::Null) || a.is_null(i) }

/// Whether a type is one Postgres writes as a composite or an array: never a key, and a line feed in `json_agg`.
fn is_composite(t: &DataType) -> bool { matches!(t, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) | DataType::Struct(_) | DataType::Map(..)) }

/// Row `i` of a text column (`a` is Utf8, LargeUtf8 or Utf8View; the callers check).
pub(crate) fn text_at(a: &ArrayRef, i: usize) -> &str {
    match a.data_type() {
        DataType::LargeUtf8 => a.as_string::<i64>().value(i),
        DataType::Utf8View => a.as_string_view().value(i),
        _ => a.as_string::<i32>().value(i),
    }
}

/// `a` as text: as it is when it holds text, else cast to Utf8.
pub(crate) fn as_text(a: &ArrayRef) -> Result<ArrayRef> {
    Ok(match a.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => a.clone(),
        _ => cast(a, &DataType::Utf8)?,
    })
}

/// Writes row `i` of `a` as JSON text, as Postgres 16's `to_json` writes a value (`json.c`'s `datum_to_json`). `json`
/// says a text is JSON already, and goes in as it is. NULL is `null` here (the strict functions answer NULL before
/// they get here). Only a key that can't be one is an error (a map's composite key); a type Arrow can't print is
/// written as its type's name.
fn value(out: &mut String, a: &ArrayRef, i: usize, json: bool) -> Result<()> {
    if is_null_at(a, i) {
        out.push_str("null");
        return Ok(());
    }
    match a.data_type() {
        DataType::Boolean => out.push_str(if a.as_boolean().value(i) { "true" } else { "false" }),
        DataType::Int8 => digits::<Int8Type>(out, a, i),
        DataType::Int16 => digits::<Int16Type>(out, a, i),
        DataType::Int32 => digits::<Int32Type>(out, a, i),
        DataType::Int64 => digits::<Int64Type>(out, a, i),
        DataType::UInt8 => digits::<UInt8Type>(out, a, i),
        DataType::UInt16 => digits::<UInt16Type>(out, a, i),
        DataType::UInt32 => digits::<UInt32Type>(out, a, i),
        DataType::UInt64 => digits::<UInt64Type>(out, a, i),
        DataType::Float32 => float_json(out, a.as_primitive::<Float32Type>().value(i)),
        DataType::Float64 => float_json(out, a.as_primitive::<Float64Type>().value(i)),
        // (the same text Arrow's display gives, from the same formatting, without a formatter per value)
        DataType::Decimal128(..) => out.push_str(&a.as_primitive::<Decimal128Type>().value_as_string(i)),
        DataType::Decimal256(..) => out.push_str(&a.as_primitive::<Decimal256Type>().value_as_string(i)),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let s = text_at(a, i);
            if json {
                out.push_str(s);
            } else {
                json_string(out, s);
            }
        }
        DataType::Date32 => date(out, a.as_primitive::<Date32Type>().value_as_date(i)),
        DataType::Date64 => date(out, a.as_primitive::<Date64Type>().value_as_date(i)),
        DataType::Timestamp(unit, zone) => stamp(out, a, i, unit, zone.is_some()),
        DataType::Time32(_) | DataType::Time64(_) => clock(out, a, i),
        DataType::Interval(_) => interval(out, a, i),
        DataType::Binary => bytea(out, a.as_binary::<i32>().value(i)),
        DataType::LargeBinary => bytea(out, a.as_binary::<i64>().value(i)),
        DataType::BinaryView => bytea(out, a.as_binary_view().value(i)),
        DataType::FixedSizeBinary(_) => bytea(out, a.as_fixed_size_binary().value(i)),
        DataType::List(item) => list(out, &a.as_list::<i32>().value(i), item)?,
        DataType::LargeList(item) => list(out, &a.as_list::<i64>().value(i), item)?,
        DataType::FixedSizeList(item, _) => list(out, &a.as_fixed_size_list().value(i), item)?,
        DataType::Struct(_) => struct_row(out, a.as_struct(), i)?,
        DataType::Map(..) => map_row(out, a.as_map(), i)?,
        _ => json_string(out, &display(a, i)),
    }
    Ok(())
}

/// An integer's digits, written into `out` (a String takes them without failing).
fn digits<T: ArrowPrimitiveType>(out: &mut String, a: &ArrayRef, i: usize)
where
    T::Native: std::fmt::Display,
{
    let _ = write!(out, "{}", a.as_primitive::<T>().value(i));
}

/// Arrow's text of row `i`, which is what a value of a type this file doesn't write comes to.
fn display(a: &ArrayRef, i: usize) -> String { array_value_to_string(a.as_ref(), i).unwrap_or_else(|_| a.data_type().to_string()) }

/// A float as JSON writes it: its shortest digits in Postgres's notation (`float_text`); NaN and the infinities as
/// strings. Generic over f32 and f64, since a float4's shortest digits are its own.
fn float_json<T: std::fmt::LowerExp + Copy + Into<f64>>(out: &mut String, v: T) {
    let x: f64 = v.into();
    if x.is_nan() {
        out.push_str("\"NaN\"");
    } else if x.is_infinite() {
        out.push_str(if x > 0.0 { "\"Infinity\"" } else { "\"-Infinity\"" });
    } else {
        out.push_str(&float_text(&format!("{v:e}")));
    }
}

/// A float as Postgres 16 prints a float8 with its shortest digits, from Rust's scientific form of it (`1.5e-7`): plain
/// notation when the decimal exponent is from -4 to 14, else `1.5e-07` (a sign, two digits at least).
fn float_text(sci: &str) -> String {
    let (mantissa, exp) = sci.split_once('e').unwrap_or((sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let (sign, digits) = match mantissa.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", mantissa),
    };
    let digits: String = digits.chars().filter(|c| *c != '.').collect();
    if (-4..=14).contains(&exp) {
        return format!("{sign}{}", plain(&digits, exp));
    }
    let (first, rest) = digits.split_at(1);
    let rest = if rest.is_empty() { String::new() } else { format!(".{rest}") };
    format!("{sign}{first}{rest}e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs())
}

/// `digits` (`15` for 1.5) with its first digit at decimal exponent `exp`, in plain notation.
fn plain(digits: &str, exp: i32) -> String {
    if exp < 0 {
        return format!("0.{}{digits}", "0".repeat((-exp - 1) as usize));
    }
    let whole = exp as usize + 1;
    if digits.len() <= whole {
        format!("{digits}{}", "0".repeat(whole - digits.len()))
    } else {
        format!("{}.{}", &digits[..whole], &digits[whole..])
    }
}

/// `s` as a JSON string, escaped as Postgres escapes it: `"` and `\` with a backslash, `\b \f \n \r \t` by name, the
/// other characters below 0x20 as `\u00xx`, and nothing else (`/` and non-ASCII stay as they are).
pub(crate) fn json_string(out: &mut String, s: &str) {
    out.push('"');
    // (the bytes that need an escape are all below 0x80, so a byte's position is a character's boundary too: the text
    // between two escapes is copied in one piece)
    let mut run = 0;
    for (at, &b) in s.as_bytes().iter().enumerate() {
        if b >= 0x20 && b != b'"' && b != b'\\' {
            continue;
        }
        out.push_str(&s[run..at]);
        run = at + 1;
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            0x8 => out.push_str("\\b"),
            0xc => out.push_str("\\f"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            _ => {
                let _ = write!(out, "\\u{:04x}", b);
            }
        }
    }
    out.push_str(&s[run..]);
    out.push('"');
}

/// A date as JSON text. Years 0 to 9999 are written with their four digits here; chrono's `%Y` takes a sign past that,
/// so other years keep its form.
fn date(out: &mut String, d: Option<NaiveDate>) {
    match d {
        Some(d) if (0..=9999).contains(&d.year()) => {
            let _ = write!(out, "\"{:04}-{:02}-{:02}\"", d.year(), d.month(), d.day());
        }
        Some(d) => json_string(out, &d.format("%Y-%m-%d").to_string()),
        None => out.push_str("null"),
    }
}

fn stamp(out: &mut String, a: &ArrayRef, i: usize, unit: &TimeUnit, zoned: bool) {
    let at = match unit {
        TimeUnit::Second => a.as_primitive::<TimestampSecondType>().value_as_datetime(i),
        TimeUnit::Millisecond => a.as_primitive::<TimestampMillisecondType>().value_as_datetime(i),
        TimeUnit::Microsecond => a.as_primitive::<TimestampMicrosecondType>().value_as_datetime(i),
        TimeUnit::Nanosecond => a.as_primitive::<TimestampNanosecondType>().value_as_datetime(i),
    };
    match at {
        Some(at) => json_string(out, &iso_stamp(at, zoned)),
        None => out.push_str("null"),
    }
}

/// A timestamp as JSON writes it: `2024-01-02T03:04:05.5`, in UTC with `+00:00` when it has a zone.
fn iso_stamp(at: NaiveDateTime, zoned: bool) -> String {
    let micros = at.time().signed_duration_since(NaiveTime::MIN).num_microseconds().unwrap_or(0);
    format!("{}T{}{}", at.format("%Y-%m-%d"), time_text(micros), if zoned { "+00:00" } else { "" })
}

fn clock(out: &mut String, a: &ArrayRef, i: usize) {
    let micros = match a.data_type() {
        DataType::Time32(TimeUnit::Second) => a.as_primitive::<Time32SecondType>().value(i) as i64 * 1_000_000,
        DataType::Time32(_) => a.as_primitive::<Time32MillisecondType>().value(i) as i64 * 1_000,
        DataType::Time64(TimeUnit::Nanosecond) => a.as_primitive::<Time64NanosecondType>().value(i) / 1_000,
        _ => a.as_primitive::<Time64MicrosecondType>().value(i),
    };
    json_string(out, &time_text(micros));
}

/// An interval as Postgres's text (`1 day 02:00:00`), in each of Arrow's three units.
fn interval(out: &mut String, a: &ArrayRef, i: usize) {
    let (months, days, micros) = match a.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => (a.as_primitive::<IntervalYearMonthType>().value(i) as i64, 0, 0),
        DataType::Interval(IntervalUnit::DayTime) => {
            let v = a.as_primitive::<IntervalDayTimeType>().value(i);
            (0, v.days as i64, v.milliseconds as i64 * 1_000)
        }
        _ => {
            let v = a.as_primitive::<IntervalMonthDayNanoType>().value(i);
            (v.months as i64, v.days as i64, v.nanoseconds / 1_000)
        }
    };
    json_string(out, &interval_text(months, days, micros));
}

/// Binary as Postgres's bytea text: `\x` and its hex digits, which JSON then escapes (`"\\x0102"`).
fn bytea(out: &mut String, bytes: &[u8]) {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    json_string(out, &format!("\\x{hex}"));
}

/// A list as `[1,2]`, no spaces, each element by `value` with its item field's mark.
fn list(out: &mut String, items: &ArrayRef, item: &Field) -> Result<()> {
    out.push('[');
    for j in 0..items.len() {
        if j > 0 {
            out.push(',');
        }
        value(out, items, j, is_json_field(item))?;
    }
    out.push(']');
    Ok(())
}

/// A struct as `{"a":1,"b":"x"}`: its field names as keys, no spaces.
fn struct_row(out: &mut String, s: &datafusion::arrow::array::StructArray, i: usize) -> Result<()> {
    out.push('{');
    for (k, field) in s.fields().iter().enumerate() {
        if k > 0 {
            out.push(',');
        }
        json_string(out, field.name());
        out.push(':');
        value(out, s.column(k), i, is_json_field(field))?;
    }
    out.push('}');
    Ok(())
}

/// A map as an object of its keys' text.
fn map_row(out: &mut String, m: &datafusion::arrow::array::MapArray, i: usize) -> Result<()> {
    let entries = m.value(i); // (a map's entries are a struct of its key and value columns)
    let (keys, values) = (entries.column(0), entries.column(1));
    let json = is_json_field(&entries.fields()[1]);
    out.push('{');
    for j in 0..keys.len() {
        if j > 0 {
            out.push(',');
        }
        key(out, keys, j, false)?;
        out.push(':');
        value(out, values, j, json)?;
    }
    out.push('}');
    Ok(())
}

/// A key of an object, as JSON text: a text's own escaped text, a number's or a boolean's in quotes (its text is the
/// key). NULL, JSON, an array, a composite or a map is refused, as Postgres refuses them (22004, 22023).
fn key(out: &mut String, a: &ArrayRef, i: usize, json: bool) -> Result<()> {
    if is_null_at(a, i) {
        return Err(null_key());
    }
    if json || is_composite(a.data_type()) {
        return Err(fail(coded("22023", "key value must be scalar, not array, composite, or json")));
    }
    match a.data_type() {
        // (a text key is its own text; a boolean or an integer is its text in quotes, as `value` writes it)
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => json_string(out, text_at(a, i)),
        DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            out.push('"');
            value(out, a, i, false)?;
            out.push('"');
        }
        _ => {
            let mut text = String::new();
            value(&mut text, a, i, false)?;
            if text.starts_with('"') {
                out.push_str(&text);
            } else {
                json_string(out, &text);
            }
        }
    }
    Ok(())
}

/// `json_build_object` and `pondra_row`: the arguments as `key colon value` pairs in braces, joined by `comma`. An odd
/// count is 22023, checked before any row, with Postgres's hint.
fn pairs(a: &[Arg], n: usize, colon: &str, comma: &str) -> Result<ArrayRef> {
    if a.len() % 2 == 1 {
        return Err(fail(coded(
            "22023",
            "argument list must have even number of elements\nHINT:  The arguments of json_build_object() must consist of alternating keys and values.",
        )));
    }
    per_row(n, |i, out| {
        out.push('{');
        for (k, pair) in a.chunks(2).enumerate() {
            if k > 0 {
                out.push_str(comma);
            }
            key(out, &pair[0].values, i, pair[0].json)?;
            out.push_str(colon);
            value(out, &pair[1].values, i, pair[1].json)?;
        }
        out.push('}');
        Ok(true)
    })
}

/// `json_build_array`: the arguments as `[a, b]`, joined by ", ".
fn array_of(a: &[Arg], n: usize) -> Result<ArrayRef> {
    per_row(n, |i, out| {
        out.push('[');
        for (k, arg) in a.iter().enumerate() {
            if k > 0 {
                out.push_str(", ");
            }
            value(out, &arg.values, i, arg.json)?;
        }
        out.push(']');
        Ok(true)
    })
}

/// How an aggregate's elements are written: `json` and `row` elements are JSON text already, `value` ones are values.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Json,
    Row,
    Value,
}

impl Kind {
    fn of(name: &str) -> Result<Kind> {
        match name {
            "json" => Ok(Kind::Json),
            "row" => Ok(Kind::Row),
            "value" => Ok(Kind::Value),
            other => Err(fail(coded("22023", format!("an aggregate's kind is 'json', 'row' or 'value', not '{other}'")))),
        }
    }
}

/// Row `i` of a list column: its elements, and the item field they are typed by. None where the row is NULL.
fn list_row(list: &ArrayRef, i: usize) -> Result<Option<(ArrayRef, FieldRef)>> {
    if list.is_null(i) {
        return Ok(None);
    }
    Ok(Some(match list.data_type() {
        DataType::List(item) => (list.as_list::<i32>().value(i), item.clone()),
        DataType::LargeList(item) => (list.as_list::<i64>().value(i), item.clone()),
        DataType::FixedSizeList(item, _) => (list.as_fixed_size_list().value(i), item.clone()),
        other => return Err(fail(coded("22023", format!("an aggregate takes a list, not {other}")))),
    }))
}

/// `pondra_json_agg(list, kind)`: a row's list as one JSON array, NULL for a NULL list. Elements are joined by ", ";
/// Postgres puts a line feed after the comma before a non-NULL element that is a row or an array or composite written
/// as a value (never before the first, and never after a NULL), so `json_agg` of rows reads `[{…}, \n {…}]`.
fn json_agg(a: &[Arg], n: usize) -> Result<ArrayRef> {
    let kinds = cast(&a[1].values, &DataType::Utf8)?;
    per_row(n, |i, out| {
        let Some((elements, item)) = list_row(&a[0].values, i)? else { return Ok(false) };
        out.push('[');
        joined(out, &elements, &item, Kind::of(kinds.as_string::<i32>().value(i))?)?;
        out.push(']');
        Ok(true)
    })
}

/// The elements of one list, joined as `kind` says (see `json_agg`), written into `out`.
fn joined(out: &mut String, elements: &ArrayRef, item: &Field, kind: Kind) -> Result<()> {
    let elements = if kind == Kind::Value { elements.clone() } else { as_text(elements)? };
    let lines = kind == Kind::Row || (kind == Kind::Value && !is_scalar(item.data_type()));
    let json = is_json_field(item);
    for j in 0..elements.len() {
        if j > 0 {
            out.push_str(", ");
            if lines && !is_null_at(&elements, j) {
                out.push_str("\n ");
            }
        }
        element(out, &elements, j, kind, json)?;
    }
    Ok(())
}

/// Element `i`: JSON text as it is for `json` and `row` (a text array, see `joined`), a value written by `value`.
fn element(out: &mut String, a: &ArrayRef, i: usize, kind: Kind, json: bool) -> Result<()> {
    if kind == Kind::Value {
        return value(out, a, i, json);
    }
    out.push_str(if is_null_at(a, i) { "null" } else { text_at(a, i) });
    Ok(())
}

/// Whether a type is one Postgres writes on one line (a scalar) rather than as an array, composite or map.
fn is_scalar(t: &DataType) -> bool { !is_composite(t) }

/// `pondra_json_object_agg(list, kind)`: a list of pairs (a struct of the key and the value) as `{ "k" : v, … }`, NULL
/// for a NULL list. A NULL key is 22004.
fn object_agg(a: &[Arg], n: usize) -> Result<ArrayRef> {
    let kinds = cast(&a[1].values, &DataType::Utf8)?;
    per_row(n, |i, out| {
        let Some((pairs, item)) = list_row(&a[0].values, i)? else { return Ok(false) };
        object_of(out, &pairs, &item, Kind::of(kinds.as_string::<i32>().value(i))?)?;
        Ok(true)
    })
}

/// One row of `pondra_json_object_agg`, written into `out`: its pairs, in order (a duplicate key is kept).
fn object_of(out: &mut String, pairs: &ArrayRef, item: &Field, kind: Kind) -> Result<()> {
    let DataType::Struct(fields) = item.data_type() else {
        return Err(fail(coded("22023", "an object aggregate takes a list of pairs (a key and a value)")));
    };
    if fields.len() != 2 {
        return Err(fail(coded("22023", "an object aggregate takes a list of pairs (a key and a value)")));
    }
    let entries = pairs.as_struct();
    let values = if kind == Kind::Value { entries.column(1).clone() } else { as_text(entries.column(1))? };
    let (keys, value_json) = (entries.column(0), is_json_field(&fields[1]));
    out.push_str("{ ");
    for j in 0..entries.len() {
        if j > 0 {
            out.push_str(", ");
        }
        if entries.is_null(j) {
            return Err(null_key());
        }
        key(out, keys, j, is_json_field(&fields[0]))?;
        out.push_str(" : ");
        element(out, &values, j, kind, value_json)?;
    }
    out.push_str(" }");
    Ok(())
}

/// `jsonb_text` of each answer: the jsonb forms of the `jsonb_*` functions. A text that isn't JSON is 22P02.
fn as_jsonb(texts: ArrayRef) -> Result<ArrayRef> {
    // (a column of text may come as Utf8View, which `as_string` refuses: it panics, and the node answers 500)
    let texts = cast(&texts, &DataType::Utf8)?;
    let texts = texts.as_string::<i32>();
    per_row(texts.len(), |i, out| {
        if texts.is_null(i) {
            return Ok(false);
        }
        jsonb_into(out, texts.value(i))?;
        Ok(true)
    })
}

/// `json` (JSON text) as jsonb writes it: an object's keys sorted by length, then bytes, its last duplicate kept;
/// numbers as Postgres's `numeric` prints them; `, ` and `: ` between members. A text that isn't JSON is 22P02, and
/// one nested past `MAX_DEPTH` is 54001.
pub(crate) fn jsonb_text(json: &str) -> Result<String> {
    let mut out = String::new();
    jsonb_into(&mut out, json)?;
    Ok(out)
}

/// `jsonb_text`, written into `out`.
pub(crate) fn jsonb_into(out: &mut String, json: &str) -> Result<()> {
    let node = parse(json)?;
    write(out, &node).ok_or_else(|| fail(coded("22P02", json_refusal(json))))
}

/// The most levels of arrays and objects a document may nest. Postgres takes 5,000; the reader and the writer recurse
/// once per level, so a deeper document would overflow a node's stack, which no request may do (invariant 178).
pub(crate) const MAX_DEPTH: usize = 10_000;

/// `json` read into a tree (`jsonb_text` writes it). Refused as Postgres refuses it: 22P02 for text that isn't JSON,
/// 54001 past `MAX_DEPTH` levels.
pub(crate) fn parse(json: &str) -> Result<Node<'_>> {
    depth_check(json)?;
    read_whole(json).ok_or_else(|| fail(coded("22P02", json_refusal(json))))
}

/// The one value `json` holds, and nothing after it. None where it isn't JSON.
fn read_whole(json: &str) -> Option<Node<'_>> {
    let mut jiter = Jiter::new(json.as_bytes());
    let peek = jiter.peek().ok()?;
    let node = read(json, &mut jiter, peek)?;
    jiter.finish().ok()?;
    Some(node)
}

/// Refuses a document nested past `MAX_DEPTH` levels, before anything is read: a bracket inside a string doesn't count,
/// and a text's escapes are skipped. The byte scan is a few lines and costs less than the reading it guards.
pub(crate) fn depth_check(json: &str) -> Result<()> {
    let mut depth = 0usize;
    let mut in_text = false;
    let mut escaped = false;
    for b in json.bytes() {
        if in_text {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_text = false,
                _ => {}
            }
        } else if b == b'"' {
            in_text = true;
        } else if b == b'[' || b == b'{' {
            depth += 1;
            if depth > MAX_DEPTH {
                return Err(fail(coded("54001", "stack depth limit exceeded")));
            }
        } else if b == b']' || b == b'}' {
            depth = depth.saturating_sub(1);
        }
    }
    Ok(())
}

/// A JSON value as jsonb keeps it, borrowing from the document it was read from: a number as written, a text as written
/// when it has no escape (`Cow::Borrowed`), an object's members in jsonb's order (`in_jsonb_order`).
#[derive(Clone)]
pub(crate) enum Node<'a> {
    Null,
    Bool(bool),
    Number(&'a str),
    Text(Cow<'a, str>),
    List(Vec<Node<'a>>),
    Object(Vec<(Cow<'a, str>, Node<'a>)>),
}

/// The value at the reader's place, whose first byte is `peek`. `doc` is the text `jiter` reads, so a value's text is
/// borrowed from it: the reader's index before a string or a number is the token's first byte (`peek` moved it there).
fn read<'a>(doc: &'a str, jiter: &mut Jiter, peek: Peek) -> Option<Node<'a>> {
    Some(if peek == Peek::Null {
        jiter.known_null().ok()?;
        Node::Null
    } else if peek == Peek::True || peek == Peek::False {
        Node::Bool(jiter.known_bool(peek).ok()?)
    } else if peek == Peek::String {
        let at = jiter.current_index();
        Node::Text(as_written(doc, at, jiter.known_str().ok()?))
    } else if peek == Peek::Array {
        Node::List(list_of(doc, jiter)?)
    } else if peek == Peek::Object {
        Node::Object(object_of_json(doc, jiter)?)
    } else {
        Node::Number(number_text(doc, jiter, peek)?)
    })
}

/// A string token as the document has it: borrowed when no escape is in it (the text between its quotes), else
/// `decoded`. `at` is the reader's index before the token; the token's opening quote is the next `"` from there.
fn as_written<'a>(doc: &'a str, at: usize, decoded: &str) -> Cow<'a, str> {
    let rest = doc.get(at..).unwrap_or("");
    let Some(open) = rest.find('"') else { return Cow::Owned(decoded.to_string()) };
    let text = &rest[open + 1..];
    match text.find(['"', '\\']) {
        Some(end) if text.as_bytes()[end] == b'"' => Cow::Borrowed(&text[..end]),
        _ => Cow::Owned(decoded.to_string()),
    }
}

fn list_of<'a>(doc: &'a str, jiter: &mut Jiter) -> Option<Vec<Node<'a>>> {
    let mut items = Vec::new();
    let mut next = jiter.known_array().ok()?;
    while let Some(peek) = next {
        items.push(read(doc, jiter, peek)?);
        next = jiter.array_step().ok()?;
    }
    Some(items)
}

/// An object's members in jsonb's order: each key's text as written, with its value read after it.
fn object_of_json<'a>(doc: &'a str, jiter: &mut Jiter) -> Option<Vec<(Cow<'a, str>, Node<'a>)>> {
    let mut members = Vec::new();
    let at = jiter.current_index();
    let mut key = jiter.known_object().ok()?.map(|name| as_written(doc, at, name));
    while let Some(name) = key {
        let peek = jiter.peek().ok()?;
        members.push((name, read(doc, jiter, peek)?));
        let at = jiter.current_index();
        key = jiter.next_key().ok()?.map(|name| as_written(doc, at, name));
    }
    in_jsonb_order(&mut members);
    Some(members)
}

/// Puts an object's members in jsonb's order, each key once: by `order`, the last of a duplicate kept. Reversed, a
/// stable sort puts a duplicate's last member first, so the dedup (which keeps the first of a run) keeps that one.
pub(crate) fn in_jsonb_order<'a>(members: &mut Vec<(Cow<'a, str>, Node<'a>)>) {
    members.reverse();
    members.sort_by(|a, b| order(&a.0, &b.0));
    members.dedup_by(|a, b| a.0 == b.0);
}

/// jsonb's order of two keys: the shorter first, then by bytes.
fn order(a: &str, b: &str) -> Ordering { (a.len(), a.as_bytes()).cmp(&(b.len(), b.as_bytes())) }

/// Where `key` is among an object's members in jsonb's order: `Ok` at its own place, `Err` at the place it goes.
pub(crate) fn find(members: &[(Cow<'_, str>, Node<'_>)], key: &str) -> std::result::Result<usize, usize> {
    members.binary_search_by(|(name, _)| order(name, key))
}

/// A number's text as the document has it (never through f64): from the reader's index before it to the index after.
fn number_text<'a>(doc: &'a str, jiter: &mut Jiter, peek: Peek) -> Option<&'a str> {
    let at = jiter.current_index();
    jiter.known_number_bytes(peek).ok()?;
    doc.get(at..jiter.current_index())
}

/// jsonb's text for a tree: `[1, 2]`, `{"a": 1, "bb": 2}`, strings escaped as `value` escapes them. None where a number
/// is past what `numeric` holds.
pub(crate) fn write(out: &mut String, node: &Node) -> Option<()> {
    match node {
        Node::Null => out.push_str("null"),
        Node::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Node::Number(text) => number_into(out, text)?,
        Node::Text(s) => json_string(out, s),
        Node::List(items) => {
            out.push('[');
            for (k, item) in items.iter().enumerate() {
                if k > 0 {
                    out.push_str(", ");
                }
                write(out, item)?;
            }
            out.push(']');
        }
        Node::Object(members) => {
            out.push('{');
            for (k, (name, item)) in members.iter().enumerate() {
                if k > 0 {
                    out.push_str(", ");
                }
                json_string(out, name);
                out.push_str(": ");
                write(out, item)?;
            }
            out.push('}');
        }
    }
    Some(())
}

/// The most digits a shift may make (`numeric` holds 131,072 before its point); a larger exponent is refused.
const MAX_SHIFT: u64 = 131_072;

/// A JSON number as jsonb writes it, into `out`. A plain number (no exponent, at most `MAX_SHIFT` bytes) is written as
/// it is, since `numeric` prints such a number as written, except that a zero loses its sign (`numeric` has no `-0`).
/// Anything else goes through `numeric`.
fn number_into(out: &mut String, json: &str) -> Option<()> {
    if json.len() as u64 <= MAX_SHIFT && !json.contains(['e', 'E']) {
        let zero = json.bytes().all(|b| b == b'-' || b == b'.' || b == b'0');
        out.push_str(if zero { json.trim_start_matches('-') } else { json });
        return Some(());
    }
    out.push_str(&numeric(json)?);
    Some(())
}

/// A JSON number's text as Postgres's `numeric` prints it: plain digits, never an exponent. Its scale is the digits
/// after the point less the exponent, never below 0 (`1.50e-1` is `0.150`, `1e2` is `100`), and a zero has no sign.
fn numeric(json: &str) -> Option<String> {
    let (negative, unsigned) = match json.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, json),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().ok()?),
        None => (unsigned, 0),
    };
    let (int_part, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    // (the value is digits × 10^shift: the exponent less the digits after the point)
    let shift = exponent.checked_sub(fraction.len() as i64)?;
    if shift.unsigned_abs() > MAX_SHIFT {
        return None;
    }
    let digits = format!("{int_part}{fraction}");
    let scale = usize::try_from(-shift).unwrap_or(0);
    let coefficient = if shift > 0 { format!("{digits}{}", "0".repeat(shift as usize)) } else { digits };
    // (padded with zeros to at least one digit before the point)
    let padded = format!("{coefficient:0>width$}", width = scale + 1);
    let (whole, part) = padded.split_at(padded.len() - scale);
    let whole = match whole.trim_start_matches('0') {
        "" => "0",
        w => w,
    };
    let is_zero = padded.bytes().all(|b| b == b'0');
    let sign = if negative && !is_zero { "-" } else { "" };
    Some(if scale == 0 { format!("{sign}{whole}") } else { format!("{sign}{whole}.{part}") })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first column of `sql`'s answer, as text (NULL as None).
    async fn column(sql: &str) -> Vec<Option<String>> {
        let ctx = SessionContext::new();
        register(&ctx);
        let mut out = vec![];
        for batch in ctx.sql(sql).await.unwrap().collect().await.unwrap() {
            out.extend(batch.column(0).as_string::<i32>().iter().map(|v| v.map(str::to_string)));
        }
        out
    }

    /// The answer of a one-row query, as text.
    async fn one(sql: &str) -> String { column(sql).await[0].clone().unwrap() }

    /// The error `sql` is refused with, whether planning or running refuses it.
    async fn refusal(sql: &str) -> DataFusionError {
        let ctx = SessionContext::new();
        register(&ctx);
        match ctx.sql(sql).await {
            Err(e) => e,
            Ok(df) => match df.collect().await {
                Err(e) => e,
                Ok(_) => panic!("{sql} should be refused"),
            },
        }
    }

    /// The message of the error `sql` is refused with.
    async fn refused(sql: &str) -> String { refusal(sql).await.to_string() }

    /// The JSON mark of a query's one column, if it has one.
    async fn mark_of(sql: &str) -> Option<String> {
        let ctx = SessionContext::new();
        register(&ctx);
        let df = ctx.sql(sql).await.unwrap();
        df.schema().field(0).metadata().get(MARK).cloned()
    }

    #[test]
    fn floats_are_written_as_postgres_writes_them() {
        assert_eq!(float_text("1.5e-7"), "1.5e-07");
        assert_eq!(float_text("1e14"), "100000000000000");
        assert_eq!(float_text("1e15"), "1e+15");
        assert_eq!(float_text("-1.2345e17"), "-1.2345e+17");
        assert_eq!(float_text("1e-3"), "0.001");
        assert_eq!(float_text("1e-5"), "1e-05");
        assert_eq!(float_text("0e0"), "0");
        assert_eq!(float_text("-0e0"), "-0");
    }

    #[test]
    fn nan_and_infinity_are_strings() {
        let mut out = String::new();
        float_json(&mut out, f64::NAN);
        out.push(' ');
        float_json(&mut out, f64::NEG_INFINITY);
        assert_eq!(out, r#""NaN" "-Infinity""#);
    }

    #[test]
    fn strings_are_escaped_as_postgres_escapes_them() {
        let mut out = String::new();
        json_string(&mut out, "a\"b\\c\n\t\u{1}/é");
        assert_eq!(out, r#""a\"b\\c\n\t\u0001/é""#);
    }

    #[test]
    fn escapes_between_plain_runs_are_written_as_postgres_writes_them() {
        let mut out = String::new();
        json_string(&mut out, "é\"x\\y\u{1}\u{8}\u{c}\n\r\t/z");
        assert_eq!(out, r#""é\"x\\y\u0001\b\f\n\r\t/z""#);
        out.clear();
        json_string(&mut out, "");
        assert_eq!(out, r#""""#);
    }

    #[test]
    fn dates_of_years_0_to_9999_are_written_as_chrono_writes_them() {
        for (year, month, day) in [(0, 1, 1), (1, 1, 1), (999, 3, 4), (9999, 12, 31), (10_000, 1, 1)] {
            let date_value = NaiveDate::from_ymd_opt(year, month, day).unwrap();
            let mut out = String::new();
            date(&mut out, Some(date_value));
            assert_eq!(out, format!("\"{}\"", date_value.format("%Y-%m-%d")), "year {year}");
        }
    }

    #[test]
    fn numbers_keep_their_scale_as_numeric_does() {
        assert_eq!(numeric("1.50e-1").as_deref(), Some("0.150"));
        assert_eq!(numeric("1e2").as_deref(), Some("100"));
        assert_eq!(numeric("1e-3").as_deref(), Some("0.001"));
        assert_eq!(numeric("-12.5").as_deref(), Some("-12.5"));
        assert_eq!(numeric("-0.0").as_deref(), Some("0.0"));
        assert_eq!(numeric("0e5").as_deref(), Some("0"));
    }

    #[test]
    fn plain_numbers_are_written_as_they_are_except_a_negative_zero() {
        assert_eq!(jsonb_text("[-0.0, -0, 1.50, -12]").unwrap(), "[0.0, 0, 1.50, -12]");
        assert_eq!(jsonb_text("[-0.000, 0.5, -0.5, 123.4500]").unwrap(), "[0.000, 0.5, -0.5, 123.4500]");
    }

    #[test]
    fn jsonb_sorts_keys_and_keeps_the_last_duplicate() {
        assert_eq!(jsonb_text(r#"{"bb":1,"a":2,"ccc":3,"a":4}"#).unwrap(), r#"{"a": 4, "bb": 1, "ccc": 3}"#);
        assert_eq!(jsonb_text(r#"[1.0, {"b":null,"a":[true]}]"#).unwrap(), r#"[1.0, {"a": [true], "b": null}]"#);
        assert_eq!(jsonb_text(r#"{"x":"é\n"}"#).unwrap(), r#"{"x": "é\n"}"#);
        assert_eq!(jsonb_text(r#"{"b":{}, "a":[]}"#).unwrap(), r#"{"a": [], "b": {}}"#);
        assert_eq!(jsonb_text("{}").unwrap(), "{}");
        assert_eq!(jsonb_text("[]").unwrap(), "[]");
    }

    #[test]
    fn a_duplicate_key_keeps_its_last_value_in_jsonb_order() {
        assert_eq!(jsonb_text(r#"{"ab":1,"ab":2}"#).unwrap(), r#"{"ab": 2}"#);
        assert_eq!(jsonb_text(r#"{"é":1,"ab":2}"#).unwrap(), r#"{"ab": 2, "é": 1}"#);
        assert_eq!(jsonb_text(r#"{"k\"x":1,"aé":2}"#).unwrap(), r#"{"aé": 2, "k\"x": 1}"#);
    }

    #[test]
    fn values_keep_their_escapes_as_jsonb_writes_them() {
        assert_eq!(jsonb_text(r#"{"a":"x\"y\/zé"}"#).unwrap(), r#"{"a": "x\"y/zé"}"#);
        assert_eq!(jsonb_text(r#"{"a":"\\"}"#).unwrap(), r#"{"a": "\\"}"#);
        assert_eq!(jsonb_text(r#"{"a":"é/"}"#).unwrap(), r#"{"a": "é/"}"#);
        assert_eq!(jsonb_text(r#"{"a":"Aé\n"}"#).unwrap(), r#"{"a": "Aé\n"}"#);
    }

    #[test]
    fn exponent_numbers_are_written_as_numeric_prints_them() {
        assert_eq!(jsonb_text("[1e2, 1.5E-3, -2.50e+1, 1.0e0, 0.5e-1, 12e0]").unwrap(), "[100, 0.0015, -25.0, 1.0, 0.05, 12]");
    }

    #[test]
    fn jsonb_refuses_what_is_not_json() {
        for bad in ["{bad", "1 2", ""] {
            assert_eq!(crate::codes::of(&anyhow::Error::new(jsonb_text(bad).unwrap_err())), "22P02", "{bad}");
        }
    }

    #[test]
    fn nesting_is_taken_to_10000_levels_and_refused_past_it() {
        // (a test thread has 2 MB; a request has a node's 8 MB, and the reader and the writer recurse once per level)
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(|| {
                let nested = |n: usize| format!("{}{}", "[".repeat(n), "]".repeat(n));
                assert_eq!(jsonb_text(&nested(MAX_DEPTH)).unwrap(), nested(MAX_DEPTH));
                let refused = jsonb_text(&nested(MAX_DEPTH + 1)).unwrap_err();
                assert_eq!(crate::codes::of(&anyhow::Error::new(refused)), "54001");
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[tokio::test]
    async fn objects_and_arrays_are_built_as_postgres_builds_them() {
        assert_eq!(one("SELECT json_build_object('a', 1, 'b', 'x', 'c', NULL)").await, r#"{"a" : 1, "b" : "x", "c" : null}"#);
        assert_eq!(one("SELECT json_build_object('a', json_build_object('b', 1))").await, r#"{"a" : {"b" : 1}}"#);
        assert_eq!(one(r#"SELECT json_build_object('a', pondra_as_json('{"b": 1}'))"#).await, r#"{"a" : {"b": 1}}"#);
        assert_eq!(one("SELECT json_build_array(1, 'a', NULL)").await, r#"[1, "a", null]"#);
        assert_eq!(one("SELECT jsonb_build_object('bb', 1, 'a', 2)").await, r#"{"a": 2, "bb": 1}"#);
        assert_eq!(one("SELECT jsonb_build_array(1, 'a')").await, r#"[1, "a"]"#);
    }

    #[tokio::test]
    async fn keys_of_booleans_and_integers_are_quoted_and_text_keys_escaped() {
        assert_eq!(one("SELECT json_build_object(true, 1, 5, 2)").await, r#"{"true" : 1, "5" : 2}"#);
        assert_eq!(one(r#"SELECT json_build_object('a"b', 1)"#).await, r#"{"a\"b" : 1}"#);
    }

    #[tokio::test]
    async fn values_of_other_types_are_written_as_postgres_writes_them() {
        assert_eq!(one("SELECT to_json(CAST('2024-01-02' AS DATE))").await, r#""2024-01-02""#);
        assert_eq!(one("SELECT to_json(CAST(1e15 AS DOUBLE))").await, "1e+15");
        assert_eq!(one("SELECT to_json(named_struct('a', 1, 'b', 'x'))").await, r#"{"a":1,"b":"x"}"#);
        assert_eq!(one("SELECT to_jsonb(1.5)").await, "1.5");
    }

    #[tokio::test]
    async fn aggregates_put_a_line_feed_before_rows_as_postgres_does() {
        assert_eq!(one("SELECT pondra_json_agg(make_array(1, 2), 'value')").await, "[1, 2]");
        assert_eq!(one("SELECT pondra_json_agg(make_array(1, NULL, 3), 'value')").await, "[1, null, 3]");
        assert_eq!(one("SELECT pondra_json_agg(make_array(make_array(1), make_array(2)), 'value')").await, "[[1], \n [2]]");
        assert_eq!(one("SELECT pondra_json_agg(make_array(json_build_object('a', 1), json_build_object('a', 2)), 'row')").await, "[{\"a\" : 1}, \n {\"a\" : 2}]");
    }

    #[tokio::test]
    async fn object_aggregates_keep_their_order_and_format() {
        let sql = "SELECT pondra_json_object_agg(make_array(named_struct('k', 'a', 'v', 1), named_struct('k', 'b', 'v', CAST(NULL AS BIGINT))), 'value')";
        assert_eq!(one(sql).await, r#"{ "a" : 1, "b" : null }"#);
    }

    #[tokio::test]
    async fn answers_carry_their_json_mark() {
        assert_eq!(mark_of("SELECT to_json(1)").await.as_deref(), Some("json"));
        assert_eq!(mark_of("SELECT to_jsonb(1)").await.as_deref(), Some("jsonb"));
        assert_eq!(mark_of("SELECT json_build_object('a', 1)").await.as_deref(), Some("json"));
        assert_eq!(mark_of("SELECT jsonb_build_array(1)").await.as_deref(), Some("jsonb"));
        assert_eq!(mark_of("SELECT pondra_as_json('{}')").await.as_deref(), Some("json"));
        assert_eq!(mark_of("SELECT pondra_as_jsonb('{}')").await.as_deref(), Some("jsonb"));
        assert_eq!(mark_of("SELECT upper('a')").await, None);
    }

    /// The JSON mark of a query's one column, in a session with the JSON operators (`->` is `json_object_field`).
    async fn json_mark(sql: &str) -> Option<String> {
        let ctx = SessionContext::new();
        crate::pg_json::register(&ctx);
        let df = ctx.sql(sql).await.unwrap();
        df.schema().field(0).metadata().get(MARK).cloned()
    }

    #[tokio::test]
    async fn an_arrow_is_json_when_its_document_is() {
        // (Postgres has `->` for JSON only: an answer of a JSON document is JSON, of a text one plain text)
        assert_eq!(json_mark(r#"SELECT json_object_field(pondra_as_json('{"a":1}'), 'a')"#).await.as_deref(), Some("json"));
        assert_eq!(json_mark(r#"SELECT json_object_field(pondra_as_jsonb('{"a":1}'), 'a')"#).await.as_deref(), Some("jsonb"));
        assert_eq!(json_mark(r#"SELECT json_object_field('{"a":1}', 'a')"#).await, None);
    }

    #[tokio::test]
    async fn pondra_as_jsonb_answers_as_jsonb_prints() {
        assert_eq!(one(r#"SELECT pondra_as_jsonb('{"b":1,"a":2}')"#).await, r#"{"a": 2, "b": 1}"#);
        assert_eq!(column("SELECT pondra_as_jsonb(CAST(NULL AS TEXT))").await, vec![None]);
        assert_eq!(crate::codes::of(&anyhow::Error::new(refusal(r#"SELECT pondra_as_jsonb('{bad')"#).await)), "22P02");
    }

    #[tokio::test]
    async fn refusals_name_the_postgres_error() {
        assert!(refused("SELECT json_build_object('a')").await.contains("argument list must have even number of elements"));
        assert!(refused("SELECT json_build_object(NULL, 1)").await.contains("null value not allowed for object key"));
        assert!(refused("SELECT json_build_object(make_array(1), 1)").await.contains("key value must be scalar, not array, composite, or json"));
    }

    #[tokio::test]
    async fn refusals_carry_their_sqlstate() {
        assert_eq!(crate::codes::of(&anyhow::Error::new(refusal("SELECT json_build_object('a')").await)), "22023");
        assert_eq!(crate::codes::of(&anyhow::Error::new(refusal("SELECT json_build_object(NULL, 1)").await)), "22004");
    }
}
