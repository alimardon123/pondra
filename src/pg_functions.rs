//! Postgres's functions that BI tools and drivers send through every door, not only in catalog queries:
//! array bounds (Grafana, pgjdbc), `format` (Metabase), `quote_*` and `parse_ident` (Grafana),
//! `to_char`, `to_date` and `to_timestamp` with Postgres's templates (Superset, Metabase), and
//! `generate_series` over any integers (DataFusion's takes only BIGINT). Registered in every session
//! (`engine::session`); the catalog's own are `pg_catalog.rs`'s. The text and templates follow Postgres 16
//! (`pg_text.rs`, `pg_formats.rs`); this file is only how they meet DataFusion.

use crate::pg_json::is_text;
use crate::{pg_formats, pg_text};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, Int64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

// ---------------------------------------------------------------- the builder
// (how a function meets DataFusion: every Postgres function the core registers is made with these)

/// The field metadata that marks an answer's column with its Postgres type (`uuid`, `json`, `jsonb`): the port reads it, other doors pass it by.
pub const MARK: &str = "pg_type";

type Body = dyn Fn(&[ArrayRef], usize) -> datafusion::error::Result<ArrayRef> + Send + Sync;

/// A function Postgres's catalog queries call, any arguments; `returns` from the arguments' types.
struct PgFn {
    name: &'static str,
    signature: Signature,
    returns: fn(&[DataType]) -> DataType,
    body: Arc<Body>,
    marks: Option<&'static str>, // the Postgres type its answers' column is marked with (`MARK`): "json"
    follows: bool, // (no fixed mark: the answers are JSON when the first argument is, as `->` is)
}

impl std::fmt::Debug for PgFn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "PgFn({})", self.name) }
}
impl PartialEq for PgFn {
    fn eq(&self, o: &Self) -> bool { self.name == o.name }
}
impl Eq for PgFn {}
impl std::hash::Hash for PgFn {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.name.hash(h) }
}

impl ScalarUDFImpl for PgFn {
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, args: &[DataType]) -> datafusion::error::Result<DataType> { Ok((self.returns)(args)) }
    // (the mark rides on the field the function returns, so a projection of it keeps it: the port sends json as json)
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let field = Field::new(self.name, (self.returns)(&types), true);
        let mark = match self.marks {
            Some(mark) => Some(mark.to_string()),
            None if self.follows => first_json_mark(args.arg_fields),
            None => None,
        };
        Ok(Arc::new(match mark {
            Some(mark) => field.with_metadata([(MARK.to_string(), mark)].into()),
            None => field,
        }))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> {
        let n = args.number_rows;
        let arrays = args.args.iter().map(|a| a.to_array(n)).collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array((self.body)(&arrays, n)?))
    }
}

/// A function of Postgres's over whole columns: `body` gets every argument as an array of the rows' values.
pub(crate) fn function(name: &'static str, signature: Signature, returns: fn(&[DataType]) -> DataType, body: impl Fn(&[ArrayRef], usize) -> datafusion::error::Result<ArrayRef> + Send + Sync + 'static) -> ScalarUDF {
    marked(name, signature, returns, None, body)
}

/// `function`, whose answers are JSON text marked `mark` ("json" or "jsonb"): the column they make is sent as that type.
pub(crate) fn marked(name: &'static str, signature: Signature, returns: fn(&[DataType]) -> DataType, mark: Option<&'static str>, body: impl Fn(&[ArrayRef], usize) -> datafusion::error::Result<ArrayRef> + Send + Sync + 'static) -> ScalarUDF {
    ScalarUDF::new_from_impl(PgFn { name, signature, returns, body: Arc::new(body), marks: mark, follows: false })
}

/// `function`, whose answers are JSON exactly when its first argument is (`doc -> 'a'` of a JSON column is JSON, of a
/// text one text, as Postgres has the operator for JSON only): the mark is the first argument's, if it is JSON's.
pub(crate) fn follows_json(name: &'static str, signature: Signature, returns: fn(&[DataType]) -> DataType, body: impl Fn(&[ArrayRef], usize) -> datafusion::error::Result<ArrayRef> + Send + Sync + 'static) -> ScalarUDF {
    ScalarUDF::new_from_impl(PgFn { name, signature, returns, body: Arc::new(body), marks: None, follows: true })
}

/// The JSON mark of a function's first argument, if it has one (`json` or `jsonb`).
fn first_json_mark(arg_fields: &[FieldRef]) -> Option<String> {
    let mark = arg_fields.first()?.metadata().get(MARK)?;
    matches!(mark.as_str(), "json" | "jsonb").then(|| mark.clone())
}

/// An argument as text (NULL where it is), whatever string or number type it came as.
pub(crate) fn text_arg(a: &ArrayRef) -> datafusion::error::Result<Vec<Option<String>>> {
    let c = datafusion::arrow::compute::cast(a, &DataType::Utf8)?;
    Ok(c.as_string::<i32>().iter().map(|v| v.map(str::to_string)).collect())
}

pub(crate) fn int_arg(a: &ArrayRef) -> datafusion::error::Result<Vec<Option<i64>>> {
    let c = datafusion::arrow::compute::cast(a, &DataType::Int64)?;
    Ok(c.as_primitive::<datafusion::arrow::datatypes::Int64Type>().iter().collect())
}

pub(crate) fn texts_out(v: impl IntoIterator<Item = Option<String>>) -> ArrayRef { Arc::new(StringArray::from_iter(v)) }

pub fn register(ctx: &SessionContext) {
    for name in ["generate_series", "range"] {
        if let Ok(f) = ctx.table_function(name) {
            ctx.register_udtf(name, Arc::new(Widened(f.function().clone())));
        }
    }
    let pure = |n: usize| Signature::any(n, Volatility::Immutable);
    ctx.register_udf(function("array_lower", pure(2), |_| DataType::Int32, |a, n| bound(a, n, "array_lower")));
    ctx.register_udf(function("array_upper", pure(2), |_| DataType::Int32, |a, n| bound(a, n, "array_upper")));
    ctx.register_udf(function("format", Signature::variadic_any(Volatility::Immutable), |_| DataType::Utf8, format));
    ctx.register_udf(function("quote_ident", pure(1), |_| DataType::Utf8, |a, _| each(&a[0], |s| Some(pg_text::quote_ident(s)))));
    ctx.register_udf(function("quote_literal", pure(1), |_| DataType::Utf8, |a, _| each(&a[0], |s| Some(pg_text::quote_literal(s)))));
    ctx.register_udf(function("quote_nullable", pure(1), |_| DataType::Utf8, |a, _| {
        Ok(texts_out(as_text(&a[0])?.into_iter().map(|v| Some(v.map_or_else(|| "NULL".into(), |s| pg_text::quote_literal(&s))))))
    }));
    let one_or_two = Signature::one_of(vec![TypeSignature::Any(1), TypeSignature::Any(2)], Volatility::Immutable);
    ctx.register_udf(function("parse_ident", one_or_two, |_| DataType::new_list(DataType::Utf8, true), parse_ident));
    for (name, kind) in [("to_char", Kind::Char), ("to_date", Kind::Date), ("to_timestamp", Kind::Timestamp)] {
        if let Ok(inner) = ctx.udf(name) {
            ctx.register_udf(ScalarUDF::new_from_impl(Templated { inner, kind }));
        }
    }
    crate::pg_bytea::register(ctx);
}

/// `generate_series(1, array_upper(…, 1))`: DataFusion's series take only BIGINT literals, and Postgres's
/// clients pass INTEGER ones (pgjdbc's type cache, Grafana's schema lookups), so any integer literal is
/// taken as a BIGINT (and a NULL of one as NULL: no rows, as in Postgres).
#[derive(Debug)]
struct Widened(Arc<dyn TableFunctionImpl>);

impl TableFunctionImpl for Widened {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let widened = args.exprs().iter().map(|e| match e {
            Expr::Literal(v, _) if v.data_type().is_integer() && (v.is_null() || v.data_type() != DataType::Int64) => {
                Ok(Expr::Literal(if v.is_null() { ScalarValue::Null } else { v.cast_to(&DataType::Int64)? }, None))
            }
            e => Ok(e.clone()),
        }).collect::<Result<Vec<_>>>()?;
        self.0.call_with_args(TableFunctionArgs::new(&widened, args.session()))
    }
}

/// `array_lower(a, d)` and `array_upper(a, d)`: an array's bounds in its `d`th dimension, from 1 to its
/// length (Postgres's arrays here always start at 1); NULL for an empty or NULL array, or a dimension it
/// doesn't have. A dimension past the first is its first element's, as Postgres's arrays are rectangular.
fn bound(a: &[ArrayRef], n: usize, name: &str) -> Result<ArrayRef> {
    if !matches!(a[0].data_type(), DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) | DataType::Null) {
        return exec_err!("{name} takes an array, not {}", a[0].data_type());
    }
    let dims = int_arg(&a[1])?;
    let upper = name == "array_upper";
    Ok(Arc::new(Int32Array::from_iter((0..n).map(|i| {
        let len = length(&a[0], i, dims[i]?)?;
        (len > 0).then_some(if upper { len as i32 } else { 1 })
    }))))
}

/// The length of row `i`'s list in dimension `dim`.
fn length(a: &ArrayRef, i: usize, dim: i64) -> Option<usize> {
    if dim < 1 || a.is_null(i) {
        return None;
    }
    let items = list_at(a, i)?;
    match dim {
        1 => Some(items.len()),
        _ if items.is_empty() => None,
        _ => length(&items, 0, dim - 1),
    }
}

/// Row `i` of a list column, as an array of its items (None when it isn't a list).
fn list_at(a: &ArrayRef, i: usize) -> Option<ArrayRef> {
    match a.data_type() {
        DataType::List(_) => Some(a.as_list::<i32>().value(i)),
        DataType::LargeList(_) => Some(a.as_list::<i64>().value(i)),
        DataType::FixedSizeList(..) => Some(a.as_fixed_size_list().value(i)),
        _ => None,
    }
}

/// `format('%s.%I', a, b)`: each argument as Postgres writes it as text.
fn format(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let template = text_arg(&a[0])?;
    let args = a[1..].iter().map(as_text).collect::<Result<Vec<_>>>()?;
    let rows = (0..n).map(|i| {
        let Some(f) = &template[i] else { return Ok(None) };
        let row: Vec<Option<&str>> = args.iter().map(|c| c[i].as_deref()).collect();
        pg_text::format(f, &row).map(Some).or_else(|e| exec_err!("format: {e}"))
    });
    Ok(texts_out(rows.collect::<Result<Vec<_>>>()?))
}

/// `parse_ident('crm."Sales"')`: the parts of a qualified name, unquoted.
fn parse_ident(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let list = DataType::new_list(DataType::Utf8, true);
    if n == 0 {
        return Ok(datafusion::arrow::array::new_empty_array(&list));
    }
    let names = text_arg(&a[0])?;
    let strict = match a.get(1) {
        Some(b) => cast(b, &DataType::Boolean)?.as_boolean().iter().collect(),
        None => vec![Some(true); n],
    };
    let rows = (0..n).map(|i| match (&names[i], strict[i]) {
        (Some(name), Some(strict)) => {
            let parts = pg_text::parse_ident(name, strict).or_else(|e| exec_err!("parse_ident: {e}"))?;
            let parts: Vec<ScalarValue> = parts.into_iter().map(|p| ScalarValue::Utf8(Some(p))).collect();
            Ok(ScalarValue::List(ScalarValue::new_list_nullable(&parts, &DataType::Utf8)))
        }
        _ => ScalarValue::try_from(&list),
    });
    ScalarValue::iter_to_array(rows.collect::<Result<Vec<_>>>()?)
}

/// A text function of one argument, NULL where it is.
fn each(a: &ArrayRef, f: impl Fn(&str) -> Option<String>) -> Result<ArrayRef> {
    Ok(texts_out(as_text(a)?.into_iter().map(|v| v.and_then(|s| f(&s)))))
}

/// A value as Postgres writes it as text: booleans as `t` and `f`, timestamps with a space, not a `T`.
fn as_text(a: &ArrayRef) -> Result<Vec<Option<String>>> {
    Ok(match a.data_type() {
        DataType::Boolean => a.as_boolean().iter().map(|v| v.map(|b| if b { "t" } else { "f" }.to_string())).collect(),
        DataType::Timestamp(..) => text_arg(a)?.into_iter().map(|v| v.map(|s| s.replacen('T', " ", 1))).collect(),
        _ => text_arg(a)?,
    })
}

/// What a templated function answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Char,
    Date,
    Timestamp,
}

/// `to_char(ts, 'YYYY-MM-DD')`, `to_date(text, 'DD Mon YYYY')`, `to_timestamp(text, 'HH24:MI')`: a format
/// with no `%` is a Postgres template, read as Postgres 16 reads it; one with `%` (chrono's) is DataFusion's,
/// and so is every other call (`to_timestamp(epoch)`, several formats, a number to `to_char`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Templated {
    inner: Arc<ScalarUDF>,
    kind: Kind,
}

impl ScalarUDFImpl for Templated {
    fn name(&self) -> &str { self.inner.name() }
    fn signature(&self) -> &Signature { self.inner.signature() }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> { self.inner.coerce_types(types) }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> { self.inner.return_type(types) }
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        let inner = self.inner.inner().with_updated_config(config)?;
        Some(ScalarUDF::new_from_impl(Templated { inner: Arc::new(inner), kind: self.kind }))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // (Postgres's signatures: `to_char(timestamp, text)`, `to_date(text, text)`, `to_timestamp(text, text)`;
        // a number, or a format that isn't text, is DataFusion's call: `to_timestamp(123.5, name)` ignores
        // its format there, and `to_date(x, 12345)` is refused)
        let templates = match args.args.as_slice() {
            [value, f] if is_text(&f.data_type()) && if self.kind == Kind::Char { is_moment(&value.data_type()) } else { is_text(&value.data_type()) } => templates(f, args.number_rows)?,
            _ => None,
        };
        let Some(templates) = templates else { return self.inner.invoke_with_args(args) };
        let want = args.return_field.data_type().clone();
        let values = args.args[0].to_array(args.number_rows)?;
        let out = match self.kind {
            Kind::Char => to_char(&values, &templates)?,
            Kind::Date => read(&values, &templates, |s, t| Ok(pg_formats::to_date(s, t)?.signed_duration_since(epoch()).num_days()), DataType::Date32)?,
            Kind::Timestamp => read(&values, &templates, |s, t| stamp(pg_formats::to_timestamp(s, t)?, &want), want.clone())?,
        };
        let out = ColumnarValue::Array(out);
        if out.data_type() == want { Ok(out) } else { out.cast_to(&want, None) }
    }
}

/// The rows' templates, when every format given is one (None when any is chrono's).
fn templates(f: &ColumnarValue, n: usize) -> Result<Option<Vec<Option<String>>>> {
    let texts = text_arg(&f.to_array(n)?)?;
    Ok(texts.iter().flatten().all(|t| pg_formats::is_template(t)).then_some(texts))
}

/// A date or a timestamp, which Postgres's `to_char` templates write.
fn is_moment(t: &DataType) -> bool { matches!(t, DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)) }

fn epoch() -> chrono::NaiveDate { chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a date") }

/// `to_char` over dates and timestamps (in UTC, the session's zone).
fn to_char(values: &ArrayRef, templates: &[Option<String>]) -> Result<ArrayRef> {
    let micros = cast(values, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
    let micros = micros.as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>();
    let rows = (0..values.len()).map(|i| match (micros.is_valid(i), &templates[i]) {
        (true, Some(t)) => {
            let Some(at) = chrono::DateTime::from_timestamp_micros(micros.value(i)) else { return exec_err!("to_char: a time out of range") };
            pg_formats::to_char(at.naive_utc(), t).map(Some).or_else(|e| exec_err!("to_char: {e}"))
        }
        _ => Ok(None),
    });
    Ok(texts_out(rows.collect::<Result<Vec<_>>>()?))
}

/// `to_date` and `to_timestamp`: each text read by its template into a number of `unit`s, then that type.
fn read(values: &ArrayRef, templates: &[Option<String>], f: impl Fn(&str, &str) -> Result<i64, String>, unit: DataType) -> Result<ArrayRef> {
    let texts = text_arg(values)?;
    let rows = (0..values.len()).map(|i| match (&texts[i], &templates[i]) {
        (Some(s), Some(t)) => f(s, t).map(Some).or_else(|e| exec_err!("{e}")),
        _ => Ok(None),
    });
    let numbers: ArrayRef = Arc::new(Int64Array::from(rows.collect::<Result<Vec<_>>>()?));
    let numbers = if unit == DataType::Date32 { cast(&numbers, &DataType::Int32)? } else { numbers };
    Ok(cast(&numbers, &unit)?)
}

/// A timestamp as a number of the unit `want` counts in, or why it doesn't fit (a nanosecond count holds
/// the years 1677 to 2262; Postgres's year 1, a template's default, doesn't).
fn stamp(t: chrono::NaiveDateTime, want: &DataType) -> Result<i64, String> {
    let t = t.and_utc();
    let n = match want {
        DataType::Timestamp(TimeUnit::Second, _) => Some(t.timestamp()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(t.timestamp_millis()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(t.timestamp_micros()),
        _ => t.timestamp_nanos_opt(),
    };
    n.ok_or_else(|| format!("to_timestamp: {} is outside the years a timestamp holds here (1677 to 2262)", t.naive_utc()))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn one(sql: &str) -> String {
        let ctx = SessionContext::new();
        register(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string()
    }

    #[tokio::test]
    async fn postgres_functions_clients_send() {
        assert!(one("SELECT count(*) AS n FROM generate_series(CAST(1 AS INT), CAST(3 AS INT))").await.contains("| 3 |"));
        assert!(one("SELECT count(*) AS n FROM generate_series(1, CAST(NULL AS INT))").await.contains("| 0 |"));
        let b = one("SELECT array_lower(make_array(4, 5, 6), 1) AS l, array_upper(make_array(4, 5, 6), 1) AS u, array_upper(make_array(make_array(1, 2)), 2) AS d, array_upper(make_array(1), 3) AS x").await;
        assert!(b.contains("| 1 | 3 | 2 |   |"), "{b}");
        let n = one("SELECT count(*) AS n FROM generate_series(array_lower(string_to_array('a,b', ','), 1), array_upper(string_to_array('a,b', ','), 1))").await;
        assert!(n.contains("| 2 |"), "{n}");
        let f = one("SELECT format('%I.%I', 'crm', 'Sales') AS f, quote_ident('Sales') AS q, quote_nullable(CAST(NULL AS TEXT)) AS n, quote_literal('it''s') AS l").await;
        assert!(f.contains(r#"| crm."Sales" | "Sales" | NULL | 'it''s' |"#), "{f}");
        let c = one("SELECT to_char(TIMESTAMP '2024-03-05 14:07:09', 'YYYY-MM-DD HH24:MI:SS') AS a, to_char(TIMESTAMP '2024-03-05 14:07:09', '%Y') AS b, to_date('05 Mar 2024', 'DD Mon YYYY') AS d").await;
        assert!(c.contains("| 2024-03-05 14:07:09 | 2024 | 2024-03-05 |"), "{c}");
    }

    #[tokio::test]
    async fn a_number_or_a_format_that_isnt_text_is_datafusions_call() {
        // (sqllogictest's timestamps.slt and dates.slt: a float's format is ignored, an integer format refused)
        let t = one("SELECT to_timestamp(123.5, 'foo') AS t").await;
        assert!(t.contains("1970-01-01T00:02:03.500"), "{t}");
        let ctx = SessionContext::new();
        register(&ctx);
        let refused = async { ctx.sql("SELECT to_date(CAST(NULL AS VARCHAR), 12345)").await?.collect().await };
        assert!(refused.await.is_err());
    }
}
