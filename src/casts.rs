//! A cast of a decimal or a float to an integer rounds, as Postgres and DuckDB round it, where Arrow's
//! cast drops the fraction: `CAST(2.5 AS INT)` is 3 (a decimal rounds half away from zero) and
//! `CAST(2.5::DOUBLE AS INT)` is 2 (a float rounds half to even, as C's `rint`: 3.5 is 4). A query's
//! casts take `pondra_whole` of their value (`Rounds`, an analyzer rule after type coercion), and a
//! value written into an integer column, or a constant or a setval's value cast into one, is rounded the
//! same way (`into`).
//!
//! In Spark's dialect (`datafusion.sql_parser.dialect = 'spark'`, and `spark_sql('…')`, whose casts Pondra's
//! SQL turns into `pondra_truncate`) the fraction is cut toward zero instead, as Spark and Polars cast:
//! `CAST(2.7 AS INT)` is 2 and `CAST(-2.7 AS INT)` is -2 (`Rounds` picks it by the dialect).
use datafusion::arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray, PrimitiveArray, StringArray};
use datafusion::arrow::compute::{cast, cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type, DecimalType, Float32Type, Float64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::common::config::{ConfigOptions, Dialect};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::{Cast, TryCast};
use datafusion::logical_expr::{ColumnarValue, Expr, ExprSchemable, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use crate::codes::Violation;
use std::sync::{Arc, LazyLock};

/// Can a value of this type hold a fraction that a cast to an integer would drop: a float, or a
/// decimal with places.
pub fn fractional(t: &DataType) -> bool {
    match t {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::Decimal32(_, s) | DataType::Decimal64(_, s) | DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => *s > 0,
        _ => false,
    }
}

/// The values rounded to whole numbers, as a cast to an integer rounds them: a decimal half away
/// from zero, a float half to even. A decimal keeps its scale and gains a digit (99.5 is 100.0).
/// Any other type is returned as it is.
pub fn whole(c: &ArrayRef) -> Result<ArrayRef> {
    Ok(match c.data_type() {
        DataType::Float64 => Arc::new(c.as_primitive::<Float64Type>().unary::<_, Float64Type>(f64::round_ties_even)),
        DataType::Float32 => Arc::new(c.as_primitive::<Float32Type>().unary::<_, Float32Type>(f32::round_ties_even)),
        DataType::Float16 => cast(&whole(&cast(c, &DataType::Float32)?)?, &DataType::Float16)?, // (every whole f16 is an f32)
        DataType::Decimal32(..) => Arc::new(decimal::<Decimal32Type>(c.as_primitive(), true)?),
        DataType::Decimal64(..) => Arc::new(decimal::<Decimal64Type>(c.as_primitive(), true)?),
        DataType::Decimal128(..) => Arc::new(decimal::<Decimal128Type>(c.as_primitive(), true)?),
        DataType::Decimal256(..) => Arc::new(decimal::<Decimal256Type>(c.as_primitive(), true)?),
        _ => c.clone(),
    })
}

/// The values with their fraction cut toward zero, as Spark's and Polars' casts to an integer cut it:
/// 2.7 is 2, -2.7 is -2. A decimal keeps its type (no digit is added). Any other type is returned as it is.
pub fn truncated(c: &ArrayRef) -> Result<ArrayRef> {
    Ok(match c.data_type() {
        DataType::Float64 => Arc::new(c.as_primitive::<Float64Type>().unary::<_, Float64Type>(f64::trunc)),
        DataType::Float32 => Arc::new(c.as_primitive::<Float32Type>().unary::<_, Float32Type>(f32::trunc)),
        DataType::Float16 => cast(&truncated(&cast(c, &DataType::Float32)?)?, &DataType::Float16)?, // (every whole f16 is an f32)
        DataType::Decimal32(..) => Arc::new(decimal::<Decimal32Type>(c.as_primitive(), false)?),
        DataType::Decimal64(..) => Arc::new(decimal::<Decimal64Type>(c.as_primitive(), false)?),
        DataType::Decimal128(..) => Arc::new(decimal::<Decimal128Type>(c.as_primitive(), false)?),
        DataType::Decimal256(..) => Arc::new(decimal::<Decimal256Type>(c.as_primitive(), false)?),
        _ => c.clone(),
    })
}

/// A column as type `t`, or the refusal of the first value that won't go (never a quiet NULL), with Postgres's words
/// and SQLSTATE (`refused`). A value with a fraction going into an integer type is rounded first, as every other cast
/// into an integer rounds it (`whole`, invariant 293); the rest is Arrow's cast with `safe` off. Rows written into
/// tables, setval's value, COPY's text and a constant folded at planning all come this way.
pub fn into(c: &ArrayRef, t: &DataType) -> Result<ArrayRef> {
    if c.data_type() == t {
        return Ok(c.clone());
    }
    let c = if t.is_integer() && fractional(c.data_type()) { whole(c)? } else { c.clone() };
    cast_with_options(&c, t, &CastOptions { safe: false, ..Default::default() }).map_err(|e| refused(&c, t, e))
}

/// The refusal of a value that the CSV reader couldn't parse as its column's type, from the text of the error (Arrow's
/// `Error while parsing value '<v>' as type '<T>' …`; a leader's answer comes as text): the words and SQLSTATE `into`
/// gives that value, so `2.5` into an integer is 22P02 as Postgres has it. None for any other text, or when the cast
/// takes the value.
pub fn parse_refusal(text: &str) -> Option<DataFusionError> { refusal_of(text, None) }

/// `parse_refusal` for a value read from a file: its words end with `place`, the file and the line a person counts it on
/// (` (file, line 2)`), which the caller works out (`copy::line_of`: Arrow's own count of lines is not a file's line).
pub fn parse_refusal_in(text: &str, place: &str) -> Option<DataFusionError> { refusal_of(text, Some(place)) }

/// The column (from 0) and the text of the value a CSV reader refused in `text`: the file's cell the caller finds the line
/// of (`copy::line_of`).
pub fn refused_cell(text: &str) -> Option<(usize, String)> {
    let c = PARSE.captures(text)?;
    Some((c[3].parse().ok()?, c[1].to_string()))
}

static PARSE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?s)Error while parsing value '(.*?)' as type '([^']+)' for column (\d+) at line \d+").expect("a regex"));

fn refusal_of(text: &str, place: Option<&str>) -> Option<DataFusionError> {
    let c = PARSE.captures(text)?;
    let t = c[2].parse::<DataType>().ok()?;
    let value: ArrayRef = Arc::new(StringArray::from(vec![c[1].to_string()]));
    let refusal = into(&value, &t).err()?;
    let Some(place) = place else { return Some(refusal) };
    let e = anyhow::Error::from(refusal);
    Some(external(format!("{} ({place})", crate::codes::words(&e)), crate::codes::of(&e)))
}

/// A refusal with Postgres's words and SQLSTATE, as a DataFusion error (`codes::of` finds the code through the chain).
fn external(words: impl Into<String>, code: &'static str) -> DataFusionError {
    DataFusionError::External(Box::new(Violation(words.into(), code)))
}

/// The refusal of a cast of `c` to `t` that Arrow failed with `e`: text by its first value the safe cast turns to
/// NULL (`text_refusal`), a number by its type's range (`number_refusal`). What Postgres has no words for keeps Arrow's error.
fn refused(c: &ArrayRef, t: &DataType, e: ArrowError) -> DataFusionError {
    let source = match c.data_type() {
        DataType::Dictionary(_, values) => values.as_ref(), // (a Hive partition's column: its values are text)
        v => v,
    };
    if matches!(source, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) {
        text_refusal(c, t, e)
    } else if source.is_numeric() {
        if t.is_decimal() && has_nan(c) {
            nan_refusal()
        } else {
            number_refusal(t, e)
        }
    } else {
        e.into()
    }
}

/// The refusal of text `c` that won't cast to `t`, by its first value the safe cast turns to NULL, as Postgres reads it:
/// a text that overflows an integer type is 22003 with its value; a number that a decimal can't hold is 22003 (its size
/// refused it: 99.95 rounds to 100.0 in a numeric(3, 1); 1e5 and inf are numbers too, as Rust reads them); other text is
/// 22P02 with the type's name, and Arrow's words when the type has no name here.
fn text_refusal(c: &ArrayRef, t: &DataType, e: ArrowError) -> DataFusionError {
    let plain = || external(e.to_string(), "22P02");
    let Ok(texts) = cast(c, &DataType::Utf8) else { return plain() };
    let safe = CastOptions { safe: true, ..Default::default() };
    let Ok(safe_cast) = cast_with_options(&texts, t, &safe) else { return plain() };
    let values = texts.as_string::<i32>();
    let Some(i) = (0..values.len()).find(|&i| values.is_valid(i) && safe_cast.is_null(i)) else { return plain() };
    let text = values.value(i);
    if t.is_decimal() && text.trim().eq_ignore_ascii_case("nan") {
        return nan_refusal();
    }
    let range = match t {
        DataType::Int16 if overflows::<i16>(text) => Some("smallint"),
        DataType::Int32 if overflows::<i32>(text) => Some("integer"),
        DataType::Int64 if overflows::<i64>(text) => Some("bigint"),
        _ => None,
    };
    if let Some(name) = range {
        return external(format!("value \"{text}\" is out of range for type {name}"), "22003");
    }
    if t.is_decimal() && text.trim().parse::<f64>().is_ok() { // (a number Rust reads that a decimal can't hold: Postgres's numeric input reads 1e5 and Infinity too)
        return external("numeric field overflow", "22003");
    }
    match pg_name(t) {
        Some(name) => external(format!("invalid input syntax for type {name}: \"{text}\""), "22P02"),
        None => plain(),
    }
}

/// The refusal of NaN going into a decimal (`numeric(p, s)`): Postgres keeps NaN in numeric, a decimal column here holds
/// numbers only (0A000, named, so it is not taken for a value that is too big: 22003).
fn nan_refusal() -> DataFusionError {
    external("NaN can't go into a decimal column (numeric(p, s)): Postgres keeps NaN in numeric, but a decimal here holds only numbers", "0A000")
}

/// Is there a NaN in a float column (a cast of it into a decimal is refused by name: `nan_refusal`).
fn has_nan(c: &ArrayRef) -> bool {
    match c.data_type() {
        DataType::Float64 => c.as_primitive::<Float64Type>().iter().flatten().any(f64::is_nan),
        DataType::Float32 => c.as_primitive::<Float32Type>().iter().flatten().any(f32::is_nan),
        _ => false,
    }
}

/// The refusal of a number that won't go into `t`: Postgres's range words for the integer types it has (smallint, integer,
/// bigint, NaN and the infinities too), 22003 for a decimal, and Arrow's words, as 22003, for an integer Postgres has no
/// type for (Int8, the unsigned ones). Any other target keeps Arrow's error.
fn number_refusal(t: &DataType, e: ArrowError) -> DataFusionError {
    match t {
        DataType::Int16 => external("smallint out of range", "22003"),
        DataType::Int32 => external("integer out of range", "22003"),
        DataType::Int64 => external("bigint out of range", "22003"),
        t if t.is_decimal() => external("numeric field overflow", "22003"),
        t if t.is_integer() => external(e.to_string(), "22003"),
        _ => e.into(),
    }
}

/// Postgres's name for a type that a text is cast to, as its `invalid input syntax for type …` names it.
fn pg_name(t: &DataType) -> Option<&'static str> {
    Some(match t {
        DataType::Int16 => "smallint",
        DataType::Int32 => "integer",
        DataType::Int64 => "bigint",
        DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Boolean => "boolean",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Timestamp(_, None) => "timestamp",
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone",
        DataType::Time32(_) | DataType::Time64(_) => "time",
        t if t.is_decimal() => "numeric",
        _ => return None,
    })
}

/// Whether `text` is an integer that parses as `T` only by overflowing it (a space around it is not a digit).
fn overflows<T: std::str::FromStr<Err = std::num::ParseIntError>>(text: &str) -> bool {
    use std::num::IntErrorKind::{NegOverflow, PosOverflow};
    matches!(text.trim().parse::<T>(), Err(e) if matches!(e.kind(), PosOverflow | NegOverflow))
}

/// `whole`'s type for values of type `t`.
fn whole_type(t: &DataType) -> DataType {
    let wider = |p: &u8, max: u8| (*p + 1).min(max);
    match t {
        DataType::Decimal32(p, s) if *s > 0 => DataType::Decimal32(wider(p, Decimal32Type::MAX_PRECISION), *s),
        DataType::Decimal64(p, s) if *s > 0 => DataType::Decimal64(wider(p, Decimal64Type::MAX_PRECISION), *s),
        DataType::Decimal128(p, s) if *s > 0 => DataType::Decimal128(wider(p, Decimal128Type::MAX_PRECISION), *s),
        DataType::Decimal256(p, s) if *s > 0 => DataType::Decimal256(wider(p, Decimal256Type::MAX_PRECISION), *s),
        t => t.clone(),
    }
}

/// A decimal column's values at the decimal point, in their own scale: rounded half away from zero
/// (`rounds`), or cut toward zero.
fn decimal<T: DecimalType>(a: &PrimitiveArray<T>, rounds: bool) -> Result<PrimitiveArray<T>> {
    let s = match a.data_type() {
        DataType::Decimal32(_, s) | DataType::Decimal64(_, s) | DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => *s,
        _ => 0,
    };
    if s <= 0 {
        return Ok(a.clone());
    }
    let one = T::Native::usize_as(10).pow_wrapping(s as u32); // (1 in the column's scale)
    let half = one.div_wrapping(T::Native::usize_as(2));
    let dropped = a.unary::<_, T>(|v| {
        let fraction = v.mod_wrapping(one); // (with the value's sign)
        let cut = v.sub_wrapping(fraction);
        if !rounds {
            cut
        } else if fraction.is_ge(half) {
            cut.add_wrapping(one)
        } else if fraction.is_le(half.neg_wrapping()) {
            cut.sub_wrapping(one)
        } else {
            cut
        }
    });
    let t = if rounds { whole_type(a.data_type()) } else { a.data_type().clone() }; // (a cut keeps the type; a rounding can need a digit more)
    Ok(dropped.with_data_type(t))
}

/// `pondra_whole(x)` (`whole`: rounds, at `[0]`) and `pondra_truncate(x)` (`truncated`: cuts toward zero, at
/// `[1]`): what `Rounds` puts in a cast to an integer. One static holds both (`tools/per_node_check.py` lists
/// this one, and a second would need its own entry there).
static WHOLE: LazyLock<[Arc<ScalarUDF>; 2]> = LazyLock::new(|| [udf(false), udf(true)]);

/// The function a cast to an integer is rounded (or truncated) by.
fn function(truncate: bool) -> &'static Arc<ScalarUDF> {
    &WHOLE[truncate as usize]
}

fn udf(truncate: bool) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(Whole { truncate, signature: Signature::any(1, Volatility::Immutable) }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Whole {
    truncate: bool, // (a cut toward zero, not a rounding)
    signature: Signature,
}

impl ScalarUDFImpl for Whole {
    fn name(&self) -> &str {
        if self.truncate { "pondra_truncate" } else { "pondra_whole" }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(if self.truncate { args[0].clone() } else { whole_type(&args[0]) })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let f = if self.truncate { truncated } else { whole };
        Ok(match &args.args[0] {
            ColumnarValue::Array(a) => ColumnarValue::Array(f(a)?),
            ColumnarValue::Scalar(v) => ColumnarValue::Scalar(ScalarValue::try_from_array(&f(&v.to_array()?)?, 0)?),
        })
    }
}

/// Registers `pondra_whole` and `pondra_truncate` in a session (`engine.rs`), for plans that name them.
pub fn register(ctx: &SessionContext) {
    for f in WHOLE.iter() {
        ctx.register_udf(f.as_ref().clone());
    }
}

/// Puts `pondra_whole` in every cast (and `TRY_CAST`) of a decimal with places or a float to an
/// integer, or `pondra_truncate` in Spark's dialect. It runs after type coercion, so the casts coercion
/// adds round too. `arrow_cast(x, 'Int64')` becomes the cast it stands for here (the optimizer's
/// simplifier would make it one later, past this rule).
#[derive(Debug)]
pub struct Rounds;

impl AnalyzerRule for Rounds {
    fn name(&self) -> &str {
        "pondra_casts_round"
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let truncate = matches!(config.sql_parser.dialect, Dialect::Spark | Dialect::Databricks); // (Spark's casts cut a fraction; Pondra's round it)
        plan.transform_up_with_subqueries(|plan| {
            // (most plans cast nothing to an integer: their inputs' schemas aren't merged for nothing)
            let mut any = false;
            plan.apply_expressions(|e| {
                any = e.exists(|e| Ok(to_integer(e).is_some()))?;
                Ok(if any { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
            })?;
            if !any {
                return Ok(Transformed::no(plan));
            }
            let mut schema = datafusion::logical_expr::utils::merge_schema(&plan.inputs());
            if let LogicalPlan::TableScan(ts) = &plan {
                schema.merge(&DFSchema::try_from_qualified_schema(ts.table_name.clone(), &ts.source.schema())?); // (its filters name its own columns)
            }
            let names = datafusion::logical_expr::expr_rewriter::NamePreserver::new(&plan);
            plan.map_expressions(|e| {
                let name = names.save(&e);
                let e = e.transform_up(|e| rounded(e, &schema, truncate))?;
                Ok(e.update_data(|e| name.restore(e)))
            })
        })
        .data()
    }
}

/// The integer type `e` casts to, if it is a cast (or `arrow_cast`) to one.
fn to_integer(e: &Expr) -> Option<DataType> {
    let t = match e {
        Expr::Cast(Cast { field, .. }) | Expr::TryCast(TryCast { field, .. }) => field.data_type().clone(),
        Expr::ScalarFunction(f) if f.func.name() == "arrow_cast" => match f.args.get(1) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(t)) | ScalarValue::Utf8View(Some(t)) | ScalarValue::LargeUtf8(Some(t)), _)) => t.parse().ok()?,
            _ => return None,
        },
        _ => return None,
    };
    t.is_integer().then_some(t)
}

/// Is `e` a call of one of these functions: a value already cut, as `spark_sql('…')`'s casts arrive. Its
/// fraction is gone, so its cast gets no second call.
fn already_cut(e: &Expr) -> bool {
    matches!(e, Expr::ScalarFunction(f) if matches!(f.func.name(), "pondra_whole" | "pondra_truncate"))
}

/// A cast to an integer of a value with a fraction, as a cast of that value rounded (or cut toward zero,
/// in Spark's dialect). A value whose type isn't known yet (a parameter) is left as it is.
fn rounded(e: Expr, schema: &DFSchema, truncate: bool) -> Result<Transformed<Expr>> {
    let Some(t) = to_integer(&e) else { return Ok(Transformed::no(e)) };
    let value = match &e {
        Expr::Cast(c) => c.expr.as_ref(),
        Expr::TryCast(c) => c.expr.as_ref(),
        Expr::ScalarFunction(f) => &f.args[0],
        _ => return Ok(Transformed::no(e)),
    };
    if already_cut(value) || !value.get_type(schema).is_ok_and(|v| fractional(&v)) {
        return Ok(Transformed::no(e));
    }
    let f = function(truncate);
    let cut = |v: Expr| Box::new(f.call(vec![v]));
    Ok(Transformed::yes(match e {
        Expr::Cast(c) => Expr::Cast(Cast::new_from_field(cut(*c.expr), c.field)),
        Expr::TryCast(c) => Expr::TryCast(TryCast::new_from_field(cut(*c.expr), c.field)),
        Expr::ScalarFunction(mut f) => Expr::Cast(Cast::new(cut(f.args.swap_remove(0)), t)), // (arrow_cast(x, 'Int64'))
        e => e,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Decimal128Array, DictionaryArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::{Int32Type, Int64Type};
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionConfig;

    #[test]
    fn decimals_round_half_away_from_zero_and_floats_half_to_even() {
        let d: ArrayRef = Arc::new(Decimal128Array::from(vec![Some(25), Some(-25), Some(24), Some(-26), Some(995), None]).with_precision_and_scale(3, 1).unwrap());
        let w = whole(&d).unwrap();
        assert_eq!(w.data_type(), &DataType::Decimal128(4, 1));
        let w: Vec<Option<i128>> = w.as_primitive::<Decimal128Type>().iter().collect();
        assert_eq!(w, vec![Some(30), Some(-30), Some(20), Some(-30), Some(1000), None]);
        let f: ArrayRef = Arc::new(Float64Array::from(vec![2.5, 3.5, -2.5, -3.5, 2.4, 1e300, f64::NAN]));
        let w: Vec<f64> = whole(&f).unwrap().as_primitive::<Float64Type>().values().to_vec();
        assert_eq!(&w[..6], &[2.0, 4.0, -2.0, -4.0, 2.0, 1e300]);
        assert!(w[6].is_nan());
    }

    #[test]
    fn a_decimal_with_no_places_and_an_integer_are_left_alone() {
        assert!(!fractional(&DataType::Decimal128(10, 0)));
        assert!(!fractional(&DataType::Int64));
        assert!(fractional(&DataType::Decimal256(40, 2)));
        assert_eq!(whole_type(&DataType::Decimal128(38, 2)), DataType::Decimal128(38, 2));
    }

    /// A cast of a value that is already `pondra_truncate`d (what `spark_sql('…')` writes) gets no call of its own:
    /// its plan holds one such call, and the answer is the cut one.
    #[tokio::test]
    async fn a_cut_value_gets_no_second_call() {
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let sql = "SELECT CAST(pondra_truncate(2.7) AS INT) AS a";
        let plan = Rounds.analyze(ctx.sql(sql).await.unwrap().into_unoptimized_plan(), ctx.state().config_options()).unwrap();
        let text = plan.display_indent().to_string();
        assert_eq!(text.matches("pondra_truncate").count(), 1, "{text}");
        assert_eq!(text.matches("pondra_whole").count(), 0, "{text}");
        assert_eq!(array_value_to_string(ctx.sql(sql).await.unwrap().collect().await.unwrap()[0].column(0), 0).unwrap(), "2");
    }

    /// `truncated` cuts the fraction toward zero in each type, keeping the decimal's type, NaN and NULL as
    /// they are, and an integer exactly (`i64::MAX` too).
    #[test]
    fn truncation_cuts_toward_zero_in_each_type() {
        let f: ArrayRef = Arc::new(Float64Array::from(vec![Some(2.7), Some(-2.7), Some(2.5), Some(-0.5), Some(f64::NAN), None]));
        let t: Vec<Option<f64>> = truncated(&f).unwrap().as_primitive::<Float64Type>().iter().collect();
        assert_eq!(t[..4], [Some(2.0), Some(-2.0), Some(2.0), Some(0.0)]);
        assert!(t[4].is_some_and(f64::is_nan));
        assert_eq!(t[5], None);
        let g: ArrayRef = Arc::new(Float32Array::from(vec![2.7f32, -2.7]));
        assert_eq!(truncated(&g).unwrap().as_primitive::<Float32Type>().values().to_vec(), vec![2.0f32, -2.0]);
        let d: ArrayRef = Arc::new(Decimal128Array::from(vec![Some(270), Some(-250), Some(-7), None]).with_precision_and_scale(5, 2).unwrap());
        let t = truncated(&d).unwrap();
        assert_eq!(t.data_type(), &DataType::Decimal128(5, 2));
        let t: Vec<Option<i128>> = t.as_primitive::<Decimal128Type>().iter().collect();
        assert_eq!(t, vec![Some(200), Some(-200), Some(0), None]);
        let i: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, -7]));
        assert_eq!(truncated(&i).unwrap().as_primitive::<Int64Type>().values().to_vec(), vec![i64::MAX, -7]);
    }

    /// `into` rounds a fraction going into an integer as a cast does, text casts, and a value that won't go is an
    /// error naming it (one bad value fails the column, so NaN is tested apart).
    #[test]
    fn into_rounds_a_fraction_and_refuses_a_value_that_wont_go() {
        let f: ArrayRef = Arc::new(Float64Array::from(vec![Some(2.5), Some(-2.5), Some(3.5), Some(2.4), None]));
        let i = into(&f, &DataType::Int32).unwrap();
        assert_eq!(i.as_primitive::<Int32Type>().iter().collect::<Vec<_>>(), vec![Some(2), Some(-2), Some(4), Some(2), None]);
        let nan: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
        assert_eq!(refusal(&nan, &DataType::Int32), ("22003", "integer out of range".to_string()));
        let d: ArrayRef = Arc::new(Decimal128Array::from(vec![Some(700250), Some(-250)]).with_precision_and_scale(10, 2).unwrap());
        assert_eq!(into(&d, &DataType::Int64).unwrap().as_primitive::<Int64Type>().values().to_vec(), vec![7003, -3]);
        let s: ArrayRef = Arc::new(StringArray::from(vec!["7"]));
        assert_eq!(into(&s, &DataType::Int64).unwrap().as_primitive::<Int64Type>().values().to_vec(), vec![7]);
        let big: ArrayRef = Arc::new(Int64Array::from(vec![300]));
        assert!(refusal(&big, &DataType::Int8).1.contains("Can't cast value 300 to type Int8"));
    }

    /// The SQLSTATE and words of a refused cast, as a door gets them.
    fn refusal(c: &ArrayRef, t: &DataType) -> (&'static str, String) {
        let e = anyhow::Error::from(into(c, t).expect_err("refused"));
        (crate::codes::of(&e), crate::codes::words(&e))
    }

    /// A text that won't go into an integer, decimal, date or timestamp is refused by its first such value, as Postgres
    /// refuses it, from a StringArray, a Utf8View and a dictionary of text alike (a Hive partition column).
    #[test]
    fn text_that_wont_go_is_refused_as_postgres_refuses_it() {
        let texts: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("1"), Some("99999999999")])),
            Arc::new(StringViewArray::from(vec![Some("1"), Some("99999999999")])),
            Arc::new(DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1]), Arc::new(StringArray::from(vec!["1", "99999999999"]))).unwrap()),
        ];
        for t in &texts {
            assert_eq!(refusal(t, &DataType::Int32), ("22003", "value \"99999999999\" is out of range for type integer".to_string()));
        }
        let small: ArrayRef = Arc::new(StringArray::from(vec![Some("40000")]));
        assert_eq!(refusal(&small, &DataType::Int16), ("22003", "value \"40000\" is out of range for type smallint".to_string()));
        let wide: ArrayRef = Arc::new(StringArray::from(vec![Some("99999999999999999999")]));
        assert_eq!(refusal(&wide, &DataType::Int64), ("22003", "value \"99999999999999999999\" is out of range for type bigint".to_string()));
        let abc: ArrayRef = Arc::new(StringArray::from(vec![Some("12"), Some("abc"), Some("99999999999")]));
        assert_eq!(refusal(&abc, &DataType::Int32), ("22P02", "invalid input syntax for type integer: \"abc\"".to_string()), "the first refused text decides");
        assert_eq!(into(&abc.slice(0, 1), &DataType::Int32).unwrap().as_primitive::<Int32Type>().value(0), 12, "text that goes is unchanged");
        let dec: ArrayRef = Arc::new(StringArray::from(vec![Some("12345")]));
        assert_eq!(refusal(&dec, &DataType::Decimal128(3, 1)), ("22003", "numeric field overflow".to_string()));
        let rounded: ArrayRef = Arc::new(StringArray::from(vec![Some("99.95")]));
        assert_eq!(refusal(&rounded, &DataType::Decimal128(3, 1)), ("22003", "numeric field overflow".to_string()), "99.95 rounds to 100.0");
        for number in ["1e5", "inf", " -1E9 "] {
            let c: ArrayRef = Arc::new(StringArray::from(vec![Some(number)]));
            assert_eq!(refusal(&c, &DataType::Decimal128(3, 1)), ("22003", "numeric field overflow".to_string()), "{number}: a number Rust reads");
        }
        let nan: ArrayRef = Arc::new(StringArray::from(vec![Some("1.5"), Some(" nan ")]));
        let (code, words) = refusal(&nan, &DataType::Decimal128(3, 1));
        assert_eq!(code, "0A000", "NaN into a decimal is refused by name: Postgres keeps NaN in numeric");
        assert!(words.contains("NaN"), "{words}");
        let float_nan: ArrayRef = Arc::new(Float64Array::from(vec![1.5, f64::NAN]));
        assert_eq!(refusal(&float_nan, &DataType::Decimal128(3, 1)).0, "0A000", "a float NaN too");
        assert_eq!(refusal(&float_nan, &DataType::Int64).0, "22003", "a NaN into an integer is still out of range");
        let junk: ArrayRef = Arc::new(StringArray::from(vec![Some("12abc")]));
        assert_eq!(refusal(&junk, &DataType::Decimal128(3, 1)).0, "22P02", "text that Rust doesn't read as a number is 22P02");
        let word: ArrayRef = Arc::new(StringArray::from(vec![Some("abc")]));
        assert_eq!(refusal(&word, &DataType::Decimal128(3, 1)), ("22P02", "invalid input syntax for type numeric: \"abc\"".to_string()));
        assert_eq!(refusal(&word, &DataType::Date32), ("22P02", "invalid input syntax for type date: \"abc\"".to_string()));
        let at = DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None);
        assert_eq!(refusal(&word, &at), ("22P02", "invalid input syntax for type timestamp: \"abc\"".to_string()));
        assert_eq!(refusal(&word, &DataType::Int8).0, "22P02", "a type with no name here keeps Arrow's words");
    }

    /// A number that won't go into an integer type is 22003 with Postgres's range words (NaN and the infinities too);
    /// into a decimal it is `numeric field overflow`; into Int8 or an unsigned type Arrow's words, as 22003.
    #[test]
    fn numbers_that_wont_go_are_22003_with_postgres_words() {
        let int: ArrayRef = Arc::new(Int64Array::from(vec![99999999999]));
        assert_eq!(refusal(&int, &DataType::Int32), ("22003", "integer out of range".to_string()));
        let float: ArrayRef = Arc::new(Float64Array::from(vec![1e20]));
        assert_eq!(refusal(&float, &DataType::Int64), ("22003", "bigint out of range".to_string()));
        let nan: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
        assert_eq!(refusal(&nan, &DataType::Int64), ("22003", "bigint out of range".to_string()));
        let inf: ArrayRef = Arc::new(Float64Array::from(vec![f64::INFINITY]));
        assert_eq!(refusal(&inf, &DataType::Int64), ("22003", "bigint out of range".to_string()));
        let small: ArrayRef = Arc::new(Int64Array::from(vec![40000]));
        assert_eq!(refusal(&small, &DataType::Int16), ("22003", "smallint out of range".to_string()));
        assert_eq!(refusal(&int, &DataType::Decimal128(3, 1)), ("22003", "numeric field overflow".to_string()));
        assert_eq!(refusal(&int, &DataType::UInt8).0, "22003");
        assert!(refusal(&int, &DataType::UInt8).1.contains("Can't cast value 99999999999 to type UInt8"));
    }

    /// A value the CSV reader refuses as its column's type is refused as the cast refuses it, from the reader's text: Postgres's
    /// words and SQLSTATE for an integer that won't parse or overflows; None for a value the cast takes, and for other text.
    #[test]
    fn a_value_the_csv_reader_refuses_is_refused_as_postgres_refuses_it() {
        let reader = |v: &str, t: &str| format!("Arrow error: Parser error: Error while parsing value '{v}' as type '{t}' for column 1 at line 2. Row data: '[7,{v}]'");
        let words = |text: &str| parse_refusal(text).map(|e| {
            let e = anyhow::Error::from(e);
            (crate::codes::of(&e), crate::codes::words(&e))
        });
        assert_eq!(words(&reader("2.5", "Int32")), Some(("22P02", "invalid input syntax for type integer: \"2.5\"".to_string())));
        // (read from a file: the place it is given, as the caller writes the file and the line)
        let located = |text: &str| parse_refusal_in(text, "s3://b/x.csv, line 3").map(|e| {
            let e = anyhow::Error::from(e);
            (crate::codes::of(&e), crate::codes::words(&e))
        });
        assert_eq!(located(&reader("2.5", "Int32")), Some(("22P02", "invalid input syntax for type integer: \"2.5\" (s3://b/x.csv, line 3)".to_string())));
        assert_eq!(located(&reader("99999999999", "Int32")), Some(("22003", "value \"99999999999\" is out of range for type integer (s3://b/x.csv, line 3)".to_string())));
        assert_eq!(words(&reader("1e3", "Int64")), Some(("22P02", "invalid input syntax for type bigint: \"1e3\"".to_string())));
        assert_eq!(words(&reader("99999999999", "Int32")), Some(("22003", "value \"99999999999\" is out of range for type integer".to_string())));
        assert_eq!(words(&reader("yes", "Boolean")), None, "a boolean the cast takes");
        assert_eq!(words("Division by zero"), None);
    }

    /// The column and the text of the value a CSV reader refused, from its error: a value may span lines.
    #[test]
    fn the_cell_a_csv_reader_refused_is_its_column_and_text() {
        let reader = |v: &str| format!("Arrow error: Parser error: Error while parsing value '{v}' as type 'Int32' for column 1 at line 9999. Row data: '[7,{v}]'");
        assert_eq!(refused_cell(&reader("2.5")), Some((1, "2.5".to_string())));
        assert_eq!(refused_cell(&reader("a\nb")), Some((1, "a\nb".to_string())));
        assert_eq!(refused_cell("Division by zero"), None);
    }

    /// Spark's dialect cuts a cast's fraction, Pondra's SQL rounds it, in the same session type: `SET` names the
    /// dialect as a client does.
    #[tokio::test]
    async fn spark_s_dialect_cuts_a_fraction_and_pondra_s_rounds_it() {
        let sql = "SELECT CAST(2.7 AS INT), CAST(-2.7 AS BIGINT), CAST(2.5 AS INT)";
        let plain = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let spark = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        spark.sql("SET datafusion.sql_parser.dialect = 'spark'").await.unwrap().collect().await.unwrap();
        for (ctx, want) in [(&plain, ["3", "-3", "3"]), (&spark, ["2", "-2", "2"])] {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let got: Vec<String> = (0..3).map(|i| array_value_to_string(batches[0].column(i), 0).unwrap()).collect();
            assert_eq!(got, want, "{sql}");
        }
    }
}
