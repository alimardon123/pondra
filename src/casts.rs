//! A cast of a decimal or a float to an integer rounds, as Postgres and DuckDB round it, where Arrow's
//! cast drops the fraction: `CAST(2.5 AS INT)` is 3 (a decimal rounds half away from zero) and
//! `CAST(2.5::DOUBLE AS INT)` is 2 (a float rounds half to even, as C's `rint`: 3.5 is 4). A query's
//! casts take `pondra_whole` of their value (`Rounds`, an analyzer rule after type coercion), and a
//! value written into an integer column is rounded the same way (`query::strict`).
//!
//! In Spark's dialect (`datafusion.sql_parser.dialect = 'spark'`, and `spark_sql('…')`, whose casts Pondra's
//! SQL turns into `pondra_truncate`) the fraction is cut toward zero instead, as Spark and Polars cast:
//! `CAST(2.7 AS INT)` is 2 and `CAST(-2.7 AS INT)` is -2 (`Rounds` picks it by the dialect).
use datafusion::arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray, PrimitiveArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type, DecimalType, Float32Type, Float64Type};
use datafusion::common::config::{ConfigOptions, Dialect};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr::{Cast, TryCast};
use datafusion::logical_expr::{ColumnarValue, Expr, ExprSchemable, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::SessionContext;
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
    use datafusion::arrow::array::{Decimal128Array, Float32Array, Float64Array, Int64Array};
    use datafusion::arrow::datatypes::Int64Type;
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

    /// `truncated` cuts the fraction toward zero in each type, keeping the decimal's type, NaN and NULL as
    /// they are, and an integer exactly (`i64::MAX` too).
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
