//! A cast of a decimal or a float to an integer rounds, as Postgres and DuckDB round it, where Arrow's
//! cast drops the fraction: `CAST(2.5 AS INT)` is 3 (a decimal rounds half away from zero) and
//! `CAST(2.5::DOUBLE AS INT)` is 2 (a float rounds half to even, as C's `rint`: 3.5 is 4). A query's
//! casts take `pondra_whole` of their value (`Rounds`, an analyzer rule after type coercion), and a
//! value written into an integer column is rounded the same way (`query::strict`).
use datafusion::arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray, PrimitiveArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type, DecimalType, Float32Type, Float64Type};
use datafusion::common::config::ConfigOptions;
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
        DataType::Decimal32(..) => Arc::new(decimal::<Decimal32Type>(c.as_primitive())?),
        DataType::Decimal64(..) => Arc::new(decimal::<Decimal64Type>(c.as_primitive())?),
        DataType::Decimal128(..) => Arc::new(decimal::<Decimal128Type>(c.as_primitive())?),
        DataType::Decimal256(..) => Arc::new(decimal::<Decimal256Type>(c.as_primitive())?),
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

/// A decimal column's values rounded half away from zero at the decimal point, in their own scale.
fn decimal<T: DecimalType>(a: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>> {
    let s = match a.data_type() {
        DataType::Decimal32(_, s) | DataType::Decimal64(_, s) | DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => *s,
        _ => 0,
    };
    if s <= 0 {
        return Ok(a.clone());
    }
    let one = T::Native::usize_as(10).pow_wrapping(s as u32); // (1 in the column's scale)
    let half = one.div_wrapping(T::Native::usize_as(2));
    let rounded = a.unary::<_, T>(|v| {
        let fraction = v.mod_wrapping(one); // (with the value's sign)
        let cut = v.sub_wrapping(fraction);
        if fraction.is_ge(half) {
            cut.add_wrapping(one)
        } else if fraction.is_le(half.neg_wrapping()) {
            cut.sub_wrapping(one)
        } else {
            cut
        }
    });
    Ok(rounded.with_data_type(whole_type(a.data_type())))
}

/// `pondra_whole(x)`: `whole` as a function, what `Rounds` puts in a cast to an integer.
static WHOLE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(Whole(Signature::any(1, Volatility::Immutable)))));

#[derive(Debug, PartialEq, Eq, Hash)]
struct Whole(Signature);

impl ScalarUDFImpl for Whole {
    fn name(&self) -> &str {
        "pondra_whole"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(whole_type(&args[0]))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(match &args.args[0] {
            ColumnarValue::Array(a) => ColumnarValue::Array(whole(a)?),
            ColumnarValue::Scalar(v) => ColumnarValue::Scalar(ScalarValue::try_from_array(&whole(&v.to_array()?)?, 0)?),
        })
    }
}

/// Registers `pondra_whole` in a session (`store.rs`), for plans that name it.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(WHOLE.as_ref().clone());
}

/// Puts `pondra_whole` in every cast (and `TRY_CAST`) of a decimal with places or a float to an
/// integer. It runs after type coercion, so the casts coercion adds round too. `arrow_cast(x,
/// 'Int64')` becomes the cast it stands for here (the optimizer's simplifier would make it one later,
/// past this rule).
#[derive(Debug)]
pub struct Rounds;

impl AnalyzerRule for Rounds {
    fn name(&self) -> &str {
        "pondra_casts_round"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
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
                let e = e.transform_up(|e| rounded(e, &schema))?;
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

/// A cast to an integer of a value with a fraction, as a cast of that value rounded. A value whose
/// type isn't known yet (a parameter) is left as it is.
fn rounded(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Some(t) = to_integer(&e) else { return Ok(Transformed::no(e)) };
    let value = match &e {
        Expr::Cast(c) => c.expr.as_ref(),
        Expr::TryCast(c) => c.expr.as_ref(),
        Expr::ScalarFunction(f) => &f.args[0],
        _ => return Ok(Transformed::no(e)),
    };
    if !value.get_type(schema).is_ok_and(|v| fractional(&v)) {
        return Ok(Transformed::no(e));
    }
    let whole = |v: Expr| Box::new(WHOLE.call(vec![v]));
    Ok(Transformed::yes(match e {
        Expr::Cast(c) => Expr::Cast(Cast::new_from_field(whole(*c.expr), c.field)),
        Expr::TryCast(c) => Expr::TryCast(TryCast::new_from_field(whole(*c.expr), c.field)),
        Expr::ScalarFunction(mut f) => Expr::Cast(Cast::new(whole(f.args.swap_remove(0)), t)), // (arrow_cast(x, 'Int64'))
        e => e,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Decimal128Array, Float64Array};

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
}
