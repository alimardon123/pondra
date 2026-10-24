//! A product of two DECIMALs runs in native integers when its values are small. Arrow multiplies them as
//! checked i128s, a call to compiler-rt's `__muloti4` on wasm32 (the browser page), for every row; when
//! both values fit an i32 their product fits an i64 and needs no check. `Products` puts `pondra_multiply(a, b)`
//! for each `a * b` of two DECIMALs (the same type and nullability, the same answers and overflow errors).
use crate::optimize::rewrite_typed;
use datafusion::arrow::array::{new_null_array, ArrowNativeTypeOp, AsArray, PrimitiveArray};
use datafusion::arrow::compute::{try_binary, try_unary};
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{exec_err, plan_err, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::sync::{Arc, LazyLock};

/// The name of the product's function (EXPLAIN shows it, and the cost rule counts it).
pub(crate) const NAME: &str = "pondra_multiply";

/// The product of two DECIMAL values, their i128s at their scales: arrow's checked multiply, unless both
/// fit an i32 (then the i64 product is exact, and the check is skipped).
fn times(a: i128, b: i128) -> Result<i128, ArrowError> {
    if a as i32 as i128 == a && b as i32 as i128 == b {
        Ok((a as i64 * b as i64) as i128)
    } else {
        a.mul_checked(b) // (arrow's own check and error, left operand first)
    }
}

/// `a * b` of two DECIMAL(p1, s1) and DECIMAL(p2, s2): arrow's type, DECIMAL(p1 + p2 + 1 at most 38, s1 + s2).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Multiply {
    signature: Signature,
}

impl ScalarUDFImpl for Multiply {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)] => {
                let scale = s1.saturating_add(*s2); // (arrow refuses a scale past 38, as this does)
                if scale > 38 {
                    return plan_err!("{NAME}: the product's scale would be {scale}, past 38");
                }
                Ok(DataType::Decimal128((p1 + p2 + 1).min(38), scale))
            }
            _ => plan_err!("{NAME} multiplies two DECIMALs, not {args:?}"),
        }
    }

    // (a product of two NOT NULL values is NOT NULL, as `*` is; DataFusion's default calls every result nullable)
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(NAME, self.return_type(&types)?, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let out = args.return_field.data_type().clone();
        let DataType::Decimal128(p, s) = out else { return exec_err!("{NAME} makes a DECIMAL, not {out}") };
        let [left, right] = &args.args[..] else { return exec_err!("{NAME} takes two values") };
        Ok(match (left, right) {
            (ColumnarValue::Array(a), ColumnarValue::Array(b)) => ColumnarValue::Array(Arc::new(times_of(a.as_primitive(), b.as_primitive(), p, s)?)),
            (ColumnarValue::Array(a), ColumnarValue::Scalar(b)) => match value(b)? {
                Some(b) => ColumnarValue::Array(Arc::new(unary(a.as_primitive(), |x| times(x, b), p, s)?)),
                None => ColumnarValue::Array(new_null_array(&out, a.len())), // (a NULL multiplies to NULLs)
            },
            (ColumnarValue::Scalar(a), ColumnarValue::Array(b)) => match value(a)? {
                Some(a) => ColumnarValue::Array(Arc::new(unary(b.as_primitive(), |x| times(a, x), p, s)?)), // (the operands' order: the error names the left one first)
                None => ColumnarValue::Array(new_null_array(&out, b.len())),
            },
            (ColumnarValue::Scalar(a), ColumnarValue::Scalar(b)) => {
                let (a, b) = (a.to_array()?, b.to_array()?); // (one row each, as a column of them is)
                let v = times_of(a.as_primitive(), b.as_primitive(), p, s)?;
                ColumnarValue::Scalar(ScalarValue::try_from_array(&v, 0)?)
            }
        })
    }
}

/// A DECIMAL scalar's value, `None` for NULL.
fn value(v: &ScalarValue) -> Result<Option<i128>> {
    match v {
        ScalarValue::Decimal128(v, _, _) => Ok(*v),
        other => exec_err!("{NAME} takes DECIMAL values, not {}", other.data_type()),
    }
}

/// `times` of two columns, each row a NULL where either is NULL (arrow's `try_binary` skips those rows), at
/// the result's precision and scale.
fn times_of(a: &PrimitiveArray<Decimal128Type>, b: &PrimitiveArray<Decimal128Type>, p: u8, s: i8) -> Result<PrimitiveArray<Decimal128Type>, ArrowError> {
    let v: PrimitiveArray<Decimal128Type> = try_binary(a, b, times)?;
    v.with_precision_and_scale(p, s)
}

/// `op` on each value of a column (a NULL stays NULL), at the result's precision and scale.
fn unary(a: &PrimitiveArray<Decimal128Type>, op: impl Fn(i128) -> Result<i128, ArrowError>, p: u8, s: i8) -> Result<PrimitiveArray<Decimal128Type>, ArrowError> {
    let v: PrimitiveArray<Decimal128Type> = try_unary(a, op)?;
    v.with_precision_and_scale(p, s)
}

/// One product's function, one definition for every lake. Not registered in any session: no SQL names it, and
/// a name would be a function users could call. `Products` puts it in the plans that need it.
static MULTIPLY: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(Multiply { signature: Signature::any(2, Volatility::Immutable) })));

/// `a * b` of two DECIMALs as `pondra_multiply(a, b)`, wherever a plan has one (after `ScaledLiterals`, which
/// writes a literal at its column's scale first). Only where the call has the product's own type and
/// nullability: an expression that would change type is left as it was.
#[derive(Debug)]
pub struct Products;

impl OptimizerRule for Products {
    fn name(&self) -> &str {
        "pondra_decimal_products"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        rewrite_typed(plan, is_product, product)
    }
}

fn is_product(e: &Expr) -> bool {
    matches!(e, Expr::BinaryExpr(b) if b.op == Operator::Multiply)
}

fn product(e: &Expr, schema: &DFSchema) -> Option<Expr> {
    let Expr::BinaryExpr(b) = e else { return None };
    let decimal = |x: &Expr| matches!(x.get_type(schema), Ok(DataType::Decimal128(..)));
    if b.op != Operator::Multiply || !decimal(&b.left) || !decimal(&b.right) {
        return None;
    }
    let call = MULTIPLY.call(vec![b.left.as_ref().clone(), b.right.as_ref().clone()]);
    let (_, was) = e.to_field(schema).ok()?;
    let (_, now) = call.to_field(schema).ok()?;
    (now.data_type() == was.data_type() && now.is_nullable() == was.is_nullable()).then_some(call) // (checked, not assumed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_values_multiply_as_arrow_does() {
        assert_eq!(times(3, 4).unwrap(), 12);
        assert_eq!(times(-7, 9).unwrap(), -63);
        assert_eq!(times(0, 123_456_789).unwrap(), 0);
        assert_eq!(times(i32::MIN as i128, i32::MIN as i128).unwrap(), 1i128 << 62);
        assert_eq!(times(i32::MAX as i128, i32::MIN as i128).unwrap(), -(i32::MAX as i128) * 2_147_483_648);
    }

    #[test]
    fn products_past_i32_and_past_i64_are_exact() {
        assert_eq!(times(3_000_000_000, 3_000_000_000).unwrap(), 9_000_000_000_000_000_000); // (past i32, still within i64)
        assert_eq!(times(4_000_000_000, 4_000_000_000).unwrap(), 16_000_000_000_000_000_000); // (past i64, within i128)
        assert_eq!(times(4_000_000_000_000_000_000_000, 3).unwrap(), 12_000_000_000_000_000_000_000);
    }

    #[test]
    fn overflow_is_arrows_error_for_the_same_operands() {
        let err = times(i128::MAX, 2).unwrap_err().to_string();
        assert_eq!(err, i128::MAX.mul_checked(2).unwrap_err().to_string());
        assert!(err.contains("Overflow happened on: 170141183460469231731687303715884105727 * 2"), "{err}");
    }

    #[test]
    fn negatives_on_both_paths() {
        assert_eq!(times(-3, 5).unwrap(), -15);
        assert_eq!(times(-3, -5).unwrap(), 15);
        assert_eq!(times(i32::MIN as i128, -1).unwrap(), 2_147_483_648);
        assert_eq!(times(-3_000_000_000, 3_000_000_000).unwrap(), -9_000_000_000_000_000_000);
        assert_eq!(times(-4_000_000_000, 4_000_000_000).unwrap(), -16_000_000_000_000_000_000);
    }
}
