//! A quotient of two DECIMALs keeps at least 16 significant digits and rounds its last place, as Postgres's does.
//! Arrow's division (so DataFusion's) gives DECIMAL(p1, s1) / DECIMAL(p2, s2) four places more than the dividend has,
//! s1 + 4, and cuts the rest off: `SELECT 71 / 3.0` was 23.6666 (Postgres 23.6666666666666667, DuckDB's DOUBLE
//! 23.666666666666668), and TPC-DS q83's shares, three divisions deep, went wrong from their ninth digit
//! (12.676056333300 for 12.676056338028…).
//!
//! `Quotients`, an analyzer rule before type coercion, puts `pondra_divide(a, b)` for each `a / b` that DataFusion divides
//! as two DECIMAL128s (an integer meeting a DECIMAL is one: a BIGINT is a DECIMAL(20, 0) there), so whatever reads the
//! quotient is typed from its own type. Its places (`quotient_type`): Postgres's `select_div_scale` picks enough for 16
//! significant digits from the values' sizes, and a column's type is fixed before them, so here they are the 16 Postgres
//! gives a quotient from 1 to 9,999, or either side's places where that is more. Within 38 digits they give way to the
//! whole digits a quotient can have (p1 - s1 + s2), but never below the s1 + 4 Arrow gave. In Spark's dialect a quotient
//! has Spark's type: max(6, s1 + p2 + 1) places, giving way down to 6. Either way the last place is rounded half away
//! from zero, as Postgres's and Spark's are.
//!
//! Each row is worked out in i64s or i128s, and in i256s where the scaled dividend doesn't fit, so every quotient is the
//! exact one rounded: one past its precision is refused as a cast past it is (22003), a zero divisor as `/` refuses it
//! (22012). `SAFE_DIVIDE` of two DECIMALs answers with the same type and digits, NULL for those two (`dialect_fns`).
use datafusion::arrow::array::{new_null_array, ArrowNativeTypeOp, AsArray, PrimitiveArray};
use datafusion::arrow::compute::{try_binary, try_unary};
use datafusion::arrow::datatypes::{i256, DataType, Decimal128Type, DecimalType, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::config::{ConfigOptions, Dialect};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{exec_err, plan_err, DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::{ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::optimizer::analyzer::AnalyzerRule;
use std::sync::{Arc, LazyLock};

/// The name of the quotient's function (EXPLAIN shows it, and the cost rule counts it).
pub(crate) const NAME: &str = "pondra_divide";

/// The type of a quotient of a DECIMAL128 `left` by a DECIMAL128 `right`, by Postgres's rule or Spark's (above). None for
/// any other types, and for a negative scale (Arrow's own division then).
pub(crate) fn quotient_type(left: &DataType, right: &DataType, spark: bool) -> Option<DataType> {
    let (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) = (left, right) else { return None };
    let (p1, s1, p2, s2) = (i32::from(*p1), i32::from(*s1), i32::from(*p2), i32::from(*s2));
    if s1 < 0 || s2 < 0 || s1 > p1 || s2 > p2 {
        return None;
    }
    let whole = p1 - s1 + s2; // (the most whole digits a quotient has: the dividend's, and one for each place of the divisor)
    let places = if spark {
        let s = (s1 + p2 + 1).max(6);
        if whole + s <= 38 { s } else { (38 - whole).max(s.min(6)) } // (Spark's `adjustPrecisionScale`)
    } else {
        let wanted = 16.max(s1).max(s2);
        wanted.min(38 - whole).max((s1 + 4).min(wanted))
    }
    .min(38);
    Some(DataType::Decimal128((whole + places).clamp(1, 38) as u8, places as i8))
}

/// How a quotient at scale `s` is worked out from a dividend at scale s1 and a divisor at s2: (a · 10^up) / (b · 10^down),
/// rounded, where up − down = s − s1 + s2. (`avg.rs` divides a sum by a count with it.)
#[derive(Debug)]
pub(crate) struct Scaling {
    up: i256,
    down: i256,
    up128: Option<i128>,
    down128: Option<i128>,
    precision: u8,
    scale: i8,
}

impl Scaling {
    pub(crate) fn new(s1: i8, s2: i8, precision: u8, scale: i8) -> Result<Scaling, ArrowError> {
        let k = i32::from(scale) - i32::from(s1) + i32::from(s2);
        let ten = |n: i32| i256::from_i128(10).pow_checked(n as u32);
        let (up, down) = (ten(k.max(0))?, ten((-k).max(0))?);
        Ok(Scaling { up, down, up128: up.to_i128(), down128: down.to_i128(), precision, scale })
    }

    /// `a / b`, two values at their scales, at the quotient's scale.
    pub(crate) fn quotient(&self, a: i128, b: i128) -> Result<i128, ArrowError> {
        if b == 0 {
            return Err(ArrowError::DivideByZero);
        }
        let narrow = match (self.up128, self.down128) {
            (Some(up), Some(down)) => a.checked_mul(up).zip(b.checked_mul(down)),
            _ => None,
        };
        let q = match narrow {
            Some((n, d)) => match (i64::try_from(n), i64::try_from(d)) {
                (Ok(n), Ok(d)) if n != i64::MIN && d != i64::MIN => i128::from(rounded(n, d)?), // (a hardware division, where compiler-rt's i128 one is ~4× slower)
                _ => rounded(n, d)?,
            },
            None => {
                let (n, d) = (i256::from_i128(a).mul_checked(self.up)?, i256::from_i128(b).mul_checked(self.down)?);
                rounded(n, d)?.to_i128().ok_or_else(|| ArrowError::ArithmeticOverflow(format!("Overflow happened on: {a} / {b}")))?
            }
        };
        Decimal128Type::validate_decimal_precision(q, self.precision, self.scale)?; // (Arrow's words, as a cast past the precision has them)
        Ok(q)
    }
}

/// `n / d` rounded half away from zero (`d` isn't 0).
fn rounded<T: ArrowNativeTypeOp>(n: T, d: T) -> Result<T, ArrowError> {
    let q = n.div_checked(d)?;
    let r = n.sub_wrapping(q.mul_wrapping(d)); // (the remainder, without a second division)
    let abs = |x: T| if x.is_lt(T::ZERO) { x.neg_wrapping() } else { x };
    // (half or more of the divisor left over: compared, not doubled, so it can't overflow)
    if abs(r).is_ge(abs(d).sub_wrapping(abs(r))) {
        return Ok(if n.is_lt(T::ZERO) != d.is_lt(T::ZERO) { q.sub_wrapping(T::ONE) } else { q.add_wrapping(T::ONE) });
    }
    Ok(q)
}

/// `a / b` of two DECIMAL128 values or columns into the type `to` (`quotient_type`'s). `safe`: a zero divisor or a quotient
/// that doesn't fit is a NULL, as `SAFE_DIVIDE` answers, not the statement's error.
pub(crate) fn divided(a: &ColumnarValue, b: &ColumnarValue, rows: usize, to: &DataType, safe: bool) -> Result<ColumnarValue> {
    let (DataType::Decimal128(_, s1), DataType::Decimal128(_, s2), DataType::Decimal128(p, s)) = (a.data_type(), b.data_type(), to) else {
        return exec_err!("{NAME} divides two DECIMALs into one, not {} / {} into {to}", a.data_type(), b.data_type());
    };
    let how = Scaling::new(s1, s2, *p, *s)?;
    let one = |v: &ScalarValue| match v {
        ScalarValue::Decimal128(v, _, _) => Ok(*v),
        other => exec_err!("{NAME} takes DECIMAL values, not {}", other.data_type()),
    };
    if let (ColumnarValue::Scalar(x), ColumnarValue::Scalar(y)) = (a, b) {
        let q = match (one(x)?, one(y)?) {
            (Some(x), Some(y)) => match how.quotient(x, y) {
                Ok(q) => Some(q),
                Err(_) if safe => None,
                Err(e) => return Err(e.into()),
            },
            _ => None,
        };
        return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(q, *p, *s)));
    }
    let q: PrimitiveArray<Decimal128Type> = match (a, b) {
        (ColumnarValue::Array(x), ColumnarValue::Scalar(y)) if !safe => match one(y)? {
            Some(y) => try_unary(x.as_primitive::<Decimal128Type>(), |x| how.quotient(x, y))?, // (`col / 3.0`: no column of threes)
            None => return Ok(ColumnarValue::Array(new_null_array(to, x.len()))),
        },
        _ => {
            let (x, y) = (a.to_array(rows)?, b.to_array(rows)?);
            let (x, y) = (x.as_primitive::<Decimal128Type>(), y.as_primitive::<Decimal128Type>());
            match safe {
                false => try_binary(x, y, |x, y| how.quotient(x, y))?,
                true => x.iter().zip(y.iter()).map(|(x, y)| how.quotient(x?, y?).ok()).collect(),
            }
        }
    };
    Ok(ColumnarValue::Array(Arc::new(q.with_precision_and_scale(*p, *s)?)))
}

/// `a / b` of two DECIMALs: Postgres's places, or Spark's (`spark`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Divide {
    signature: Signature,
    spark: bool,
}

impl ScalarUDFImpl for Divide {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    // (named as the `/` it stands for, so a column keeps its name with no alias over it)
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        match args {
            [a, b] => Ok(format!("{} / {}", a.schema_name(), b.schema_name())),
            _ => plan_err!("{NAME} takes two values"),
        }
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [a, b] => match quotient_type(a, b, self.spark) {
                Some(t) => Ok(t),
                None => plan_err!("{NAME} divides two DECIMALs, not {a} / {b}"),
            },
            _ => plan_err!("{NAME} takes two values"),
        }
    }

    // (a quotient of two NOT NULL values is NOT NULL, as `/`'s is; DataFusion's default calls every result nullable)
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(NAME, self.return_type(&types)?, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [a, b] = &args.args[..] else { return exec_err!("{NAME} takes two values") };
        divided(a, b, args.number_rows, args.return_field.data_type(), false)
    }
}

/// The quotient's function by Postgres's places and by Spark's, one definition each for every lake. Not registered in any
/// session: no SQL names it, and a name would be a function users could call. `Quotients` puts it in the plans that need it.
static DIVIDE: LazyLock<[Arc<ScalarUDF>; 2]> = LazyLock::new(|| [false, true].map(|spark| Arc::new(ScalarUDF::new_from_impl(Divide { signature: Signature::any(2, Volatility::Immutable), spark }))));

/// `e` as `pondra_divide(a, b)` where it is an `a / b` that DataFusion divides as two DECIMAL128s, its operands cast to the
/// types they would be divided as. Anything else, and a side whose type isn't known yet (a parameter's), stays as it is.
fn quotient(e: Expr, schema: &DFSchema, spark: bool) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(b) = &e else { return Ok(Transformed::no(e)) };
    if b.op != Operator::Divide {
        return Ok(Transformed::no(e));
    }
    let (Ok(l), Ok(r)) = (b.left.get_type(schema), b.right.get_type(schema)) else { return Ok(Transformed::no(e)) };
    if l.is_null() || r.is_null() {
        return Ok(Transformed::no(e)); // (a NULL, or a parameter not typed yet: no decimal to divide)
    }
    let Ok((lt, rt)) = BinaryTypeCoercer::new(&l, &Operator::Divide, &r).get_input_types() else { return Ok(Transformed::no(e)) };
    if quotient_type(&lt, &rt, spark).is_none() {
        return Ok(Transformed::no(e));
    }
    let Expr::BinaryExpr(b) = e else { unreachable!("matched above") };
    let (left, right) = ((*b.left).cast_to(&lt, schema)?, (*b.right).cast_to(&rt, schema)?);
    Ok(Transformed::yes(DIVIDE[usize::from(spark)].call(vec![left, right])))
}

/// `quotient` for every division of an expression planned with no analyzer of its own (a table's CHECK), before its types
/// are coerced, so it divides as a query's does.
pub(crate) fn alone(e: Expr, schema: &DFSchema, options: &ConfigOptions) -> Result<Expr> {
    let spark = spark(options);
    e.transform_up(|e| quotient(e, schema, spark)).data()
}

fn spark(options: &ConfigOptions) -> bool {
    matches!(options.sql_parser.dialect, Dialect::Spark | Dialect::Databricks)
}

/// Every DECIMAL division of a plan as `pondra_divide` (above). Before type coercion, which then types what reads each
/// quotient from the quotient's own type.
#[derive(Debug)]
pub struct Quotients;

impl AnalyzerRule for Quotients {
    fn name(&self) -> &str {
        "pondra_decimal_quotients"
    }

    fn analyze(&self, plan: LogicalPlan, options: &ConfigOptions) -> Result<LogicalPlan> {
        let spark = spark(options);
        plan.transform_up_with_subqueries(|plan| {
            let mut has = false;
            plan.apply_expressions(|e| {
                has = e.exists(|e| Ok(matches!(e, Expr::BinaryExpr(b) if b.op == Operator::Divide)))?;
                Ok(if has { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
            })?;
            if !has {
                return Ok(Transformed::no(plan)); // (nearly every step: no division in it)
            }
            let mut schema = datafusion::logical_expr::utils::merge_schema(&plan.inputs());
            if let LogicalPlan::TableScan(ts) = &plan {
                schema.merge(&DFSchema::try_from_qualified_schema(ts.table_name.clone(), &ts.source.schema())?); // (its filters name its own columns)
            }
            let names = NamePreserver::new(&plan); // (a column keeps the name its expression gave it)
            let t = plan.map_expressions(|e| {
                let name = names.save(&e);
                Ok(e.transform_up(|e| quotient(e, &schema, spark))?.update_data(|e| name.restore(e)))
            })?;
            if t.transformed { t.map_data(crate::optimize::settled) } else { Ok(t) } // (a node above reads the types its columns have now)
        })
        .data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(p: u8, s: i8) -> DataType {
        DataType::Decimal128(p, s)
    }

    #[test]
    fn a_quotient_has_postgres_places_within_38_digits() {
        let pg = |l: DataType, r: DataType| quotient_type(&l, &r, false);
        assert_eq!(pg(decimal(20, 0), decimal(2, 1)), Some(decimal(37, 16)), "71 / 3.0: a BIGINT by a DECIMAL(2, 1)");
        assert_eq!(pg(decimal(2, 1), decimal(20, 0)), Some(decimal(17, 16)), "1.0 / 3");
        assert_eq!(pg(decimal(20, 18), decimal(5, 0)), Some(decimal(20, 18)), "either side's places, when more");
        assert_eq!(pg(decimal(30, 18), decimal(10, 2)), Some(decimal(32, 18)), "the dividend's 18 places");
        assert_eq!(pg(decimal(25, 2), decimal(25, 2)), Some(decimal(38, 13)), "sum / sum: the places give way to whole digits");
        assert_eq!(pg(decimal(38, 10), decimal(38, 10)), Some(decimal(38, 14)), "never fewer than Arrow's s1 + 4");
        assert_eq!(pg(decimal(38, 30), decimal(5, 0)), Some(decimal(38, 30)), "nor fewer than the dividend's own");
        assert_eq!(pg(decimal(38, 0), decimal(38, 38)), Some(decimal(38, 4)), "Arrow's s1 + 4 where nothing fits");
        assert_eq!(pg(decimal(10, -2), decimal(5, 0)), None, "a negative scale: Arrow's own division");
        assert_eq!(pg(DataType::Int64, decimal(5, 0)), None);
    }

    #[test]
    fn in_sparks_dialect_a_quotient_has_sparks_type() {
        let spark = |l: DataType, r: DataType| quotient_type(&l, &r, true);
        assert_eq!(spark(decimal(20, 0), decimal(2, 1)), Some(decimal(27, 6)), "max(6, s1 + p2 + 1) places");
        assert_eq!(spark(decimal(10, 2), decimal(10, 2)), Some(decimal(23, 13)));
        assert_eq!(spark(decimal(38, 10), decimal(38, 10)), Some(decimal(38, 6)), "down to 6 places, to keep the whole digits");
        assert_eq!(spark(decimal(30, 2), decimal(10, 4)), Some(decimal(38, 6)));
    }

    fn at(s1: i8, s2: i8, p: u8, s: i8) -> Scaling {
        Scaling::new(s1, s2, p, s).unwrap()
    }

    #[test]
    fn the_last_place_rounds_half_away_from_zero() {
        let q = at(1, 1, 10, 0); // (at scale 0: whole quotients)
        assert_eq!(q.quotient(20, 30).unwrap(), 1, "2/3 rounds up");
        assert_eq!(q.quotient(-20, 30).unwrap(), -1, "-2/3 rounds down");
        assert_eq!(q.quotient(10, 30).unwrap(), 0, "1/3 to 0");
        assert_eq!(q.quotient(-10, 30).unwrap(), 0);
        assert_eq!(q.quotient(10, 20).unwrap(), 1, "a half rounds away from zero");
        assert_eq!(q.quotient(10, -20).unwrap(), -1);
        assert_eq!(q.quotient(-10, -20).unwrap(), 1);
        assert_eq!(q.quotient(-30, 20).unwrap(), -2, "-1.5 to -2");
        assert_eq!(q.quotient(50, 20).unwrap(), 3, "2.5 to 3, not to even");
        let q = at(0, 1, 37, 16); // (71 / 3.0)
        assert_eq!(q.quotient(71, 30).unwrap(), 236_666_666_666_666_667);
        assert_eq!(q.quotient(-71, 30).unwrap(), -236_666_666_666_666_667);
    }

    #[test]
    fn a_dividend_that_overflows_i128_is_worked_out_in_i256() {
        let ten37 = 10i128.pow(37);
        let q = at(0, 0, 38, 16); // (10^37 · 10^16 doesn't fit an i128)
        assert!(ten37.checked_mul(10i128.pow(16)).is_none());
        assert!(q.quotient(ten37, 3).is_err(), "3.33…e36 at 16 places is past 38 digits");
        assert_eq!(q.quotient(ten37, ten37 / 1000 * 3).unwrap(), 3_333_333_333_333_333_333, "333.3333333333333333");
        assert_eq!(q.quotient(-ten37, ten37 / 1000 * 3).unwrap(), -3_333_333_333_333_333_333);
        assert_eq!(q.quotient(2 * ten37, ten37 * 3).unwrap(), 6_666_666_666_666_667, "0.6666666666666667, rounded in i256 too");
    }

    #[test]
    fn a_zero_divisor_and_a_quotient_past_its_precision_are_refused_as_arrow_refuses_them() {
        let q = at(1, 1, 5, 2);
        assert_eq!(q.quotient(10, 0).unwrap_err().to_string(), "Divide by zero error");
        let code = |e: ArrowError| crate::codes::of(&anyhow::Error::new(e));
        assert_eq!(code(q.quotient(10, 0).unwrap_err()), "22012");
        let e = q.quotient(1_000_000, 1).unwrap_err(); // (100000.0 / 0.1 = 1000000.00, 9 digits)
        assert!(e.to_string().contains("too large to store in a Decimal128 of precision 5"), "{e}");
        assert_eq!(code(e), "22003");
    }

    #[test]
    fn values_and_columns_divide_alike_and_safe_answers_null() {
        use datafusion::arrow::array::{Array, Decimal128Array};
        let col = |v: Vec<Option<i128>>, p: u8, s: i8| ColumnarValue::Array(Arc::new(Decimal128Array::from(v).with_precision_and_scale(p, s).unwrap()));
        let to = decimal(37, 16);
        let a = col(vec![Some(71), None, Some(-1), Some(5)], 20, 0);
        let three = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(30), 2, 1));
        let values = |c: ColumnarValue| match c {
            ColumnarValue::Array(a) => a.as_primitive::<Decimal128Type>().iter().collect::<Vec<_>>(),
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, ..)) => vec![v],
            other => panic!("{other:?}"),
        };
        let by_three = divided(&a, &three, 4, &to, false).unwrap();
        assert_eq!(by_three.data_type(), to);
        assert_eq!(values(by_three), vec![Some(236_666_666_666_666_667), None, Some(-3_333_333_333_333_333), Some(16_666_666_666_666_667)]);
        let threes = col(vec![Some(30); 4], 2, 1);
        assert_eq!(values(divided(&a, &threes, 4, &to, false).unwrap()), values(divided(&a, &three, 4, &to, false).unwrap()), "a column of threes, as one three");
        let one = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(71), 20, 0));
        assert_eq!(values(divided(&one, &three, 1, &to, false).unwrap()), vec![Some(236_666_666_666_666_667)]);
        let zeros = col(vec![Some(30), Some(30), Some(0), Some(30)], 2, 1);
        assert!(divided(&a, &zeros, 4, &to, false).is_err(), "a zero divisor fails the division");
        let safe = divided(&a, &zeros, 4, &to, true).unwrap();
        assert_eq!(values(safe), vec![Some(236_666_666_666_666_667), None, None, Some(16_666_666_666_666_667)], "SAFE: NULL where the divisor is 0");
        let nothing = ColumnarValue::Scalar(ScalarValue::Decimal128(None, 2, 1));
        let nulls = divided(&a, &nothing, 4, &to, false).unwrap();
        assert_eq!(values(nulls), vec![None; 4]);
        if let ColumnarValue::Array(x) = divided(&a, &three, 4, &to, false).unwrap() {
            assert_eq!(x.null_count(), 1);
        }
    }
}
