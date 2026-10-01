//! Postgres's interval arithmetic (round 31): `n * INTERVAL '37 seconds'`, `INTERVAL '1 month' *
//! 2.5`, `INTERVAL '1 day' / 3`, which DataFusion refuses. An interval scaled as Postgres scales one
//! (`interval_mul`): what a fraction of a month leaves goes to days (30 a month), what a fraction of
//! a day leaves goes to time (24 hours a day).
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, IntervalMonthDayNanoArray};
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::{cast, lit, ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::sql::sqlparser::ast::BinaryOperator;
use std::sync::{Arc, LazyLock};

pub fn planner() -> Arc<dyn ExprPlanner> { Arc::new(Arithmetic) }

#[derive(Debug)]
struct Arithmetic;

impl ExprPlanner for Arithmetic {
    fn plan_binary_op(&self, e: RawBinaryExpr, schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>> {
        let interval = |x: &Expr| x.get_type(schema).is_ok_and(|t| matches!(t, DataType::Interval(_)));
        let number = |x: &Expr| x.get_type(schema).is_ok_and(|t| t.is_numeric());
        let (i, n, divide) = match e.op {
            BinaryOperator::Multiply if interval(&e.left) && number(&e.right) => (&e.left, &e.right, false),
            BinaryOperator::Multiply if number(&e.left) && interval(&e.right) => (&e.right, &e.left, false),
            BinaryOperator::Divide if interval(&e.left) && number(&e.right) => (&e.left, &e.right, true),
            _ => return Ok(PlannerResult::Original(e)),
        };
        let factor = cast(n.clone(), DataType::Float64);
        let factor = if divide { lit(1.0) / factor } else { factor };
        let i = cast(i.clone(), DataType::Interval(IntervalUnit::MonthDayNano));
        Ok(PlannerResult::Planned(Expr::ScalarFunction(ScalarFunction::new_udf(SCALE.clone(), vec![i, factor]))))
    }
}

static SCALE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(Scale(Signature::exact(vec![DataType::Interval(IntervalUnit::MonthDayNano), DataType::Float64], Volatility::Immutable)))));

/// `interval_mul(interval, factor)`, as Postgres has it.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Scale(Signature);

impl ScalarUDFImpl for Scale {
    fn name(&self) -> &str { "interval_mul" }
    fn signature(&self) -> &Signature { &self.0 }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Interval(IntervalUnit::MonthDayNano)) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (i, f): (&IntervalMonthDayNanoArray, &Float64Array) = (arrays[0].as_primitive(), arrays[1].as_primitive());
        let out: IntervalMonthDayNanoArray = (0..n.max(i.len())).map(|r| (!i.is_null(r) && !f.is_null(r)).then(|| scale(i.value(r), f.value(r)))).collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    }
}

/// Postgres's `interval_mul`: months, then what a fraction of a month leaves as days (30 a month),
/// then what a fraction of a day leaves as time (24 hours a day), in nanoseconds here.
fn scale(i: IntervalMonthDayNano, f: f64) -> IntervalMonthDayNano {
    const DAY_NS: f64 = 86_400e9;
    let months = i.months as f64 * f;
    let month_days = (months - months.trunc()) * 30.0;
    let days = i.days as f64 * f;
    let rest = (days - days.trunc() + month_days - month_days.trunc()) * DAY_NS;
    IntervalMonthDayNano::new(months.trunc() as i32, days.trunc() as i32 + month_days.trunc() as i32, (i.nanoseconds as f64 * f + rest).round() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scales_as_postgres() {
        let i = |m, d, s: i64| IntervalMonthDayNano::new(m, d, s * 1_000_000_000);
        assert_eq!(scale(i(0, 0, 37), 3.0), i(0, 0, 111)); // 3 * '37 seconds'
        assert_eq!(scale(i(1, 0, 0), 2.5), i(2, 15, 0)); // '1 month' * 2.5 = '2 mons 15 days'
        assert_eq!(scale(i(0, 1, 0), 1.0 / 3.0), i(0, 0, 28_800)); // '1 day' / 3 = '08:00:00'
        assert_eq!(scale(i(1, 1, 0), -1.0), i(-1, -1, 0));
    }
}
