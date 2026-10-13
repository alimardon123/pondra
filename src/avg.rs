//! `avg` over a DECIMAL rounded at its last place, as Spark and Postgres round it. DataFusion's
//! cuts off the digits after that place, so TPC-H q1's `avg(l_quantity)` came back 25.522005 for
//! 25.5220058…. Here DataFusion works the average out one place further and that place is rounded
//! away (half away from zero); the answer keeps DataFusion's type, decimal(p + 4, s + 4).
//!
//! Only decimals of up to 24 digits: one place more costs the sum a digit of its room, and at 24
//! digits it keeps the billion rows DataFusion leaves it at 25. Wider decimals, and every other
//! type, stay DataFusion's own `avg`.
//!
//! A materialized view's `avg` is finished from the sum and the count it keeps (`finish.rs`), by
//! `pondra_avg_of(sum, count)`, which works it out the same way, so the view and the query agree.
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Decimal128Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int64Type};
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result, ScalarValue};
use datafusion::functions_aggregate::average::Avg;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Documentation, EmitTo, GroupsAccumulator, ReversedUDAF,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, SetMonotonicity, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Replaces `avg` (and `mean`) in a session, and adds `pondra_avg_of`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(RoundedAvg::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(AvgOf(Signature::any(2, Volatility::Immutable))));
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
struct RoundedAvg(Avg);

/// The answer's precision and scale, when this one is worked out a place further and rounded.
fn rounded(args: &AccumulatorArgs) -> Option<(u8, i8)> {
    match (args.expr_fields[0].data_type(), args.return_type()) {
        (DataType::Decimal128(p, _), DataType::Decimal128(rp, rs)) if *p <= 24 => Some((*rp, *rs)),
        _ => None,
    }
}

/// DataFusion's accumulator for the answer one place further, and the answer's precision and scale.
fn finer<'a, T>(args: AccumulatorArgs<'a>, make: impl FnOnce(AccumulatorArgs<'a>) -> Result<T>) -> Result<(T, Option<(u8, i8)>)> {
    let Some((p, s)) = rounded(&args) else { return Ok((make(args)?, None)) };
    let field = Field::new(args.return_field.name(), DataType::Decimal128(p + 1, s + 1), true);
    Ok((make(AccumulatorArgs { return_field: Arc::new(field), ..args })?, Some((p, s))))
}

/// Takes the last place off, rounding half away from zero (`/` on integers rounds toward zero).
fn round(v: i128) -> i128 {
    (v + if v < 0 { -5 } else { 5 }) / 10
}

impl AggregateUDFImpl for RoundedAvg {
    fn name(&self) -> &str { "avg" }
    fn aliases(&self) -> &[String] { self.0.aliases() }
    fn signature(&self) -> &Signature { self.0.signature() }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> { self.0.return_type(types) }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match finer(args, |a| self.0.accumulator(a))? {
            (inner, Some(to)) => Ok(Box::new(Rounded { inner, to })),
            (inner, None) => Ok(inner),
        }
    }

    // The state is the count and the sum, whatever the answer's scale.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> { self.0.state_fields(args) }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool { self.0.groups_accumulator_supported(args) }

    fn create_groups_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        match finer(args, |a| self.0.create_groups_accumulator(a))? {
            (inner, Some(to)) => Ok(Box::new(RoundedGroups { inner, to })),
            (inner, None) => Ok(inner),
        }
    }

    // The rest as DataFusion's `avg`.
    fn reverse_expr(&self) -> ReversedUDAF { self.0.reverse_expr() }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity { self.0.order_sensitivity() }
    fn documentation(&self) -> Option<&Documentation> { self.0.documentation() }
    fn set_monotonicity(&self, t: &DataType) -> SetMonotonicity { self.0.set_monotonicity(t) }
}

/// One average (no GROUP BY, DISTINCT, and windows, which take values back out).
#[derive(Debug)]
struct Rounded {
    inner: Box<dyn Accumulator>,
    to: (u8, i8),
}

impl Accumulator for Rounded {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> { self.inner.update_batch(values) }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> { self.inner.retract_batch(values) }
    fn supports_retract_batch(&self) -> bool { self.inner.supports_retract_batch() }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> { self.inner.merge_batch(states) }
    fn state(&mut self) -> Result<Vec<ScalarValue>> { self.inner.state() }
    fn size(&self) -> usize { self.inner.size() + size_of::<Self>() }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.inner.evaluate()? {
            ScalarValue::Decimal128(v, ..) => Ok(ScalarValue::Decimal128(v.map(round), self.to.0, self.to.1)),
            other => Ok(other),
        }
    }
}

/// An average per group (GROUP BY).
struct RoundedGroups {
    inner: Box<dyn GroupsAccumulator>,
    to: (u8, i8),
}

impl GroupsAccumulator for RoundedGroups {
    fn update_batch(&mut self, values: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, total: usize) -> Result<()> {
        self.inner.update_batch(values, groups, filter, total)
    }
    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], total: usize) -> Result<()> {
        self.inner.merge_batch(states, groups, total)
    }
    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> { self.inner.state(emit) }
    fn convert_to_state(&self, values: &[ArrayRef], filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, filter)
    }
    fn size(&self) -> usize { self.inner.size() }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let finer = self.inner.evaluate(emit)?;
        let rounded = finer.as_primitive::<Decimal128Type>().unary::<_, Decimal128Type>(round);
        Ok(Arc::new(rounded.with_precision_and_scale(self.to.0, self.to.1)?))
    }
}

/// A view's finished average (`finish.rs`): a decimal sum over a count, as `avg` answers it, in its
/// scale (the sum's and four more): a place further, then rounded when `avg` rounds (a sum of up to
/// 34 digits: of values of up to 24) and cut off when it doesn't. NULL for no rows.
#[derive(Debug, PartialEq, Eq, Hash)]
struct AvgOf(Signature);

impl ScalarUDFImpl for AvgOf {
    fn name(&self) -> &str { "pondra_avg_of" }
    fn signature(&self) -> &Signature { &self.0 }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types[0] {
            DataType::Decimal128(_, s) => Ok(DataType::Decimal128(38, (s + 4).min(38))),
            ref t => plan_err!("pondra_avg_of takes a DECIMAL sum, not {t}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (DataType::Decimal128(p, s), Some(counts)) = (arrays[0].data_type().clone(), arrays.get(1)) else {
            return exec_err!("pondra_avg_of takes a DECIMAL sum and a count");
        };
        let scale = (s + 4).min(38);
        let (counts, ten) = (cast(counts, &DataType::Int64)?, 10i128.pow((scale - s + 1) as u32));
        let averages = arrays[0].as_primitive::<Decimal128Type>().iter().zip(counts.as_primitive::<Int64Type>().iter())
            .map(|(sum, n)| match (sum, n) {
                (Some(sum), Some(n)) if n != 0 => {
                    let (n, further) = (n as i128, |q: i128| q.checked_mul(ten));
                    // (sum · ten / n without overflow: the whole part and the remainder apart)
                    let v = further(sum / n).and_then(|q| q.checked_add(sum % n * ten / n)).ok_or_else(|| exec_datafusion_err!("Arithmetic overflow in avg"))?;
                    Ok(Some(if p <= 34 { round(v) } else { v / 10 }))
                }
                _ => Ok(None),
            })
            .collect::<Result<Decimal128Array>>()?;
        Ok(ColumnarValue::Array(Arc::new(averages.with_precision_and_scale(38, scale)?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_half_away_from_zero() {
        assert_eq!([round(255_220_058), round(255_220_054), round(-255_220_055), round(-255_220_054), round(0)],
                   [25_522_006, 25_522_005, -25_522_006, -25_522_005, 0]);
    }
}
