//! `avg` over a DECIMAL as `sum(x) / count(x)` answers it: the same type (`divide::quotient_type`, from DataFusion's
//! `sum`'s type over a BIGINT's) and the same digits, the last place rounded half away from zero. TPC-H's
//! `avg(l_quantity)` was 25.522006 (DataFusion's s + 4 places, here rounded) while `sum(l_quantity) / count(l_quantity)`
//! was 25.522005853257337; now both are the second, which Postgres's 25.5220058532573370 is to 15 places.
//!
//! DataFusion keeps the count and the sum as it does (its accumulators, so every node's partial rows, windows that take
//! values back out, and DISTINCT are its own), and the answer is that sum divided by that count (`divide::divided`):
//! exact, in i256s where the scaled sum doesn't fit an i128. In Spark's dialect `avg` is Spark's (`set::apply`).
//! A DOUBLE's, and an integer's, is the same in any order (`fvar`), integers of 32 bits or fewer summed exactly as they
//! are.
//!
//! A materialized view's `avg` is finished from the sum and the count it keeps (`finish.rs`), by
//! `pondra_avg_of(sum, count)`, which divides them the same way, so the view and the query agree.
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Decimal128Array};
use datafusion::arrow::compute::{cast, cast_with_options, sum, CastOptions};
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int64Type, UInt64Type};
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result, ScalarValue};
use datafusion::functions_aggregate::average::Avg;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Documentation, EmitTo, GroupsAccumulator, ReversedUDAF,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, SetMonotonicity, Signature, Volatility,
};
use crate::divide::Scaling;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Replaces `avg` (and `mean`) in a session, and adds `pondra_avg_of`.
pub fn register(ctx: &SessionContext) {
    let avg = Avg::default();
    let wholes = crate::fvar::with_wholes(avg.signature());
    ctx.register_udaf(AggregateUDF::from(RoundedAvg(avg, wholes)));
    ctx.register_udf(ScalarUDF::new_from_impl(AvgOf(Signature::any(2, Volatility::Immutable))));
}

/// A count as `/` divides by it: a BIGINT meeting a DECIMAL is a DECIMAL(20, 0) (`divide.rs`).
const COUNT: DataType = DataType::Decimal128(20, 0);

/// The type of a quotient of a sum of this type by a count (`divide::quotient_type`): an average's. None for a sum that
/// isn't a DECIMAL128.
fn quotient_of(sum: &DataType) -> Option<DataType> {
    crate::divide::quotient_type(sum, &COUNT, false)
}

/// The type of `avg` of a DECIMAL128 of this type: `sum(x) / count(x)`'s, `sum` keeping 10 digits more (DataFusion's,
/// and Spark's). None for any other type (DataFusion's `avg` then).
fn averaged(input: &DataType) -> Option<DataType> {
    let DataType::Decimal128(p, s) = input else { return None };
    quotient_of(&DataType::Decimal128((*p + 10).min(38), *s))
}

/// DataFusion's `avg`, and its signature with integers of 32 bits or fewer taken as they are (`fvar::with_wholes`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct RoundedAvg(Avg, Signature);

/// How an average is worked out: its sum, at the values' scale, over its count, into the average's type.
#[derive(Debug)]
struct Quotient {
    how: Scaling,
    to: (u8, i8),
}

impl Quotient {
    fn new(sum_scale: i8, to: &DataType) -> Result<Quotient> {
        let &DataType::Decimal128(p, s) = to else { return exec_err!("an average of a DECIMAL is a DECIMAL, not {to}") };
        Ok(Quotient { how: Scaling::new(sum_scale, 0, p, s)?, to: (p, s) })
    }

    /// `sum / count`, rounded at its last place; NULL for no sum or a count of 0 (no values, or all taken back out of a
    /// window's frame).
    fn one(&self, sum: Option<i128>, count: i128) -> Result<ScalarValue> {
        let q = match sum { Some(sum) if count != 0 => Some(self.how.quotient(sum, count)?), _ => None };
        Ok(ScalarValue::Decimal128(q, self.to.0, self.to.1))
    }

    /// Each sum over its count (`one`). A sum DataFusion keeps wider than 38 digits (of values of more than 25) is taken
    /// as the 38 `sum` keeps: one past them is refused, not wrapped.
    fn each(&self, sums: &ArrayRef, counts: &ArrayRef) -> Result<ArrayRef> {
        let (sums, counts) = (in_128(sums)?, cast(counts, &DataType::Int64)?);
        let averages: Decimal128Array = sums.as_primitive::<Decimal128Type>().iter().zip(counts.as_primitive::<Int64Type>().iter())
            .map(|(sum, n)| match (sum, n) {
                (Some(sum), Some(n)) if n != 0 => self.how.quotient(sum, i128::from(n)).map(Some),
                _ => Ok(None),
            })
            .collect::<Result<_, _>>()?;
        Ok(Arc::new(averages.with_precision_and_scale(self.to.0, self.to.1)?))
    }
}

/// DECIMAL values as DECIMAL128s at their scale: a DECIMAL256's refused past 38 digits.
fn in_128(values: &ArrayRef) -> Result<ArrayRef> {
    match values.data_type() {
        DataType::Decimal128(..) => Ok(values.clone()),
        DataType::Decimal256(_, s) => Ok(cast_with_options(values, &DataType::Decimal128(38, *s), &CastOptions { safe: false, ..Default::default() })?),
        t => exec_err!("avg of a DECIMAL kept {t}"),
    }
}

/// A DECIMAL value as an i128 (None: NULL), a DECIMAL256's refused past 38 digits.
fn value_128(v: &ScalarValue) -> Result<Option<i128>> {
    match v {
        ScalarValue::Decimal128(v, ..) => Ok(*v),
        ScalarValue::Decimal256(v, ..) => v.map(|v| v.to_i128().ok_or_else(|| exec_datafusion_err!("Arithmetic overflow in avg"))).transpose(),
        v => exec_err!("avg of a DECIMAL kept {}", v.data_type()),
    }
}

/// When the answer is a DECIMAL sum over a count (`averaged`): DataFusion's arguments, with its own answer's type, for the
/// accumulator that keeps them, and how its sum becomes the answer.
fn keeper<'a>(avg: &Avg, args: AccumulatorArgs<'a>) -> Result<(AccumulatorArgs<'a>, Option<Quotient>)> {
    let input = args.expr_fields[0].data_type().clone();
    match (&input, averaged(&input).filter(|t| t == args.return_type())) {
        (&DataType::Decimal128(_, s), Some(to)) => {
            let field = Field::new(args.return_field.name(), avg.return_type(&[input])?, true);
            Ok((AccumulatorArgs { return_field: Arc::new(field), ..args }, Some(Quotient::new(s, &to)?)))
        }
        _ => Ok((args, None)),
    }
}

impl AggregateUDFImpl for RoundedAvg {
    fn name(&self) -> &str { "avg" }
    fn aliases(&self) -> &[String] { self.0.aliases() }
    fn signature(&self) -> &Signature { &self.1 }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types.first().and_then(averaged) {
            Some(t) => Ok(t),
            None => self.0.return_type(types),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if crate::fvar::steady(&args) {
            return Ok(crate::fvar::mean_accumulator(&args));
        }
        if args.expr_fields.first().is_some_and(|f| crate::fvar::is_whole(f.data_type())) {
            return crate::fvar::as_double(args, |a| self.0.accumulator(a)); // (DISTINCT)
        }
        let distinct = args.is_distinct;
        match keeper(&self.0, args)? {
            (inner, Some(by)) => match (distinct, inner.expr_fields[0].data_type()) {
                // (DataFusion keeps their sum as a DECIMAL(38, s): `Running` keeps it alike)
                (false, &DataType::Decimal128(p, s)) if p <= 25 => Ok(Box::new(Running { count: 0, sum: None, scale: s, by })),
                _ => Ok(Box::new(Average { inner: self.0.accumulator(inner)?, by, distinct })),
            },
            (args, None) => self.0.accumulator(args),
        }
    }

    // The state is the count and the sum (DataFusion's, whatever the answer's type), or for DISTINCT the values; over
    // DOUBLE, the sum's error too, and over whole numbers the sum exact (`fvar`).
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let double = |f: &FieldRef| f.data_type() == &DataType::Float64;
        let input = |f: &FieldRef| double(f) || crate::fvar::is_whole(f.data_type());
        match !args.is_distinct && double(&args.return_field) && args.input_fields.first().is_some_and(input) {
            true => Ok(crate::fvar::mean_state(args.name, crate::fvar::is_whole(args.input_fields[0].data_type()))),
            false => self.0.state_fields(args),
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool { self.0.groups_accumulator_supported(args) }

    fn create_groups_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        if crate::fvar::steady(&args) {
            return Ok(crate::fvar::mean_groups(&args));
        }
        match keeper(&self.0, args)? {
            (inner, Some(by)) => Ok(Box::new(Averages { inner: self.0.create_groups_accumulator(inner)?, by })),
            (args, None) => self.0.create_groups_accumulator(args),
        }
    }

    // (a frame that slides takes values back out: over DOUBLE DataFusion's, in its ORDER BY's order)
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if crate::fvar::steady(&args) { crate::fvar::as_double(args, |a| self.0.accumulator(a)) } else { self.accumulator(args) }
    }

    // The rest as DataFusion's `avg`.
    fn reverse_expr(&self) -> ReversedUDAF { self.0.reverse_expr() }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity { self.0.order_sensitivity() }
    fn documentation(&self) -> Option<&Documentation> { self.0.documentation() }
    fn set_monotonicity(&self, t: &DataType) -> SetMonotonicity { self.0.set_monotonicity(t) }
}

/// One average of DECIMALs of up to 25 digits (no GROUP BY, and windows, which take values back out): the count and the
/// sum kept as DataFusion keeps them (the same state, a DECIMAL(38, s) sum that wraps as `sum`'s does), divided as it is
/// read with nothing built on the way, since a window's frame asks for each row's.
#[derive(Debug)]
struct Running {
    count: u64,
    sum: Option<i128>,
    scale: i8,
    by: Quotient,
}

impl Running {
    fn add(&mut self, values: &ArrayRef, sign: i128) {
        let values = values.as_primitive::<Decimal128Type>();
        if let Some(x) = sum(values) {
            self.sum = Some(self.sum.unwrap_or(0).wrapping_add(x.wrapping_mul(sign)));
        }
    }
}

impl Accumulator for Running {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.count += (values[0].len() - values[0].null_count()) as u64;
        self.add(&values[0], 1);
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.count -= (values[0].len() - values[0].null_count()) as u64;
        self.add(&values[0], -1);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool { true }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or(0);
        self.add(&in_128(&states[1])?, 1);
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> { Ok(vec![ScalarValue::UInt64(Some(self.count)), ScalarValue::Decimal128(self.sum, 38, self.scale)]) }
    fn evaluate(&mut self) -> Result<ScalarValue> { self.by.one(self.sum, i128::from(self.count)) }
    fn size(&self) -> usize { size_of::<Self>() }
}

/// One average of wider DECIMALs, or of DISTINCT ones: DataFusion's count and sum (or its values), divided.
#[derive(Debug)]
struct Average {
    inner: Box<dyn Accumulator>,
    by: Quotient,
    /// The state is the distinct values (DataFusion's `avg(DISTINCT …)`), not a count and a sum.
    distinct: bool,
}

impl Accumulator for Average {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> { self.inner.update_batch(values) }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> { self.inner.retract_batch(values) }
    fn supports_retract_batch(&self) -> bool { self.inner.supports_retract_batch() }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> { self.inner.merge_batch(states) }
    fn state(&mut self) -> Result<Vec<ScalarValue>> { self.inner.state() }
    fn size(&self) -> usize { self.inner.size() + size_of::<Self>() }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match (self.distinct, &self.inner.state()?[..]) {
            (false, [ScalarValue::UInt64(count), sum]) => self.by.one(value_128(sum)?, i128::from(count.unwrap_or(0))),
            (true, [ScalarValue::List(values)]) => {
                let values = in_128(&values.value(0))?;
                let values = values.as_primitive::<Decimal128Type>();
                let sum = values.iter().flatten().try_fold(0i128, i128::checked_add).ok_or_else(|| exec_datafusion_err!("Arithmetic overflow in avg"))?;
                self.by.one(Some(sum), (values.len() - values.null_count()) as i128)
            }
            (_, state) => exec_err!("avg of a DECIMAL kept {} values", state.len()),
        }
    }
}

/// An average per group (GROUP BY): DataFusion's counts and sums, divided.
struct Averages {
    inner: Box<dyn GroupsAccumulator>,
    by: Quotient,
}

impl GroupsAccumulator for Averages {
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

    // (the state takes the groups `emit` names, as the answer would)
    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        match &self.inner.state(emit)?[..] {
            [counts, sums] => self.by.each(sums, counts),
            state => exec_err!("avg of a DECIMAL kept {} values per group", state.len()),
        }
    }
}

/// A view's finished average (`finish.rs`): a DECIMAL sum over a count, as `avg` answers it (`Quotient`), so in the type
/// the view's query has. NULL for no rows.
#[derive(Debug, PartialEq, Eq, Hash)]
struct AvgOf(Signature);

impl ScalarUDFImpl for AvgOf {
    fn name(&self) -> &str { "pondra_avg_of" }
    fn signature(&self) -> &Signature { &self.0 }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match quotient_of(&types[0]) {
            Some(t) => Ok(t),
            None => plan_err!("pondra_avg_of takes a DECIMAL sum, not {}", types[0]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let ([sums, counts], &DataType::Decimal128(_, s)) = (&arrays[..], arrays[0].data_type()) else {
            return exec_err!("pondra_avg_of takes a DECIMAL sum and a count");
        };
        Ok(ColumnarValue::Array(Quotient::new(s, args.return_field.data_type())?.each(sums, counts)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionConfig;

    fn decimal(p: u8, s: i8) -> DataType {
        DataType::Decimal128(p, s)
    }

    #[test]
    fn an_average_has_the_type_of_a_sum_over_a_count() {
        assert_eq!(averaged(&decimal(15, 2)), Some(decimal(38, 15)), "TPC-H's l_quantity: sum(l_quantity) / count(l_quantity)'s");
        assert_eq!(averaged(&decimal(10, 2)), Some(decimal(34, 16)), "16 places where the whole digits leave room");
        assert_eq!(averaged(&decimal(38, 10)), Some(decimal(38, 14)), "Arrow's s + 4 at 38 digits");
        assert_eq!(averaged(&DataType::Float64), None);
        assert_eq!(quotient_of(&decimal(25, 2)), averaged(&decimal(15, 2)), "a view's kept sum gives its query's type");
    }

    /// `avg` and `sum / count` answer alike: one value, per group, DISTINCT, over a sliding frame, and none.
    #[tokio::test]
    async fn avg_answers_as_sum_over_count() {
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let t = "(VALUES (1, CAST(17 AS DECIMAL(15,2))), (1, 36.50), (1, 8.00), (2, -1.25), (2, -1.25), (2, NULL), (3, NULL)) t(k, x)";
        let one = |sql: String| {
            let ctx = ctx.clone();
            async move {
                let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                (0..batches[0].num_columns()).map(|c| array_value_to_string(batches[0].column(c), 0).unwrap()).collect::<Vec<_>>()
            }
        };
        let [avg, quotient] = [format!("SELECT avg(x), arrow_typeof(avg(x)) FROM {t}"), format!("SELECT sum(x) / count(x), arrow_typeof(sum(x) / count(x)) FROM {t}")];
        assert_eq!(one(avg).await, ["11.800000000000000", "Decimal128(38, 15)"], "59 / 5");
        assert_eq!(one(quotient).await, ["11.800000000000000", "Decimal128(38, 15)"]);
        let by = format!("SELECT string_agg(CAST(a AS VARCHAR), ' ' ORDER BY k) FROM (SELECT k, avg(x) AS a FROM {t} GROUP BY k)");
        assert_eq!(one(by).await, ["20.500000000000000 -1.250000000000000"], "per group; NULL for a group of no values");
        assert_eq!(one(format!("SELECT avg(DISTINCT x) FROM {t} WHERE k = 2")).await, ["-1.250000000000000"]);
        assert_eq!(one(format!("SELECT avg(x) IS NULL FROM {t} WHERE k = 3")).await, ["true"], "no values");
        let thirds = "(VALUES (1, CAST(1 AS DECIMAL(10,2))), (2, 1), (3, 2), (4, 2)) t(i, x)";
        let frame = format!("SELECT string_agg(CAST(a AS VARCHAR), ' ' ORDER BY i) FROM (SELECT i, avg(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a FROM {thirds})");
        // (a DECIMAL(22, 2), the integers' type and the first value's: 8 places)
        assert_eq!(one(frame).await, ["1.00000000 1.33333333 1.66666667 2.00000000"], "a frame that slides, the last place rounded");
        let wide = format!("SELECT avg(CAST(x AS DECIMAL(38, 2))), avg(DISTINCT CAST(x AS DECIMAL(38, 2))) FROM {t}");
        assert_eq!(one(wide).await, ["11.800000", "15.062500"], "a sum DataFusion keeps in 76 digits, and DISTINCT's values");
    }
}
