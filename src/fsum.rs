//! `sum` over floating point that gives the same answer whatever the order. DataFusion adds a
//! group's values as they arrive, and each partition's partial sums in whatever order they finish,
//! so a sum of DOUBLEs could differ in its last bits from run to run. TPC-H q15 compares a sum with
//! the max of the same sums, computed twice, and so found its row only some of the time.
//!
//! Here each addition's rounding error is kept in a second double (the two-sum trick) and added
//! back at the end: the answer is the true sum rounded once, in any order and on any number of
//! nodes (short of a sum within about n·2⁻¹⁰⁶ of a rounding tie). Integers, decimals and
//! `sum(DISTINCT …)` stay DataFusion's own `sum`.
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float64Array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::{format_state_name, AggregateOrderSensitivity};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, Expr, GroupsAccumulator, Operator, ReversedUDAF,
    SetMonotonicity, Signature, StatisticsArgs,
};
use datafusion::physical_expr::NullState;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Replaces `sum` in a session.
pub fn register(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(ExactSum::default()));
}

/// Adds `v` to the sum `hi`, and what rounding took off it to `lo`.
#[inline]
fn add(hi: &mut f64, lo: &mut f64, v: f64) {
    let s = *hi + v;
    if s.is_finite() {
        let b = s - *hi;
        *lo += (*hi - (s - b)) + (v - b);
    }
    *hi = s;
}

fn total(hi: f64, lo: f64) -> f64 {
    if hi.is_finite() { hi + lo } else { hi }
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
struct ExactSum(Sum);

fn float(args: &AccumulatorArgs) -> bool {
    !args.is_distinct && args.return_field.data_type() == &DataType::Float64
}

impl AggregateUDFImpl for ExactSum {
    fn name(&self) -> &str { "sum" }
    fn signature(&self) -> &Signature { self.0.signature() }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> { self.0.return_type(types) }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if float(&args) { Ok(Box::<Exact>::default()) } else { self.0.accumulator(args) }
    }

    /// A float sum's state is two columns: the sum, and its rounding error.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct || args.return_type() != &DataType::Float64 {
            return self.0.state_fields(args);
        }
        Ok(vec![Field::new(format_state_name(args.name, "sum"), DataType::Float64, true).into(),
                Field::new(format_state_name(args.name, "sum error"), DataType::Float64, true).into()])
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool { !args.is_distinct }

    fn create_groups_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        if float(&args) { Ok(Box::<Groups>::default()) } else { self.0.create_groups_accumulator(args) }
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if float(&args) { Ok(Box::<Exact>::default()) } else { self.0.create_sliding_accumulator(args) }
    }

    // The rest as DataFusion's `sum`.
    fn reverse_expr(&self) -> ReversedUDAF { self.0.reverse_expr() }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity { self.0.order_sensitivity() }
    fn documentation(&self) -> Option<&Documentation> { self.0.documentation() }
    fn set_monotonicity(&self, t: &DataType) -> SetMonotonicity { self.0.set_monotonicity(t) }
    fn value_from_stats(&self, stats: &StatisticsArgs) -> Option<ScalarValue> { self.0.value_from_stats(stats) }
    fn simplify_expr_op_literal(&self, f: &AggregateFunction, arg: &Expr, op: Operator, lit: &Expr, left: bool) -> Result<Option<Expr>> {
        self.0.simplify_expr_op_literal(f, arg, op, lit, left)
    }
}

/// One sum (no GROUP BY, and windows: it can take values back out).
#[derive(Debug, Default)]
struct Exact {
    hi: f64,
    lo: f64,
    n: i64, // values in it (none: NULL)
}

impl Exact {
    fn each(&mut self, values: &ArrayRef, sign: f64) {
        for v in values.as_primitive::<Float64Type>().iter().flatten() {
            add(&mut self.hi, &mut self.lo, sign * v);
            self.n += sign as i64;
        }
    }
}

impl Accumulator for Exact {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.each(&values[0], 1.0);
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.each(&values[0], -1.0);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool { true }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let (hi, lo) = (states[0].as_primitive::<Float64Type>(), states[1].as_primitive::<Float64Type>());
        for i in (0..hi.len()).filter(|&i| hi.is_valid(i)) {
            add(&mut self.hi, &mut self.lo, hi.value(i));
            add(&mut self.hi, &mut self.lo, lo.value(i));
            self.n += 1;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64((self.n > 0).then_some(self.hi)), ScalarValue::Float64(Some(self.lo))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64((self.n > 0).then(|| total(self.hi, self.lo))))
    }

    fn size(&self) -> usize { size_of::<Self>() }
}

/// A sum per group (GROUP BY).
#[derive(Debug, Default)]
struct Groups {
    hi: Vec<f64>,
    lo: Vec<f64>,
    seen: NullState, // groups with a value (the others are NULL)
}

impl GroupsAccumulator for Groups {
    fn update_batch(&mut self, values: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, total: usize) -> Result<()> {
        self.hi.resize(total, 0.0);
        self.lo.resize(total, 0.0);
        let (hi, lo) = (&mut self.hi, &mut self.lo);
        self.seen.accumulate(groups, values[0].as_primitive::<Float64Type>(), filter, total, |g, v| add(&mut hi[g], &mut lo[g], v));
        Ok(())
    }

    /// Partial sums: the sums as values, then their errors (0 where there was no sum).
    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], total: usize) -> Result<()> {
        self.update_batch(&states[..1], groups, None, total)?;
        for (&g, &e) in groups.iter().zip(states[1].as_primitive::<Float64Type>().values().iter()) {
            if e != 0.0 {
                add(&mut self.hi[g], &mut self.lo[g], e);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let nulls = self.seen.build(emit);
        let (hi, lo) = (emit.take_needed(&mut self.hi), emit.take_needed(&mut self.lo));
        let sums: Vec<f64> = hi.into_iter().zip(lo).map(|(h, l)| total(h, l)).collect();
        Ok(Arc::new(Float64Array::new(sums.into(), nulls)))
    }

    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = self.seen.build(emit);
        let (hi, lo) = (emit.take_needed(&mut self.hi), emit.take_needed(&mut self.lo));
        Ok(vec![Arc::new(Float64Array::new(hi.into(), nulls)), Arc::new(Float64Array::from(lo))])
    }

    /// Rows as partial sums of one value each (when grouping first doesn't pay).
    fn convert_to_state(&self, values: &[ArrayRef], filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        let v = values[0].as_primitive::<Float64Type>();
        let kept = filter.map(|f| NullBuffer::from(f.iter().map(|b| b == Some(true)).collect::<Vec<_>>()));
        let nulls = NullBuffer::union(v.nulls(), kept.as_ref());
        Ok(vec![Arc::new(Float64Array::new(v.values().clone(), nulls)), Arc::new(Float64Array::from(vec![0.0; v.len()]))])
    }

    fn size(&self) -> usize {
        (self.hi.capacity() + self.lo.capacity()) * 8 + self.seen.size()
    }
}
