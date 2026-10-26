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

/// Adds `v` to the sum `hi`, and what rounding took off it to `lo`. There is no branch: once `hi` is
/// infinite or NaN it stays so (inf + x is inf or NaN), and `lo` may go NaN with it. `total` never
/// reads `lo` then, and the states hand on 0 in its place (`error_of`), so a NaN never reaches a merge.
#[inline]
pub(crate) fn add(hi: &mut f64, lo: &mut f64, v: f64) {
    let s = *hi + v;
    let b = s - *hi;
    *lo += (*hi - (s - b)) + (v - b);
    *hi = s;
}

/// Adds a partial sum, its `hi` and its rounding error `p_lo`. A partial whose `hi` isn't finite may
/// carry a NaN error, so the error is left out then: the partial's sum is infinite or NaN either way,
/// and it merges to an infinite sum, not a NaN.
#[inline]
fn add_partial(hi: &mut f64, lo: &mut f64, p_hi: f64, p_lo: f64) {
    add(hi, lo, p_hi);
    add(hi, lo, if p_hi.is_finite() { p_lo } else { 0.0 });
}

/// The rounding error a sum hands on in a state: 0 once the sum isn't finite, since `lo` may be NaN then.
#[inline]
fn error_of(hi: f64, lo: f64) -> f64 {
    if hi.is_finite() { lo } else { 0.0 }
}

pub(crate) fn total(hi: f64, lo: f64) -> f64 {
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

/// Independent sums that a column without nulls is split across. Each is a chain of its own adds, so
/// the adds don't wait on each other one row at a time.
const LANES: usize = 8;

/// One sum (no GROUP BY, and windows: it can take values back out).
#[derive(Debug, Default)]
struct Exact {
    hi: f64,
    lo: f64,
    n: i64, // values in it (none: NULL)
}

impl Exact {
    /// Adds `sign` times each value (-1 takes them out, as a window's retract does).
    fn each(&mut self, values: &ArrayRef, sign: f64) {
        let values = values.as_primitive::<Float64Type>();
        if values.null_count() > 0 {
            for v in values.iter().flatten() {
                add(&mut self.hi, &mut self.lo, sign * v);
                self.n += sign as i64;
            }
        } else {
            self.lanes(values.values(), sign);
            self.n += sign as i64 * values.len() as i64;
        }
    }

    /// The values of a column with no nulls, added in `LANES` lanes and then folded into the sum.
    fn lanes(&mut self, values: &[f64], sign: f64) {
        let (mut hi, mut lo) = ([0.0; LANES], [0.0; LANES]);
        let chunks = values.chunks_exact(LANES);
        let rest = chunks.remainder();
        for chunk in chunks {
            for ((h, l), &v) in hi.iter_mut().zip(lo.iter_mut()).zip(chunk) {
                add(h, l, sign * v);
            }
        }
        for &v in rest {
            add(&mut hi[0], &mut lo[0], sign * v);
        }
        for (&h, &l) in hi.iter().zip(&lo) {
            add_partial(&mut self.hi, &mut self.lo, h, l);
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
            add_partial(&mut self.hi, &mut self.lo, hi.value(i), lo.value(i));
            self.n += 1;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64((self.n > 0).then_some(self.hi)), ScalarValue::Float64(Some(error_of(self.hi, self.lo)))])
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

impl Groups {
    fn grow(&mut self, total: usize) {
        self.hi.resize(total, 0.0);
        self.lo.resize(total, 0.0);
    }
}

impl GroupsAccumulator for Groups {
    fn update_batch(&mut self, values: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, total: usize) -> Result<()> {
        self.grow(total);
        let (hi, lo) = (&mut self.hi, &mut self.lo);
        self.seen.accumulate(groups, values[0].as_primitive::<Float64Type>(), filter, total, |g, v| add(&mut hi[g], &mut lo[g], v));
        Ok(())
    }

    /// Partial sums: the sums as values, then their errors. An error goes in only where its sum is finite
    /// (an infinite sum's error may be NaN, and the sum stays infinite either way).
    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], total: usize) -> Result<()> {
        self.update_batch(&states[..1], groups, None, total)?;
        let sums = states[0].as_primitive::<Float64Type>();
        for (i, (&g, &e)) in groups.iter().zip(states[1].as_primitive::<Float64Type>().values().iter()).enumerate() {
            if e != 0.0 && sums.value(i).is_finite() {
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
        let errors: Vec<f64> = hi.iter().zip(&lo).map(|(&h, &l)| error_of(h, l)).collect();
        Ok(vec![Arc::new(Float64Array::new(hi.into(), nulls)), Arc::new(Float64Array::from(errors))])
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

#[cfg(test)]
mod tests {
    use super::*;

    const INF: f64 = f64::INFINITY;

    fn array(values: &[f64]) -> ArrayRef { Arc::new(Float64Array::from(values.to_vec())) }

    /// Values from -1e6 to 1e6 with full mantissas. A xorshift generator, so the same values every run.
    fn random_values(n: usize, seed: u64) -> Vec<f64> {
        let mut x = seed;
        (0..n).map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            ((x >> 11) as f64 / (1u64 << 53) as f64 - 0.5) * 2e6
        }).collect()
    }

    /// The old loop, one value at a time: the reference the lanes must match.
    fn one_by_one(values: &[f64]) -> f64 {
        let (mut hi, mut lo) = (0.0, 0.0);
        for &v in values {
            add(&mut hi, &mut lo, v);
        }
        total(hi, lo)
    }

    /// `Exact` over one batch, as a sum with no GROUP BY sees it. None when the sum is NULL.
    fn exact_sum(values: &[f64]) -> Option<f64> {
        let mut acc = Exact::default();
        acc.update_batch(&[array(values)]).unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::Float64(v) => v,
            other => panic!("not a double: {other:?}"),
        }
    }

    /// Shewchuk's partials, then the last rounding as CPython's math.fsum does it: the exact sum,
    /// rounded once. The reference the groups are checked against.
    fn fsum(values: &[f64]) -> f64 {
        let mut partials: Vec<f64> = Vec::new();
        for &value in values {
            let mut x = value;
            let mut kept = 0;
            for i in 0..partials.len() {
                let (big, small) = if x.abs() < partials[i].abs() { (partials[i], x) } else { (x, partials[i]) };
                let hi = big + small;
                let lo = small - (hi - big);
                if lo != 0.0 {
                    partials[kept] = lo;
                    kept += 1;
                }
                x = hi;
            }
            partials.truncate(kept);
            partials.push(x);
        }
        let Some(mut n) = partials.len().checked_sub(1) else { return 0.0 };
        let mut hi = partials[n];
        let mut lo = 0.0;
        while n > 0 {
            n -= 1;
            let x = hi;
            let y = partials[n];
            hi = x + y;
            lo = y - (hi - x);
            if lo != 0.0 {
                break;
            }
        }
        if n > 0 && ((lo < 0.0 && partials[n - 1] < 0.0) || (lo > 0.0 && partials[n - 1] > 0.0)) {
            let y = lo * 2.0;
            let x = hi + y;
            if y == x - hi {
                hi = x;
            }
        }
        hi
    }

    /// Equal bit for bit (two NaNs are equal here).
    fn same(a: f64, b: f64) -> bool {
        (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits()
    }

    #[test]
    fn lanes_match_one_by_one() {
        let mut overflow64 = vec![1.0; 64];
        overflow64[0] = 1e308;
        overflow64[8] = 1e308; // lane 0 overflows
        overflow64[63] = -1e308;
        let mut inf_in_middle = random_values(1_000, 3);
        inf_in_middle[500] = INF;
        let cases: Vec<(&str, Vec<f64>)> = vec![
            ("10,001 random values", random_values(10_001, 1)),
            ("cancelling, repeated", [1e16, 1.0, -1e16].repeat(100)),
            ("overflow, 3 values", vec![1e308, 1e308, -1e308]),
            ("overflow in one lane, 64 values", overflow64),
            ("one inf among finite values", inf_in_middle),
            ("inf and -inf", vec![INF, 1.0, -INF]),
        ];
        for (name, values) in cases {
            let expect = one_by_one(&values);
            let got = exact_sum(&values).expect("a non-empty sum");
            assert!(same(got, expect), "{name}: lanes {got:?}, one by one {expect:?}");
        }
        assert_eq!(exact_sum(&[1e16, 1.0, -1e16].repeat(100)), Some(100.0));
    }

    #[test]
    fn retract_takes_values_out() {
        let a = [1.5, 2.25, -0.75];
        let b = [0.1, 0.2, 0.3];

        let mut acc = Exact::default();
        acc.update_batch(&[array(&a)]).unwrap();
        acc.retract_batch(&[array(&a)]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(None), "a frame emptied by retract is NULL");

        let mut acc = Exact::default();
        acc.update_batch(&[array(&[a, b].concat())]).unwrap();
        acc.retract_batch(&[array(&a)]).unwrap();
        let ScalarValue::Float64(Some(sum)) = acc.evaluate().unwrap() else { panic!("expected a sum") };
        assert!(same(sum, fsum(&b)), "{sum:?} is not the sum of b, {:?}", fsum(&b));
    }

    #[test]
    fn partial_with_infinite_sum_merges_to_infinite() {
        // A partial whose sum overflowed: its error is NaN inside, and its state hands on 0 instead.
        let mut overflowed = Exact::default();
        overflowed.update_batch(&[array(&[1e308, 1e308])]).unwrap();
        let state: Vec<ArrayRef> = overflowed.state().unwrap().iter().map(|s| s.to_array().unwrap()).collect();
        assert_eq!(state[1].as_primitive::<Float64Type>().value(0), 0.0);

        let mut finite = Exact::default();
        finite.update_batch(&[array(&[3.0])]).unwrap();
        let finite_state: Vec<ArrayRef> = finite.state().unwrap().iter().map(|s| s.to_array().unwrap()).collect();

        for order in [[&state, &finite_state], [&finite_state, &state]] {
            let mut merged = Exact::default();
            for partial in order {
                merged.merge_batch(partial).unwrap();
            }
            assert_eq!(merged.evaluate().unwrap(), ScalarValue::Float64(Some(INF)));
        }

        // A NaN error left in a partial with an infinite sum is not read.
        let (mut hi, mut lo) = (3.0, 0.0);
        add_partial(&mut hi, &mut lo, INF, f64::NAN);
        assert_eq!(total(hi, lo), INF);
    }

    #[test]
    fn groups_are_the_exact_sum_of_each_group() {
        // Three groups over 9,000 rows, with NULLs (every 7th value) and a filter. Group 2 has no row
        // the filter keeps, so it is NULL.
        let n = 9_000;
        let raw = random_values(n, 7);
        let groups: Vec<usize> = (0..n).map(|i| i % 3).collect();
        let values: Float64Array = (0..n).map(|i| (i % 7 != 0).then_some(raw[i])).collect();
        let filter: BooleanArray = (0..n).map(|i| Some(i % 5 != 0 && i % 3 != 2)).collect();
        let expect: Vec<Option<f64>> = (0..3)
            .map(|g| {
                let kept: Vec<f64> = (0..n)
                    .filter(|&i| groups[i] == g && filter.value(i) && values.is_valid(i))
                    .map(|i| raw[i])
                    .collect();
                (!kept.is_empty()).then(|| fsum(&kept))
            })
            .collect();
        assert!(expect[2].is_none());

        let check = |out: &ArrayRef, what: &str| {
            let sums = out.as_primitive::<Float64Type>();
            for g in 0..3 {
                match expect[g] {
                    None => assert!(sums.is_null(g), "{what}: group {g} should be NULL"),
                    Some(want) => assert!(!sums.is_null(g) && same(sums.value(g), want), "{what}: group {g} is {:?}, want {want:?}", sums.value(g)),
                }
            }
        };

        let mut whole = Groups::default();
        whole.update_batch(&[Arc::new(values.clone())], &groups, Some(&filter), 3).unwrap();
        check(&whole.evaluate(EmitTo::All).unwrap(), "update");

        // The same rows as two partials, merged: the states' sums and errors, as a second node would send them.
        let half = n / 2;
        let mut partials = Vec::new();
        for range in [0..half, half..n] {
            let mut part = Groups::default();
            let part_groups: Vec<usize> = groups[range.clone()].to_vec();
            let part_values: Float64Array = range.clone().map(|i| values.is_valid(i).then_some(raw[i])).collect();
            let part_filter: BooleanArray = range.clone().map(|i| Some(filter.value(i))).collect();
            part.update_batch(&[Arc::new(part_values)], &part_groups, Some(&part_filter), 3).unwrap();
            partials.push(part.state(EmitTo::All).unwrap());
        }
        let mut merged = Groups::default();
        for state in &partials {
            merged.merge_batch(state, &[0, 1, 2], 3).unwrap();
        }
        check(&merged.evaluate(EmitTo::All).unwrap(), "merge");
    }

    #[test]
    fn partial_with_infinite_sum_merges_to_infinite_per_group() {
        let mut overflowed = Groups::default();
        overflowed.update_batch(&[array(&[1e308, 1e308])], &[0, 0], None, 1).unwrap();
        let state = overflowed.state(EmitTo::All).unwrap();

        let mut finite = Groups::default();
        finite.update_batch(&[array(&[3.0])], &[0], None, 1).unwrap();
        let finite_state = finite.state(EmitTo::All).unwrap();

        for order in [[&state, &finite_state], [&finite_state, &state]] {
            let mut merged = Groups::default();
            for partial in order {
                merged.merge_batch(partial, &[0], 1).unwrap();
            }
            let out = merged.evaluate(EmitTo::All).unwrap();
            assert_eq!(out.as_primitive::<Float64Type>().value(0), INF);
        }
    }
}
