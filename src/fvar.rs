//! `var`, `stddev` (the sample's and the population's) and `avg` over DOUBLE that give the same answer whatever the
//! order, as `fsum` does for `sum`. DataFusion averages by a plain sum, and works a variance out by Welford's updates,
//! merging each partition's in whatever order the partitions finish, so their last digits changed from run to run at
//! every door. TPC-DS q39 keeps the rows whose `stddev_samp / avg` passes 1, so its rows could change.
//!
//! Here what they need is kept as sums exact to about 2⁻¹⁰⁶ in any order (`fsum::add`): the count, the values' sum,
//! and the sum of their squares, each square split exactly in two (`mul_add`). The variance is worked out from them
//! once, at the end, in double-double arithmetic, so values far from 0 keep their digits where a plain sum of squares
//! would cancel them. A group whose values are all one value answers 0, as before. Values of 2⁴⁸⁰ or more are summed
//! scaled by 2⁻⁶⁰⁰, exactly, so their squares don't overflow. An average is the sum over the count, divided in
//! double-double. Integers of 32 bits or fewer are taken as they are (`with_wholes`), and their sums are integers,
//! exact and as quick as a plain sum, in the same state columns (`Wholes`). DISTINCT, and windows whose frames slide
//! (they take values back out, in their ORDER BY's order), stay DataFusion's own.
use crate::fsum::{add, add_partial, error_of};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float64Array, Int64Array, PrimitiveArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Field, FieldRef, Float64Type, Int32Type, Int64Type,
};
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::{format_state_name, AggregateOrderSensitivity};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, ReversedUDAF, Signature, TypeSignature,
};
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Replaces `var` (`var_samp`, `variance`), `var_pop`, `stddev` (`stddev_samp`) and `stddev_pop` in a session. `avg`
/// takes `Of::Mean` in `avg.rs`.
pub fn register(ctx: &SessionContext) {
    use datafusion::functions_aggregate::{stddev, variance};
    for (inner, of, more) in [
        (variance::var_samp_udaf(), Of::Var { sample: true, root: false }, vec!["variance"]), // (Postgres's and DuckDB's name)
        (variance::var_pop_udaf(), Of::Var { sample: false, root: false }, vec![]),
        (stddev::stddev_udaf(), Of::Var { sample: true, root: true }, vec![]),
        (stddev::stddev_pop_udaf(), Of::Var { sample: false, root: true }, vec![]),
    ] {
        let aliases = inner.aliases().iter().cloned().chain(more.into_iter().map(String::from)).collect();
        let signature = with_wholes(inner.signature());
        ctx.register_udaf(AggregateUDF::from(Steady { inner, of, aliases, signature }));
    }
}

/// What is worked out of the moments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Of {
    Mean,
    Var { sample: bool, root: bool },
}

/// Does an aggregate of these arguments take the order-free path: over DOUBLE (or whole numbers, `whole`), without
/// DISTINCT?
pub(crate) fn steady(args: &AccumulatorArgs) -> bool {
    !args.is_distinct
        && args.return_type() == &DataType::Float64
        && args.expr_fields.first().is_some_and(|f| f.data_type() == &DataType::Float64 || is_whole(f.data_type()))
}

/// Integers of 32 bits or fewer: their sums, and their squares', are exact as i128s. (And NULL's type, kept as it is
/// by `with_wholes`: no values.)
pub(crate) fn is_whole(t: &DataType) -> bool {
    use DataType::*;
    matches!(t, Int8 | Int16 | Int32 | UInt8 | UInt16 | UInt32 | Null)
}

/// These aggregates take such integers as they are (`is_whole`), not cast to DOUBLE as DataFusion's do: their sums are
/// then kept as integers (`Wholes`), exact, so the same in any order, and as quick as a plain sum. (Summed as doubles,
/// each step's error carried, a group of many rows waits on each step: TPC-DS q22's ROLLUP puts 30 million values
/// into one group. And a cast that two aggregates share is taken out below them as a column of DOUBLE, as in q39.)
/// Every other type as `inner` takes it.
pub(crate) fn with_wholes(inner: &Signature) -> Signature {
    use DataType::*;
    let mut all: Vec<TypeSignature> = [Int8, Int16, Int32, UInt8, UInt16, UInt32].into_iter().map(|t| TypeSignature::Exact(vec![t])).collect();
    match &inner.type_signature {
        TypeSignature::OneOf(them) => all.extend(them.iter().cloned()),
        it => all.push(it.clone()),
    }
    Signature::one_of(all, inner.volatility)
}

/// Does the aggregate keep whole numbers (`Wholes`): integers as they are (`with_wholes`), or a DOUBLE cast from them?
fn whole(args: &AccumulatorArgs) -> bool {
    let Some(f) = args.expr_fields.first() else { return false };
    is_whole(f.data_type())
}

/// `make(args)`, for DataFusion's own accumulator over DOUBLE (DISTINCT, frames that slide): whole numbers taken as
/// they are (`with_wholes`) cast to DOUBLE on their way in.
pub(crate) fn as_double(args: AccumulatorArgs, make: impl FnOnce(AccumulatorArgs) -> Result<Box<dyn Accumulator>>) -> Result<Box<dyn Accumulator>> {
    if !args.expr_fields.first().is_some_and(|f| is_whole(f.data_type())) {
        return make(args);
    }
    let fields = [Arc::new(args.expr_fields[0].as_ref().clone().with_data_type(DataType::Float64))];
    let exprs: [Arc<dyn PhysicalExpr>; 1] = [Arc::new(CastExpr::new(args.exprs[0].clone(), DataType::Float64, None))];
    Ok(Box::new(AsDouble(make(AccumulatorArgs { expr_fields: &fields, exprs: &exprs, ..args })?)))
}

/// An accumulator over DOUBLE fed whole numbers (`as_double`).
#[derive(Debug)]
struct AsDouble(Box<dyn Accumulator>);

impl Accumulator for AsDouble {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> { self.0.update_batch(&[cast(&values[0], &DataType::Float64)?]) }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> { self.0.retract_batch(&[cast(&values[0], &DataType::Float64)?]) }
    fn supports_retract_batch(&self) -> bool { self.0.supports_retract_batch() }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> { self.0.merge_batch(states) }
    fn state(&mut self) -> Result<Vec<ScalarValue>> { self.0.state() }
    fn evaluate(&mut self) -> Result<ScalarValue> { self.0.evaluate() }
    fn size(&self) -> usize { self.0.size() + size_of::<Self>() }
}

/// DataFusion's variance or standard deviation, whose answers over DOUBLE are worked out of the moments (`Moments`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Steady {
    inner: Arc<AggregateUDF>,
    of: Of,
    aliases: Vec<String>,
    signature: Signature, // (`with_wholes`)
}

impl AggregateUDFImpl for Steady {
    fn name(&self) -> &str { self.inner.name() }
    fn aliases(&self) -> &[String] { &self.aliases }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> { self.inner.return_type(types) }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match (steady(&args), whole(&args)) {
            (true, true) => Ok(whole_one(self.of)),
            (true, false) => Ok(Box::new(One::new(self.of))),
            _ => as_double(args, |a| self.inner.accumulator(a)),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct { self.inner.state_fields(args) } else { Ok(state_fields(args.name, self.of, args.input_fields.first().is_some_and(|f| is_whole(f.data_type())))) }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool { !args.is_distinct }

    fn create_groups_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        if whole(&args) { Ok(wholes(self.of)) } else { Ok(Box::new(Many::new(self.of))) }
    }

    // (a frame that slides takes values back out: DataFusion's, in its ORDER BY's order)
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        as_double(args, |a| self.inner.create_sliding_accumulator(a))
    }

    fn reverse_expr(&self) -> ReversedUDAF { self.inner.inner().reverse_expr() }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity { self.inner.inner().order_sensitivity() }
    fn documentation(&self) -> Option<&Documentation> { self.inner.documentation() }
}

/// 2⁴⁸⁰: values from here on are summed scaled (`DOWN`), so that their squares, summed, stay finite.
const BIG: f64 = f64::from_bits((1023 + 480) << 52);
/// 2⁻⁶⁰⁰, and back (`UP`).
const DOWN: f64 = f64::from_bits((1023 - 600) << 52);
const UP: f64 = f64::from_bits((1023 + 600) << 52);

/// Whether a group's values are all one value. It doesn't depend on their order.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
enum Alike {
    #[default]
    None, // no value yet
    One,  // all `Moments::one`
    Two,  // two values, or one that isn't finite
}

/// One group's moments: its count, and its values' sum and sum of squares, each with its rounding error (`fsum`).
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Moments {
    n: i64,
    big: bool, // summed scaled by DOWN (a value of BIG or more)
    s1: f64,
    e1: f64,
    s2: f64,
    e2: f64,
    one: f64,
    alike: Alike,
}

impl Moments {
    fn put(&mut self, x: f64, squares: bool) {
        if squares && !self.big && x.abs() >= BIG {
            self.scale();
        }
        let v = if self.big { x * DOWN } else { x };
        add(&mut self.s1, &mut self.e1, v);
        if squares {
            let p = v * v;
            add(&mut self.s2, &mut self.e2, p);
            add(&mut self.s2, &mut self.e2, v.mul_add(v, -p)); // (what rounding took off the square)
        }
        self.see(x, Alike::One);
        self.n += 1;
    }

    fn see(&mut self, x: f64, alike: Alike) {
        match (self.alike, alike) {
            (_, Alike::None) => {}
            (Alike::None, Alike::One) if x.is_finite() => (self.alike, self.one) = (Alike::One, x),
            (Alike::One, Alike::One) if self.one == x => {}
            _ => self.alike = Alike::Two,
        }
    }

    /// The sums from here on scaled by DOWN: exact, but for parts too small to matter beside a value of BIG.
    fn scale(&mut self) {
        self.big = true;
        (self.s1, self.e1) = (self.s1 * DOWN, self.e1 * DOWN);
        (self.s2, self.e2) = (self.s2 * DOWN * DOWN, self.e2 * DOWN * DOWN);
    }

    /// Another part's moments added in, in this one's scale.
    fn merge(&mut self, o: &Moments) {
        if o.n == 0 {
            return;
        }
        if o.big && !self.big {
            self.scale();
        }
        let k = if self.big && !o.big { DOWN } else { 1.0 };
        add_partial(&mut self.s1, &mut self.e1, o.s1 * k, o.e1 * k);
        add_partial(&mut self.s2, &mut self.e2, o.s2 * k * k, o.e2 * k * k);
        self.see(o.one, o.alike);
        self.n += o.n;
    }

    fn answer(&self, of: Of) -> Option<f64> {
        match of {
            Of::Mean => (self.n > 0).then(|| quotient(self.s1, error_of(self.s1, self.e1), self.n as f64)),
            Of::Var { sample, root } => {
                let d = self.n - sample as i64;
                if d <= 0 {
                    return None; // (no values; or, for the sample's, one)
                }
                if self.alike == Alike::One {
                    return Some(0.0);
                }
                let v = self.m2() / d as f64;
                Some(match (root, self.big) {
                    (false, false) => v,
                    (false, true) => v * UP * UP,
                    (true, false) => v.sqrt(),
                    (true, true) => v.sqrt() * UP,
                })
            }
        }
    }

    /// The sum of the squares of the values less their mean, S2 - S1²/n, in double-double: it keeps the digits the
    /// subtraction cancels. Rounding can take a variance near 0 under it: then 0.
    fn m2(&self) -> f64 {
        let n = self.n as f64;
        let (a, b) = two_sum(self.s1, error_of(self.s1, self.e1));
        let p = a * a; // (S1², a² exactly as p + e, and 2ab)
        let e = a.mul_add(a, -p) + 2.0 * a * b;
        let q = p / n; // (S1²/n as q + r/n)
        let r = (-q).mul_add(n, p) + e;
        let (h, l) = two_sum(self.s2, -q);
        let m2 = h + (l + error_of(self.s2, self.e2) - r / n);
        if m2 < 0.0 { 0.0 } else { m2 }
    }

    /// One row's moments: of its value, or none (NULL, or filtered out).
    fn of_row(x: Option<f64>, squares: bool) -> Moments {
        let mut m = Moments::default();
        if let Some(x) = x {
            m.put(x, squares);
        }
        m
    }
}

/// a + b, and what rounding took off it (Knuth's two-sum).
fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    let bb = s - a;
    (s, (a - (s - bb)) + (b - bb))
}

/// (s + e) / n, in double-double: s / n, and the remainder's part.
fn quotient(s: f64, e: f64, n: f64) -> f64 {
    let q = s / n;
    if !q.is_finite() {
        return q;
    }
    q + ((-q).mul_add(n, s) + e) / n
}

/// The state's columns: the count, (for a variance) whether it is scaled, the sums and their errors, and (for a
/// variance) the one value all of them are (NULL when they aren't).
fn state_fields(name: &str, of: Of, whole: bool) -> Vec<FieldRef> {
    let f = |part: &str, t: DataType| Arc::new(Field::new(format_state_name(name, part), t, true));
    if whole {
        // whole numbers' (`Wholes`): the count and the exact sums, as many columns as DataFusion's own states
        return match of {
            Of::Mean => vec![f("count", DataType::Int64), f("sum", EXACT)],
            Of::Var { .. } => vec![f("count", DataType::Int64), f("sum", EXACT), f("squares", EXACT)],
        };
    }
    match of {
        Of::Mean => vec![f("count", DataType::Int64), f("sum", DataType::Float64), f("sum error", DataType::Float64)],
        Of::Var { .. } => vec![
            f("count", DataType::Int64),
            f("scaled", DataType::Boolean),
            f("sum", DataType::Float64),
            f("sum error", DataType::Float64),
            f("squares", DataType::Float64),
            f("squares error", DataType::Float64),
            f("alike", DataType::Float64),
        ],
    }
}

/// Moments as state columns (`state_fields`).
fn state_of(of: Of, all: &[Moments]) -> Vec<ArrayRef> {
    let f64s = |g: &dyn Fn(&Moments) -> f64| -> ArrayRef { Arc::new(all.iter().map(g).collect::<Float64Array>()) };
    let count: ArrayRef = Arc::new(all.iter().map(|m| m.n).collect::<Int64Array>());
    let (sum, error) = (f64s(&|m| m.s1), f64s(&|m| error_of(m.s1, m.e1)));
    match of {
        Of::Mean => vec![count, sum, error],
        Of::Var { .. } => {
            let scaled: ArrayRef = Arc::new(all.iter().map(|m| Some(m.big)).collect::<BooleanArray>());
            let alike: ArrayRef = Arc::new(all.iter().map(|m| (m.alike == Alike::One).then_some(m.one)).collect::<Float64Array>());
            vec![count, scaled, sum, error, f64s(&|m| m.s2), f64s(&|m| error_of(m.s2, m.e2)), alike]
        }
    }
}

/// Row `i` of state columns (`state_of`).
fn moments_at(of: Of, states: &[ArrayRef], i: usize) -> Moments {
    let f = |c: usize| states[c].as_primitive::<Float64Type>().value(i);
    let n = states[0].as_primitive::<Int64Type>();
    let n = if n.is_valid(i) { n.value(i) } else { 0 };
    match of {
        Of::Mean => Moments { n, s1: f(1), e1: f(2), alike: Alike::Two, ..Default::default() },
        Of::Var { .. } => {
            let alike = &states[6];
            let (alike, one) = match (n, alike.is_valid(i)) {
                (0, _) => (Alike::None, 0.0),
                (_, true) => (Alike::One, alike.as_primitive::<Float64Type>().value(i)),
                (_, false) => (Alike::Two, 0.0),
            };
            Moments { n, big: states[1].as_boolean().value(i), s1: f(2), e1: f(3), s2: f(4), e2: f(5), one, alike }
        }
    }
}

/// One group's (no GROUP BY, and a window over frames that don't slide).
#[derive(Debug)]
struct One {
    of: Of,
    m: Moments,
}

impl One {
    fn new(of: Of) -> One { One { of, m: Moments::default() } }
}

impl Accumulator for One {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let squares = self.of != Of::Mean;
        for x in values[0].as_primitive::<Float64Type>().iter().flatten() {
            self.m.put(x, squares);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for i in 0..states[0].len() {
            self.m.merge(&moments_at(self.of, states, i));
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        state_of(self.of, &[self.m]).iter().map(|c| ScalarValue::try_from_array(c, 0)).collect()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> { Ok(ScalarValue::Float64(self.m.answer(self.of))) }

    fn size(&self) -> usize { size_of::<Self>() }
}

/// Each group's (GROUP BY).
/// Each group's value of one kind, the first groups taken out as they are emitted (`EmitTo::First`) without moving the
/// rest each time: the rest moves once half is gone. (DataFusion's `take_needed` moves the rest at every emit, and an
/// ordered aggregate's last groups go out a batch at a time: over many groups, TPC-DS q39's, the moving outweighed
/// the summing.)
#[derive(Debug, Default)]
struct Groups<T> {
    v: Vec<T>,
    from: usize,
}

impl<T: Clone + Default> Groups<T> {
    fn grow(&mut self, total: usize) { self.v.resize(self.from + total, T::default()) }

    fn take(&mut self, emit: EmitTo) -> Vec<T> {
        match emit {
            EmitTo::All => {
                let mut all = std::mem::take(&mut self.v);
                all.drain(..std::mem::take(&mut self.from));
                all
            }
            EmitTo::First(n) => {
                let first = self.v[self.from..self.from + n].to_vec();
                self.from += n;
                if 2 * self.from >= self.v.len() {
                    self.v.drain(..std::mem::take(&mut self.from));
                }
                first
            }
        }
    }

    fn capacity(&self) -> usize { self.v.capacity() }
}

impl<T> From<Vec<T>> for Groups<T> {
    fn from(v: Vec<T>) -> Groups<T> { Groups { v, from: 0 } }
}

impl<T> std::ops::Deref for Groups<T> {
    type Target = [T];
    fn deref(&self) -> &[T] { &self.v[self.from..] }
}

impl<T> std::ops::DerefMut for Groups<T> {
    fn deref_mut(&mut self) -> &mut [T] { &mut self.v[self.from..] }
}

#[derive(Debug)]
struct Many {
    of: Of,
    m: Groups<Moments>,
}

impl Many {
    fn new(of: Of) -> Many { Many { of, m: Groups::default() } }
}

/// Is row `i` kept by the aggregate's FILTER, if it has one?
fn kept(filter: Option<&BooleanArray>, i: usize) -> bool { filter.is_none_or(|f| f.is_valid(i) && f.value(i)) }

/// `f(i)` for each row kept: not NULL, and the FILTER's if there is one; over NULLs, by the set bits of their bitmap,
/// as DataFusion's own accumulators step over them. (A batch with neither, the callers zip, quicker still.)
fn kept_rows(values: &dyn Array, filter: Option<&BooleanArray>, f: impl FnMut(usize)) {
    match (values.nulls(), filter) {
        (Some(nulls), None) => nulls.valid_indices().for_each(f),
        (None, None) => (0..values.len()).for_each(f),
        (_, Some(_)) => (0..values.len()).filter(|&i| values.is_valid(i) && kept(filter, i)).for_each(f),
    }
}

impl GroupsAccumulator for Many {
    fn update_batch(&mut self, values: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, total: usize) -> Result<()> {
        self.m.grow(total);
        let (values, squares, m) = (values[0].as_primitive::<Float64Type>(), self.of != Of::Mean, &mut *self.m);
        if values.null_count() == 0 && filter.is_none() {
            for (&g, &x) in groups.iter().zip(values.values().iter()) {
                m[g].put(x, squares);
            }
            return Ok(());
        }
        kept_rows(values, filter, |i| m[groups[i]].put(values.value(i), squares));
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], total: usize) -> Result<()> {
        self.m.grow(total);
        let (of, m) = (self.of, &mut *self.m);
        for (i, &g) in groups.iter().enumerate() {
            m[g].merge(&moments_at(of, states, i));
        }
        Ok(())
    }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let of = self.of;
        Ok(Arc::new(self.m.take(emit).iter().map(|m| m.answer(of)).collect::<Float64Array>()))
    }

    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> { Ok(state_of(self.of, &self.m.take(emit))) }

    /// Rows as states of one value each (when grouping first doesn't pay).
    fn convert_to_state(&self, values: &[ArrayRef], filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        let (values, squares) = (values[0].as_primitive::<Float64Type>(), self.of != Of::Mean);
        let rows: Vec<Moments> = (0..values.len()).map(|i| Moments::of_row((values.is_valid(i) && kept(filter, i)).then(|| values.value(i)), squares)).collect();
        Ok(state_of(self.of, &rows))
    }


    fn size(&self) -> usize { self.m.capacity() * size_of::<Moments>() }
}

/// For `avg.rs`: the order-free average's accumulators and state.
/// Each group's average (GROUP BY): its count and its sum with the sum's error, a column each, as DataFusion keeps
/// them (a group's `Moments` would carry a variance's parts too, and the average is the hot one).
#[derive(Debug, Default)]
struct Means {
    n: Groups<i64>,
    hi: Groups<f64>,
    lo: Groups<f64>,
}

impl Means {
    fn grow(&mut self, total: usize) {
        self.n.grow(total);
        self.hi.grow(total);
        self.lo.grow(total);
    }
}

impl GroupsAccumulator for Means {
    fn update_batch(&mut self, values: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, total: usize) -> Result<()> {
        self.grow(total);
        let values = values[0].as_primitive::<Float64Type>();
        let (n, hi, lo) = (&mut *self.n, &mut *self.hi, &mut *self.lo);
        if values.null_count() == 0 && filter.is_none() {
            for (&g, &x) in groups.iter().zip(values.values().iter()) {
                n[g] += 1;
                add(&mut hi[g], &mut lo[g], x);
            }
            return Ok(());
        }
        kept_rows(values, filter, |i| {
            let g = groups[i];
            n[g] += 1;
            add(&mut hi[g], &mut lo[g], values.value(i));
        });
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], total: usize) -> Result<()> {
        self.grow(total);
        let (count, sum, error) = (states[0].as_primitive::<Int64Type>(), states[1].as_primitive::<Float64Type>(), states[2].as_primitive::<Float64Type>());
        let (n, hi, lo) = (&mut *self.n, &mut *self.hi, &mut *self.lo);
        for (i, &g) in groups.iter().enumerate() {
            if count.is_valid(i) && count.value(i) > 0 {
                n[g] += count.value(i);
                add_partial(&mut hi[g], &mut lo[g], sum.value(i), error.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let (n, hi, lo) = (self.n.take(emit), self.hi.take(emit), self.lo.take(emit));
        let means = n.iter().zip(hi.iter().zip(&lo)).map(|(&n, (&h, &l))| (n > 0).then(|| quotient(h, error_of(h, l), n as f64)));
        Ok(Arc::new(means.collect::<Float64Array>()))
    }

    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        let (n, hi, lo) = (self.n.take(emit), self.hi.take(emit), self.lo.take(emit));
        let errors: Vec<f64> = hi.iter().zip(&lo).map(|(&h, &l)| error_of(h, l)).collect();
        Ok(vec![Arc::new(Int64Array::from(n)), Arc::new(Float64Array::from(hi)), Arc::new(Float64Array::from(errors))])
    }

    /// Rows as states of one value each (when grouping first doesn't pay).
    fn convert_to_state(&self, values: &[ArrayRef], filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<Float64Type>();
        let count: Vec<i64> = (0..values.len()).map(|i| (values.is_valid(i) && kept(filter, i)) as i64).collect();
        let sum: Vec<f64> = (0..values.len()).map(|i| if count[i] == 1 { values.value(i) } else { 0.0 }).collect();
        Ok(vec![Arc::new(Int64Array::from(count)), Arc::new(Float64Array::from(sum)), Arc::new(Float64Array::from(vec![0.0; values.len()]))])
    }

    fn size(&self) -> usize { self.n.capacity() * 8 + (self.hi.capacity() + self.lo.capacity()) * 8 }
}

pub(crate) fn mean_accumulator(args: &AccumulatorArgs) -> Box<dyn Accumulator> {
    if whole(args) { whole_one(Of::Mean) } else { Box::new(One::new(Of::Mean)) }
}
pub(crate) fn mean_groups(args: &AccumulatorArgs) -> Box<dyn GroupsAccumulator> {
    if whole(args) { wholes(Of::Mean) } else { Box::<Means>::default() }
}

/// An exact integer sum as a sum and its error, as the state of a sum of doubles holds them (`state_fields`): exact
/// below 2¹⁰⁶.
fn split(s: i128) -> (f64, f64) {
    if s.unsigned_abs() < 1 << 62 {
        let hi = s as i64 as f64; // (the same, in i64s: one instruction each, where i128s are calls)
        return (hi, (s as i64 - hi as i64) as f64);
    }
    wide_split(s)
}

/// An i128 rounded to the nearest double (through an i64 where it fits: the same rounding, one instruction).
fn double(x: i128) -> f64 {
    if x.unsigned_abs() < 1 << 62 { x as i64 as f64 } else { wide_double(x) }
}

// The i128 paths, out of line: inlined, the compiler works both sides out and keeps one, a call each time.
#[cold]
#[inline(never)]
fn wide_split(s: i128) -> (f64, f64) {
    let hi = s as f64;
    (hi, (s - hi as i128) as f64)
}
#[cold]
#[inline(never)]
fn wide_double(x: i128) -> f64 { x as f64 }

/// The answer worked out of whole numbers' count and exact sums (`Wholes`).
fn whole_answer(of: Of, n: i64, s1: i128, s2: i128) -> Option<f64> {
    match of {
        Of::Mean => (n > 0).then(|| { let (h, l) = split(s1); quotient(h, l, n as f64) }),
        Of::Var { sample, root } => {
            let d = n - sample as i64;
            if d <= 0 {
                return None;
            }
            let v = whole_m2(n, s1, s2) / d as f64;
            Some(if root { v.sqrt() } else { v })
        }
    }
}

/// S2 - S1²/n for exact integer sums: (n·S2 - S1²) / n, the integer worked out exactly and rounded once, so a group
/// of one value many times is 0, and the cancelling loses nothing. Where n·S2 doesn't fit 128 bits, with S1 = qn + r
/// (0 <= r < n), S2 - q²n - 2qr, exactly, less r²/n; past that (rows of 32-bit values beyond any table), the
/// double-double of `Moments::m2`.
fn whole_m2(n: i64, s1: i128, s2: i128) -> f64 {
    if let (Ok(s1), Ok(s2)) = (i64::try_from(s1), i64::try_from(s2)) {
        // (sums of 64 bits: each product one instruction and below 2¹²⁶, where the checked ones of i128s are calls)
        let num = n as i128 * s2 as i128 - s1 as i128 * s1 as i128;
        return double(num.max(0)) / n as f64;
    }
    let n128 = n as i128;
    if let Some(num) = n128.checked_mul(s2).zip(s1.checked_mul(s1)).and_then(|(a, b)| a.checked_sub(b)) {
        return double(num.max(0)) / n as f64;
    }
    let (q, r) = (s1.div_euclid(n128), s1.rem_euclid(n128));
    let exact = q.checked_mul(q).and_then(|qq| qq.checked_mul(n128)).and_then(|a| s2.checked_sub(a)).and_then(|a| a.checked_sub(q.checked_mul(r)?.checked_mul(2)?));
    let m2 = match exact {
        Some(a) => a as f64 - (r as f64) * (r as f64) / n as f64,
        None => {
            let ((s1, e1), (s2, e2)) = (split(s1), split(s2));
            Moments { n, s1, e1, s2, e2, alike: Alike::Two, ..Default::default() }.m2()
        }
    };
    if m2 < 0.0 { 0.0 } else { m2 }
}

/// Each group's count, and its whole numbers' sum and sum of squares as integers (`whole`).
#[derive(Debug)]
struct Wholes<S> {
    of: Of,
    g: Groups<S>,
}

/// One group's count and exact sums, side by side: a row touches one place, not one for each (over many groups, each
/// a miss in the cache). Packed: an i128 alone would align them to 16 bytes.
trait Sums: Copy + Default + std::fmt::Debug + Send + Sync + 'static {
    /// Whether the squares are summed too (a variance's).
    const SQUARES: bool;
    fn put(&mut self, n: i64, s1: i128, s2: i128);
    /// The count, the sum and the sum of squares (0 where not summed).
    fn get(self) -> (i64, i128, i128);
}

/// An average's, 24 bytes.
#[repr(C, packed(8))]
#[derive(Clone, Copy, Debug, Default)]
struct MeanSums {
    n: i64,
    s1: i128,
}

impl Sums for MeanSums {
    const SQUARES: bool = false;
    #[inline(always)]
    fn put(&mut self, n: i64, s1: i128, _: i128) {
        self.n += n;
        self.s1 = self.s1 + s1;
    }
    #[inline(always)]
    fn get(self) -> (i64, i128, i128) { (self.n, self.s1, 0) }
}

/// A variance's, 40 bytes.
#[repr(C, packed(8))]
#[derive(Clone, Copy, Debug, Default)]
struct VarSums {
    n: i64,
    s1: i128,
    s2: i128,
}

impl Sums for VarSums {
    const SQUARES: bool = true;
    #[inline(always)]
    fn put(&mut self, n: i64, s1: i128, s2: i128) {
        self.n += n;
        self.s1 = self.s1 + s1;
        self.s2 = self.s2 + s2;
    }
    #[inline(always)]
    fn get(self) -> (i64, i128, i128) { (self.n, self.s1, self.s2) }
}

/// `Wholes` with the sums `of` needs.
fn wholes(of: Of) -> Box<dyn GroupsAccumulator> {
    if of == Of::Mean { Box::new(Wholes::<MeanSums>::new(of)) } else { Box::new(Wholes::<VarSums>::new(of)) }
}

/// `Whole` with the sums `of` needs.
fn whole_one(of: Of) -> Box<dyn Accumulator> {
    if of == Of::Mean { Box::new(Whole::<MeanSums>::new(of)) } else { Box::new(Whole::<VarSums>::new(of)) }
}

impl<S: Sums> Wholes<S> {
    fn new(of: Of) -> Self { Wholes { of, g: Groups::default() } }

    fn grow(&mut self, total: usize) {
        self.g.grow(total);
    }

    /// Rows `values` into `groups`, those kept (not NULL, and the FILTER's); their squares too where summed (each
    /// below 2⁶⁴ as a u64).
    fn put_all<T: ArrowPrimitiveType>(&mut self, values: &PrimitiveArray<T>, groups: &[usize], filter: Option<&BooleanArray>)
    where
        T::Native: AsWhole,
    {
        let sums = &mut *self.g;
        let mut put = |g: usize, x: i64| {
            let a = x.unsigned_abs();
            sums[g].put(1, x as i128, if S::SQUARES { (a * a) as i128 } else { 0 });
        };
        if values.null_count() == 0 && filter.is_none() {
            for (&g, &x) in groups.iter().zip(values.values().iter()) {
                put(g, x.whole());
            }
            return;
        }
        kept_rows(values, filter, |i| put(groups[i], values.value(i).whole()));
    }

    /// Rows of whole numbers (`whole`) into `groups`.
    fn put_values(&mut self, values: &ArrayRef, groups: &[usize], filter: Option<&BooleanArray>) -> Result<()> {
        match native(values)? {
            None => {}
            Some(v) if v.data_type() == &DataType::Int32 => self.put_all(v.as_primitive::<Int32Type>(), groups, filter),
            Some(v) => self.put_all(v.as_primitive::<Int64Type>(), groups, filter),
        }
        Ok(())
    }

    /// States' rows added in, row `i` to group `groups[i]`.
    fn merge_states(&mut self, states: &[ArrayRef], groups: &[usize]) {
        let count = states[0].as_primitive::<Int64Type>();
        let sums = states[1].as_primitive::<Decimal128Type>().values();
        let squares = S::SQUARES.then(|| states[2].as_primitive::<Decimal128Type>().values());
        let all = &mut *self.g;
        for (i, &g) in groups.iter().enumerate() {
            if count.is_null(i) {
                continue; // (a NULL state is of no values)
            }
            all[g].put(count.value(i), sums[i], squares.map_or(0, |q| q[i]));
        }
    }

    /// The states of groups `range`, in the columns `state_fields` names.
    fn states(&self, range: std::ops::Range<usize>) -> Vec<ArrayRef> {
        let sums = &self.g[range];
        let count: ArrayRef = Arc::new(sums.iter().map(|s| s.get().0).collect::<Int64Array>());
        let sum = exact(sums.iter().map(|s| s.get().1).collect());
        if S::SQUARES { vec![count, sum, exact(sums.iter().map(|s| s.get().2).collect())] } else { vec![count, sum] }
    }

    /// Takes the first groups `emit` asks for out.
    fn take(&mut self, emit: EmitTo) -> Self {
        Wholes { of: self.of, g: self.g.take(emit).into() }
    }

    fn answer(&self, g: usize) -> Option<f64> {
        let (n, s1, s2) = self.g[g].get();
        whole_answer(self.of, n, s1, s2)
    }
}

impl<S: Sums> GroupsAccumulator for Wholes<S> {
    fn update_batch(&mut self, values: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, total: usize) -> Result<()> {
        self.grow(total);
        self.put_values(&values[0], groups, filter)
    }

    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], total: usize) -> Result<()> {
        self.grow(total);
        self.merge_states(states, groups);
        Ok(())
    }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let taken = self.take(emit);
        Ok(Arc::new((0..taken.g.len()).map(|g| taken.answer(g)).collect::<Float64Array>()))
    }

    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        let taken = self.take(emit);
        Ok(taken.states(0..taken.g.len()))
    }

    /// Rows as states of one value each (when grouping first doesn't pay).
    fn convert_to_state(&self, values: &[ArrayRef], filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        Ok(match native(&values[0])? {
            None => {
                let mut none = Self::new(self.of); // (no values: each row's state is of none)
                none.grow(values[0].len());
                none.states(0..values[0].len())
            }
            Some(v) if v.data_type() == &DataType::Int32 => row_states(v.as_primitive::<Int32Type>(), filter, S::SQUARES),
            Some(v) => row_states(v.as_primitive::<Int64Type>(), filter, S::SQUARES),
        })
    }

    fn size(&self) -> usize { self.g.capacity() * size_of::<S>() }
}

/// Whole numbers as one of the two types their loops are for (`put_all`, `row_states`): INT as it is, the narrower ones as
/// INT, and UInt32 as BIGINT (a square still below 2⁶⁴); none for NULLs. Two loops where there were seven: the page's
/// download stays within its budget (`tools/web_check.mjs`).
fn native(values: &ArrayRef) -> Result<Option<ArrayRef>> {
    Ok(match values.data_type() {
        DataType::Null => None,
        DataType::Int32 => Some(values.clone()),
        DataType::UInt32 => Some(cast(values, &DataType::Int64)?),
        t if is_whole(t) => Some(cast(values, &DataType::Int32)?),
        other => return internal_err!("{other} is not a whole number type"),
    })
}

/// Rows as the states of one value each, as `Wholes::states` gives them (`convert_to_state`: when grouping first
/// doesn't pay, every row), straight into the columns; a row not kept (NULL, or not the FILTER's) is a state of none.
fn row_states<T: ArrowPrimitiveType>(values: &PrimitiveArray<T>, filter: Option<&BooleanArray>, squared: bool) -> Vec<ArrayRef>
where
    T::Native: AsWhole,
{
    let (len, all) = (values.len(), values.null_count() == 0 && filter.is_none());
    let (mut count, mut sum) = (Vec::with_capacity(len), Vec::with_capacity(len));
    let mut squares = Vec::with_capacity(if squared { len } else { 0 });
    for (i, &x) in values.values().iter().enumerate() {
        let k = all || (values.is_valid(i) && kept(filter, i));
        let x = if k { x.whole() } else { 0 };
        count.push(k as i64);
        sum.push(x as i128);
        if squared {
            squares.push((x.unsigned_abs() * x.unsigned_abs()) as i128); // (each below 2³²: below 2⁶⁴)
        }
    }
    let (count, sum): (ArrayRef, ArrayRef) = (Arc::new(Int64Array::from(count)), exact(sum));
    if squared { vec![count, sum, exact(squares)] } else { vec![count, sum] }
}

/// A whole number (`native`'s) as an i64: one instruction, where to an i128 is a call.
trait AsWhole: Copy {
    fn whole(self) -> i64;
}

macro_rules! whole_as_i64 {
    ($($t:ty),*) => { $(impl AsWhole for $t { #[inline(always)] fn whole(self) -> i64 { self as i64 } })* };
}
whole_as_i64!(i32, i64);

/// One group's whole numbers (no GROUP BY, and a window over frames that don't slide: its answer asked after each
/// row), as `Wholes` keeps them.
#[derive(Debug)]
struct Whole<S>(Wholes<S>);

impl<S: Sums> Whole<S> {
    fn new(of: Of) -> Self {
        let mut w = Wholes::new(of);
        w.grow(1);
        Whole(w)
    }
}

impl<S: Sums> Accumulator for Whole<S> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.put_values(&values[0], &vec![0; values[0].len()], None)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge_states(states, &vec![0; states[0].len()]);
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> { self.0.states(0..1).iter().map(|c| ScalarValue::try_from_array(c, 0)).collect() }

    fn evaluate(&mut self) -> Result<ScalarValue> { Ok(ScalarValue::Float64(self.0.answer(0))) }

    fn size(&self) -> usize { size_of::<Self>() + self.0.size() }
}
pub(crate) fn mean_state(name: &str, whole: bool) -> Vec<FieldRef> { state_fields(name, Of::Mean, whole) }

/// Exact sums in a state: integers of up to 38 digits (an i128 each).
const EXACT: DataType = DataType::Decimal128(38, 0);

fn exact(sums: Vec<i128>) -> ArrayRef { Arc::new(Decimal128Array::from(sums).with_data_type(EXACT)) }

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::SessionConfig;

    /// Values with full mantissas around `at`. A xorshift generator, so the same values every run.
    fn values(n: usize, at: f64, seed: u64) -> Vec<f64> {
        let mut x = seed;
        (0..n).map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            at + ((x >> 11) as f64 / (1u64 << 53) as f64 - 0.5) * 2.0
        }).collect()
    }

    /// The variance of `xs` by exact arithmetic, rounded at its end: the reference. (The values are multiples of 2⁻³⁰
    /// of up to 2²⁰, so their sums are exact as i128 multiples of 2⁻⁶⁰.)
    fn exact_var(xs: &[f64], sample: bool) -> f64 {
        let unit = 2f64.powi(30);
        let ints: Vec<i128> = xs.iter().map(|&x| { let v = x * unit; assert_eq!(v.fract(), 0.0, "{x}"); v as i128 }).collect();
        let (s1, s2): (i128, i128) = ints.iter().fold((0, 0), |(a, b), &v| (a + v, b + v * v));
        let n = xs.len() as i128;
        let num = n * s2 - s1 * s1; // n²·m2·2⁸⁰
        let d = n * (n - sample as i128);
        num as f64 / d as f64 / unit / unit
    }

    /// Moments over `xs`, split into `parts` parts merged in the order given.
    fn split(xs: &[f64], parts: &[std::ops::Range<usize>], of: Of) -> f64 {
        let mut whole = Moments::default();
        for r in parts {
            let mut m = Moments::default();
            for &x in &xs[r.clone()] {
                m.put(x, of != Of::Mean);
            }
            let state = state_of(of, &[m]);
            whole.merge(&moments_at(of, &state, 0));
        }
        whole.answer(of).unwrap()
    }

    /// The same values in any order and split into any parts give the same bits; and the variance is the exact
    /// one, rounded, though the values are far from 0 (a plain sum of squares would keep none of its digits).
    #[test]
    fn any_order_gives_the_same_bits() {
        for at in [0.0, 1000.0, 1e6] {
            let xs: Vec<f64> = values(4000, at, 7).into_iter().map(|x| (x * 2f64.powi(30)).round() / 2f64.powi(30)).collect();
            let mut rev = xs.clone();
            rev.reverse();
            for of in [Of::Mean, Of::Var { sample: true, root: false }, Of::Var { sample: false, root: true }] {
                let a = split(&xs, &[0..4000], of);
                assert_eq!(a.to_bits(), split(&rev, &[0..4000], of).to_bits(), "{of:?} at {at}");
                assert_eq!(a.to_bits(), split(&xs, &[2500..4000, 0..1000, 1000..2500], of).to_bits(), "{of:?} at {at}");
                assert_eq!(a.to_bits(), split(&rev, &[0..1, 1..3999, 3999..4000], of).to_bits(), "{of:?} at {at}");
            }
            // (a GROUP BY's averages, kept apart from the moments, in parts merged out of order: the same bits)
            let (mut a1, mut a2, mut all) = (Means::default(), Means::default(), Means::default());
            a1.update_batch(&[Arc::new(Float64Array::from(xs[..1500].to_vec()))], &[0; 1500], None, 1).unwrap();
            a2.update_batch(&[Arc::new(Float64Array::from(xs[1500..].to_vec()))], &[0; 2500], None, 1).unwrap();
            all.merge_batch(&a2.state(EmitTo::All).unwrap(), &[0], 1).unwrap();
            all.merge_batch(&a1.state(EmitTo::All).unwrap(), &[0], 1).unwrap();
            let means = all.evaluate(EmitTo::All).unwrap();
            assert_eq!(means.as_primitive::<Float64Type>().value(0).to_bits(), split(&xs, &[0..4000], Of::Mean).to_bits(), "at {at}");
            let exact = exact_var(&xs, true);
            let got = split(&xs, &[0..4000], Of::Var { sample: true, root: false });
            assert!((got - exact).abs() <= exact * 1e-15, "at {at}: {got} for {exact}");
        }
    }

    /// One value, many times, is a variance of 0 and its own average; one value is no sample's variance; nothing is
    /// no average; and a value past 2⁴⁸⁰ is summed scaled, its square finite.
    #[test]
    fn edges_answer_as_before() {
        let var = Of::Var { sample: true, root: false };
        let pop = Of::Var { sample: false, root: false };
        let m = |xs: &[f64]| { let mut m = Moments::default(); xs.iter().for_each(|&x| m.put(x, true)); m };
        assert_eq!(m(&[0.1, 0.1, 0.1]).answer(var), Some(0.0));
        assert_eq!(m(&[0.1, 0.1, 0.1]).answer(Of::Mean), Some(0.1));
        assert_eq!(m(&[0.1]).answer(var), None);
        assert_eq!(m(&[0.1]).answer(pop), Some(0.0));
        assert_eq!(m(&[]).answer(Of::Mean), None);
        assert_eq!(m(&[]).answer(pop), None);
        assert_eq!(m(&[1.0, 1.0, 1.0, 2.0]).answer(pop), Some(0.1875));
        assert!(m(&[1.0, f64::INFINITY]).answer(var).unwrap().is_nan());
        assert!(m(&[f64::NAN]).answer(pop).unwrap().is_nan());
        let big = m(&[1e200, 3e200]).answer(Of::Var { sample: true, root: true }).unwrap();
        assert!((big - 2f64.sqrt() * 1e200).abs() <= 1e185, "{big}");
        let mut late = m(&[1e200]);
        late.merge(&m(&[3e200, 1.0]));
        let mut early = m(&[3e200, 1.0]);
        early.merge(&m(&[1e200]));
        assert_eq!(late.answer(var), early.answer(var));
    }

    /// Whole numbers (an INTEGER column's) summed as integers: any order and any parts give the same bits, a
    /// group of one value answers 0, and the variance is the exact one, rounded, as the doubles' path gives it.
    #[test]
    fn whole_numbers_sum_exactly() {
        let xs: Vec<f64> = (0..5000i64).map(|i| ((i * 7919) % 100_003 - 50_000 + 2_000_000_000) as f64).collect();
        let col = |v: &[f64]| -> ArrayRef { Arc::new(Int32Array::from(v.iter().map(|&x| x as i32).collect::<Vec<_>>())) };
        let var = Of::Var { sample: true, root: false };
        for of in [Of::Mean, var, Of::Var { sample: false, root: true }] {
            let run = |parts: &[std::ops::Range<usize>], rev: bool| -> f64 {
                let mut states: Vec<_> = parts.iter().map(|r| {
                    let mut w = wholes(of);
                    w.update_batch(&[col(&xs[r.clone()])], &vec![0; r.len()], None, 1).unwrap();
                    w.state(EmitTo::All).unwrap()
                }).collect();
                if rev {
                    states.reverse();
                }
                let mut all = whole_one(of);
                states.iter().for_each(|s| all.merge_batch(s).unwrap());
                let ScalarValue::Float64(Some(v)) = all.evaluate().unwrap() else { panic!() };
                v
            };
            let a = run(&[0..5000], false);
            assert_eq!(a.to_bits(), run(&[0..1, 1..4000, 4000..5000], true).to_bits(), "{of:?}");
            assert_eq!(a.to_bits(), run(&[2500..5000, 0..2500], false).to_bits(), "{of:?}");
            let doubles = split(&xs, &[0..5000], of);
            assert!((a - doubles).abs() <= doubles.abs() * 1e-15, "{of:?}: {a} for {doubles}");
        }
        let ints: Vec<i128> = xs.iter().map(|&x| x as i128).collect();
        let (s1, s2) = ints.iter().fold((0, 0), |(a, b): (i128, i128), &v| (a + v, b + v * v));
        let n = ints.len() as i128;
        let exact = (n * s2 - s1 * s1) as f64 / (n * (n - 1)) as f64;
        let got = whole_answer(var, n as i64, s1, s2).unwrap();
        assert!((got - exact).abs() <= exact * 1e-15, "{got} for {exact}");
        // (one value many times; one value; nothing; values below 0: -3, -3, -4)
        assert_eq!(whole_answer(var, 100, 100 * 2_000_000_007, 100 * 2_000_000_007i128.pow(2)), Some(0.0));
        assert_eq!(whole_answer(var, 1, 5, 25), None);
        // (n·S2 past 128 bits: S2 - q²n - 2qr instead, still exact)
        let (n, x) = (1i64 << 40, i32::MAX as i128);
        assert_eq!(whole_answer(var, n, n as i128 * x, n as i128 * x * x), Some(0.0));
        let got = whole_answer(Of::Var { sample: false, root: false }, n, n as i128 * x + 1, n as i128 * x * x + 2 * x + 1).unwrap();
        let want = (1.0 - 1.0 / n as f64) / n as f64; // ((n - 1) values of x and one of x + 1)
        assert!((got - want).abs() <= want * 1e-15, "{got} for {want}");
        assert_eq!(whole_answer(Of::Var { sample: false, root: false }, 1, 5, 25), Some(0.0));
        assert_eq!(whole_answer(Of::Mean, 0, 0, 0), None);
        assert!((whole_answer(Of::Var { sample: false, root: false }, 3, -10, 34).unwrap() - 2.0 / 9.0).abs() < 1e-16);
        assert_eq!(whole_answer(Of::Mean, 3, -10, 34), Some(-10.0 / 3.0));
        for s in [0i128, 1, -1, 1 << 80, -(1 << 100) + 12345] {
            let (h, l) = super::split(s);
            assert_eq!(h as i128 + l as i128, s); // (exact below 2¹⁰⁶)
        }
    }

    /// Groups emitted a few at a time come out as all at once would, and groups added after an emit land after the
    /// rest.
    #[test]
    fn groups_emitted_in_parts_come_out_in_order() {
        let mut g: Groups<i64> = Groups::default();
        g.grow(10);
        g.iter_mut().enumerate().for_each(|(i, v)| *v = i as i64);
        assert_eq!(g.take(EmitTo::First(3)), vec![0, 1, 2]);
        assert_eq!(g.take(EmitTo::First(3)), vec![3, 4, 5]); // (here the rest moves)
        g.grow(6);
        g[4] = 40;
        g[5] = 50;
        assert_eq!(g.take(EmitTo::First(1)), vec![6]);
        assert_eq!(g.take(EmitTo::All), vec![7, 8, 9, 40, 50]);
        assert!(g.is_empty());
        let mut w = Wholes::<VarSums>::new(Of::Var { sample: false, root: false });
        let xs: ArrayRef = Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>()));
        let groups: Vec<usize> = (0..100).map(|i| i % 10).collect();
        w.update_batch(&[xs], &groups, None, 10).unwrap();
        let mut parts: Vec<f64> = vec![];
        for emit in [EmitTo::First(4), EmitTo::First(4), EmitTo::All] {
            parts.extend(w.evaluate(emit).unwrap().as_primitive::<Float64Type>().values().iter());
        }
        assert_eq!(parts, vec![825.0; 10]); // (each group: 10 values 10 apart)
    }

    /// Rows turned straight into states (`convert_to_state`) are the states each row grouped alone gives, NULLs and a
    /// FILTER's rows left out included, and they merge to the same answers.
    #[test]
    fn rows_as_states_are_rows_grouped_alone() {
        assert_eq!((size_of::<MeanSums>(), size_of::<VarSums>()), (24, 40));
        let xs: ArrayRef = Arc::new(Int32Array::from((0..200).map(|i| (i % 7 != 3).then_some(i * 40_000 - 4_000_000)).collect::<Vec<_>>()));
        let filter = BooleanArray::from((0..200).map(|i| (i % 5 != 0).then_some(i % 11 != 0)).collect::<Vec<_>>());
        for of in [Of::Mean, Of::Var { sample: true, root: true }] {
            for f in [None, Some(&filter)] {
                let mut alone = wholes(of);
                alone.update_batch(&[xs.clone()], &(0..200).collect::<Vec<_>>(), f, 200).unwrap();
                let rows = wholes(of).convert_to_state(&[xs.clone()], f).unwrap();
                assert_eq!(rows, alone.state(EmitTo::All).unwrap());
                let mut merged = wholes(of);
                merged.merge_batch(&rows, &(0..200).map(|i| i % 3).collect::<Vec<_>>(), 3).unwrap();
                let mut direct = wholes(of);
                direct.update_batch(&[xs.clone()], &(0..200).map(|i| i % 3).collect::<Vec<_>>(), f, 3).unwrap();
                assert_eq!(&merged.evaluate(EmitTo::All).unwrap(), &direct.evaluate(EmitTo::All).unwrap());
            }
        }
    }

    /// Integers of 32 bits or fewer go in as they are, no CAST in the plan, and answer exactly; DISTINCT, a frame that
    /// slides, a BIGINT and a NULL answer as before.
    #[tokio::test]
    async fn sql_takes_small_integers_as_they_are() -> Result<()> {
        let ctx = crate::engine::session(SessionConfig::new().with_target_partitions(4), Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()));
        ctx.sql("CREATE TABLE w AS SELECT v, v % 3 AS g, (v % 10)::INT AS i, (v % 10)::SMALLINT AS s, arrow_cast(v % 10, 'UInt8') AS u \
                 FROM generate_series(1, 1000) AS t(v)").await?.collect().await?;
        let one = |sql: &'static str| {
            let ctx = ctx.clone();
            async move { pretty_format_batches(&ctx.sql(sql).await?.collect().await?).map(|t| t.to_string().lines().nth(3).unwrap_or("").to_string()) }
        };
        // (0 to 9, a hundred times each: an average of 4.5 and a variance of 99/12)
        let plan = pretty_format_batches(&ctx.sql("EXPLAIN SELECT avg(i), stddev(i) FROM w").await?.collect().await?)?.to_string();
        assert!(plan.contains("stddev(w.i)") && !plan.contains("CAST"), "{plan}");
        assert_eq!(one("SELECT avg(i) a, var_pop(i) b, avg(s) c, var_pop(s) d, avg(u) e, var_pop(u) f FROM w").await?, "| 4.5 | 8.25 | 4.5 | 8.25 | 4.5 | 8.25 |");
        assert_eq!(one("SELECT avg(DISTINCT i) a, var_pop(DISTINCT i) b FROM w").await?, "| 4.5 | 8.25 |");
        let slides = "SELECT max(a) m FROM (SELECT avg(i) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM w)";
        assert_eq!(one(slides).await?, "| 8.5 |");
        // (DataFusion's, taking values back out: its last digits, the same as over the DOUBLEs)
        let slides = "SELECT max(a) m FROM (SELECT var_pop(i) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM w)";
        let doubles = "SELECT max(a) m FROM (SELECT var_pop(i::DOUBLE) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM w)";
        let (got, want) = (one(slides).await?, one(doubles).await?);
        assert!(got == want && got.starts_with("| 20.25"), "{got} for {want}");
        assert_eq!(one("SELECT avg(v) a, var_pop(v % 10) b FROM w").await?, "| 500.5 | 8.25 |");
        // (NULLs stepped over, and a FILTER: as a WHERE gives them, grouped and not)
        let k = "CASE WHEN v % 7 = 0 THEN NULL ELSE (v % 10)::INT END";
        for by in ["", " GROUP BY g ORDER BY g"] {
            let filtered = format!("SELECT avg({k}) FILTER (WHERE v > 500) a, var_pop({k}) FILTER (WHERE v > 500) b, avg({k} + 0.5) FILTER (WHERE v > 500) c FROM w{by}");
            let wherever = format!("SELECT avg({k}) a, var_pop({k}) b, avg({k} + 0.5) c FROM w WHERE v > 500{by}");
            let (f, w) = (ctx.sql(&filtered).await?.collect().await?, ctx.sql(&wherever).await?.collect().await?);
            assert_eq!(pretty_format_batches(&f)?.to_string(), pretty_format_batches(&w)?.to_string());
        }
        let exact = one("SELECT avg(CASE WHEN v % 7 = 0 THEN NULL ELSE (v % 10)::INT END) = sum(CASE WHEN v % 7 = 0 THEN NULL ELSE v % 10 END)::DOUBLE / count(CASE WHEN v % 7 = 0 THEN NULL ELSE v END) e FROM w");
        assert_eq!(exact.await?, "| true |");
        let nulls = ctx.sql("SELECT avg(NULL), var(NULL) FROM w").await?.collect().await?;
        assert!(nulls[0].column(0).is_null(0) && nulls[0].column(1).is_null(0), "{}", pretty_format_batches(&nulls)?);
        Ok(())
    }

    /// Through SQL, grouped and not, over partitions that finish in any order: the same answers every run, and NULL
    /// where DataFusion's are.
    #[tokio::test]
    async fn sql_answers_every_run_alike() -> Result<()> {
        let ctx = crate::engine::session(SessionConfig::new().with_target_partitions(8), Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()));
        ctx.sql("CREATE TABLE t AS SELECT v % 7 AS g, 1000 + sin(v) * 3.7 AS x, (v * 7919 % 100003)::INT AS y FROM generate_series(1, 100000) AS s(v)").await?.collect().await?;
        let sql = "SELECT g, var(x), var_pop(x), variance(x), stddev(x), stddev_samp(x), stddev_pop(x), avg(x), var(y), stddev_pop(y), avg(y) FROM t GROUP BY g \
                   UNION ALL SELECT -1, var(x), var_pop(x), variance(x), stddev(x), stddev_samp(x), stddev_pop(x), avg(x), var(y), stddev_pop(y), avg(y) FROM t ORDER BY 1";
        let first = pretty_format_batches(&ctx.sql(sql).await?.collect().await?)?.to_string();
        for _ in 0..5 {
            assert_eq!(pretty_format_batches(&ctx.sql(sql).await?.collect().await?)?.to_string(), first);
        }
        // (DataFusion's: 0.30000000000000004 / 3, and a variance of a few ulps)
        let tenths = ctx.sql("SELECT avg(x) AS a, var_pop(x) AS v FROM (VALUES (0.1::DOUBLE), (0.1::DOUBLE), (0.1::DOUBLE)) AS s(x)").await?.collect().await?;
        assert_eq!(pretty_format_batches(&tenths)?.to_string().lines().nth(3), Some("| 0.1 | 0.0 |"));
        let none = ctx.sql("SELECT var(x), stddev(x), var_pop(x), avg(x) FROM t WHERE g > 100").await?.collect().await?;
        assert!((0..4).all(|c| none[0].column(c).is_null(0)), "{}", pretty_format_batches(&none)?);
        Ok(())
    }
}
