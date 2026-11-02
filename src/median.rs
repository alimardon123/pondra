//! DataFusion's median with each group's values held in chunks of `CHUNK` values (`held::HeldOnce` plans it).
//!
//! DataFusion's own holds a group's values in one array that doubles as it fills: each doubling copies the array, and the
//! old one, freed, stays with the allocator for a while, so the node's memory reached twice what the pool counted (a median
//! of 60 M values in 3 groups passed a node's 1 GB line before its 512 MB pool refused it). In chunks nothing held is
//! copied or freed until the answer, and what the pool counts is what is held; the answer picks each group's middle from
//! the few values between two sampled bounds (`picked`), not from a copy of all its values in one array. The answer is
//! DataFusion's, for every type DataFusion's groups take (integers, floats, decimals). Any other use of the median is DataFusion's.

use datafusion::arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray, BooleanArray, ListArray, PrimitiveArray, PrimitiveBuilder};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{self as types, ArrowNativeType, ArrowNumericType, DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::functions_aggregate::median::Median;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, Signature};
use std::mem::size_of;
use std::sync::Arc;

/// Values in a chunk: 512 KB of 64-bit values. A group's first chunk grows by doubling up to it, so a small group holds
/// little; the chunks after it are made whole.
const CHUNK: usize = 1 << 16;

/// The median (`median(x)`, not DISTINCT): DataFusion's, its groups' values held in chunks.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct ChunkedMedian(Median);

impl AggregateUDFImpl for ChunkedMedian {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.0.return_type(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.0.state_fields(args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.0.accumulator(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.0.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        match chunked(args.expr_fields[0].data_type()) {
            Some(chunked) => Ok(chunked),
            None => self.0.create_groups_accumulator(args),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.0.documentation()
    }
}

/// The chunked accumulator for values of `data_type`: each type DataFusion's median takes in groups.
fn chunked(data_type: &DataType) -> Option<Box<dyn GroupsAccumulator>> {
    macro_rules! chunked {
        ($t:ty) => {
            Some(Box::new(Chunked::<$t> { data_type: data_type.clone(), groups: vec![], held: 0 }))
        };
    }
    match data_type {
        DataType::Int8 => chunked!(types::Int8Type),
        DataType::Int16 => chunked!(types::Int16Type),
        DataType::Int32 => chunked!(types::Int32Type),
        DataType::Int64 => chunked!(types::Int64Type),
        DataType::UInt8 => chunked!(types::UInt8Type),
        DataType::UInt16 => chunked!(types::UInt16Type),
        DataType::UInt32 => chunked!(types::UInt32Type),
        DataType::UInt64 => chunked!(types::UInt64Type),
        DataType::Float16 => chunked!(types::Float16Type),
        DataType::Float32 => chunked!(types::Float32Type),
        DataType::Float64 => chunked!(types::Float64Type),
        DataType::Decimal32(..) => chunked!(types::Decimal32Type),
        DataType::Decimal64(..) => chunked!(types::Decimal64Type),
        DataType::Decimal128(..) => chunked!(types::Decimal128Type),
        DataType::Decimal256(..) => chunked!(types::Decimal256Type),
        _ => None,
    }
}

/// A group's values: full chunks of `CHUNK` values, and the one filling.
#[derive(Debug)]
struct Group<N> {
    full: Vec<Vec<N>>,
    filling: Vec<N>,
}

impl<N> Default for Group<N> {
    fn default() -> Self {
        Self { full: vec![], filling: vec![] }
    }
}

impl<N: ArrowNativeType> Group<N> {
    fn bytes(&self) -> usize {
        (self.full.len() * CHUNK + self.filling.capacity()) * size_of::<N>() + self.full.capacity() * size_of::<Vec<N>>()
    }

    fn len(&self) -> usize {
        self.full.len() * CHUNK + self.filling.len()
    }

    /// Its values in one array, each chunk freed once copied.
    fn values(self) -> Vec<N> {
        if self.full.is_empty() {
            return self.filling;
        }
        let mut all = Vec::with_capacity(self.len());
        for chunk in self.full {
            all.extend_from_slice(&chunk);
        }
        all.extend_from_slice(&self.filling);
        all
    }
}

#[derive(Debug)]
struct Chunked<T: ArrowNumericType> {
    data_type: DataType,
    groups: Vec<Group<T::Native>>,
    /// Bytes the groups' chunks hold (`Group::bytes`), kept as they grow.
    held: usize,
}

impl<T: ArrowNumericType> Chunked<T> {
    fn push(&mut self, group: usize, value: T::Native) {
        let g = &mut self.groups[group];
        if g.filling.len() == g.filling.capacity() {
            let before = g.bytes();
            if g.filling.len() >= CHUNK {
                let full = std::mem::replace(&mut g.filling, Vec::with_capacity(CHUNK));
                g.full.push(full);
            } else {
                g.filling.reserve_exact((g.filling.capacity() * 2).clamp(4, CHUNK) - g.filling.len());
            }
            self.held = self.held + g.bytes() - before;
        }
        g.filling.push(value);
    }

    fn emit(&mut self, emit_to: EmitTo) -> Vec<Group<T::Native>> {
        let groups = emit_to.take_needed(&mut self.groups);
        self.held -= groups.iter().map(Group::bytes).sum::<usize>();
        groups
    }
}

impl<T: ArrowNumericType> GroupsAccumulator for Chunked<T> {
    fn update_batch(&mut self, values: &[ArrayRef], group_indices: &[usize], opt_filter: Option<&BooleanArray>, total_num_groups: usize) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.groups.resize_with(total_num_groups, Group::default);
        let pairs = group_indices.iter().zip(values.values().iter());
        if values.null_count() == 0 && opt_filter.is_none() {
            pairs.for_each(|(&g, &v)| self.push(g, v));
        } else {
            for (i, (&g, &v)) in pairs.enumerate() {
                if values.is_valid(i) && opt_filter.is_none_or(|f| f.is_valid(i) && f.value(i)) {
                    self.push(g, v);
                }
            }
        }
        Ok(())
    }

    /// States as `state` makes them, or as DataFusion's median does: a list of each group's values, NULL for none.
    fn merge_batch(&mut self, values: &[ArrayRef], group_indices: &[usize], total_num_groups: usize) -> Result<()> {
        let lists = values[0].as_list::<i32>();
        let (offsets, inner) = (lists.value_offsets(), lists.values().as_primitive::<T>());
        self.groups.resize_with(total_num_groups, Group::default);
        for (i, &g) in group_indices.iter().enumerate() {
            if lists.is_valid(i) {
                inner.values()[offsets[i] as usize..offsets[i + 1] as usize].iter().for_each(|&v| self.push(g, v));
            }
        }
        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let groups = self.emit(emit_to);
        let mut offsets = Vec::with_capacity(groups.len() + 1);
        offsets.push(0i32);
        let mut all = Vec::with_capacity(groups.iter().map(Group::len).sum());
        for g in groups {
            for chunk in g.full {
                all.extend_from_slice(&chunk);
            }
            all.extend_from_slice(&g.filling);
            offsets.push(i32::try_from(all.len()).map_err(|_| DataFusionError::ResourcesExhausted("a median's state holds more than 2^31 values".into()))?);
        }
        let inner = PrimitiveArray::<T>::new(ScalarBuffer::from(all), None).with_data_type(self.data_type.clone());
        let field = Arc::new(Field::new_list_field(self.data_type.clone(), true));
        Ok(vec![Arc::new(ListArray::new(field, OffsetBuffer::new(ScalarBuffer::from(offsets)), Arc::new(inner), None))])
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let groups = self.emit(emit_to);
        let mut answers = PrimitiveBuilder::<T>::with_capacity(groups.len()).with_data_type(self.data_type.clone());
        for g in groups {
            answers.append_option(picked::<T>(g));
        }
        Ok(Arc::new(answers.finish()))
    }

    /// Each row as its own group's state: a list of its one value, NULL where it is NULL or filtered out.
    fn convert_to_state(&self, values: &[ArrayRef], opt_filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<T>();
        let kept: Vec<bool> = (0..values.len()).map(|i| values.is_valid(i) && opt_filter.is_none_or(|f| f.is_valid(i) && f.value(i))).collect();
        let inner = PrimitiveArray::<T>::new(values.values().clone(), None).with_data_type(self.data_type.clone());
        let field = Arc::new(Field::new_list_field(self.data_type.clone(), true));
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1, values.len()));
        Ok(vec![Arc::new(ListArray::new(field, offsets, Arc::new(inner), Some(NullBuffer::from(kept))))])
    }

    fn size(&self) -> usize {
        self.held + self.groups.capacity() * size_of::<Group<T::Native>>()
    }
}

/// The middle value, or the mean of the two middle ones, as DataFusion's median picks it (`calculate_median`).
fn middle<T: ArrowNumericType>(values: &mut [T::Native]) -> Option<T::Native> {
    let len = values.len();
    if len == 0 {
        return None;
    }
    let (low, mid, _) = values.select_nth_unstable_by(len / 2, |x, y| x.compare(*y));
    let high = *mid;
    if len % 2 == 1 {
        return Some(high);
    }
    Some(mean::<T>(*low.iter().max_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Less))?, high))
}

/// The mean of two middle values as DataFusion takes it: past overflow each is halved before they are added.
fn mean<T: ArrowNumericType>(low: T::Native, high: T::Native) -> T::Native {
    let two = T::Native::usize_as(2);
    match low.add_checked(high) {
        Ok(sum) => sum.div_wrapping(two),
        Err(_) => {
            let halves = low.div_wrapping(two).add_wrapping(high.div_wrapping(two));
            halves.add_wrapping(low.mod_wrapping(two).add_wrapping(high.mod_wrapping(two)).div_wrapping(two))
        }
    }
}

/// Values sampled from a group in chunks to bound its middle (`picked`): the middle value's rank among them varies by 64
/// (half their square root), and the bounds are 256 ranks each side, so they miss the middle about 3 times in 100,000.
const SAMPLED: usize = 1 << 14;
const MARGIN: usize = 256;

/// A group's median without copying its values into one array: the middle values lie between two of `SAMPLED` values
/// drawn from it, so one pass counts the values below the lower and keeps those between (about a thirty-second of them),
/// and the middle is picked from those. Where the middle isn't between them (rarely), or a float is NaN (DataFusion's
/// order puts it first or last, its mean of two middle values another way), its values are copied into one array and
/// picked as DataFusion does (`middle`). The answer is the same either way.
fn picked<T: ArrowNumericType>(group: Group<T::Native>) -> Option<T::Native> {
    let len = group.len();
    if group.full.is_empty() {
        return middle::<T>(&mut group.values());
    }
    let cmp = |x: &T::Native, y: &T::Native| x.compare(*y);
    let at = |i: usize| if i < group.full.len() * CHUNK { group.full[i / CHUNK][i % CHUNK] } else { group.filling[i - group.full.len() * CHUNK] };
    let mut seed = len as u64 | 1;
    let mut sample: Vec<T::Native> = (0..SAMPLED)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            at((seed % len as u64) as usize)
        })
        .collect();
    sample.sort_unstable_by(cmp);
    let (k, two) = (len / 2, len % 2 == 0);
    let rank = (k as u64 * SAMPLED as u64 / len as u64) as usize;
    let lower = (rank >= MARGIN).then(|| sample[rank - MARGIN]);
    let upper = sample.get(rank + MARGIN).copied();
    drop(sample);
    let (mut below, mut between, mut nan) = (0, Vec::with_capacity(len / 16), false);
    for &v in group.full.iter().flatten().chain(group.filling.iter()) {
        nan |= v != v;
        if lower.is_some_and(|l| v.compare(l).is_lt()) {
            below += 1;
        } else if upper.is_none_or(|u| v.compare(u).is_le()) {
            between.push(v);
        }
    }
    let first = if two { k - 1 } else { k }; // (the lowest middle value's rank)
    if nan || first < below || k >= below + between.len() {
        drop(between);
        return middle::<T>(&mut group.values());
    }
    let (low, mid, _) = between.select_nth_unstable_by(k - below, cmp);
    let high = *mid;
    if !two {
        return Some(high);
    }
    Some(mean::<T>(*low.iter().max_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Less))?, high))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Float64Array;

    #[test]
    fn chunks_hold_what_they_count_and_answer_as_one_array() -> Result<()> {
        // 2 groups of 100,000 values (a chunk and a half each), a NULL and a filtered row among them.
        let n: i64 = 200_000;
        let values: ArrayRef = Arc::new(Float64Array::from_iter((0..n).map(|i| (i != 7).then_some(((i * 7919) % n) as f64))));
        let groups: Vec<usize> = (0..n).map(|i| (i % 2) as usize).collect();
        let filter = BooleanArray::from_iter((0..n).map(|i| Some(i != 8)));
        let mut acc = Chunked::<types::Float64Type> { data_type: DataType::Float64, groups: vec![], held: 0 };
        acc.update_batch(&[values.clone()], &groups, Some(&filter), 2)?;
        let held = acc.groups.iter().map(Group::bytes).sum::<usize>();
        assert_eq!(acc.held, held);
        assert!(held <= (n as usize + 2 * CHUNK) * 8 + 1024, "{held}");

        let want = |g: usize| {
            let mut v: Vec<f64> = (0..n).filter(|&i| i % 2 == g as i64 && i != 7 && i != 8).map(|i| ((i * 7919) % n) as f64).collect();
            v.sort_by(f64::total_cmp);
            if v.len() % 2 == 1 { v[v.len() / 2] } else { (v[v.len() / 2 - 1] + v[v.len() / 2]) / 2.0 }
        };
        // Its state merged into another, which answers group by group.
        let state = acc.state(EmitTo::All)?;
        assert_eq!((acc.held, acc.groups.len()), (0, 0));
        let mut merged = Chunked::<types::Float64Type> { data_type: DataType::Float64, groups: vec![], held: 0 };
        merged.merge_batch(&state, &[1, 0], 2)?;
        let first = merged.evaluate(EmitTo::First(1))?;
        let rest = merged.evaluate(EmitTo::All)?;
        assert_eq!((first.as_primitive::<types::Float64Type>().value(0), rest.as_primitive::<types::Float64Type>().value(0)), (want(1), want(0)));
        assert_eq!(merged.held, 0);
        Ok(())
    }

    #[test]
    fn a_middle_picked_between_sampled_bounds_is_the_one_array_middle() {
        let group = |values: &[f64]| {
            let mut g = Group::default();
            for chunk in values.chunks(CHUNK) {
                g.full.push(chunk.to_vec());
            }
            g.filling = g.full.pop().unwrap_or_default(); // (the last one filling, full or not)
            g
        };
        let n = 5 * CHUNK + 123;
        let cases: Vec<(&str, Vec<f64>)> = vec![
            ("rising", (0..n).map(|i| i as f64).collect()),
            ("falling, even", (0..n + 1).rev().map(|i| i as f64).collect()),
            ("shuffled", (0..n).map(|i| ((i * 7919) % n) as f64).collect()),
            ("five values", (0..n).map(|i| (i % 5) as f64).collect()),
            ("one value", vec![2.5; n]),
            ("a NaN", (0..n).map(|i| if i == 9 { f64::NAN } else { i as f64 }).collect()),
            ("skewed", (0..n).map(|i| if i % 100 == 0 { 1e300 } else { (i % 1000) as f64 }).collect()),
        ];
        for (what, values) in cases {
            let want = middle::<types::Float64Type>(&mut values.clone());
            let got = picked::<types::Float64Type>(group(&values));
            assert_eq!(got.map(f64::to_bits), want.map(f64::to_bits), "{what}");
        }
    }

    #[test]
    fn integers_are_held_in_chunks_and_answer_as_datafusions_median() -> Result<()> {
        // 11-02 audit M1: a BIGINT median, the commonest, went to DataFusion's doubling array.
        for t in [DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::UInt8, DataType::UInt16, DataType::UInt32, DataType::UInt64] {
            assert!(chunked(&t).is_some(), "{t}");
        }
        assert!(chunked(&DataType::Utf8).is_none());
        // One group of three chunks and a bit, an even count: the mean of the two middle values, truncated as DataFusion's.
        let n = 3 * CHUNK as i64 + 11;
        let values: ArrayRef = Arc::new(datafusion::arrow::array::Int64Array::from_iter_values((0..n).map(|i| (i * 7919) % n - n / 3)));
        let mut acc = chunked(&DataType::Int64).unwrap();
        acc.update_batch(&[values.clone()], &vec![0; n as usize], None, 1)?;
        let mut all: Vec<i64> = values.as_primitive::<types::Int64Type>().values().to_vec();
        let want = middle::<types::Int64Type>(&mut all);
        assert_eq!(acc.evaluate(EmitTo::All)?.as_primitive::<types::Int64Type>().value(0), want.unwrap());
        assert_eq!(middle::<types::Int64Type>(&mut [-3, 0]), Some(-1));
        assert_eq!(middle::<types::UInt64Type>(&mut [u64::MAX, u64::MAX - 2]), Some(u64::MAX - 1));
        Ok(())
    }

    #[test]
    fn two_middle_values_past_overflow_are_halved_first() {
        assert_eq!(middle::<types::Decimal64Type>(&mut [i64::MAX, i64::MAX - 2]), Some(i64::MAX - 1));
        assert_eq!(middle::<types::Decimal64Type>(&mut [3, 1, 2]), Some(2));
        assert_eq!(middle::<types::Decimal64Type>(&mut []), None);
    }
}
