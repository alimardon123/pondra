//! What the planner knows of a column without keeping its values: how many different values it holds, and its range.
//! The count is a HyperLogLog sketch per column (256 one-byte registers, about 6% off), made as each file is written and
//! merged into its table's as the leader commits the file; a page's tables work it out from their rows (`statistics`).
//! The join order (`optimize::JoinOrder`) divides by it: a join on a key with few values multiplies rows, one on a key
//! with many doesn't. A column's range bounded that before, which says nothing about a string, and little about a key
//! spread over a wide range.
#[cfg(feature = "lake")]
use crate::store::{DataFile, TableMeta};
use base64::Engine;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::{ScalarValue, Statistics};
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::logical_expr::Accumulator;
use std::collections::{BTreeMap, HashMap};

const P: u32 = 8; // 2^8 registers
const M: usize = 1 << P;

/// A sketch of each of the first 32 columns that can be a join key (whole numbers, strings, dates,
/// decimals), made a batch at a time as a file is written (`tier::Encoding`).
pub struct Sketches(Vec<(usize, String, [u8; M])>);

impl Sketches {
    pub fn new(schema: &Schema) -> Self {
        let key = |t: &DataType| t.is_integer() || matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Date32 | DataType::Date64 | DataType::Decimal128(..));
        let columns = schema.fields().iter().enumerate().take(32).filter(|(_, f)| key(f.data_type()) && !crate::text::NAMES.contains(&f.name().as_str()));
        Sketches(columns.map(|(i, f)| (i, f.name().clone(), [0u8; M])).collect())
    }

    pub fn add(&mut self, b: &RecordBatch) {
        let mut hashes = vec![]; // (one buffer for the batch's columns)
        for (i, _, regs) in &mut self.0 {
            let c = b.column(*i);
            hashes.clear();
            hashes.resize(c.len(), 0u64);
            if datafusion::common::hash_utils::create_hashes_with_hasher([c], &datafusion::common::hash_utils::HLL_RANDOM_STATE, &mut hashes).is_err() {
                continue;
            }
            let mut put = |h: u64| {
                let (at, rest) = ((h >> (64 - P)) as usize, h << P);
                regs[at] = regs[at].max((rest.leading_zeros() + 1).min(64 - P + 1) as u8);
            };
            // (a null counts for nothing; a column with none is read without asking each row, as `is_valid` does through the
            // array: a fifth of a file's sketching)
            match c.nulls().filter(|n| n.null_count() > 0) {
                None => hashes.iter().for_each(|&h| put(h)),
                Some(nulls) => hashes.iter().zip(nulls.iter()).filter(|(_, valid)| *valid).for_each(|(&h, _)| put(h)),
            }
        }
    }

    /// Each sketched column's index and how many different values its registers count.
    pub fn counts(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        self.0.iter().map(|(i, _, regs)| (*i, count(regs)))
    }

    pub fn done(self) -> BTreeMap<String, String> { self.0.into_iter().map(|(_, name, regs)| (name, base64::engine::general_purpose::STANDARD.encode(regs))).collect() }
}

fn registers(text: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(text).ok().filter(|r| r.len() == M)
}

/// Fold new files' sketches into their table's (the files keep none: a table's entry lists up
/// to 128 files, and each would carry a sketch of every column).
#[cfg(feature = "lake")]
pub fn add(meta: &mut TableMeta, files: &mut [DataFile]) {
    for f in files {
        for (column, sketch) in std::mem::take(&mut f.sketch) {
            let Some(new) = registers(&sketch) else { continue };
            let merged = match meta.sketch.get(&column).and_then(|s| registers(s)) {
                Some(old) => old.iter().zip(&new).map(|(a, b)| *a.max(b)).collect(),
                None => new,
            };
            meta.sketch.insert(column, base64::engine::general_purpose::STANDARD.encode(merged));
        }
    }
}

/// About how many different values the sketch has seen.
pub fn estimate(text: &str) -> Option<u64> {
    registers(text).map(|r| count(&r))
}

/// HyperLogLog's estimate from a sketch's registers (with the small-range correction for few values).
fn count(regs: &[u8]) -> u64 {
    let m = M as f64;
    let sum: f64 = regs.iter().map(|&r| (-(r as f64)).exp2()).sum();
    let e = 0.7213 / (1.0 + 1.079 / m) * m * m / sum;
    let zeros = regs.iter().filter(|&&r| r == 0).count();
    let estimate = if e <= 2.5 * m && zeros > 0 { m * (m / zeros as f64).ln() } else { e };
    estimate.round() as u64
}

/// How many values a range could hold, for the types that count in whole numbers: an upper bound
/// on a column's distinct values, which is what the size of a join on it turns on. None where a
/// range says nothing about that — floats, strings, timestamps.
pub fn span(lo: &ScalarValue, hi: &ScalarValue) -> Option<u64> {
    // (dates as the days they are: a difference of dates comes out in seconds)
    let whole = |v: &ScalarValue| match v.cast_to(&DataType::Int64).ok()? {
        ScalarValue::Int64(Some(n)) if matches!(v.data_type(), DataType::Date64) => Some(n / 86_400_000),
        ScalarValue::Int64(Some(n)) if v.data_type().is_integer() || v.data_type() == DataType::Date32 => Some(n),
        _ => None,
    };
    let (lo, hi) = (whole(lo)?, whole(hi)?);
    (hi >= lo).then(|| hi.abs_diff(lo) + 1)
}

/// How many different values a column holds, as the planner may take it: the smallest of its sketch's count and what its
/// range could hold (`span`), no more than the rows and at least one. None when neither is known.
pub fn distinct(sketched: Option<u64>, range: Option<(&ScalarValue, &ScalarValue)>, rows: u64) -> Option<usize> {
    let spanned = range.and_then(|(lo, hi)| span(lo, hi));
    [sketched, spanned].into_iter().flatten().min().map(|n| (n as usize).min(rows as usize).max(1))
}

/// The least and greatest value in `cols` (None if they can't be ordered).
pub fn min_max(cols: &[ArrayRef]) -> Option<(ScalarValue, ScalarValue)> {
    let (mut lo, mut hi) = (MinAccumulator::try_new(cols.first()?.data_type()).ok()?, MaxAccumulator::try_new(cols[0].data_type()).ok()?);
    cols.iter().try_for_each(|c| lo.update_batch(&[c.clone()]).and_then(|_| hi.update_batch(&[c.clone()]))).ok()?;
    Some((lo.evaluate().ok()?, hi.evaluate().ok()?))
}

/// What the planner knows of rows held in memory, told as a lake's table tells it (`query::count`): their rows and
/// bytes, and each column's NULLs, least and greatest value and distinct values. Everything is inexact, never exact: nothing
/// may answer a query from it. A column whose type `min_max` refuses, or whose values are all NULL, keeps its range unknown.
pub fn statistics(schema: &SchemaRef, batches: &[RecordBatch]) -> Statistics {
    let mut stats = Statistics::new_unknown(schema);
    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    if rows == 0 {
        stats.num_rows = Precision::Inexact(0);
        return stats;
    }
    stats.num_rows = Precision::Inexact(rows as usize);
    stats.total_byte_size = Precision::Inexact(batches.iter().map(|b| b.get_array_memory_size()).sum());
    let mut sketches = Sketches::new(schema);
    batches.iter().for_each(|b| sketches.add(b));
    let counts: HashMap<usize, u64> = sketches.counts().collect();
    for (i, column) in stats.column_statistics.iter_mut().enumerate() {
        let arrays: Vec<ArrayRef> = batches.iter().map(|b| b.column(i).clone()).collect();
        column.null_count = Precision::Inexact(arrays.iter().map(|a| a.null_count()).sum());
        let range = min_max(&arrays).filter(|(lo, _)| !lo.is_null());
        if let Some((lo, hi)) = &range {
            column.min_value = Precision::Inexact(lo.clone());
            column.max_value = Precision::Inexact(hi.clone());
        }
        let bounds = range.as_ref().map(|(lo, hi)| (lo, hi));
        if let Some(n) = distinct(counts.get(&i).copied(), bounds, rows) {
            column.distinct_count = Precision::Inexact(n);
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::Field;
    use std::sync::Arc;

    fn sketched(values: Vec<Option<i64>>) -> String {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let mut s = Sketches::new(&schema);
        for chunk in values.chunks(1000) {
            s.add(&RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(chunk.to_vec()))]).unwrap());
        }
        s.done().remove("k").unwrap()
    }

    /// A null counts for nothing: a column's registers with nulls among its values are those of its values alone, and a
    /// column of a few thousand different values counts about that many.
    #[test]
    fn a_sketch_counts_a_columns_values_and_never_its_nulls() {
        let values: Vec<Option<i64>> = (0..5000).map(Some).collect();
        let with_nulls: Vec<Option<i64>> = (0..5000).flat_map(|v| [Some(v), None]).collect();
        assert_eq!(sketched(with_nulls), sketched(values.clone()));
        let n = count(&registers(&sketched(values)).unwrap());
        assert!((4500..5500).contains(&n), "{n}");
        assert_eq!(count(&registers(&sketched(vec![None; 3000])).unwrap()), 0);
    }

    /// The registers of a sliced column with nulls (its bitmap at an offset) are those of asking each row `is_valid`, the
    /// way it was read before.
    #[test]
    fn a_sliced_columns_sketch_is_its_rows_asked_one_by_one() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let whole = Int64Array::from((0..10_000).map(|v| (v % 7 != 0).then_some(v * 31)).collect::<Vec<_>>());
        for (at, len) in [(3, 5000), (1001, 777), (0, 10_000), (9_999, 1)] {
            let c: ArrayRef = Arc::new(whole.slice(at, len));
            let mut s = Sketches::new(&schema);
            s.add(&RecordBatch::try_new(schema.clone(), vec![c.clone()]).unwrap());
            let mut hashes = vec![0u64; c.len()];
            datafusion::common::hash_utils::create_hashes_with_hasher([&c], &datafusion::common::hash_utils::HLL_RANDOM_STATE, &mut hashes).unwrap();
            let mut regs = [0u8; M];
            for h in hashes.into_iter().enumerate().filter(|(row, _)| c.is_valid(*row)).map(|(_, h)| h) {
                let (at, rest) = ((h >> (64 - P)) as usize, h << P);
                regs[at] = regs[at].max((rest.leading_zeros() + 1).min(64 - P + 1) as u8);
            }
            assert_eq!(s.0[0].2, regs, "slice at {at}, {len} rows");
        }
    }
}
