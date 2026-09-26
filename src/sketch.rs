//! How many different values a column holds, without keeping them: a HyperLogLog sketch per
//! column (256 one-byte registers, about 6% off), made as each file is written and merged into
//! its table's as the leader commits the file. The join order (`optimize::JoinOrder`) divides by
//! it: a join on a key with few values multiplies rows, one on a key with many doesn't. A column's
//! range bounded that before, which says nothing about a string, and little about a key spread
//! over a wide range.
use crate::store::{DataFile, TableMeta};
use base64::Engine;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use std::collections::BTreeMap;

const P: u32 = 8; // 2^8 registers
const M: usize = 1 << P;

/// A sketch of each of the first 32 columns that can be a join key: whole numbers, strings,
/// dates, decimals.
pub fn of(batches: &[RecordBatch]) -> BTreeMap<String, String> {
    let Some(first) = batches.first() else { return BTreeMap::new() };
    let schema = first.schema();
    let key = |t: &DataType| t.is_integer() || matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Date32 | DataType::Date64 | DataType::Decimal128(..));
    let mut out = BTreeMap::new();
    for (i, f) in schema.fields().iter().enumerate().take(32).filter(|(_, f)| key(f.data_type()) && !crate::sys::NAMES.contains(&f.name().as_str())) {
        let mut regs = [0u8; M];
        for b in batches {
            let c = b.column(i);
            let mut hashes = vec![0u64; c.len()];
            if datafusion::common::hash_utils::create_hashes_with_hasher([c], &datafusion::common::hash_utils::HLL_RANDOM_STATE, &mut hashes).is_err() {
                continue;
            }
            for h in hashes.into_iter().enumerate().filter(|(row, _)| c.is_valid(*row)).map(|(_, h)| h) {
                let (at, rest) = ((h >> (64 - P)) as usize, h << P);
                regs[at] = regs[at].max((rest.leading_zeros() + 1).min(64 - P + 1) as u8);
            }
        }
        out.insert(f.name().clone(), base64::engine::general_purpose::STANDARD.encode(regs));
    }
    out
}

fn registers(text: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(text).ok().filter(|r| r.len() == M)
}

/// Fold new files' sketches into their table's (the files keep none: a table's entry lists up
/// to 128 files, and each would carry a sketch of every column).
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
    let regs = registers(text)?;
    let m = M as f64;
    let sum: f64 = regs.iter().map(|&r| (-(r as f64)).exp2()).sum();
    let e = 0.7213 / (1.0 + 1.079 / m) * m * m / sum;
    let zeros = regs.iter().filter(|&&r| r == 0).count();
    Some(if e <= 2.5 * m && zeros > 0 { m * (m / zeros as f64).ln() } else { e }.round() as u64)
}
