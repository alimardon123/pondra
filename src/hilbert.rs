//! Hilbert order for `cluster_by` over two or more columns (ADR-021). Rows sorted by (a, b) are
//! grouped by a alone: every stretch of them spans all of b, so a filter on b skips nothing.
//! Ordered along a Hilbert curve through the columns' ranks, rows next to each other are near in
//! every column at once, so each row group holds a narrow range of each, and a filter on any one
//! skips most of them (what Databricks' liquid clustering does).
use anyhow::{anyhow, Result};
use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute::kernels::rank::rank;
use datafusion::arrow::compute::{cast, concat_batches, take_record_batch};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

/// `batches` along a Hilbert curve through columns `cols` (each by its rank among these rows).
pub fn sort(batches: &[RecordBatch], cols: &[String]) -> Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else { return Ok(vec![]) };
    let all = concat_batches(&first.schema(), batches)?;
    let (dims, bits) = (cols.len().min(8), (63 / cols.len().min(8)).min(31) as u32);
    let coords = cols[..dims].iter().map(|c| {
        let a = all.column_by_name(c).ok_or_else(|| anyhow!("no column {c}"))?;
        let ranks = match rank(a, None) {
            Ok(r) => r,
            Err(_) => rank(&cast(a, &DataType::Utf8)?, None)?, // (types rank can't order directly, as text)
        };
        let top = ranks.iter().max().copied().unwrap_or(0).max(1) as u64;
        Ok(ranks.into_iter().map(|r| (r as u64 * ((1 << bits) - 1) / top) as u32).collect::<Vec<u32>>())
    }).collect::<Result<Vec<_>>>()?;
    let mut keys: Vec<(u64, u32)> = (0..all.num_rows()).map(|i| {
        let mut x = [0u32; 8];
        coords.iter().enumerate().for_each(|(d, c)| x[d] = c[i]);
        (index(&mut x[..dims], bits), i as u32)
    }).collect();
    keys.sort_unstable();
    Ok(vec![take_record_batch(&all, &UInt32Array::from_iter_values(keys.into_iter().map(|(_, i)| i)))?])
}

/// The distance along a Hilbert curve of point `x` (`bits` a coordinate): Skilling's "axes to
/// transpose" (2004), then its bits interleaved, most significant first.
fn index(x: &mut [u32], bits: u32) -> u64 {
    let n = x.len();
    let mut q = 1u32 << (bits - 1);
    while q > 1 {
        let p = q - 1;
        for i in 0..n {
            if x[i] & q != 0 {
                x[0] ^= p;
            } else {
                let t = (x[0] ^ x[i]) & p;
                x[0] ^= t;
                x[i] ^= t;
            }
        }
        q >>= 1;
    }
    for i in 1..n {
        x[i] ^= x[i - 1];
    }
    let (mut t, mut q) = (0, 1u32 << (bits - 1));
    while q > 1 {
        if x[n - 1] & q != 0 {
            t ^= q - 1;
        }
        q >>= 1;
    }
    x.iter_mut().for_each(|v| *v ^= t);
    (0..bits).rev().fold(0u64, |h, b| x.iter().fold(h, |h, v| (h << 1) | ((v >> b) & 1) as u64))
}

#[cfg(test)]
mod tests {
    /// The 2-D curve of order 1 visits (0,0), (0,1), (1,1), (1,0): each step moves one cell.
    #[test]
    fn walks_neighbours() {
        let order: Vec<u64> = [[0, 0], [0, 1], [1, 1], [1, 0]].iter().map(|p| super::index(&mut p.clone(), 1)).collect();
        assert_eq!(order, vec![0, 1, 2, 3]);
    }
}
