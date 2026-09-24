//! Slicing tables by a key's ranges, so that big tables meet on that key without a shuffle.
//!
//! Data that arrives in order — by time, by an id handed out in sequence — lands in files that
//! each hold a narrow range of it (`DataFile::stats`, kept narrow by `write::write_files` and
//! `tier`'s merges, which keep rows in the order they came). A distributed query can then slice
//! its biggest table by ranges of such a column, cut where its bytes split evenly over the nodes,
//! and every other big table it reads by the same ranges of a matching column: node i holds every
//! row of each of them whose key is in range i, and a join, an aggregation or an `EXISTS` on that
//! key runs where the rows already are (`spmd::spread`, `Spread::Ranged`). TPC-H's `lineitem`
//! and `orders`, both in order-key order, meet this way.
//!
//! Each node reads the files that overlap its range and keeps the rows inside it (NULLs go to
//! the first range), so the ranges split a table exactly whatever its files hold. They are only
//! chosen where that is cheap: few files read by two nodes, and the nodes about even.
use crate::manifest::{Manifest, Stats};
use crate::spmd::Part;
use crate::store::{DataFile, Lake, TableMeta};
use anyhow::{Context, Result};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{ident, lit};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

/// One node's range of a key: [lo, hi), unbounded where None. The first range also holds NULLs.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Range {
    pub column: String,
    pub lo: Option<String>,
    pub hi: Option<String>,
}

impl Range {
    /// The rows of a table in this range.
    pub fn expr(&self, t: &DataType) -> Result<Expr> {
        let value = |s: &String| -> Result<Expr> { Ok(lit(ScalarValue::try_from_string(s.clone(), t)?)) };
        let c = || ident(&self.column);
        Ok(match (&self.lo, &self.hi) {
            (None, None) => lit(true),
            (None, Some(hi)) => c().lt(value(hi)?).or(c().is_null()),
            (Some(lo), None) => c().gt_eq(value(lo)?),
            (Some(lo), Some(hi)) => c().gt_eq(value(lo)?).and(c().lt(value(hi)?)),
        })
    }
}

/// How a query's big tables are sliced: where the ranges are cut, and each table's column.
pub struct Scheme {
    cuts: Vec<ScalarValue>,
    pub columns: BTreeMap<String, String>,
}

/// A piece of a table: a file, or a sealed manifest of them.
#[derive(Clone)]
enum Piece {
    File(DataFile),
    Sealed(Manifest),
}

impl Piece {
    fn bytes(&self) -> u64 {
        match self { Piece::File(f) => f.bytes, Piece::Sealed(m) => m.bytes }.max(1)
    }
    fn stats(&self) -> &Stats {
        match self { Piece::File(f) => &f.stats, Piece::Sealed(m) => &m.stats }
    }
    /// Whether it may hold a NULL in `column` (not knowing counts as yes).
    fn nulls(&self, column: &str) -> bool {
        let n = match self { Piece::File(f) => &f.nulls, Piece::Sealed(m) => &m.nulls };
        n.as_ref().is_none_or(|n| n.iter().any(|c| c == column))
    }
}

/// A piece and the range of a column it holds; whether it may hold NULLs in it too (the first
/// range's rows, so the first node reads it as well).
struct Span {
    piece: Piece,
    lo: ScalarValue,
    hi: ScalarValue,
    nulls: bool,
}

/// A table's pieces: its files, and its manifests whole when there are enough of them to go
/// round (as `spmd::deal` deals them).
async fn pieces(lake: &Lake, meta: &TableMeta, n: usize) -> Result<Vec<Piece>> {
    let manifests = crate::manifest::list(lake, meta).await?;
    let mut out: Vec<Piece> = meta.files.iter().cloned().map(Piece::File).collect();
    if manifests.len() >= 4 * n {
        out.extend(manifests.into_iter().map(Piece::Sealed));
        return Ok(out);
    }
    for m in manifests {
        out.extend(crate::manifest::files(lake, &m).await?.into_iter().map(Piece::File));
    }
    Ok(out)
}

/// Every piece with its range of `column` (None if any piece has none: its rows could be anywhere).
fn spans(pieces: &[Piece], column: &str, t: &DataType) -> Option<Vec<Span>> {
    let parse = |s: &String| ScalarValue::try_from_string(s.clone(), t).ok();
    let mut out: Vec<Span> = pieces.iter().map(|p| {
        let (lo, hi) = p.stats().get(column)?;
        Some(Span { piece: p.clone(), lo: parse(lo)?, hi: parse(hi)?, nulls: p.nulls(column) })
    }).collect::<Option<_>>()?;
    out.sort_by(|a, b| a.lo.partial_cmp(&b.lo).unwrap_or(Ordering::Equal));
    Some(out)
}

/// Where to cut the biggest table's pieces into `n` ranges of about equal bytes: where a piece
/// starts, or — for whole numbers, when a piece is too big to leave whole — inside it, as if its
/// values were spread evenly.
fn cuts(spans: &[Span], n: usize) -> Option<Vec<ScalarValue>> {
    let total: u64 = spans.iter().map(|s| s.piece.bytes()).sum();
    let (mut out, mut before): (Vec<ScalarValue>, u64) = (vec![], 0);
    for s in spans {
        let bytes = s.piece.bytes();
        while out.len() + 1 < n {
            let want = total * (out.len() as u64 + 1) / n as u64; // (bytes before the next cut)
            let cut = match want {
                w if w <= before => s.lo.clone(),
                w if w < before + bytes && s.lo.data_type().is_integer() => {
                    let (lo, hi) = (int(&s.lo)?, int(&s.hi)?);
                    let at = lo + ((hi - lo) as f64 * (w - before) as f64 / bytes as f64) as i64;
                    ScalarValue::Int64(Some(at.max(lo + 1))).cast_to(&s.lo.data_type()).ok()?
                }
                _ => break,
            };
            if out.last().is_some_and(|l| l.partial_cmp(&cut) != Some(Ordering::Less)) {
                return None; // (two cuts at one value: too few distinct keys to go round)
            }
            out.push(cut);
        }
        before += bytes;
    }
    (out.len() + 1 == n).then_some(out)
}

fn int(v: &ScalarValue) -> Option<i64> {
    match v.cast_to(&DataType::Int64).ok()? {
        ScalarValue::Int64(Some(x)) => Some(x),
        _ => None,
    }
}

/// What slicing a table's pieces by `cuts` costs: the bytes every node reads, over the table's
/// (a piece two ranges share is read twice), and the most any node keeps, as a share of all
/// (splitting a piece's whole numbers evenly over its range).
fn cost(spans: &[Span], cuts: &[ScalarValue]) -> (f64, f64) {
    let n = cuts.len() + 1;
    let total: u64 = spans.iter().map(|s| s.piece.bytes()).sum();
    let (mut read, mut kept) = (0f64, vec![0f64; n]);
    for s in spans {
        let nodes: Vec<usize> = (0..n).filter(|&i| overlaps(s, cuts, i)).collect();
        read += (s.piece.bytes() * nodes.len() as u64) as f64;
        let (lo, hi) = (int(&s.lo), int(&s.hi));
        for &i in &nodes {
            let share = match (lo, hi) {
                (Some(lo), Some(hi)) if hi > lo => {
                    let a = if i == 0 { lo } else { int(&cuts[i - 1]).unwrap_or(lo).max(lo) };
                    let b = if i + 1 == n { hi + 1 } else { int(&cuts[i]).unwrap_or(hi).min(hi + 1) };
                    (b - a).max(0) as f64 / (hi + 1 - lo) as f64
                }
                _ => 1.0 / nodes.len() as f64,
            };
            kept[i] += s.piece.bytes() as f64 * share;
        }
    }
    (read / total as f64, kept.iter().cloned().fold(0.0, f64::max) / total as f64)
}

/// The most a table's slicing may read, over its size: each cut through one piece (its biggest),
/// which both nodes read — and nothing more, which is what files that each hold a narrow range of
/// the key give. (Files holding all of it would be read by every node.)
fn straddled(spans: &[Span], n: usize) -> f64 {
    let total: u64 = spans.iter().map(|s| s.piece.bytes()).sum();
    let biggest = spans.iter().map(|s| s.piece.bytes()).max().unwrap_or(0);
    1.0 + (n - 1) as f64 * biggest as f64 / total.max(1) as f64 + 0.01
}

/// Whether a piece can hold rows of range `i` (between cut i-1 and cut i; the first one also
/// holds the NULLs).
fn overlaps(s: &Span, cuts: &[ScalarValue], i: usize) -> bool {
    let after_lo = i == 0 || s.hi.partial_cmp(&cuts[i - 1]) != Some(Ordering::Less);
    let before_hi = i == cuts.len() || s.lo.partial_cmp(&cuts[i]) == Some(Ordering::Less);
    (after_lo && before_hi) || (i == 0 && s.nulls)
}

/// The columns of `meta` a query can be sliced by: named in its text, with an order.
fn named(meta: &TableMeta, words: &HashSet<String>) -> Result<Vec<(String, DataType)>> {
    let schema = crate::query::read_schema(&meta.columns)?;
    let key = |t: &DataType| t.is_integer() || matches!(t, DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) | DataType::Decimal128(..) | DataType::Utf8);
    Ok(schema.fields().iter().filter(|f| words.contains(&f.name().to_lowercase()) && key(f.data_type())).map(|f| (f.name().clone(), f.data_type().clone())).collect())
}

/// How to slice a query's big tables by ranges (the biggest one first in `tables`), or None
/// when its biggest table has no column whose files hold narrow ranges of it. Every other table
/// is sliced by the same ranges of a column of the same type, if that reads it at most about
/// twice over and leaves no node much more than its share; else by size, as before.
pub async fn scheme(lake: &Lake, sql: &str, tables: &[(String, TableMeta)], n: usize) -> Result<Option<Scheme>> {
    let words: HashSet<String> = sql.split(|c: char| !(c.is_alphanumeric() || c == '_')).map(str::to_lowercase).collect();
    let Some((main, meta)) = tables.first() else { return Ok(None) };
    let main_pieces = pieces(lake, meta, n).await?;
    let mut best: Option<(f64, String, DataType, Vec<ScalarValue>)> = None;
    for (column, t) in named(meta, &words)? {
        let Some(spans) = spans(&main_pieces, &column, &t) else { continue };
        // (cuts are sent as text: a string too long to write down whole can't be one)
        let Some(cuts) = cuts(&spans, n).filter(|c| c.iter().all(|c| crate::manifest::text(c).is_some())) else { continue };
        let (read, _) = cost(&spans, &cuts);
        if read <= straddled(&spans, n) && best.as_ref().is_none_or(|b| read < b.0) {
            best = Some((read, column, t, cuts));
        }
    }
    let Some((_, column, t, cuts)) = best else { return Ok(None) };
    let mut columns = BTreeMap::from([(main.clone(), column)]);
    for (name, meta) in &tables[1..] {
        let theirs = pieces(lake, meta, n).await?;
        let mut pick: Option<(f64, String)> = None;
        for (column, _) in named(meta, &words)?.into_iter().filter(|(_, ct)| *ct == t) {
            let Some(spans) = spans(&theirs, &column, &t) else { continue };
            let (read, most) = cost(&spans, &cuts);
            if read <= straddled(&spans, n).max(2.0) && most <= 1.5 / n as f64 && pick.as_ref().is_none_or(|p| read < p.0) {
                pick = Some((read, column));
            }
        }
        if let Some((_, column)) = pick {
            columns.insert(name.clone(), column);
        }
    }
    Ok(Some(Scheme { cuts, columns }))
}

/// A table's parts by the scheme's ranges, one per node: the pieces overlapping its range (a
/// piece at a cut goes to both sides; each keeps its own rows), and the log tail for every node,
/// which each reads through its range.
pub async fn parts(lake: &Lake, meta: &TableMeta, table: &str, scheme: &Scheme, n: usize, tail: (u64, u64)) -> Result<Vec<Part>> {
    let column = &scheme.columns[table];
    let schema = crate::query::read_schema(&meta.columns)?;
    let t = schema.field_with_name(column)?.data_type().clone();
    let spans = spans(&pieces(lake, meta, n).await?, column, &t).context("a piece without a range")?;
    let cuts: Vec<String> = scheme.cuts.iter().map(|c| crate::manifest::text(c).context("a cut that can't be written down")).collect::<Result<_>>()?;
    Ok((0..n).map(|i| {
        let mut part = Part { table: table.into(), tail: Some(tail), ..Default::default() };
        for s in spans.iter().filter(|s| overlaps(s, &scheme.cuts, i)) {
            match &s.piece {
                Piece::File(f) => part.files.push(f.clone()),
                Piece::Sealed(m) => part.manifests.push(m.clone()),
            }
        }
        part.range = Some(Range { column: column.clone(), lo: (i > 0).then(|| cuts[i - 1].clone()), hi: (i + 1 < n).then(|| cuts[i].clone()) });
        part
    }).collect())
}
