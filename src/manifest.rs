//! Table metadata that stays small at any size (append tables).
//!
//! Every Parquet file carries its columns' min and max (`DataFile::stats`). A table's recent
//! files are listed inline in its catalog entry; once there are more than `INLINE`, all but the
//! newest `SEAL` are sealed into immutable *manifest* objects (their `DataFile`s, up to `BIG` per
//! manifest, grouped by partition), and the table keeps
//! one *manifest list* object naming its manifests with each one's totals and min/max. Commits
//! stay the same size however big the table grows, and a query reads the list, skips every
//! manifest whose ranges can't match its filters, then skips files the same way: it opens only
//! the files that can hold its rows, without reading the others' footers. (Iceberg's manifests,
//! kept by Pondra's leader.) Small manifests are merged, so a list stays short: about one entry
//! per `BIG` files.
//!
//! Manifests and lists are written once and never changed; a replaced list goes to the table's
//! garbage like a replaced Parquet file. Every node caches them in memory (and the SSD tier).
use crate::store::{DataFile, Lake, TableMeta};
use anyhow::Result;
use futures::TryStreamExt;
use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::logical_expr::{Accumulator, Expr};
use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

pub const INLINE: usize = 128; // at most this many files in the catalog entry…
pub const SEAL: usize = 64; // …then all but the newest this many are sealed
const BIG: u64 = 4096; // files per manifest at most; smaller manifests are merged, 16 at a time

/// Column -> (min, max), as text parsed with the column's type.
pub type Stats = BTreeMap<String, (String, String)>;

/// A manifest's entry in the table's list: where it is, what it holds, its columns' ranges.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Manifest {
    pub path: String,
    pub files: u64,
    pub rows: u64,
    pub bytes: u64,
    pub stats: Stats,
}

/// A table's sealed files: its manifest list object, and totals.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct Sealed {
    pub list: String,
    pub files: u64,
    pub rows: u64,
    pub bytes: u64,
}

// ---------------------------------------------------------------- statistics

/// The min and max of each of the first 32 columns that has an order (as Delta does, so a wide
/// table's entries stay small; strings only up to 64 characters, where a cut-off value would no
/// longer bound them).
pub fn stats(batches: &[RecordBatch]) -> Stats {
    use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
    let Some(first) = batches.first() else { return Stats::new() };
    let mut out = Stats::new();
    for (i, f) in first.schema().fields().iter().enumerate().take(32) {
        if !orderable(f.data_type()) {
            continue;
        }
        let (Ok(mut lo), Ok(mut hi)) = (MinAccumulator::try_new(f.data_type()), MaxAccumulator::try_new(f.data_type())) else { continue };
        let cols: Vec<ArrayRef> = batches.iter().map(|b| b.column(i).clone()).collect();
        let (Ok(()), Ok(())) = (cols.iter().try_for_each(|c| lo.update_batch(&[c.clone()])), cols.iter().try_for_each(|c| hi.update_batch(&[c.clone()]))) else { continue };
        if let (Ok(lo), Ok(hi)) = (lo.evaluate(), hi.evaluate()) {
            if let (Some(lo), Some(hi)) = (text(&lo), text(&hi)) {
                out.insert(f.name().clone(), (lo, hi));
            }
        }
    }
    out
}

/// A value as text that casts back to it exactly (timestamps as ISO 8601), or None for a null
/// or a string too long to keep.
fn text(v: &ScalarValue) -> Option<String> {
    use datafusion::arrow::array::AsArray;
    if v.is_null() {
        return None;
    }
    let s = datafusion::arrow::compute::cast(&v.to_array().ok()?, &DataType::Utf8).ok()?;
    Some(s.as_string::<i32>().value(0).to_string()).filter(|s| s.len() <= 64)
}

fn orderable(t: &DataType) -> bool {
    t.is_integer() || t.is_floating() || matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) | DataType::Decimal128(..) | DataType::Boolean)
}

/// The ranges covering a table's whole contents: its manifests' and its files'. What a query's
/// planning knows about the values in each column (`query::Pruned::statistics`). Worked out once
/// per version of the table — every query would otherwise re-parse every file's min and max.
pub fn ranges(table: &str, manifests: &[Manifest], files: &[DataFile], schema: &SchemaRef) -> Arc<Stats> {
    use std::hash::{Hash, Hasher};
    static SEEN: LazyLock<Mutex<lru::LruCache<String, (u64, Arc<Stats>)>>> = LazyLock::new(|| Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(256).unwrap())));
    let mut h = std::collections::hash_map::DefaultHasher::new();
    files.iter().for_each(|f| f.path.hash(&mut h));
    manifests.iter().for_each(|m| m.path.hash(&mut h));
    let mark = h.finish();
    if let Some((had, stats)) = SEEN.lock().unwrap().get(table) {
        if *had == mark {
            return stats.clone();
        }
    }
    let parts: Vec<&Stats> = manifests.iter().map(|m| &m.stats).chain(files.iter().map(|f| &f.stats)).collect();
    let stats = Arc::new(union(&parts, schema));
    SEEN.lock().unwrap().put(table.to_string(), (mark, stats.clone()));
    stats
}

/// How many values a range could hold, for the types that count in whole numbers: an upper bound
/// on a column's distinct values, which is what the size of a join on it turns on. None where a
/// range says nothing about that — floats, strings, timestamps.
pub fn span(lo: &ScalarValue, hi: &ScalarValue) -> Option<u64> {
    if !(lo.data_type().is_integer() || matches!(lo.data_type(), DataType::Date32 | DataType::Date64)) {
        return None;
    }
    match hi.sub(lo).ok()?.cast_to(&DataType::Int64).ok()? {
        ScalarValue::Int64(Some(n)) if n >= 0 => Some(n as u64 + 1),
        _ => None,
    }
}

/// The ranges covering all of `parts` (a column missing from any of them has no range).
fn union(parts: &[&Stats], schema: &SchemaRef) -> Stats {
    let Some(first) = parts.first() else { return Stats::new() };
    let mut out = Stats::new();
    for (col, _) in first.iter() {
        let Ok(f) = schema.field_with_name(col) else { continue };
        let parse = |s: &String| ScalarValue::try_from_string(s.clone(), f.data_type()).ok();
        let ranges: Option<Vec<(ScalarValue, ScalarValue)>> = parts.iter().map(|s| s.get(col).and_then(|(a, b)| Some((parse(a)?, parse(b)?)))).collect();
        let Some(ranges) = ranges else { continue };
        let lo = ranges.iter().map(|r| &r.0).min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let hi = ranges.iter().map(|r| &r.1).max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if let (Some(lo), Some(hi)) = (lo.and_then(text), hi.and_then(text)) {
            out.insert(col.clone(), (lo, hi));
        }
    }
    out
}

/// Which of the containers (files or manifests, by their stats) can hold rows matching all of
/// `filters`. Anything the stats can't decide is kept.
pub fn keep(stats: &[&Stats], filters: &[Expr], schema: &SchemaRef) -> Vec<bool> {
    let all = vec![true; stats.len()];
    let Some(expr) = filters.iter().cloned().reduce(Expr::and) else { return all };
    let unqualified = expr.transform(|e| Ok(match e {
        Expr::Column(c) => Transformed::yes(Expr::Column(Column::new_unqualified(c.name))),
        e => Transformed::no(e),
    }));
    let pruned = || -> Result<Vec<bool>> {
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let phys = datafusion::physical_expr::create_physical_expr(&unqualified?.data, &df_schema, &Default::default(), &Default::default())?;
        Ok(PruningPredicateBuilder::new().with_file_schema(schema.clone()).try_build(phys)?.prune(&View { stats, schema })?)
    };
    pruned().unwrap_or(all)
}

struct View<'a> {
    stats: &'a [&'a Stats],
    schema: &'a SchemaRef,
}

impl View<'_> {
    fn values(&self, c: &Column, max: bool) -> Option<ArrayRef> {
        let t = self.schema.field_with_name(&c.name).ok()?.data_type().clone();
        let null = ScalarValue::try_from(&t).ok()?;
        let v = self.stats.iter().map(|s| s.get(&c.name).and_then(|(lo, hi)| ScalarValue::try_from_string(if max { hi } else { lo }.clone(), &t).ok()).unwrap_or(null.clone()));
        ScalarValue::iter_to_array(v).ok()
    }
}

impl PruningStatistics for View<'_> {
    fn min_values(&self, c: &Column) -> Option<ArrayRef> { self.values(c, false) }
    fn max_values(&self, c: &Column) -> Option<ArrayRef> { self.values(c, true) }
    fn num_containers(&self) -> usize { self.stats.len() }
    fn null_counts(&self, _: &Column) -> Option<ArrayRef> { None }
    fn row_counts(&self) -> Option<ArrayRef> { None }
    fn contained(&self, _: &Column, _: &HashSet<ScalarValue>) -> Option<BooleanArray> { None }
}

// ---------------------------------------------------------------- reading

/// Decompressed metadata objects, newest used kept, up to 256 MB (they never change).
static CACHE: LazyLock<Mutex<(lru::LruCache<String, Arc<Vec<u8>>>, usize)>> = LazyLock::new(|| Mutex::new((lru::LruCache::unbounded(), 0)));
const CACHE_BYTES: usize = 256 << 20;

async fn object(lake: &Lake, path: &str) -> Result<Arc<Vec<u8>>> {
    if let Some(b) = CACHE.lock().unwrap().0.get(path) {
        return Ok(b.clone());
    }
    let b = Arc::new(zstd::decode_all(&lake.object(path).await?[..])?);
    let mut c = CACHE.lock().unwrap();
    if c.0.put(path.to_string(), b.clone()).is_none() {
        c.1 += b.len();
    }
    while c.1 > CACHE_BYTES {
        let Some((_, old)) = c.0.pop_lru() else { break };
        c.1 -= old.len();
    }
    Ok(b)
}

/// The table's manifests (none if nothing is sealed).
pub async fn list(lake: &Lake, meta: &TableMeta) -> Result<Vec<Manifest>> {
    match &meta.sealed {
        Some(s) => Ok(serde_json::from_slice(&object(lake, &s.list).await?)?),
        None => Ok(vec![]),
    }
}

/// The files in one manifest.
pub async fn files(lake: &Lake, m: &Manifest) -> Result<Vec<DataFile>> { Ok(serde_json::from_slice(&object(lake, &m.path).await?)?) }

/// The files that can hold rows matching `filters`: manifests pruned first, then files.
/// `manifests`: a subset to look in (a distributed query's slice), else the table's list.
pub async fn pruned(lake: &Lake, meta: &TableMeta, manifests: Option<&[Manifest]>, filters: &[Expr], schema: &SchemaRef) -> Result<Vec<DataFile>> {
    let listed = match manifests {
        Some(m) => m.to_vec(),
        None => list(lake, meta).await?,
    };
    let kept = keep(&listed.iter().map(|m| &m.stats).collect::<Vec<_>>(), filters, schema);
    let wanted: Vec<&Manifest> = listed.iter().zip(kept).filter(|(_, k)| *k).map(|(m, _)| m).collect();
    let loads: Vec<_> = wanted.into_iter().map(|m| files(lake, m)).collect();
    let loaded: Vec<Vec<DataFile>> = futures::StreamExt::buffered(futures::stream::iter(loads), 32).try_collect().await?;
    let candidates: Vec<DataFile> = loaded.into_iter().flatten().chain(meta.files.iter().cloned()).collect();
    let kept = keep(&candidates.iter().map(|f| &f.stats).collect::<Vec<_>>(), filters, schema);
    let files: Vec<DataFile> = candidates.into_iter().zip(kept).filter(|(_, k)| *k).map(|(f, _)| f).collect();
    let total = listed.iter().map(|m| m.files).sum::<u64>() + meta.files.len() as u64;
    crate::metrics::add(&crate::metrics::FILES_SCANNED, files.len() as u64);
    crate::metrics::add(&crate::metrics::FILES_SKIPPED, total - files.len() as u64);
    Ok(files)
}

// ---------------------------------------------------------------- sealing (the leader)

async fn put(lake: &Lake, dir: &str, value: &impl Serialize) -> Result<String> {
    let path = format!("{dir}/{}.json.zst", uuid::Uuid::new_v4());
    lake.put(&path, zstd::encode_all(&serde_json::to_vec(value)?[..], 3)?).await?;
    Ok(path)
}

/// `files` as manifests of up to `BIG` files each.
async fn write(lake: &Lake, dir: &str, files: &[DataFile], schema: &SchemaRef) -> Result<Vec<Manifest>> {
    let puts = files.chunks(BIG as usize).map(|chunk| async move { Ok::<_, anyhow::Error>(summary(put(lake, dir, &chunk).await?, chunk, schema)) });
    futures::future::try_join_all(puts).await
}

fn summary(path: String, files: &[DataFile], schema: &SchemaRef) -> Manifest {
    let stats = union(&files.iter().map(|f| &f.stats).collect::<Vec<_>>(), schema);
    Manifest { path, files: files.len() as u64, rows: files.iter().map(|f| f.rows).sum(), bytes: files.iter().map(|f| f.bytes).sum(), stats }
}

/// Leader, in `tier::maintain`: seal the oldest inline files once there are too many, and merge
/// small manifests. Writes the new objects and changes `meta` (the caller commits it); a replaced
/// list goes to the garbage. Returns whether anything changed.
/// The files the next `seal` takes: all but the newest `SEAL`, once there are more than `INLINE`.
pub fn to_seal(meta: &TableMeta) -> Vec<&DataFile> {
    if !meta.key.is_empty() || meta.files.len() <= INLINE {
        return vec![];
    }
    let mut by_age: Vec<&DataFile> = meta.files.iter().collect();
    by_age.sort_by_key(|f| f.ord);
    by_age.truncate(meta.files.len() - SEAL);
    by_age
}

pub async fn seal(lake: &Lake, table: &str, meta: &mut TableMeta) -> Result<bool> {
    if !meta.key.is_empty() || meta.files.len() <= INLINE {
        return Ok(false);
    }
    let (dir, schema) = (format!("data/{table}/_manifests"), crate::query::schema(&meta.columns)?);
    meta.files.sort_by_key(|f| f.ord);
    let mut sealing: Vec<DataFile> = meta.files.drain(..meta.files.len() - SEAL).collect();
    sealing.sort_by(|a, b| (&a.part, a.ord).cmp(&(&b.part, b.ord))); // (a manifest covers few partitions)
    let mut list = list(lake, meta).await?;
    list.extend(write(lake, &dir, &sealing, &schema).await?);
    // Merge the newest run of small manifests once there are 16 (lists stay about files / BIG long).
    let small = list.iter().rev().take_while(|m| m.files < BIG).count();
    if small >= 16 {
        let run: Vec<Manifest> = list.split_off(list.len() - small);
        let mut merged = vec![];
        for m in &run {
            merged.extend(files(lake, m).await?);
        }
        list.extend(write(lake, &dir, &merged, &schema).await?);
        let now = crate::log::now_ms();
        meta.garbage.extend(run.into_iter().map(|m| (m.path, now)));
    }
    let old = meta.sealed.take();
    let (files, rows, bytes) = list.iter().fold((0, 0, 0), |(f, r, b), m| (f + m.files, r + m.rows, b + m.bytes));
    meta.sealed = Some(Sealed { list: put(lake, &dir, &list).await?, files, rows, bytes });
    if let Some(old) = old {
        meta.garbage.push((old.list, crate::log::now_ms()));
    }
    Ok(true)
}
