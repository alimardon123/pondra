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
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::{Column, ScalarValue};
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::logical_expr::{Accumulator, Expr};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

pub const INLINE: usize = 128; // at most this many files in the catalog entry…
pub const SEAL: usize = 64; // …then all but the newest this many are sealed
const BIG: u64 = 4096; // files per manifest at most; smaller manifests are merged, 16 at a time

pub use crate::data_file::{text, Stats};

/// A manifest's entry in the table's list: where it is, what it holds, its columns' ranges.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Manifest {
    pub path: String,
    pub files: u64,
    pub rows: u64,
    pub bytes: u64,
    pub stats: Stats,
    /// The columns any of its files holds a NULL in (None: some file doesn't say).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nulls: Option<Vec<String>>,
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

/// What a file's rows show as they are written, a batch at a time (`tier::Encoding`): which of its
/// first 32 columns hold a NULL, and its floats' ranges. Every other column's range is read from the
/// file's footer (`stats`), whose statistics the writer kept as it went: no second pass over the rows.
pub struct Seen {
    schema: SchemaRef,
    nulls: Vec<bool>,
    floats: Vec<(usize, MinAccumulator, MaxAccumulator)>,
}

impl Seen {
    pub fn new(schema: SchemaRef) -> Self {
        let floats = schema.fields().iter().enumerate().filter(|(i, f)| ranged(*i, f) && f.data_type().is_floating())
            .filter_map(|(i, f)| Some((i, MinAccumulator::try_new(f.data_type()).ok()?, MaxAccumulator::try_new(f.data_type()).ok()?))).collect();
        Seen { nulls: vec![false; schema.fields().len().min(32)], floats, schema }
    }

    pub fn add(&mut self, b: &RecordBatch) -> Result<()> {
        for (i, held) in self.nulls.iter_mut().enumerate() {
            *held |= b.column(i).null_count() > 0;
        }
        for (i, lo, hi) in &mut self.floats {
            lo.update_batch(&[b.column(*i).clone()])?;
            hi.update_batch(&[b.column(*i).clone()])?;
        }
        Ok(())
    }

    /// The columns (of the first 32, as `stats`) that hold a NULL.
    pub fn nulls(&self) -> Vec<String> { self.nulls.iter().enumerate().filter(|(_, held)| **held).map(|(i, _)| self.schema.field(i).name().clone()).collect() }

    /// The min and max of each of the first 32 columns that has an order (as Delta does, so a wide
    /// table's entries stay small; strings only up to 64 characters, where a cut-off value would no
    /// longer bound them), and of the system columns (`sys.rs`: a purge finds changed rows by them).
    /// Floats' come from their rows (Parquet leaves NaN out of its own, and a NaN must not be pruned
    /// away), the rest from the footer, when exact: Parquet cuts a value at 64 bytes, which `text`
    /// would leave out anyway.
    pub fn stats(&mut self, written: &ParquetMetaData) -> Stats {
        let mut out = Stats::new();
        let floats: BTreeMap<usize, (ScalarValue, ScalarValue)> = self.floats.iter_mut().filter_map(|(i, lo, hi)| Some((*i, (lo.evaluate().ok()?, hi.evaluate().ok()?)))).collect();
        for (i, f) in self.schema.fields().iter().enumerate().filter(|(i, f)| ranged(*i, f)) {
            let range = match f.data_type().is_floating() {
                true => floats.get(&i).cloned(),
                false => from_footer(written, &self.schema, f.name()),
            };
            if let Some((Some(lo), Some(hi))) = range.map(|(lo, hi)| (text(&lo), text(&hi))) {
                out.insert(f.name().clone(), (lo, hi));
            }
        }
        out
    }
}

/// Whether a file keeps column `i`'s range: the first 32 columns that have an order, and the
/// system columns.
fn ranged(i: usize, f: &datafusion::arrow::datatypes::Field) -> bool { (i < 32 || crate::text::NAMES.contains(&f.name().as_str())) && orderable(f.data_type()) }

/// Column `name`'s min and max from a file's footer: over its row groups' own, when every row
/// group that holds a value has them, exact (not cut short).
pub fn from_footer(md: &ParquetMetaData, schema: &datafusion::arrow::datatypes::Schema, name: &str) -> Option<(ScalarValue, ScalarValue)> {
    use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
    let c = StatisticsConverter::try_new(name, schema, md.file_metadata().schema_descr()).ok()?;
    let groups = || md.row_groups().iter();
    let (lo, hi, nulls) = (c.row_group_mins(groups()).ok()?, c.row_group_maxes(groups()).ok()?, c.row_group_null_counts(groups()).ok()?);
    let (lo_exact, hi_exact) = (c.row_group_is_min_value_exact(groups()).ok()?, c.row_group_is_max_value_exact(groups()).ok()?);
    let complete = groups().enumerate().all(|(j, g)| {
        nulls.value(j) as i64 == g.num_rows() || (lo.is_valid(j) && hi.is_valid(j) && lo_exact.value(j) && hi_exact.value(j))
    });
    complete.then(|| crate::sketch::min_max(&[lo]).zip(crate::sketch::min_max(&[hi]))).flatten().map(|((lo, _), (_, hi))| (lo, hi))
}

/// A type whose files keep a range (floats too, from their rows: Parquet leaves NaN out of its own).
pub fn orderable(t: &DataType) -> bool {
    t.is_integer() || t.is_floating() || matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) | DataType::Decimal128(..) | DataType::Boolean)
}

/// The ranges covering a table's whole contents: its manifests' and its files'. What a query's
/// planning knows about the values in each column (`query::Pruned::statistics`). Worked out once
/// per version of the table — every query would otherwise re-parse every file's min and max.
pub fn ranges(table: &str, manifests: &[Manifest], files: &[DataFile], schema: &SchemaRef) -> Arc<Stats> {
    use std::hash::{Hash, Hasher};
    crate::per_node!(static SEEN: Mutex<lru::LruCache<String, (u64, Arc<Stats>)>> = Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(256).unwrap())); clear = |m| m.lock().unwrap_or_else(|e| e.into_inner()).clear());
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
    crate::scan::may_match(&View { stats, schema }, filters, schema)
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

crate::per_node!(
    /// Decompressed metadata objects, newest used kept, up to 256 MB (they never change).
    static CACHE: Mutex<(lru::LruCache<String, Arc<Vec<u8>>>, usize)> = Mutex::new((lru::LruCache::unbounded(), 0)); clear = |m| { let mut c = m.lock().unwrap_or_else(|e| e.into_inner()); c.0.clear(); c.1 = 0; });
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
        Some(s) => {
            let mut all: Vec<Manifest> = serde_json::from_slice(&object(lake, &s.list).await?)?;
            if let Some(id) = crate::branch::base_of(&s.list) {
                all.iter_mut().for_each(|m| m.path = crate::branch::rebase(&m.path, id)); // (a branch's base's list names its paths: ADR-047)
            }
            Ok(all)
        }
        None => Ok(vec![]),
    }
}

/// The files in one manifest.
pub async fn files(lake: &Lake, m: &Manifest) -> Result<Vec<DataFile>> {
    let mut all: Vec<DataFile> = serde_json::from_slice(&object(lake, &m.path).await?)?;
    if let Some(id) = crate::branch::base_of(&m.path) {
        all.iter_mut().for_each(|f| crate::branch::rebase_file(f, id));
    }
    Ok(all)
}

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
    let nulls = files.iter().map(|f| f.nulls.clone()).collect::<Option<Vec<_>>>().map(|n| n.concat().into_iter().collect::<std::collections::BTreeSet<_>>().into_iter().collect());
    Manifest { path, files: files.len() as u64, rows: files.iter().map(|f| f.rows).sum(), bytes: files.iter().map(|f| f.bytes).sum(), stats, nulls }
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
    let (dir, schema) = (format!("data/{}/_manifests", meta.folder(table)), crate::answer::schema(&crate::sys::with_sys(meta).columns)?);
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

/// `files` sealed under `dir` of another lake's `store` (a backup's: ADR-070), as `seal` keeps them:
/// manifests of up to `BIG` files and their list, each named by its contents, so a run that seals
/// the same files again writes nothing new. What it names goes in `written`.
pub async fn sealed_into(store: &crate::store::Store, dir: &str, files: &[DataFile], schema: &SchemaRef, written: &mut Vec<String>) -> Result<Sealed> {
    async fn put(store: &crate::store::Store, dir: &str, value: &impl Serialize, written: &mut Vec<String>) -> Result<String> {
        use object_store::ObjectStore;
        let body = serde_json::to_vec(value)?;
        let name = crate::text::hex(&aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &body).as_ref()[..16]);
        let path = format!("{dir}/{name}.json.zst");
        let opts = object_store::PutOptions { mode: object_store::PutMode::Create, ..Default::default() };
        match store.put_opts(&object_store::path::Path::from(path.as_str()), zstd::encode_all(&body[..], 3)?.into(), opts).await {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(e) => return Err(anyhow::Error::new(e).context(format!("writing {path}"))),
        }
        written.push(path.clone());
        Ok(path)
    }
    let mut list = vec![];
    for chunk in files.chunks(BIG as usize) {
        list.push(summary(put(store, dir, &chunk, written).await?, chunk, schema));
    }
    let (n, rows, bytes) = list.iter().fold((0, 0, 0), |(f, r, b), m| (f + m.files, r + m.rows, b + m.bytes));
    Ok(Sealed { list: put(store, dir, &list, written).await?, files: n, rows, bytes })
}

/// Take manifests `which` (of `list`, the table's) apart: their files back inline, the list
/// without them (a purge is about to replace some of their files; `seal` puts the rest away again).
pub async fn unseal(lake: &Lake, table: &str, meta: &mut TableMeta, mut list: Vec<Manifest>, which: &[usize]) -> Result<()> {
    if which.is_empty() {
        return Ok(());
    }
    let now = crate::log::now_ms();
    for i in which.iter().rev() {
        let m = list.remove(*i);
        meta.files.extend(files(lake, &m).await?);
        meta.garbage.push((m.path, now));
    }
    let old = meta.sealed.take();
    if !list.is_empty() {
        let (files, rows, bytes) = list.iter().fold((0, 0, 0), |(f, r, b), m| (f + m.files, r + m.rows, b + m.bytes));
        let dir = format!("data/{}/_manifests", meta.folder(table));
        meta.sealed = Some(Sealed { list: put(lake, &dir, &list).await?, files, rows, bytes });
    }
    meta.garbage.extend(old.map(|o| (o.list, now)));
    Ok(())
}
