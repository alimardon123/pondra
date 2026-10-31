//! Hot + cold reads: every table = its Parquet files ∪ the log segments after them.
//! Reading the table meta first and the tail second gives one consistent snapshot:
//! the files cover segments <= `tiered`, the tail covers everything after.
//! Upsert tables (with a key) keep only the latest row per key; merge tables combine the rows
//! of each key (sum, min, max).
use crate::store::*;
use anyhow::{bail, Result};
use datafusion::arrow::array::{ArrayRef, Int64Array, UInt64Array};
use datafusion::logical_expr::Expr;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::prelude::*;
use datafusion::common::utils::memory::RecordBatchMemoryCounter;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation, PeakRecordingPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

/// A table's columns as queries read them: strings as views (Utf8View), so scans don't copy them
/// and string filters and joins take their fast paths (TPC-H runs about 8% faster). Tables store
/// plain Utf8 (see `schema`).
pub fn read_schema(columns: &[(String, String)]) -> Result<SchemaRef> {
    let stored = schema(columns)?;
    let fields = stored.fields().iter().map(|f| match f.data_type() {
        DataType::Utf8 => Arc::new(f.as_ref().clone().with_data_type(DataType::Utf8View)),
        _ => f.clone(),
    });
    Ok(Arc::new(Schema::new(fields.collect::<Vec<_>>())))
}

pub use crate::rules::fit::{conform, strict};

/// A query's rows as `s`'s columns, by position, cast to its types: a view's or a task's output
/// into its table (strings a query reads from files are views; the table holds plain ones).
pub fn cast_as(b: &RecordBatch, s: &SchemaRef) -> Result<RecordBatch> {
    // (a keyed table made in SQL has a `_deleted` column last, which a query's rows leave out)
    let marker = s.fields().last().is_some_and(|f| f.name() == "_deleted") && b.num_columns() + 1 == s.fields().len();
    anyhow::ensure!(b.num_columns() == s.fields().len() || marker, "{} columns for a table of {}", b.num_columns(), s.fields().len());
    let mut columns = b.columns().iter().zip(s.fields()).map(|(c, f)| strict(c, f.data_type())).collect::<Result<Vec<_>>>()?;
    if marker {
        columns.push(datafusion::arrow::array::new_null_array(&DataType::Boolean, b.num_rows()));
    }
    let options = datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(b.num_rows()));
    Ok(RecordBatch::try_new_with_options(s.clone(), columns, &options)?)
}

/// Rows of `table` from committed segments in (after, upto], file commits' too (what the log's
/// readers see: tasks, `/watch`, watermarks); `None` = up to the latest; under the names SQL
/// knows their columns by (ADR-022: `tail_of` gives them as stored).
/// With `ord`, each row gets `_ord` (`log::ord`: its segment, then its position), so later versions sort last.
pub async fn tail(lake: &Lake, table: &str, after: u64, upto: Option<u64>, ord: bool) -> Result<Vec<RecordBatch>> {
    let rows = tail_of(lake, table, after, upto, ord, false, true).await?;
    match lake.cat.meta(&table_key(table)).await? {
        Some(m) if m.mapped() => rows.iter().map(|b| m.to_logical(b)).collect(),
        _ => Ok(rows),
    }
}

/// `tail`, with the system columns (`sys.rs`) when `sys`: each row's commit, and its `_row_id`.
/// `filed`: file commits' rows too (`Segment::files`), as the log's readers take them; queries
/// and tiering don't (those files are the table's already).
pub async fn tail_of(lake: &Lake, table: &str, after: u64, upto: Option<u64>, ord: bool, sys: bool, filed: bool) -> Result<Vec<RecordBatch>> {
    Ok(tail_segments(lake, table, after, upto, ord, sys, filed).await?.0.into_iter().flat_map(|(_, rows)| rows).collect())
}

/// `tail_of`'s rows a segment at a time, and the newest segment the scan saw, of any table: the
/// view held every segment up to it (commits number them in order).
async fn tail_segments(lake: &Lake, table: &str, after: u64, upto: Option<u64>, ord: bool, sys: bool, filed: bool) -> Result<(Vec<(u64, Vec<RecordBatch>)>, u64)> {
    let end = upto.map_or("s0".to_string(), |u| seg_key(u + 1)); // "s0" sorts right after every "s/…"
    let (mut segs, mut seen) = (vec![], after);
    for (key, seg) in lake.cat.scan::<Segment>(&seg_key(after + 1), &end).await? {
        let n = key[2..].parse::<u64>()?;
        seen = seen.max(n);
        if seg.parts.contains_key(table) || (filed && seg.files.contains_key(table)) {
            segs.push((n, seg));
        }
    }
    // Fetch many segments at once: on object storage each one may be a round trip. (A file
    // commit's rows come with their system columns: the log's rows get theirs from their segment.)
    let fetches: Vec<_> = segs.iter().map(|(n, seg)| async move {
        let mut rows: Vec<(RecordBatch, bool)> = lake.segment_rows(*n, seg, table).await?.iter().map(|b| (b.clone(), true)).collect();
        if filed {
            rows.extend(lake.filed_rows(seg, table, false, 0, u64::MAX).await?.into_iter().map(|b| (b, false)));
        }
        anyhow::Ok(rows)
    }).collect();
    let fetched: Vec<Vec<(RecordBatch, bool)>> = futures::stream::iter(fetches).buffered(32).try_collect().await?;
    let target = match lake.cat.meta(&table_key(table)).await? {
        Some(m) if sys => Some(schema(&crate::sys::with_sys(&m).columns)?),
        Some(m) => Some(schema(&m.columns)?),
        None => None,
    };
    let mut out = vec![];
    for ((n, seg), rows) in segs.iter().zip(fetched) {
        let (n, mut pos, mut mine) = (*n, 0u64, vec![]);
        for (b, logged) in rows.iter() {
            let b = &match &target {
                Some(s) if sys && *logged => conform(&crate::sys::derive(b, n, pos, seg.ts_ms)?, s)?,
                Some(s) => conform(b, s)?,
                None => b.clone(),
            };
            if !ord {
                pos += b.num_rows() as u64;
                mine.push(b.clone());
                continue;
            }
            let ords: ArrayRef = Arc::new(UInt64Array::from_iter_values((pos..pos + b.num_rows() as u64).map(|p| crate::log::ord(n, p))));
            pos += b.num_rows() as u64;
            let mut fields = b.schema().fields().to_vec();
            fields.push(Arc::new(Field::new("_ord", DataType::UInt64, false)));
            let mut cols = b.columns().to_vec();
            cols.push(ords);
            mine.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?);
        }
        out.push((n, mine));
    }
    Ok((out, seen))
}

/// A table's log tail as its queries read it (`sources`): conformed to its read schema, in
/// batches of up to 8,192 rows, kept from one query to the next and extended by the segments
/// committed since. Each query had conformed every segment's rows again (its strings copied into
/// views) and planned and run a batch a segment: hundreds of small ones while rows stream in,
/// which made dashboards three times as costly while writes landed.
#[derive(Clone)]
pub struct Tail {
    of: (u64, String),       // the table's `tiered` mark, and the schema and flags it was read with
    through: u64,            // every segment up to here is in it
    /// Its batches share one schema `Arc`, the first kept batch's, so a scan can compare them by pointer.
    rows: Vec<RecordBatch>,
    ends: Vec<(u64, usize)>, // each segment with rows of the table, and the rows up to its end
    bytes: usize,
}

const TAILS: usize = 128 << 20; // tails kept, per node
const TAIL_BATCH: usize = 8192;

impl Tail {
    /// Its rows up to segment `u`: a prefix (a slice of a query as of an older commit).
    fn upto(&self, u: u64) -> Vec<RecordBatch> {
        let mut left = self.ends.iter().take_while(|(n, _)| *n <= u).last().map_or(0, |e| e.1);
        let mut out = vec![];
        for b in &self.rows {
            if left == 0 {
                break;
            }
            let k = left.min(b.num_rows());
            out.push(if k == b.num_rows() { b.clone() } else { b.slice(0, k) });
            left -= k;
        }
        out
    }

    fn add(&mut self, segs: Vec<(u64, Vec<RecordBatch>)>, s: &SchemaRef) -> Result<()> {
        let schema = self.rows.first().map_or_else(|| s.clone(), |b| b.schema());
        let (mut new, mut total) = (vec![], self.ends.last().map_or(0, |e| e.1));
        for (n, batches) in segs {
            let before = total;
            for b in batches.iter().filter(|b| b.num_rows() > 0) {
                total += b.num_rows();
                let b = conform(b, &schema)?;
                new.push(if Arc::ptr_eq(b.schema_ref(), &schema) { b } else { b.with_schema(schema.clone())? });
            }
            if total > before {
                self.ends.push((n, total));
            }
        }
        self.bytes += new.iter().map(|b| b.get_array_memory_size()).sum::<usize>();
        self.rows.extend(new);
        // The small batches at the end become one once they would fill it: a query a segment
        // copies nothing, and the batches stay few.
        let small = self.rows.iter().rev().take_while(|b| b.num_rows() < TAIL_BATCH).count();
        let rows: usize = self.rows[self.rows.len() - small..].iter().map(|b| b.num_rows()).sum();
        if small > 1 && rows >= TAIL_BATCH {
            let tail = self.rows.split_off(self.rows.len() - small);
            let all = datafusion::arrow::compute::concat_batches(&schema, &tail)?;
            self.bytes += all.get_array_memory_size();
            self.bytes = self.bytes.saturating_sub(tail.iter().map(|b| b.get_array_memory_size()).sum());
            self.rows.extend((0..all.num_rows()).step_by(TAIL_BATCH).map(|at| all.slice(at, TAIL_BATCH.min(all.num_rows() - at))));
        }
        Ok(())
    }
}

/// A table's log rows after `after` up to segment `upto` (None: as far as this node's view goes),
/// as `sources` reads them, through the table's kept `Tail`.
async fn tail_rows(lake: &Lake, table: &str, after: u64, upto: Option<u64>, keyed: bool, sys: bool, s: &SchemaRef) -> Result<Vec<RecordBatch>> {
    if upto.is_some_and(|u| u <= after) {
        return Ok(vec![]); // (a slice of a spread query that reads no log)
    }
    let of = (after, format!("{keyed} {sys} {:?}", s.fields()));
    let key = (table.to_string(), of.1.clone());
    let kept = lake.tails.lock().unwrap().0.get(&key).filter(|t| t.of == of).cloned();
    if let (Some(t), Some(u)) = (&kept, upto) {
        if u <= t.through {
            return Ok(t.upto(u));
        }
    }
    let mut t = kept.as_deref().cloned().unwrap_or(Tail { of, through: after, rows: vec![], ends: vec![], bytes: 0 });
    let (segs, seen) = tail_segments(lake, table, t.through, upto, keyed, sys, false).await?;
    t.add(segs, s)?;
    t.through = t.through.max(seen);
    let rows = t.upto(upto.unwrap_or(u64::MAX));
    if t.bytes <= TAILS {
        keep(&mut lake.tails.lock().unwrap(), table, Arc::new(t));
    }
    Ok(rows)
}

/// Keeps `t` as its table's tail of its form, in the cache `c` (with its total bytes). A newer
/// mark wins over what is kept for the form. Nothing is kept while any form of the table has a
/// newer mark: a slice read at an older one, as its coordinator saw it, keeps none, whichever
/// form it reads. A kept tail drops the other forms' tails of the table at an older mark, and
/// past `TAILS` the least recently used go.
fn keep(c: &mut (lru::LruCache<(String, String), Arc<Tail>>, usize), table: &str, t: Arc<Tail>) {
    if c.0.iter().any(|(k, o)| k.0 == table && o.of.0 > t.of.0) {
        return;
    }
    let key = (table.to_string(), t.of.1.clone());
    if c.0.peek(&key).is_some_and(|o| t.of.0 < o.of.0 || (t.of.0 == o.of.0 && t.through <= o.through)) {
        return;
    }
    c.1 += t.bytes;
    if let Some(old) = c.0.put(key, t.clone()) {
        c.1 -= old.bytes;
    }
    let older: Vec<(String, String)> = c.0.iter().filter(|(k, o)| k.0 == table && k.1 != t.of.1 && o.of.0 < t.of.0).map(|(k, _)| k.clone()).collect();
    for k in older {
        if let Some(old) = c.0.pop(&k) {
            c.1 -= old.bytes;
        }
    }
    while c.1 > TAILS && c.0.len() > 1 {
        let Some((_, old)) = c.0.pop_lru() else { break };
        c.1 -= old.bytes;
    }
}

/// All rows of a table (files ∪ tail up to `upto`); upsert tables carry `_ord` (0 for files).
pub async fn raw(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, upto: Option<u64>) -> Result<DataFrame> {
    let (tail, files) = sources(lake, ctx, name, meta, upto).await?;
    Ok(match tail.into_iter().chain(files).reduce(|a, b| a.union(b).expect("same schema")) {
        Some(df) => df,
        None => empty(ctx, meta)?,
    })
}

/// An append table whose rows have changed (`change.rs`): its rows but the old versions that
/// `{t}$deleted` holds (every (`_row_id`, `_version`) in it) from the changes not yet purged out
/// of its files (`tier::purge`). Those are few next to the table's rows, so they are held as a list;
/// only the files whose ranges can hold one of them, and the log, go through the anti-join, and
/// with none waiting the table reads as if it never changed.
/// `at`: the commit the whole query reads as of, up to which `{t}$deleted` is read — a slice of a
/// spread query may read none of the table's log itself (`upto` 0), but its files hold rows
/// changed since.
async fn current(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, upto: Option<u64>, at: Option<u64>) -> Result<DataFrame> {
    use crate::sys::with_sys;
    use crate::text::{ROW_ID, VERSION};
    let upto = upto.or(Some(lake.visible()));
    let at = at.or(upto); // (as of one commit: a change writes both. `at.or` of the `upto` given, None,
    // read every old row while the table's own were read to `visible`: a reader whose view is behind
    // its catalog's entries lost a changed row, `harness.py attached`)
    let dead = dead(lake, ctx, name, meta, at).await?;
    if dead.is_empty() {
        return raw(lake, ctx, name, meta, upto).await;
    }
    let (touched, clean): (Vec<DataFile>, Vec<DataFile>) = meta.files.iter().cloned().partition(|f| crate::sys::holds(&dead, &f.stats));
    let meta = with_sys(meta);
    let clean = raw(lake, ctx, name, &TableMeta { files: clean, ..meta.clone() }, Some(meta.tiered)).await?; // (the log goes with the touched files)
    let touched = raw(lake, ctx, name, &TableMeta { files: touched, ..meta }, upto).await?;
    let (ids, versions): (Vec<i64>, Vec<i64>) = dead.into_iter().unzip();
    let s = Arc::new(Schema::new(vec![Field::new("__id", DataType::Int64, false), Field::new("__v", DataType::Int64, false)]));
    let gone = ctx.read_batch(RecordBatch::try_new(s, vec![Arc::new(Int64Array::from(ids)), Arc::new(Int64Array::from(versions))])?)?;
    Ok(clean.union(gone.join(touched, JoinType::RightAnti, &["__id", "__v"], &[ROW_ID, VERSION], None)?)?)
}

/// A changed table's old rows (`_row_id`, `_version`), sorted, from the changes after its last
/// purge up to commit `at` (`{t}$deleted`'s files may be newer than the query's snapshot).
async fn dead(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, at: Option<u64>) -> Result<Vec<(i64, i64)>> {
    use crate::sys::with_sys;
    use crate::text::{ROW_ID, VERSION};
    use datafusion::arrow::array::AsArray;
    let deleted = crate::sys::deleted(name);
    let Some(dmeta) = lake.cat.meta(&table_key(&deleted)).await? else { return Ok(vec![]) };
    let within = col(VERSION).gt(lit(meta.purged() as i64)).and(col(VERSION).lt_eq(lit(at.unwrap_or(u64::MAX >> 1) as i64)));
    let dmeta = with_sys(&dmeta);
    let files = crate::manifest::pruned(lake, &dmeta, None, &[within.clone()], &schema(&dmeta.columns)?).await?;
    let dmeta = TableMeta { files, sealed: None, ..dmeta };
    let mut out = vec![];
    for b in raw(lake, ctx, &deleted, &dmeta, at).await?.filter(within)?.select(vec![col(ROW_ID), col("_old_version")])?.collect().await? {
        let (ids, versions) = (b.column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>(), b.column(1).as_primitive::<datafusion::arrow::datatypes::Int64Type>());
        out.extend(ids.iter().zip(versions.iter()).filter_map(|(i, v)| Some((i?, v?))));
    }
    out.sort_unstable();
    Ok(out)
}

/// A table's rows as separate reads: the log tail up to `upto` (if any rows), and the files.
/// Upsert tables get one read per generation of files, newest first, and `_ord` on every row: a
/// newer file's rows above an older one's, and log rows (`log::ord`) above both.
/// Other tables read all their files as one (merge tables combine rows in any order).
pub async fn sources(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, upto: Option<u64>) -> Result<(Option<DataFrame>, Vec<DataFrame>)> {
    if let Some(spec) = &meta.ext {
        let files = meta.files.iter().collect::<Vec<_>>(); // (files outside the lake: no log, no hot columns)
        return Ok((None, if files.is_empty() { vec![] } else { vec![crate::ext::read(lake, ctx, &files, &read_schema(&meta.columns)?, spec, meta.outside.as_ref()).await?] }));
    }
    let (schema, keyed, upsert) = (read_schema(&meta.columns)?, !meta.key.is_empty(), !meta.key.is_empty() && meta.merge.is_empty());
    let mut files = vec![];
    if upsert {
        let mut by_ord: std::collections::BTreeMap<u64, Vec<&DataFile>> = Default::default();
        for f in &meta.files {
            by_ord.entry(f.ord).or_default().push(f);
        }
        for (ord, group) in by_ord.into_iter().rev() {
            files.push(read_files(lake, ctx, group, meta, &schema).await?.with_column("_ord", lit(crate::log::ord(ord, 0)))?);
        }
    } else if !meta.files.is_empty() {
        let df = read_files(lake, ctx, meta.files.iter().collect(), meta, &schema).await?;
        files.push(if keyed { df.with_column("_ord", lit(0u64))? } else { df });
    }
    let sys = meta.columns.iter().any(|(c, _)| c == crate::text::ROW_ID); // (`sys::with_sys`)
    let mut fields = schema.fields().to_vec(); // the table's own schema, so every batch agrees
    if keyed {
        fields.push(Arc::new(Field::new("_ord", DataType::UInt64, false)));
    }
    let hot = tail_rows(lake, name, meta.tiered, upto, keyed, sys, &Arc::new(Schema::new(fields))).await?;
    if hot.is_empty() {
        return Ok((None, files));
    }
    Ok((Some(ctx.read_batches(hot)?), files))
}

/// Parquet files as one read: through the hot columns (`hot.rs`) when they're on, which read a
/// file with a lineage or deleted rows as `files_once` does.
async fn read_files(lake: &Lake, ctx: &SessionContext, files: Vec<&DataFile>, meta: &TableMeta, schema: &SchemaRef) -> Result<DataFrame> {
    if !lake.hot.on() {
        return files_once(lake, ctx, &files, meta, schema).await;
    }
    Ok(ctx.read_table(Arc::new(hot_files(lake, files.into_iter().cloned().collect(), meta, schema, Default::default())))?)
}

fn hot_files(lake: &Lake, files: Vec<DataFile>, meta: &TableMeta, schema: &SchemaRef, known: Arc<std::collections::HashMap<String, crate::hot::Known>>) -> crate::hot::HotFiles {
    let meta = TableMeta { columns: meta.columns.clone(), names: meta.names.clone(), key: meta.key.clone(), ..Default::default() }; // (what reading a file takes of its table)
    crate::hot::HotFiles { lake: lake.arc(), files, schema: schema.clone(), meta, known }
}

/// An append table that never changed, read through the hot columns, as a plan: its log tail and
/// its files side by side, as `raw` reads them. Planned here, not as a query of its own: that
/// query's optimizer passes are the ones the whole query runs over it again anyway, and they were
/// a sixth of a dashboard query's time while rows streamed in.
async fn plain_scan(lake: &Lake, state: &dyn datafusion::catalog::Session, name: &str, meta: &TableMeta, files: Vec<DataFile>, upto: Option<u64>, projection: Option<&Vec<usize>>, known: Arc<std::collections::HashMap<String, crate::hot::Known>>) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    use datafusion::datasource::memory::MemorySourceConfig;
    let schema = read_schema(&meta.columns)?;
    let sys = meta.columns.iter().any(|(c, _)| c == crate::text::ROW_ID);
    let tail = tail_rows(lake, name, meta.tiered, upto, false, sys, &schema).await?;
    let mut parts: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> = vec![];
    if !tail.is_empty() {
        parts.push(MemorySourceConfig::try_new_exec(&[tail], schema.clone(), projection.cloned())?);
    }
    if !files.is_empty() {
        parts.push(hot_files(lake, files, meta, &schema, known).scan(state, projection, &[], None).await?);
    }
    if parts.is_empty() {
        parts.push(MemorySourceConfig::try_new_exec(&[vec![]], schema, projection.cloned())?);
    }
    Ok(datafusion::physical_plan::union::UnionExec::try_new(parts)?)
}

/// Files of a table as one read, as `read_files` reads them but not through the hot columns (and
/// so not loading them there: maintenance reads a file once). A file with deleted rows is read
/// without them, one with a lineage with its rows' system columns from it (`scan.rs`).
pub async fn files_once(lake: &Lake, ctx: &SessionContext, files: &[&DataFile], meta: &TableMeta, schema: &SchemaRef) -> Result<DataFrame> {
    let (adopted, own): (Vec<&DataFile>, Vec<&DataFile>) = files.iter().copied().partition(|f| f.lineage.is_some());
    // (a keyed table's positions are the older versions its newer files hold: what others read; its own reads take the newest anyway)
    let (deleted, own): (Vec<&DataFile>, Vec<&DataFile>) = own.into_iter().partition(|f| !f.deletes.is_empty() && meta.key.is_empty());
    let mut parts = vec![];
    if !own.is_empty() {
        parts.push(ctx.read_parquet(own.iter().map(|f| lake.full(&f.path)).collect::<Vec<_>>(), ParquetReadOptions::default().schema(schema)).await?);
    }
    if !deleted.is_empty() {
        parts.push(crate::lake_scan::with_deletes(lake, ctx, &deleted, schema).await?);
    }
    if !adopted.is_empty() {
        parts.push(crate::lake_scan::adopted(lake, ctx, &adopted, meta, schema).await?);
    }
    union_all(parts)
}

fn union_all(parts: Vec<DataFrame>) -> Result<DataFrame> {
    let mut all = parts.into_iter();
    let first = all.next().ok_or_else(|| anyhow::anyhow!("no files"))?;
    all.try_fold(first, |a, b| Ok(a.union(b)?))
}

fn empty(ctx: &SessionContext, meta: &TableMeta) -> Result<DataFrame> {
    let df = ctx.read_table(Arc::new(MemTable::try_new(read_schema(&meta.columns)?, vec![vec![]])?))?;
    Ok(if meta.key.is_empty() { df } else { df.with_column("_ord", lit(0u64))? })
}

/// An upsert table as its users see it: the newest row of each key, deleted ones left out.
/// Every file holds one row per key, so rather than group all rows by key, each source keeps the
/// rows whose key no newer source has — an anti-join against the newer keys, which are usually
/// few (the log tail and recent files) — and only the log tail is deduplicated itself. A table
/// that is one compacted file reads as that file.
async fn upsert_view(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, upto: Option<u64>) -> Result<Arc<dyn TableProvider>> {
    let (tail, files) = sources(lake, ctx, name, meta, upto).await?;
    let (mut names, aux) = (vec![], lake.session()); // (the parts live in their own context: SHOW TABLES lists only tables)
    if let Some(t) = tail {
        aux.register_table("__tail", t.into_view())?;
        let newest = latest_sql(meta, "__tail", false, true); // (delete markers still shadow)
        aux.register_table("__s0", aux.sql(&newest).await?.into_view())?;
        names.push("__s0".to_string());
    }
    for (i, f) in files.into_iter().enumerate() {
        aux.register_table(format!("__f{i}").as_str(), f.into_view())?;
        names.push(format!("__f{i}"));
    }
    if names.is_empty() {
        return Ok(Arc::new(MemTable::try_new(read_schema(&meta.columns)?, vec![vec![]])?));
    }
    let q = |c: &String| format!("\"{c}\"");
    let cols = |p: &str| meta.columns.iter().map(|(c, _)| format!("{p}{}", q(c))).collect::<Vec<_>>().join(", ");
    let keys = meta.key.iter().map(q).collect::<Vec<_>>().join(", ");
    let on = meta.key.iter().map(|k| format!("s.{} = n.{}", q(k), q(k))).collect::<Vec<_>>().join(" AND ");
    let parts: Vec<String> = names.iter().enumerate().map(|(i, src)| match i {
        0 => format!("SELECT {} FROM \"{src}\"", cols("")),
        _ => {
            let newer = names[..i].iter().map(|n| format!("SELECT {keys} FROM \"{n}\"")).collect::<Vec<_>>().join(" UNION ALL ");
            format!("SELECT {} FROM \"{src}\" s LEFT ANTI JOIN ({newer}) n ON {on}", cols("s."))
        }
    }).collect();
    let sql = format!("SELECT {} FROM ({}){}", cols(""), parts.join(" UNION ALL "), live(meta));
    Ok(aux.sql(&sql).await?.into_view())
}

/// A table's reads under the names SQL knows its columns by (ADR-022): renamed columns aliased
/// from their stored names, dropped ones left out. (Filters still reach the files by their stored
/// names, through the projection: min/max pruning works as before.) A keyed table's `_deleted`
/// is left out too, unless the query names it (`deleted`): reads leave deleted rows out anyway,
/// so `SELECT *` shows the table's own columns.
pub fn named(ctx: &SessionContext, provider: Arc<dyn TableProvider>, meta: &TableMeta, deleted: bool) -> Result<Arc<dyn TableProvider>> {
    let hide = !deleted && !meta.key.is_empty() && meta.columns.iter().any(|(c, _)| c == "_deleted");
    // (a view that finishes its answers reads under its query's names already: `finish.rs`, and a
    // view's table is never renamed or dropped a column of)
    if (!meta.mapped() && !hide) || meta.finish.is_some() {
        return Ok(Arc::new(Table(provider)));
    }
    let df = ctx.read_table(provider)?;
    let shown = |c: &str| !meta.dropped.iter().any(|d| d == c) && !(hide && c == "_deleted");
    Ok(Arc::new(Table(crate::answer::renamed(df, |c| meta.name_of(c).to_string(), shown)?.into_view())))
}

/// `name` as the request being served may read it: as it is for a token or a superuser; for a user
/// granted some tables, `Guarded` (and left out of a listing if it may read none of it).
fn guarded(name: &str, t: Arc<dyn TableProvider>, listing: bool) -> Option<Arc<dyn TableProvider>> {
    let Some(access) = crate::auth::limited() else { return Some(t) };
    let columns = access.columns("select", name);
    if listing && columns.is_none() {
        return None;
    }
    Some(Arc::new(Guarded { inner: t, name: name.to_string(), columns, why: None }))
}

/// An attached lake's table `name` as the request being served may read it: not at all when that
/// lake signs in on its own (`locked`, why); for a user granted some tables, by grants naming it.
fn across(name: &str, t: Arc<dyn TableProvider>, listing: bool, locked: &Option<String>) -> Option<Arc<dyn TableProvider>> {
    let columns = match (locked, crate::auth::limited()) {
        (Some(_), _) => None,
        (None, None) => return Some(t),
        (None, Some(access)) => access.named("select", name),
    };
    if listing && columns.is_none() {
        return None;
    }
    Some(Arc::new(Guarded { inner: t, name: name.to_string(), columns, why: locked.clone() }))
}

/// A table as a user granted some of it reads it (`users.rs`): planned as the table is (its
/// columns are no secret, as in Postgres), but a scan of a column it may not read is refused, the
/// columns its filters use included; and every scan, if it may read none (or `why` not).
#[derive(Debug)]
struct Guarded {
    inner: Arc<dyn TableProvider>,
    name: String,
    columns: Option<Option<std::collections::HashSet<String>>>,
    why: Option<String>,
}

#[async_trait::async_trait]
impl TableProvider for Guarded {
    fn schema(&self) -> SchemaRef { self.inner.schema() }
    fn table_type(&self) -> datafusion::datasource::TableType { self.inner.table_type() }
    fn get_logical_plan(&self) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> { None } // (a view's plan would read around the check)
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        // (inexact: each filter stays above the scan too, so its columns are in what is scanned)
        Ok(self.inner.supports_filters_pushdown(filters)?.into_iter().map(|p| match p {
            datafusion::logical_expr::TableProviderFilterPushDown::Exact => datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
            p => p,
        }).collect())
    }
    async fn scan(&self, state: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let denied = |what: String| datafusion::error::DataFusionError::Plan(self.why.clone().unwrap_or_else(|| format!("permission denied: SELECT{what} on {} (GRANT SELECT ON {} TO …)", self.name, self.name)));
        let Some(columns) = &self.columns else { return Err(denied(String::new())) };
        if let Some(allowed) = columns {
            let schema = self.inner.schema();
            let mut used: Vec<String> = match projection {
                Some(p) => p.iter().map(|&i| schema.field(i).name().clone()).collect(),
                None => schema.fields().iter().map(|f| f.name().clone()).collect(),
            };
            used.extend(filters.iter().flat_map(|f| f.column_refs()).map(|c| c.name.clone()));
            let mut not: Vec<String> = used.into_iter().filter(|c| !allowed.contains(c)).collect();
            not.sort();
            not.dedup();
            if !not.is_empty() {
                return Err(denied(format!(" ({})", not.join(", "))));
            }
        }
        self.inner.scan(state, projection, filters, limit).await
    }
}

/// A lake's table as clients list it (`information_schema.tables`, the shell's `.tables`, the
/// console): a `BASE TABLE`, though it is planned as the query over its files and log it is (every
/// call is `inner`'s, its plan included).
#[derive(Debug)]
pub struct Table(pub Arc<dyn TableProvider>);

#[async_trait::async_trait]
impl TableProvider for Table {
    fn schema(&self) -> SchemaRef { self.0.schema() }
    fn table_type(&self) -> datafusion::datasource::TableType { datafusion::datasource::TableType::Base }
    fn constraints(&self) -> Option<&datafusion::common::Constraints> { self.0.constraints() }
    fn get_logical_plan(&self) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> { self.0.get_logical_plan() }
    fn get_column_default(&self, column: &str) -> Option<&Expr> { self.0.get_column_default(column) }
    fn statistics(&self) -> Option<datafusion::common::Statistics> { self.0.statistics() }
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.0.supports_filters_pushdown(filters)
    }
    async fn scan(&self, state: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.0.scan(state, projection, filters, limit).await
    }
}

/// Does a query (with the stored views it reads) name a keyed table's `_deleted` (`named`)? As a
/// word: a column of the query's own named `dbt_is_deleted` isn't it (dbt's snapshots write one).
pub fn names_deleted(text: &str) -> bool { text.to_lowercase().split(|c: char| !(c.is_alphanumeric() || c == '_')).any(|w| w == "_deleted") }

/// A table as its users see it: append tables as files + log; keyed tables their current rows.
/// `sys`: with its system columns (`sys::with_sys`). `upto`: the log only that far (a distributed query reads every
/// node's copy at one snapshot).
pub async fn table_view(lake: &Lake, ctx: &SessionContext, name: &str, meta: &Arc<TableMeta>, sys: bool, upto: Option<u64>) -> Result<Arc<dyn TableProvider>> {
    let facts = match meta.key.is_empty() {
        true => Some(facts(lake, meta, sys).await?), // (kept for every query that plans this version of the table)
        false => None,
    };
    view(lake, ctx, name, meta, sys, facts, upto).await
}

/// `table_view` for a meta that is not the catalog's, or not one a query keeps (an ext table's, a copy made for the
/// call): its facts are worked out for this call and not kept.
pub async fn table_view_once(lake: &Lake, ctx: &SessionContext, name: &str, meta: Arc<TableMeta>, upto: Option<u64>) -> Result<Arc<dyn TableProvider>> {
    let facts = match meta.key.is_empty() {
        true => Some(Arc::new(facts_once(lake, &meta, false).await?)),
        false => None,
    };
    view(lake, ctx, name, &meta, false, facts, upto).await
}

/// A table's view, over its facts (an append table's; none for a keyed one).
async fn view(lake: &Lake, ctx: &SessionContext, name: &str, meta: &Arc<TableMeta>, sys: bool, facts: Option<Arc<Facts>>, upto: Option<u64>) -> Result<Arc<dyn TableProvider>> {
    let meta = match sys {
        true => Arc::new(crate::sys::with_sys(meta)), // (a copy only when the query names system columns)
        false => meta.clone(),
    };
    if !meta.key.is_empty() && meta.merge.is_empty() && meta.order.is_none() {
        return upsert_view(lake, ctx, name, &meta, upto).await; // (by event time: every source's rows grouped by key, below)
    }
    if let Some(facts) = facts {
        return Ok(Arc::new(Pruned { lake: lake.arc(), name: name.into(), meta, facts, manifests: None, upto, at: upto, share: None, range: None, own: Default::default() }));
    }
    let df = raw(lake, ctx, name, &meta, upto).await?;
    let aux = lake.session();
    aux.register_table("__raw", df.into_view())?;
    let current = aux.sql(&current_sql(&meta, "__raw", upto.unwrap_or(lake.visible()))).await?;
    match &meta.finish {
        None => Ok(current.into_view()),
        Some(f) => crate::finish::finished(&aux, current, f).await, // (avg, HAVING, …: worked out as it is read)
    }
}

/// What a version of an append table gives every query that plans it, worked out once (`facts`): its read schema, its
/// manifests, the ranges of its whole contents, its statistics (`Pruned::sized`) and what `hot::known` makes of them.
pub struct Facts {
    pub schema: SchemaRef,
    pub manifests: Vec<crate::manifest::Manifest>,
    pub ranges: crate::manifest::Stats,
    pub count: datafusion::common::Statistics,
    pub known: Arc<std::collections::HashMap<String, crate::hot::Known>>,
}

/// Each append table version's `Facts`, by the address of its parsed entry and whether its system columns are in. A key is
/// a hit only while its `Weak` upgrades to the same `Arc`: the `Weak` holds the allocation, so no other entry takes its
/// address while it is held, and a changed entry is never this one (`Arc::make_mut`, `Arc::try_unwrap` and
/// `Arc::unwrap_or_clone` either give it a new allocation or cut the `Weak` off). At most 256 entries, and at most
/// `FACTS_BYTES` of manifests (`facts_bytes`: a big lake's manifest lists are what this keeps): on insert the entries
/// whose entry is gone leave, and the least recently used go until the new one fits, and a version bigger than the
/// whole budget is not kept. Each tiering round makes a new version, and a dead one's manifests must not wait for 256 newer.
struct FactsMemo {
    by_key: lru::LruCache<(usize, bool), Kept>, // (unbounded: `put` keeps it to `FACTS_ENTRIES` and `FACTS_BYTES`)
    bytes: usize,                               // the manifests' bytes of every entry in `by_key`
}

impl Default for FactsMemo {
    fn default() -> Self { FactsMemo { by_key: lru::LruCache::unbounded(), bytes: 0 } }
}

/// One version's entry in `FactsMemo`: its `Weak`, its facts, and what they count for.
struct Kept {
    held: std::sync::Weak<TableMeta>,
    facts: Arc<Facts>,
    bytes: usize,
}

/// Versions kept at once, and the manifests' bytes they may hold together, per node (`cache_bytes{cache="facts"}`).
const FACTS_ENTRIES: usize = 256;
const FACTS_BYTES: usize = 64 << 20;

/// What a version's facts count against `FACTS_BYTES`: each manifest by its path and its statistics (as JSON), and 128 bytes
/// for the record itself. A list of ten thousand manifests counts its ten thousand records, not only the first few.
fn facts_bytes(facts: &Facts) -> usize {
    facts.manifests.iter().map(|m| 128 + m.path.len() + crate::store::json(&m.stats).len()).sum()
}

impl FactsMemo {
    fn get(&mut self, meta: &Arc<TableMeta>, sys: bool) -> Option<Arc<Facts>> {
        let kept = self.by_key.get(&(Arc::as_ptr(meta) as usize, sys))?;
        let same = kept.held.upgrade().is_some_and(|m| Arc::ptr_eq(&m, meta));
        same.then(|| kept.facts.clone())
    }

    fn put(&mut self, meta: &Arc<TableMeta>, sys: bool, facts: &Arc<Facts>) {
        let key = (Arc::as_ptr(meta) as usize, sys);
        let gone: Vec<(usize, bool)> = self.by_key.iter().filter(|(_, k)| k.held.strong_count() == 0).map(|(k, _)| *k).collect();
        for k in gone {
            self.remove(&k);
        }
        self.remove(&key); // (the version's own older facts: replaced)
        let bytes = facts_bytes(facts);
        if bytes > FACTS_BYTES {
            return; // (a version bigger than the whole budget is planned without the memo)
        }
        while self.by_key.len() >= FACTS_ENTRIES || self.bytes + bytes > FACTS_BYTES {
            let Some((_, old)) = self.by_key.pop_lru() else { break };
            self.bytes -= old.bytes;
        }
        self.by_key.put(key, Kept { held: Arc::downgrade(meta), facts: facts.clone(), bytes });
        self.bytes += bytes;
    }

    fn remove(&mut self, key: &(usize, bool)) {
        if let Some(old) = self.by_key.pop(key) {
            self.bytes -= old.bytes;
        }
    }
}

crate::per_node!(static FACTS: std::sync::Mutex<FactsMemo> = std::sync::Mutex::new(FactsMemo::default()); clear = |m| *m.lock().unwrap_or_else(|e| e.into_inner()) = FactsMemo::default());

/// The manifests' bytes the planning facts kept now (`cache_bytes{cache="facts"}`).
pub fn facts_held() -> usize { FACTS.lock().unwrap_or_else(|e| e.into_inner()).bytes }

/// An append table version's `Facts` as its queries plan it, kept per version of its entry (`FactsMemo`): a table that has
/// not changed is planned from the same facts by every query.
pub async fn facts(lake: &Lake, meta: &Arc<TableMeta>, sys: bool) -> Result<Arc<Facts>> {
    if let Some(facts) = FACTS.lock().unwrap().get(meta, sys) {
        return Ok(facts);
    }
    let facts = Arc::new(facts_once(lake, meta, sys).await?);
    FACTS.lock().unwrap().put(meta, sys, &facts);
    Ok(facts)
}

/// `facts` worked out for this call (`sys`: with the system columns, as `sys::with_sys` gives them).
pub async fn facts_once(lake: &Lake, meta: &TableMeta, sys: bool) -> Result<Facts> {
    let columns = match sys {
        true => crate::sys::with_sys(meta).columns,
        false => meta.columns.clone(),
    };
    let schema = read_schema(&columns)?;
    let manifests = crate::manifest::list(lake, meta).await?;
    let ranges = crate::manifest::ranges(&manifests, &meta.files, &schema);
    let statistics = count(meta, &schema, &ranges, None);
    let known = Arc::new(crate::hot::known(&statistics, &schema));
    Ok(Facts { schema, manifests, ranges, count: statistics, known })
}

/// A table's statistics for planning (`Pruned::sized`): its rows and bytes (a slice's `share`, else the entry's: its
/// sealed manifests' totals and its files'), and each column's range and distinct values (`sketch.rs`, no more than its
/// range or its rows could hold).
fn count(meta: &TableMeta, schema: &SchemaRef, ranges: &crate::manifest::Stats, share: Option<(u64, u64)>) -> datafusion::common::Statistics {
    use datafusion::common::stats::Precision;
    let (rows, bytes) = share.unwrap_or_else(|| {
        let sealed = meta.sealed.clone().unwrap_or_default();
        let (rows, bytes) = meta.files.iter().fold((0, 0), |(r, b), f| (r + f.rows, b + f.bytes));
        (sealed.rows + rows, sealed.bytes + bytes)
    });
    let mut stats = datafusion::common::Statistics::new_unknown(schema);
    (stats.num_rows, stats.total_byte_size) = (Precision::Inexact(rows as usize), Precision::Inexact(bytes as usize));
    for (i, f) in schema.fields().iter().enumerate() {
        let parse = |v: &String| datafusion::common::ScalarValue::try_from_string(v.clone(), f.data_type()).ok();
        let range = ranges.get(f.name()).and_then(|(lo, hi)| Some((parse(lo)?, parse(hi)?)));
        // Distinct values: what the table's sketch saw (`sketch.rs`), no more than its range could hold, nor than its rows.
        let sketched = meta.sketch.get(f.name()).and_then(|s| crate::sketch::estimate(s));
        if let Some(n) = crate::sketch::distinct(sketched, range.as_ref().map(|(lo, hi)| (lo, hi)), rows) {
            stats.column_statistics[i].distinct_count = Precision::Inexact(n);
        }
        if let Some((lo, hi)) = range {
            (stats.column_statistics[i].min_value, stats.column_statistics[i].max_value) = (Precision::Inexact(lo), Precision::Inexact(hi));
        }
    }
    stats
}

/// An append table (or a distributed query's slice of one) that picks its files per query: those
/// whose min/max can match the query's filters, from its inline files and, through the manifest
/// list, its sealed ones (`manifest.rs`). Plus the log tail, which always counts.
pub struct Pruned {
    pub lake: Arc<Lake>,
    pub name: String,
    pub meta: Arc<TableMeta>,
    pub facts: Arc<Facts>,                                  // the whole table's: its schema, manifests, ranges and statistics (`facts`)
    pub manifests: Option<Vec<crate::manifest::Manifest>>, // a slice's share; None: the table's list (in `facts`)
    pub upto: Option<u64>,                                  // the log up to this segment (None: the latest)
    pub at: Option<u64>,                                    // the commit the query reads as of (a slice: its coordinator's; None: the latest)
    pub share: Option<(u64, u64)>, // a distributed query's slice of it, and the whole table's (rows, bytes)
    pub range: Option<crate::ranges::Range>, // a distributed query's slice by a key's range: only its rows
    pub own: std::sync::OnceLock<(datafusion::common::Statistics, Arc<std::collections::HashMap<String, crate::hot::Known>>)>, // a slice's own, worked out once (`sized`)
}

impl std::fmt::Debug for Pruned {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Pruned({})", self.name) }
}

impl Pruned {
    /// The statistics a scan plans with, and `hot::known` of them: the whole table's (from `facts`) for a scan of all of it;
    /// a slice's own, its rows the whole table's (`share`), worked out once here (the join order asks for them per input it costs).
    fn sized(&self) -> (datafusion::common::Statistics, Arc<std::collections::HashMap<String, crate::hot::Known>>) {
        match self.share {
            None => (self.facts.count.clone(), self.facts.known.clone()),
            Some(_) => self.own.get_or_init(|| {
                let stats = count(&self.meta, &self.facts.schema, &self.facts.ranges, self.share);
                let known = Arc::new(crate::hot::known(&stats, &self.facts.schema));
                (stats, known)
            }).clone(),
        }
    }

    /// Any other table's rows, planned as a query of their own.
    async fn planned(&self, meta: &TableMeta, range: Option<Expr>, projection: &[usize], upto: Option<u64>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let e = |e: anyhow::Error| datafusion::error::DataFusionError::External(e.into());
        let ctx = self.lake.session();
        if meta.changed {
            // (the query this scan is part of may still swap the anti-join's sides: DataFusion
            // refuses once a join has built its dynamic filter)
            ctx.state_ref().write().config_mut().options_mut().set("datafusion.optimizer.enable_dynamic_filter_pushdown", "false").map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
        }
        let df = match meta.changed {
            true => current(&self.lake, &ctx, &self.name, meta, upto, self.at).await.map_err(e)?,
            false => raw(&self.lake, &ctx, &self.name, meta, upto).await.map_err(e)?,
        };
        let df = match range {
            Some(r) => df.filter(r)?, // (reaches the Parquet reader: row groups outside it are skipped)
            None => df,
        };
        let df = df.select_columns(&projection.iter().map(|&i| self.facts.schema.field(i).name().as_str()).collect::<Vec<_>>())?;
        df.create_physical_plan().await
    }
}

#[async_trait::async_trait]
impl TableProvider for Pruned {
    fn schema(&self) -> SchemaRef { self.facts.schema.clone() }
    fn table_type(&self) -> datafusion::datasource::TableType { datafusion::datasource::TableType::Base }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        Ok(vec![datafusion::logical_expr::TableProviderFilterPushDown::Inexact; filters.len()]) // (they choose files; rows are filtered above)
    }

    /// How big the table is, from what the catalog already knows: its files' row counts and bytes,
    /// plus the sealed manifests' totals (`manifest.rs`), without opening a single footer. A
    /// distributed query's slice reports the whole table (`share`), so every node orders its joins
    /// alike. The log tail isn't counted — it is bounded by a tiering round, and this is for
    /// choosing a join order (`optimize::JoinOrder`), not for counting rows.
    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        Some(self.sized().0)
    }

    async fn scan(&self, state: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, filters: &[Expr], _: Option<usize>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let e = |e: anyhow::Error| datafusion::error::DataFusionError::External(e.into());
        let range = match &self.range {
            Some(r) => Some(r.expr(self.facts.schema.field_with_name(&r.column)?.data_type()).map_err(e)?),
            None => None,
        };
        let filters: Vec<Expr> = filters.iter().cloned().chain(range.clone()).collect();
        let listed = self.manifests.as_deref().unwrap_or(self.facts.manifests.as_slice());
        let files = crate::manifest::pruned(&self.lake, &self.meta, Some(listed), &filters, &self.facts.schema).await.map_err(e)?;
        // The log's end is fixed first, then the table asked whether it changed since its entry was
        // read: a change makes `{t}$deleted` in or before its commit, so one not there now means no
        // row up to `upto` is a change's. (An entry read before a table's first UPDATE, its log read
        // past it, kept both versions of the row: `harness.py attached`.)
        let upto = Some(self.upto.unwrap_or_else(|| self.lake.visible()));
        let mut changed = self.meta.changed;
        if !changed && self.meta.key.is_empty() && self.meta.ext.is_none() {
            changed = self.lake.cat.get_raw(&crate::store::table_key(&crate::sys::deleted(&self.name))).await.map_err(e)?.is_some();
        }
        let all: Vec<usize> = (0..self.facts.schema.fields().len()).collect();
        let plain = !changed && range.is_none() && self.meta.key.is_empty() && self.meta.ext.is_none() && self.lake.hot.on();
        let plan = match plain {
            true => {
                let known = self.sized().1;
                plain_scan(&self.lake, state, &self.name, &self.meta, files, upto, projection, known).await.map_err(e)?
            }
            false => {
                let meta = TableMeta { files, sealed: None, changed, ..(*self.meta).clone() };
                self.planned(&meta, range, projection.unwrap_or(&all), upto).await?
            }
        };
        let Some((rows, bytes)) = self.share else { return Ok(plan) };
        // (with each column's distinct values and range, from the catalog: an aggregate over a
        // slice then knows it puts out a few groups, not a row per row — `guard.rs` weighs that)
        let stats = self.sized().0;
        let columns = Some(projection.unwrap_or(&all).iter().map(|&i| stats.column_statistics[i].clone()).collect());
        Ok(Arc::new(crate::spmd::ShareExec::new(plan, &self.name, rows, bytes, self.range.as_ref().map(|r| r.column.clone()), columns)?))
    }
}

/// Keyed tables as their users see them. Upsert tables: the latest row per key, without deleted
/// rows (a true `_deleted` column). Merge tables: each key's rows combined by their merge
/// functions. `sorted` orders by key, so a file prunes well on key lookups (used when writing
/// files) — after the table's `cluster_by` column, if it has one. `keep_deleted` leaves delete markers in: an intermediate file still has to shadow what
/// older files hold for that key; a full compaction drops them.
pub fn latest_sql(meta: &TableMeta, raw_table: &str, sorted: bool, keep_deleted: bool) -> String {
    debug_assert!(!meta.key.is_empty(), "latest_sql needs a key: an append table has no versions");
    let q = |c: &String| format!("\"{c}\"");
    let key = meta.key.iter().map(q).collect::<Vec<_>>().join(", ");
    let order = match (sorted, meta.cluster.len()) {
        (true, 0 | 1) => format!(" ORDER BY {}", meta.cluster.iter().chain(&meta.key).map(q).collect::<Vec<_>>().join(", ")),
        _ => String::new(), // (two or more cluster_by columns: a Hilbert curve's order, `tier::latest`)
    };
    if !meta.merge.is_empty() {
        let cols = meta.columns.iter().map(|(c, _)| match meta.merge.get(c).map(String::as_str) {
            Some("count") => format!("sum({}) AS {}", q(c), q(c)), // (partial counts add up)
            Some(f) => format!("{f}({}) AS {}", q(c), q(c)),
            None => q(c),
        });
        let live = if keep_deleted { String::new() } else { live(meta) }; // (windows past their TTL drop out; emptied groups too)
        return format!("SELECT * FROM (SELECT {} FROM \"{raw_table}\" GROUP BY {key}){live}{order}", cols.collect::<Vec<_>>().join(", "));
    }
    let out = meta.columns.iter().map(|(c, _)| q(c)).collect::<Vec<_>>().join(", ");
    let deleted = if keep_deleted { String::new() } else { live(meta) }; // (a partial merge keeps markers and expired rows)
    if let Some(o) = &meta.order {
        // By event time: each key's greatest `order_by`, then the last to come among rows that tie
        // on it — two hash aggregates and two hash joins (`(key, _ord)` is one row).
        let on = |a: &str, b: &str| meta.key.iter().map(|k| format!("{a}.{} = {b}.{}", q(k), q(k))).collect::<Vec<_>>().join(" AND ");
        let picked = meta.columns.iter().map(|(c, _)| format!("c.{} AS {}", q(c), q(c))).collect::<Vec<_>>().join(", ");
        return format!("WITH m AS (SELECT {key}, max({o}) AS __m FROM \"{raw_table}\" GROUP BY {key}), \
            c AS (SELECT r.* FROM \"{raw_table}\" r JOIN m ON {} AND r.{o} IS NOT DISTINCT FROM m.__m), \
            l AS (SELECT {key}, max(\"_ord\") AS __o FROM c GROUP BY {key}) \
            SELECT {out} FROM (SELECT {picked} FROM c JOIN l ON {} AND c.\"_ord\" = l.__o){deleted}{order}", on("r", "m"), on("c", "l"), o = q(o));
    }
    // Newest version per key as a grouped aggregate (a hash table), not a window (a sort).
    let newest = |c: &String| format!("first_value({} ORDER BY \"_ord\" DESC) AS {}", q(c), q(c));
    let cols = meta.columns.iter().map(|(c, _)| if meta.key.contains(c) { q(c) } else { newest(c) });
    format!("SELECT {out} FROM (SELECT {} FROM \"{raw_table}\" GROUP BY {key}){deleted}{order}",
            cols.collect::<Vec<_>>().join(", "))
}

/// The current rows of a keyed table, over `raw_table`. When the source already holds one row
/// per key — a single file, with nothing in the log after it — the "newest wins" window (or the
/// merge GROUP BY) is skipped, so reads of a compacted table cost a plain scan.
pub fn current_sql(meta: &TableMeta, raw_table: &str, upto: u64) -> String {
    if meta.files.len() > 1 || upto != meta.tiered {
        return latest_sql(meta, raw_table, false, false);
    }
    let cols = meta.columns.iter().map(|(c, _)| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
    format!("SELECT {cols} FROM \"{raw_table}\"{}", live(meta))
}

/// ` WHERE …` keeping a keyed table's live rows: not deleted, not past their TTL ("" if nothing to drop).
pub fn live(meta: &TableMeta) -> String {
    let deleted = meta.columns.iter().any(|(c, _)| c == "_deleted").then(|| "\"_deleted\" IS NOT TRUE".to_string());
    // A view's group whose rows a change took back (its count added up to 0) is gone.
    let emptied = meta.merge.iter().find(|(_, f)| *f == "count").map(|(c, _)| format!("\"{c}\" <> 0"));
    let conds: Vec<String> = deleted.into_iter().chain(emptied).chain(Some(meta.ttl_sql()).filter(|t| !t.is_empty())).collect();
    if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) }
}

tokio::task_local! {
    /// Tables a request sent along with its SQL (Arrow): its queries see them by name, nothing
    /// else does, and they run on this node only (`POST /sql`, ADR-023).
    pub static SENT: Arc<Vec<(String, Vec<RecordBatch>)>>;
}

/// Does this request carry tables of its own?
pub fn sent() -> bool { SENT.try_with(|t| !t.is_empty()).unwrap_or(false) }

tokio::task_local! {
    /// The memory of the last answer a request collected (`collect`), counted until the request's body
    /// is sent: an answer sent in pieces (`statement::Rendered`) takes it over. Only the last one is kept:
    /// an earlier statement's batches were dropped when that statement ended.
    pub static HELD: std::cell::RefCell<Option<MemoryReservation>>;
}

tokio::task_local! {
    /// The pieces the one query a door sends may hand its answer to the door in (`statement::sql`): once an
    /// answer reaches `stream_bytes()`, it goes to the door as its plan makes it (`collect_or_send`).
    pub static PIECES: Arc<Pieces>;
}

/// A request's way to hand its door an answer in pieces (`statement::sql`).
pub struct Pieces {
    /// Some(format) while a query may send its answer this way (`arm`).
    armed: std::sync::Mutex<Option<Option<String>>>,
    /// The door's end, taken by the one answer handed over.
    hand: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<Handed>>>,
    /// The rows sent, once an answer was handed over: 0 at once, then the count at its end.
    sent: tokio::sync::watch::Sender<Option<u64>>,
}

impl Pieces {
    pub fn new(hand: tokio::sync::oneshot::Sender<Handed>) -> Arc<Pieces> {
        Arc::new(Pieces { armed: Default::default(), hand: std::sync::Mutex::new(Some(hand)), sent: tokio::sync::watch::channel(None).0 })
    }
}

/// An answer handed to its door: its first piece's bytes, the rest as they are written, and whether its last
/// was written (a channel that closes before it is an answer cut short: the body must fail then, never end as
/// if whole). `stop`: the statement's stop flag, for a plan's pieces (`send_rest`), which the body sets when the
/// client goes before the end (`audit::guarded`); None when the rows were already collected.
pub struct Handed {
    pub first: Vec<u8>,
    pub rest: tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    pub ended: Arc<std::sync::atomic::AtomicBool>,
    pub stop: Option<Arc<std::sync::atomic::AtomicBool>>,
}

/// Lets the query a request runs send its answer in pieces, until it is dropped (`arm`).
pub struct Armed;

impl Drop for Armed {
    fn drop(&mut self) {
        let _ = PIECES.try_with(|p| *p.armed.lock().unwrap_or_else(|e| e.into_inner()) = None);
    }
}

/// Lets the query a door runs send its answer in `format`'s pieces (`collect_or_send`); nothing outside `PIECES`.
pub fn arm(format: Option<&str>) -> Armed {
    let _ = PIECES.try_with(|p| *p.armed.lock().unwrap_or_else(|e| e.into_inner()) = Some(format.map(str::to_string)));
    Armed
}

/// The rows of the request's answer once it was handed to its door (`Some(0)` at once, then the count at its
/// end); None before, and outside `PIECES`.
pub fn handed() -> Option<u64> { PIECES.try_with(|p| *p.sent.borrow()).ok().flatten() }

/// Waits until the request's answer is handed to its door: for ever outside `PIECES`, or if it never is.
pub async fn when_handed() {
    let Ok(mut sent) = PIECES.try_with(|p| p.sent.subscribe()) else { return std::future::pending().await };
    let reached = sent.wait_for(|s| s.is_some()).await.is_ok();
    if !reached {
        std::future::pending::<()>().await
    }
}

/// The size from which a query's answer goes to its door in pieces (`PONDRA_STREAM_MB`, 8 MB of Arrow), read
/// once. 0 sends every answer in pieces (the tests compare the two ways).
pub fn stream_bytes() -> usize {
    static MB: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| crate::env::or("PONDRA_STREAM_MB", 8));
    *MB << 20
}

/// An answer's batches, collected from a plan's partitions. They are counted in the query memory
/// (DataFusion counts the operators', not what they hand back), so an answer bigger than the memory
/// stops with 53200 instead of taking the node down.
pub async fn collect(plan: Arc<dyn ExecutionPlan>, task: Arc<TaskContext>) -> Result<Vec<RecordBatch>> {
    gather(plan, task, None).await
}

/// `collect`, for the query a door sends (`App::run_query`): once `arm`ed, its answer goes to the door in pieces
/// past `stream_bytes()` (`send_rest`), and no batches come back here (`handed` says how many rows went).
pub async fn collect_or_send(plan: Arc<dyn ExecutionPlan>, task: Arc<TaskContext>) -> Result<Vec<RecordBatch>> {
    let format = PIECES.try_with(|p| p.armed.lock().unwrap_or_else(|e| e.into_inner()).clone()).ok().flatten();
    gather(plan, task, format).await
}

/// `collect`'s loop. `send`: the answer may go to its door in pieces of this format (`collect_or_send`).
async fn gather(plan: Arc<dyn ExecutionPlan>, task: Arc<TaskContext>, send: Option<Option<String>>) -> Result<Vec<RecordBatch>> {
    let held = MemoryConsumer::new("an answer").register(task.memory_pool());
    let mut counted = RecordBatchMemoryCounter::new(); // (each buffer once: batches cut from one batch or page share theirs)
    let mut stream = datafusion::physical_plan::execute_stream(plan, task)?;
    let mut out = vec![];
    while let Some(batch) = stream.try_next().await? {
        // (made what its door sends (`answer::as_sent`) before it is counted when it may be handed over: the hand-off measures
        // what `big` measures, so a small answer whose strings were other rows' is not sent in pieces, nor left out of `Results`)
        let batch = match &send { Some(format) => crate::answer::as_sent(batch, format.as_deref()), None => batch };
        if held.try_grow(counted.count_batch(&batch)).is_err() {
            return Err(too_big());
        }
        out.push(batch);
        if let Some(format) = &send {
            if held.size() >= stream_bytes() && may_hand() {
                return send_rest(out, stream, held, format.as_deref()).await;
            }
        }
    }
    keep_held(held);
    Ok(out)
}

/// The memory an answer kept for a ticket to read back holds in `pool` (Flight's `kept`): its batches counted as `gather`
/// counts them, each buffer once. None when it doesn't fit: the caller refuses it with `too_big`, and keeps nothing.
pub fn kept_memory(pool: &Arc<dyn MemoryPool>, batches: &[RecordBatch]) -> Option<MemoryReservation> {
    let held = MemoryConsumer::new("a Flight answer kept for its ticket").register(pool);
    let mut counted = RecordBatchMemoryCounter::new();
    batches.iter().try_for_each(|b| held.try_grow(counted.count_batch(b))).ok()?;
    Some(held)
}

/// The refusal of an answer bigger than the query memory (53200), with the way out.
pub(crate) fn too_big() -> anyhow::Error {
    crate::codes::coded("53200", format!("the answer is bigger than this node's memory for queries ({} MB): ask for fewer rows (a LIMIT or a filter), or write them out with COPY (…) TO a file or folder", memory_limit() >> 20))
}

/// The refusal of a client that went before the answer's end (57014).
fn gone() -> anyhow::Error { crate::codes::coded("57014", "the client went before the answer's end") }

/// Whether the request's door can still take an answer in pieces: its one `hand` is not given away yet.
fn may_hand() -> bool { PIECES.try_with(|p| p.hand.lock().unwrap_or_else(|e| e.into_inner()).is_some()).unwrap_or(false) }

fn take_hand() -> Option<tokio::sync::oneshot::Sender<Handed>> {
    PIECES.try_with(|p| p.hand.lock().unwrap_or_else(|e| e.into_inner()).take()).ok().flatten()
}

fn set_sent(rows: Option<u64>) {
    let _ = PIECES.try_with(|p| p.sent.send_replace(rows));
}

/// Sends an answer in pieces as its plan makes them: the batches so far, then the rest, each written as `format`
/// writes it (`answer::Encoding`). The first piece is written before the door is handed anything, so a format that
/// can't write these columns is refused as `render` refuses it. Then the door's body takes each piece as its client
/// reads it (two at a time), and each batch is counted only while it is written.
async fn send_rest(out: Vec<RecordBatch>, mut stream: datafusion::physical_plan::SendableRecordBatchStream, mut held: MemoryReservation, format: Option<&str>) -> Result<Vec<RecordBatch>> {
    let mut enc = crate::answer::Encoding::new(format);
    let mut prefix: std::collections::VecDeque<RecordBatch> = crate::answer::pieces(out.into_iter().map(|b| enc.lean(b))).into();
    let (mut rows, mut first) = (0u64, vec![]);
    if let Some(piece) = prefix.pop_front() {
        first = enc.step(Some(&piece))?;
        rows = piece.num_rows() as u64;
    }
    let Some(hand) = take_hand() else { bail!("an answer in pieces has no door") };
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let ended = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop = crate::audit::stop(); // (this runs inside the statement: its flag is here, and the door's body outlives this)
    if hand.send(Handed { first, rest: rx, ended: ended.clone(), stop }).is_err() {
        return Err(gone());
    }
    set_sent(Some(0));
    match pour(&mut enc, &tx, prefix, &mut stream, &mut held).await {
        Ok(more) => {
            ended.store(true, std::sync::atomic::Ordering::Release); // (set before the channel closes: the body ends whole only then)
            drop(tx);
            set_sent(Some(rows + more));
            Ok(vec![])
        }
        Err(e) => {
            let _ = tx.try_send(Err(std::io::Error::other(format!("{e:#}")))); // (if the channel is full, its close says the answer was cut short)
            Err(e)
        }
    }
}

/// An answer already whole, sent in pieces as `send_rest` sends a plan's (`statement::answer`): its first piece is written
/// now, the rest on the queries' runtime as its client reads them. `held`'s memory goes when the last piece is written,
/// or when the client goes (the body that holds the pieces counts the request for a node told to stop: `server::pieces`).
pub fn hand_out(batches: Vec<RecordBatch>, format: Option<&str>, held: Option<MemoryReservation>) -> Result<Handed> {
    let mut enc = crate::answer::Encoding::new(format);
    let mut parts: std::collections::VecDeque<RecordBatch> = crate::answer::pieces(batches.into_iter().map(|b| enc.lean(b))).into();
    let ended = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<bytes::Bytes>>(2);
    let Some(piece) = parts.pop_front() else {
        // (no rows: the answer is its end, and nothing is left to write)
        let first = enc.step(None)?;
        ended.store(true, std::sync::atomic::Ordering::Release);
        return Ok(Handed { first, rest: rx, ended, stop: None });
    };
    let first = enc.step(Some(&piece))?;
    let done = ended.clone();
    crate::panics::aside(async move {
        let _held = held;
        let written = match write_all(&mut enc, &tx, parts).await {
            Ok(_) => step_to(&mut enc, &tx, None).await,
            Err(e) => Err(e),
        };
        match written {
            Ok(()) => done.store(true, std::sync::atomic::Ordering::Release), // (the end is written: the body may end)
            Err(e) => {
                let _ = tx.try_send(Err(std::io::Error::other(format!("{e:#}")))); // (if the channel is full, its close says the answer was cut short)
            }
        }
    });
    Ok(Handed { first, rest: rx, ended, stop: None }) // (the rows are collected: no search runs after this)
}

/// Writes each piece to the body (`step_to`), and returns the rows written.
async fn write_all(enc: &mut crate::answer::Encoding, tx: &tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>, pieces: impl IntoIterator<Item = RecordBatch>) -> Result<u64> {
    let mut rows = 0u64;
    for piece in pieces {
        rows += piece.num_rows() as u64;
        step_to(enc, tx, Some(&piece)).await?;
    }
    Ok(rows)
}

/// The pieces after the first: the prefix's, then each batch of the stream's, counted while it is written; then the
/// end. Returns the rows written.
async fn pour(enc: &mut crate::answer::Encoding, tx: &tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>, prefix: std::collections::VecDeque<RecordBatch>, stream: &mut datafusion::physical_plan::SendableRecordBatchStream, held: &mut MemoryReservation) -> Result<u64> {
    let mut rows = write_all(enc, tx, prefix).await?;
    held.free(); // (the prefix's batches are written: their memory goes)
    loop {
        let next = tokio::select! {
            next = stream.try_next() => next?,
            () = tx.closed() => return Err(gone()), // (the client went: the plan is let go now, not at its next send)
        };
        let Some(batch) = next else { break };
        // (with the batches the plan already has ready, up to a piece: a message for each of them would be one per 8,192 rows)
        let (mut ready, mut ended) = (vec![enc.lean(batch)], false);
        let mut size = ready[0].get_array_memory_size();
        while size < crate::answer::PIECE {
            match futures::FutureExt::now_or_never(stream.try_next()) {
                Some(Ok(Some(more))) => {
                    let more = enc.lean(more);
                    size += more.get_array_memory_size();
                    ready.push(more);
                }
                Some(Ok(None)) => {
                    ended = true;
                    break;
                }
                Some(Err(e)) => return Err(e.into()),
                None => break, // (none is ready: what is goes now)
            }
        }
        held.try_grow(size).map_err(|_| too_big())?;
        for piece in crate::answer::pieces(ready) {
            rows += piece.num_rows() as u64;
            step_to(enc, tx, Some(&piece)).await?;
        }
        held.shrink(size);
        if ended {
            break;
        }
    }
    step_to(enc, tx, None).await?; // (the end)
    Ok(rows)
}

/// Writes one step of the answer (a piece, or None: what closes it) to the body.
async fn step_to(enc: &mut crate::answer::Encoding, tx: &tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>, piece: Option<&RecordBatch>) -> Result<()> {
    let bytes = enc.step(piece)?;
    give(tx, Ok(bytes::Bytes::from(bytes))).await
}

/// Gives the body one piece, or says why it can't: its client went, or took no piece for a minute (a client that
/// stops reading would hold the query's turn and its memory for ever).
async fn give(tx: &tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>, piece: std::io::Result<bytes::Bytes>) -> Result<()> {
    match tokio::time::timeout(std::time::Duration::from_secs(60), tx.send(piece)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(gone()),
        Err(_) => Err(crate::codes::coded("57014", "the client took no piece of the answer for a minute")),
    }
}

/// Keeps an answer's memory counted in the request's scope (`HELD`); a door with no scope of its own
/// (Postgres, Flight, MCP) releases it when the answer is returned.
fn keep_held(held: MemoryReservation) {
    let mut held = Some(held);
    let _ = HELD.try_with(|h| *h.borrow_mut() = held.take());
}

/// The memory of the answer a request kept, for the body that sends it (`statement::answer`).
pub fn take_held() -> Option<MemoryReservation> { HELD.try_with(|h| h.borrow_mut().take()).ok().flatten() }

tokio::task_local! {
    /// The stages of the one statement a request runs, while `PONDRA_TRACE_QUERY` is on (`traced`, `mark`).
    static TRACE: std::cell::RefCell<Option<StatementTrace>>;
}

/// `PONDRA_TRACE_QUERY=1`: where one HTTP `/sql` statement's time goes, one line on stderr at its end. A stage is the
/// time since the mark before it, so the stages add up to the statement's time up to its last mark.
struct StatementTrace {
    start: datafusion::common::instant::Instant,
    last: datafusion::common::instant::Instant,
    stages: Vec<(&'static str, f64)>, // (name, ms)
}

impl StatementTrace {
    fn mark(&mut self, stage: &'static str) {
        let now = datafusion::common::instant::Instant::now();
        self.stages.push((stage, (now - self.last).as_secs_f64() * 1000.0));
        self.last = now;
    }

    fn done(self) {
        let stages: Vec<String> = self.stages.iter().map(|(name, ms)| format!("{name} {ms:.3}")).collect();
        eprintln!("query {:.3}: {}", self.start.elapsed().as_secs_f64() * 1000.0, stages.join(", "));
    }
}

/// `PONDRA_TRACE_QUERY`, checked once: a statement with it off carries no trace.
fn tracing() -> bool {
    static ON: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| crate::env::is_set("PONDRA_TRACE_QUERY"));
    *ON
}

/// A request's statement, run with its trace when `PONDRA_TRACE_QUERY` is on: `mark` stages it as it goes, and
/// its line is printed when `work` ends.
pub async fn traced<T>(work: impl std::future::Future<Output = T>) -> T {
    if !tracing() {
        return work.await;
    }
    let now = datafusion::common::instant::Instant::now();
    let trace = std::cell::RefCell::new(Some(StatementTrace { start: now, last: now, stages: vec![] }));
    TRACE.scope(trace, async {
        let out = work.await;
        let _ = TRACE.try_with(|t| t.borrow_mut().take().map(StatementTrace::done));
        out
    })
    .await
}

/// Ends the stage that has run since the last mark, named `stage`. Nothing outside `traced` (or a task it spawns) is traced.
pub fn mark(stage: &'static str) {
    let _ = TRACE.try_with(|t| t.borrow_mut().as_mut().map(|t| t.mark(stage)));
}

use crate::answer::schema;
use crate::engine::sql;

/// An Arrow IPC stream's batches (none for no bytes).
pub fn read_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(vec![]);
    }
    Ok(datafusion::arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?.collect::<Result<Vec<_>, _>>()?)
}

/// The schema a plan's rows have as they run: the analyzer's types, not the SQL planner's. The planner
/// types a literal such as `1e15` as a decimal with a negative scale, which no table can hold; the
/// analyzer's rules (`optimize::WholeLiterals`, type coercion) type it as the query runs. Only the
/// analyzer runs here: the optimizer keeps a plan's schema (DataFusion checks it), so a query that is
/// only described (a table's columns, a Flight or Postgres description) needn't be optimized to be.
pub fn run_schema(ctx: &SessionContext, plan: &datafusion::logical_expr::LogicalPlan) -> Result<datafusion::common::DFSchemaRef> {
    let state = ctx.state();
    Ok(state.analyzer().execute_and_check(plan.clone(), state.config_options(), |_, _| {})?.schema().clone())
}

/// A table of this lake in a session: `t` in the default schema (`public`), `s.t` in schema `s`,
/// exactly as named (SQL folds unquoted names to lower case; a name that isn't must be quoted).
pub fn table_ref(name: &str) -> datafusion::common::TableReference {
    match name.split_once('.') {
        Some((s, t)) => datafusion::common::TableReference::partial(s.to_string(), t.to_string()),
        None => datafusion::common::TableReference::bare(name.to_string()),
    }
}

/// A session with every table and stored view `sql` names registered (a cheap word filter),
/// except `except`, which the caller registers itself. A lake is a catalog of schemas (`ddl.rs`):
/// this one is the default catalog, and also goes by its own name; each attached lake is a
/// catalog by the name it was attached as, and — unless a schema here has that name — its
/// `public` tables are also that schema's (`name.table`, from before schemas).
pub async fn session(lake: &Lake, sql: &str, except: &str) -> Result<SessionContext> { session_at(lake, sql, except, None).await }

/// `session`, this lake's tables as of log segment `upto` (every query in it reads the same rows).
pub async fn session_at(lake: &Lake, sql: &str, except: &str, upto: Option<u64>) -> Result<SessionContext> {
    use crate::text::{mentions, split, PUBLIC};
    lake.caught_up().await?; // (a node that just started: not from an older catalog than its leader's)
    let upto = upto.or_else(crate::txn::snapshot); // (in a transaction: as of its snapshot, its writes over it)
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
    let ctx = crate::settings::apply(lake.session()).await?; // (the session's `SET datafusion.…`)
    crate::udf::register(lake, &ctx).await?; // the lake's own functions (`POST /functions/…`)
    crate::pyfn::register(lake, &ctx).await?; // …and its Python functions (`CREATE FUNCTION … LANGUAGE python`)
    let listing = ["information_schema", "show tables", "show columns"].iter().any(|w| sql.to_lowercase().contains(w)); // (every table)
    let mut temps = crate::temp::views(sql); // (the session's own: over the lake's of the same name)
    for (_, v) in temps.iter_mut() {
        let expanded = crate::routines::expand_stored(lake, v).await?; // (read as a stored view is: the door's rewrites, with the tables' types now; the text was kept as written)
        *v = expanded;
    }
    let seen = temps.iter().fold(sql.to_string(), |t, (_, s)| format!("{t} {s}"));
    let mut views = stored_views(lake, &seen, listing).await?; // (their tables are wanted too)
    views.retain(|(n, _)| !temps.iter().any(|(m, _)| m == n));
    views.extend(temps);
    let text = views.iter().fold(sql.to_string(), |t, (_, s)| format!("{t} {s}"));
    if crate::pattern::wanted(&text) || text.contains("pondra_match") || text.contains("pondra_open") {
        crate::matcher::register(&ctx); // (again, last: a DEFINE's glue sees the session's every function, the lake's own and Python ones too)
    }
    let named_sys = crate::sys::mentioned(&text); // (named: the tables get their system columns)
    let default = ctx.catalog(&crate::ddl::lake_name(lake)).expect("the lake's catalog"); // (`lake.schema.table`)
    for s in crate::ddl::schemas(lake).await?.into_iter().filter(|s| s != PUBLIC) {
        default.register_schema(&s, Arc::new(MemorySchemaProvider::new()))?;
    }
    let entries = lake.cat.scan_raw("t/", "t0").await?;
    lake.cat.settle().await?; // (the log read as far as these entries: `Catalog::settle`)
    let upto = upto.or_else(|| Some(lake.visible())); // (one end for every table it reads, fixed once their entries are read: a table read twice, or two a transaction wrote, read one commit)
    let mut unlisted = vec![]; // (tables a listing leaves out, which the views over them still plan against: below)
    for (key, raw) in entries {
        let name = &key[2..];
        // (a staged table is read only where the SQL names it quoted: its constraint checks, `constraints::held` and `kept`)
        let staged = crate::sys::staged_for(name).is_some() && text.contains(&format!("\"{name}\""));
        if name != except && (staged || !crate::sys::hidden(name) && (listing || mentions(&text, name))) {
            let meta = lake.cat.parsed(&key, &raw)?; // (only the tables it names: decoding every one was half of a small query's time)
            let touched = crate::txn::touched(name);
            let view = table_view(lake, &ctx, name, &meta, touched || named_sys, upto).await?;
            let mut t = named(&ctx, view, &meta, names_deleted(&text))?;
            if touched {
                t = crate::txn::overlaid(&ctx, name, &meta.logical().key, t, crate::sys::mentioned(&text))?;
            }
            if let Some(h) = &meta.history {
                t = crate::views::history_view(&ctx, t, h, crate::sys::mentioned(&text)).await?; // (each version's __start_at and __end_at, as it is read)
            }
            match guarded(name, t.clone(), listing) {
                Some(t) => {
                    ctx.register_table(table_ref(name), t)?;
                }
                None => unlisted.push((name.to_string(), Arc::new(Guarded { inner: t, name: name.to_string(), columns: None, why: None }) as Arc<dyn TableProvider>)),
            }
        }
    }
    let (names, direct) = (crate::ext_names::names(&text), crate::ext_names::names(sql));
    let metas = futures::future::join_all(names.iter().map(|n| crate::ext::meta(lake, n))).await; // (listed at once)
    for (name, meta) in names.into_iter().zip(metas) {
        let meta = match meta {
            Ok(m) => Arc::new(m.expect("files")), // (files anywhere: `ext.rs`)
            Err(_) if listing && !direct.contains(&name) => continue, // (a view's files this caller can't read, or that are gone: a listing shows the rest)
            Err(e) => return Err(e),
        };
        let view = table_view_once(lake, &ctx, &name, meta.clone(), upto).await?;
        ctx.register_table(datafusion::common::TableReference::bare(name.clone()), named(&ctx, view, &meta, false)?)?; // (another engine's names for its columns)
    }
    for name in crate::past_names::names(&text) {
        let (table, t) = crate::past::table(lake, &ctx, &name, upto, crate::sys::mentioned(&text)).await?; // (`t AT (…)`: ADR-043)
        if let Some(t) = guarded(&table, t, false) {
            ctx.register_table(datafusion::common::TableReference::bare(name), t)?;
        }
    }
    let attached = lake.attached.read().unwrap().clone();
    for (ns, other) in attached {
        if !listing && !mentions(&text, &ns) {
            continue;
        }
        let locked = crate::users::across(&other, &ns).await.err().map(|e| e.to_string()); // (its own sign-in: `users::across`)
        let catalog = Arc::new(MemoryCatalogProvider::new());
        for s in crate::ddl::schemas(&other).await? {
            catalog.register_schema(&s, Arc::new(MemorySchemaProvider::new()))?;
        }
        let old = default.schema(&ns).is_none();
        if old {
            default.register_schema(&ns, Arc::new(MemorySchemaProvider::new()))?;
        }
        let entries = other.cat.scan_raw("t/", "t0").await?;
        other.cat.settle().await?; // (as this lake's own, above)
        let upto = Some(other.visible()); // (one end for its tables too, as this lake's above)
        for (key, raw) in entries {
            let name = &key[2..];
            if crate::sys::hidden(name) || !(listing || mentions(&text, name)) {
                continue;
            }
            let meta = other.cat.parsed(&key, &raw)?;
            let view = named(&ctx, table_view(&other, &ctx, name, &meta, named_sys, upto).await?, &meta, names_deleted(&text))?;
            let Some(view) = across(&format!("{ns}.{name}"), view, listing, &locked) else { continue };
            let (s, t) = split(name);
            if old && s == PUBLIC {
                default.schema(&ns).expect("registered").register_table(t.to_string(), view.clone())?;
            }
            catalog.schema(s).expect("registered").register_table(t.to_string(), view)?;
        }
        ctx.register_catalog(ns, catalog);
    }
    if crate::runs::mentioned(&text) && default.schema("pondra").is_none() {
        // (`pondra.runs`, `pondra.routines`, `pondra.tasks`; a lake's own schema called pondra is one made before 1.0, which wins as it did: 1.0 refuses a new one, 42939)
        use datafusion::catalog::SchemaProvider;
        let system = Arc::new(MemorySchemaProvider::new());
        for (name, table) in crate::runs::tables(lake).await?.into_iter().chain(crate::users::tables(lake).await?).chain(crate::objects::tables(lake, &text).await?).chain(crate::copy::tables(lake, &text).await?) {
            system.register_table(name.into(), table)?;
        }
        let runs = match lake.cat.meta(&crate::store::table_key(crate::runs::TABLE)).await? {
            Some(meta) => named(&ctx, table_view(lake, &ctx, crate::runs::TABLE, &meta, named_sys, upto).await?, &meta, false)?,
            None => crate::runs::no_runs()?,
        };
        system.register_table("runs".into(), runs)?;
        if crate::audit::check(&text)? {
            // (a superuser's only: `audit.rs`)
            let audit = match lake.cat.meta(&crate::store::table_key(crate::audit::TABLE)).await? {
                Some(meta) => named(&ctx, table_view(lake, &ctx, crate::audit::TABLE, &meta, named_sys, upto).await?, &meta, false)?,
                None => crate::audit::empty()?,
            };
            system.register_table("audit".into(), audit)?;
        }
        if text.to_lowercase().contains("pondra.history") || text.to_lowercase().contains("pondra.learned") {
            // (every statement's row: an admin's all, anyone else's their own: `history.rs`; what they learned, `learned.rs`)
            let history = match lake.cat.meta(&crate::store::table_key(crate::history::TABLE)).await? {
                Some(meta) => named(&ctx, table_view(lake, &ctx, crate::history::TABLE, &meta, named_sys, upto).await?, &meta, false)?,
                None => crate::history::empty()?,
            };
            let history = crate::history::visible(&ctx, history)?;
            if text.to_lowercase().contains("pondra.learned") {
                system.register_table("learned".into(), crate::learned::table(&ctx, history.clone()).await?)?;
            }
            system.register_table("history".into(), history)?;
        }
        if text.to_lowercase().contains("pondra.flows") || text.to_lowercase().contains("pondra.expectations") {
            // (a materialized view's flow and its expectations: `views::system`)
            let counts = match lake.cat.meta(&crate::store::table_key(crate::views::EXPECTED)).await? {
                Some(meta) => ctx.read_table(table_view(lake, &ctx, crate::views::EXPECTED, &meta, false, upto).await?)?.collect().await?,
                None => vec![],
            };
            for (name, table) in crate::views::system(lake, counts).await? {
                system.register_table(name.into(), table)?;
            }
        }
        if crate::vars::mentioned(&text) {
            system.register_table("variables".into(), crate::vars::table()?)?; // (the session's or the run's: `vars.rs`)
        }
        default.register_schema("pondra", system)?;
    }
    if let Ok(sent) = SENT.try_with(|t| t.clone()) {
        for (name, batches) in sent.iter() {
            let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(Schema::empty()));
            ctx.deregister_table(name.as_str())?; // (named for this request: it wins)
            ctx.register_table(name.as_str(), Arc::new(MemTable::try_new(schema, vec![batches.clone()])?))?;
        }
    }
    crate::temp::register(&ctx, &text)?; // (the session's temporary tables, over the lake's)
    // A listing leaves out a table its user may not read, but a view over it is planned against it: the table is there
    // for the views' planning and taken out again after, unless the session has a table of that name already.
    let mut planned_only = vec![];
    for (name, t) in unlisted {
        if !ctx.table_exist(table_ref(&name))? {
            ctx.register_table(table_ref(&name), t)?;
            planned_only.push(table_ref(&name));
        }
    }
    register_views(&ctx, views, !listing, listing).await?; // (a listing shows what it can)
    for name in planned_only {
        ctx.deregister_table(name)?;
    }
    if listing {
        let direct = crate::ext_names::names(sql);
        for n in crate::ext_names::names(&text).into_iter().filter(|n| !direct.contains(n)) {
            ctx.deregister_table(datafusion::common::TableReference::bare(n))?; // (files its views read: theirs, not tables to list)
        }
    }
    Ok(ctx)
}

/// A memory pool for one statement, over the lake's (which every statement shares): what its operators reserve goes
/// through it (`task_with_peak`), so `PeakRecordingPool`'s peak is the most that statement held at once.
pub fn peak_pool(ctx: &SessionContext) -> Arc<PeakRecordingPool> {
    Arc::new(PeakRecordingPool::new(ctx.runtime_env().memory_pool.clone()))
}

/// The task a statement's plan runs on: `state`'s own (`SessionState::task_ctx`), but with `peak` for its memory pool.
/// Only the runtime differs: the caches, the object stores and the spill disk are the lake's, and the session isn't
/// made again.
pub fn task_with_peak(state: &SessionState, peak: &Arc<PeakRecordingPool>) -> Result<Arc<TaskContext>> {
    let rt = RuntimeEnvBuilder::from_runtime_env(state.runtime_env()).with_memory_pool(peak.clone()).build_arc()?;
    Ok(Arc::new(TaskContext::from(state).with_runtime(rt)))
}

/// Whether a query ran out of memory, so a frugal run may help (DataFusion's "Resources exhausted", 53200).
/// A spill past its cap carries the same words (53100), and a frugal run would spill to the cap again.
pub fn exhausted(e: &anyhow::Error) -> bool { format!("{e:#}").contains("Resources exhausted") && crate::codes::of(e) != "53100" }

/// A session made to run frugally: hash joins as sort-merge joins (hash joins can't spill), sorts
/// keeping 1 MB aside to merge, and the rewrites in `frugal.rs` (a window over whole partitions as a GROUP BY).
pub fn frugal(ctx: &SessionContext) {
    let state = ctx.state_ref();
    let mut state = state.write();
    let o = state.config_mut().options_mut();
    (o.optimizer.prefer_hash_join, o.execution.sort_spill_reservation_bytes) = (false, 1 << 20);
    crate::frugal::mark(o);
}

/// Whether `ctx` is a frugal retry's session (`frugally` made it), for a caller that must know it is the second try.
pub fn retrying(ctx: &SessionContext) -> bool {
    let state = ctx.state_ref();
    let state = state.read();
    crate::frugal::is_on(state.config().options())
}

/// Run `work` on `ctx`, and once more when it ran out of memory: on a frugal copy of `ctx` (`frugal`), which
/// shares its catalogs, so the retry reads the tables the first try registered at the same snapshot, and changes
/// only its own options (`ctx` itself is never changed). An answer half sent is never run again: its client has
/// the first rows. The paths that run a query for someone go through it: a door's query (`app::App::run_query`),
/// `CREATE TABLE … AS` and `INSERT … SELECT` (`write::write_files`) and a node's share of one (`spmd::insert_share`),
/// and `COPY … TO` a file or a Delta or Iceberg table (`copy::copy_to`). Not a Kafka topic (records sent are sent),
/// and not the shares of a spread query or COPY: when one fails, the coordinator runs it again from its own node.
pub async fn frugally<T, F: std::future::Future<Output = Result<T>>>(ctx: &SessionContext, work: impl Fn(SessionContext) -> F) -> Result<T> {
    match work(ctx.clone()).await {
        Err(e) if exhausted(&e) && handed().is_none() => {
            let copy = SessionContext::new_with_state(ctx.state()); // (`state` copies the options: `frugal` changes only the copy's)
            frugal(&copy);
            work(copy).await
        }
        r => r,
    }
}

/// The stored views (`CREATE VIEW`) `sql` names, and the ones they name; every one for a listing.
pub async fn stored_views(lake: &Lake, sql: &str, listing: bool) -> Result<Vec<(String, String)>> {
    let (mut text, mut views) = (sql.to_string(), vec![]);
    let stored = lake.cat.scan_raw("q/", "q0").await?; // (decoded and expanded only when named: every query paid for every view)
    loop {
        let more: Vec<(&String, &bytes::Bytes)> = stored.iter()
            .filter(|(k, _)| !views.iter().any(|(m, _): &(String, String)| m == &k[2..]) && (listing || crate::text::mentions(&text, &k[2..]))).collect();
        if more.is_empty() {
            return Ok(views);
        }
        for (k, raw) in more {
            let v: crate::ddl::StoredView = serde_json::from_slice(raw)?;
            let sql = crate::routines::expand_stored(lake, &v.sql).await?; // (macros as they are now; the path it was made under is in its text)
            text.push_str(&format!(" {sql}"));
            views.push((k[2..].to_string(), sql));
        }
    }
}

/// Stored views over the tables `ctx` has now — again, if they are there already: a node's shares
/// of its tables replace them (`spmd.rs`), and the views must read those. One view may use
/// another: until none is left.
pub async fn register_views(ctx: &SessionContext, mut views: Vec<(String, String)>, strict: bool, listing: bool) -> Result<()> {
    let mut failed = None;
    while !views.is_empty() {
        let before = views.len();
        let mut left = vec![];
        for (name, sql) in views {
            match self::sql(ctx, &crate::sys::planned(&sql)?).await {
                Ok(df) => {
                    ctx.deregister_table(table_ref(&name))?;
                    let view = Arc::new(datafusion::catalog::view::ViewTable::new(df.into_unoptimized_plan(), Some(crate::ext_names::readable(&sql)))); // (its files as SQL named them)
                    if let Some(v) = guarded(&name, view, listing) {
                        ctx.register_table(table_ref(&name), v)?;
                    }
                }
                Err(e) => {
                    failed = Some(anyhow::anyhow!("view {name}: {e}"));
                    left.push((name, sql));
                }
            }
        }
        if left.len() == before {
            match failed {
                Some(e) if strict => return Err(e),
                _ => break,
            }
        }
        views = left;
    }
    Ok(())
}

/// Run `sql` with table `source` standing for just `rows` (new rows of a streaming source); every
/// other table it mentions is read from the lake as usual.
pub async fn over(lake: &Lake, source: &str, rows: Vec<RecordBatch>, sql: &str) -> Result<RecordBatch> {
    let meta: TableMeta = lake.cat.get::<TableMeta>(&table_key(source)).await?.ok_or_else(|| anyhow::anyhow!("no table {source}"))?.logical(); // (rows under SQL's names)
    let sql = crate::sys::planned(sql)?;
    let (out, batches) = crate::fresh::run(lake, source, schema(&meta.columns)?, &rows, &sql, "", Ok).await?; // (its plan kept, when it reads the rows alone)
    Ok(datafusion::arrow::compute::concat_batches(&out, &batches)?)
}

/// A session for `sql` where table `source` is just `rows`, as columns `s`.
pub async fn over_ctx(lake: &Lake, source: &str, s: SchemaRef, rows: Vec<RecordBatch>, sql: &str) -> Result<SessionContext> {
    let ctx = session(lake, sql, source).await?;
    let rows = rows.iter().map(|b| conform(b, &s)).collect::<Result<Vec<_>>>()?;
    ctx.register_table(table_ref(source), Arc::new(MemTable::try_new(s, vec![rows])?))?;
    Ok(ctx)
}

/// The first table in the FROM clause (looking inside a FROM subquery): the stream a view follows,
/// or the table a distributed query slices.
pub fn first_table(sql: &str) -> Result<String> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    fn first(q: &Query) -> Option<String> {
        let SetExpr::Select(s) = q.body.as_ref() else { return None };
        match &s.from.first()?.relation {
            TableFactor::Table { name, .. } => Some(crate::text::object(name)),
            TableFactor::Derived { subquery, .. } => first(subquery),
            _ => None,
        }
    }
    match &Parser::parse_sql(&GenericDialect {}, sql)?[..] {
        [Statement::Query(q)] => first(q).ok_or_else(|| anyhow::anyhow!("a view is one SELECT … FROM <table> …")),
        _ => bail!("a view is one SELECT … FROM <table> …"),
    }
}

#[cfg(test)]
mod tests {
    use super::{exhausted, keep, Facts, FactsMemo, Tail};
    use crate::store::TableMeta;
    use datafusion::common::DataFusionError;
    use std::sync::Arc;

    /// A version's facts, told apart by their row count.
    fn facts(rows: usize) -> Arc<Facts> {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let mut count = datafusion::common::Statistics::new_unknown(&schema);
        count.num_rows = datafusion::common::stats::Precision::Exact(rows);
        Arc::new(Facts { schema, manifests: vec![], ranges: Default::default(), count, known: Default::default() })
    }

    fn rows(facts: Option<Arc<Facts>>) -> Option<usize> { facts?.count.num_rows.get_value().copied() }

    fn memo() -> FactsMemo { FactsMemo::default() }

    #[test]
    fn the_same_entry_hits_and_its_system_columns_version_is_its_own() {
        let mut memo = memo();
        let entry = Arc::new(TableMeta::default());
        assert!(memo.get(&entry, false).is_none(), "nothing kept yet");
        memo.put(&entry, false, &facts(1));
        assert_eq!(rows(memo.get(&entry, false)), Some(1), "the same Arc: the facts kept for it");
        memo.put(&entry, true, &facts(2));
        assert_eq!(rows(memo.get(&entry, true)), Some(2), "with system columns: facts of their own");
        assert_eq!(rows(memo.get(&entry, false)), Some(1), "and without them, still the first");
    }

    #[test]
    fn a_dropped_entry_misses_and_a_copy_made_by_unwrap_or_clone_misses() {
        let mut memo = memo();
        let entry = Arc::new(TableMeta::default());
        memo.put(&entry, false, &facts(3));
        let held = entry.clone();
        let copy = Arc::new(Arc::unwrap_or_clone(entry)); // (`held` still holds the entry, so this clones it)
        assert!(memo.get(&copy, false).is_none(), "a copy is another entry");
        assert_eq!(rows(memo.get(&held, false)), Some(3), "the entry itself still hits");
        drop(held);
        let next = Arc::new(TableMeta::default());
        assert!(memo.get(&next, false).is_none(), "an entry that is gone is never hit by another one");
    }

    #[test]
    fn dead_entries_leave_on_insert() {
        let mut memo = memo();
        let gone = Arc::new(TableMeta::default());
        memo.put(&gone, false, &facts(4));
        drop(gone);
        let next = Arc::new(TableMeta::default());
        memo.put(&next, false, &facts(5));
        assert_eq!(memo.by_key.len(), 1, "the dead entry left when the next one came in");
        assert_eq!(rows(memo.get(&next, false)), Some(5));
    }

    /// A version's facts with these manifests, and nothing else.
    fn sized(manifests: Vec<crate::manifest::Manifest>) -> Arc<Facts> {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let count = datafusion::common::Statistics::new_unknown(&schema);
        Arc::new(Facts { schema, manifests, ranges: Default::default(), count, known: Default::default() })
    }

    /// A manifest whose path is `path` bytes long (its size is what `facts_bytes` counts).
    fn manifest(path: usize) -> crate::manifest::Manifest {
        crate::manifest::Manifest { path: "m".repeat(path), files: 1, rows: 1, bytes: 1, stats: Default::default(), nulls: None }
    }

    /// The planning facts memo holds at most `FACTS_BYTES` of manifests and `FACTS_ENTRIES` versions: a big table's
    /// manifests make room for what fits, and a version past the whole budget is not kept (so a lake of big tables
    /// can't hold memory for their manifest lists).
    #[test]
    fn the_facts_memo_is_bounded_by_bytes_as_well_as_entries() {
        use super::{FACTS_BYTES, FACTS_ENTRIES};
        let mut memo = memo();
        let (one, two, big) = (Arc::new(TableMeta::default()), Arc::new(TableMeta::default()), Arc::new(TableMeta::default()));
        let half = sized(vec![manifest(FACTS_BYTES / 2)]);
        let three_quarters = sized(vec![manifest(FACTS_BYTES / 2), manifest(FACTS_BYTES / 4)]);
        let past_the_budget = sized(vec![manifest(FACTS_BYTES), manifest(FACTS_BYTES)]);
        assert!(super::facts_bytes(&past_the_budget) > FACTS_BYTES);
        memo.put(&one, false, &half);
        assert!(memo.get(&one, false).is_some(), "a version that fits is kept");
        memo.put(&two, false, &three_quarters);
        assert!(memo.bytes <= FACTS_BYTES, "two big versions don't both fit");
        assert!(memo.get(&one, false).is_none(), "the older one made room");
        assert!(memo.get(&two, false).is_some(), "the newer one is kept");
        memo.put(&big, false, &past_the_budget);
        assert!(memo.get(&big, false).is_none(), "past the whole budget: not kept");
        assert!(memo.get(&two, false).is_some(), "and it evicts nothing");
        assert!(memo.bytes <= FACTS_BYTES);
        let metas: Vec<Arc<TableMeta>> = (0..FACTS_ENTRIES + 44).map(|_| Arc::new(TableMeta::default())).collect();
        for m in &metas {
            memo.put(m, false, &sized(vec![]));
        }
        assert_eq!(memo.by_key.len(), FACTS_ENTRIES, "the count bound holds");
        assert_eq!(memo.bytes, 0, "and empty manifests count for nothing");
    }

    #[test]
    fn only_memory_runs_out_is_retried_frugally() {
        // A window that ran out of memory (`held.rs`) is retried frugally.
        let window = DataFusionError::ResourcesExhausted("out of memory: a window holds more rows than this node's memory for queries (64 MB): partition it by more columns, or give the node more memory (--memory-gb)".into());
        assert!(exhausted(&anyhow::Error::new(window)));
        // DataFusion's spill-cap refusal (53100) carries the same words, and a frugal run would spill to the cap again.
        let cap = DataFusionError::ResourcesExhausted("The used disk space during the spilling process has exceeded the allowable limit of 10.00 MB. Please try increasing the config: `datafusion.runtime.max_temp_directory_size`.".into());
        assert!(!exhausted(&anyhow::Error::new(cap)));
        // The memory watch's stop (`oom::OOM`) says "out of memory", not "Resources exhausted": not retried either.
        assert!(!exhausted(&anyhow::anyhow!("{}", crate::oom::OOM)));
    }

    /// A tail of `form` at mark `of` through segment `through`, `bytes` long (the cache keeps only sizes here).
    fn tail(form: &str, of: u64, through: u64, bytes: usize) -> Arc<Tail> {
        Arc::new(Tail { of: (of, form.to_string()), through, rows: vec![], ends: vec![], bytes })
    }

    fn key(table: &str, form: &str) -> (String, String) { (table.to_string(), form.to_string()) }

    fn cache() -> (lru::LruCache<(String, String), Arc<Tail>>, usize) { (lru::LruCache::unbounded(), 0) }

    fn kept_bytes(c: &(lru::LruCache<(String, String), Arc<Tail>>, usize)) -> usize { c.0.iter().map(|(_, t)| t.bytes).sum() }

    #[test]
    fn two_forms_of_one_table_at_one_mark_are_both_kept() {
        let mut c = cache();
        keep(&mut c, "t", tail("keyed sys", 5, 9, 100));
        keep(&mut c, "t", tail("keyed", 5, 9, 40));
        assert_eq!(c.0.len(), 2, "one tail per form");
        assert_eq!(c.0.peek(&key("t", "keyed sys")).map(|t| t.bytes), Some(100), "found by its form");
        assert_eq!(c.0.peek(&key("t", "keyed")).map(|t| t.bytes), Some(40), "found by its form");
        assert_eq!(c.1, 140);
        assert_eq!(c.1, kept_bytes(&c));
    }

    #[test]
    fn a_newer_mark_for_one_form_drops_the_others_older_entry() {
        let mut c = cache();
        keep(&mut c, "t", tail("a", 5, 9, 100));
        keep(&mut c, "t", tail("b", 5, 9, 40));
        keep(&mut c, "u", tail("b", 1, 1, 8)); // (another table's tail is not dropped)
        keep(&mut c, "t", tail("a", 6, 7, 30));
        assert!(c.0.peek(&key("t", "b")).is_none(), "b at mark 5 is older than a's new mark 6");
        assert!(c.0.peek(&key("u", "b")).is_some(), "another table keeps its tail");
        assert_eq!(c.0.len(), 2);
        assert_eq!(c.1, 38, "the total is the kept entries' bytes");
        assert_eq!(c.1, kept_bytes(&c));
    }

    #[test]
    fn an_older_mark_is_not_kept_for_a_form_with_no_entry() {
        let mut c = cache();
        keep(&mut c, "t", tail("a", 6, 7, 100));
        keep(&mut c, "t", tail("b", 5, 9, 40));
        assert!(c.0.peek(&key("t", "b")).is_none(), "b at mark 5 is older than the table's mark 6");
        assert_eq!(c.0.len(), 1);
        assert_eq!(c.1, 100, "the total is a's bytes");
        assert_eq!(c.1, kept_bytes(&c));
    }

    #[test]
    fn an_entry_at_an_older_mark_is_not_kept() {
        let mut c = cache();
        keep(&mut c, "t", tail("a", 5, 9, 100));
        keep(&mut c, "t", tail("a", 3, 9, 50)); // an older mark
        keep(&mut c, "t", tail("a", 5, 8, 60)); // the same mark, fewer segments
        let kept = c.0.peek(&key("t", "a")).expect("kept");
        assert_eq!((kept.of.0, kept.through, kept.bytes), (5, 9, 100), "the newer entry stays");
        assert_eq!(c.1, 100);
        keep(&mut c, "t", tail("a", 5, 12, 150)); // the same mark, more segments
        assert_eq!(c.0.peek(&key("t", "a")).map(|t| t.through), Some(12), "a longer tail at the same mark replaces it");
        assert_eq!(c.1, 150);
    }

    #[test]
    fn a_tails_batches_share_the_first_kept_batchs_schema() {
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        let fresh = || Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let batch = |schema: &Arc<Schema>, values: Vec<i64>| {
            let column: ArrayRef = Arc::new(Int64Array::from(values));
            RecordBatch::try_new(schema.clone(), vec![column]).unwrap()
        };
        // Two `Arc`s of one schema, as two segments' batches may carry them.
        let (first, second) = (fresh(), fresh());
        assert!(!Arc::ptr_eq(&first, &second));
        let mut t = Tail { of: (1, "f".to_string()), through: 0, rows: vec![], ends: vec![], bytes: 0 };
        t.add(vec![(1, vec![batch(&first, vec![1, 2])])], &first).unwrap();
        t.add(vec![(2, vec![batch(&second, vec![3])])], &fresh()).unwrap();
        let kept = t.rows[0].schema();
        assert_eq!(t.rows.len(), 2);
        assert!(t.rows.iter().all(|b| Arc::ptr_eq(b.schema_ref(), &kept)), "every kept batch has the first kept batch's schema");
        assert_eq!(t.rows.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }
}
