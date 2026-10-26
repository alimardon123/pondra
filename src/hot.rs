//! Hot columns: the columns queries read lately, decoded, in memory. A scan takes a file's columns
//! from here when all it needs are here, and skips reading and decoding the Parquet file (TPC-H
//! runs about 1.5x faster hot). Files never change, so nothing here goes stale.
//!
//! Filled in the background, one file at a time, within `PONDRA_HOT_GB` (default: a quarter of the
//! query memory, an eighth of RAM; 0 turns it off), and only for a file a second scan came back to:
//! data read once — a backfill, a consumer reading the log through, a one-off report — passes through
//! without costing the CPU that decoding it would. A table whose columns are held keeps each file it
//! writes as written (`put`), so its new files and a merge's are read from memory at once. When full,
//! the cache makes room only by dropping columns nobody read for a minute: a working set bigger than
//! memory is read from Parquet as before, instead of being churned through the cache. The loads a scan
//! starts wait for its query: the files the scan's plan reads carry a gate (`gated`), and the loads start
//! when the last copy of those files goes, with the query's plan, so no query pays for the loads it
//! starts, and a concurrent scan of the same file starts no second load (`busy`).
//!
//! A lake held in memory (`memory://`) is different: its files are in memory already, so its cache
//! may take the query memory's room, within the memory line (`room_for`), and it loads a file on its
//! first read, not its second. Once the first query's loads have started and run, every file it holds
//! is hot; the first query itself reads Parquet, as before. A quarter of the query memory held the first
//! files a scan loaded and left the rest in Parquet for good (a 10M-row table of nine files kept five of
//! them hot at 0.33 GB of query memory).
//!
//! A file with a lineage (another engine's, or written without a leader) gets its rows' system
//! columns from it here, and an append table's file with rows deleted is held without them: both
//! read as `query::files_once` reads them, cold or hot.
//!
//! Each column whose order can rule a batch out (integers, dates, times, decimals) keeps every
//! batch's least and greatest value, so a scan skips the 8,192-row batches its filters can't match,
//! as Parquet skips row groups: a query's own filters, and those its joins and top-N find as they
//! run (DataFusion's dynamic filters). Rows that arrive in time order make that most of a table
//! for a filter on the time; a top-N reads the batches in its key's order, so its bound is tight
//! after the first few, either way round.
use crate::store::{DataFile, Lake, Lineage, TableMeta};
use anyhow::Result;
use datafusion::arrow::array::{new_null_array, Array, ArrayData, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchOptions, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::parquet::arrow::{arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder}, ProjectionMask};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{project_schema, stats::Precision, ColumnStatistics, ScalarValue, Statistics};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_expr::{DynamicFilterTracking, PhysicalExpr};
use datafusion::physical_optimizer::pruning::{PruningPredicateBuilder, PruningStatistics};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::{empty::EmptyExec, execution_plan::replace_children_if_necessary, union::UnionExec, ExecutionPlan};
use datafusion::physical_plan::projection::{all_alias_free_columns, new_projections_for_columns};
use datafusion::prelude::ParquetReadOptions;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use datafusion::common::instant::Instant;
use std::time::Duration;

const IDLE: Duration = Duration::from_secs(60); // unread this long: may make room for others
const BATCH: usize = 8192;

pub struct Hot {
    max: usize,
    memory: bool, // a lake held in memory (`memory://`): see the module's doc
    state: Mutex<State>,
    slot: tokio::sync::Semaphore, // one file at a time: loading competes with queries for CPU
}

#[derive(Default)]
struct State {
    cols: HashMap<(String, String), Col>, // (file, column) -> its arrays, a batch each
    rows: BTreeMap<String, Arc<Vec<usize>>>, // file -> rows per batch (the same for all its columns)
    used: usize,
    busy: HashSet<String>,           // files being loaded
    once: std::collections::VecDeque<String>, // files a scan read once (the next read loads them)
    need: HashMap<String, usize>,    // a lake in memory's files that didn't fit: the bytes they decode to, so no try decodes them again
}

struct Col {
    arrays: Arc<Vec<ArrayRef>>,
    ranges: Option<Ranges>,
    bytes: usize,
    read: Instant,
}

/// A column's least and greatest value in each batch of a file (null: none known), and each
/// batch's nulls and rows, as Parquet's statistics give them for a row group.
type Ranges = Arc<[ArrayRef; 4]>;

/// A file's hot columns as one scan takes them: each column's arrays (a batch each), each batch's rows, and the
/// ranges of the columns that have them. A batch is put together only when the scan reads it (`HotSource::open`),
/// so planning costs a few steps a file, not a step a batch.
#[derive(Clone)]
struct Held {
    columns: Vec<Arc<Vec<ArrayRef>>>,
    rows: Arc<Vec<usize>>,
    ranges: Arc<HashMap<String, Ranges>>,
}

impl Held {
    /// A file with no columns held: its rows in batches of `BATCH`, the last one shorter. A scan of no columns reads
    /// these, and no data.
    fn rows_only(rows: u64) -> Held {
        let rows = rows as usize;
        let batches: Vec<usize> = (0..rows).step_by(BATCH).map(|at| BATCH.min(rows - at)).collect();
        Held { columns: vec![], rows: Arc::new(batches), ranges: Default::default() }
    }

    /// The i-th batch of the columns `projection` names (indexes into `columns`), under `projected`, the source's
    /// projected schema, so every batch shares one schema `Arc`. Built with the batch's row count, so a batch of no columns keeps its rows.
    fn batch(&self, projected: &SchemaRef, projection: &[usize], i: usize) -> datafusion::error::Result<RecordBatch> {
        let columns = projection.iter().map(|&c| self.columns[c][i].clone()).collect();
        Ok(RecordBatch::try_new_with_options(projected.clone(), columns, &RecordBatchOptions::new().with_row_count(Some(self.rows[i])))?)
    }
}

impl Hot {
    /// The cache of a lake: `memory` for a lake held in memory. Its budget is the query memory (`room_for` takes what
    /// queries don't reserve, within the memory line), not a quarter of it: see the module's doc.
    pub fn new(memory: bool) -> Hot {
        let gb = crate::env::parse::<f64>("PONDRA_HOT_GB");
        let max = gb.map(|g| (g * (1u64 << 30) as f64) as usize).unwrap_or_else(|| match memory {
            true => crate::store::memory_limit(),
            false => crate::store::memory_limit() / 4, // (a quarter of the query memory: an eighth of RAM)
        });
        Hot { max, memory, state: Default::default(), slot: tokio::sync::Semaphore::new(1) }
    }

    pub fn on(&self) -> bool { self.max > 0 }

    /// Bytes held, the most it may hold, and files being loaded (or waiting to be).
    pub fn usage(&self) -> (usize, usize, usize) {
        let s = self.state.lock().unwrap();
        (s.used, self.max, s.busy.len())
    }

    /// Drop the least recently read columns until `bytes` are freed, and say how many were (the one
    /// watch, `oom::start`, asks for it when the node's memory passes its trim line).
    pub(crate) fn trim(&self, bytes: usize) -> usize {
        let mut s = self.state.lock().unwrap();
        let mut oldest: Vec<(Instant, (String, String))> = s.cols.iter().map(|(k, c)| (c.read, k.clone())).collect();
        oldest.sort();
        let mut freed = 0;
        for (_, k) in oldest {
            if freed >= bytes {
                break;
            }
            if let Some(c) = s.cols.remove(&k) {
                (freed, s.used) = (freed + c.bytes, s.used.saturating_sub(c.bytes));
                if !s.cols.keys().any(|(file, _)| *file == k.0) {
                    s.rows.remove(&k.0);
                }
            }
        }
        freed
    }

    /// Let go of the files a table's commit replaced (`TableMeta::garbage`): no query plans them
    /// again, and one that already has holds its own batches. Kept until the budget needed room, a
    /// busy table's merges filled it with files nobody could read (a soak's nodes held 537 MB of
    /// columns after 25 minutes, of a table of a few tens).
    pub fn forget(&self, replaced: &[(String, u64)]) {
        if !self.on() || replaced.is_empty() {
            return;
        }
        let mut s = self.state.lock().unwrap();
        // (held as its path, or as `path#…` without its deleted rows: `key`)
        let held = |rows: &BTreeMap<String, Arc<Vec<usize>>>, p: &str| rows.range(p.to_string()..).next().is_some_and(|(k, _)| base(k) == p);
        let gone: HashSet<&str> = replaced.iter().map(|(p, _)| p.as_str()).filter(|p| held(&s.rows, p)).collect();
        if gone.is_empty() {
            return; // (most commits: what they replaced went at an earlier one)
        }
        let mut freed = 0;
        s.cols.retain(|(k, _), c| {
            let keep = !gone.contains(base(k));
            freed += if keep { 0 } else { c.bytes };
            keep
        });
        s.rows.retain(|k, _| !gone.contains(base(k)));
        s.need.retain(|k, _| !gone.contains(base(k)));
        s.used = s.used.saturating_sub(freed);
    }

    /// `file`'s columns of `schema`, if all of them are here, as a `Held`. None when a column's arrays aren't of the
    /// field's type, or a field that can't be null holds one: the checks a batch's build made, made once here (all of
    /// a column's arrays share its type).
    fn get(&self, file: &str, schema: &SchemaRef) -> Option<Held> {
        let mut s = self.state.lock().unwrap();
        let rows = s.rows.get(file)?.clone();
        let now = Instant::now();
        let (mut columns, mut ranges) = (vec![], HashMap::new());
        for f in schema.fields() {
            let c = s.cols.get_mut(&(file.to_string(), f.name().clone()))?;
            if c.arrays.iter().any(|a| a.data_type() != f.data_type() || (!f.is_nullable() && a.null_count() > 0)) {
                return None;
            }
            c.read = now;
            columns.push(c.arrays.clone());
            if let Some(r) = &c.ranges {
                ranges.insert(f.name().clone(), r.clone());
            }
        }
        Some(Held { columns, rows, ranges: Arc::new(ranges) })
    }

    /// Decode `file`'s columns among `fields` that aren't here yet, in the background, if they
    /// can fit (`deletes`: without its deleted rows). The decode waits for `gate` to open: the scan's
    /// query is over then (`gated`). Returns whether a load was started.
    fn load(self: &Arc<Self>, lake: Arc<Lake>, file: DataFile, fields: Vec<FieldRef>, deletes: bool, gate: LoadGate) -> bool {
        let guess = (file.bytes as usize).max(1 << 20) * 3; // decoded LZ4 Parquet: roughly 3x (checked after)
        // Queries come first: what they hold now is off the cache's budget, so a node under load
        // keeps its memory for them (both are bounded by `--memory-gb`).
        let room = self.room_for(&lake);
        let key = key(&file, deletes);
        let memory = self.memory;
        let fields: Vec<FieldRef> = {
            let mut s = self.state.lock().unwrap();
            let missing: Vec<FieldRef> = fields.into_iter().filter(|f| !s.cols.contains_key(&(key.clone(), f.name().clone()))).collect();
            // (a lake in memory guesses nothing: its file is decoded and fits or not, and a file that didn't fit
            // waits for the room its decoded size needs, not a guess of it: `need`)
            let need = match memory {
                true => s.need.get(&key).copied().unwrap_or(0),
                false => guess,
            };
            if missing.is_empty() || s.busy.contains(&key) || !(memory || s.twice(&key)) || !s.room(need, room) {
                return false;
            }
            s.busy.insert(key.clone());
            missing
        };
        let hot = self.clone();
        crate::panics::spawn(async move {
            let _ = gate.await; // (the scan's query is over: its plan was dropped)
            let _slot = hot.slot.acquire().await;
            let decoded = decode(&lake, &file, &fields, deletes).await;
            let mut s = hot.state.lock().unwrap();
            s.busy.remove(&key);
            let Ok((rows, arrays, ranges)) = decoded else { return };
            if s.rows.get(&key).is_some_and(|r| **r != rows) {
                return; // (batches cut differently: can't be combined)
            }
            let sizes: Vec<usize> = arrays.iter().map(|a| size(a)).collect();
            let bytes: usize = sizes.iter().sum();
            if !s.room(bytes, room) {
                if memory {
                    s.need.insert(key.clone(), bytes); // (the next scan waits for this much room before it decodes the file again)
                }
                return;
            }
            if memory {
                s.need.remove(&key);
            }
            s.rows.insert(key.clone(), Arc::new(rows));
            for (((f, arrays), ranges), bytes) in fields.iter().zip(arrays).zip(ranges).zip(sizes) {
                s.used += bytes;
                s.cols.insert((key.clone(), f.name().clone()), Col { arrays: Arc::new(arrays), ranges, bytes, read: Instant::now() });
            }
            drop(s);
            crate::oom::wake(); // (the columns are in: the watch may need to give some back)
        });
        true
    }

    /// The most the cache may hold now: its budget, less what queries hold; and past the node's trim line
    /// (`oom::trim_line`), no more than the room the node has under it. So the cache loads only while the
    /// node is under the line, rather than loading what the watch would trim and decoding it again.
    /// Called outside the state's lock (it takes the lock itself).
    pub fn room_for(&self, lake: &Lake) -> usize {
        let (reserved, limit) = lake.memory();
        let room = self.max.min(limit.saturating_sub(reserved));
        match crate::oom::trim_line() {
            Some(trim) => room.min(self.usage().0 + trim.saturating_sub(crate::store::resident().unwrap_or(0))),
            None => room,
        }
    }

    /// The columns of `schema` (by name) that the table's files under `folder` are held with here,
    /// each with the type it is held as: what a new file of the table keeps (`tier::Encoding`).
    /// Empty when hot is off or none of the table is held.
    pub fn columns(&self, folder: &str, schema: &Schema) -> Vec<FieldRef> {
        if !self.on() {
            return vec![];
        }
        let s = self.state.lock().unwrap();
        let prefix = format!("data/{folder}/"); // (the slash: table `t` doesn't take `t2`'s files)
        let files: Vec<&String> = s.rows.range(prefix.clone()..).take_while(|(k, _)| k.starts_with(&prefix)).map(|(k, _)| k).collect();
        schema
            .fields()
            .iter()
            .filter_map(|f| {
                let held = files.iter().find_map(|file| s.cols.get(&(file.to_string(), f.name().clone()))?.arrays.first().map(|a| a.data_type().clone()))?;
                Some(Arc::new(Field::new(f.name(), held, true)))
            })
            .collect()
    }

    /// Holds the columns a write kept of the file it wrote at `path`: `columns`' fields, each with its
    /// arrays in the order written. They are held as `decode` would hold them, so a scan reads the
    /// file from here at once, and a merge's file takes the replaced files' place (`forget`), rather
    /// than each new file being read cold twice first.
    pub fn put(&self, path: &str, fields: &[FieldRef], columns: Vec<Vec<ArrayRef>>, room: usize) {
        if !self.on() || fields.is_empty() {
            return;
        }
        // The Parquet reader cuts a file into batches of `BATCH` rows, whatever its writes were cut at:
        // a later `load` of more of its columns must find the same batches, or it gives up.
        let Some(cut) = columns.iter().map(|arrays| cut_batches(arrays)).collect::<Option<Vec<_>>>() else { return };
        let lens = |c: &[ArrayRef]| c.iter().map(|a| a.len()).collect::<Vec<usize>>();
        let rows = lens(&cut[0]);
        if cut.iter().any(|c| lens(c) != rows) {
            return;
        }
        let held: Vec<(Vec<ArrayRef>, Option<Ranges>, usize)> = cut
            .into_iter()
            .map(|c| {
                let ranges = ranges(&c);
                let arrays = whole(c);
                let bytes = size(&arrays);
                (arrays, ranges, bytes)
            })
            .collect();
        let total: usize = held.iter().map(|h| h.2).sum();
        let mut s = self.state.lock().unwrap();
        if s.rows.contains_key(path) || !s.room(total, room) {
            return;
        }
        s.rows.insert(path.to_string(), Arc::new(rows));
        for (f, (arrays, ranges, bytes)) in fields.iter().zip(held) {
            s.used += bytes;
            s.cols.insert((path.to_string(), f.name().clone()), Col { arrays: Arc::new(arrays), ranges, bytes, read: Instant::now() });
        }
        drop(s);
        crate::oom::wake(); // (the columns are in: the watch may need to give some back)
    }
}

/// A column's arrays as batches of `BATCH` rows, the last one shorter: slices of one concatenated array.
fn cut_batches(arrays: &[ArrayRef]) -> Option<Vec<ArrayRef>> {
    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
    let all = datafusion::arrow::compute::concat(&refs).ok()?;
    let n = all.len();
    Some((0..n).step_by(BATCH).map(|at| all.slice(at, BATCH.min(n - at))).collect())
}

impl State {
    /// Has a scan read this file before? (The first read only notes it: data read once isn't worth
    /// decoding twice.)
    fn twice(&mut self, path: &str) -> bool {
        if self.rows.contains_key(path) || self.once.contains(&path.to_string()) {
            return true;
        }
        self.once.push_back(path.to_string());
        if self.once.len() > 1 << 16 {
            self.once.pop_front();
        }
        false
    }

    /// Is there room for `bytes` more, after dropping columns unread for a minute (oldest first)?
    fn room(&mut self, bytes: usize, max: usize) -> bool {
        if self.used + bytes <= max {
            return true;
        }
        let mut idle: Vec<_> = self.cols.iter().filter(|(_, c)| c.read.elapsed() > IDLE).map(|(k, c)| (c.read, k.clone())).collect();
        idle.sort();
        for (_, k) in idle {
            if self.used + bytes <= max {
                break;
            }
            let c = self.cols.remove(&k).expect("listed");
            self.used -= c.bytes;
            if !self.cols.keys().any(|(file, _)| *file == k.0) {
                self.rows.remove(&k.0);
            }
        }
        self.used + bytes <= max
    }
}

/// Memory held by a column's arrays: each buffer once (string views of one page share its buffer).
fn size(arrays: &[ArrayRef]) -> usize {
    fn add(d: &ArrayData, seen: &mut HashSet<usize>) -> usize {
        let own: usize = d.buffers().iter().chain(d.nulls().map(|n| n.buffer())).filter(|b| seen.insert(b.data_ptr().as_ptr() as usize)).map(|b| b.capacity()).sum();
        own + d.child_data().iter().map(|c| add(c, seen)).sum::<usize>()
    }
    let mut seen = HashSet::new();
    arrays.iter().map(|a| add(&a.to_data(), &mut seen)).sum()
}

/// A file's name here: its path, and for a file read without its deleted rows, which deletes it
/// has (more rows deleted since: another entry, decoded again).
fn key(f: &DataFile, deletes: bool) -> String {
    if !deletes || f.deletes.is_empty() {
        return f.path.clone();
    }
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    format!("{:?}", f.deletes).hash(&mut h);
    format!("{}#{:x}", f.path, h.finish())
}

/// A name here's file: `key` without its deletes.
fn base(key: &str) -> &str { key.rsplit_once('#').map_or(key, |(path, _)| path) }

/// `fields` of `file`, cast to their types (a column the file lacks, added to the table after it
/// was written, is null): rows per batch, and each field's arrays and their ranges. Its system
/// columns come from its lineage if it has one; with `deletes`, its deleted rows are left out.
async fn decode(lake: &Lake, file: &DataFile, fields: &[FieldRef], deletes: bool) -> Result<(Vec<usize>, Vec<Vec<ArrayRef>>, Vec<Option<Ranges>>)> {
    let bytes = lake.object(&file.path).await?;
    let gone = if deletes && !file.deletes.is_empty() { crate::lake_scan::deleted_rows(lake, file).await? } else { vec![] };
    let (fields, lineage) = (fields.to_vec(), file.lineage);
    tokio::task::spawn_blocking(move || {
        // Strings straight to views (no copy of their bytes), as the table is read (see `read_schema`).
        let file = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?.schema().clone();
        let views = file.fields().iter().map(|f| match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Arc::new(f.as_ref().clone().with_data_type(DataType::Utf8View)),
            _ => f.clone(),
        });
        let hint = Arc::new(Schema::new_with_metadata(views.collect::<Vec<_>>(), file.metadata().clone()));
        let b = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, ArrowReaderOptions::new().with_schema(hint))?;
        let present: Vec<usize> = fields.iter().filter_map(|f| b.schema().index_of(f.name()).ok()).collect();
        let mask = ProjectionMask::roots(b.parquet_schema(), present);
        let (mut rows, mut out, mut at, mut next) = (vec![], vec![vec![]; fields.len()], 0, 0); // (at: the batch's first row's place in the file)
        for batch in b.with_projection(mask).with_batch_size(BATCH).build()? {
            let batch = batch?;
            let n = batch.num_rows();
            let keep = (!gone.is_empty()).then(|| {
                BooleanArray::from_iter((at..at + n as u64).map(|i| {
                    while next < gone.len() && gone[next] < i {
                        next += 1;
                    }
                    Some(gone.get(next) != Some(&i))
                }))
            });
            for (f, arrays) in fields.iter().zip(&mut out) {
                let c = datafusion::arrow::compute::cast(&column(&batch, f, lineage, at), f.data_type())?;
                arrays.push(match &keep {
                    Some(k) => datafusion::arrow::compute::filter(&c, k)?,
                    None => c,
                });
            }
            rows.push(keep.as_ref().map_or(n, |k| k.true_count()));
            at += n as u64;
        }
        let ranges = out.iter().map(|arrays| ranges(arrays)).collect();
        let out = out.into_iter().map(whole).collect();
        Ok((rows, out, ranges))
    })
    .await?
}

/// A file's batches of one column copied into one allocation of their own (invariant 212), each buffer a piece of
/// it that says its own size. Kept in the decoder's buffers, the columns sat among its short-lived
/// ones in the allocator's pages, which a column alone then kept from being given back: the process
/// held about twice what the columns count. A buffer several batches share (the Parquet page their
/// strings point into, a dictionary's values) is copied once and stays shared: copying each
/// batch's strings out instead made joins on a low-cardinality string column carry and count a
/// copy per batch (TPC-H q12's build 3.4 times its memory, and slower). Slices of one array would
/// do the first, but each batch would then count the whole file's buffer as its own, and the
/// operators that budget their memory by what their batches hold would think them a file's size.
fn whole(arrays: Vec<ArrayRef>) -> Vec<ArrayRef> {
    use datafusion::arrow::array::make_array;
    use datafusion::arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
    const ALIGN: usize = 64;
    let datas: Vec<ArrayData> = arrays.iter().map(|a| a.to_data()).collect();
    fn each(d: &ArrayData, f: &mut dyn FnMut(&Buffer)) {
        d.buffers().iter().for_each(&mut *f);
        if let Some(n) = d.nulls() {
            f(n.buffer());
        }
        d.child_data().iter().for_each(|c| each(c, f));
    }
    let key = |b: &Buffer| (b.as_ptr() as usize, b.len());
    let (mut at, mut distinct, mut total) = (HashMap::new(), vec![], 0);
    datas.iter().for_each(|d| {
        each(d, &mut |b| {
            at.entry(key(b)).or_insert_with(|| {
                distinct.push(b.clone());
                total += b.len().next_multiple_of(ALIGN);
                total - b.len().next_multiple_of(ALIGN)
            });
        })
    });
    let mut arena = MutableBuffer::with_capacity(total);
    for b in &distinct {
        arena.extend_from_slice(b.as_slice());
        arena.resize(arena.len().next_multiple_of(ALIGN), 0);
    }
    let arena = Arc::new(Buffer::from(arena));
    let pieces: HashMap<(usize, usize), Buffer> = at
        .into_iter()
        .map(|((ptr, len), offset)| {
            let owner: Arc<dyn datafusion::arrow::alloc::Allocation> = arena.clone();
            // SAFETY: `offset..offset + len` is inside the arena (laid out above), which `owner` keeps alive.
            let p = unsafe { Buffer::from_custom_allocation(std::ptr::NonNull::new_unchecked(arena.as_ptr().add(offset) as *mut u8), len, owner) };
            ((ptr, len), p)
        })
        .collect();
    let piece = |b: &Buffer| pieces[&key(b)].clone();
    fn rebuild(d: &ArrayData, piece: &dyn Fn(&Buffer) -> Buffer) -> ArrayData {
        let buffers = d.buffers().iter().map(piece).collect();
        // SAFETY: the same bits, so the same count.
        let nulls = d.nulls().map(|n| unsafe { NullBuffer::new_unchecked(BooleanBuffer::new(piece(n.buffer()), n.offset(), n.len()), n.null_count()) });
        let children = d.child_data().iter().map(|c| rebuild(c, piece)).collect();
        // SAFETY: the same array, its buffers' bytes copied as they were (offsets, lengths and nulls kept).
        unsafe { d.clone().into_builder().buffers(buffers).nulls(nulls).child_data(children).build_unchecked() }
    }
    datas.iter().map(|d| make_array(rebuild(d, &piece))).collect()
}

/// Each batch's least and greatest value, for a column whose order can rule a batch out: not
/// floats (a NaN sorts apart, invariant 65) or strings (rarely in order, and costly to keep).
fn ranges(arrays: &[ArrayRef]) -> Option<Ranges> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::compute::{max, min};
    use datafusion::arrow::datatypes::*;
    use datafusion::common::ScalarValue;
    fn ends<T: ArrowPrimitiveType>(a: &ArrayRef) -> Option<(ScalarValue, ScalarValue)>
    where
        T::Native: datafusion::arrow::datatypes::ArrowNativeTypeOp,
    {
        let p = a.as_primitive::<T>();
        Some((ScalarValue::new_primitive::<T>(min(p), a.data_type()).ok()?, ScalarValue::new_primitive::<T>(max(p), a.data_type()).ok()?))
    }
    let one = |a: &ArrayRef| match a.data_type() {
        DataType::Int8 => ends::<Int8Type>(a),
        DataType::Int16 => ends::<Int16Type>(a),
        DataType::Int32 => ends::<Int32Type>(a),
        DataType::Int64 => ends::<Int64Type>(a),
        DataType::UInt8 => ends::<UInt8Type>(a),
        DataType::UInt16 => ends::<UInt16Type>(a),
        DataType::UInt32 => ends::<UInt32Type>(a),
        DataType::UInt64 => ends::<UInt64Type>(a),
        DataType::Date32 => ends::<Date32Type>(a),
        DataType::Date64 => ends::<Date64Type>(a),
        DataType::Timestamp(TimeUnit::Second, _) => ends::<TimestampSecondType>(a),
        DataType::Timestamp(TimeUnit::Millisecond, _) => ends::<TimestampMillisecondType>(a),
        DataType::Timestamp(TimeUnit::Microsecond, _) => ends::<TimestampMicrosecondType>(a),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => ends::<TimestampNanosecondType>(a),
        DataType::Time32(TimeUnit::Second) => ends::<Time32SecondType>(a),
        DataType::Time32(TimeUnit::Millisecond) => ends::<Time32MillisecondType>(a),
        DataType::Time64(TimeUnit::Microsecond) => ends::<Time64MicrosecondType>(a),
        DataType::Time64(TimeUnit::Nanosecond) => ends::<Time64NanosecondType>(a),
        DataType::Decimal128(..) => ends::<Decimal128Type>(a),
        _ => None,
    };
    let (lo, hi): (Vec<_>, Vec<_>) = arrays.iter().map(one).collect::<Option<Vec<_>>>()?.into_iter().unzip();
    let count = |n: fn(&ArrayRef) -> usize| Arc::new(datafusion::arrow::array::UInt64Array::from_iter_values(arrays.iter().map(|a| n(a) as u64))) as ArrayRef;
    Some(Arc::new([ScalarValue::iter_to_array(lo).ok()?, ScalarValue::iter_to_array(hi).ok()?, count(|a| a.null_count()), count(|a| a.len())]))
}

/// A field of `batch`, whose first row is the file's `at`-th: a system column from the file's
/// lineage when it has one (as `lake_scan::adopted` gives it), else the file's column, or nulls.
fn column(batch: &RecordBatch, f: &FieldRef, lineage: Option<Lineage>, at: u64) -> ArrayRef {
    let n = batch.num_rows();
    match (lineage, f.name().as_str()) {
        (Some(l), crate::text::ROW_ID) => Arc::new(Int64Array::from_iter_values((0..n as i64).map(|i| l.first + at as i64 + i))),
        (Some(l), crate::text::VERSION) => Arc::new(Int64Array::from_value(l.version as i64, n)),
        (Some(l), crate::text::CREATED | crate::text::UPDATED) => Arc::new(TimestampMicrosecondArray::from_value(l.ms as i64 * 1000, n).with_timezone("UTC")),
        _ => batch.column_by_name(f.name()).cloned().unwrap_or_else(|| new_null_array(f.data_type(), n)),
    }
}

/// A table's Parquet files, read through the hot columns: files whose columns a scan needs are
/// all in memory come from there, the others from Parquet (with the scan's filters, to skip row
/// groups; a file with a lineage or deleted rows as `query::files_once` reads it), and are loaded
/// for next time.
pub struct HotFiles {
    pub lake: Arc<Lake>,
    pub files: Vec<DataFile>,
    pub schema: SchemaRef,
    pub meta: TableMeta, // (what reading a file takes of its table: its columns, their names, the key)
    pub known: Arc<HashMap<String, Known>>, // (what the planner knows of the table's columns: `Known`)
}

impl std::fmt::Debug for HotFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "HotFiles({} files)", self.files.len()) }
}

#[async_trait::async_trait]
impl TableProvider for HotFiles {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()]) // (they skip row groups; rows are filtered above)
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(match projection {
            Some(p) => self.schema.project(p)?,
            None => self.schema.as_ref().clone(),
        });
        let deletes = self.meta.key.is_empty(); // (a keyed table's positions are older versions its own reads pass over)
        let (mut held, mut cold) = (vec![], vec![]);
        for f in &self.files {
            // A scan of no columns reads no data: a plain file is its rows, from the catalog (no lookup, no load).
            if schema.fields().is_empty() && plain_file(f, deletes) {
                held.push(Held::rows_only(f.rows));
                continue;
            }
            match self.lake.hot.get(&key(f, deletes), &schema) {
                Some(h) => held.push(h),
                None => cold.push(f),
            }
        }
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
        let (plain, special): (Vec<&DataFile>, Vec<&DataFile>) = cold.iter().copied().partition(|f| plain_file(f, deletes));
        // The loads this scan starts wait on this sender: the files of its plan carry it (`gated`), so they start when the
        // last copy of those files goes, with the query's plan. Here the scan lets go of its own copy.
        let (go, gate) = tokio::sync::oneshot::channel::<()>();
        let gate: LoadGate = futures::FutureExt::shared(gate);
        let holder = Arc::new(Gate(go));
        if !plain.is_empty() {
            let urls = plain.iter().map(|f| ListingTableUrl::parse(self.lake.full(&f.path))).collect::<Result<Vec<_>, _>>()?;
            let options = ParquetReadOptions::default().to_listing_options(state.config(), state.default_table_options());
            let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options).with_schema(self.schema.clone());
            let table = ListingTable::try_new(config)?.with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
            plans.push(gated(told(table.scan(state, projection, filters, limit).await?, &self.known)?, &holder)?);
        }
        if !special.is_empty() {
            let state = state.as_any().downcast_ref::<datafusion::execution::SessionState>().ok_or_else(|| datafusion::error::DataFusionError::Internal("a session without its state".into()))?;
            let ctx = datafusion::prelude::SessionContext::new_with_state(state.clone());
            let df = crate::query::files_once(&self.lake, &ctx, &special, &self.meta, &self.schema).await.map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            plans.push(gated(df.select_columns(&names)?.create_physical_plan().await?, &holder)?);
        }
        drop(holder); // (its copies are in the plan's files: the loads wait for them; with no file to carry it, they start now)
        for f in cold {
            self.lake.hot.load(self.lake.clone(), f.clone(), schema.fields().to_vec(), deletes, gate.clone()); // (in the background: the next read has the file)
        }
        if !held.is_empty() {
            let source = HotSource::new(schema.clone(), held, state.config().target_partitions().max(1), self.known.clone());
            if !source.parts.is_empty() {
                plans.push(DataSourceExec::from_data_source(source));
            }
        }
        let plan: Arc<dyn ExecutionPlan> = match plans.len() {
            0 => Arc::new(EmptyExec::new(schema)),
            _ => UnionExec::try_new(plans)?,
        };
        Ok(plan)
    }
}

/// A scan's loads wait on this: a shared receiver, which opens (with `Ok`, or `Err` once the sender is gone) when the
/// last copy of the sender goes, with the scan's files (`gated`).
type LoadGate = futures::future::Shared<tokio::sync::oneshot::Receiver<()>>;

/// The sender a scan's loads wait for. Each file of the scan's plan holds an `Arc` of it (`gated`), so it goes when the
/// last of those files goes: with the query's plan, after its streams have ended, or when an EXPLAIN or a failed query
/// drops the plan.
struct Gate(#[allow(dead_code)] tokio::sync::oneshot::Sender<()>); // (held, never read: its drop is the signal)

/// A file a scan reads as Parquet: no lineage, and no deleted rows to drop (`deletes`: the table's rows are read
/// without them, as a keyed table's are not).
fn plain_file(f: &DataFile, deletes: bool) -> bool { f.lineage.is_none() && (f.deletes.is_empty() || !deletes) }

/// Each file's batches dealt to `n` partitions in turn: the i-th batch, in the order the files hold them, goes to
/// partition `i % n`. Each batch is a (file, batch) pair; an empty partition is left out.
fn deal(files: &[Held], n: usize) -> Vec<Vec<(u32, u32)>> {
    let mut parts = vec![vec![]; n];
    let batches = files.iter().enumerate().flat_map(|(f, h)| (0..h.rows.len()).map(move |b| (f as u32, b as u32)));
    for (i, batch) in batches.enumerate() {
        parts[i % n].push(batch);
    }
    parts.retain(|p| !p.is_empty());
    parts
}

/// What the planner knows of a column of the batches: the table's least and greatest value and
/// how many values it holds, as the table gives them to the join order (`query::count`:
/// its files' footers and its sketch), so a filter or a join over the batches is sized alike there
/// and here. Without them DataFusion took a range of dates for every row, and a nation's name for
/// a fifth of the nations. They are the catalog's, the same on every node whatever it holds in
/// memory.
#[derive(Debug, Clone, Default)]
pub struct Known {
    range: Option<(ScalarValue, ScalarValue)>,
    distinct: Option<usize>,
}

/// A table's statistics as `Known`, by column name. Ranges only of numbers, dates and times: the
/// estimates measure no other (a string's least and greatest value sized nothing, and each plan
/// copied them at every step it asked for statistics).
pub fn known(stats: &datafusion::common::Statistics, schema: &Schema) -> HashMap<String, Known> {
    let measured = |t: &DataType| t.is_numeric() || matches!(t, DataType::Date32 | DataType::Date64 | DataType::Timestamp(..));
    let column = |f: &FieldRef, c: &datafusion::common::ColumnStatistics| Known {
        range: c.min_value.get_value().cloned().zip(c.max_value.get_value().cloned()).filter(|(lo, hi)| measured(f.data_type()) && !lo.is_null() && !hi.is_null()),
        distinct: c.distinct_count.get_value().copied(),
    };
    schema.fields().iter().zip(&stats.column_statistics).map(|(f, c)| (f.name().clone(), column(f, c))).filter(|(_, k)| k.range.is_some() || k.distinct.is_some()).collect()
}

/// A table's Parquet files told what the planner knows of its columns' distinct values, as the hot
/// batches are told. The footers hold no distinct counts, so a GROUP BY over a table read cold was sized
/// at every input row, and a join built the big side instead of the groups.
fn told(plan: Arc<dyn ExecutionPlan>, known: &HashMap<String, Known>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let Some(scan) = plan.downcast_ref::<DataSourceExec>() else { return Ok(plan) };
    let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else { return Ok(plan) };
    if known.values().all(|k| k.distinct.is_none()) {
        return Ok(plan);
    }
    // (the statistics are the whole table's columns, unprojected: the file groups' and the config's alike)
    let names: Vec<String> = config.file_source.table_schema().table_schema().fields().iter().map(|f| f.name().clone()).collect();
    let fill = |stats: &Statistics| {
        let mut s = stats.clone();
        let rows = s.num_rows.get_value().copied();
        for (c, name) in s.column_statistics.iter_mut().zip(&names) {
            let Some(Known { distinct: Some(d), .. }) = known.get(name) else { continue };
            let Some(rows) = rows else { continue };
            if c.distinct_count == Precision::Absent {
                c.distinct_count = Precision::Inexact((*d).min(rows).max(1));
            }
        }
        s
    };
    let groups: Vec<FileGroup> = config.file_groups.iter().map(|g| match g.file_statistics(None) {
        Some(s) => g.clone().with_statistics(Arc::new(fill(s))),
        None => g.clone(),
    }).collect();
    let built = FileScanConfigBuilder::from(config.clone()).with_file_groups(groups).with_statistics(fill(&config.statistics())).build();
    Ok(DataSourceExec::from_data_source(built))
}

/// A scan's plan with each file it reads holding the scan's gate (`Gate`), as a typed extension of the file: the loads the
/// scan starts wait until the last copy of those files goes, with the query's plan. The files are the same files in the same
/// groups, and no node is added, so the plan's shape and its EXPLAIN text are the same (an optimizer rule still sees the
/// scan it saw). DataFusion copies a file with its extensions when it splits or repartitions the groups, so a copy in
/// the optimized plan holds the gate too; a plan with no file reading Parquet holds none, and nothing waits for it.
fn gated(plan: Arc<dyn ExecutionPlan>, gate: &Arc<Gate>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let plan = plan.transform_up(|node| {
        let Some(scan) = node.downcast_ref::<DataSourceExec>() else { return Ok(Transformed::no(node)) };
        let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else { return Ok(Transformed::no(node)) };
        let groups: Vec<FileGroup> = config.file_groups.iter().map(|g| {
            let files = g.iter().map(|f| f.clone().with_extension(gate.clone())).collect::<Vec<_>>();
            let group = FileGroup::new(files);
            match g.file_statistics(None) {
                Some(s) => group.with_statistics(Arc::new(s.clone())),
                None => group,
            }
        }).collect();
        let built = FileScanConfigBuilder::from(config.clone()).with_file_groups(groups).build();
        Ok(Transformed::yes(DataSourceExec::from_data_source(built)))
    })?;
    Ok(plan.data)
}

/// The hot batches of a scan, as DataFusion's memory source reads them (the same partitions in the same order, and
/// the same EXPLAIN text), skipping each batch the scan's filters rule out by its ranges. The filters stay above (rows
/// are filtered there): here they only skip batches, as a Parquet scan skips row groups. A dynamic filter (a join's
/// keys, a top-N's bound) is looked at again as it moves. A batch is put together only when it is read.
#[derive(Clone)]
struct HotSource {
    schema: SchemaRef,              // the scan's columns, before a projection pushed into it
    projection: Option<Vec<usize>>, // (indexes into `schema`)
    projected_schema: SchemaRef,    // `schema` projected: what each batch is
    files: Arc<Vec<Held>>,
    parts: Arc<Vec<Vec<(u32, u32)>>>, // each partition's batches, as (file, batch), in the order it reads them
    fetch: Option<usize>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Each partition's statistics and then all of them, as the memory source works them out, kept
    /// once worked out: it adds up every column of every batch, and planning asks again at every
    /// step (a third of planning TPC-H q8 from memory). They don't change with the fetch or the
    /// predicate, only with the batches and the columns.
    stats: Arc<[std::sync::OnceLock<Arc<Statistics>>]>,
    /// What the planner knows of each column (`Known`), by name.
    known: Arc<HashMap<String, Known>>,
}

impl HotSource {
    /// A source reading `schema`'s columns of `files`, their batches dealt to `n` partitions (`deal`).
    fn new(schema: SchemaRef, files: Vec<Held>, n: usize, known: Arc<HashMap<String, Known>>) -> HotSource {
        let parts = deal(&files, n);
        HotSource {
            projected_schema: schema.clone(),
            schema,
            projection: None,
            stats: stats_for(parts.len()),
            parts: Arc::new(parts),
            files: Arc::new(files),
            fetch: None,
            predicate: None,
            known,
        }
    }

    /// The scan's columns the source reads: its projection, or all of them.
    fn projection_indices(&self) -> Vec<usize> {
        self.projection.clone().unwrap_or_else(|| (0..self.schema.fields().len()).collect())
    }
}

impl std::fmt::Debug for HotSource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "HotSource({} partitions)", self.parts.len()) }
}

/// The statistics slots of a source: one per partition, and one for all of them.
fn stats_for(partitions: usize) -> Arc<[std::sync::OnceLock<Arc<Statistics>>]> { (0..=partitions).map(|_| std::sync::OnceLock::new()).collect() }

/// The rows, bytes and each projected column's nulls of the batches `list` names, as `compute_record_batch_statistics`
/// adds them up, worked out from the held arrays (no batch is built).
fn statistics_of(files: &[Held], list: impl Iterator<Item = (u32, u32)>, projection: &[usize]) -> Statistics {
    let (mut rows, mut bytes, mut nulls) = (0, 0, vec![0; projection.len()]);
    for (file, batch) in list {
        let (held, batch) = (&files[file as usize], batch as usize);
        rows += held.rows[batch];
        for (n, &c) in nulls.iter_mut().zip(projection) {
            let array = &held.columns[c][batch];
            bytes += array.get_array_memory_size();
            *n += array.logical_nulls().map_or(0, |nulls| nulls.null_count());
        }
    }
    Statistics {
        num_rows: Precision::Exact(rows),
        total_byte_size: Precision::Exact(bytes),
        column_statistics: nulls
            .into_iter()
            .map(|null_count| ColumnStatistics { null_count: Precision::Exact(null_count), ..ColumnStatistics::new_unknown() })
            .collect(),
    }
}

impl DataSource for HotSource {
    fn open(&self, partition: usize, _context: Arc<datafusion::execution::TaskContext>) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let list = self.parts[partition].clone();
        let projection = self.projection_indices();
        let projected = self.projected_schema.clone();
        let files = self.files.clone();
        // (a fetch counts the rows it reads, so none is skipped then)
        let mut skip = match (&self.predicate, self.fetch) {
            (Some(predicate), None) => Some(Skip {
                predicate: predicate.clone(),
                tracking: DynamicFilterTracking::classify(predicate),
                schema: self.schema.clone(),
                files: files.clone(),
                list: list.clone(),
                keep: None,
                next: 0,
                gap: 1,
                stats: Default::default(),
            }),
            _ => None,
        };
        let (mut left, mut at) = (self.fetch, 0); // (the rows a fetch has left, and the next batch of the list)
        let batches = std::iter::from_fn(move || loop {
            if left == Some(0) {
                return None;
            }
            let i = at;
            at += 1;
            let &(file, batch) = list.get(i)?;
            if skip.as_mut().is_some_and(|s| !s.keep(i)) {
                continue;
            }
            let batch = match files[file as usize].batch(&projected, &projection, batch as usize) {
                Ok(batch) => batch,
                Err(e) => return Some(Err(e)),
            };
            let rows = batch.num_rows();
            let batch = match left {
                Some(n) if rows > n => batch.slice(0, n), // (the fetch's last batch, cut)
                _ => batch,
            };
            left = left.map(|n| n.saturating_sub(rows));
            return Some(Ok(batch));
        });
        let stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(self.projected_schema.clone(), futures::stream::iter(batches));
        Ok(Box::pin(datafusion::physical_plan::coop::cooperative(stream)))
    }
    fn fmt_as(&self, t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // (the text DataFusion's memory source writes in each arm, so EXPLAIN reads as before)
        match t {
            datafusion::physical_plan::DisplayFormatType::Default | datafusion::physical_plan::DisplayFormatType::Verbose => {
                let sizes: Vec<usize> = self.parts.iter().map(|p| p.len()).collect();
                let limit = self.fetch.map_or(String::new(), |n| format!(", fetch={n}"));
                write!(f, "partitions={}, partition_sizes={sizes:?}{limit}", sizes.len())
            }
            datafusion::physical_plan::DisplayFormatType::TreeRender => {
                // (memory counts its batches as rows, and each batch's arrays of every column, the projected or not)
                let batches: usize = self.parts.iter().map(|p| p.len()).sum();
                let bytes: usize = self
                    .parts
                    .iter()
                    .flatten()
                    .map(|&(file, batch)| self.files[file as usize].columns.iter().map(|c| c[batch as usize].get_array_memory_size()).sum::<usize>())
                    .sum();
                writeln!(f, "format=memory")?;
                writeln!(f, "rows={batches}")?;
                writeln!(f, "bytes={bytes}")
            }
        }
    }
    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning { datafusion::physical_plan::Partitioning::UnknownPartitioning(self.parts.len()) }
    fn eq_properties(&self) -> datafusion::physical_expr::EquivalenceProperties { datafusion::physical_expr::EquivalenceProperties::new(self.projected_schema.clone()) }
    fn scheduling_type(&self) -> datafusion::physical_plan::execution_plan::SchedulingType { datafusion::physical_plan::execution_plan::SchedulingType::Cooperative }
    fn partition_statistics(&self, partition: Option<usize>) -> datafusion::error::Result<Arc<Statistics>> {
        let all = self.stats.len() - 1;
        let slot = match partition {
            Some(i) if i < all => &self.stats[i],
            None => &self.stats[all],
            Some(_) => return Ok(Arc::new(Statistics::new_unknown(&self.projected_schema))),
        };
        if let Some(s) = slot.get() {
            return Ok(s.clone());
        }
        let projection = self.projection_indices();
        let mut s = match partition {
            Some(i) => statistics_of(&self.files, self.parts[i].iter().copied(), &projection),
            None => statistics_of(&self.files, self.parts.iter().flatten().copied(), &projection),
        };
        let names: Vec<&str> = projection.iter().map(|&i| self.schema.field(i).name().as_str()).collect();
        for (c, name) in s.column_statistics.iter_mut().zip(names) {
            let Some(k) = self.known.get(name) else { continue };
            if let Some((lo, hi)) = &k.range {
                (c.min_value, c.max_value) = (Precision::Inexact(lo.clone()), Precision::Inexact(hi.clone()));
            }
            if let (Some(d), Some(&rows)) = (k.distinct, s.num_rows.get_value()) {
                c.distinct_count = Precision::Inexact(d.min(rows).max(1));
            }
        }
        Ok(slot.get_or_init(|| Arc::new(s)).clone())
    }
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> { Some(Arc::new(HotSource { fetch: limit, ..self.clone() })) }
    fn fetch(&self) -> Option<usize> { self.fetch }
    fn try_swapping_with_projection(&self, projection: &datafusion::physical_expr::projection::ProjectionExprs) -> datafusion::error::Result<Option<Arc<dyn DataSource>>> {
        // (a projection of plain columns: the batches and their order stay, and ranges go by name)
        let exprs = projection.iter().cloned().collect::<Vec<_>>();
        if !all_alias_free_columns(&exprs) {
            return Ok(None);
        }
        let all: Vec<usize> = (0..self.schema.fields().len()).collect();
        let columns = new_projections_for_columns(&exprs, self.projection.as_ref().unwrap_or(&all));
        let projected_schema = project_schema(&self.schema, Some(&columns))?;
        Ok(Some(Arc::new(HotSource { projection: Some(columns), projected_schema, stats: stats_for(self.parts.len()), ..self.clone() })))
    }
    fn try_pushdown_sort(&self, order: &[datafusion::physical_expr::PhysicalSortExpr]) -> datafusion::error::Result<datafusion::physical_plan::SortOrderPushdownResult<Arc<dyn DataSource>>> {
        // A top-N reads each partition's batches in the order its first key's ranges give, so its
        // bound is found in the first batches and the rest are skipped: the newest rows of a table
        // whose rows came oldest first, too. The sort above still sorts.
        use datafusion::physical_plan::SortOrderPushdownResult::{Inexact, Unsupported};
        if self.fetch.is_some() {
            return Ok(Unsupported); // (a fetch keeps the first rows: reordered, it would keep others)
        }
        let Some((key, desc)) = order.first().and_then(|o| Some((o.expr.downcast_ref::<datafusion::physical_expr::expressions::Column>()?.name().to_string(), o.options.descending))) else { return Ok(Unsupported) };
        let mut parts = Vec::with_capacity(self.parts.len());
        for list in self.parts.iter() {
            let Some(ends) = list.iter().map(|&(file, batch)| self.files[file as usize].ranges.get(&key).map(|r| datafusion::common::ScalarValue::try_from_array(&r[usize::from(desc)], batch as usize).ok())).collect::<Option<Vec<_>>>() else { return Ok(Unsupported) };
            let mut order: Vec<usize> = (0..list.len()).collect();
            // (by least value up, or greatest down; a batch of NULLs, or of no known range, last)
            order.sort_by(|&a, &b| match (ends[a].as_ref().filter(|v| !v.is_null()), ends[b].as_ref().filter(|v| !v.is_null())) {
                (Some(x), Some(y)) => (if desc { y.partial_cmp(x) } else { x.partial_cmp(y) }).unwrap_or(std::cmp::Ordering::Equal),
                (x, y) => x.is_none().cmp(&y.is_none()),
            });
            parts.push(order.iter().map(|&i| list[i]).collect::<Vec<_>>());
        }
        Ok(Inexact { inner: Arc::new(HotSource { parts: Arc::new(parts), ..self.clone() }) })
    }
    fn try_pushdown_filters(&self, filters: Vec<Arc<dyn PhysicalExpr>>, _: &datafusion::common::config::ConfigOptions) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let below = vec![PushedDown::No; filters.len()];
        let schema = self.schema.clone();
        let skips = |f: &Arc<dyn PhysicalExpr>| DynamicFilterTracking::classify(f).contains_dynamic_filter() || datafusion::physical_expr::utils::collect_columns(f).iter().any(|c| schema.field_with_name(c.name()).is_ok_and(|f| ranges(&[new_null_array(f.data_type(), 0)]).is_some()));
        let useful: Vec<_> = filters.into_iter().filter(skips).collect();
        if useful.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(below));
        }
        let predicate = datafusion::physical_expr::conjunction(self.predicate.iter().cloned().chain(useful));
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(below).with_updated_node(Arc::new(HotSource { predicate: Some(predicate), ..self.clone() }) as Arc<dyn DataSource>))
    }
    fn apply_expressions(&self, _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>) -> datafusion::error::Result<TreeNodeRecursion> {
        // (the predicate isn't shown: a join or an aggregate builds its dynamic filter only for a
        // plan below it that shows it, and building a join's cost TPC-H from memory a tenth, for
        // batches a key range never rules out; a top-N keeps its bound up whoever reads it)
        Ok(TreeNodeRecursion::Continue)
    }
}

/// A top-N reads its hot scan in its first key's order through the filters, projections and
/// exchanges between them (DataFusion's own sort pushdown stops at a filter), so its bound is
/// tight after the first batches and the rest are skipped. Only the batches' order changes.
#[derive(Debug)]
pub struct TopFirst;

impl datafusion::physical_optimizer::PhysicalOptimizerRule for TopFirst {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &datafusion::common::config::ConfigOptions) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
        plan.transform_down(|p| {
            let Some(sort) = p.downcast_ref::<datafusion::physical_plan::sorts::sort::SortExec>().filter(|s| s.fetch().is_some()) else { return Ok(Transformed::no(p)) };
            Ok(match top_first(sort.input(), sort.expr())? {
                Some(input) => Transformed::yes(replace_children_if_necessary(p.clone(), vec![input])?),
                None => Transformed::no(p),
            })
        })
        .data()
    }
    fn name(&self) -> &str { "hot_top_first" }
    fn schema_check(&self) -> bool { true }
}

/// `p` with the hot scan under it ordered for `order`, when only operators that keep every row
/// as it is lie between (a filter drops rows, never changes them).
fn top_first(p: &Arc<dyn ExecutionPlan>, order: &[datafusion::physical_expr::PhysicalSortExpr]) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
    use datafusion::physical_plan::{filter::FilterExec, projection::ProjectionExec, repartition::RepartitionExec, SortOrderPushdownResult};
    if let Some(scan) = p.downcast_ref::<DataSourceExec>() {
        if !scan.data_source().is::<HotSource>() {
            return Ok(None);
        }
        return Ok(match scan.try_pushdown_sort(order)? {
            SortOrderPushdownResult::Exact { inner } | SortOrderPushdownResult::Inexact { inner } => Some(inner),
            SortOrderPushdownResult::Unsupported => None,
        });
    }
    let keeps = p.downcast_ref::<FilterExec>().is_some() || p.downcast_ref::<ProjectionExec>().is_some() || p.downcast_ref::<RepartitionExec>().is_some();
    match p.children()[..] {
        [c] if keeps => top_first(c, order)?.map(|c| replace_children_if_necessary(p.clone(), vec![c])).transpose(),
        _ => Ok(None),
    }
}

crate::per_node!(
    /// Batches scans skipped by their ranges (`/metrics`).
    pub static SKIPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0));

/// Which of a partition's batches its scan's predicate may match, worked out again when a dynamic
/// filter in it moves.
struct Skip {
    predicate: Arc<dyn PhysicalExpr>,
    tracking: DynamicFilterTracking,
    schema: SchemaRef,
    files: Arc<Vec<Held>>,
    list: Vec<(u32, u32)>, // the partition's batches, as (file, batch), in the order it reads them
    keep: Option<Vec<bool>>,
    next: usize, // (the batch from which a moved filter is looked at again)
    gap: usize,
    stats: Mutex<HashMap<String, Option<Ranges>>>, // (the partition's ranges of a column, made when first asked)
}

impl Skip {
    fn keep(&mut self, i: usize) -> bool {
        let again = match self.keep {
            None => true,
            Some(_) => i >= self.next && self.tracking.watcher().is_some_and(|w| w.changed()),
        };
        if again {
            // (a predicate the ranges can't decide, or that fails to build, keeps every batch)
            let keep: Vec<bool> = PruningPredicateBuilder::new().with_file_schema(self.schema.clone()).build(self.predicate.clone()).and_then(|p| p.prune(self).ok()).unwrap_or_default();
            // Looking again costs as much as a batch: soon at first, then ever later (a top-N
            // moves its bound after every batch; read in its order, the first batches set it).
            self.next = i + self.gap;
            self.gap = (self.gap * 2).min(64);
            self.keep = Some(keep);
        }
        let keep = self.keep.as_ref().and_then(|k| k.get(i)).copied().unwrap_or(true);
        if !keep {
            SKIPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        keep
    }

    /// The partition's batches' ranges of `column` (one per batch), if it has them.
    fn ranges(&self, column: &str) -> Option<Ranges> {
        let mut stats = self.stats.lock().unwrap();
        stats
            .entry(column.to_string())
            .or_insert_with(|| {
                // (a run of one file's batches in order is one slice: a partition holds hundreds)
                let mut runs: Vec<(&Ranges, usize, usize)> = Vec::new();
                for &(file, batch) in &self.list {
                    let r = self.files[file as usize].ranges.get(column)?;
                    let batch = batch as usize;
                    match runs.last_mut() {
                        Some((last, start, len)) if Arc::ptr_eq(last, r) && *start + *len == batch => *len += 1,
                        _ => runs.push((r, batch, 1)),
                    }
                }
                let each: Vec<[ArrayRef; 4]> = runs.iter().map(|(r, i, n)| std::array::from_fn(|k| r[k].slice(*i, *n))).collect();
                let all = |k: usize| datafusion::arrow::compute::concat(&each.iter().map(|r| r[k].as_ref()).collect::<Vec<_>>()).ok();
                Some(Arc::new([all(0)?, all(1)?, all(2)?, all(3)?]))
            })
            .clone()
    }
}

impl PruningStatistics for Skip {
    fn min_values(&self, column: &datafusion::common::Column) -> Option<ArrayRef> { self.ranges(column.name()).map(|r| r[0].clone()) }
    fn max_values(&self, column: &datafusion::common::Column) -> Option<ArrayRef> { self.ranges(column.name()).map(|r| r[1].clone()) }
    fn num_containers(&self) -> usize { self.list.len() }
    fn null_counts(&self, column: &datafusion::common::Column) -> Option<ArrayRef> { self.ranges(column.name()).map(|r| r[2].clone()) }
    fn row_counts(&self) -> Option<ArrayRef> { self.list.first().and_then(|&(file, _)| self.files[file as usize].ranges.keys().next().cloned()).and_then(|c| self.ranges(&c)).map(|r| r[3].clone()) }
    fn contained(&self, _: &datafusion::common::Column, _: &HashSet<datafusion::common::ScalarValue>) -> Option<BooleanArray> { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{AsArray, StringViewArray};
    use datafusion::arrow::datatypes::Int64Type;

    fn names_and_types(fields: Vec<FieldRef>) -> Vec<(String, DataType)> {
        fields.iter().map(|f| (f.name().to_string(), f.data_type().clone())).collect()
    }

    #[test]
    fn put_holds_a_file_in_the_readers_batches() {
        let hot = Hot { max: 1 << 30, memory: false, state: Default::default(), slot: tokio::sync::Semaphore::new(1) };
        // Three writes of different sizes: the file is still read in batches of 8,192 rows.
        let pieces = [(0i64, 5_000i64), (5_000, 20_000), (25_000, 3)];
        let ints: Vec<ArrayRef> = pieces.iter().map(|&(from, n)| Arc::new(Int64Array::from_iter_values(from..from + n)) as ArrayRef).collect();
        let strings: Vec<ArrayRef> = pieces.iter().map(|&(from, n)| Arc::new(StringViewArray::from_iter_values((from..from + n).map(|v| format!("s{v}")))) as ArrayRef).collect();
        let fields: Vec<FieldRef> = vec![Arc::new(Field::new("i", DataType::Int64, true)), Arc::new(Field::new("s", DataType::Utf8View, true))];
        hot.put("data/t/x.parquet", &fields, vec![ints, strings], 1 << 30);

        let schema = Arc::new(Schema::new(fields.iter().map(|f| f.as_ref().clone()).collect::<Vec<Field>>()));
        let held = hot.get("data/t/x.parquet", &schema).expect("held");
        let batches: Vec<RecordBatch> = (0..held.rows.len()).map(|i| held.batch(&schema, &[0, 1], i)).collect::<datafusion::error::Result<_>>().unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(), [8192, 8192, 8192, 427]);
        let values: Vec<i64> = batches.iter().flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec()).collect();
        assert_eq!(values, (0..25_003).collect::<Vec<i64>>());
        let texts: Vec<String> = batches.iter().flat_map(|b| b.column(1).as_string_view().iter().map(|s| s.unwrap_or_default().to_string()).collect::<Vec<_>>()).collect();
        assert!(texts.iter().enumerate().all(|(i, s)| *s == format!("s{i}")));
        assert!(held.ranges.get("i").is_some());
        assert!(held.ranges.get("s").is_none());

        // The table's folder holds them; another table whose name starts the same doesn't.
        let held_fields = hot.columns("t", &schema);
        assert_eq!(names_and_types(held_fields), vec![("i".to_string(), DataType::Int64), ("s".to_string(), DataType::Utf8View)]);
        assert!(hot.columns("t2", &schema).is_empty());
    }

    #[test]
    fn a_source_reads_its_batches_in_the_order_they_are_dealt() {
        // Two files, of 3 and 2 batches (4, 4, 2 and 3, 5 rows), with an Int64 column counting up through them.
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        let file = |from: i64, sizes: &[usize]| {
            let mut at = from;
            let arrays: Vec<ArrayRef> = sizes
                .iter()
                .map(|&n| {
                    let array: ArrayRef = Arc::new(Int64Array::from_iter_values(at..at + n as i64));
                    at += n as i64;
                    array
                })
                .collect();
            Held { columns: vec![Arc::new(arrays)], rows: Arc::new(sizes.to_vec()), ranges: Default::default() }
        };
        let files = vec![file(0, &[4, 4, 2]), file(10, &[3, 5])];
        // Batch i goes to partition i % 2 in turn: file 0's batches 0 and 2 and file 1's batch 1, then file 0's 1 and file 1's 0.
        assert_eq!(deal(&files, 2), vec![vec![(0, 0), (0, 2), (1, 1)], vec![(0, 1), (1, 0)]]);

        let source = HotSource::new(schema, files, 2, Default::default());
        let rows = |source: &dyn DataSource, p: usize| -> Vec<i64> {
            let stream = source.open(p, Arc::new(datafusion::execution::TaskContext::default())).unwrap();
            let batches = futures::executor::block_on(datafusion::physical_plan::common::collect(stream)).unwrap();
            batches.iter().flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec()).collect()
        };
        assert_eq!(rows(&source, 0), [0, 1, 2, 3, 8, 9, 13, 14, 15, 16, 17]);
        assert_eq!(rows(&source, 1), [4, 5, 6, 7, 10, 11, 12]);

        // A fetch of 5 ends in the middle of the second batch read (its first row only).
        let limited = source.with_fetch(Some(5)).unwrap();
        assert_eq!(rows(limited.as_ref(), 0), [0, 1, 2, 3, 8]);

        assert_eq!(source.partition_statistics(None).unwrap().num_rows, Precision::Exact(18));
        assert_eq!(source.partition_statistics(Some(1)).unwrap().num_rows, Precision::Exact(7));
    }

    #[test]
    fn gated_holds_a_scans_loads_until_the_last_copy_of_its_plan_goes() {
        use datafusion::datasource::physical_plan::ParquetSource;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::physical_plan::displayable;
        use tokio::sync::oneshot::error::TryRecvError;
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        let groups = vec![
            FileGroup::new(vec![datafusion::datasource::listing::PartitionedFile::new("a.parquet", 10), datafusion::datasource::listing::PartitionedFile::new("b.parquet", 10)]),
            FileGroup::new(vec![datafusion::datasource::listing::PartitionedFile::new("c.parquet", 10)]),
        ];
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(ParquetSource::new(schema))).with_file_groups(groups).build();
        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        let (go, mut wait) = tokio::sync::oneshot::channel::<()>();
        let gate = Arc::new(Gate(go));
        let tagged = gated(plan.clone(), &gate).unwrap();
        drop(gate);

        // Every file of every group holds the gate, and the plan reads as it did (no node added, the same files).
        let scan = tagged.downcast_ref::<DataSourceExec>().expect("a file scan");
        let config = scan.data_source().downcast_ref::<FileScanConfig>().expect("its config");
        let files: Vec<bool> = config.file_groups.iter().flat_map(|g| g.iter().map(|f| f.extension::<Arc<Gate>>().is_some())).collect();
        assert_eq!(files, [true, true, true]);
        assert_eq!(displayable(tagged.as_ref()).indent(false).to_string(), displayable(plan.as_ref()).indent(false).to_string());

        // The loads wait while either copy of the plan is held, and go once the last one is dropped.
        assert!(matches!(wait.try_recv(), Err(TryRecvError::Empty)));
        drop(plan);
        assert!(matches!(wait.try_recv(), Err(TryRecvError::Empty)));
        drop(tagged);
        assert!(matches!(wait.try_recv(), Err(TryRecvError::Closed)));
    }
}
