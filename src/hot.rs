//! Hot columns: the columns queries read lately, decoded, in memory. A scan takes a file's columns
//! from here when all it needs are here, and skips reading and decoding the Parquet file (TPC-H
//! runs about 1.5x faster hot). Files never change, so nothing here goes stale.
//!
//! Filled in the background, one file at a time, within `PONDRA_HOT_GB` (default: a quarter of
//! RAM; 0 turns it off), and only for a file a second scan came back to: data read once — a
//! backfill, a consumer reading the log through, a one-off report — passes through without
//! costing the CPU that decoding it would. When full, the cache makes room only by dropping
//! columns nobody read for a minute: a working set bigger than memory is read from Parquet as
//! before, instead of being churned through the cache.
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
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::parquet::arrow::{arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder}, ProjectionMask};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_expr::{DynamicFilterTracking, PhysicalExpr};
use datafusion::physical_optimizer::pruning::{PruningPredicateBuilder, PruningStatistics};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::{empty::EmptyExec, execution_plan::replace_children_if_necessary, union::UnionExec, ExecutionPlan};
use futures::StreamExt;
use datafusion::prelude::ParquetReadOptions;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const IDLE: Duration = Duration::from_secs(60); // unread this long: may make room for others
const BATCH: usize = 8192;

pub struct Hot {
    max: usize,
    state: Mutex<State>,
    slot: tokio::sync::Semaphore, // one file at a time: loading competes with queries for CPU
}

#[derive(Default)]
struct State {
    cols: HashMap<(String, String), Col>, // (file, column) -> its arrays, a batch each
    rows: HashMap<String, Vec<usize>>,    // file -> rows per batch (the same for all its columns)
    used: usize,
    busy: HashSet<String>,           // files being loaded
    once: std::collections::VecDeque<String>, // files a scan read once (the next read loads them)
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

impl Hot {
    pub fn new() -> Hot {
        let gb = std::env::var("PONDRA_HOT_GB").ok().and_then(|g| g.parse::<f64>().ok());
        let max = gb.map(|g| (g * (1u64 << 30) as f64) as usize).unwrap_or_else(|| crate::store::memory_limit() / 4); // (a quarter of the query memory: an eighth of RAM)
        Hot { max, state: Default::default(), slot: tokio::sync::Semaphore::new(1) }
    }

    pub fn on(&self) -> bool { self.max > 0 }

    /// Bytes held, the most it may hold, and files being loaded (or waiting to be).
    pub fn usage(&self) -> (usize, usize, usize) {
        let s = self.state.lock().unwrap();
        (s.used, self.max, s.busy.len())
    }

    /// Give memory back when the process is using most of the machine's. Queries, the Parquet
    /// decoding under them and the batches in flight are not all counted anywhere, so the cache
    /// watches the process itself rather than only its own budget, and drops the columns nobody
    /// has read lately until the node is under the line again.
    pub fn watch(self: &Arc<Self>) {
        let (hot, Some(ram)) = (self.clone(), crate::store::ram()) else { return };
        if !self.on() {
            return;
        }
        crate::panics::spawn(async move {
            let line = ram / 5 * 3; // three fifths of the machine
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if let Some(rss) = crate::store::resident() {
                    if rss > line {
                        hot.trim(rss - line);
                    }
                }
            }
        });
    }

    /// Drop the least recently read columns until `bytes` are freed.
    fn trim(&self, bytes: usize) {
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
    }

    /// `file`'s batches with `schema`'s columns, if all of them are here, and their columns' ranges.
    fn get(&self, file: &str, schema: &SchemaRef) -> Option<(Vec<RecordBatch>, Arc<HashMap<String, Ranges>>)> {
        let mut s = self.state.lock().unwrap();
        let rows = s.rows.get(file)?.clone();
        let now = Instant::now();
        let (mut cols, mut ranges) = (vec![], HashMap::new());
        for f in schema.fields() {
            let c = s.cols.get_mut(&(file.to_string(), f.name().clone()))?;
            c.read = now;
            cols.push(c.arrays.clone());
            if let Some(r) = &c.ranges {
                ranges.insert(f.name().clone(), r.clone());
            }
        }
        let batch = |i: usize| RecordBatch::try_new_with_options(schema.clone(), cols.iter().map(|c| c[i].clone()).collect(), &RecordBatchOptions::new().with_row_count(Some(rows[i])));
        Some(((0..rows.len()).map(batch).collect::<Result<_, _>>().ok()?, Arc::new(ranges)))
    }

    /// Decode `file`'s columns among `fields` that aren't here yet, in the background, if they
    /// can fit (`deletes`: without its deleted rows).
    fn load(self: &Arc<Self>, lake: Arc<Lake>, file: DataFile, fields: Vec<FieldRef>, deletes: bool) {
        let guess = (file.bytes as usize).max(1 << 20) * 3; // decoded LZ4 Parquet: roughly 3x (checked after)
        // Queries come first: what they hold now is off the cache's budget, so a node under load
        // keeps its memory for them (both are bounded by `--memory-gb`).
        let (reserved, limit) = lake.memory();
        let room = self.max.min(limit.saturating_sub(reserved));
        let key = key(&file, deletes);
        let fields: Vec<FieldRef> = {
            let mut s = self.state.lock().unwrap();
            let missing: Vec<FieldRef> = fields.into_iter().filter(|f| !s.cols.contains_key(&(key.clone(), f.name().clone()))).collect();
            if missing.is_empty() || s.busy.contains(&key) || !s.twice(&key) || !s.room(guess, room) {
                return;
            }
            s.busy.insert(key.clone());
            missing
        };
        let hot = self.clone();
        crate::panics::spawn(async move {
            let _slot = hot.slot.acquire().await;
            let decoded = decode(&lake, &file, &fields, deletes).await;
            let mut s = hot.state.lock().unwrap();
            s.busy.remove(&key);
            let Ok((rows, arrays, ranges)) = decoded else { return };
            if s.rows.get(&key).is_some_and(|r| *r != rows) {
                return; // (batches cut differently: can't be combined)
            }
            let sizes: Vec<usize> = arrays.iter().map(|a| size(a)).collect();
            if !s.room(sizes.iter().sum(), room) {
                return;
            }
            s.rows.insert(key.clone(), rows);
            for (((f, arrays), ranges), bytes) in fields.iter().zip(arrays).zip(ranges).zip(sizes) {
                s.used += bytes;
                s.cols.insert((key.clone(), f.name().clone()), Col { arrays: Arc::new(arrays), ranges, bytes, read: Instant::now() });
            }
        });
    }
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

/// `fields` of `file`, cast to their types (a column the file lacks, added to the table after it
/// was written, is null): rows per batch, and each field's arrays and their ranges. Its system
/// columns come from its lineage if it has one; with `deletes`, its deleted rows are left out.
async fn decode(lake: &Lake, file: &DataFile, fields: &[FieldRef], deletes: bool) -> Result<(Vec<usize>, Vec<Vec<ArrayRef>>, Vec<Option<Ranges>>)> {
    let bytes = lake.object(&file.path).await?;
    let gone = if deletes && !file.deletes.is_empty() { crate::scan::deleted_rows(lake, file).await? } else { vec![] };
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
    let fill = |base: *mut u8| {
        let mut offset = 0;
        for b in &distinct {
            // SAFETY: `base` has `total` bytes, laid out above for these buffers in this order; nothing reads them yet.
            unsafe { std::ptr::copy_nonoverlapping(b.as_ptr(), base.add(offset), b.len()) };
            offset += b.len().next_multiple_of(ALIGN);
        }
    };
    let (base, arena) = match mapped(total) {
        Some((base, mapped)) => {
            fill(base);
            (base, mapped)
        }
        None => {
            let mut heap = MutableBuffer::from_len_zeroed(total);
            fill(heap.as_mut_ptr());
            let heap = Arc::new(Buffer::from(heap));
            (heap.as_ptr() as *mut u8, heap as Arc<dyn datafusion::arrow::alloc::Allocation>)
        }
    };
    let pieces: HashMap<(usize, usize), Buffer> = at
        .into_iter()
        .map(|((ptr, len), offset)| {
            // SAFETY: `offset..offset + len` is inside the arena (laid out above), which the owner keeps alive.
            let p = unsafe { Buffer::from_custom_allocation(std::ptr::NonNull::new_unchecked(base.add(offset)), len, arena.clone()) };
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

/// A file column's arena in memory of its own from the OS, given back whole when the file leaves
/// the hot columns. In the heap, files coming and going under steady writes left holes that the
/// next, larger files couldn't use: a node held hundreds of MB more than it counted, and still grew
/// after hours (`tools/soak.py`). Small arenas stay in the heap, which keeps them well, and so does
/// everything past `MOST` mappings: the OS allows a process about 65,000, and the allocator and the
/// threads need theirs. Elsewhere than Unix, or if the OS says no: the heap.
#[cfg(unix)]
fn mapped(len: usize) -> Option<(*mut u8, Arc<dyn datafusion::arrow::alloc::Allocation>)> {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    const SMALL: usize = 64 << 10;
    const MOST: usize = 16_384;
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    struct Mapped(std::ptr::NonNull<u8>, usize);
    // SAFETY: plain memory, written once before it is shared and only read after.
    unsafe impl Send for Mapped {}
    unsafe impl Sync for Mapped {}
    impl std::panic::RefUnwindSafe for Mapped {}
    impl Drop for Mapped {
        fn drop(&mut self) {
            // SAFETY: the mapping made below; every buffer pointing into it is gone (each held this).
            unsafe { libc::munmap(self.0.as_ptr() as *mut libc::c_void, self.1) };
            LIVE.fetch_sub(1, Relaxed);
        }
    }
    if len < SMALL || LIVE.fetch_add(1, Relaxed) >= MOST {
        if len >= SMALL {
            LIVE.fetch_sub(1, Relaxed);
        }
        return None;
    }
    // SAFETY: a fresh private anonymous mapping (zeroed), unmapped only by `Mapped`'s drop.
    let p = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0) };
    match std::ptr::NonNull::new(p as *mut u8).filter(|_| p != libc::MAP_FAILED) {
        Some(p) => Some((p.as_ptr(), Arc::new(Mapped(p, len)))),
        None => {
            LIVE.fetch_sub(1, Relaxed);
            None
        }
    }
}

#[cfg(not(unix))]
fn mapped(_: usize) -> Option<(*mut u8, Arc<dyn datafusion::arrow::alloc::Allocation>)> { None }

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
/// lineage when it has one (as `scan::adopted` gives it), else the file's column, or nulls.
fn column(batch: &RecordBatch, f: &FieldRef, lineage: Option<Lineage>, at: u64) -> ArrayRef {
    let n = batch.num_rows();
    match (lineage, f.name().as_str()) {
        (Some(l), crate::sys::ROW_ID) => Arc::new(Int64Array::from_iter_values((0..n as i64).map(|i| l.first + at as i64 + i))),
        (Some(l), crate::sys::VERSION) => Arc::new(Int64Array::from_value(l.version as i64, n)),
        (Some(l), crate::sys::CREATED | crate::sys::UPDATED) => Arc::new(TimestampMicrosecondArray::from_value(l.ms as i64 * 1000, n).with_timezone("UTC")),
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
        let (mut cached, mut cold) = (vec![], vec![]);
        for f in &self.files {
            match self.lake.hot.get(&key(f, deletes), &schema) {
                Some((batches, ranges)) => cached.extend(batches.into_iter().enumerate().map(|(i, b)| (b, (ranges.clone(), i)))),
                None => cold.push(f),
            }
        }
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
        let (plain, special): (Vec<&DataFile>, Vec<&DataFile>) = cold.iter().copied().partition(|f| f.lineage.is_none() && (f.deletes.is_empty() || !deletes));
        if !plain.is_empty() {
            let urls = plain.iter().map(|f| ListingTableUrl::parse(self.lake.full(&f.path))).collect::<Result<Vec<_>, _>>()?;
            let options = ParquetReadOptions::default().to_listing_options(state.config(), state.table_options().clone());
            let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options).with_schema(self.schema.clone());
            let table = ListingTable::try_new(config)?.with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
            plans.push(table.scan(state, projection, filters, limit).await?);
        }
        if !special.is_empty() {
            let state = state.as_any().downcast_ref::<datafusion::execution::SessionState>().ok_or_else(|| datafusion::error::DataFusionError::Internal("a session without its state".into()))?;
            let ctx = datafusion::prelude::SessionContext::new_with_state(state.clone());
            let df = crate::query::files_once(&self.lake, &ctx, &special, &self.meta, &self.schema).await.map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            plans.push(df.select_columns(&names)?.create_physical_plan().await?);
        }
        for f in cold {
            self.lake.hot.load(self.lake.clone(), f.clone(), schema.fields().to_vec(), deletes);
        }
        if !cached.is_empty() {
            let n = state.config().target_partitions().max(1);
            let (mut parts, mut pieces) = (vec![vec![]; n], vec![vec![]; n]);
            for (i, (b, piece)) in cached.into_iter().enumerate() {
                parts[i % n].push(b);
                pieces[i % n].push(piece);
            }
            parts.retain(|p| !p.is_empty());
            pieces.retain(|p| !p.is_empty());
            let memory = MemorySourceConfig::try_new(&parts, schema.clone(), None)?;
            plans.push(DataSourceExec::from_data_source(HotSource { memory, pieces: Arc::new(pieces), predicate: None }));
        }
        match plans.len() {
            0 => Ok(Arc::new(EmptyExec::new(schema))),
            _ => UnionExec::try_new(plans),
        }
    }
}

/// A batch's place: its file's columns' ranges, and which of its batches it is.
type Piece = (Arc<HashMap<String, Ranges>>, usize);

/// The hot batches of a scan, as DataFusion's memory source reads them, skipping each batch the
/// scan's filters rule out by its ranges. The filters stay above (rows are filtered there): here
/// they only skip batches, as a Parquet scan skips row groups. A dynamic filter (a join's keys, a
/// top-N's bound) is looked at again as it moves.
#[derive(Debug, Clone)]
struct HotSource {
    memory: MemorySourceConfig,
    pieces: Arc<Vec<Vec<Piece>>>, // (each partition's batches')
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl DataSource for HotSource {
    fn open(&self, partition: usize, context: Arc<datafusion::execution::TaskContext>) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let stream = self.memory.open(partition, context)?;
        let (Some(predicate), None) = (&self.predicate, self.memory.fetch()) else { return Ok(stream) }; // (a fetch counts the rows it reads: none skipped)
        let mut skip = Skip { predicate: predicate.clone(), tracking: DynamicFilterTracking::classify(predicate), schema: self.memory.original_schema(), pieces: self.pieces[partition].clone(), keep: None, next: 0, gap: 1, stats: Default::default() };
        let schema = stream.schema();
        let kept = stream.enumerate().filter_map(move |(i, b)| std::future::ready(skip.keep(i).then_some(b)));
        Ok(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, kept)))
    }
    fn fmt_as(&self, t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result { self.memory.fmt_as(t, f) }
    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning { self.memory.output_partitioning() }
    fn eq_properties(&self) -> datafusion::physical_expr::EquivalenceProperties { self.memory.eq_properties() }
    fn scheduling_type(&self) -> datafusion::physical_plan::execution_plan::SchedulingType { self.memory.scheduling_type() }
    fn partition_statistics(&self, partition: Option<usize>) -> datafusion::error::Result<Arc<datafusion::common::Statistics>> { self.memory.partition_statistics(partition) }
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> { Some(Arc::new(HotSource { memory: self.memory.clone().with_limit(limit), ..self.clone() })) }
    fn fetch(&self) -> Option<usize> { self.memory.fetch() }
    fn try_swapping_with_projection(&self, projection: &datafusion::physical_expr::projection::ProjectionExprs) -> datafusion::error::Result<Option<Arc<dyn DataSource>>> {
        // (a projection of plain columns: the batches and their order stay, and ranges go by name)
        let Some(projected) = self.memory.try_swapping_with_projection(projection)? else { return Ok(None) };
        let projected: Arc<dyn std::any::Any + Send + Sync> = projected;
        Ok(projected.downcast::<MemorySourceConfig>().ok().map(|m| Arc::new(HotSource { memory: Arc::unwrap_or_clone(m), ..self.clone() }) as Arc<dyn DataSource>))
    }
    fn try_pushdown_sort(&self, order: &[datafusion::physical_expr::PhysicalSortExpr]) -> datafusion::error::Result<datafusion::physical_plan::SortOrderPushdownResult<Arc<dyn DataSource>>> {
        // A top-N reads each partition's batches in the order its first key's ranges give, so its
        // bound is found in the first batches and the rest are skipped: the newest rows of a table
        // whose rows came oldest first, too. The sort above still sorts.
        use datafusion::physical_plan::SortOrderPushdownResult::{Inexact, Unsupported};
        if self.memory.fetch().is_some() {
            return Ok(Unsupported); // (a fetch keeps the first rows: reordered, it would keep others)
        }
        let Some((key, desc)) = order.first().and_then(|o| Some((o.expr.downcast_ref::<datafusion::physical_expr::expressions::Column>()?.name().to_string(), o.options.descending))) else { return Ok(Unsupported) };
        let (mut parts, mut pieces) = (vec![], vec![]);
        for (batches, own) in self.memory.partitions().iter().zip(self.pieces.iter()) {
            let Some(ends) = own.iter().map(|(r, i)| r.get(&key).map(|r| datafusion::common::ScalarValue::try_from_array(&r[usize::from(desc)], *i).ok())).collect::<Option<Vec<_>>>() else { return Ok(Unsupported) };
            let mut order: Vec<usize> = (0..own.len()).collect();
            // (by least value up, or greatest down; a batch of NULLs, or of no known range, last)
            order.sort_by(|&a, &b| match (ends[a].as_ref().filter(|v| !v.is_null()), ends[b].as_ref().filter(|v| !v.is_null())) {
                (Some(x), Some(y)) => (if desc { y.partial_cmp(x) } else { x.partial_cmp(y) }).unwrap_or(std::cmp::Ordering::Equal),
                (x, y) => x.is_none().cmp(&y.is_none()),
            });
            parts.push(order.iter().map(|&i| batches[i].clone()).collect::<Vec<_>>());
            pieces.push(order.iter().map(|&i| own[i].clone()).collect::<Vec<_>>());
        }
        let memory = MemorySourceConfig::try_new(&parts, self.memory.original_schema(), self.memory.projection().clone())?.with_limit(self.memory.fetch());
        Ok(Inexact { inner: Arc::new(HotSource { memory, pieces: Arc::new(pieces), predicate: self.predicate.clone() }) })
    }
    fn try_pushdown_filters(&self, filters: Vec<Arc<dyn PhysicalExpr>>, _: &datafusion::common::config::ConfigOptions) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let below = vec![PushedDown::No; filters.len()];
        let schema = self.memory.original_schema();
        let skips = |f: &Arc<dyn PhysicalExpr>| DynamicFilterTracking::classify(f).contains_dynamic_filter() || datafusion::physical_expr::utils::collect_columns(f).iter().any(|c| schema.field_with_name(c.name()).is_ok_and(|f| ranges(&[new_null_array(f.data_type(), 0)]).is_some()));
        let useful: Vec<_> = filters.into_iter().filter(skips).collect();
        if useful.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(below));
        }
        let predicate = datafusion::physical_expr::conjunction(self.predicate.iter().cloned().chain(useful));
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(below).with_updated_node(Arc::new(HotSource { predicate: Some(predicate), ..self.clone() }) as Arc<dyn DataSource>))
    }
    fn apply_expressions(&self, f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>) -> datafusion::error::Result<TreeNodeRecursion> {
        // (the predicate isn't shown: a join or an aggregate builds its dynamic filter only for a
        // plan below it that shows it, and building a join's cost TPC-H from memory a tenth, for
        // batches a key range never rules out; a top-N keeps its bound up whoever reads it)
        self.memory.apply_expressions(f)
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

/// Batches scans skipped by their ranges (`/metrics`).
pub static SKIPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Which of a partition's batches its scan's predicate may match, worked out again when a dynamic
/// filter in it moves.
struct Skip {
    predicate: Arc<dyn PhysicalExpr>,
    tracking: DynamicFilterTracking,
    schema: SchemaRef,
    pieces: Vec<Piece>,
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
                let each: Vec<[ArrayRef; 4]> = self.pieces.iter().map(|(r, i)| r.get(column).map(|r| std::array::from_fn(|k| r[k].slice(*i, 1)))).collect::<Option<_>>()?;
                let all = |k: usize| datafusion::arrow::compute::concat(&each.iter().map(|r| r[k].as_ref()).collect::<Vec<_>>()).ok();
                Some(Arc::new([all(0)?, all(1)?, all(2)?, all(3)?]))
            })
            .clone()
    }
}

impl PruningStatistics for Skip {
    fn min_values(&self, column: &datafusion::common::Column) -> Option<ArrayRef> { self.ranges(column.name()).map(|r| r[0].clone()) }
    fn max_values(&self, column: &datafusion::common::Column) -> Option<ArrayRef> { self.ranges(column.name()).map(|r| r[1].clone()) }
    fn num_containers(&self) -> usize { self.pieces.len() }
    fn null_counts(&self, column: &datafusion::common::Column) -> Option<ArrayRef> { self.ranges(column.name()).map(|r| r[2].clone()) }
    fn row_counts(&self) -> Option<ArrayRef> { self.pieces.first().and_then(|(r, _)| r.keys().next().cloned()).and_then(|c| self.ranges(&c)).map(|r| r[3].clone()) }
    fn contained(&self, _: &datafusion::common::Column, _: &HashSet<datafusion::common::ScalarValue>) -> Option<BooleanArray> { None }
}
