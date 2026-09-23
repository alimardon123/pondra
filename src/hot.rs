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
use crate::store::{DataFile, Lake};
use anyhow::Result;
use datafusion::arrow::array::{new_null_array, ArrayData, ArrayRef, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::parquet::arrow::{arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder}, ProjectionMask};
use datafusion::physical_plan::{empty::EmptyExec, union::UnionExec, ExecutionPlan};
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
    bytes: usize,
    read: Instant,
}

impl Hot {
    pub fn new() -> Hot {
        let gb = std::env::var("PONDRA_HOT_GB").ok().and_then(|g| g.parse::<f64>().ok());
        let max = gb.map(|g| (g * (1u64 << 30) as f64) as usize).unwrap_or_else(|| crate::store::memory_limit() / 4); // (a quarter of the query memory: an eighth of RAM)
        Hot { max, state: Default::default(), slot: tokio::sync::Semaphore::new(1) }
    }

    pub fn on(&self) -> bool { self.max > 0 }

    /// Bytes held, and the most it may hold.
    pub fn usage(&self) -> (usize, usize) { (self.state.lock().unwrap().used, self.max) }

    /// Give memory back when the process is using most of the machine's. Queries, the Parquet
    /// decoding under them and the batches in flight are not all counted anywhere, so the cache
    /// watches the process itself rather than only its own budget, and drops the columns nobody
    /// has read lately until the node is under the line again.
    pub fn watch(self: &Arc<Self>) {
        let (hot, Some(ram)) = (self.clone(), crate::store::ram()) else { return };
        if !self.on() {
            return;
        }
        tokio::spawn(async move {
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

    /// `file`'s batches with `schema`'s columns, if all of them are here.
    fn get(&self, file: &str, schema: &SchemaRef) -> Option<Vec<RecordBatch>> {
        let mut s = self.state.lock().unwrap();
        let rows = s.rows.get(file)?.clone();
        let now = Instant::now();
        let mut cols = vec![];
        for f in schema.fields() {
            let c = s.cols.get_mut(&(file.to_string(), f.name().clone()))?;
            c.read = now;
            cols.push(c.arrays.clone());
        }
        let batch = |i: usize| RecordBatch::try_new_with_options(schema.clone(), cols.iter().map(|c| c[i].clone()).collect(), &RecordBatchOptions::new().with_row_count(Some(rows[i])));
        (0..rows.len()).map(batch).collect::<Result<_, _>>().ok()
    }

    /// Decode `file`'s columns among `fields` that aren't here yet, in the background, if they
    /// can fit.
    fn load(self: &Arc<Self>, lake: Arc<Lake>, file: DataFile, fields: Vec<FieldRef>) {
        let guess = (file.bytes as usize).max(1 << 20) * 3; // decoded LZ4 Parquet: roughly 3x (checked after)
        // Queries come first: what they hold now is off the cache's budget, so a node under load
        // keeps its memory for them (both are bounded by `--memory-gb`).
        let (reserved, limit) = lake.memory();
        let room = self.max.min(limit.saturating_sub(reserved));
        let fields: Vec<FieldRef> = {
            let mut s = self.state.lock().unwrap();
            let missing: Vec<FieldRef> = fields.into_iter().filter(|f| !s.cols.contains_key(&(file.path.clone(), f.name().clone()))).collect();
            if missing.is_empty() || s.busy.contains(&file.path) || !s.twice(&file.path) || !s.room(guess, room) {
                return;
            }
            s.busy.insert(file.path.clone());
            missing
        };
        let hot = self.clone();
        tokio::spawn(async move {
            let _slot = hot.slot.acquire().await;
            let decoded = decode(&lake, &file.path, &fields).await;
            let mut s = hot.state.lock().unwrap();
            s.busy.remove(&file.path);
            let Ok((rows, arrays)) = decoded else { return };
            if s.rows.get(&file.path).is_some_and(|r| *r != rows) {
                return; // (batches cut differently: can't be combined)
            }
            let sizes: Vec<usize> = arrays.iter().map(|a| size(a)).collect();
            if !s.room(sizes.iter().sum(), room) {
                return;
            }
            s.rows.insert(file.path.clone(), rows);
            for ((f, arrays), bytes) in fields.iter().zip(arrays).zip(sizes) {
                s.used += bytes;
                s.cols.insert((file.path.clone(), f.name().clone()), Col { arrays: Arc::new(arrays), bytes, read: Instant::now() });
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

/// `fields` of the Parquet file at `path`, cast to their types (a column the file lacks, added
/// to the table after it was written, is null): rows per batch, and each field's arrays.
async fn decode(lake: &Lake, path: &str, fields: &[FieldRef]) -> Result<(Vec<usize>, Vec<Vec<ArrayRef>>)> {
    let bytes = lake.object(path).await?;
    let fields = fields.to_vec();
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
        let (mut rows, mut out) = (vec![], vec![vec![]; fields.len()]);
        for batch in b.with_projection(mask).with_batch_size(BATCH).build()? {
            let batch = batch?;
            rows.push(batch.num_rows());
            for (f, arrays) in fields.iter().zip(&mut out) {
                arrays.push(match batch.column_by_name(f.name()) {
                    Some(c) => datafusion::arrow::compute::cast(c, f.data_type())?,
                    None => new_null_array(f.data_type(), batch.num_rows()),
                });
            }
        }
        Ok((rows, out))
    })
    .await?
}

/// A table's Parquet files, read through the hot columns: files whose columns a scan needs are
/// all in memory come from there, the others from Parquet (with the scan's filters, to skip row
/// groups), and are loaded for next time.
pub struct HotFiles {
    pub lake: Arc<Lake>,
    pub files: Vec<DataFile>,
    pub schema: SchemaRef,
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
        let (mut cached, mut cold) = (vec![], vec![]);
        for f in &self.files {
            match self.lake.hot.get(&f.path, &schema) {
                Some(batches) => cached.extend(batches),
                None => cold.push(f),
            }
        }
        let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
        if !cold.is_empty() {
            let urls = cold.iter().map(|f| ListingTableUrl::parse(self.lake.full(&f.path))).collect::<Result<Vec<_>, _>>()?;
            let options = ParquetReadOptions::default().to_listing_options(state.config(), state.table_options().clone());
            let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options).with_schema(self.schema.clone());
            let table = ListingTable::try_new(config)?.with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
            plans.push(table.scan(state, projection, filters, limit).await?);
            for f in cold {
                self.lake.hot.load(self.lake.clone(), f.clone(), schema.fields().to_vec());
            }
        }
        if !cached.is_empty() {
            let n = state.config().target_partitions().max(1);
            let mut parts = vec![vec![]; n];
            for (i, b) in cached.into_iter().enumerate() {
                parts[i % n].push(b);
            }
            parts.retain(|p| !p.is_empty());
            plans.push(MemorySourceConfig::try_new_exec(&parts, schema.clone(), None)?);
        }
        match plans.len() {
            0 => Ok(Arc::new(EmptyExec::new(schema))),
            _ => UnionExec::try_new(plans),
        }
    }
}
