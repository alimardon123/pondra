//! Vortex's entry (designs/redesign/table-file-formats.md; the spike, step 5 piece 15): a table's files in Vortex's
//! format, read through vortex-datafusion. Built only with the `vortex` feature; `PONDRA_FILE_FORMAT=vortex` makes it
//! the format new files are written in (`file_formats::default`), until a table's own `file_format` option does (15c).

use super::{FileStats, Finished, Rows, TableFileFormat, TableWriter};
use anyhow::{anyhow, Result};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::logical_expr::Accumulator;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, LazyLock};
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::Buffer;
use vortex::file::WriteOptionsSessionExt;
use vortex::scan::selection::Selection;
use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;
use vortex_datafusion::{VortexAccessPlan, VortexFormat, VortexTableOptions};

#[derive(Debug)]
pub struct Vortex;

/// The entry (the engine registers it when built with the feature: `engine.rs`).
pub const VORTEX: Vortex = Vortex;

/// Vortex's own session: its encodings, its Arrow conversions and the runtime its reads run on, one for the process.
static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::default);

impl TableFileFormat for Vortex {
    fn name(&self) -> &'static str { "vortex" }
    fn ending(&self) -> &'static str { "vortex" }
    /// (`keys`: Vortex keeps its own zone maps for every column, and has no bloom filters to ask for)
    fn writer(&self, schema: SchemaRef, _keys: &[String]) -> Result<Box<dyn TableWriter>> { Ok(Box::new(Writer::new(schema)?)) }
    fn reader(&self, _: &dyn Session) -> Arc<dyn FileFormat> {
        // (the spike's switch for vortex-datafusion's newer scan, `PONDRA_VORTEX_SCAN_V2=1`, to measure both)
        let scan_v2 = crate::env::get("PONDRA_VORTEX_SCAN_V2").is_some_and(|v| v == "1");
        let mut options = VortexTableOptions::default();
        options.scan_v2 = scan_v2;
        Arc::new(VortexFormat::new_with_options(SESSION.clone(), options))
    }
    /// (Vortex's own selection: the rows' places, which its scan reads only the chunks of)
    fn select(&self, file: PartitionedFile, rows: &Rows) -> Option<PartitionedFile> {
        Some(file.with_extension(VortexAccessPlan::default().with_selection(selection(rows)?)))
    }
}

/// `rows` as a Vortex selection: the places read, or those left out.
fn selection(rows: &Rows) -> Option<Selection> {
    let sorted = |places: Vec<u64>| StrictSortedBuffer::try_new(Buffer::from_iter(places)).ok();
    if let Some(at) = rows.only {
        return Some(Selection::IncludeByIndex(sorted(at.iter().copied().filter(|p| *p < rows.count).collect())?));
    }
    let gone = || rows.gone.iter().copied().filter(|g| *g < rows.count);
    match rows.range {
        None => Some(Selection::ExcludeByIndex(sorted(gone().collect())?)),
        // (a part of the file: the places before and after it are left out with the deleted ones)
        Some((from, to)) => {
            let to = to.min(rows.count).max(from);
            let out: Vec<u64> = (0..from).chain(gone().filter(|g| *g >= from && *g < to)).chain(to..rows.count).collect();
            Some(Selection::ExcludeByIndex(sorted(out)?))
        }
    }
}

/// A file being written: its batches go to a thread of its own, which drives Vortex's writer (its API is async, and the
/// future it keeps isn't `Send`) into the file's bytes. What a file's entry takes of its columns is kept here, from
/// its rows, as they pass (`Ranges`).
struct Writer {
    send: Option<mpsc::SyncSender<RecordBatch>>,
    done: Option<std::thread::JoinHandle<Result<Vec<u8>>>>,
    written: Arc<AtomicU64>,
    buffered: vortex::layout::BufferedBytesTracker,
    ranges: Ranges,
}

impl Writer {
    fn new(schema: SchemaRef) -> Result<Self> {
        let options = SESSION.write_options();
        let buffered = options.buffered_bytes_tracker();
        let written = Arc::new(AtomicU64::new(0));
        let dtype = SESSION.arrow().from_arrow_schema(&schema).map_err(|e| anyhow!("vortex: {e}"))?;
        let (send, take) = mpsc::sync_channel::<RecordBatch>(1);
        let (count, of) = (written.clone(), schema.clone());
        // (Vortex's writer spawns its work on the session's runtime, the engine's tokio, which its thread enters)
        let tokio = tokio::runtime::Handle::try_current().map_err(|_| anyhow!("vortex: a file is written on the engine's runtime"))?;
        let done = std::thread::Builder::new().name("vortex-write".into()).spawn(move || -> Result<Vec<u8>> {
            let _on = tokio.enter();
            let runtime = vortex::io::runtime::current::CurrentThreadRuntime::new();
            let mut out = Counted { bytes: vec![], count };
            let mut w = options.blocking(&runtime).writer(&mut out, dtype);
            for b in take {
                let a = SESSION.arrow().from_arrow_record_batch(b, &of).map_err(|e| anyhow!("vortex: {e}"))?;
                w.push(a).map_err(|e| anyhow!("vortex: {e}"))?;
            }
            w.finish().map_err(|e| anyhow!("vortex: {e}"))?;
            Ok(out.bytes)
        })?;
        Ok(Writer { send: Some(send), done: Some(done), written, buffered, ranges: Ranges::new(&schema) })
    }

    /// The writing thread's error, once it has stopped.
    fn failed(&mut self) -> anyhow::Error {
        self.send = None;
        match self.done.take().map(|t| t.join()) {
            Some(Ok(Err(e))) => e,
            Some(Err(_)) => anyhow!("vortex: the file's writer panicked"),
            _ => anyhow!("vortex: the file's writer stopped"),
        }
    }
}

impl TableWriter for Writer {
    fn write(&mut self, b: &RecordBatch) -> Result<()> {
        self.ranges.add(b)?;
        let send = self.send.as_ref().ok_or_else(|| anyhow!("vortex: the file's writer stopped"))?;
        if send.send(b.clone()).is_err() {
            return Err(self.failed());
        }
        Ok(())
    }
    /// (what reached the file, and what its layouts hold before they write it)
    fn bytes(&self) -> usize { (self.written.load(Ordering::Relaxed) + self.buffered.buffered_bytes()) as usize }
    fn finish(mut self: Box<Self>) -> Result<Finished> {
        self.send = None; // (the end of its rows: the thread writes the footer)
        let bytes = match self.done.take().map(|t| t.join()) {
            Some(Ok(r)) => r?,
            _ => return Err(anyhow!("vortex: the file's writer panicked")),
        };
        Ok(Finished { bytes, stats: Box::new(std::mem::take(&mut self.ranges).done()) })
    }
}

/// Bytes written, counted as they go (`bytes`).
struct Counted {
    bytes: Vec<u8>,
    count: Arc<AtomicU64>,
}

impl std::io::Write for Counted {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(b);
        self.count.fetch_add(b.len() as u64, Ordering::Relaxed);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

/// A file's columns' min and max and sizes, from its rows as they are written: the columns a file's entry keeps a range
/// of (`manifest::Seen::ranges`: the first 32 that have an order, and the system columns; floats it keeps itself).
#[derive(Default)]
struct Ranges {
    ranged: Vec<(String, MinAccumulator, MaxAccumulator)>,
    at: Vec<usize>,
    sizes: BTreeMap<String, u64>,
    sized: Vec<(usize, String)>,
}

impl Ranges {
    fn new(schema: &Schema) -> Self {
        let mut out = Ranges::default();
        for (i, f) in schema.fields().iter().enumerate() {
            let t = f.data_type();
            if (i < 32 || crate::text::NAMES.contains(&f.name().as_str())) && crate::manifest::orderable(t) && !t.is_floating() {
                if let (Ok(lo), Ok(hi)) = (MinAccumulator::try_new(t), MaxAccumulator::try_new(t)) {
                    out.ranged.push((f.name().clone(), lo, hi));
                    out.at.push(i);
                }
            }
            if !fixed(t) {
                out.sized.push((i, f.name().clone()));
            }
        }
        out
    }

    fn add(&mut self, b: &RecordBatch) -> Result<()> {
        for ((_, lo, hi), i) in self.ranged.iter_mut().zip(&self.at) {
            lo.update_batch(&[b.column(*i).clone()])?;
            hi.update_batch(&[b.column(*i).clone()])?;
        }
        for (i, name) in &self.sized {
            *self.sizes.entry(name.clone()).or_default() += b.column(*i).to_data().get_slice_memory_size().unwrap_or(0) as u64;
        }
        Ok(())
    }
}

/// A type Parquet writes in a fixed width (`manifest::sizes` leaves those out): integers, floats, dates, times.
fn fixed(t: &DataType) -> bool { t.is_primitive() && !matches!(t, DataType::Decimal128(p, _) if *p > 18) && !matches!(t, DataType::Decimal256(..)) }

impl Ranges {
    /// What the file kept of its columns, once its rows are all written.
    fn done(self) -> Kept {
        let ranges = self.ranged.into_iter().filter_map(|(name, mut lo, mut hi)| Some((name, (lo.evaluate().ok()?, hi.evaluate().ok()?)))).collect();
        Kept { ranges, sizes: self.sizes }
    }
}

/// A finished file's columns' ranges and sizes (`Ranges::done`).
struct Kept {
    ranges: BTreeMap<String, (ScalarValue, ScalarValue)>,
    sizes: BTreeMap<String, u64>,
}

impl FileStats for Kept {
    fn range(&self, _: &Schema, name: &str) -> Option<(ScalarValue, ScalarValue)> { self.ranges.get(name).cloned() }
    fn sizes(&self) -> BTreeMap<String, u64> { self.sizes.clone() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::Field;

    /// A Vortex file written through the entry reads back with its rows, through DataFusion, and keeps its columns' ranges.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_file_reads_back_with_its_rows_and_keeps_ranges() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true), Field::new("s", DataType::Utf8, true)]));
        let dir = std::env::temp_dir().join(format!("pondra-vortex-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut w = VORTEX.writer(schema.clone(), &[]).unwrap();
        for from in [0i64, 1000] {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(from..from + 1000)),
                Arc::new(StringArray::from_iter_values((from..from + 1000).map(|i| format!("v{i:05}")))),
            ];
            w.write(&RecordBatch::try_new(schema.clone(), columns).unwrap()).unwrap();
        }
        let done = w.finish().unwrap();
        assert_eq!(done.stats.range(&schema, "i"), Some((ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(1999)))));
        assert_eq!(done.stats.range(&schema, "s"), Some((ScalarValue::from("v00000"), ScalarValue::from("v01999"))));
        assert!(done.stats.sizes().contains_key("s") && !done.stats.sizes().contains_key("i"));
        let path = dir.join("a.vortex");
        std::fs::write(&path, &done.bytes).unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        let df = super::super::read(&ctx, &VORTEX, vec![path.to_str().unwrap().to_string()], Some(schema.clone())).await.unwrap();
        let got = df.filter(datafusion::prelude::col("i").gt_eq(datafusion::prelude::lit(1990i64))).unwrap().collect().await.unwrap();
        assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A file's rows by their places, as Vortex's selection: those its deletes leave in its range, or those at places.
    #[test]
    fn rows_by_their_places_are_a_selection() {
        let picked = |gone: &[u64], range: Option<(u64, u64)>, only: Option<&[u64]>| {
            let s = selection(&Rows { count: 10, gone, range, only }).unwrap();
            (0..10u64).filter(|p| s.row_mask(&(*p..*p + 1)).mask().true_count() == 1).collect::<Vec<_>>()
        };
        assert_eq!(picked(&[2, 5], Some((1, 8)), None), vec![1, 3, 4, 6, 7]);
        assert_eq!(picked(&[3, 12], None, None), vec![0, 1, 2, 4, 5, 6, 7, 8, 9]);
        assert_eq!(picked(&[], None, Some(&[0, 9, 12])), vec![0, 9], "a place past its rows is none of them");
    }
}
