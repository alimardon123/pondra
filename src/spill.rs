//! Shuffle buckets that don't have to fit in memory.
//!
//! A step's output for another node is held in memory while it is small and written to this
//! node's scratch disk beyond that — Arrow IPC, one file per piece (`PONDRA_SPILL_MB`). The next
//! step reads it back a piece at a time, here or over HTTP, so what a shuffle can move is bounded
//! by disk, not by memory. A piece is the unit everywhere: it is what is written and what crosses
//! the wire (length-prefixed, `Frames`); a partition of the next step reads a `chain` of them, in
//! node order, so the same query adds up the same rows the same way every time. Everything a job
//! wrote goes when the job does.
use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::{StreamExt, TryStreamExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Rows held in memory before they go to disk (`PONDRA_SPILL_MB`, 64 by default).
fn piece() -> usize {
    std::env::var("PONDRA_SPILL_MB").ok().and_then(|m| m.parse::<usize>().ok()).unwrap_or(64) << 20
}

/// Where a node keeps what it spills (`--cache-dir`, else the temp dir): one folder per job and
/// node. The node's own id is in the path because two nodes on one machine share a cache dir, and
/// they name their buckets the same way — without it they would write over each other's rows.
pub fn dir(job: &str) -> PathBuf {
    static ME: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let me = ME.get_or_init(|| uuid::Uuid::new_v4().as_simple().to_string()[..8].to_string());
    let base = std::env::var("PONDRA_CACHE_DIR").map(PathBuf::from).unwrap_or_else(|_| std::env::temp_dir().join("pondra-cache"));
    base.join("shuffle").join(match job.is_empty() {
        true => String::new(), // (the folder itself: sweeping and measuring)
        false => format!("{job}-{me}"),
    })
}

/// One node's rows for one other node, in pieces. Cloning one costs nothing (the pieces are
/// files, or batches behind an Arc), so a bucket can be read again if a step has to be retried;
/// the files go when the job does.
#[derive(Default, Clone)]
pub struct Spill {
    dir: PathBuf,
    name: String,
    schema: Option<SchemaRef>,
    rows: Vec<RecordBatch>, // the piece being filled
    bytes: usize,
    files: Vec<PathBuf>, // the pieces already written
}

impl Spill {
    /// A bucket that writes its pieces into `dir` as `{name}-{n}.arrow`.
    pub fn new(dir: PathBuf, name: String) -> Spill {
        Spill { dir, name, ..Default::default() }
    }

    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = compact(batch)?;
        self.schema.get_or_insert_with(|| batch.schema());
        self.bytes += size(&batch);
        self.rows.push(batch);
        if self.bytes >= piece() {
            self.write()?;
        }
        Ok(())
    }

    /// Write what is in memory as the next piece.
    fn write(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.dir)?;
        let path = self.dir.join(format!("{}-{}.arrow", self.name, self.files.len()));
        std::fs::write(&path, crate::log::encode_ipc(&self.rows)?)?;
        crate::metrics::add(&crate::metrics::SPILLED, self.bytes as u64);
        (self.rows, self.bytes) = (vec![], 0);
        self.files.push(path);
        Ok(())
    }

    /// Its pieces in the order they were made: the files, then what is still in memory.
    fn parts(self) -> Vec<Piece> {
        let rows = (!self.rows.is_empty()).then_some(Piece::Rows(self.rows));
        self.files.into_iter().map(Piece::File).chain(rows).collect()
    }

    /// The pieces as bytes, each with its length before it (what `GET /cluster/shuffle` sends and
    /// `receive` reads back). Files are read as they are sent, one at a time.
    pub fn framed(self) -> impl futures::Stream<Item = Result<Vec<u8>, std::io::Error>> {
        futures::stream::iter(self.parts()).then(|piece| async move {
            let bytes = match piece {
                Piece::File(path) => tokio::fs::read(path).await?,
                Piece::Rows(rows) => crate::log::encode_ipc(&rows).map_err(std::io::Error::other)?,
            };
            Ok(frame(&bytes))
        })
    }

    /// How many bytes it holds (what it wrote, plus what is still in memory).
    pub fn bytes(&self) -> u64 {
        self.files.iter().filter_map(|p| std::fs::metadata(p).ok()).map(|m| m.len()).sum::<u64>() + self.bytes as u64
    }

    /// The rows' schema, once something has been put in (None while it is empty).
    pub fn schema(&self) -> Option<SchemaRef> { self.schema.clone() }

    /// How many pieces it holds (what goes on the wire before them).
    pub fn count(&self) -> usize { self.files.len() + usize::from(!self.rows.is_empty()) }

    /// Everything another bucket holds, added to this one: what several threads filled in
    /// parallel becomes the one bucket a node fetches. Two small ones stay in memory.
    pub fn absorb(&mut self, mut other: Spill) -> Result<()> {
        if other.files.is_empty() {
            return other.rows.drain(..).try_for_each(|b| self.push(b));
        }
        self.write()?; // (ours first: a bucket's pieces keep the order they were made in)
        other.write()?;
        self.files.extend(other.files);
        self.schema = self.schema.take().or(other.schema);
        Ok(())
    }

    /// The next `n` pieces of a stream that carries several buckets one after another.
    pub async fn take(dir: PathBuf, name: String, frames: &mut Frames<impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin>, n: usize) -> Result<Spill> {
        let mut spill = Spill::new(dir, name);
        for _ in 0..n {
            spill.keep(&frames.next().await?.context("the stream ended early")?)?;
        }
        Ok(spill)
    }

    /// Keep one piece, as it came: written to disk, or in memory if it is the only one and small.
    fn keep(&mut self, piece: &[u8]) -> Result<()> {
        if piece.is_empty() {
            return Ok(());
        }
        crate::metrics::add(&crate::metrics::WIRE, piece.len() as u64);
        if self.files.is_empty() && piece.len() < self::piece() / 8 && self.rows.is_empty() {
            self.rows = crate::log::decode(piece)?;
            self.bytes = self.rows.iter().map(size).sum();
            self.schema = self.rows.first().map(|b| b.schema());
            return Ok(());
        }
        self.write()?; // (anything already in memory becomes a piece of its own)
        std::fs::create_dir_all(&self.dir)?;
        let path = self.dir.join(format!("{}-{}.arrow", self.name, self.files.len()));
        std::fs::write(&path, piece)?;
        self.files.push(path);
        Ok(())
    }
}

/// The bytes a batch's rows take — not the buffers they sit in, which a batch cut from a bigger
/// one shares with it, and a builder leaves half empty.
fn size(b: &RecordBatch) -> usize {
    use datafusion::arrow::array::Array;
    b.columns().iter().map(|c| c.to_data().get_slice_memory_size().unwrap_or_else(|_| c.get_array_memory_size())).sum()
}

/// A batch with its string and binary views copied out of the buffers they share with the batch
/// they were cut from. A shuffle cuts every batch into a piece per node and partition; without
/// this, each piece would carry — and write to disk, and send — all of the original's strings
/// (TPC-H q21 wrote 2.5 GB a node instead of a tenth of that).
fn compact(b: RecordBatch) -> Result<RecordBatch> {
    use datafusion::arrow::array::{ArrayRef, AsArray};
    use datafusion::arrow::datatypes::DataType;
    if !b.columns().iter().any(|c| matches!(c.data_type(), DataType::Utf8View | DataType::BinaryView)) {
        return Ok(b);
    }
    let columns = b.columns().iter().map(|c| match c.data_type() {
        DataType::Utf8View => Arc::new(c.as_string_view().gc()) as ArrayRef,
        DataType::BinaryView => Arc::new(c.as_binary_view().gc()) as ArrayRef,
        _ => c.clone(),
    });
    Ok(RecordBatch::try_new(b.schema(), columns.collect())?)
}

/// One piece with its length before it, as `framed` sends them.
pub fn frame(bytes: &[u8]) -> Vec<u8> {
    let mut out = (bytes.len() as u32).to_le_bytes().to_vec();
    out.extend(bytes);
    out
}

/// A stream of length-prefixed pieces, read one at a time — several buckets can follow each other
/// on one connection, each preceded by a frame saying how many pieces it has.
pub struct Frames<S> {
    body: S,
    buf: Vec<u8>,
}

impl<S: futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin> Frames<S> {
    pub fn new(body: S) -> Frames<S> {
        Frames { body, buf: vec![] }
    }

    /// The next piece, or None where the stream ends.
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if self.buf.len() >= 4 {
                let n = u32::from_le_bytes(self.buf[..4].try_into()?) as usize;
                if self.buf.len() >= 4 + n {
                    let piece = self.buf[4..4 + n].to_vec();
                    self.buf.drain(..4 + n);
                    return Ok(Some(piece));
                }
            }
            match self.body.next().await {
                Some(chunk) => self.buf.extend_from_slice(&chunk?),
                None => {
                    anyhow::ensure!(self.buf.is_empty(), "a piece was cut short");
                    return Ok(None);
                }
            }
        }
    }

    /// The next piece read as a count.
    pub async fn count(&mut self) -> Result<usize> {
        let b = self.next().await?.context("the stream ended early")?;
        Ok(u32::from_le_bytes(b[..].try_into()?) as usize)
    }
}

/// One piece: a file, or rows small enough to have stayed in memory.
#[derive(Debug, Clone)]
enum Piece {
    File(PathBuf),
    Rows(Vec<RecordBatch>),
}

/// Buckets read one after another as one partition, in the order given. Every node lists what
/// it received in node order, so a query adds up the same rows in the same order each time it
/// runs — the same floating-point sum, not one a last digit away (which is all it takes for
/// `x = (SELECT max(x) …)` to find nothing).
pub fn chain(buckets: Vec<Spill>, schema: SchemaRef) -> Arc<dyn PartitionStream> {
    Arc::new(Chain { pieces: buckets.into_iter().flat_map(Spill::parts).collect(), schema })
}

#[derive(Debug)]
struct Chain {
    pieces: Vec<Piece>,
    schema: SchemaRef,
}

impl PartitionStream for Chain {
    fn schema(&self) -> &SchemaRef { &self.schema }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let read = |piece: Piece| async move {
            let rows = match piece {
                Piece::File(path) => crate::log::decode(&tokio::fs::read(path).await?)?,
                Piece::Rows(rows) => rows,
            };
            Ok::<_, anyhow::Error>(futures::stream::iter(rows.into_iter().map(Ok)))
        };
        let stream = futures::stream::iter(self.pieces.clone()).then(read).try_flatten();
        let stream = stream.map(|r: Result<RecordBatch, anyhow::Error>| r.map_err(|e| datafusion::error::DataFusionError::External(e.into())));
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Held while a job's spilled results are being sent: what it wrote goes when it is dropped,
/// whether the stream finished or the reader walked away.
pub struct Gone(pub String);

impl Drop for Gone {
    fn drop(&mut self) { clear(&self.0) }
}

/// Everything a job spilled here.
pub fn clear(job: &str) {
    let _ = std::fs::remove_dir_all(dir(job));
}

/// What earlier runs left behind (a node that died mid-shuffle), older than an hour.
pub fn sweep() {
    let base = dir("");
    let Ok(entries) = std::fs::read_dir(&base) else { return };
    for e in entries.flatten() {
        let old = e.metadata().and_then(|m| m.modified()).map(|t| t.elapsed().unwrap_or_default().as_secs() > 3600);
        if old.unwrap_or(false) {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

/// Bytes under this node's shuffle scratch (for `/metrics`).
pub fn held() -> u64 {
    fn walk(p: &Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(p) else { return 0 };
        entries.flatten().map(|e| match e.path().is_dir() {
            true => walk(&e.path()),
            false => e.metadata().map(|m| m.len()).unwrap_or(0),
        }).sum()
    }
    walk(&dir(""))
}
