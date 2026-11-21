//! The registry's file formats section (redesign §6, step 5 piece 15; `designs/redesign/table-file-formats.md`): what a
//! table's data files are written with. Parquet is the one built in (`parquet.rs`); another format (Vortex, Lance) is one
//! more entry, so what writes a table's files (`tier::Encoding`, a page's staged INSERT) and what scans them
//! (`scan::Files`, `read`) names no format's own types. What a plan does inside a format's scan is the format's too: rows
//! read again by their places (`take`: late rows) and filters applied as rows are decoded (`filtering`). Key lookups and
//! the hot tier's reads, and a file's statistics from bytes another engine wrote (`stats`, `copy_blocks`) move behind the
//! entry next (pieces 15b-3 and 15c).

use anyhow::Result;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::future::BoxFuture;
use std::sync::Arc;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use std::collections::BTreeMap;

pub mod parquet;

/// One file format a table's data files may be kept in.
pub trait TableFileFormat: Sync + std::fmt::Debug {
    /// Its name: the word a table's `file_format` option says, and a file's entry when it isn't the default.
    fn name(&self) -> &'static str;
    /// What a file's name ends in, after its dot.
    fn ending(&self) -> &'static str;
    /// A file of rows of `schema`, written as they come. `keys`: the columns a row is looked up by (a format that can
    /// make a lookup cheap, such as Parquet's bloom filters and small row groups, does so for them).
    fn writer(&self, schema: SchemaRef, keys: &[String]) -> Result<Box<dyn TableWriter>>;
    /// What a scan reads files of this format with: DataFusion's format, with the session's options.
    fn reader(&self, state: &dyn Session) -> Arc<dyn FileFormat>;
    /// `file`, to be read for only these of its rows. None: the format can't read rows by their places (a table whose
    /// files have deleted rows, or a scan of a file's part, is refused naming it).
    fn select(&self, file: PartitionedFile, rows: &Rows) -> Option<PartitionedFile>;
    /// Its rows read again by their places, if it can (`Take`). None: a top-N of a wide scan of its files reads every
    /// column as it sorts (`late_rows.rs`).
    fn take(&self) -> Option<&dyn Take> { None }
    /// `scan`, if it is this format's, filtering its rows as it decodes them (a filter's columns first, the others only
    /// for the rows it keeps), when it doesn't already: what a wide scan is read with (`optimize::late`). None: as it is.
    fn filtering(&self, _scan: &DataSourceExec) -> Option<Arc<dyn ExecutionPlan>> { None }
}

/// Rows read again by their places: what a top-N of a wide scan reads its rows' other columns with, once it has sorted
/// only its keys and each row's place (`late_rows.rs`).
pub trait Take: Sync + std::fmt::Debug {
    /// `scan`, if it is this format's and its files can be read again at places: its files, and the same scan with no
    /// filter. Each row's place is DataFusion's `file_row_index()`, which its scans give.
    fn again(&self, scan: &DataSourceExec) -> Option<(FileScanConfig, Arc<dyn ExecutionPlan>)>;
    /// `file`, one of `files`', read for only its rows at `rows` (sorted, each once): a scan of that file alone.
    fn at(&self, ctx: Arc<TaskContext>, files: FileScanConfig, file: PartitionedFile, rows: Vec<usize>) -> BoxFuture<'static, datafusion::error::Result<Arc<dyn ExecutionPlan>>>;
}

/// Every file format this build has.
pub fn all() -> impl Iterator<Item = &'static dyn TableFileFormat> {
    crate::registry::get().find::<FileFormats>().into_iter().flat_map(|f| f.0.iter().copied())
}

/// The rows of a file a scan reads, by their places in it: those its deletes leave, in its range; or those at `only`.
pub struct Rows<'a> {
    pub count: u64,                // the file's rows
    pub gone: &'a [u64],           // its deleted rows' places, sorted
    pub range: Option<(u64, u64)>, // only its rows from .0 to (not including) .1
    pub only: Option<&'a [u64]>,   // only its rows at these places, sorted
}

/// Files of `format` at `paths` (full URLs), as one read, as DataFusion's own `read_parquet` reads Parquet ones: `schema`
/// theirs, or none to take it from the first.
pub async fn read(ctx: &datafusion::prelude::SessionContext, format: &dyn TableFileFormat, paths: Vec<String>, schema: Option<SchemaRef>) -> Result<datafusion::prelude::DataFrame> {
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
    anyhow::ensure!(!paths.is_empty(), "no files to read");
    let state = ctx.state();
    let urls = paths.iter().map(ListingTableUrl::parse).collect::<datafusion::error::Result<Vec<_>>>()?;
    let options = ListingOptions::new(format.reader(&state)).with_file_extension(format!(".{}", format.ending()));
    let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(options);
    let config = match schema {
        Some(s) => config.with_schema(s),
        None => config.infer_schema(&state).await?,
    };
    Ok(ctx.read_table(Arc::new(ListingTable::try_new(config)?))?)
}

/// One file being written.
pub trait TableWriter: Send {
    fn write(&mut self, b: &RecordBatch) -> Result<()>;
    /// Its size so far, encoded (a file is cut by it).
    fn bytes(&self) -> usize;
    /// The file's bytes, finished, and what its format kept of its columns.
    fn finish(self: Box<Self>) -> Result<Finished>;
}

/// A finished file.
pub struct Finished {
    pub bytes: Vec<u8>,
    pub stats: Box<dyn FileStats + Send>,
}

/// What a file's format kept of its columns, as it wrote them: what the manifest's entry takes (`manifest::Seen`, `DataFile::sizes`).
pub trait FileStats {
    /// Column `name`'s min and max, when the format kept them exactly for every block that holds a value.
    fn range(&self, schema: &Schema, name: &str) -> Option<(ScalarValue, ScalarValue)>;
    /// Each column of no fixed width, its bytes before compression.
    fn sizes(&self) -> BTreeMap<String, u64>;
}

/// The registry's file formats section.
#[derive(Default)]
pub struct FileFormats(Vec<&'static dyn TableFileFormat>);

impl crate::registry::Section for FileFormats {
    const NAME: &'static str = "file formats";

    /// A format's name and its ending are lower case and no other format's (a file is known by its ending).
    fn check(&self, _: &crate::registry::Registry) -> Result<(), String> {
        let (mut names, mut endings) = (std::collections::HashSet::new(), std::collections::HashSet::new());
        for f in &self.0 {
            if f.name() != f.name().to_ascii_lowercase() || !names.insert(f.name()) {
                return Err(format!("{}: a file format's name is lower case and no other format's", f.name()));
            }
            if f.ending().is_empty() || f.ending() != f.ending().to_ascii_lowercase() || f.ending().contains('.') || !endings.insert(f.ending()) {
                return Err(format!("{}: a file format's ending is lower case, without a dot, and no other format's, not {:?}", f.name(), f.ending()));
            }
        }
        Ok(())
    }
}

impl FileFormats {
    pub fn add(&mut self, f: &'static dyn TableFileFormat) { self.0.push(f) }
}

/// The file format `name` names, if this build has it.
pub fn named(name: &str) -> Option<&'static dyn TableFileFormat> {
    crate::registry::get().find::<FileFormats>()?.0.iter().copied().find(|f| f.name() == name)
}

/// The format a table's new files are written in: Parquet, until a table's `file_format` names another (piece 15c).
pub fn default() -> &'static dyn TableFileFormat {
    named(parquet::PARQUET.name()).unwrap_or(&parquet::PARQUET) // (a registry without the engine's entries: Parquet is built in here)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    /// The engine's default is Parquet's entry, from the registry, and a file written through it reads back with its rows,
    /// its columns' ranges and sizes.
    #[test]
    fn parquet_is_the_default_and_its_file_keeps_ranges() {
        let f = default();
        assert_eq!((f.name(), f.ending()), ("parquet", "parquet"));
        assert!(named("parquet").is_some() && named("vortex").is_none());
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true), Field::new("s", DataType::Utf8, true)]));
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(3), None, Some(-2)])), Arc::new(StringArray::from(vec!["b", "a", "c"]))];
        let mut w = f.writer(schema.clone(), &[]).unwrap();
        w.write(&RecordBatch::try_new(schema.clone(), columns).unwrap()).unwrap();
        assert!(w.bytes() > 0);
        let done = w.finish().unwrap();
        assert_eq!(done.stats.range(&schema, "i"), Some((ScalarValue::Int64(Some(-2)), ScalarValue::Int64(Some(3)))));
        assert_eq!(done.stats.range(&schema, "s"), Some((ScalarValue::from("a"), ScalarValue::from("c"))));
        assert!(done.stats.sizes().contains_key("s") && !done.stats.sizes().contains_key("i"));
        let read = datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(done.bytes)).unwrap().build().unwrap();
        assert_eq!(read.map(|b| b.unwrap().num_rows()).sum::<usize>(), 3);
    }

    /// Files are read through the entry as `read_parquet` read them: their schema given, or taken from the first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn files_are_read_through_the_entry() {
        let dir = std::env::temp_dir().join(format!("pondra-file-formats-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (f, schema) = (default(), Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)])));
        let mut paths = vec![];
        for from in [0i64, 10] {
            let mut w = f.writer(schema.clone(), &[]).unwrap();
            w.write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from_iter_values(from..from + 10))]).unwrap()).unwrap();
            let path = dir.join(format!("{from}.{}", f.ending()));
            std::fs::write(&path, w.finish().unwrap().bytes).unwrap();
            paths.push(path.to_str().unwrap().to_string());
        }
        let ctx = datafusion::prelude::SessionContext::new();
        for given in [None, Some(schema.clone())] {
            assert_eq!(read(&ctx, f, paths.clone(), given).await.unwrap().count().await.unwrap(), 20);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
