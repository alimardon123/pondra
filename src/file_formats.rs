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
    /// Its files read a block at a time by their own index, if it can (`Blocks`): what a key lookup reads. None: a lookup
    /// can't read its files (`serve.rs`).
    fn blocks(&self) -> Option<&dyn Blocks> { None }
    /// Its files' columns decoded from their bytes one at a time, if it can (`Columns`): what the hot tier keeps in memory.
    /// None: its files aren't kept there (`hot.rs`).
    fn columns(&self) -> Option<&dyn Columns> { None }
    /// Whether a scan of its files gives each row's place in its file (`place`: what a lake's own row ids are read by,
    /// `lake_scan.rs`). false: such a scan of its files is refused, naming it.
    fn numbers(&self) -> bool { false }
}

/// A file's bytes where they are, read by range: a lookup reads only a file's index and the blocks it needs.
pub struct Stored {
    pub store: Arc<dyn datafusion::object_store::ObjectStore>,
    pub path: datafusion::object_store::path::Path,
    pub bytes: u64, // its size
}

/// A file read a block at a time by its format's own index (`serve.rs`: a key's newest row, block by block).
pub trait Blocks: Sync + std::fmt::Debug {
    /// `file`'s index: what a lookup keeps of a file, once read (files never change).
    fn index<'a>(&'a self, file: &'a Stored) -> BoxFuture<'a, Result<Arc<dyn BlockIndex>>>;
}

/// A file's blocks, as its index tells them.
pub trait BlockIndex: Send + Sync {
    /// The blocks that may hold `key` in column `column`, in the file's order (a block its index says nothing of: may).
    fn holding(&self, column: &str, key: &ScalarValue) -> Result<Vec<usize>>;
    /// Block `b`'s rows, decoded.
    fn read<'a>(&'a self, file: &'a Stored, b: usize) -> BoxFuture<'a, Result<RecordBatch>>;
}

/// A file's columns decoded from its bytes (`hot.rs`: a file's columns kept in memory, decoded one at a time).
pub trait Columns: Sync + std::fmt::Debug {
    /// `bytes`, a whole file, open for its columns: strings read as views (no copy of their bytes).
    fn open(&self, bytes: bytes::Bytes) -> Result<Box<dyn OpenFile>>;
}

/// A file open for its columns.
pub trait OpenFile: Send {
    /// Its columns.
    fn schema(&self) -> SchemaRef;
    /// About the bytes its columns at `columns` (places in `schema`) decode to.
    fn estimate(&self, columns: &[usize]) -> usize;
    /// Its columns at `columns`, in batches of `batch` rows (no columns: only how many rows each batch has).
    fn read(&self, columns: &[usize], batch: usize) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>>;
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
    use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
    anyhow::ensure!(!paths.is_empty(), "no files to read");
    let state = ctx.state();
    let urls = paths.iter().map(ListingTableUrl::parse).collect::<datafusion::error::Result<Vec<_>>>()?;
    let config = ListingTableConfig::new_with_multi_paths(urls).with_listing_options(listing(format, &state));
    let config = match schema {
        Some(s) => config.with_schema(s),
        None => config.infer_schema(&state).await?,
    };
    Ok(ctx.read_table(Arc::new(ListingTable::try_new(config)?))?)
}

/// How a listing of files of `format` reads them: as `read_parquet` reads Parquet ones (`ParquetReadOptions`'s).
pub fn listing(format: &dyn TableFileFormat, state: &dyn Session) -> datafusion::datasource::listing::ListingOptions {
    datafusion::datasource::listing::ListingOptions::new(format.reader(state)).with_file_extension(format!(".{}", format.ending()))
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

/// The format a table's new files are written in: Parquet, until a table's `file_format` names another (piece 15c-2).
pub fn default() -> &'static dyn TableFileFormat { parquet() }

/// Parquet's entry: the registry's, or the one built in here (a registry without the engine's entries).
pub fn parquet() -> &'static dyn TableFileFormat { named(parquet::PARQUET.name()).unwrap_or(&parquet::PARQUET) }

/// The format file `f` is in, as its entry says (none: Parquet). One this build doesn't have is refused, naming it.
pub fn of(f: &crate::data_file::DataFile) -> Result<&'static dyn TableFileFormat> {
    match f.format.as_str() {
        "" => Ok(parquet()),
        name => named(name).ok_or_else(|| anyhow::anyhow!("{}: a file in the {name} format, which this build doesn't have", f.path)),
    }
}

/// The format a table's new files are written in, as its entry says (`TableMeta::file_format`; none: Parquet).
pub fn table(file_format: &str) -> Result<&'static dyn TableFileFormat> {
    match file_format {
        "" => Ok(default()),
        name => chosen(name),
    }
}

/// The format `file_format = '…'` names (any case), refused naming the ones this build has when it has no such format.
pub fn chosen(name: &str) -> Result<&'static dyn TableFileFormat> {
    let name = name.trim().to_lowercase();
    named(&name).ok_or_else(|| anyhow::anyhow!("file_format: this build has no {name} format (it has {})", all().map(|f| f.name()).collect::<Vec<_>>().join(", ")))
}

/// What a file's entry says of its format (`DataFile::format`): nothing for Parquet.
pub fn recorded(format: &dyn TableFileFormat) -> String {
    match format.name() == parquet::PARQUET.name() {
        true => String::new(),
        false => format.name().to_string(),
    }
}

/// The column of each row's place in its file, under `name`: a scan's virtual column, last (`Files::row`), which an entry
/// that `numbers` its rows fills. It is marked as Parquet's row number, the one mark DataFusion's readers know.
pub fn place(name: &str) -> datafusion::arrow::datatypes::FieldRef {
    Arc::new(datafusion::arrow::datatypes::Field::new(name, datafusion::arrow::datatypes::DataType::Int64, false).with_extension_type(datafusion::parquet::arrow::RowNumber))
}

/// Whether other engines read `f` when a table is published (Delta, Iceberg): a file in Parquet, the one format both
/// name and every reader reads. A table's file_format refuses publishing (`delta::refused`); this holds out the files a
/// table kept from before its format changed back to Parquet: its publish fails, naming one, while it has any.
pub fn published(f: &crate::data_file::DataFile) -> Result<()> {
    match f.format.is_empty() {
        true => Ok(()),
        false => Err(anyhow::anyhow!("{}: a file in the {} format, which other engines don't read: the table publishes again once its files are all Parquet", f.path, f.format)),
    }
}

/// `files` by their formats, in the order each format is first met (one group for a table whose files share one).
pub fn by_format<'a>(files: impl IntoIterator<Item = &'a crate::data_file::DataFile>) -> Result<Vec<(&'static dyn TableFileFormat, Vec<&'a crate::data_file::DataFile>)>> {
    let mut out: Vec<(&'static dyn TableFileFormat, Vec<&crate::data_file::DataFile>)> = vec![];
    for f in files {
        let format = of(f)?;
        match out.iter_mut().find(|(g, _)| g.name() == format.name()) {
            Some((_, group)) => group.push(f),
            None => out.push((format, vec![f])),
        }
    }
    Ok(out)
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

    /// A file's entry names its format only when it isn't Parquet; one this build lacks is refused, naming it; files are
    /// grouped by their formats in the order each is first met.
    #[test]
    fn a_files_format_is_its_entrys() {
        let file = |path: &str, format: &str| crate::data_file::DataFile { path: path.into(), format: format.into(), ..Default::default() };
        let (a, b, c) = (file("a", ""), file("b", "vortex"), file("c", ""));
        assert_eq!(of(&a).unwrap().name(), "parquet");
        assert_eq!(recorded(of(&a).unwrap()), "", "Parquet goes unsaid");
        let err = of(&b).err().expect("no vortex here").to_string();
        assert!(err.contains("b: a file in the vortex format"), "{err}");
        let groups = by_format([&a, &c]).unwrap();
        assert_eq!(groups.iter().map(|(f, g)| (f.name(), g.iter().map(|f| f.path.as_str()).collect::<Vec<_>>())).collect::<Vec<_>>(), [("parquet", vec!["a", "c"])]);
        assert!(by_format([&a, &b]).is_err());
        assert!(!serde_json::to_string(&a).unwrap().contains("format"), "an entry written as before");
        assert!(published(&a).is_ok());
        let held = published(&b).err().expect("held out of a publish").to_string();
        assert!(held.contains("b: a file in the vortex format, which other engines don't read"), "{held}");
    }

    /// Parquet under another name: a second format, for a table whose files are in two.
    #[derive(Debug)]
    struct Renamed;

    impl TableFileFormat for Renamed {
        fn name(&self) -> &'static str { "renamed" }
        fn ending(&self) -> &'static str { "renamed" }
        fn writer(&self, schema: SchemaRef, keys: &[String]) -> Result<Box<dyn TableWriter>> { parquet::PARQUET.writer(schema, keys) }
        fn reader(&self, state: &dyn Session) -> Arc<dyn FileFormat> { parquet::PARQUET.reader(state) }
        fn select(&self, _: PartitionedFile, _: &Rows) -> Option<PartitionedFile> { None }
    }

    /// A scan of files in two formats is one scan of each format's, side by side, with every file's rows; a file's rows
    /// read by their places in a format that can't is refused, naming it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_scan_of_two_formats_reads_each_by_its_own() {
        let dir = std::env::temp_dir().join(format!("pondra-two-formats-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let file = |from: i64, format: &'static dyn TableFileFormat, deleted: Option<Vec<u64>>| {
            let mut w = format.writer(schema.clone(), &[]).unwrap();
            w.write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from_iter_values(from..from + 10))]).unwrap()).unwrap();
            let path = dir.join(format!("{from}.{}", format.ending()));
            std::fs::write(&path, w.finish().unwrap().bytes).unwrap();
            let key = path.to_str().unwrap().trim_start_matches('/').to_string();
            let stats = Arc::new(datafusion::common::Statistics::new_unknown(&schema));
            crate::scan::File { key, size: std::fs::metadata(&path).unwrap().len(), rows: 10, partition: vec![], deleted: deleted.map(Arc::new), range: None, only: None, stats, modified: None, format }
        };
        let files_of = |files| crate::scan::Files { store: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(), file_schema: schema.clone(), partition: vec![], files, adapter: None, row: None };
        let ctx = datafusion::prelude::SessionContext::new();
        let two = ctx.read_table(Arc::new(files_of(vec![file(0, &parquet::PARQUET, None), file(10, &Renamed, None), file(20, &parquet::PARQUET, Some(vec![0, 1]))]))).unwrap();
        let plan = datafusion::physical_plan::displayable(two.clone().create_physical_plan().await.unwrap().as_ref()).indent(true).to_string();
        assert!(plan.contains("UnionExec") && plan.matches("DataSourceExec").count() == 2, "{plan}");
        assert_eq!(two.count().await.unwrap(), 28, "every file's rows but the two deleted");
        let refused = ctx.read_table(Arc::new(files_of(vec![file(30, &Renamed, Some(vec![3]))]))).unwrap().count().await.err().expect("refused").to_string();
        assert!(refused.contains("which this build's renamed can't"), "{refused}");
        // (each row's place in its file, as a row id reads it: from a format that numbers its rows; refused, naming the
        // format, from one that doesn't, but only when the scan reads it)
        use datafusion::arrow::array::AsArray;
        let placed = |files| ctx.read_table(Arc::new(crate::scan::Files { row: Some(place("__row")), ..files_of(files) })).unwrap();
        let rows = placed(vec![file(40, &parquet::PARQUET, Some(vec![2]))]).collect().await.unwrap();
        let at: Vec<i64> = rows.iter().flat_map(|b| b.column(1).as_primitive::<datafusion::arrow::datatypes::Int64Type>().values().to_vec()).collect();
        assert_eq!(at, [0, 1, 3, 4, 5, 6, 7, 8, 9], "each row's place, the deleted one's skipped");
        let refused = placed(vec![file(50, &Renamed, None)]).collect().await.err().expect("refused").to_string();
        assert!(refused.contains("each row's place in its file is read (a row id), which this build's renamed can't give"), "{refused}");
        assert_eq!(placed(vec![file(60, &Renamed, None)]).count().await.unwrap(), 10, "a scan that doesn't read the places reads it");
        std::fs::remove_dir_all(&dir).unwrap();
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
