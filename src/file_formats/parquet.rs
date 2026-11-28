//! Parquet's entry: the format every table's files are in today, and the default.

use super::{BlockIndex, Blocks, Columns, FileStats, Finished, OpenFile, Rows, Stored, TableFileFormat, TableWriter, Take};
use anyhow::Context;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::statistics::Statistics;
use futures::TryStreamExt;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, ParquetRowSelection};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::parquet::file::metadata::PageIndexPolicy;
use datafusion::physical_plan::ExecutionPlan;
use futures::future::{BoxFuture, FutureExt};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use std::sync::Arc;
use anyhow::Result;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::properties::WriterProperties;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Parquet;

/// The entry (the engine registers it: `engine.rs`).
pub const PARQUET: Parquet = Parquet;

impl TableFileFormat for Parquet {
    fn name(&self) -> &'static str { "parquet" }
    fn ending(&self) -> &'static str { "parquet" }
    fn writer(&self, schema: SchemaRef, keys: &[String]) -> Result<Box<dyn TableWriter>> { Ok(Box::new(Writer(writer(schema, keys)?))) }
    fn reader(&self, state: &dyn Session) -> Arc<dyn FileFormat> { Arc::new(ParquetFormat::default().with_options(state.default_table_options().parquet)) }
    /// (Parquet's row selection: the pages and row groups no selected row is in are skipped unread)
    fn select(&self, file: PartitionedFile, rows: &Rows) -> Option<PartitionedFile> {
        Some(file.with_extension(ParquetRowSelection::new(selection(rows))))
    }
    fn take(&self) -> Option<&dyn Take> { Some(self) }
    fn filtering(&self, scan: &DataSourceExec) -> Option<Arc<dyn ExecutionPlan>> {
        let (config, parquet) = scan.downcast_to_file_source::<ParquetSource>()?;
        if parquet.table_parquet_options().global.pushdown_filters {
            return None;
        }
        let parquet = parquet.clone().with_pushdown_filters(true).with_reorder_filters(true);
        Some(DataSourceExec::from_data_source(FileScanConfigBuilder::from(config.clone()).with_source(Arc::new(parquet)).build()))
    }
    fn blocks(&self) -> Option<&dyn Blocks> { Some(self) }
    fn columns(&self) -> Option<&dyn Columns> { Some(self) }
    fn numbers(&self) -> bool { true }
}

/// A file's blocks are its row groups, its index its footer: a row group's min and max say whether it may hold a key.
impl Blocks for Parquet {
    fn index<'a>(&'a self, file: &'a Stored) -> BoxFuture<'a, Result<Arc<dyn BlockIndex>>> {
        async move { Ok(Arc::new(Footer(ArrowReaderMetadata::load_async(&mut Reader::of(file), ArrowReaderOptions::new()).await?)) as Arc<dyn BlockIndex>) }.boxed()
    }
}

/// A file's footer, as a lookup keeps it.
struct Footer(ArrowReaderMetadata);

impl BlockIndex for Footer {
    fn holding(&self, column: &str, key: &ScalarValue) -> Result<Vec<usize>> {
        let meta = self.0.metadata();
        let col = meta.file_metadata().schema_descr().columns().iter().position(|c| c.name() == column).context("key column not in file")?;
        Ok((0..meta.num_row_groups()).filter(|g| may_hold(meta.row_group(*g).column(col).statistics(), key)).collect())
    }

    fn read<'a>(&'a self, file: &'a Stored, b: usize) -> BoxFuture<'a, Result<RecordBatch>> {
        async move {
            let rows = self.0.metadata().row_group(b).num_rows() as usize;
            let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(Reader::of(file), self.0.clone()).with_row_groups(vec![b]).with_batch_size(rows.max(1)).build()?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            Ok(datafusion::arrow::compute::concat_batches(self.0.schema(), &batches)?)
        }
        .boxed()
    }
}

/// Whether a row group's min/max can contain `key` (unknown types and missing statistics: yes).
fn may_hold(stats: Option<&Statistics>, key: &ScalarValue) -> bool {
    match (stats, key) {
        (Some(Statistics::Int64(s)), ScalarValue::Int64(Some(k))) => s.min_opt().is_none_or(|m| m <= k) && s.max_opt().is_none_or(|m| k <= m),
        (Some(Statistics::Int32(s)), ScalarValue::Int32(Some(k))) => s.min_opt().is_none_or(|m| m <= k) && s.max_opt().is_none_or(|m| k <= m),
        (Some(Statistics::ByteArray(s)), ScalarValue::Utf8(Some(k))) => {
            s.min_opt().is_none_or(|m| m.data() <= k.as_bytes()) && s.max_opt().is_none_or(|m| k.as_bytes() <= m.data())
        }
        _ => true,
    }
}

/// A file, read by byte range where it is.
struct Reader(Arc<dyn datafusion::object_store::ObjectStore>, datafusion::object_store::path::Path, u64);

impl Reader {
    fn of(f: &Stored) -> Self { Reader(f.store.clone(), f.path.clone(), f.bytes) }
}

impl AsyncFileReader for Reader {
    fn get_bytes(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, datafusion::parquet::errors::Result<bytes::Bytes>> {
        use datafusion::object_store::ObjectStoreExt;
        async move { self.0.get_range(&self.1, range).await.map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e))) }.boxed()
    }

    fn get_metadata<'a>(&'a mut self, _: Option<&'a ArrowReaderOptions>) -> BoxFuture<'a, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let size = self.2;
        async move { Ok(Arc::new(datafusion::parquet::file::metadata::ParquetMetaDataReader::new().load_and_finish(self, size).await?)) }.boxed()
    }
}

/// A file's columns are read from its bytes as the table is read: strings straight to views (see `read_schema`).
impl Columns for Parquet {
    fn open(&self, bytes: bytes::Bytes) -> Result<Box<dyn OpenFile>> {
        use datafusion::arrow::datatypes::DataType;
        let file = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?.schema().clone();
        let views = file.fields().iter().map(|f| match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Arc::new(f.as_ref().clone().with_data_type(DataType::Utf8View)),
            _ => f.clone(),
        });
        let hint = Arc::new(Schema::new_with_metadata(views.collect::<Vec<_>>(), file.metadata().clone()));
        Ok(Box::new(Open(bytes.clone(), ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::new().with_schema(hint))?)))
    }
}

/// A file's bytes and their footer.
struct Open(bytes::Bytes, ArrowReaderMetadata);

impl OpenFile for Open {
    fn schema(&self) -> SchemaRef { self.1.schema().clone() }

    fn estimate(&self, columns: &[usize]) -> usize { estimate(self.1.metadata(), &ProjectionMask::roots(self.1.parquet_schema(), columns.iter().copied())) }

    fn read(&self, columns: &[usize], batch: usize) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>> {
        let b = ParquetRecordBatchReaderBuilder::new_with_metadata(self.0.clone(), self.1.clone());
        let mask = ProjectionMask::roots(b.parquet_schema(), columns.iter().copied());
        Ok(Box::new(b.with_projection(mask).with_batch_size(batch).build()?.map(|b| b.map_err(anyhow::Error::from))))
    }
}

/// About the bytes the columns `mask` takes decode to, from the file's footer: each column chunk's uncompressed bytes,
/// and at least its rows times what a value takes once decoded (a dictionary's indices decode to whole values; a string
/// to a 16-byte view beside its bytes).
fn estimate(meta: &ParquetMetaData, mask: &ProjectionMask) -> usize {
    use datafusion::parquet::basic::Type;
    let columns = meta.file_metadata().schema_descr().columns();
    let mut bytes = 0usize;
    for g in meta.row_groups() {
        let rows = g.num_rows().max(0) as usize;
        for (i, c) in columns.iter().enumerate().filter(|(i, _)| mask.leaf_included(*i)) {
            let raw = g.column(i).uncompressed_size().max(0) as usize;
            bytes += match c.physical_type() {
                Type::BYTE_ARRAY => raw + rows * 16,
                Type::BOOLEAN => raw.max(rows / 8),
                Type::INT32 | Type::FLOAT => raw.max(rows * 4),
                Type::INT64 | Type::DOUBLE => raw.max(rows * 8),
                Type::INT96 => raw.max(rows * 16), // (read as a timestamp of 8 bytes, or 12 kept: the larger)
                Type::FIXED_LEN_BYTE_ARRAY => raw.max(rows * c.type_length().max(1) as usize),
            };
        }
    }
    bytes
}

impl Take for Parquet {
    fn again(&self, scan: &DataSourceExec) -> Option<(FileScanConfig, Arc<dyn ExecutionPlan>)> {
        let (files, parquet) = scan.downcast_to_file_source::<ParquetSource>()?;
        if files.file_groups.iter().flat_map(|g| g.files()).any(|f| f.extension::<ParquetAccessPlan>().is_some()) {
            return None; // (a row selection could not join a file's own access plan)
        }
        let all = DataSourceExec::from_data_source(FileScanConfigBuilder::from(files.clone()).with_source(Arc::new(parquet.with_predicate(datafusion::physical_expr::expressions::lit(true)))).build());
        Some((files.clone(), all))
    }

    fn at(&self, ctx: Arc<TaskContext>, files: FileScanConfig, file: PartitionedFile, rows: Vec<usize>) -> BoxFuture<'static, datafusion::error::Result<Arc<dyn ExecutionPlan>>> {
        async move {
            let store = ctx.runtime_env().object_store(&files.object_store_url)?;
            let cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
            // (with its page index, so only the rows' pages are read: in the cache once read)
            let meta = DFParquetMetadata::new(store.as_ref(), &file.object_meta).with_file_metadata_cache(Some(cache)).with_page_index_policy(Some(PageIndexPolicy::Optional)).fetch_metadata().await?;
            let selection = RowSelection::from_consecutive_ranges(rows.iter().map(|r| *r..*r + 1), meta.file_metadata().num_rows() as usize);
            let mut file = file.with_extension(ParquetRowSelection::new(selection));
            file.range = None;
            Ok(DataSourceExec::from_data_source(FileScanConfigBuilder::from(files).with_file_groups(vec![FileGroup::new(vec![file])]).build()) as Arc<dyn ExecutionPlan>)
        }
        .boxed()
    }
}

/// `rows` as a Parquet row selection.
fn selection(rows: &Rows) -> RowSelection {
    if let Some(at) = rows.only {
        return RowSelection::from_consecutive_ranges(at.iter().filter(|p| **p < rows.count).map(|p| *p as usize..*p as usize + 1), rows.count as usize);
    }
    let kept = kept(rows.gone, rows.count);
    match rows.range {
        Some((from, to)) => kept.intersection(&RowSelection::from_consecutive_ranges(std::iter::once(from as usize..to.min(rows.count) as usize), rows.count as usize)),
        None => kept,
    }
}

/// The rows a file keeps, as a row selection: all of its `rows` but the positions `gone`.
fn kept(gone: &[u64], rows: u64) -> RowSelection {
    let (mut out, mut at) = (vec![], 0u64);
    for &g in gone.iter().filter(|g| **g < rows) {
        if g > at {
            out.push(RowSelector::select((g - at) as usize));
        }
        if g >= at {
            out.push(RowSelector::skip(1));
            at = g + 1;
        }
    }
    if rows > at {
        out.push(RowSelector::select((rows - at) as usize));
    }
    out.into()
}

struct Writer(ArrowWriter<Vec<u8>>);

impl TableWriter for Writer {
    fn write(&mut self, b: &RecordBatch) -> Result<()> { Ok(self.0.write(b)?) }
    fn bytes(&self) -> usize { self.0.bytes_written() + self.0.in_progress_size() }
    fn finish(mut self: Box<Self>) -> Result<Finished> {
        let footer = self.0.finish()?; // (its statistics: the file's min and max, without a second pass)
        Ok(Finished { bytes: std::mem::take(self.0.inner_mut()), stats: Box::new(footer) })
    }
}

/// A footer's statistics (`manifest::from_footer`, `manifest::sizes`).
impl FileStats for ParquetMetaData {
    fn range(&self, schema: &Schema, name: &str) -> Option<(ScalarValue, ScalarValue)> { crate::manifest::from_footer(self, schema, name) }
    fn sizes(&self) -> BTreeMap<String, u64> { crate::manifest::sizes(self) }
}

/// A row group at most, as the writer counts it (`writer`).
pub(crate) const ROW_GROUP_BYTES: usize = 64 << 20;

/// Parquet writer. Keyed tables are written for lookups as well as scans — sorted by key (see
/// `latest_sql`), with a bloom filter per key column, and in small row groups and pages, so reading
/// one key touches one page instead of a million rows.
///
/// Every table's row groups hold at most `ROW_GROUP_BYTES` as the writer counts them (about 40 MB
/// written): a scan holds a row group's columns while it decodes it, so a million rows of a wide
/// table held hundreds of MB a partition (ClickBench's 105 columns on four cores: a top-10 of every
/// column peaked at 1.9 GB, and a node in a 1.6 GB container was killed; 1.0 GB in groups of this
/// size, and faster). A narrower table's million rows stay one group (TPC-H's lineitem: 34 MB).
pub(crate) fn writer(schema: SchemaRef, keys: &[String]) -> Result<ArrowWriter<Vec<u8>>> {
    let mut props = WriterProperties::builder().set_compression(codec()).set_max_row_group_bytes(Some(ROW_GROUP_BYTES));
    if !keys.is_empty() {
        props = props.set_max_row_group_row_count(Some(256 << 10));
    }
    for k in keys {
        props = props.set_column_bloom_filter_enabled(k.as_str().into(), true);
    }
    // System columns: row ids count up, deltas of a few bits each; versions and times repeat per
    // commit, and plain values the codec squeezes are what costs least to write (the files come
    // out the size they were without them).
    for c in crate::text::NAMES.iter().filter(|c| schema.index_of(c).is_ok()) {
        let path: datafusion::parquet::schema::types::ColumnPath = (*c).into();
        let encoding = if *c == crate::text::ROW_ID { datafusion::parquet::basic::Encoding::DELTA_BINARY_PACKED } else { datafusion::parquet::basic::Encoding::PLAIN };
        props = props.set_column_dictionary_enabled(path.clone(), false).set_column_encoding(path, encoding);
    }
    Ok(ArrowWriter::try_new_with_options(vec![], schema, plain(props.build()))?)
}

/// Writer options with the Parquet file's own types only: no Arrow schema in its footer, which would
/// have other engines' Arrow read strings as views (`string_view`), which PyArrow can't yet take
/// rows of — PyIceberg applying a position delete, say.
pub fn plain(props: WriterProperties) -> datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions {
    datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions::new().with_properties(props).with_skip_arrow_metadata(true)
}

/// The codec data files are written with (`PONDRA_CODEC`). LZ4 by default: it decodes fastest,
/// so scans are CPU-cheap (TPC-H runs 13-20% faster than on ZSTD, with files a third bigger).
/// `zstd` where storage or bandwidth costs more than CPU; `snappy` or `none` also work.
pub(crate) fn codec() -> Compression {
    match crate::env::get("PONDRA_CODEC").unwrap_or_default().to_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::try_new(1).expect("a valid level")),
        "snappy" => Compression::SNAPPY,
        "none" => Compression::UNCOMPRESSED,
        _ => Compression::LZ4_RAW,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file's rows by their places, as Parquet's row selection: those its deletes leave in its range, or those at places.
    #[test]
    fn rows_by_their_places_are_a_row_selection() {
        let picked = |gone: &[u64], range: Option<(u64, u64)>, only: Option<&[u64]>| {
            let s = selection(&Rows { count: 10, gone, range, only });
            s.iter().fold((0, vec![]), |(at, mut out), r| {
                if !r.skip {
                    out.extend(at..at + r.row_count);
                }
                (at + r.row_count, out)
            }).1
        };
        assert_eq!(picked(&[2, 5], Some((1, 8)), None), vec![1, 3, 4, 6, 7]);
        assert_eq!(picked(&[3, 12], None, None), vec![0, 1, 2, 4, 5, 6, 7, 8, 9]);
        assert_eq!(picked(&[], None, Some(&[0, 9, 12])), vec![0, 9], "a place past its rows is none of them");
    }
}
