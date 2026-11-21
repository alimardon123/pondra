//! Parquet's entry: the format every table's files are in today, and the default.

use super::{FileStats, Finished, Rows, TableFileFormat, TableWriter};
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
        Some(file.with_extension(datafusion::datasource::physical_plan::parquet::ParquetRowSelection::new(selection(rows))))
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
