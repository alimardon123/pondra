//! The registry's file formats section (redesign §6, step 5 piece 15; `designs/redesign/table-file-formats.md`): what a
//! table's data files are written with. Parquet is the one built in (`parquet.rs`); another format (Vortex, Lance) is one
//! more entry, so what writes a table's files (`tier::Encoding`, a page's staged INSERT) names no format's own types.
//! Reading (`reader`, `take`) and a file's statistics from bytes another engine wrote (`stats`, `copy_blocks`) move
//! behind the entry next (pieces 15b and 15c).

use anyhow::Result;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use std::collections::BTreeMap;

pub mod parquet;

/// One file format a table's data files may be kept in.
pub trait TableFileFormat: Sync {
    /// Its name: the word a table's `file_format` option says, and a file's entry when it isn't the default.
    fn name(&self) -> &'static str;
    /// What a file's name ends in, after its dot.
    fn ending(&self) -> &'static str;
    /// A file of rows of `schema`, written as they come. `keys`: the columns a row is looked up by (a format that can
    /// make a lookup cheap, such as Parquet's bloom filters and small row groups, does so for them).
    fn writer(&self, schema: SchemaRef, keys: &[String]) -> Result<Box<dyn TableWriter>>;
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
}
