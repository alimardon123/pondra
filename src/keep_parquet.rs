//! A Parquet file kept as it was written (designs/l2-load-keeps-parquet.md, K1): its row groups' column chunks are
//! copied, undecoded, into files of ours, which take our footer. `plan` cuts a file's row groups into kept files, or
//! says why the file isn't kept (in words; the file is then written as today, and no error reaches the user). `copy`
//! writes one cut as one file. `keep` does both for a whole file in memory, for the tests.

use crate::adopt::same;
use crate::text::{CREATED, NAMES, ROW_ID, UPDATED};
use anyhow::{ensure, Context, Result};
use bytes::{Buf, Bytes};
use datafusion::arrow::datatypes::Schema;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::basic::{ColumnOrder, Compression, Encoding, LogicalType, Repetition, SortOrder, TimeUnit, Type as Physical};
use datafusion::parquet::data_type::Int64Type;
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::parquet::column::page::PageReader;
use datafusion::parquet::column::writer::ColumnCloseResult;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
#[cfg(test)]
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::file::reader::{ChunkReader, Length};
use datafusion::parquet::file::serialized_reader::SerializedPageReader;
use datafusion::parquet::file::statistics::Statistics;
use datafusion::parquet::file::writer::SerializedFileWriter;
use datafusion::parquet::schema::types::{ColumnDescPtr, SchemaDescriptor, Type as Node, TypePtr};
use std::ops::Range;
use std::sync::Arc;

/// What a kept file may hold: `tier::FILE_BYTES` and `tier::ROW_GROUP_BYTES` (the tests: small).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub file: usize,
    pub group: usize,
}

/// The row groups of one kept file, and the source's bytes they are: from the first chunk's first byte to the last
/// chunk's last byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cut {
    pub groups: Range<usize>,
    pub bytes: Range<u64>,
}

/// One kept file: its bytes, its footer (ours, not the source's), and its rows.
#[derive(Debug)]
pub(crate) struct Kept {
    pub bytes: Bytes,
    pub footer: ParquetMetaData,
    pub rows: u64,
}

/// The room a file keeps for its footer and page indexes, out of `file` bytes.
fn room(file: usize) -> usize { file / 64 + (64 << 10) }

/// The cuts a file's row groups make, or why the file isn't kept. The table's columns are `table`: stored names and
/// types, as `answer::schema` gives them.
pub(crate) fn plan(md: &ParquetMetaData, table: &Schema, limits: &Limits) -> Result<Vec<Cut>, String> {
    if md.num_row_groups() == 0 {
        return Err("it has no row groups".into());
    }
    if md.file_metadata().num_rows() == 0 {
        return Err("it has no rows".into());
    }
    let descr = md.file_metadata().schema_descr();
    let fields = descr.root_schema().get_fields();
    let names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
    let wanted: Vec<&str> = table.fields().iter().map(|f| f.name().as_str()).collect();
    if names != wanted {
        return Err(format!("its columns ({}) aren't the table's ({}), by name and in order", names.join(", "), wanted.join(", ")));
    }
    if fields.is_empty() {
        return Err("it has no columns".into());
    }
    if let Some(f) = fields.iter().find(|f| !f.is_primitive()) {
        return Err(format!("column {} is nested (a list, struct or map), which isn't kept yet", f.name()));
    }
    let arrow = parquet_to_arrow_schema(descr, None).map_err(|e| format!("its schema doesn't read: {e}"))?;
    for (i, f) in fields.iter().enumerate() {
        if descr.column(i).physical_type() == Physical::INT96 {
            return Err(format!("column {} is INT96, which the table's types don't hold", f.name()));
        }
        let (got, want) = (arrow.field(i).data_type(), table.field(i).data_type());
        if !same(got, want) {
            return Err(format!("column {} is {got} in the file and {want} in the table", f.name()));
        }
    }
    let budget = limits.file.saturating_sub(room(limits.file)) as u64;
    let mut spans = Vec::with_capacity(md.num_row_groups());
    for (g, rg) in md.row_groups().iter().enumerate() {
        if rg.total_byte_size() as u64 > limits.group as u64 {
            return Err(format!("row group {g} is {} bytes uncompressed, over the {} a row group may be", rg.total_byte_size(), limits.group));
        }
        let (mut lo, mut hi) = (u64::MAX, 0u64);
        for (c, chunk) in rg.columns().iter().enumerate() {
            let name = fields[c].name();
            let codec = chunk.compression();
            if !matches!(codec, Compression::UNCOMPRESSED | Compression::SNAPPY | Compression::GZIP(_) | Compression::BROTLI(_) | Compression::ZSTD(_) | Compression::LZ4_RAW) {
                return Err(format!("column {name} in row group {g} is {codec:?}, a codec that isn't kept"));
            }
            let (a, b) = chunk_bytes(chunk).map_err(|why| format!("column {name} in row group {g} {why}"))?;
            (lo, hi) = (lo.min(a), hi.max(b));
        }
        if hi - lo > budget {
            return Err(format!("row group {g} is {} bytes in the file, more than a kept file holds ({budget})", hi - lo));
        }
        spans.push((lo, hi));
    }
    let mut cuts = Vec::new();
    let mut g = 0;
    while g < spans.len() {
        let first = g;
        let (mut lo, mut hi) = spans[g];
        g += 1;
        while g < spans.len() {
            let (a, b) = (lo.min(spans[g].0), hi.max(spans[g].1));
            if b - a > budget {
                break;
            }
            (lo, hi) = (a, b);
            g += 1;
        }
        cuts.push(Cut { groups: first..g, bytes: lo..hi });
    }
    Ok(cuts)
}

/// A chunk's bytes in the source, or why they can't be copied as they are: a chunk in another file, or whose first
/// page isn't where its offsets say (old writers', which `append_column` would copy from the wrong place).
fn chunk_bytes(c: &ColumnChunkMetaData) -> std::result::Result<(u64, u64), String> {
    if c.file_path().is_some() {
        return Err("is in another file".into());
    }
    let start = c.dictionary_page_offset().unwrap_or(c.data_page_offset());
    if start < 4 || c.data_page_offset() < 4 || c.compressed_size() < 0 || c.dictionary_page_offset().is_some_and(|d| d >= c.data_page_offset()) {
        return Err(format!("starts at offset {start}, not where its pages are"));
    }
    Ok((start as u64, start as u64 + c.compressed_size() as u64))
}

/// The system columns a written file of a bulk INSERT carries (`sys::stamp_new`), for one cut's rows: ids from `first`
/// on, and the commit number and time (µs) the writer reserved. With them a kept file is a plain file, read as a written
/// one is (no lineage: `hot::plain_file`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Sys {
    pub first: i64,
    pub version: i64,
    pub at_us: i64,
}

/// Writes one cut as one file of ours: its row groups' chunks copied as they are, under our schema (the table's names,
/// types as the source's, field ids 1..n), then, with `sys`, the four system columns encoded as `tier::writer` encodes
/// them. `piece` is the source's bytes of `cut.bytes`. Err if a chunk doesn't start with a page (encrypted, or a bad
/// cut), or if the file comes out over what `plan` leaves room for.
pub(crate) fn copy(md: &ParquetMetaData, table: &Schema, cut: &Cut, piece: Bytes, sys: Option<Sys>) -> Result<Kept> {
    let descr = md.file_metadata().schema_descr();
    let fields = descr.root_schema().get_fields();
    ensure!(table.fields().len() == fields.len(), "the table has {} columns, the file {}", table.fields().len(), fields.len());
    let len = piece.len();
    ensure!(len as u64 == cut.bytes.end - cut.bytes.start, "the piece is {len} bytes, the cut {}", cut.bytes.end - cut.bytes.start);
    let mut ours: Vec<TypePtr> = Vec::with_capacity(fields.len());
    for (i, f) in fields.iter().enumerate() {
        let info = f.get_basic_info();
        let field = Node::primitive_type_builder(table.field(i).name(), f.get_physical_type())
            .with_repetition(info.repetition())
            .with_logical_type(info.logical_type_ref().cloned())
            .with_converted_type(info.converted_type())
            .with_length(descr.column(i).type_length())
            .with_precision(f.get_precision())
            .with_scale(f.get_scale())
            .with_id(Some(i as i32 + 1))
            .build()?;
        ours.push(Arc::new(field));
    }
    let mut props = WriterProperties::builder().set_compression(crate::tier::codec());
    if md.offset_index().is_none() {
        // (a file's chunks all have an offset index or none do: a source with none, the system columns with none, and
        // ranges per chunk, not per page, which would ask for one)
        props = props.set_offset_index_disabled(true).set_statistics_enabled(EnabledStatistics::Chunk);
    }
    if sys.is_some() {
        // (as `tier::writer` writes them, with the ids `tier::field_ids` gives them)
        for (k, name) in NAMES.iter().enumerate() {
            let mut b = Node::primitive_type_builder(name, Physical::INT64).with_repetition(Repetition::OPTIONAL).with_id(Some(1_000_001 + k as i32));
            if *name == CREATED || *name == UPDATED {
                b = b.with_logical_type(Some(LogicalType::timestamp(true, TimeUnit::MICROS)));
            }
            ours.push(Arc::new(b.build()?));
            let path: ColumnPath = (*name).into();
            let encoding = if *name == ROW_ID { Encoding::DELTA_BINARY_PACKED } else { Encoding::PLAIN };
            props = props.set_column_dictionary_enabled(path.clone(), false).set_column_encoding(path, encoding);
        }
    }
    let root: TypePtr = Arc::new(Node::group_type_builder("schema").with_fields(ours).build()?);
    let our_descr = SchemaDescriptor::new(root.clone());
    let reader = Arc::new(Piece { from: cut.bytes.start, bytes: piece });
    let mut w = SerializedFileWriter::new(Vec::with_capacity(len + (1 << 20)), root, Arc::new(props.build()))?;
    let mut rows = 0u64;
    for g in cut.groups.clone() {
        let rg = md.row_group(g);
        let mut out = w.next_row_group()?;
        for (i, chunk) in rg.columns().iter().enumerate() {
            SerializedPageReader::new(reader.clone(), chunk, rg.num_rows() as usize, None)?.peek_next_page()?;
            out.append_column(reader.as_ref(), column_close(md, g, i, chunk, our_descr.column(i))?)?;
        }
        if let Some(s) = sys {
            let n = rg.num_rows() as usize;
            let (defs, from) = (vec![1i16; n], s.first + rows as i64);
            let columns: [Vec<i64>; 4] = [(from..from + n as i64).collect(), vec![s.version; n], vec![s.at_us; n], vec![s.at_us; n]];
            for values in columns {
                let mut c = out.next_column()?.context("a system column")?;
                c.typed::<Int64Type>().write_batch(&values, Some(&defs), None)?;
                c.close()?;
            }
        }
        rows += rg.num_rows() as u64;
        out.close()?;
    }
    let footer = w.finish()?;
    let bytes = std::mem::take(w.inner_mut());
    // (the system columns: ids of a few bits each, versions and times one value a column, squeezed by the codec)
    let cap = len + room(len) + if sys.is_some() { rows as usize / 4 + (64 << 10) } else { 0 };
    ensure!(bytes.len() <= cap, "the kept file is {} bytes, over the {cap} its {len} bytes leave room for", bytes.len());
    Ok(Kept { bytes: Bytes::from(bytes), footer, rows })
}

/// Every cut of a whole file that `plan` allows, copied (tests, and a file already in memory). `Ok(Err(why))` when the
/// file isn't kept; `Err` only when a chunk can't be read (`copy`).
#[cfg(test)]
pub(crate) fn keep(file: Bytes, table: &Schema, limits: &Limits) -> Result<Result<Vec<Kept>, String>> {
    let md = match ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional).parse_and_finish(&file) {
        Ok(md) => md,
        Err(_) if file.ends_with(b"PARE") => return Ok(Err("its footer is encrypted, which this build doesn't read".into())),
        Err(e) => return Ok(Err(format!("its footer doesn't open: {e}"))),
    };
    let cuts = match plan(&md, table, limits) {
        Ok(cuts) => cuts,
        Err(why) => return Ok(Err(why)),
    };
    let mut kept = Vec::with_capacity(cuts.len());
    for cut in &cuts {
        let k = copy(&md, table, cut, file.slice(cut.bytes.start as usize..cut.bytes.end as usize), None)?;
        if k.bytes.len() > limits.file {
            return Ok(Err(format!("a kept file came out at {} bytes, over the {}", k.bytes.len(), limits.file)));
        }
        kept.push(k);
    }
    Ok(Ok(kept))
}

/// The close result of a source chunk: its metadata under our descriptor, its statistics only where they are trusted
/// (`trusted`), else their NULL count alone; its column index only with trusted statistics; its offset index always.
fn column_close(md: &ParquetMetaData, g: usize, i: usize, src: &ColumnChunkMetaData, ours: ColumnDescPtr) -> Result<ColumnCloseResult> {
    let (stats, column_index) = match src.statistics() {
        Some(s) if trusted(md, i, s) => (Some(s.clone()), md.column_index().and_then(|ci| ci.get(g)).and_then(|row| row.get(i)).cloned()),
        Some(s) => (nulls_only(s), None),
        None => (None, None),
    };
    let mut b = ColumnChunkMetaData::builder(ours)
        .set_compression_codec(src.compression_codec())
        .set_encodings_mask(*src.encodings_mask())
        .set_total_compressed_size(src.compressed_size())
        .set_total_uncompressed_size(src.uncompressed_size())
        .set_num_values(src.num_values())
        .set_data_page_offset(src.data_page_offset())
        .set_dictionary_page_offset(src.dictionary_page_offset())
        .set_unencoded_byte_array_data_bytes(src.unencoded_byte_array_data_bytes())
        .set_repetition_level_histogram(src.repetition_level_histogram().cloned())
        .set_definition_level_histogram(src.definition_level_histogram().cloned());
    if let Some(s) = stats {
        b = b.set_statistics(s);
    }
    if let Some(p) = src.page_encoding_stats() {
        b = b.set_page_encoding_stats(p.clone());
    } else if let Some(m) = src.page_encoding_stats_mask() {
        b = b.set_page_encoding_stats_mask(*m);
    }
    Ok(ColumnCloseResult {
        bytes_written: src.compressed_size() as u64,
        rows_written: md.row_group(g).num_rows() as u64,
        metadata: b.build()?,
        bloom_filter: None,
        column_index,
        offset_index: md.offset_index().and_then(|oi| oi.get(g)).and_then(|row| row.get(i)).cloned(),
    })
}

/// Whether a column's min and max may be kept. Our footer says every column is TYPE_DEFINED_ORDER, so a pair the
/// source doesn't order by its type (no column order, or an undefined sort order) or that is only in the deprecated
/// `min` and `max` fields (signed bytes, wrong for unsigned values) would be read as trusted.
fn trusted(md: &ParquetMetaData, i: usize, s: &Statistics) -> bool {
    let ordered = matches!(md.file_metadata().column_order(i), ColumnOrder::TYPE_DEFINED_ORDER(order) if !matches!(order, SortOrder::UNDEFINED));
    ordered && !s.is_min_max_deprecated()
}

/// The NULL count of a column's statistics, as statistics of its type with no min and max.
fn nulls_only(s: &Statistics) -> Option<Statistics> {
    let n = s.null_count_opt()?;
    Some(match s {
        Statistics::Boolean(_) => Statistics::boolean(None, None, None, Some(n), false),
        Statistics::Int32(_) => Statistics::int32(None, None, None, Some(n), false),
        Statistics::Int64(_) => Statistics::int64(None, None, None, Some(n), false),
        Statistics::Int96(_) => Statistics::int96(None, None, None, Some(n), false),
        Statistics::Float(_) => Statistics::float(None, None, None, Some(n), false),
        Statistics::Double(_) => Statistics::double(None, None, None, Some(n), false),
        Statistics::ByteArray(_) => Statistics::byte_array(None, None, None, Some(n), false),
        Statistics::FixedLenByteArray(_) => Statistics::fixed_len_byte_array(None, None, None, Some(n), false),
    })
}

/// A cut's source bytes, read at the source's own offsets: `append_column` reads each chunk where the source has it.
struct Piece {
    from: u64,
    bytes: Bytes,
}

impl Piece {
    fn at(&self, start: u64) -> datafusion::parquet::errors::Result<usize> {
        start
            .checked_sub(self.from)
            .filter(|&a| a <= self.bytes.len() as u64)
            .map(|a| a as usize)
            .ok_or_else(|| ParquetError::General(format!("offset {start} isn't in the piece, which starts at {}", self.from)))
    }
}

impl Length for Piece {
    fn len(&self) -> u64 { self.from + self.bytes.len() as u64 }
}

impl ChunkReader for Piece {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> datafusion::parquet::errors::Result<Self::T> {
        Ok(self.bytes.slice(self.at(start)?..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> datafusion::parquet::errors::Result<Bytes> {
        let at = self.at(start)?;
        if at + length > self.bytes.len() {
            return Err(ParquetError::General(format!("{length} bytes at offset {start} run past the piece")));
        }
        Ok(self.bytes.slice(at..at + length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tier;
    use datafusion::arrow::array::{ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array, ListArray, StringArray, StringViewArray, TimestampNanosecondArray};
    use datafusion::arrow::compute::{cast, concat_batches};
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, TimeUnit};
    use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::basic::Repetition;
    use datafusion::parquet::data_type::{Int96, Int96Type};
    use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterVersion};
    use std::collections::HashMap;

    const BIG: Limits = Limits { file: 1 << 30, group: 1 << 30 };

    fn limits() -> Limits { Limits { file: tier::FILE_BYTES, group: tier::ROW_GROUP_BYTES } }

    /// The file's rows, read with no schema hint, as one batch; Utf8View as Utf8, so a source that carries Arrow's
    /// schema and a kept file that doesn't compare equal.
    fn rows(file: &Bytes) -> RecordBatch {
        let reader = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap().build().unwrap();
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let batch = concat_batches(&schema, &batches).unwrap();
        let fields: Vec<Field> = batch.schema().fields().iter().map(|f| f.as_ref().clone().with_data_type(match f.data_type() {
            DataType::Utf8View => DataType::Utf8,
            t => t.clone(),
        })).collect();
        let columns: Vec<ArrayRef> = batch.columns().iter().zip(&fields).map(|(c, f)| cast(c, f.data_type()).unwrap()).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn assert_same_rows(kept: &Bytes, source: &Bytes) {
        let (a, b) = (rows(kept), rows(source));
        assert_eq!(a.num_rows(), b.num_rows(), "rows");
        for (i, (x, y)) in a.schema().fields().iter().zip(b.schema().fields()).enumerate() {
            assert_eq!((x.name(), x.data_type()), (y.name(), y.data_type()), "column {i}");
            assert_eq!(a.column(i).to_data(), b.column(i).to_data(), "column {}", x.name());
        }
    }

    /// What every kept file keeps: within `limits.file`, no Arrow schema, our field ids 1..n, no sorting columns.
    fn check_kept(k: &Kept, limits: &Limits) {
        assert!(k.bytes.len() <= limits.file, "{} bytes over {}", k.bytes.len(), limits.file);
        let file = k.footer.file_metadata();
        assert!(file.key_value_metadata().is_none_or(|kv| kv.iter().all(|e| e.key != "ARROW:schema")), "an ARROW:schema key");
        let ids: Vec<i32> = file.schema_descr().root_schema().get_fields().iter().map(|f| f.get_basic_info().id()).collect();
        assert_eq!(ids, (1..=ids.len() as i32).collect::<Vec<_>>());
        assert!(k.footer.row_groups().iter().all(|rg| rg.sorting_columns().is_none()), "sorting columns");
    }

    fn footer_of(file: &Bytes) -> ParquetMetaData {
        ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional).parse_and_finish(file).unwrap()
    }

    /// Our own writer's file: three row groups (flushed batch by batch), with ints, strings, a date, a decimal, a
    /// double and NULLs.
    #[test]
    fn our_own_file_is_kept_and_reads_the_same() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Date32, true),
            Field::new("dec", DataType::Decimal128(10, 2), true),
            Field::new("f", DataType::Float64, true),
        ]));
        let batch = |from: i64| {
            let n = from..from + 1000;
            RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int64Array::from_iter(n.clone().map(|v| (v % 7 != 3).then_some(v)))),
                Arc::new(StringArray::from_iter(n.clone().map(|v| (v % 5 != 0).then(|| format!("s{v}"))))),
                Arc::new(Date32Array::from_iter(n.clone().map(|v| Some(v as i32)))),
                Arc::new(Decimal128Array::from_iter(n.clone().map(|v| Some(v as i128 * 3 - 900))).with_precision_and_scale(10, 2).unwrap()),
                Arc::new(Float64Array::from_iter(n.map(|v| (v % 11 != 0).then_some(v as f64 * 0.5)))),
            ]).unwrap()
        };
        let mut w = tier::writer(schema.clone(), &[]).unwrap();
        for from in [0, 1000, 2000] {
            w.write(&batch(from)).unwrap();
            w.flush().unwrap();
        }
        let file = Bytes::from(w.into_inner().unwrap());
        let source = footer_of(&file);
        assert_eq!(source.num_row_groups(), 3);

        let kept = keep(file.clone(), &schema, &limits()).unwrap().unwrap();
        assert_eq!(kept.len(), 1);
        check_kept(&kept[0], &limits());
        assert_eq!(kept[0].rows, 3000);
        assert_eq!(kept[0].footer.num_row_groups(), 3);
        assert_same_rows(&kept[0].bytes, &file);
        for g in 0..3 {
            for c in 0..5 {
                let (a, b) = (source.row_group(g).column(c).statistics().unwrap(), kept[0].footer.row_group(g).column(c).statistics().unwrap());
                assert_eq!((a.min_bytes_opt(), a.max_bytes_opt(), a.null_count_opt()), (b.min_bytes_opt(), b.max_bytes_opt(), b.null_count_opt()), "row group {g} column {c}");
            }
        }
    }

    /// With `sys`, a kept file carries its rows' system columns as a written file of a bulk INSERT does
    /// (`sys::stamp_new`, `tier::writer`): ids counting up from `first` across its row groups, the commit number and time,
    /// the same types, field ids and encodings. The table's own columns are copied as they are.
    #[test]
    fn a_kept_file_carries_its_rows_system_columns() {
        use crate::text::VERSION;
        use datafusion::arrow::array::AsArray;
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true), Field::new("s", DataType::Utf8, true)]));
        let batch = |from: i64| RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from_iter_values(from..from + 1000)) as ArrayRef,
            Arc::new(StringArray::from_iter((from..from + 1000).map(|v| Some(format!("s{}", v % 13))))),
        ]).unwrap();
        let mut w = tier::writer(schema.clone(), &[]).unwrap();
        for from in [0, 1000, 2000] {
            w.write(&batch(from)).unwrap();
            w.flush().unwrap();
        }
        let file = Bytes::from(w.into_inner().unwrap());
        let md = footer_of(&file);
        let cuts = plan(&md, &schema, &limits()).unwrap();
        assert_eq!(cuts.len(), 1);
        let reserved = crate::log::Reserved { version: 41, ms: 1_700_000_000_123, block: 5 };
        let sys = Sys { first: (5 << 32) + 7, version: 41, at_us: 1_700_000_000_123_000 };
        let k = copy(&md, &schema, &cuts[0], file.slice(cuts[0].bytes.start as usize..cuts[0].bytes.end as usize), Some(sys)).unwrap();
        assert_eq!(k.rows, 3000);
        let ids: Vec<i32> = k.footer.file_metadata().schema_descr().root_schema().get_fields().iter().map(|f| f.get_basic_info().id()).collect();
        assert_eq!(ids, vec![1, 2, 1_000_001, 1_000_002, 1_000_003, 1_000_004]);

        // What a written file of the same rows, stamped, reads as.
        let stamped: Vec<RecordBatch> = [0i64, 1000, 2000].iter().map(|&from| crate::sys::stamp_new(&batch(from), sys.first + from, reserved).unwrap()).collect();
        let mut w = tier::writer(stamped[0].schema(), &[]).unwrap();
        for b in &stamped {
            w.write(b).unwrap();
        }
        let written = Bytes::from(w.into_inner().unwrap());
        assert_same_rows(&k.bytes, &written);
        let got = rows(&k.bytes);
        let ids = got.column_by_name(ROW_ID).unwrap().as_primitive::<Int64Type>();
        assert_eq!((ids.value(0), ids.value(2999)), (sys.first, sys.first + 2999));
        assert_eq!(got.column_by_name(VERSION).unwrap().as_primitive::<Int64Type>().value(1500), 41);
        // The system columns' chunks encoded as the writer's: no dictionary, row ids as deltas.
        for rg in k.footer.row_groups() {
            let encodings = |c: usize| rg.column(c).encodings().collect::<Vec<_>>();
            assert!(encodings(2).contains(&Encoding::DELTA_BINARY_PACKED), "{:?}", encodings(2));
            for c in 2..6 {
                assert!(!encodings(c).contains(&Encoding::RLE_DICTIONARY), "column {c}: {:?}", encodings(c));
                assert!(rg.column(c).statistics().is_some_and(|s| s.min_bytes_opt().is_some()), "column {c}'s range");
            }
        }
    }

    /// A source with no offset index (pyarrow's default): its kept file has none either, the system columns' included
    /// (a file's chunks all have one or none do), and reads with them.
    #[test]
    fn a_file_with_no_offset_index_takes_system_columns_with_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let props = WriterProperties::builder().set_offset_index_disabled(true).set_statistics_enabled(EnabledStatistics::Chunk).build();
        let mut w = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
        for from in [0i64, 500] {
            w.write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from_iter_values(from..from + 500))]).unwrap()).unwrap();
            w.flush().unwrap();
        }
        let file = Bytes::from(w.into_inner().unwrap());
        let md = footer_of(&file);
        assert!(md.offset_index().is_none(), "the source has no offset index");
        let cuts = plan(&md, &schema, &limits()).unwrap();
        let sys = Sys { first: 1 << 32, version: 3, at_us: 9 };
        let k = copy(&md, &schema, &cuts[0], file.slice(cuts[0].bytes.start as usize..cuts[0].bytes.end as usize), Some(sys)).unwrap();
        assert_eq!((k.rows, k.footer.num_row_groups()), (1000, 2));
        assert!(footer_of(&k.bytes).offset_index().is_none());
        let got = rows(&k.bytes);
        assert_eq!(got.num_columns(), 5);
        assert_eq!(got.column(0).to_data(), rows(&file).column(0).to_data());
        assert!(k.footer.row_groups().iter().all(|rg| (1..5).all(|c| rg.column(c).statistics().is_some_and(|s| s.min_bytes_opt().is_some()))), "the system columns' ranges");
    }

    /// Another writer's file: parquet-rs's ArrowWriter with SNAPPY, dictionary pages, data page v2, page indexes, an
    /// ARROW:schema with Utf8View, and field ids 7, 3, 9 (kept as 1, 2, 3).
    #[test]
    fn another_writers_file_is_kept_with_our_field_ids() {
        let with_id = |name: &str, t: DataType, id: &str| Field::new(name, t, true).with_metadata(HashMap::from([("PARQUET:field_id".to_string(), id.to_string())]));
        let schema = Arc::new(Schema::new(vec![with_id("a", DataType::Int64, "7"), with_id("b", DataType::Utf8View, "3"), with_id("c", DataType::Float64, "9")]));
        let batch = |from: i64| {
            let n = from..from + 900;
            RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int64Array::from_iter(n.clone().map(|v| (v % 13 != 0).then_some(v % 40)))),
                Arc::new(StringViewArray::from_iter(n.clone().map(|v| (v % 9 != 0).then(|| format!("word{}", v % 25))))),
                Arc::new(Float64Array::from_iter(n.map(|v| Some(v as f64 / 4.0)))),
            ]).unwrap()
        };
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(true)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut w = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
        for from in [0, 900, 1800] {
            w.write(&batch(from)).unwrap();
            w.flush().unwrap();
        }
        let file = Bytes::from(w.into_inner().unwrap());

        let source = footer_of(&file);
        let chunk = source.row_group(0).column(1);
        assert!(matches!(chunk.compression(), Compression::SNAPPY));
        assert!(chunk.dictionary_page_offset().is_some(), "a dictionary page");
        assert!(source.column_index().is_some(), "page indexes");
        assert!(source.file_metadata().key_value_metadata().is_some_and(|kv| kv.iter().any(|e| e.key == "ARROW:schema")));

        let kept = keep(file.clone(), &schema, &limits()).unwrap().unwrap();
        assert_eq!(kept.len(), 1);
        check_kept(&kept[0], &limits());
        assert_eq!(kept[0].rows, 2700);
        assert_same_rows(&kept[0].bytes, &file);
    }

    /// pyarrow 26's file (tests/fixtures/parquet/pyarrow-26.parquet: 1200 rows in three row groups, Snappy, dictionary
    /// pages, page indexes, an ARROW:schema, field ids 7, 3, 9, and a string and a decimal column): another writer's,
    /// kept, and read back as the source's rows.
    #[test]
    fn a_pyarrow_file_is_kept() {
        let file = Bytes::from_static(include_bytes!("../tests/fixtures/parquet/pyarrow-26.parquet"));
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(12, 2), true),
        ]);
        let source = footer_of(&file);
        assert_eq!(source.num_row_groups(), 3);
        assert!(matches!(source.row_group(0).column(0).compression(), Compression::SNAPPY));
        assert!(source.row_group(0).column(1).dictionary_page_offset().is_some(), "a dictionary page");
        assert!(source.column_index().is_some(), "page indexes");

        let kept = keep(file.clone(), &schema, &limits()).unwrap().unwrap();
        assert_eq!(kept.len(), 1);
        check_kept(&kept[0], &limits());
        assert_eq!(kept[0].rows, 1200);
        assert_same_rows(&kept[0].bytes, &file);
    }

    /// The same file made again through `append_column`, with the string chunk's statistics replaced by deprecated
    /// min and max (signed bytes, wrong for unsigned order), its column index left as it was. Kept: that chunk has no
    /// min, no max and no column index, but its NULL count; the ints keep theirs.
    #[test]
    fn old_style_statistics_are_dropped_and_the_nulls_kept() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true), Field::new("s", DataType::Utf8, true)]));
        let mut w = ArrowWriter::try_new(Vec::new(), schema.clone(), None).unwrap();
        let n = 0..5000i64;
        w.write(&RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from_iter(n.clone().map(|v| (v % 6 != 1).then_some(v)))),
            Arc::new(StringArray::from_iter(n.map(|v| (v % 4 != 2).then(|| format!("é{}", v % 300))))),
        ]).unwrap()).unwrap();
        let original = Bytes::from(w.into_inner().unwrap());
        let md = footer_of(&original);

        let mut out = SerializedFileWriter::new(Vec::new(), md.file_metadata().schema_descr_ptr().root_schema_ptr(), Arc::new(WriterProperties::builder().build())).unwrap();
        let old = Statistics::byte_array(Some("é".into()), Some("z".into()), None, Some(0), true);
        let mut rg_out = out.next_row_group().unwrap();
        for (c, chunk) in md.row_group(0).columns().iter().enumerate() {
            let metadata = if c == 1 { chunk.clone().into_builder().set_statistics(old.clone()).build().unwrap() } else { chunk.clone() };
            let column_index = md.column_index().and_then(|ci| ci[0].get(c)).cloned();
            let offset_index = md.offset_index().and_then(|oi| oi[0].get(c)).cloned();
            rg_out.append_column(&original, ColumnCloseResult { bytes_written: chunk.compressed_size() as u64, rows_written: 5000, metadata, bloom_filter: None, column_index, offset_index }).unwrap();
        }
        rg_out.close().unwrap();
        let file = Bytes::from(out.into_inner().unwrap());
        let made = footer_of(&file);
        assert!(made.row_group(0).column(1).statistics().unwrap().is_min_max_deprecated());

        let kept = keep(file.clone(), &schema, &limits()).unwrap().unwrap();
        check_kept(&kept[0], &limits());
        assert_same_rows(&kept[0].bytes, &file);
        let footer = &kept[0].footer;
        let s = footer.row_group(0).column(1).statistics().unwrap();
        assert_eq!((s.min_bytes_opt(), s.max_bytes_opt()), (None, None));
        assert_eq!(s.null_count_opt(), made.row_group(0).column(1).statistics().unwrap().null_count_opt());
        let index = footer.column_index().expect("the ints' column index");
        assert!(matches!(index[0][1], ColumnIndexMetaData::NONE), "the string chunk's column index");
        assert!(!matches!(index[0][0], ColumnIndexMetaData::NONE), "the ints' column index");
        let i = footer.row_group(0).column(0).statistics().unwrap();
        let source_i = made.row_group(0).column(0).statistics().unwrap();
        assert_eq!((i.min_bytes_opt(), i.max_bytes_opt()), (source_i.min_bytes_opt(), source_i.max_bytes_opt()));
    }

    /// Over three row groups of 16K random-looking ints (128 KB each), in a file of about 1 MB: 256 KB limits give
    /// several kept files, which read back as the source's rows in order.
    #[test]
    fn a_file_over_the_limit_is_split_in_row_group_order() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let props = WriterProperties::builder().set_dictionary_enabled(false).build();
        let mut w = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
        for g in 0..8i64 {
            let vals = (0..16_384i64).map(|v| (g * 16_384 + v).wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407));
            w.write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from_iter_values(vals))]).unwrap()).unwrap();
            w.flush().unwrap();
        }
        let file = Bytes::from(w.into_inner().unwrap());
        assert!(file.len() > 900 << 10 && file.len() < 1200 << 10, "{} bytes", file.len());
        assert_eq!(footer_of(&file).num_row_groups(), 8);

        let small = Limits { file: 256 << 10, group: tier::ROW_GROUP_BYTES };
        let kept = keep(file.clone(), &schema, &small).unwrap().unwrap();
        assert!(kept.len() >= 2, "{} kept files", kept.len());
        for k in &kept {
            check_kept(k, &small);
        }
        assert_eq!(kept.iter().map(|k| k.rows).sum::<u64>(), 8 * 16_384);
        let parts: Vec<RecordBatch> = kept.iter().map(|k| rows(&k.bytes)).collect();
        let joined = concat_batches(&parts[0].schema(), &parts).unwrap();
        assert_eq!(joined.column(0).to_data(), rows(&file).column(0).to_data());
    }

    /// The reasons for not keeping a file, each one parquet-rs can make.
    #[test]
    fn a_file_parquet_rs_makes_that_isnt_kept_says_why() {
        let table_of = |name: &str, t: DataType| Schema::new(vec![Field::new(name, t, true)]);
        let int_schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let ints = |n: i64| RecordBatch::try_new(int_schema.clone(), vec![Arc::new(Int64Array::from_iter_values(0..n))]).unwrap();
        let as_ints = table_of("i", DataType::Int64);

        // INT96, from the low-level writer.
        let node = Arc::new(Node::group_type_builder("schema").with_fields(vec![Arc::new(
            Node::primitive_type_builder("t", Physical::INT96).with_repetition(Repetition::REQUIRED).build().unwrap(),
        )]).build().unwrap());
        let mut w = SerializedFileWriter::new(Vec::new(), node, Arc::new(WriterProperties::builder().build())).unwrap();
        let mut rg = w.next_row_group().unwrap();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<Int96Type>().write_batch(&[Int96::from(vec![1, 2, 3])], None, None).unwrap();
        col.close().unwrap();
        rg.close().unwrap();
        let int96 = Bytes::from(w.into_inner().unwrap());
        let why = keep(int96, &table_of("t", DataType::Timestamp(TimeUnit::Microsecond, None)), &BIG).unwrap().unwrap_err();
        assert!(why.contains("INT96"), "{why}");

        // Not a Parquet file at all.
        assert!(keep(Bytes::from_static(b"not parquet"), &as_ints, &BIG).unwrap().is_err());

        // A nanosecond timestamp.
        let ns = Arc::new(Schema::new(vec![Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true)]));
        let mut w = ArrowWriter::try_new(Vec::new(), ns.clone(), None).unwrap();
        w.write(&RecordBatch::try_new(ns, vec![Arc::new(TimestampNanosecondArray::from_iter_values(0..10i64))]).unwrap()).unwrap();
        let file = Bytes::from(w.into_inner().unwrap());
        let why = keep(file, &table_of("t", DataType::Timestamp(TimeUnit::Microsecond, None)), &BIG).unwrap().unwrap_err();
        assert!(why.contains("Timestamp(ns)"), "{why}");

        // A list.
        let list_t = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let lists = Arc::new(Schema::new(vec![Field::new("xs", list_t.clone(), true)]));
        let mut w = ArrowWriter::try_new(Vec::new(), lists.clone(), None).unwrap();
        let xs = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1)]), None]);
        w.write(&RecordBatch::try_new(lists, vec![Arc::new(xs)]).unwrap()).unwrap();
        let file = Bytes::from(w.into_inner().unwrap());
        let why = keep(file, &table_of("xs", list_t), &BIG).unwrap().unwrap_err();
        assert!(why.contains("nested"), "{why}");

        // A row group over the group limit, and a field named otherwise.
        let mut w = ArrowWriter::try_new(Vec::new(), int_schema.clone(), None).unwrap();
        w.write(&ints(5000)).unwrap();
        let file = Bytes::from(w.into_inner().unwrap());
        let small_group = Limits { file: 1 << 30, group: 1000 };
        let why = keep(file.clone(), &as_ints, &small_group).unwrap().unwrap_err();
        assert!(why.starts_with("row group 0 is"), "{why}");
        let why = keep(file, &table_of("x", DataType::Int64), &BIG).unwrap().unwrap_err();
        assert!(why.contains("aren't the table's"), "{why}");

        // No rows at all: no row groups.
        let w = ArrowWriter::try_new(Vec::new(), int_schema.clone(), None).unwrap();
        let file = Bytes::from(w.into_inner().unwrap());
        assert_eq!(keep(file, &as_ints, &BIG).unwrap().unwrap_err(), "it has no row groups");

        // A PARE tail: the footer is encrypted, and is not read.
        let mut w = ArrowWriter::try_new(Vec::new(), int_schema.clone(), None).unwrap();
        w.write(&ints(10)).unwrap();
        let mut bytes = w.into_inner().unwrap();
        let end = bytes.len();
        bytes[end - 4..].copy_from_slice(b"PARE");
        let why = keep(Bytes::from(bytes), &as_ints, &BIG).unwrap().unwrap_err();
        assert!(why.contains("encrypted"), "{why}");
    }

    /// A chunk whose first page header is zeroed doesn't start with a page: `copy` is an `Err`, not a reason.
    #[test]
    fn a_chunk_whose_first_page_header_is_zeroed_is_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let mut w = tier::writer(schema.clone(), &[]).unwrap();
        w.write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from_iter_values(0..5000))]).unwrap()).unwrap();
        let mut bytes = w.into_inner().unwrap();
        let md = footer_of(&Bytes::from(bytes.clone()));
        let at = md.row_group(0).column(0).byte_range().0 as usize;
        bytes[at..at + 16].fill(0);
        assert!(keep(Bytes::from(bytes), &schema, &limits()).is_err());
    }
}
