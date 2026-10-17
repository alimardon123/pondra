//! How an answer's rows are written, in every format a door sends: JSON, a text table, Arrow, CSV, TSV, NDJSON,
//! Parquet, Excel and the console's typed answer, whole or in pieces. In every build (no lake), so a page writes the
//! bytes a node writes (designs/l2-b4-front-and-web.md).

use anyhow::Result;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json as arrow_json;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use serde_json::{json as j, Value};
use std::sync::Arc;

pub fn content_type(format: Option<&str>) -> &'static str {
    match format {
        Some("table") => "text/plain",
        Some("arrow") => "application/vnd.apache.arrow.stream",
        Some("csv") => "text/csv; charset=utf-8",
        Some("tsv") => "text/tab-separated-values; charset=utf-8",
        Some("ndjson") => "application/x-ndjson",
        Some("parquet") => "application/vnd.apache.parquet",
        Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        _ => "application/json",
    }
}

/// The formats an answer can be sent in pieces of (the others are rendered whole: a table, a typed
/// answer, a Parquet or an Excel file is one thing to the reader).
pub fn streams(format: Option<&str>) -> bool { !matches!(format, Some("table" | "typed" | "parquet" | "xlsx")) }

/// A batch cut into pieces of about 1 MB of Arrow (by its rows' average size). An empty batch is one
/// empty piece, so that CSV's header comes with the first piece, as `render` writes it.
pub fn piece_rows(b: &RecordBatch) -> Vec<RecordBatch> {
    const PIECE: usize = 1 << 20;
    let rows = b.num_rows();
    if rows == 0 {
        return vec![b.clone()];
    }
    let per_row = (b.get_array_memory_size() / rows).max(1);
    let step = (PIECE / per_row).max(1);
    (0..rows).step_by(step).map(|at| b.slice(at, step.min(rows - at))).collect()
}

/// The pieces of one answer in one format. Written together they are what `render` writes for the
/// whole answer: JSON's brackets and commas across pieces, CSV's header once, one Arrow stream.
pub struct Encoding {
    shape: Shape,
    begun: bool, // the first piece is written (JSON's "[" and CSV's header go with it)
    rows: usize, // JSON rows written so far (a comma goes before each but the first)
    arrow: Option<StreamWriter<Vec<u8>>>,
}

#[derive(Clone, Copy)]
enum Shape {
    Json,
    Lines,
    Csv(u8),
    Arrow,
}

impl Encoding {
    pub fn new(format: Option<&str>) -> Self {
        let shape = match format {
            Some("csv") => Shape::Csv(b','),
            Some("tsv") => Shape::Csv(b'\t'),
            Some("ndjson") => Shape::Lines,
            Some("arrow") => Shape::Arrow,
            _ => Shape::Json, // (as `render`: any other format is JSON)
        };
        Encoding { shape, begun: false, rows: 0, arrow: None }
    }

    /// The bytes of one piece.
    fn piece(&mut self, b: &RecordBatch) -> anyhow::Result<Vec<u8>> {
        match self.shape {
            Shape::Json => {
                let mut out = self.open();
                for row in lines(b)?.split(|c| *c == b'\n').filter(|r| !r.is_empty()) {
                    if self.rows > 0 {
                        out.push(b',');
                    }
                    out.extend_from_slice(row);
                    self.rows += 1;
                }
                Ok(out)
            }
            Shape::Lines => lines(b),
            Shape::Csv(delimiter) => {
                let header = !std::mem::replace(&mut self.begun, true);
                let mut w = datafusion::arrow::csv::WriterBuilder::new().with_header(header).with_delimiter(delimiter).build(Vec::new());
                w.write(b)?;
                Ok(w.into_inner())
            }
            Shape::Arrow => {
                let mut w = match self.arrow.take() {
                    Some(w) => w,
                    None => StreamWriter::try_new(Vec::new(), &b.schema())?, // (its schema goes with the first piece)
                };
                w.write(&compact(b.clone()))?;
                let out = std::mem::take(w.get_mut());
                self.arrow = Some(w);
                Ok(out)
            }
        }
    }

    /// One step of the answer: a piece, or (None) what closes it.
    pub fn step(&mut self, piece: Option<&RecordBatch>) -> anyhow::Result<Vec<u8>> {
        match piece {
            Some(b) => self.piece(b),
            None => self.end(),
        }
    }

    /// What closes the answer: JSON's "]" (after its "[", if no piece had rows), Arrow's end of stream.
    fn end(&mut self) -> anyhow::Result<Vec<u8>> {
        match self.shape {
            Shape::Json => {
                let mut out = self.open();
                out.push(b']');
                Ok(out)
            }
            Shape::Arrow => {
                let mut w = match self.arrow.take() {
                    Some(w) => w,
                    None => StreamWriter::try_new(Vec::new(), &datafusion::arrow::datatypes::Schema::empty())?, // (no rows at all: as `query::ipc` writes them)
                };
                w.finish()?;
                Ok(std::mem::take(w.get_mut()))
            }
            Shape::Lines | Shape::Csv(_) => Ok(vec![]),
        }
    }

    /// JSON's "[", once: before the first piece's rows, or before the "]" when there are none.
    fn open(&mut self) -> Vec<u8> {
        if std::mem::replace(&mut self.begun, true) {
            vec![]
        } else {
            b"[".to_vec()
        }
    }
}

/// A piece's rows as newline-delimited JSON (`render`'s own writer, one line a row).
fn lines(b: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut w = arrow_json::LineDelimitedWriter::new(Vec::new());
    w.write_batches(&[b])?;
    w.finish()?;
    Ok(w.into_inner())
}

/// What keeps an answer's rows for its later pages and says the id they are kept under (`pages::keep` on a node; a
/// page keeps none). Only the console's typed answer asks it.
pub type Keep = fn(&[RecordBatch]) -> Option<String>;

/// Rows as `?format=` asks: JSON, a text table, or Arrow IPC (straight into pandas, Polars and
/// DuckDB, no JSON parsing); or a file of them to download: CSV, TSV, NDJSON, Parquet, Excel.
pub fn render(batches: &[RecordBatch], format: Option<&str>, keep: Keep) -> anyhow::Result<bytes::Bytes> {
    Ok(bytes::Bytes::from(match format {
        Some("table") => pretty_format_batches(batches)?.to_string().into_bytes(),
        Some("typed") => typed(batches, SHOWN, keep)?,
        Some("arrow") => ipc(batches)?,
        Some(f @ ("csv" | "tsv")) => {
            let mut w = datafusion::arrow::csv::WriterBuilder::new().with_header(true).with_delimiter(if f == "tsv" { b'\t' } else { b',' }).build(Vec::new());
            for b in batches {
                w.write(b)?;
            }
            w.into_inner()
        }
        Some("ndjson") => {
            let mut w = arrow_json::LineDelimitedWriter::new(Vec::new());
            w.write_batches(&batches.iter().collect::<Vec<_>>())?;
            w.finish()?;
            w.into_inner()
        }
        Some("parquet") => {
            let batches = batches.iter().map(storable).collect::<anyhow::Result<Vec<_>>>()?; // (intervals as text)
            let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty()));
            let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None)?;
            for b in &batches {
                w.write(b)?;
            }
            w.into_inner()?
        }
        Some("xlsx") => crate::xlsx::workbook(batches)?,
        _ => {
            let mut w = arrow_json::ArrayWriter::new(Vec::new());
            w.write_batches(&batches.iter().collect::<Vec<_>>())?;
            w.finish()?;
            w.into_inner()
        }
    }))
}

/// The rows of an answer the console is sent at once, a page, unless it asks for another number
/// (`?rows=`, its rows a page), at most `MOST`.
pub const SHOWN: usize = 10_000;
pub const MOST: usize = 100_000;

/// The console's answer (`?format=typed`, `console/console.js`): the columns with their types, the
/// first `shown` rows as lists in the columns' order (two columns may share a name: a join's), how
/// many rows there were, and the id it is kept under (`pages.rs`): its other pages, and every row
/// downloaded, are read from there (`page`), not run again.
pub fn typed(batches: &[RecordBatch], shown: usize, keep: Keep) -> anyhow::Result<Vec<u8>> {
    let columns: Vec<Value> = batches.first().map(|b| b.schema().fields().iter().map(|f| j!({"name": f.name(), "type": type_name(f.data_type())})).collect()).unwrap_or_default();
    let mut w = arrow_json::ArrayWriter::new(Vec::new());
    let mut left = shown;
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let b = b.slice(0, left.min(b.num_rows()));
        left -= b.num_rows();
        let exact: Vec<datafusion::arrow::array::ArrayRef> = b.columns().iter().map(exact).collect::<Result<_, _>>()?;
        let numbered: Vec<_> = b.schema().fields().iter().zip(&exact).enumerate().map(|(i, (f, c))| f.as_ref().clone().with_name(i.to_string()).with_data_type(c.data_type().clone())).collect(); // (unique keys)
        w.write(&RecordBatch::try_new(Arc::new(datafusion::arrow::datatypes::Schema::new(numbered)), exact)?)?;
        if left == 0 {
            break;
        }
    }
    w.finish()?;
    let objects: Vec<serde_json::Map<String, Value>> = serde_json::from_slice(&w.into_inner()).unwrap_or_default();
    let rows: Vec<Value> = objects.into_iter().map(|mut o| Value::Array((0..columns.len()).map(|i| o.remove(&i.to_string()).unwrap_or(Value::Null)).collect())).collect();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let pages = keep(batches);
    Ok(serde_json::to_vec(&j!({"columns": columns, "rows": rows, "total": total, "pages": pages}))?)
}

/// A column as JavaScript can hold it exactly: decimals as their digits (`1.50`), and 64-bit
/// integers past 2^53 as text.
fn exact(c: &datafusion::arrow::array::ArrayRef) -> anyhow::Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::{array::AsArray, compute, datatypes::{DataType, Int64Type, UInt64Type}};
    const SAFE: i64 = (1 << 53) - 1;
    let text = match c.data_type() {
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..) => true,
        DataType::Int64 => compute::min(c.as_primitive::<Int64Type>()).is_some_and(|m| m < -SAFE) || compute::max(c.as_primitive::<Int64Type>()).is_some_and(|m| m > SAFE),
        DataType::UInt64 => compute::max(c.as_primitive::<UInt64Type>()).is_some_and(|m| m > SAFE as u64),
        _ => false,
    };
    Ok(if text { compute::cast(c, &DataType::Utf8)? } else { c.clone() })
}

/// Batches as an Arrow IPC stream.
pub fn ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(Schema::empty()));
    let mut w = datafusion::arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &schema)?;
    batches.iter().try_for_each(|b| w.write(&compact(b.clone())))?;
    w.finish()?;
    Ok(w.into_inner()?)
}

/// A batch whose string and binary views are copied out of buffers that mostly hold other rows'
/// strings: those a filter, a limit or a slice kept a few rows of (decoded pages, a whole
/// group's keys, the batch a shuffle cut it from). Arrow IPC sends every buffer a view points
/// into, and a batch kept in memory holds them: a 10-row answer of ClickBench's was 220 MB.
pub fn compact(b: RecordBatch) -> RecordBatch {
    use datafusion::arrow::array::{Array, AsArray};
    fn wasteful(held: &[datafusion::arrow::buffer::Buffer], used: usize) -> bool { held.iter().map(|b| b.len()).sum::<usize>() > 2 * used + (8 << 10) }
    let lean = |c: &ArrayRef| -> Option<ArrayRef> {
        match c.data_type() {
            DataType::Utf8View => Some(c.as_string_view()).filter(|v| wasteful(v.data_buffers(), v.total_buffer_bytes_used())).map(|v| Arc::new(v.gc()) as ArrayRef),
            DataType::BinaryView => Some(c.as_binary_view()).filter(|v| wasteful(v.data_buffers(), v.total_buffer_bytes_used())).map(|v| Arc::new(v.gc()) as ArrayRef),
            _ => None,
        }
    };
    let leaner: Vec<Option<ArrayRef>> = b.columns().iter().map(lean).collect();
    if leaner.iter().all(Option::is_none) {
        return b;
    }
    let columns = leaner.into_iter().zip(b.columns()).map(|(l, c)| l.unwrap_or_else(|| c.clone())).collect();
    RecordBatch::try_new_with_options(b.schema(), columns, &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(b.num_rows()))).unwrap_or(b)
}

/// The name a table's columns record a type under (what `dtype` reads back).
pub fn type_name(t: &DataType) -> String {
    match stored(t) {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => format!("{}[]", type_name(f.data_type())),
        t => t.to_string(),
    }
}

pub fn schema(columns: &[(String, String)]) -> Result<SchemaRef> {
    let fields = columns.iter().map(|(n, t)| Ok(Field::new(n, dtype(t)?, true)));
    Ok(Arc::new(Schema::new(fields.collect::<Result<Vec<_>>>()?)))
}

/// A column's type by name: Arrow's own (`Utf8`, `Int64`, `Timestamp(Microsecond, None)`,
/// `Binary`), `T[]` for a list of T (`Float32[]`: an embedding), and `VARIANT`/`JSON` for
/// semi-structured text, which is stored as a string and read with `json_get(…)`, `->` and `->>`.
pub fn dtype(t: &str) -> Result<DataType> {
    let t = t.trim();
    Ok(match t.strip_suffix("[]") {
        Some(item) => DataType::List(Arc::new(Field::new("item", dtype(item)?, true))),
        None => match t.to_uppercase().as_str() {
            "VARIANT" | "JSON" => DataType::Utf8,
            _ => t.parse::<DataType>().map_err(|e| anyhow::anyhow!("unknown column type {t}: {e}"))?,
        },
    })
}

/// How a SQL type is kept: strings as Utf8; timestamps in microseconds, as Iceberg, Delta, Spark
/// and Postgres keep them.
pub fn stored(t: &DataType) -> DataType {
    match t {
        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        t => t.clone(),
    }
}

/// `batch` as Parquet can hold it. Parquet has no type for Arrow's intervals of months, days and
/// nanoseconds (its own `INTERVAL` keeps milliseconds), so such a column is written as its text
/// (`1 mons -2 days 3.000000004 secs`), which reads cast back to the table's type exactly. Written
/// as it was, a table with an `INTERVAL` column never left the log, and its failing round stopped
/// every other table's too.
pub fn storable(batch: &RecordBatch) -> Result<RecordBatch> {
    use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema};
    fn stored(t: &DataType) -> DataType {
        let item = |f: &std::sync::Arc<Field>| std::sync::Arc::new(f.as_ref().clone().with_data_type(stored(f.data_type())));
        match t {
            DataType::Interval(IntervalUnit::MonthDayNano) => DataType::Utf8,
            DataType::List(f) => DataType::List(item(f)),
            DataType::LargeList(f) => DataType::LargeList(item(f)),
            DataType::FixedSizeList(f, n) => DataType::FixedSizeList(item(f), *n),
            DataType::Struct(fs) => DataType::Struct(fs.iter().map(item).collect()),
            t => t.clone(),
        }
    }
    let schema = batch.schema();
    if schema.fields().iter().all(|f| stored(f.data_type()) == *f.data_type()) {
        return Ok(batch.clone());
    }
    let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone().with_data_type(stored(f.data_type()))).collect();
    let columns = batch.columns().iter().zip(&fields).map(|(c, f)| datafusion::arrow::compute::cast(c, f.data_type())).collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(std::sync::Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringViewArray};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    /// `rows` rows: an integer, a text (one value with a comma and a quote in it), a float with a null.
    fn sample(rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
            Field::new("f", DataType::Float64, true),
        ]);
        let ints = Int64Array::from_iter_values(0..rows as i64);
        let texts = StringViewArray::from_iter((0..rows).map(|i| Some(if i % 7 == 0 { "a, \"quoted\"" } else { "plain" })));
        let floats = Float64Array::from_iter((0..rows).map(|i| (i % 5 != 0).then_some(i as f64 / 4.0)));
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ints) as ArrayRef, Arc::new(texts) as ArrayRef, Arc::new(floats) as ArrayRef]).unwrap()
    }

    /// The pieces of an answer, written one by one, as `stream` writes them.
    fn written(f: Option<&str>, parts: &[RecordBatch]) -> Vec<u8> {
        let mut enc = Encoding::new(f);
        let mut out: Vec<u8> = parts.iter().flat_map(|p| enc.step(Some(p)).unwrap()).collect();
        out.extend(enc.step(None).unwrap());
        out
    }

    #[test]
    fn pieces_are_the_whole() {
        let batch = sample(200_000);
        let pieces = piece_rows(&batch);
        assert!(pieces.len() > 1, "a piece is about 1 MB of Arrow: {} pieces", pieces.len());
        for f in [None, Some("csv"), Some("tsv"), Some("ndjson")] {
            let whole = render(&[batch.clone()], f, |_| None).unwrap();
            assert!(written(f, &pieces) == whole.as_ref(), "{f:?}: the pieces are not the answer render writes");
        }
        // Arrow: the pieces and the whole decode to the same rows.
        let decoded = |bytes: &[u8]| -> Vec<RecordBatch> { StreamReader::try_new(Cursor::new(bytes), None).unwrap().map(|b| b.unwrap()).collect() };
        let same = |bs: &[RecordBatch]| concat_batches(&batch.schema(), bs).unwrap();
        let whole = render(&[batch.clone()], Some("arrow"), |_| None).unwrap();
        assert_eq!(same(&decoded(&written(Some("arrow"), &pieces))), batch);
        assert_eq!(same(&decoded(&whole)), batch);
        // An empty batch: JSON's [] and CSV's header alone, whole or in pieces.
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true), Field::new("b", DataType::Utf8, true)])));
        assert_eq!(render(&[empty.clone()], None, |_| None).unwrap().as_ref(), b"[]");
        assert_eq!(render(&[empty.clone()], Some("csv"), |_| None).unwrap().as_ref(), b"a,b\n");
        assert_eq!(written(None, &piece_rows(&empty)), b"[]");
        assert_eq!(written(Some("csv"), &piece_rows(&empty)), b"a,b\n");
    }
}
