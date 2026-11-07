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
use datafusion::logical_expr::Expr;
use datafusion::prelude::{ident, DataFrame};
use serde_json::{json as j, Value};
use std::sync::Arc;

// ---------------------------------------------------------------- the formats
//
// Each format an answer is sent in is one entry: its name (`?format=`), its content type, how a piece of it is written
// when it is sent in pieces, and how the whole answer is written. A door asks `format` for the entry by name; the
// registry (redesign step 1f) takes the list over, so an extension adds a format by adding an entry.

/// One answer format.
pub struct Format {
    /// What `?format=` names it.
    pub name: &'static str,
    pub content_type: &'static str,
    /// How a piece of it is written (`Encoding`); none for a format rendered whole: a text table, a typed answer, a
    /// Parquet or an Excel file is one thing to its reader.
    shape: Option<Shape>,
    /// The whole answer's bytes, from its parts.
    whole: Encode,
}

/// An answer's bytes from its parts: its schema, its rows and its status. These are the parts of step 8's answer
/// stream, so the stream takes the entries as they are.
type Encode = fn(&SchemaRef, &[RecordBatch], &Status) -> Result<Vec<u8>>;

/// What an answer says besides its rows. Step 8's stream adds what a statement that isn't a query says (the rows it
/// changed, its notices, its tag); today it says where a typed answer's rows are kept for its later pages.
pub struct Status {
    pub keep: Keep,
}

/// The formats, JSON first: an answer is JSON when no format is named, or one this build doesn't have.
const FORMATS: &[Format] = &[
    Format { name: "json", content_type: "application/json", shape: Some(Shape::Json), whole: |_, b, _| json(b) },
    Format { name: "table", content_type: "text/plain", shape: None, whole: |_, b, _| Ok(pretty_format_batches(b)?.to_string().into_bytes()) },
    Format { name: "typed", content_type: "application/json", shape: None, whole: |_, b, status| typed(b, SHOWN, status.keep) },
    Format { name: "arrow", content_type: "application/vnd.apache.arrow.stream", shape: Some(Shape::Arrow), whole: |_, b, _| arrow_stream(b) },
    Format { name: "csv", content_type: "text/csv; charset=utf-8", shape: Some(Shape::Csv(b',')), whole: |_, b, _| csv(b, b',') },
    Format { name: "tsv", content_type: "text/tab-separated-values; charset=utf-8", shape: Some(Shape::Csv(b'\t')), whole: |_, b, _| csv(b, b'\t') },
    Format { name: "ndjson", content_type: "application/x-ndjson", shape: Some(Shape::Lines), whole: |_, b, _| json_lines(b) },
    Format { name: "parquet", content_type: "application/vnd.apache.parquet", shape: None, whole: |_, b, _| parquet(b) },
    Format { name: "xlsx", content_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", shape: None, whole: |_, b, _| crate::xlsx::workbook(b) },
];

/// The format `name`s (`?format=`): its entry, JSON's when none is named or this build has none of that name.
pub fn format(name: Option<&str>) -> &'static Format {
    name.and_then(|n| FORMATS.iter().find(|f| f.name == n)).unwrap_or(&FORMATS[0])
}

pub fn content_type(format: Option<&str>) -> &'static str { self::format(format).content_type }

/// The formats an answer can be sent in pieces of (the others are rendered whole).
pub fn streams(format: Option<&str>) -> bool { self::format(format).shape.is_some() }

/// About how much Arrow a door sends at a time: at SF1, lineitem over HTTP comes in 0.98 s in pieces of 4 MB that take
/// the batches a plan has ready together (`pieces`), against 1.22 s in pieces of 4 MB alone and 1.94 s in pieces of 1 MB.
/// What an answer holds as it goes is bounded by it, whatever the answer's size: about 50 MB over a fetch of SF1
/// lineitem over HTTP (6M rows), against about 17 MB in pieces of 1 MB.
pub const PIECE: usize = 4 << 20;

/// A batch cut into pieces of about `PIECE` of Arrow (by its rows' average size). An empty batch is one
/// empty piece, so that CSV's header comes with the first piece, as `render` writes it; so is a batch of
/// one row, whatever its size (a piece is at least a row, and a row's average is the whole batch).
pub fn piece_rows(b: &RecordBatch) -> Vec<RecordBatch> {
    let rows = b.num_rows();
    if rows <= 1 {
        return vec![b.clone()];
    }
    let per_row = (b.get_array_memory_size() / rows).max(1);
    let step = (PIECE / per_row).max(1);
    (0..rows).step_by(step).map(|at| b.slice(at, step.min(rows - at))).collect()
}

/// The bytes a batch's rows take — not the buffers they sit in, which a batch cut from a bigger
/// one shares with it, and a builder leaves half empty.
pub fn size(b: &RecordBatch) -> usize {
    use datafusion::arrow::array::Array;
    b.columns().iter().map(|c| c.to_data().get_slice_memory_size().unwrap_or_else(|_| c.get_array_memory_size())).sum()
}

/// Batches as the pieces a door sends them in: small ones next to each other as one, up to about `PIECE` (a plan's batches
/// are 8,192 rows, so a narrow answer would otherwise be a message every 100 KB), and big ones cut (`piece_rows`).
pub fn pieces(batches: impl IntoIterator<Item = RecordBatch>) -> Vec<RecordBatch> {
    let (mut out, mut run, mut size) = (vec![], vec![], 0);
    for b in batches {
        let s = b.get_array_memory_size();
        if size + s > PIECE {
            out.extend(together(std::mem::take(&mut run)));
            size = 0;
        }
        size += s;
        run.push(b);
    }
    out.extend(together(run));
    out
}

/// Batches of one answer as one batch's pieces (each batch's own when their schemas differ).
fn together(run: Vec<RecordBatch>) -> Vec<RecordBatch> {
    match run.as_slice() {
        [one] => piece_rows(one),
        [first, rest @ ..] if rest.iter().all(|b| b.schema_ref() == first.schema_ref()) => match datafusion::arrow::compute::concat_batches(first.schema_ref(), &run) {
            Ok(b) => piece_rows(&b),
            Err(_) => run.iter().flat_map(piece_rows).collect(),
        },
        _ => run.iter().flat_map(piece_rows).collect(),
    }
}

/// The pieces of one answer in one format. Written together they are what `render` writes for the
/// whole answer: JSON's brackets and commas across pieces, CSV's header once, one Arrow stream.
pub struct Encoding {
    shape: Shape,
    begun: bool, // the first piece is written (JSON's "[" and CSV's header go with it)
    rows: usize, // JSON rows written so far (a comma goes before each but the first)
    arrow: Option<(StreamWriter<Vec<u8>>, SchemaRef)>, // (the stream's writer, and the schema it is written with: `arrow_schema`)
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
        let shape = self::format(format).shape.unwrap_or(Shape::Json); // (a format rendered whole goes in pieces as JSON)
        Encoding { shape, begun: false, rows: 0, arrow: None }
    }

    /// A batch as this encoding sends it (`as_sent`).
    pub fn lean(&self, b: RecordBatch) -> RecordBatch {
        match self.shape {
            Shape::Arrow => as_sent(b, Some("arrow")),
            _ => compact(b),
        }
    }

    /// The Arrow form, its stream written with `schema` (an `arrow_schema`) from the start: a whole answer whose schema is
    /// known before its first batch keeps its columns when it has no rows.
    pub fn with_schema(schema: SchemaRef) -> Encoding {
        // (an `arrow_schema` has no dictionary, the one thing a writer refuses, and a Vec takes every byte)
        let w = StreamWriter::try_new(Vec::new(), &schema).expect("an arrow_schema is written as an Arrow stream");
        Encoding { shape: Shape::Arrow, begun: false, rows: 0, arrow: Some((w, schema)) }
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
                let (mut w, schema) = match self.arrow.take() {
                    Some(open) => open,
                    None => {
                        let schema = Arc::new(arrow_schema(&b.schema())); // (its schema goes with the first piece)
                        (StreamWriter::try_new(Vec::new(), &schema)?, schema)
                    }
                };
                let b = arrow_batch(&schema, b.clone())?; // (no view is sent: the cast copies out only its rows' strings)
                // (the piece's bytes go into a Vec reserved for their size once, not grown a copy at a time: its rows' bytes,
                // and room for the message's header and each buffer's padding)
                w.get_mut().reserve(size(&b) + 64 * 3 * b.num_columns() + 4096);
                w.write(&b)?;
                let out = std::mem::take(w.get_mut());
                self.arrow = Some((w, schema));
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
                    Some((w, _)) => w,
                    None => StreamWriter::try_new(Vec::new(), &datafusion::arrow::datatypes::Schema::empty())?, // (no rows at all: as `answer::ipc` writes them)
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

/// `out` with `more` after it. The first piece's bytes are the answer's as they are, not copied into an empty buffer.
fn joined(mut out: Vec<u8>, more: Vec<u8>) -> Vec<u8> {
    if out.is_empty() {
        return more;
    }
    out.extend(more);
    out
}

/// A piece's rows as newline-delimited JSON (`render`'s own writer, one line a row).
fn lines(b: &RecordBatch) -> anyhow::Result<Vec<u8>> { json_lines([b]) }

// ---------------------------------------------------------------- rows as JSON
//
// The one place rows become JSON, for every door and every answer that carries rows inside it: Arrow's writer, so a
// value is written the same way wherever it goes (a NULL left out of its row's object, but where an answer asks for it
// said: `json_first`). `json` is an array of row objects (`[]` for none), `json_lines` one object a line (NDJSON),
// `json_each` each row's object on its own, and `json_values` the first rows as values, for an answer that holds them
// inside JSON of its own.

/// Rows as a JSON array of objects.
pub fn json<'a>(batches: impl IntoIterator<Item = &'a RecordBatch>) -> Result<Vec<u8>> { json_array(batches, false) }

/// Rows as a JSON array of objects, a NULL as `null` (`nulls`) or left out.
fn json_array<'a>(batches: impl IntoIterator<Item = &'a RecordBatch>, nulls: bool) -> Result<Vec<u8>> {
    let mut w = arrow_json::WriterBuilder::new().with_explicit_nulls(nulls).build::<_, arrow_json::writer::JsonArray>(Vec::new());
    for b in batches {
        w.write(b)?;
    }
    w.finish()?;
    Ok(w.into_inner())
}

/// Rows as newline-delimited JSON, one object a line.
pub fn json_lines<'a>(batches: impl IntoIterator<Item = &'a RecordBatch>) -> Result<Vec<u8>> {
    let mut w = arrow_json::LineDelimitedWriter::new(Vec::new());
    for b in batches {
        w.write(b)?;
    }
    w.finish()?;
    Ok(w.into_inner())
}

/// Each row's JSON object on its own, in order.
pub fn json_each(b: &RecordBatch) -> Result<Vec<Vec<u8>>> {
    Ok(json_lines([b])?.split(|&c| c == b'\n').take(b.num_rows()).map(<[u8]>::to_vec).collect())
}

/// The first `n` rows as JSON values (only those rows are written). Parsed back, a number keeps what a double holds:
/// an answer that carries rows to a reader takes `json_first`'s text instead.
pub fn json_values(batches: &[RecordBatch], n: usize) -> Result<Vec<Value>> { Ok(serde_json::from_slice(&json(&first(batches, n))?)?) }

/// The first `n` rows as a JSON array, as written: for an answer that holds them inside JSON of its own, put in as they
/// are (`serde_json::value::RawValue`), so a value reads as every door writes it (a DECIMAL's every digit). With `nulls`,
/// each row has every column, a NULL as `null`: MCP's, so an agent tells a NULL from a column that isn't there.
pub fn json_first(batches: &[RecordBatch], n: usize, nulls: bool) -> Result<Box<serde_json::value::RawValue>> {
    Ok(serde_json::value::RawValue::from_string(String::from_utf8(json_array(&first(batches, n), nulls)?)?)?)
}

/// The batches' first `n` rows.
fn first(batches: &[RecordBatch], n: usize) -> Vec<RecordBatch> {
    let mut left = n;
    batches.iter().map_while(|b| (left > 0).then(|| {
        let take = left.min(b.num_rows());
        left -= take;
        b.slice(0, take)
    })).collect()
}

/// What keeps an answer's rows for its later pages and says the id they are kept under (`pages::keep` on a node; a
/// page keeps none). Only the console's typed answer asks it.
pub type Keep = fn(&[RecordBatch]) -> Option<String>;

/// Rows as `?format=` asks: JSON, a text table, or Arrow IPC (straight into pandas, Polars and
/// DuckDB, no JSON parsing); or a file of them to download: CSV, TSV, NDJSON, Parquet, Excel.
pub fn render(batches: &[RecordBatch], format: Option<&str>, keep: Keep) -> anyhow::Result<bytes::Bytes> {
    let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(Schema::empty()));
    Ok(bytes::Bytes::from((self::format(format).whole)(&schema, batches, &Status { keep })?))
}

/// An Arrow stream: the pieces a door sends an answer in, one after another (the same bytes as they make).
fn arrow_stream(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut enc = Encoding::new(Some("arrow"));
    let mut out = vec![];
    for piece in pieces(batches.iter().map(|b| enc.lean(b.clone()))) {
        out = joined(out, enc.step(Some(&piece))?);
    }
    Ok(joined(out, enc.step(None)?))
}

/// CSV (or TSV, by its delimiter), with a header.
fn csv(batches: &[RecordBatch], delimiter: u8) -> Result<Vec<u8>> {
    let mut w = datafusion::arrow::csv::WriterBuilder::new().with_header(true).with_delimiter(delimiter).build(Vec::new());
    for b in batches {
        w.write(b)?;
    }
    Ok(w.into_inner())
}

/// A Parquet file of the rows (intervals as text: `storable`).
fn parquet(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let batches = batches.iter().map(storable).collect::<Result<Vec<_>>>()?;
    let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| Arc::new(Schema::empty()));
    let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None)?;
    for b in &batches {
        w.write(b)?;
    }
    Ok(w.into_inner()?)
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
    let mut left = shown;
    let mut written = vec![];
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        let b = b.slice(0, left.min(b.num_rows()));
        left -= b.num_rows();
        let exact: Vec<datafusion::arrow::array::ArrayRef> = b.columns().iter().map(exact).collect::<Result<_, _>>()?;
        let numbered: Vec<_> = b.schema().fields().iter().zip(&exact).enumerate().map(|(i, (f, c))| f.as_ref().clone().with_name(i.to_string()).with_data_type(c.data_type().clone())).collect(); // (unique keys)
        written.push(RecordBatch::try_new(Arc::new(datafusion::arrow::datatypes::Schema::new(numbered)), exact)?);
        if left == 0 {
            break;
        }
    }
    let objects: Vec<serde_json::Map<String, Value>> = serde_json::from_slice(&json(&written)?).unwrap_or_default();
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

/// The schema an answer's Arrow stream is written with, on every door: each field as Flight's encoder sends it
/// (`sent_field`), and every column nullable. The plan's nullability and the run's differ where the optimizer folds an
/// expression (`upper('x')` becomes a literal, never NULL), and where the plan says NOT NULL the run gives NULL (a scalar
/// subquery takes its inner field's nullability: `SELECT (SELECT 1 WHERE false)`); table scans are nullable anyway
/// (`schema`). Clients compare a stream's schema with the one its flight's info announced, so both say this.
pub fn arrow_schema(schema: &Schema) -> Schema {
    let fields = schema.fields().iter().map(|f| sent_field(f).with_nullable(true)).collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// A field as the encoder sends it (arrow-flight's `prepare_field_for_flight`, in its default, hydrated form):
/// a dictionary is its values' type, and a string or binary view its string or binary type, at any depth. Its name,
/// nullability and metadata stay as they are.
fn sent_field(f: &Field) -> Field { Field::new(f.name(), sent_type(f.data_type()), f.is_nullable()).with_metadata(f.metadata().clone()) }

fn sent_type(t: &DataType) -> DataType {
    match t {
        DataType::Dictionary(_, values) => sent_type(values),
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::List(f) => DataType::List(Arc::new(sent_field(f))),
        DataType::LargeList(f) => DataType::LargeList(Arc::new(sent_field(f))),
        DataType::ListView(f) => DataType::ListView(Arc::new(sent_field(f))),
        DataType::LargeListView(f) => DataType::LargeListView(Arc::new(sent_field(f))),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(Arc::new(sent_field(f)), *n),
        DataType::Map(f, sorted) => DataType::Map(Arc::new(sent_field(f)), *sorted),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(|f| sent_field(f)).collect()),
        DataType::Union(fields, mode) => DataType::Union(fields.iter().map(|(id, f)| (id, Arc::new(sent_field(f)))).collect(), *mode),
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(run_ends.clone(), Arc::new(sent_field(values))),
        other => other.clone(),
    }
}

/// `b` taken to `schema` (an `arrow_schema`): as it is when the schema is a looser one than its own (`contains`: the same
/// fields with the same types, only nullable where the batch's may not be), else each column as it is when its type is
/// its field's, or cast to the field's type (a dictionary to its values, a view to its string), so the batch says what
/// the stream says.
pub fn arrow_batch(schema: &SchemaRef, b: RecordBatch) -> anyhow::Result<RecordBatch> {
    if schema.contains(b.schema_ref()) {
        return Ok(b.with_schema(schema.clone())?);
    }
    let columns = b
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(c, f)| match c.data_type() == f.data_type() {
            true => Ok(c.clone()),
            false => datafusion::arrow::compute::cast(c, f.data_type()),
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    Ok(RecordBatch::try_new_with_options(schema.clone(), columns, &datafusion::arrow::array::RecordBatchOptions::new().with_row_count(Some(b.num_rows())))?)
}

/// A batch as a door sends it in `format`, for counting before it is written: an Arrow answer's batch with a top-level
/// view taken to its `arrow_schema` (the cast copies out only its rows' strings, so nothing is left to compact), any
/// other batch `compact`ed.
pub fn as_sent(b: RecordBatch, format: Option<&str>) -> RecordBatch {
    let view = |f: &Arc<Field>| matches!(f.data_type(), DataType::Utf8View | DataType::BinaryView);
    if format != Some("arrow") || !b.schema_ref().fields().iter().any(view) {
        return compact(b);
    }
    let schema = Arc::new(arrow_schema(b.schema_ref()));
    arrow_batch(&schema, b.clone()).unwrap_or_else(|_| compact(b))
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

pub fn schema(columns: &[(String, String)]) -> Result<SchemaRef> { schema_of(columns.iter().map(|(n, t)| (n.as_str(), t.as_str()))) }

/// `schema`, of (name, Arrow type text) pairs however they are held (`TableMeta::columns`, of its list).
pub fn schema_of<'a>(columns: impl IntoIterator<Item = (&'a str, &'a str)>) -> Result<SchemaRef> {
    let fields = columns.into_iter().map(|(n, t)| Ok(Field::new(n, dtype(t)?, true)));
    Ok(Arc::new(Schema::new(fields.collect::<Result<Vec<_>>>()?)))
}

/// `schema_of` as queries read it: strings as views (Utf8View), so scans don't copy them and string filters and joins
/// take their fast paths (TPC-H runs about 8% faster). Tables store plain Utf8 (`query::read_schema`).
pub fn read_schema_of<'a>(columns: impl IntoIterator<Item = (&'a str, &'a str)>) -> Result<SchemaRef> {
    let fields = columns.into_iter().map(|(n, t)| Ok(Field::new(n, match dtype(t)? { DataType::Utf8 => DataType::Utf8View, d => d }, true)));
    Ok(Arc::new(Schema::new(fields.collect::<Result<Vec<_>>>()?)))
}

/// `df`'s columns under the names SQL knows them by: each column `shown` keeps is named as `name_of` gives it. A column
/// that keeps its name is passed as it is, never aliased to its own name, as SQL's planner does for `t.id AS id`:
/// DataFusion 55's push_down_leaf_projections, which takes a JSON or struct field down to the scan, adds `?table?.id`
/// beside such an alias's `id`, and refused the plan as ambiguous (`doc->'a'` on every keyed table, and on any table with
/// a renamed column). Used by a node's tables (`query::named`).
/// A column `sql_type` gives a type is read as that type (`relabel`): a mapped Delta table's struct, its fields' physical
/// names as SQL's.
pub fn renamed(df: DataFrame, name_of: impl Fn(&str) -> String, shown: impl Fn(&str) -> bool, sql_type: impl Fn(&str) -> Option<DataType>) -> datafusion::error::Result<DataFrame> {
    let keep: Vec<Expr> = df.schema().fields().iter().filter(|f| shown(f.name())).map(|f| {
        let name = name_of(f.name());
        let column = ident(f.name());
        match sql_type(f.name()) {
            Some(t) => relabel(column, t).alias(name),
            None if name == *f.name() => column,
            None => column.alias(name),
        }
    }).collect();
    df.select(keep)
}

/// `a` with its type's names as `to` says them: the same values, its structs' fields (at any depth, a list's items' too)
/// under `to`'s names, with `to`'s nullability and metadata (a field's Parquet id), no value copied; each leaf keeps its
/// own type. A mapped Delta table's struct is read so, its files' physical names as SQL's (`relabel`), and written so
/// the other way (`write_outside`).
pub fn relabeled(a: &ArrayRef, to: &DataType) -> datafusion::error::Result<ArrayRef> {
    use datafusion::arrow::array::{Array, AsArray, ListArray, StructArray};
    let named = |want: &Field, c: &ArrayRef| Arc::new(Field::new(want.name(), c.data_type().clone(), want.is_nullable()).with_metadata(want.metadata().clone()));
    Ok(match (a.data_type(), to) {
        (from, to) if from == to => a.clone(),
        (DataType::Struct(have), DataType::Struct(want)) if have.len() == want.len() => {
            let s = a.as_struct();
            let columns = s.columns().iter().zip(want.iter()).map(|(c, f)| relabeled(c, f.data_type())).collect::<datafusion::error::Result<Vec<_>>>()?;
            let fields: Vec<_> = want.iter().zip(&columns).map(|(f, c)| named(f, c)).collect();
            Arc::new(StructArray::try_new(fields.into(), columns, s.nulls().cloned())?)
        }
        (DataType::List(_), DataType::List(item)) => {
            let l = a.as_list::<i32>();
            let values = relabeled(l.values(), item.data_type())?;
            Arc::new(ListArray::try_new(named(item, &values), l.offsets().clone(), values, l.nulls().cloned())?)
        }
        _ => a.clone(),
    })
}

/// The type `relabeled` makes of a `from` array.
fn relabeled_type(from: &DataType, to: &DataType) -> DataType {
    let named = |want: &Field, t: DataType| Arc::new(Field::new(want.name(), t, want.is_nullable()).with_metadata(want.metadata().clone()));
    match (from, to) {
        (DataType::Struct(have), DataType::Struct(want)) if have.len() == want.len() => {
            DataType::Struct(have.iter().zip(want.iter()).map(|(h, w)| named(w, relabeled_type(h.data_type(), w.data_type()))).collect())
        }
        (DataType::List(have), DataType::List(item)) => DataType::List(named(item, relabeled_type(have.data_type(), item.data_type()))),
        _ => from.clone(),
    }
}

/// `e` read as `to` (`relabeled`): a function made for that one type, in a plan built here (`renamed`), never named in SQL.
pub fn relabel(e: Expr, to: DataType) -> Expr {
    use datafusion::logical_expr::{expr::ScalarFunction, ScalarUDF, Signature, Volatility};
    let f = ScalarUDF::new_from_impl(Relabel { to, signature: Signature::any(1, Volatility::Immutable) });
    Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(f), vec![e]))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Relabel {
    to: DataType,
    signature: datafusion::logical_expr::Signature,
}

impl datafusion::logical_expr::ScalarUDFImpl for Relabel {
    fn name(&self) -> &str { "pondra_relabel" }

    fn signature(&self) -> &datafusion::logical_expr::Signature { &self.signature }

    fn return_type(&self, args: &[DataType]) -> datafusion::error::Result<DataType> { Ok(relabeled_type(&args[0], &self.to)) }

    fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::logical_expr::ColumnarValue;
        Ok(match &args.args[0] {
            ColumnarValue::Array(a) => ColumnarValue::Array(relabeled(a, &self.to)?),
            ColumnarValue::Scalar(v) => ColumnarValue::Scalar(datafusion::common::ScalarValue::try_from_array(&relabeled(&v.to_array()?, &self.to)?, 0)?),
        })
    }
}

/// `df` with the columns `order` names in that order, in the places they hold among its columns (a table's columns in SQL's
/// order, `TableMeta::sql_order`); the others (the system columns) where they are.
pub fn reordered(df: DataFrame, order: &[String]) -> datafusion::error::Result<DataFrame> {
    let names: Vec<String> = df.schema().fields().iter().map(|f| f.name().clone()).collect();
    let mut next = order.iter().filter(|c| names.contains(c));
    let list: Vec<Expr> = names.iter().map(|n| if order.contains(n) { next.next().unwrap_or(n) } else { n }).map(|n| ident(n)).collect();
    df.select(list)
}

/// `b` with its columns as `reordered` puts a frame's: those `order` names in that order, in the places they hold.
pub fn batch_reordered(b: &RecordBatch, order: &[String]) -> datafusion::error::Result<RecordBatch> {
    let schema = b.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut next = order.iter().filter_map(|c| names.iter().position(|n| n == c));
    let picks: Vec<usize> = names.iter().enumerate().map(|(i, n)| if order.iter().any(|c| c == n) { next.next().unwrap_or(i) } else { i }).collect();
    Ok(b.project(&picks)?)
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
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray, StringViewArray};
    use datafusion::arrow::compute::{cast, concat_batches};
    use datafusion::arrow::datatypes::{Field, Fields};
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
    fn each_format_is_one_entry_and_json_is_the_default() {
        let names: Vec<&str> = FORMATS.iter().map(|f| f.name).collect();
        assert!(names.iter().enumerate().all(|(i, n)| !names[..i].contains(n)), "a name twice: {names:?}");
        assert_eq!(format(None).name, "json");
        assert_eq!(format(Some("nope")).name, "json", "a format this build doesn't have is JSON, as before");
        let streamed: Vec<&str> = FORMATS.iter().filter(|f| f.shape.is_some()).map(|f| f.name).collect();
        assert_eq!(streamed, ["json", "arrow", "csv", "tsv", "ndjson"]);
        assert_eq!(content_type(Some("typed")), "application/json");
        assert_eq!(content_type(Some("csv")), "text/csv; charset=utf-8");
        // (every format writes an answer with no rows, and one with rows)
        for f in FORMATS {
            render(&[], Some(f.name), |_| None).unwrap_or_else(|e| panic!("{}: no rows: {e}", f.name));
            assert!(!render(&[sample(3)], Some(f.name), |_| None).unwrap().is_empty(), "{}", f.name);
        }
    }

    #[test]
    fn pieces_are_the_whole() {
        let batch = sample(400_000);
        for f in FORMATS.iter().filter(|f| f.shape.is_some()).map(|f| Some(f.name)).chain([None]) {
            // (cut as a door cuts them: the batch as its format sends it, in `pieces`)
            let parts = pieces([Encoding::new(f).lean(batch.clone())]);
            assert!(parts.len() > 1, "{f:?}: a piece is about PIECE of Arrow: {} pieces", parts.len());
            let whole = render(&[batch.clone()], f, |_| None).unwrap();
            assert!(written(f, &parts) == whole.as_ref(), "{f:?}: the pieces are not the answer render writes");
        }
        // Arrow: the pieces and the whole decode to the rows as they are sent (a view as its string).
        let sent = as_sent(batch.clone(), Some("arrow"));
        let decoded = |bytes: &[u8]| -> Vec<RecordBatch> { StreamReader::try_new(Cursor::new(bytes), None).unwrap().map(|b| b.unwrap()).collect() };
        let same = |bs: &[RecordBatch]| concat_batches(&sent.schema(), bs).unwrap();
        let whole = render(&[batch.clone()], Some("arrow"), |_| None).unwrap();
        assert_eq!(same(&decoded(&written(Some("arrow"), &pieces([sent.clone()])))), sent);
        assert_eq!(same(&decoded(&whole)), sent);
        // An empty batch: JSON's [] and CSV's header alone, whole or in pieces.
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true), Field::new("b", DataType::Utf8, true)])));
        assert_eq!(render(&[empty.clone()], None, |_| None).unwrap().as_ref(), b"[]");
        assert_eq!(render(&[empty.clone()], Some("csv"), |_| None).unwrap().as_ref(), b"a,b\n");
        assert_eq!(written(None, &piece_rows(&empty)), b"[]");
        assert_eq!(written(Some("csv"), &piece_rows(&empty)), b"a,b\n");
    }

    fn dictionary() -> DataType { DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)) }

    #[test]
    fn arrow_schema_maps_dictionaries_to_their_values_at_any_depth() {
        let dict = dictionary();
        let schema = Schema::new(vec![
            Field::new("d", dict.clone(), false),
            Field::new("l", DataType::List(Arc::new(Field::new("item", dict.clone(), false))), false),
            Field::new("s", DataType::Struct(Fields::from(vec![Field::new("k", dict.clone(), true)])), false),
            Field::new("v", DataType::Utf8View, true),
            Field::new("b", DataType::BinaryView, true),
        ]);
        let out = arrow_schema(&schema);
        assert_eq!(out.field(0), &Field::new("d", DataType::Utf8, true));
        assert_eq!(out.field(1), &Field::new("l", DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))), true));
        assert_eq!(out.field(2), &Field::new("s", DataType::Struct(Fields::from(vec![Field::new("k", DataType::Utf8, true)])), true));
        assert_eq!(out.field(3), &Field::new("v", DataType::Utf8, true));
        assert_eq!(out.field(4), &Field::new("b", DataType::Binary, true));
    }

    #[test]
    fn arrow_schema_keeps_names_metadata_and_the_schema_metadata() {
        let field = Field::new("a", DataType::Int64, false).with_metadata([("k".to_string(), "v".to_string())].into());
        let schema = Schema::new_with_metadata(vec![field], [("s".to_string(), "t".to_string())].into());
        let out = arrow_schema(&schema);
        assert_eq!(out.field(0), &Field::new("a", DataType::Int64, true).with_metadata([("k".to_string(), "v".to_string())].into()));
        assert_eq!(out.metadata(), schema.metadata());
    }

    #[test]
    fn arrow_batch_takes_a_dictionary_batch_to_its_schema() -> anyhow::Result<()> {
        let dict = dictionary();
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("d", dict.clone(), false)])), vec![cast(&strings, &dict)?])?;
        let schema = Arc::new(arrow_schema(&batch.schema()));
        let out = arrow_batch(&schema, batch)?;
        assert_eq!(out.schema().as_ref(), schema.as_ref());
        assert_eq!(out.column(0).data_type(), &DataType::Utf8);
        Ok(())
    }

    #[test]
    fn arrow_batch_keeps_the_columns_it_need_not_cast_and_sends_a_view_as_its_string() -> anyhow::Result<()> {
        let batch = sample(10);
        let schema = Arc::new(arrow_schema(&batch.schema()));
        let out = arrow_batch(&schema, batch.clone())?;
        assert_eq!(out.schema().as_ref(), schema.as_ref());
        for (a, b) in out.columns().iter().zip(batch.columns()) {
            match b.data_type() {
                DataType::Utf8View => assert_eq!(a.as_ref(), cast(b, &DataType::Utf8)?.as_ref()),
                _ => assert!(Arc::ptr_eq(a, b)),
            }
        }
        Ok(())
    }

    #[test]
    fn small_batches_go_together_and_big_ones_are_cut() {
        let small: Vec<RecordBatch> = (0..10).map(|_| sample(8_192)).collect();
        let together = pieces(small.clone());
        assert_eq!(together, vec![concat_batches(&small[0].schema(), &small).unwrap()], "ten batches of 8,192 rows are one piece");
        let big = sample(400_000);
        let cut = pieces([sample(10), big.clone()]);
        assert!(cut.len() > 2, "a batch of {} MB is cut: {} pieces", big.get_array_memory_size() >> 20, cut.len());
        assert_eq!(concat_batches(&big.schema(), &cut).unwrap().num_rows(), 400_010);
        let apart = pieces([sample(10), sample(10).project(&[0]).unwrap()]);
        assert_eq!(apart.len(), 2, "batches of two schemas stay apart");
    }

    #[test]
    fn a_one_row_batch_is_one_piece_whatever_its_size() {
        let text = "x".repeat(PIECE + (1 << 20));
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef]).unwrap();
        assert_eq!(piece_rows(&batch), vec![batch.clone()]);
    }
}
