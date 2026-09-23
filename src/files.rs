//! Files in the lake, next to the tables: images, PDFs, audio, models — whatever a table's rows
//! point at. The bytes stay in the bucket; a column holds the path, and a query fetches only the
//! rows it reads.
//!
//! * `SELECT * FROM files('photos/')` — path, size and when it was written, for the objects under
//!   a prefix (`files/` by default: what `PUT /files/…` wrote).
//! * `file_read(path)` — one object's bytes, `FETCHES` at a time, fetched only where a query
//!   needs them (`SELECT ai_caption(file_read(path)) FROM photos WHERE day = …`).
//! * `PUT /files/<path>` and `GET /files/<path>` — put an object there and read it back.
//!
//! Bytes are `BINARY` columns: `octet_length`, `sha256`, `md5`, `encode(…, 'base64')`,
//! `substr`, `decode` and the rest of DataFusion's byte functions work on them.
use crate::store::Lake;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{MemTable, Session, TableFunctionImpl, TableProvider};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, TableType, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{create_udf, SessionContext};
use futures::StreamExt;
use std::sync::Arc;

const FETCHES: usize = 8; // objects read at once by file_read
const BIGGEST: usize = 256 << 20; // the largest object file_read will return

pub fn register(ctx: &SessionContext, lake: Arc<Lake>) {
    ctx.register_udtf("files", Arc::new(Listing(lake.clone())));
    // `octet_length` is for text; bytes have their own (DataFusion's other byte functions —
    // sha256, md5, encode, decode, substr — take BINARY as they are).
    let bytes = |args: &[ColumnarValue]| {
        let arrays: Vec<ArrayRef> = args.iter().map(|a| match a {
            ColumnarValue::Array(a) => Ok(a.clone()),
            ColumnarValue::Scalar(s) => s.to_array(),
        }).collect::<Result<_>>()?;
        let binary = datafusion::arrow::compute::cast(&arrays[0], &DataType::Binary)?;
        let binary = binary.as_binary::<i32>();
        let lengths = (0..binary.len()).map(|i| binary.is_valid(i).then(|| binary.value(i).len() as i64));
        Ok(ColumnarValue::Array(Arc::new(lengths.collect::<Int64Array>())))
    };
    ctx.register_udf(create_udf("byte_length", vec![DataType::Binary], DataType::Int64, Volatility::Immutable, Arc::new(bytes)));
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(FileRead { lake, signature: Signature::string(1, Volatility::Volatile) })).into_scalar_udf());
}

/// The objects under a prefix: `files('photos/')`, or `files()` for everything under `files/`.
#[derive(Debug)]
struct Listing(Arc<Lake>);

impl TableFunctionImpl for Listing {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let prefix = match args {
            [] => "files/".to_string(),
            [Expr::Literal(ScalarValue::Utf8(Some(p)) | ScalarValue::Utf8View(Some(p)), _)] => p.clone(),
            _ => return plan_err!("files('<prefix>')"),
        };
        Ok(Arc::new(Files { lake: self.0.clone(), prefix, schema: files_schema() }))
    }
}

fn files_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("written", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]))
}

struct Files {
    lake: Arc<Lake>,
    prefix: String,
    schema: SchemaRef,
}

impl std::fmt::Debug for Files {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Files({})", self.prefix) }
}

#[async_trait::async_trait]
impl TableProvider for Files {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, _: &[Expr], _: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        let (mut paths, mut sizes, mut written) = (vec![], vec![], vec![]);
        let mut list = self.lake.store.list(Some(&object_store::path::Path::from(self.prefix.as_str())));
        while let Some(o) = list.next().await {
            let o = o.map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
            paths.push(o.location.to_string());
            sizes.push(o.size as i64);
            written.push(o.last_modified.timestamp_micros());
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(paths)),
            Arc::new(Int64Array::from(sizes)),
            Arc::new(TimestampMicrosecondArray::from(written)),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        MemTable::try_new(self.schema.clone(), vec![vec![batch]])?.scan(state, projection, &[], None).await
    }
}

/// `file_read(path)`: an object's bytes (null if it isn't there).
#[derive(Debug)]
struct FileRead {
    lake: Arc<Lake>,
    signature: Signature,
}

impl PartialEq for FileRead {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.lake, &other.lake) }
}
impl Eq for FileRead {}
impl std::hash::Hash for FileRead {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) { self.signature.hash(h) }
}

impl ScalarUDFImpl for FileRead {
    fn name(&self) -> &str { "file_read" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Binary) }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> { exec_err!("file_read is asynchronous") }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for FileRead {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let paths = match args.args.first() {
            Some(ColumnarValue::Array(a)) => datafusion::arrow::compute::cast(a, &DataType::Utf8)?,
            Some(ColumnarValue::Scalar(s)) => s.to_array_of_size(args.number_rows)?,
            None => return exec_err!("file_read(path)"),
        };
        let paths = paths.as_string::<i32>();
        let read = |i: usize| async move {
            let path = paths.is_valid(i).then(|| paths.value(i))?;
            let bytes = self.lake.object(path).await.ok()?;
            (bytes.len() <= BIGGEST).then_some(bytes)
        };
        let bytes: Vec<_> = futures::stream::iter((0..paths.len()).map(read)).buffered(FETCHES).collect().await;
        let mut out = BinaryBuilder::new();
        for b in bytes {
            out.append_option(b);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}
