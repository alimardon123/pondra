//! Files in the lake, next to the tables: images, PDFs, audio, models — whatever a table's rows
//! point at. The bytes stay in the bucket; a column holds the path, and a query fetches only the
//! rows it reads.
//!
//! * `SELECT * FROM files('photos/')` — path, size and when it was written, for the objects under
//!   a prefix (`files/` by default: what `PUT /files/…` wrote).
//! * `file_read(path)` — one object's bytes, `FETCHES` at a time, fetched only where a query
//!   needs them (`SELECT ai_caption(file_read(path)) FROM photos WHERE day = …`).
//! * `PUT /files/<path>` and `GET /files/<path>` — put an object there and read it back.
//! * Every file keeps its versions (ADR-035 §8): each save through `PUT /files` is kept too, at
//!   `files/.versions/<path>/<ms>.<who>` (hidden from `files()`), the newest
//!   `PONDRA_FILE_VERSIONS` (50) of them for `PONDRA_FILE_VERSIONS_DAYS` (90) days, and a file
//!   deleted keeps them. `GET /files/<path>?versions` lists them, `?version=<id>` reads one, and
//!   `POST /files/<path>?restore=<id>` makes one the file again. A notebook saved before, as
//!   `notebooks/<name>/<time>.ipynb`, has those as versions of `notebooks/<name>.ipynb`.
//!
//! Bytes are `BINARY` columns: `byte_length`, `sha256`, `md5`, `encode(…, 'base64')`, `decode`
//! and `substr` (here: DataFusion's own takes only text) work on them.
use crate::store::Lake;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{MemTable, Session, TableFunctionImpl, TableProvider};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TableType, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{create_udf, SessionContext};
use futures::StreamExt;
use std::sync::Arc;

const FETCHES: usize = 8; // objects read at once by file_read
const BIGGEST: usize = 256 << 20; // the largest object file_read will return

pub fn register(ctx: &SessionContext, lake: Arc<Lake>) {
    ctx.register_udtf("files", Arc::new(Listing(lake.clone())));
    // `octet_length` is for text; bytes have their own (DataFusion's other byte functions —
    // sha256, md5, encode, decode — take BINARY as they are; substr is below).
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
    ctx.register_udf(substr().as_ref().clone());
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(FileRead { lake, signature: Signature::string(1, Volatility::Volatile) })).into_scalar_udf());
}

/// `substr` and `SUBSTRING(… FROM … FOR …)` for bytes and text: planned here, ahead of
/// DataFusion's own planner, which knows only the text one.
pub fn planner() -> Arc<dyn ExprPlanner> {
    Arc::new(Substrings)
}

#[derive(Debug)]
struct Substrings;

impl ExprPlanner for Substrings {
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(substr().call(args)))
    }
}

fn substr() -> Arc<ScalarUDF> {
    static SUBSTR: std::sync::LazyLock<Arc<ScalarUDF>> = std::sync::LazyLock::new(|| {
        let signature = Signature::user_defined(Volatility::Immutable).with_parameter_names(vec!["str", "start_pos", "length"]).expect("names"); // (DataFusion's: `substr(str => …, start_pos => …)`)
        Arc::new(ScalarUDF::new_from_impl(Substr { text: datafusion::functions::unicode::substr(), signature }))
    });
    SUBSTR.clone()
}

/// `substr(bytes, from [, count])` on BINARY, 1-based like the text one; text goes to DataFusion's.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Substr {
    text: Arc<ScalarUDF>,
    signature: Signature,
}

fn bytes(t: &DataType) -> bool {
    matches!(t, DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_))
}

impl ScalarUDFImpl for Substr {
    fn name(&self) -> &str {
        "substr"
    }
    fn aliases(&self) -> &[String] {
        self.text.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [b, rest @ ..] if bytes(b) && (1..=2).contains(&rest.len()) => Ok([DataType::Binary].into_iter().chain(rest.iter().map(|_| DataType::Int64)).collect()),
            _ => {
                let fields: Vec<_> = types.iter().map(|t| Arc::new(Field::new("a", t.clone(), true))).collect();
                Ok(datafusion::logical_expr::type_coercion::functions::fields_with_udf(&fields, self.text.as_ref())?.iter().map(|f| f.data_type().clone()).collect())
            }
        }
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.first().is_some_and(bytes) { Ok(DataType::Binary) } else { self.text.return_type(types) }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.first().is_some_and(|a| bytes(&a.data_type())) {
            return self.text.invoke_with_args(args);
        }
        let n = args.number_rows;
        let arrays: Vec<ArrayRef> = args.args.iter().map(|a| a.to_array(n)).collect::<Result<_>>()?;
        let (b, from) = (arrays[0].as_binary::<i32>(), arrays[1].as_primitive::<datafusion::arrow::datatypes::Int64Type>());
        let count = arrays.get(2).map(|c| c.as_primitive::<datafusion::arrow::datatypes::Int64Type>());
        let mut out = BinaryBuilder::new();
        for i in 0..n {
            if b.is_null(i) || from.is_null(i) || count.is_some_and(|c| c.is_null(i)) {
                out.append_null();
                continue;
            }
            let (v, start) = (b.value(i), from.value(i));
            // As SQL's: characters before position 1 count against the length.
            let end = match count {
                Some(c) if c.value(i) < 0 => return exec_err!("negative substring length not allowed"),
                Some(c) => start.saturating_add(c.value(i)),
                None => i64::MAX,
            };
            let (lo, hi) = (start.max(1) - 1, (end - 1).clamp(0, v.len() as i64));
            out.append_value(if lo < hi { &v[lo as usize..hi as usize] } else { &[][..] });
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// The objects under a prefix: `files('photos/')`, or `files()` for everything under `files/`.
#[derive(Debug)]
struct Listing(Arc<Lake>);

impl TableFunctionImpl for Listing {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let prefix = match args {
            [] => "files/".to_string(),
            [Expr::Literal(ScalarValue::Utf8(Some(p)) | ScalarValue::Utf8View(Some(p)), _)] => under_files(p),
            _ => return plan_err!("files('<prefix>')"),
        };
        Ok(Arc::new(Files { lake: self.0.clone(), prefix, schema: files_schema() }))
    }
}

/// A path as `PUT /files/…` named it (`photos/a.jpg`), or as `files()` lists it (`files/photos/a.jpg`):
/// the object's place in the lake either way.
pub fn under_files(path: &str) -> String {
    let path = path.trim_start_matches('/');
    if path.starts_with("files/") { path.to_string() } else { format!("files/{path}") }
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
            if [VERSIONS, DEPLOYS].iter().any(|d| o.location.as_ref().starts_with(d) && !self.prefix.starts_with(d)) {
                continue; // (files' versions: `?versions` lists them; what each deploy ran: `deploy.rs`)
            }
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
            let path = paths.is_valid(i).then(|| under_files(paths.value(i)))?;
            let bytes = self.lake.object(&path).await.ok()?;
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

// ---------------------------------------------------------------- versions (ADR-035 §8)

pub const VERSIONS: &str = "files/.versions/";
pub const DEPLOYS: &str = "files/.deploys/"; // (each deploy's project as it ran: `deploy::keep_files`)
const KEPT_MB: usize = 64; // (a bigger file isn't kept twice)

/// Where `path`'s versions are kept.
fn kept_at(path: &str) -> String { format!("{VERSIONS}{}/", path.trim_start_matches("files/")) }

/// A notebook saved before versions, as `notebooks/<name>/<time>.ipynb`: where its saves are.
fn saved_before(path: &str) -> Option<String> {
    let name = path.strip_prefix("files/notebooks/")?.strip_suffix(".ipynb").filter(|n| !n.contains('/'))?;
    Some(format!("files/notebooks/{name}/"))
}

/// Keep this save of `path` as a version, by whoever made it; then let the oldest go.
pub async fn keep(lake: &Lake, path: &str, bytes: &[u8]) -> anyhow::Result<()> {
    if bytes.len() > KEPT_MB << 20 || path.starts_with(VERSIONS) {
        return Ok(());
    }
    let who = crate::auth::current().map(|p| p.name).filter(|n| !n.is_empty()).unwrap_or_else(|| "node".into());
    let who: String = who.chars().map(|c| if c.is_ascii_alphanumeric() || "_.-".contains(c) { c } else { '_' }).collect();
    let now = crate::log::now_ms();
    for ms in now..now + 8 {
        match lake.put(&format!("{}{ms:013}.{who}", kept_at(path)), bytes.to_vec()).await {
            Err(e) if format!("{e:#}").contains("already exists") => continue, // (two saves in one millisecond: the next)
            done => {
                done?;
                break;
            }
        }
    }
    let (lake, path) = (lake.arc(), path.to_string());
    crate::panics::spawn(async move {
        if let Err(e) = prune(&lake, &path).await {
            eprintln!("{path}: its oldest versions not let go: {e:#}");
        }
    });
    Ok(())
}

/// One version: its id (`<ms>.<who>`, or a notebook's earlier save by its path), when, by whom, size.
pub struct Version {
    pub id: String,
    pub ms: u64,
    pub who: String,
    pub bytes: u64,
}

/// `path`'s versions, newest first.
pub async fn versions(lake: &Lake, path: &str) -> anyhow::Result<Vec<Version>> {
    use futures::TryStreamExt;
    let list = |prefix: String| async move { lake.store.list(Some(&object_store::path::Path::from(prefix))).try_collect::<Vec<_>>().await };
    let mut out: Vec<Version> = list(kept_at(path)).await?.into_iter().filter_map(|m| {
        let name = m.location.filename()?.to_string();
        let (ms, who) = (name.get(..13)?, name.get(14..)?); // (`<ms>.<who>`: the time 13 digits)
        Some(Version { id: name.clone(), ms: ms.parse().ok()?, who: who.to_string(), bytes: m.size })
    }).collect();
    if let Some(before) = saved_before(path) {
        for m in list(before).await? {
            let rel = m.location.as_ref().trim_start_matches("files/").to_string();
            if rel.ends_with(".ipynb") && rel.matches('/').count() == 2 {
                out.push(Version { id: rel, ms: m.last_modified.timestamp_millis() as u64, who: String::new(), bytes: m.size });
            }
        }
    }
    out.sort_by(|a, b| (b.ms, &b.id).cmp(&(a.ms, &a.id))); // (saves in the same millisecond: by id, which is dated)
    Ok(out)
}

/// One version's bytes.
pub async fn version(lake: &Lake, path: &str, id: &str) -> anyhow::Result<bytes::Bytes> {
    let at = match saved_before(path) {
        Some(before) if id.contains('/') => format!("files/{id}").starts_with(&before).then(|| format!("files/{id}")), // (only this notebook's)
        _ => (!id.contains('/')).then(|| format!("{}{id}", kept_at(path))),
    };
    let at = at.filter(|_| !id.contains("..")).ok_or_else(|| anyhow::anyhow!("{id} is not a version of {path}"))?;
    use object_store::ObjectStoreExt;
    Ok(lake.store.get(&object_store::path::Path::from(at)).await?.bytes().await?)
}

/// Let the oldest go: past the newest `PONDRA_FILE_VERSIONS`, or older than
/// `PONDRA_FILE_VERSIONS_DAYS` (the newest is always kept). A notebook's earlier saves stay.
async fn prune(lake: &Lake, path: &str) -> anyhow::Result<()> {
    use object_store::ObjectStoreExt;
    let var = |v: &str, d: u64| std::env::var(v).ok().and_then(|n| n.parse().ok()).unwrap_or(d);
    let (most, days) = (var("PONDRA_FILE_VERSIONS", 50).max(1), var("PONDRA_FILE_VERSIONS_DAYS", 90));
    let since = crate::log::now_ms().saturating_sub(days * 86_400_000);
    let all = versions(lake, path).await?;
    for (i, v) in all.iter().enumerate().filter(|(_, v)| !v.id.contains('/')) {
        if i > 0 && (i as u64 >= most || v.ms < since) {
            lake.store.delete(&object_store::path::Path::from(format!("{}{}", kept_at(path), v.id))).await?;
        }
    }
    Ok(())
}
