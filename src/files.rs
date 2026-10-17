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
//! and `substr` (`engine.rs`: DataFusion's own takes only text) work on them.
use crate::lake_files::under_files;
use crate::store::Lake;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{MemTable, Session, TableFunctionImpl, TableProvider};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, TableType, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::sync::Arc;

const FETCHES: usize = 8; // objects read at once by file_read
const BIGGEST: usize = 256 << 20; // the largest object file_read will return

pub fn register(ctx: &SessionContext, lake: Arc<Lake>) {
    ctx.register_udtf("files", Arc::new(Listing(lake.clone())));
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(FileRead { lake, signature: Signature::string(1, Volatility::Volatile) })).into_scalar_udf());
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
            if [VERSIONS, APPLIES].iter().any(|d| o.location.as_ref().starts_with(d) && !self.prefix.starts_with(d)) {
                continue; // (files' versions: `?versions` lists them; what each apply ran: `apply.rs`)
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
// (the folder keeps the old word, deploy: its files are stored paths, which never change)
pub const APPLIES: &str = "files/.deploys/"; // (each apply's project as it ran: `apply::keep_files`)
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
    let (most, days) = (crate::env::or::<u64>("PONDRA_FILE_VERSIONS", 50).max(1), crate::env::or::<u64>("PONDRA_FILE_VERSIONS_DAYS", 90));
    let since = crate::log::now_ms().saturating_sub(days * 86_400_000);
    let all = versions(lake, path).await?;
    for (i, v) in all.iter().enumerate().filter(|(_, v)| !v.id.contains('/')) {
        if i > 0 && (i as u64 >= most || v.ms < since) {
            lake.store.delete(&object_store::path::Path::from(format!("{}{}", kept_at(path), v.id))).await?;
        }
    }
    Ok(())
}
