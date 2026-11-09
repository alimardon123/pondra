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
//! * A deleted file is listed in `pondra.dropped` (kind `file`) while a version of it is kept, with
//!   when and by whom it was deleted, and `UNDROP FILE 'path'` brings its newest version back.
//!
//! Bytes are `BINARY` columns: `byte_length`, `sha256`, `md5`, `encode(…, 'base64')`, `decode`
//! and `substr` (`engine.rs`: DataFusion's own takes only text) work on them.
use crate::lake_files::{self, under_files, Filed};
use crate::store::{Changed, Lake};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{MemTable, Session, TableFunctionImpl, TableProvider};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, TableType, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

const FETCHES: usize = 8; // objects read at once by file_read
const BIGGEST: usize = 256 << 20; // the largest object file_read will return

pub fn register(ctx: &SessionContext, lake: Arc<Lake>) {
    ctx.register_udtf("files", Arc::new(Listing(lake.clone())));
    ctx.register_udf(AsyncScalarUDF::new(Arc::new(FileRead { lake, signature: Signature::string(1, Volatility::Volatile) })).into_scalar_udf());
}

/// A path's refusal as DataFusion carries it: a coded one stays coded (`codes::of` finds it as the error's source), so the
/// client gets its SQLSTATE and not an internal error.
fn refusal(e: anyhow::Error) -> datafusion::error::DataFusionError {
    match e.downcast::<crate::codes::Coded>() {
        Ok(coded) => datafusion::error::DataFusionError::External(Box::new(coded)),
        Err(other) => datafusion::error::DataFusionError::External(other.into()),
    }
}

/// The objects under a prefix: `files('photos/')`, or `files()` for everything under `files/`.
#[derive(Debug)]
struct Listing(Arc<Lake>);

impl TableFunctionImpl for Listing {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let prefix = match args {
            [] => "files/".to_string(),
            [Expr::Literal(ScalarValue::Utf8(Some(p)) | ScalarValue::Utf8View(Some(p)), _)] => under_files(p).map_err(refusal)?,
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
            if [VERSIONS, APPLIES, DELETED].iter().any(|d| o.location.as_ref().starts_with(d) && !self.prefix.starts_with(d)) {
                continue; // (files' versions: `?versions` lists them; what each apply ran: `apply.rs`; deleted files' notes: `pondra.dropped`)
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
            if !paths.is_valid(i) {
                return anyhow::Ok(None);
            }
            let path = under_files(paths.value(i))?; // (a path the workspace keeps is refused, not read as a missing file)
            let bytes = self.lake.object(&path).await.ok();
            anyhow::Ok(bytes.filter(|b| b.len() <= BIGGEST))
        };
        let bytes: Vec<_> = futures::stream::iter((0..paths.len()).map(read)).buffered(FETCHES).try_collect().await.map_err(refusal)?;
        let mut out = BinaryBuilder::new();
        for b in bytes {
            out.append_option(b);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------- the four `/files` requests (ADR-061's contract)

/// The name the store keeps `path` under (`object_store`'s `Path`: empty segments go, `.` and `..` are escaped). The
/// checks below decide on it, so `files//.versions/x` is `files/.versions/x`.
fn stored_name(path: &str) -> String { String::from(object_store::path::Path::from(path)) }

/// Refuses a path a door may not write, restore or remove: a file's versions, each apply's files and deleted files'
/// notes are kept by Pondra (`keep`, `apply::keep_files`, `note_deleted`). Case-blind, as a disk that ignores case
/// keeps `.Versions` as `.versions`.
fn refuse_kept(path: &str) -> anyhow::Result<()> {
    let name = stored_name(path).to_ascii_lowercase();
    match [VERSIONS, APPLIES, DELETED].iter().any(|d| name.starts_with(d)) {
        true => Err(crate::codes::coded("42501", format!("{path}: a file's versions, each apply's files and deleted files' notes are kept by Pondra: name the file itself"))),
        false => Ok(()),
    }
}

/// `PUT /files/<path>`: an object in the lake next to the tables — an image, a PDF, a model, a SQL
/// file — for `files('…')` to list and `file_read(path)` to read. A path that exists is refused (409),
/// unless `if_match` names the version `GET` gave: then it is replaced, if nobody replaced it meanwhile
/// (412 if so; ADR-034). Each save is kept as a version too (`keep`, ADR-035 §8). A `.py` is an admin's to save,
/// and a path Pondra keeps is refused. Both doors call it.
pub async fn put(lake: &Lake, path: &str, if_match: Option<&str>, body: bytes::Bytes, by: &crate::auth::Principal) -> anyhow::Result<Filed> {
    let role = by.role;
    let (path, bytes) = (under_files(path)?, body.len());
    refuse_kept(&path)?;
    // (the HTTP door's gate (`Auth::needed`) asks the same of the raw path; this asks it of the name the store keeps, so the
    // two can't disagree. `role` is its door's caller's)
    if crate::auth::py_name(&stored_name(&path)) && role < crate::auth::Role::Admin {
        return Err(crate::codes::coded("42501", format!("{path}: code a node runs is saved by an admin")));
    }
    let expect = if_match.map(|v| v.trim_matches('"').to_string());
    let done = match &expect {
        Some(v) => lake.replace(&path, body.clone(), v).await, // (`Bytes`: the body itself, not a copy of it)
        None => match lake.put(&path, body.clone()).await {
            Ok(()) => lake.version(&path).await,
            Err(e) => Err(e),
        },
    };
    match done {
        Ok(version) => {
            if let Err(e) = keep(lake, &path, &body, &by.name).await {
                eprintln!("{path}: saved, but not kept as a version: {e:#}");
            }
            let etag = Some(version.clone()); // (the version written, as GET's `etag`: the next save's `If-Match`)
            Ok(Filed { etag, ..Filed::json(serde_json::json!({"path": path, "bytes": bytes, "version": version})) })
        }
        Err(e) if e.is::<Changed>() => Ok(Filed::text(412, lake_files::changed(&path))),
        Err(e) if expect.is_none() && format!("{e:#}").contains("already exists") => Ok(Filed::text(409, lake_files::there_already(&path))),
        Err(e) => Err(e),
    }
}

/// `GET /files/<path>`: that object's bytes, its version (`etag`, for `If-Match`) and a content type by its
/// name. `?versions`: the versions it keeps, newest first; `?version=<id>`: one's bytes. Both doors call it.
pub async fn get(lake: &Lake, path: &str, q: &std::collections::HashMap<String, String>) -> anyhow::Result<Filed> {
    let path = under_files(path)?;
    if q.contains_key("versions") {
        let all = versions(lake, &path).await?;
        return Ok(Filed::json(serde_json::Value::Array(all.iter().map(|v| serde_json::json!({"id": v.id, "at": v.ms, "who": v.who, "bytes": v.bytes})).collect())));
    }
    let found = match q.get("version") {
        Some(id) => match version(lake, &path, id).await {
            Ok(b) => Ok((b, id.clone())),
            Err(e) => return Ok(Filed::text(404, crate::ext::said(&e))), // (not one of its versions, or gone)
        },
        None => lake.file(&path).await,
    };
    let (body, tag) = match found {
        Ok(found) => found,
        Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) => {
            return Ok(Filed::text(404, lake_files::not_found(&path)));
        }
        Err(e) => return Err(e),
    };
    Ok(Filed { status: 200, content_type: lake_files::content_type(&path), etag: Some(tag), body })
}

/// `POST /files/<path>?restore=<id>`: that version made the file again (itself kept as the newest). Without
/// `restore` it is refused by name. Both doors call it.
pub async fn restore(lake: &Lake, path: &str, q: &std::collections::HashMap<String, String>, by: &crate::auth::Principal) -> anyhow::Result<Filed> {
    let path = under_files(path)?;
    refuse_kept(&path)?;
    let id = q.get("restore").ok_or_else(|| anyhow::anyhow!("POST /files/<path>?restore=<a version's id> (GET ?versions lists them)"))?;
    let bytes = version(lake, &path, id).await?;
    let now = match lake.version(&path).await {
        Ok(now) => lake.replace(&path, bytes.clone(), &now).await?,
        Err(_) => {
            lake.put(&path, bytes.clone()).await?; // (deleted since: back again)
            lake.version(&path).await?
        }
    };
    keep(lake, &path, &bytes, &by.name).await?;
    Ok(Filed::json(serde_json::json!({"path": path, "version": now, "restored": id})))
}

/// `DELETE /files/<path>`: that object gone (a writer's, as `PUT`); its versions stay. A path no file is at:
/// 404. Both doors call it; `by` (a name) deleted it.
pub async fn remove(lake: &Lake, path: &str, by: &str) -> anyhow::Result<Filed> {
    let path = under_files(path)?;
    refuse_kept(&path)?;
    match lake.remove_file(&path).await {
        Ok(()) => {
            if let Err(e) = note_deleted(lake, &path, by).await {
                eprintln!("{path}: deleted, but when and by whom not noted: {e:#}"); // (UNDROP FILE still brings it back)
            }
            Ok(Filed::json(serde_json::json!({"removed": path})))
        }
        Err(e) if e.downcast_ref::<object_store::Error>().is_some_and(|e| matches!(e, object_store::Error::NotFound { .. })) => {
            Ok(Filed::text(404, lake_files::not_found(&path)))
        }
        Err(e) => Err(e),
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

/// Keep this save of `path` as a version, by whoever made it (`by`, a name; none: the node's own); then let the oldest go.
pub async fn keep(lake: &Lake, path: &str, bytes: &bytes::Bytes, by: &str) -> anyhow::Result<()> {
    if bytes.len() > KEPT_MB << 20 || path.starts_with(VERSIONS) {
        return Ok(());
    }
    let who = who(by);
    let now = crate::log::now_ms();
    let mut kept = None;
    for ms in now..now + 8 {
        let id = format!("{ms:013}.{who}");
        match lake.put(&format!("{}{id}", kept_at(path)), bytes.clone()).await { // (the saved bytes shared, not copied)
            Err(e) if format!("{e:#}").contains("already exists") => continue, // (two saves in one millisecond: the next)
            done => {
                done?;
                kept = Some(id);
                break;
            }
        }
    }
    let (lake, path) = (lake.arc(), path.to_string());
    let node = lake.node.clone();
    node.tasks.spawn_node(crate::spawns::Vital::Stop, async move {
        if let Err(e) = prune(&lake, &path).await {
            eprintln!("{path}: its oldest versions not let go: {e:#}");
        }
        if let Some(kept) = kept {
            if let Err(e) = forget_deleted(&lake, &path, &kept).await {
                eprintln!("{path}: saved again, but its deletion's note not let go: {e:#}"); // (`deleted` passes a file that is there by)
            }
        }
    });
    Ok(())
}

/// A name as a version's name holds it (none: the node's own work, `node`).
fn who(by: &str) -> String {
    let who = if by.is_empty() { "node" } else { by };
    who.chars().map(|c| if c.is_ascii_alphanumeric() || "_.-".contains(c) { c } else { '_' }).collect()
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
    let (most, days) = (crate::env::value::<u64>("PONDRA_FILE_VERSIONS").max(1), crate::env::value::<u64>("PONDRA_FILE_VERSIONS_DAYS"));
    let since = crate::log::now_ms().saturating_sub(days * 86_400_000);
    let all = versions(lake, path).await?;
    for (i, v) in all.iter().enumerate().filter(|(_, v)| !v.id.contains('/')) {
        if i > 0 && (i as u64 >= most || v.ms < since) {
            lake.store.delete(&object_store::path::Path::from(format!("{}{}", kept_at(path), v.id))).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- deleted files (UNDROP FILE)

/// A deleted file's note: `<DELETED><path>/<its newest version's id>`, holding when it was deleted, by whom and that
/// version's size (`note_deleted`). Only a deleted file has one, so `pondra.dropped` lists this folder and not the
/// lake's files; a save lets its file's notes go (`forget_deleted`). A note is named by the version it would bring
/// back, so letting go of an earlier one never lets go of a later deletion's.
pub const DELETED: &str = "files/.deleted/";

/// Where `path`'s deletion notes are kept.
fn noted_at(path: &str) -> String { format!("{DELETED}{}/", path.trim_start_matches("files/")) }

#[derive(serde::Serialize, serde::Deserialize)]
struct Deletion {
    at: u64,
    by: String,
    bytes: u64, // (its newest version's, the note's name)
}

/// Note when `path` was deleted and by whom (`by`), when it keeps a version (a file kept nowhere can't come back).
async fn note_deleted(lake: &Lake, path: &str, by: &str) -> anyhow::Result<()> {
    use object_store::ObjectStoreExt;
    let Some(newest) = versions(lake, path).await?.into_iter().find(|v| !v.id.contains('/')) else { return Ok(()) };
    let note = serde_json::to_vec(&Deletion { at: crate::log::now_ms(), by: who(by), bytes: newest.bytes })?;
    lake.store.put(&object_store::path::Path::from(format!("{}{}", noted_at(path), newest.id)), bytes::Bytes::from(note).into()).await?;
    Ok(())
}

/// `path` is a file again, from the version `kept`: its notes from before it go. One named by a later version is a
/// later deletion's, so it stays.
async fn forget_deleted(lake: &Lake, path: &str, kept: &str) -> anyhow::Result<()> {
    use object_store::ObjectStoreExt;
    let at = object_store::path::Path::from(noted_at(path));
    for m in lake.store.list(Some(&at)).try_collect::<Vec<_>>().await? {
        let id = m.location.as_ref().strip_prefix(at.as_ref()).and_then(|r| r.strip_prefix('/')).filter(|id| !id.contains('/')); // (not a file's under it)
        if id.is_some_and(|id| id < kept) {
            match lake.store.delete(&m.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

/// A deleted file a version of which is kept: `pondra.dropped` lists it, and `UNDROP FILE` brings it back.
pub struct Deleted {
    pub path: String, // (under `files/`, as `PUT /files/<path>` named it)
    pub at_ms: u64,
    pub by: String,
    pub bytes: u64, // (its newest version's, which UNDROP FILE brings back)
}

impl Deleted {
    /// When its versions may be let go: `PONDRA_FILE_VERSIONS_DAYS` after it was deleted.
    pub fn kept_until(&self) -> u64 { self.at_ms + crate::env::value::<u64>("PONDRA_FILE_VERSIONS_DAYS") * 86_400_000 }
}

/// Every file deleted with a note, by path: one listing of the notes, then each one's file looked for and its note
/// read, 16 at a time. A file that is there again is passed by; so is a note that doesn't read (UNDROP FILE still
/// brings its file back).
pub async fn deleted(lake: &Lake) -> anyhow::Result<Vec<Deleted>> {
    let mut newest: std::collections::BTreeMap<String, (String, object_store::path::Path)> = std::collections::BTreeMap::new(); // (path: its latest note's id and place)
    for m in lake.store.list(Some(&object_store::path::Path::from(DELETED))).try_collect::<Vec<_>>().await? {
        let Some((path, id)) = m.location.as_ref().strip_prefix(DELETED).and_then(|r| r.rsplit_once('/')) else { continue };
        let path = percent_encoding::percent_decode_str(path).decode_utf8_lossy().into_owned(); // (as it was named: the store's spelling escapes some characters)
        match newest.get(&path) {
            Some((was, _)) if was.as_str() >= id => {}
            _ => {
                newest.insert(path, (id.to_string(), m.location.clone()));
            }
        }
    }
    let read = |(path, (_, at)): (String, (String, object_store::path::Path))| async move {
        use object_store::ObjectStoreExt;
        let file = format!("files/{path}");
        let (there, note) = futures::join!(lake.version(&file), async { lake.store.get(&at).await?.bytes().await });
        if there.is_ok() {
            return None;
        }
        let note: Deletion = match note.map_err(anyhow::Error::from).and_then(|b| Ok(serde_json::from_slice(&b)?)) {
            Ok(note) => note,
            Err(e) => {
                eprintln!("{path}: its deletion's note doesn't read: {e:#}");
                return None;
            }
        };
        Some(Deleted { path, at_ms: note.at, by: note.by, bytes: note.bytes })
    };
    let mut out: Vec<Deleted> = futures::stream::iter(newest).map(read).buffer_unordered(16).filter_map(|d| async { d }).collect().await;
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// `UNDROP FILE 'path'`: a deleted file's newest version made the file again (kept as its newest save, as `?restore=`
/// keeps one), and its deletion's note let go. Refused while a file is there (42710), and when no version of it is
/// kept (42704). Who may is the statement's rights' (`rights::asks`: a writer's, and a `.py`, code a node runs, an admin's);
/// the version kept is `by`'s save (a name).
pub async fn undrop(lake: &Lake, path: &str, by: &str) -> anyhow::Result<serde_json::Value> {
    let path = under_files(path)?;
    refuse_kept(&path)?;
    let named = path.trim_start_matches("files/").to_string();
    let there = || crate::codes::coded("42710", format!("{named} is there: UNDROP FILE brings back a deleted file. GET /files/{named}?versions lists its versions, and POST /files/{named}?restore=<id> makes one the file"));
    if lake.version(&path).await.is_ok() {
        return Err(there());
    }
    let Some(newest) = versions(lake, &path).await?.into_iter().next() else {
        return Err(crate::codes::coded("42704", format!("no deleted file {named} is kept (pondra.dropped lists those that are)")));
    };
    let bytes = version(lake, &path, &newest.id).await?;
    match lake.put(&path, bytes.clone()).await {
        Err(e) if format!("{e:#}").contains("already exists") => return Err(there()), // (saved since it was looked for)
        done => done?,
    }
    keep(lake, &path, &bytes, by).await?;
    use object_store::ObjectStoreExt;
    match lake.store.delete(&object_store::path::Path::from(format!("{}{}", noted_at(&path), newest.id))).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => eprintln!("{path}: back, but its deletion's note not let go: {e}"), // (`deleted` passes a file that is there by)
    }
    Ok(serde_json::json!({"undropped": named, "version": newest.id, "bytes": bytes.len()}))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `path` saved with `bytes`, kept as a version as `put` keeps each save (who may save is its door's).
    async fn save(lake: &Lake, path: &str, bytes: &'static [u8]) {
        use object_store::ObjectStoreExt;
        let (at, bytes) = (format!("files/{path}"), bytes::Bytes::from_static(bytes));
        lake.store.put(&object_store::path::Path::from(at.as_str()), bytes.clone().into()).await.unwrap();
        keep(lake, &at, &bytes, "tester").await.unwrap();
    }

    #[tokio::test]
    async fn a_deleted_file_is_listed_until_undrop_file_brings_its_newest_version_back() {
        let lake = Lake::open(&format!("memory://undrop-{}", uuid::Uuid::new_v4().simple()), true, false, crate::node_state::NodeState::unhosted()).await.unwrap();
        save(&lake, "notes/a.txt", b"one").await;
        save(&lake, "notes/a.txt", b"second").await;
        save(&lake, "b.txt", b"b").await;
        assert!(deleted(&lake).await.unwrap().is_empty(), "a file that is there is no deleted one");
        let code = |e: anyhow::Error| crate::codes::of(&e).to_string();
        let by = ""; // (the node's own: `who`)
        assert_eq!(undrop(&lake, "notes/a.txt", by).await.map_err(code).unwrap_err(), "42710", "refused while the file is there");

        remove(&lake, "notes/a.txt", by).await.unwrap();
        let gone = deleted(&lake).await.unwrap();
        assert_eq!(gone.iter().map(|d| (d.path.as_str(), d.bytes, d.by.as_str())).collect::<Vec<_>>(), [("notes/a.txt", 6, "node")]);
        assert_eq!(gone[0].kept_until(), gone[0].at_ms + 90 * 86_400_000);

        let back = undrop(&lake, "/files/notes/a.txt", by).await.unwrap();
        assert_eq!((back["undropped"].as_str(), back["bytes"].as_u64()), (Some("notes/a.txt"), Some(6)));
        assert_eq!(&lake.file("files/notes/a.txt").await.unwrap().0[..], b"second", "its newest version, back");
        assert!(deleted(&lake).await.unwrap().is_empty());
        assert_eq!(versions(&lake, "files/notes/a.txt").await.unwrap().len(), 3, "kept as its newest save too");

        assert_eq!(undrop(&lake, "never.txt", by).await.map_err(code).unwrap_err(), "42704");
        assert_eq!(undrop(&lake, ".versions/notes/a.txt", by).await.map_err(code).unwrap_err(), "42501", "a version is Pondra's, not a file");
        assert_eq!(undrop(&lake, ".deleted/notes/a.txt", by).await.map_err(code).unwrap_err(), "42501", "and so is a note");
        // (deleted again and saved again: the save lets its note go, after it answers)
        let second = versions(&lake, "files/notes/a.txt").await.unwrap().remove(1).id;
        remove(&lake, "notes/a.txt", by).await.unwrap();
        let noted = || async { lake.store.list(Some(&object_store::path::Path::from(DELETED))).try_collect::<Vec<_>>().await.unwrap().len() };
        assert_eq!(noted().await, 1);
        save(&lake, "notes/a.txt", b"third").await;
        for _ in 0..200 {
            if noted().await == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(noted().await, 0, "a save lets its file's notes go");
        // (a note named by a later version than a save kept is a later deletion's: it stays)
        remove(&lake, "notes/a.txt", by).await.unwrap();
        forget_deleted(&lake, "files/notes/a.txt", &second).await.unwrap();
        let gone = deleted(&lake).await.unwrap();
        assert_eq!((gone.len(), gone[0].bytes), (1, 5), "the later deletion's note, of the third version (5 bytes)");
    }
}
