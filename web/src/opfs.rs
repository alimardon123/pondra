//! The page's store over the origin's private file system (OPFS, designs/l2-b4-web.md section 4j): one folder of it, which
//! is the lake's store of a kept page (`Page::kept`). A page keeps its files and its tables there, so they are there after
//! a reload. A folder is one page's at a time: `Page::kept` holds a Web Lock on it (`js::lock`), which is what makes one
//! writer at a time across tabs, so two tabs never write over each other's tables. Inside the module, each object's
//! changes take its lock (`hold`), so a put that creates an object, or replaces one version, is checked and written as one
//! step, even when two requests for the same object are in flight at once.
//!
//! A write is whole or not at all. Over an existing file, Chromium keeps the old bytes until the new ones are closed. A
//! new object is written under a temporary name (`{name}#writing`: object_store encodes `#`, so no object has one) and
//! moved to its name, so a page closed mid-write leaves no empty object behind. A version is the file's `lastModified`
//! and size, so a write waits until the clock has passed an object's last change: the same size written in the same
//! millisecond would keep the old version.

use crate::js;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, PoisonError};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;

/// The most bytes one part of a read holds: a large file is read a part at a time, as the stream is polled.
const PART: u64 = 4 << 20;

/// The objects being changed now, each with the lock its changes take in turn (`hold`). An entry goes once no change
/// holds or waits for it.
type Changing = std::sync::Mutex<std::collections::HashMap<Path, std::sync::Arc<futures::lock::Mutex<()>>>>;

/// One folder of the origin's private file system, its objects the files under it. The name is checked by the page
/// (`lib.rs`). Each operation reaches the folder afresh: a JavaScript handle is never kept.
#[derive(Debug)]
pub struct Opfs {
    folder: String,
    changing: Arc<Changing>,
}

impl Opfs {
    pub fn new(folder: String) -> Opfs {
        Opfs { folder, changing: Arc::default() }
    }
}

/// A change's hold on one object: until it is dropped, no other change to the object is checked or written.
struct Held {
    guard: Option<futures::lock::OwnedMutexGuard<()>>,
    at: Path,
    changing: Arc<Changing>,
}

impl Drop for Held {
    fn drop(&mut self) {
        // (the guard goes first: the entry's count falls only once this hold's lock is let go)
        drop(self.guard.take());
        let mut map = self.changing.lock().unwrap_or_else(PoisonError::into_inner);
        if map.get(&self.at).is_some_and(|entry| Arc::strong_count(entry) == 1) {
            map.remove(&self.at);
        }
    }
}

/// Waits for the lock on an object's changes, which take it in turn, and holds it until the `Held` is dropped. The
/// object's entry is made if there is none. The std mutex is let go before the wait: it is never held across an await.
async fn hold(changing: &Arc<Changing>, at: &Path) -> Held {
    let entry = {
        let mut map = changing.lock().unwrap_or_else(PoisonError::into_inner);
        map.entry(at.clone()).or_insert_with(|| Arc::new(futures::lock::Mutex::new(()))).clone()
    };
    let guard = entry.lock_owned().await;
    Held { guard: Some(guard), at: at.clone(), changing: changing.clone() }
}

impl fmt::Display for Opfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Opfs({})", self.folder)
    }
}

/// Whether this page has the origin's private file system: `navigator.storage.getDirectory`, in a page or a worker. Node
/// has no `navigator.storage`, and neither has a browser without OPFS.
pub fn available() -> bool {
    let global = JsValue::from(js_sys::global());
    let storage = js::get(&global, "navigator").and_then(|navigator| js::get(&navigator, "storage"));
    storage.and_then(|storage| js::get(&storage, "getDirectory")).is_ok_and(|method| method.is_function())
}

#[async_trait]
impl ObjectStore for Opfs {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> StoreResult<PutResult> {
        let chunks: Vec<Bytes> = payload.into_iter().collect();
        // (the hold is taken inside the work: `step` runs the work to its end, even if this caller is dropped)
        let (folder, changing, at) = (self.folder.clone(), self.changing.clone(), location.clone());
        let outcome = step(location, async move {
            let _held = hold(&changing, &at).await;
            put(folder, at, opts.mode, chunks).await
        })
        .await?;
        match outcome {
            Put::Written(e_tag) => Ok(PutResult { e_tag: Some(e_tag), version: None, extensions: Default::default() }),
            Put::Exists => Err(Error::AlreadyExists {
                path: location.to_string(),
                source: "an object is there already, and this put is to create one".into(),
            }),
            Put::Stale => Err(Error::Precondition {
                path: location.to_string(),
                source: "the object is not there, or its version is not the one asked".into(),
            }),
        }
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        Err(unsupported("a page's objects are put whole: OPFS has no multipart upload"))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let folder = self.folder.clone();
        let at = location.clone();
        let meta = step(location, async move { meta_at(&folder, &at).await }).await?;
        options.check_preconditions(&meta)?;
        // (what is read, and whether a body is wanted, is `js::read_of`'s)
        let read = js::read_of(options);
        let range = answered(&read, meta.size, location)?;
        let payload = if read.body {
            let e_tag = meta.e_tag.clone().unwrap_or_default();
            parts(self.folder.clone(), location.clone(), e_tag, range.clone())
        } else {
            stream::empty().boxed()
        };
        Ok(GetResult { payload: GetResultPayload::Stream(payload), meta, range, attributes: Attributes::default(), extensions: Default::default() })
    }

    fn delete_stream(&self, locations: BoxStream<'static, StoreResult<Path>>) -> BoxStream<'static, StoreResult<Path>> {
        let (folder, changing) = (self.folder.clone(), self.changing.clone());
        locations
            .then(move |location| {
                let (folder, changing) = (folder.clone(), changing.clone());
                async move {
                    let at = location?;
                    let removing = at.clone();
                    step(&at, async move {
                        let _held = hold(&changing, &removing).await;
                        remove_at(folder, removing).await
                    })
                    .await?;
                    Ok(at)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        let (folder, names) = (self.folder.clone(), names_under(prefix));
        let at = prefix.cloned().unwrap_or_default();
        let found = async move { step(&at, files_under(folder, names)).await };
        stream::once(found)
            .map_ok(|metas| stream::iter(metas.into_iter().map(Ok::<ObjectMeta, Error>)))
            .try_flatten()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        let (folder, names) = (self.folder.clone(), names_under(prefix));
        let at = prefix.cloned().unwrap_or_default();
        let level = step(&at, level_of(folder, names)).await?;
        Ok(ListResult { common_prefixes: level.folders, objects: level.files, extensions: Default::default() })
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> StoreResult<()> {
        Err(unsupported("OPFS has no copy: a page's objects are put again, not copied"))
    }
}

/// Why a step of a store's work failed: the object is not there (the browser's `NotFoundError`, or `TypeMismatchError`,
/// a file where a folder is or the other way), or the browser's words.
enum Failed {
    Missing,
    Words(String),
}

impl From<JsValue> for Failed {
    fn from(e: JsValue) -> Failed {
        failed(e)
    }
}

impl From<String> for Failed {
    fn from(words: String) -> Failed {
        Failed::Words(words)
    }
}

/// A JavaScript failure as a `Failed`: the browser names a missing object in the exception's `name`, and a write over the
/// site's quota as `QuotaExceededError`, which is said in the words the page keeps (`page::FULL`).
fn failed(e: JsValue) -> Failed {
    let name = js::get(&e, "name").ok().and_then(|name| name.as_string()).unwrap_or_default();
    match name.as_str() {
        "NotFoundError" | "TypeMismatchError" => Failed::Missing,
        "QuotaExceededError" => Failed::Words(format!("{}: {}", pondra::page::FULL, js::js_words(e))),
        _ => Failed::Words(js::js_words(e)),
    }
}

/// One step of a store's work, run on the page's thread: its failure is the store's error for `at`. The work goes to the
/// page's thread here, not inside an async block: a block that held it would hold a future that is not `Send`.
fn step<T: Send + 'static>(
    at: &Path,
    work: impl Future<Output = Result<T, Failed>> + 'static,
) -> impl Future<Output = StoreResult<T>> + Send {
    let (at, done) = (at.clone(), js::sent(work));
    async move { done.await.map_err(|_| dropped(&at))?.map_err(|failure| to_error(&at, failure)) }
}

/// A missing object is `NotFound`; anything else is the browser's words.
fn to_error(at: &Path, failure: Failed) -> Error {
    match failure {
        Failed::Missing => Error::NotFound { path: at.to_string(), source: "no such object in the page's folder".into() },
        Failed::Words(words) => Error::Generic { store: "opfs", source: words.into() },
    }
}

fn dropped(at: &Path) -> Error {
    Error::Generic { store: "opfs", source: format!("{at}: the browser dropped the operation").into() }
}

fn unsupported(text: &str) -> Error {
    Error::NotSupported { source: text.into() }
}

/// The page's folder, from the origin's private file system: `navigator.storage.getDirectory()`, then the folder's name
/// (made if it is not there yet).
async fn folder_of(folder: &str) -> Result<JsValue, Failed> {
    let global = JsValue::from(js_sys::global());
    let storage = js::get(&js::get(&global, "navigator")?, "storage")?;
    let root = js::awaited(&storage, "getDirectory", &[]).await?;
    Ok(js::awaited(&root, "getDirectoryHandle", &[folder.into(), create_option(true)?]).await?)
}

/// The folder at `dirs` below the page's folder: each is made when `create`, else it must be there (`Missing` if not).
async fn dir_at(folder: &str, dirs: &[String], create: bool) -> Result<JsValue, Failed> {
    let mut dir = folder_of(folder).await?;
    for name in dirs {
        dir = js::awaited(&dir, "getDirectoryHandle", &[name.as_str().into(), create_option(create)?]).await?;
    }
    Ok(dir)
}

/// The folder at `dirs`, or None when it is not there: a listing of a folder that is not there lists nothing.
async fn listed_dir(folder: &str, dirs: &[String]) -> Result<Option<JsValue>, Failed> {
    match dir_at(folder, dirs, false).await {
        Ok(dir) => Ok(Some(dir)),
        Err(Failed::Missing) => Ok(None),
        Err(other) => Err(other),
    }
}

/// The file `name` in a folder: made (empty, for now) when `create`.
async fn file_at(dir: &JsValue, name: &str, create: bool) -> Result<JsValue, Failed> {
    Ok(js::awaited(dir, "getFileHandle", &[name.into(), create_option(create)?]).await?)
}

/// The `{create}` option of getDirectoryHandle and getFileHandle.
fn create_option(create: bool) -> Result<JsValue, String> {
    let options = JsValue::from(js_sys::Object::new());
    js::set(&options, "create", create)?;
    Ok(options)
}

/// Removes an entry of a folder (a file, or a folder left empty): `removeEntry(name)`.
async fn remove(dir: &JsValue, name: &str) -> Result<(), Failed> {
    js::awaited(dir, "removeEntry", &[name.into()]).await?;
    Ok(())
}

/// An object's path as its parts' names: each encoded already, as object_store keeps them (`a#b.csv` is `a%23b.csv`).
fn parts_of(at: &Path) -> Vec<String> {
    at.parts().map(|part| part.as_ref().to_string()).collect()
}

/// An object's folders' names and its file's name. None for the root, which is no object.
fn names_of(at: &Path) -> Option<(Vec<String>, String)> {
    let mut dirs = parts_of(at);
    let name = dirs.pop()?;
    Some((dirs, name))
}

/// The names a listing starts from: none for the whole folder.
fn names_under(prefix: Option<&Path>) -> Vec<String> {
    prefix.map(parts_of).unwrap_or_default()
}

/// A path from names a listing found. None when `Path::parse` refuses them: no page put such a name.
fn joined(names: &[String]) -> Option<Path> {
    Path::parse(names.join("/")).ok()
}

fn is_dir(handle: &JsValue) -> Result<bool, Failed> {
    Ok(js::get(handle, "kind")?.as_string().as_deref() == Some("directory"))
}

/// A number of a file (its `lastModified` in ms, or its `size`).
fn number_of(file: &JsValue, name: &str) -> Result<f64, Failed> {
    js::get(file, name)?.as_f64().ok_or_else(|| Failed::Words(format!("a file has no {name}")))
}

/// An object's e_tag: its file's `lastModified` and size, which a write changes.
fn e_tag_of(file: &JsValue) -> Result<String, Failed> {
    let modified = number_of(file, "lastModified")? as i64;
    let size = number_of(file, "size")? as u64;
    Ok(format!("{modified}-{size}"))
}

/// An object's meta, from its file: its last change, its size and its e_tag. Its version is the e_tag.
fn meta_of(at: &Path, file: &JsValue) -> Result<ObjectMeta, Failed> {
    let modified = number_of(file, "lastModified")? as i64;
    Ok(ObjectMeta {
        location: at.clone(),
        last_modified: DateTime::<Utc>::from_timestamp_millis(modified).unwrap_or_default(),
        size: number_of(file, "size")? as u64,
        e_tag: Some(e_tag_of(file)?),
        version: None,
    })
}

/// An object's file as it is now (`getFile`): what is read of it is this file's bytes and meta.
async fn open(folder: &str, at: &Path) -> Result<JsValue, Failed> {
    let (dirs, name) = names_of(at).ok_or(Failed::Missing)?;
    let dir = dir_at(folder, &dirs, false).await?;
    let handle = file_at(&dir, &name, false).await?;
    Ok(js::awaited(&handle, "getFile", &[]).await?)
}

async fn meta_at(folder: &str, at: &Path) -> Result<ObjectMeta, Failed> {
    let file = open(folder, at).await?;
    meta_of(at, &file)
}

/// The range a read answers: an empty range's (its start, and no bytes), else the range asked of the object's `size`,
/// else the whole object (a HEAD asks for none).
fn answered(read: &js::Read, size: u64, at: &Path) -> StoreResult<Range<u64>> {
    if let Some(start) = read.empty_at {
        return Ok(start..start);
    }
    match &read.range {
        Some(range) => {
            range.as_range(size).map_err(|e| Error::Generic { store: "opfs", source: format!("{at}: {e}").into() })
        }
        None => Ok(0..size),
    }
}

/// The rest of a read of an object's bytes, in parts: where the next part starts, and where the read ends.
struct Parts {
    folder: String,
    at: Path,
    e_tag: String,
    next: u64,
    end: u64,
}

/// The bytes of `range` as a stream of parts of at most `PART` bytes, each read when the stream is polled: a large file
/// is never held whole.
fn parts(folder: String, at: Path, e_tag: String, range: Range<u64>) -> BoxStream<'static, StoreResult<Bytes>> {
    let state = Parts { folder, at, e_tag, next: range.start, end: range.end };
    stream::unfold(state, |mut state| async move {
        if state.next >= state.end {
            return None;
        }
        let part = state.next..state.next.saturating_add(PART).min(state.end);
        let (folder, at, e_tag) = (state.folder.clone(), state.at.clone(), state.e_tag.clone());
        match step(&state.at, read_part(folder, at, e_tag, part.clone())).await {
            Ok(bytes) => {
                state.next = part.end;
                Some((Ok(bytes), state))
            }
            Err(failure) => {
                state.next = state.end;
                Some((Err(failure), state))
            }
        }
    })
    .boxed()
}

/// One part of an object's bytes, from its file as it is now. Its e_tag must still be the one the read began with, so
/// a part of another version is never mixed in.
async fn read_part(folder: String, at: Path, e_tag: String, range: Range<u64>) -> Result<Bytes, Failed> {
    let file = open(&folder, &at).await?;
    if e_tag_of(&file)? != e_tag {
        return Err(Failed::Words(format!("{at} changed while it was read")));
    }
    let blob = js::awaited(&file, "slice", &[(range.start as f64).into(), (range.end as f64).into()]).await?;
    let buffer = js::awaited(&blob, "arrayBuffer", &[]).await?;
    Ok(Bytes::from(js_sys::Uint8Array::new(&buffer).to_vec()))
}

/// What a put did: written, with the e_tag of the new version, or refused by the object's state.
enum Put {
    Written(String),
    Exists,
    Stale,
}

/// A put, if the object's state allows it. `Create` refuses an object that is there; `Update` refuses one whose version
/// is not the e_tag it names (a missing object too); `Overwrite` writes over whatever is there.
async fn put(folder: String, at: Path, mode: PutMode, chunks: Vec<Bytes>) -> Result<Put, Failed> {
    let (dirs, name) = names_of(&at).ok_or(Failed::Missing)?;
    let dir = dir_at(&folder, &dirs, true).await?;
    let existing = match file_at(&dir, &name, false).await {
        Ok(handle) => Some(handle),
        Err(Failed::Missing) => None,
        Err(other) => return Err(other),
    };
    if let Some(refused) = refusal(&mode, existing.as_ref()).await? {
        return Ok(refused);
    }
    let handle = match existing {
        Some(handle) => {
            wait_past(&handle).await?;
            write_whole(&handle, &chunks).await?;
            handle
        }
        None => write_new(&dir, &name, &chunks).await?,
    };
    let file = js::awaited(&handle, "getFile", &[]).await?;
    Ok(Put::Written(e_tag_of(&file)?))
}

/// The refusal the mode gives the object's state, if it gives one.
async fn refusal(mode: &PutMode, existing: Option<&JsValue>) -> Result<Option<Put>, Failed> {
    Ok(match (mode, existing) {
        (PutMode::Create, Some(_)) => Some(Put::Exists),
        (PutMode::Update(_), None) => Some(Put::Stale),
        (PutMode::Update(version), Some(handle)) => {
            let file = js::awaited(handle, "getFile", &[]).await?;
            (version.e_tag.as_deref() != Some(e_tag_of(&file)?.as_str())).then_some(Put::Stale)
        }
        _ => None,
    })
}

/// A new object's file: written under a temporary name, then moved to its name, so the object is whole or not there. A
/// write that fails removes the temporary file.
async fn write_new(dir: &JsValue, name: &str, chunks: &[Bytes]) -> Result<JsValue, Failed> {
    let temp = format!("{name}#writing");
    let handle = file_at(dir, &temp, true).await?;
    if let Err(failure) = write_whole(&handle, chunks).await {
        let _ = remove(dir, &temp).await;
        return Err(failure);
    }
    js::awaited(&handle, "move", &[name.into()]).await?;
    Ok(handle)
}

/// Writes a file's new bytes as one write: a writable, each chunk, then `close`, which is when the new bytes replace the
/// old (Chromium keeps the old ones until then). A failed write aborts the writable, so the old bytes stay.
async fn write_whole(file: &JsValue, chunks: &[Bytes]) -> Result<(), Failed> {
    let writable = js::awaited(file, "createWritable", &[]).await?;
    match write_chunks(&writable, chunks).await {
        Ok(()) => {
            js::awaited(&writable, "close", &[]).await?;
            Ok(())
        }
        Err(failure) => {
            let _ = js::awaited(&writable, "abort", &[]).await;
            Err(failure)
        }
    }
}

async fn write_chunks(writable: &JsValue, chunks: &[Bytes]) -> Result<(), Failed> {
    for chunk in chunks {
        js::awaited(writable, "write", &[js_sys::Uint8Array::from(&chunk[..]).into()]).await?;
    }
    Ok(())
}

/// Waits until the clock has passed an object's last change, so that a write over it has a version the object never had.
async fn wait_past(handle: &JsValue) -> Result<(), Failed> {
    let file = js::awaited(handle, "getFile", &[]).await?;
    let modified = number_of(&file, "lastModified")?;
    while modified >= js_sys::Date::now() {
        tick().await;
    }
    Ok(())
}

/// Waits a millisecond: a `setTimeout` of 1 (a page and a worker both have one).
async fn tick() {
    let global = JsValue::from(js_sys::global());
    let wait = js_sys::Promise::new(&mut |resolve, _reject| {
        let scheduled = js::function(&global, "setTimeout")
            .and_then(|set| set.call2(&global, &resolve, &JsValue::from_f64(1.0)).map_err(js::js_words));
        if scheduled.is_err() {
            let _ = resolve.call0(&global); // (no timer: resolve now, rather than wait for one that never comes)
        }
    });
    let _ = JsFuture::from(wait).await;
}

/// Removes an object's file. A folder or a file that is not there leaves nothing to remove, which is no error.
async fn remove_at(folder: String, at: Path) -> Result<(), Failed> {
    let Some((dirs, name)) = names_of(&at) else { return Ok(()) };
    let Some(dir) = listed_dir(&folder, &dirs).await? else { return Ok(()) };
    match remove(&dir, &name).await {
        Err(Failed::Missing) => Ok(()),
        other => other,
    }
}

/// A folder's entries: its files, as the objects' metas, and its folders, as paths, each sorted.
struct Level {
    files: Vec<ObjectMeta>,
    folders: Vec<Path>,
}

/// The entries of the folder at `names`. A name `Path::parse` refuses is left out.
async fn level_of(folder: String, names: Vec<String>) -> Result<Level, Failed> {
    let mut level = Level { files: vec![], folders: vec![] };
    let Some(dir) = listed_dir(&folder, &names).await? else { return Ok(level) };
    for (name, handle) in entries(&dir).await? {
        let mut path = names.clone();
        path.push(name);
        let Some(at) = joined(&path) else { continue };
        if is_dir(&handle)? {
            level.folders.push(at);
        } else {
            let file = js::awaited(&handle, "getFile", &[]).await?;
            level.files.push(meta_of(&at, &file)?);
        }
    }
    level.files.sort_by(|a, b| a.location.cmp(&b.location));
    level.folders.sort();
    Ok(level)
}

/// Every file under the folder at `names`, its folders walked with an explicit stack (a recursive async fn would need
/// boxing), sorted by path.
async fn files_under(folder: String, names: Vec<String>) -> Result<Vec<ObjectMeta>, Failed> {
    let mut found = vec![];
    let mut stack = vec![names];
    while let Some(names) = stack.pop() {
        let level = level_of(folder.clone(), names).await?;
        found.extend(level.files);
        stack.extend(level.folders.iter().map(parts_of));
    }
    found.sort_by(|a, b| a.location.cmp(&b.location));
    Ok(found)
}

/// A folder's entries as (name, handle), in the order the browser gives them. Left out: a write in progress (a name with
/// `#`) and Chromium's file for an open writable (`.crswap`), which are not objects yet.
async fn entries(dir: &JsValue) -> Result<Vec<(String, JsValue)>, Failed> {
    let iterator = js::call_method(dir, "entries")?;
    let mut found = vec![];
    loop {
        let next = js::awaited(&iterator, "next", &[]).await?;
        if js::get(&next, "done")?.as_bool().unwrap_or(true) {
            return Ok(found);
        }
        let pair = js::get(&next, "value")?;
        let name = js::get(&pair, "0")?.as_string().unwrap_or_default();
        let handle = js::get(&pair, "1")?;
        if !name.contains('#') && !name.ends_with(".crswap") {
            found.push((name, handle));
        }
    }
}
