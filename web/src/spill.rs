//! The page's spill file (designs/l2-page-spill.md, S1): a page in a dedicated Worker spills DataFusion's spill files to the
//! browser's private file system, as extents of one file written through a sync access handle (`FileSystemSyncAccessHandle`,
//! which exists only in a Worker). The core takes the factory (`pondra::page::give_spills`) before the page's lake opens, so a
//! sort, an aggregate or a join past the page's pool spills here, and nothing else changes.
//!
//! The file is `.pondra-spill/<uuid>` in the origin's root, one per page. Its open removes the folder's other files: a page
//! that ended (its tab closed, or crashed) left them, and a file a live Worker holds refuses its removal, so it is left. A
//! failed open is no factory, and the page runs as on the main thread.
//!
//! Each spill file is a list of extents of the one file: a writer buffers up to `BUFFER` and writes it at the tail, or in a
//! freed extent of its size or more (first fit); a reader reads the extents in order, `READ` at a time. A dropped spill file
//! gives its extents back, and when none is left the file is emptied. The file's size is capped (`State::cap`), and the cap is
//! worked out again at most once a second while the file is written (`refresh`), so two tabs that spill share what is free
//! rather than each holding half of it as it was when they opened (11-02 audit L7).
//!
//! Every page's open sweeps the folder, a page with no spill file too (`start`): a crashed Worker's file goes at the next
//! page's open, in a Worker or not.

use crate::js;
use crate::opfs;
use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::{LocalBoxFuture, Shared};
use futures::stream::Stream;
use futures::FutureExt;
use pondra::page::{give_spills, take_spills, DataFusionError, DataFusionResult, SpillFile, SpillWriter, TempFileFactory, FULL};
use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::spawn_local;

/// The folder in the origin's root that holds the spill files. `Page.kept` refuses it as a folder's name (lib.rs).
pub(crate) const FOLDER: &str = ".pondra-spill";
/// A writer's buffer, and the least size of a freed extent it takes: a spill's writes reach the file a megabyte at a time.
const BUFFER: usize = 1 << 20;
/// What a read takes at a time: DataFusion's own read size (a larger one would be charged whole to the merge's memory for
/// every small batch sliced from it).
const READ: usize = 128 << 10;
/// How often, at most, the cap is worked out again while the file is written (`refresh`), in ms.
const REFRESH_MS: f64 = 1000.0;

/// Bytes written to spill files, ever (`spilledBytes`).
static SPILLED: AtomicU64 = AtomicU64::new(0);
/// The module's open spill file, for `spillFileBytes`: weak, so a file that has ended is none.
static OPEN: Mutex<Weak<Mutex<State>>> = Mutex::new(Weak::new());

/// A JavaScript handle the file keeps: the sync access handle, or the folder's. Only the page's thread holds one: a module is
/// built without atomics and runs on one thread (heap.rs), so a handle never crosses to another.
struct Handle(JsValue);

// SAFETY: the module runs on one thread, as `heap.rs` says of its heap (which is `Sync` for the same reason): a handle is
// never used from another thread, and no other value here is shared unsafely.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

/// A run of the file: `len` bytes at `at`.
#[derive(Clone, Copy)]
struct Extent {
    at: u64,
    len: u64,
}

/// The file's state, behind the factory's one lock: the handle, the extents of each spill file alive, and the extents no
/// spill file holds now.
struct State {
    handle: Option<Handle>, // the sync access handle (None once the file is closed)
    folder: Option<Handle>, // the folder it is in, and `name` its name there, for its removal at the end
    name: String,
    cap: u64,          // the most the file may hold: its tail never grows past it
    fixed: bool,       // the cap is `PONDRA_SPILL_GB`'s, never worked out again
    refreshed: f64,    // when the cap was last worked out (`Date.now()`); infinite while a refresh is under way
    tail: u64,         // the file's end: a write that takes no freed extent goes there
    free: Vec<Extent>, // extents no spill file holds, sorted by `at`, none touching another
    files: HashMap<u64, Vec<Extent>>, // each spill file alive, with its extents in the order it reads them
    next: u64,         // the next spill file's id
}

/// The lock of a file's state. The module's one thread holds it across no other call of this, and a poisoned lock keeps its state.
fn lock(shared: &Arc<Mutex<State>>) -> MutexGuard<'_, State> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The page's spill file, as DataFusion's factory: each spill file it makes is a list of extents of this one file.
pub(crate) struct OpfsSpills {
    shared: Arc<Mutex<State>>,
}

impl OpfsSpills {
    /// The page's file, opened with a sync access handle in the origin's root after the folder's other files are removed. Any
    /// failure is the words of why.
    async fn open() -> Result<OpfsSpills, String> {
        let global = JsValue::from(js_sys::global());
        let storage = js::get(&js::get(&global, "navigator")?, "storage")?;
        let (cap, fixed) = cap_of(&storage).await?;
        let root = js::awaited(&storage, "getDirectory", &[]).await.map_err(js::js_words)?;
        let folder = js::awaited(&root, "getDirectoryHandle", &[FOLDER.into(), opfs::create_option(true)?]).await.map_err(js::js_words)?;
        let name = uuid(&global)?;
        let file = js::awaited(&folder, "getFileHandle", &[name.as_str().into(), opfs::create_option(true)?]).await.map_err(js::js_words)?;
        let handle = js::awaited(&file, "createSyncAccessHandle", &[]).await.map_err(js::js_words)?;
        sweep(&folder, &name).await;
        let refreshed = js_sys::Date::now();
        let state = State { handle: Some(Handle(handle)), folder: Some(Handle(folder)), name, cap, fixed, refreshed, tail: 0, free: vec![], files: HashMap::new(), next: 0 };
        let shared = Arc::new(Mutex::new(state));
        *OPEN.lock().unwrap_or_else(PoisonError::into_inner) = Arc::downgrade(&shared);
        Ok(OpfsSpills { shared })
    }

    /// Ends the file: its handle closed, then the file removed from the folder (a removal that fails is left to the next page's
    /// sweep). After this the state holds no handle, so every call that comes later does nothing or fails.
    pub(crate) async fn close(&self) {
        let (handle, folder, name) = {
            let mut state = lock(&self.shared);
            (state.handle.take(), state.folder.take(), state.name.clone())
        };
        if let Some(Handle(handle)) = handle {
            let _ = js::call_with(&handle, "close", &[]);
        }
        if let Some(Handle(folder)) = folder {
            let _ = js::awaited(&folder, "removeEntry", &[name.as_str().into()]).await;
        }
    }
}

impl TempFileFactory for OpfsSpills {
    fn create_temp_file(&self, _description: &str) -> DataFusionResult<Arc<dyn SpillFile>> {
        let mut state = lock(&self.shared);
        if state.handle.is_none() {
            return Err(DataFusionError::IoError(io::Error::other("this page's spill file is closed")));
        }
        let id = state.next;
        state.next += 1;
        state.files.insert(id, Vec::new());
        Ok(Arc::new(OpfsSpill { shared: self.shared.clone(), id }))
    }
}

/// One spill file: its extents are the state's (`files`), and it gives them back when it is dropped.
struct OpfsSpill {
    shared: Arc<Mutex<State>>,
    id: u64,
}

impl SpillFile for OpfsSpill {
    fn size(&self) -> Option<u64> {
        let state = lock(&self.shared);
        Some(state.files.get(&self.id).map_or(0, |extents| extents.iter().map(|extent| extent.len).sum()))
    }

    fn read_stream(&self) -> DataFusionResult<Pin<Box<dyn Stream<Item = DataFusionResult<Bytes>> + Send>>> {
        Ok(Box::pin(Reader { shared: self.shared.clone(), id: self.id, extent: 0, within: 0 }))
    }

    fn open_writer(&self) -> DataFusionResult<Box<dyn SpillWriter>> {
        Ok(Box::new(Writer { shared: self.shared.clone(), id: self.id, buffer: Vec::with_capacity(BUFFER) }))
    }
}

impl Drop for OpfsSpill {
    /// Gives the file's extents back, and when no spill file is left, empties the file: a page holds no disk between queries.
    /// Once the file is closed there is no handle, and nothing to empty.
    fn drop(&mut self) {
        let mut state = lock(&self.shared);
        let Some(extents) = state.files.remove(&self.id) else { return };
        for extent in extents {
            state.give_back(extent);
        }
        if state.files.is_empty() {
            state.free.clear();
            state.tail = 0;
            if let Some(Handle(handle)) = &state.handle {
                let _ = js::call_with(handle, "truncate", &[0.0.into()]);
            }
        }
    }
}

/// A spill file's writer: its bytes are buffered up to `BUFFER`, then written as one extent (`State::place`); `flush` and
/// `finish` write out what is buffered.
struct Writer {
    shared: Arc<Mutex<State>>,
    id: u64,
    buffer: Vec<u8>,
}

impl Writer {
    fn out(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let due = {
            let mut state = lock(&self.shared);
            state.place(self.id, &self.buffer)?;
            state.due()
        };
        self.buffer.clear();
        if due {
            spawn_local(refresh(Arc::downgrade(&self.shared)));
        }
        Ok(())
    }
}

impl io::Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let taken = (BUFFER - self.buffer.len()).min(bytes.len());
        self.buffer.extend_from_slice(&bytes[..taken]);
        if self.buffer.len() == BUFFER {
            self.out()?;
        }
        Ok(taken)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out()
    }
}

impl SpillWriter for Writer {
    fn finish(&mut self) -> DataFusionResult<()> {
        self.out().map_err(DataFusionError::IoError)
    }
}

/// A spill file's bytes in order, `READ` at a time as it is polled. It reads the extents the file has when it is polled, from
/// the extent and offset it has reached: the list only grows while the file lives, so a spill pool that reads while it writes
/// sees what has been written.
struct Reader {
    shared: Arc<Mutex<State>>,
    id: u64,
    extent: usize,
    within: u64,
}

impl Stream for Reader {
    type Item = DataFusionResult<Bytes>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let state = lock(&this.shared);
        let Some(extents) = state.files.get(&this.id) else {
            return Poll::Ready(Some(Err(DataFusionError::IoError(io::Error::other("this spill file was dropped while it was read")))));
        };
        // (the extents read so far are passed)
        while let Some(extent) = extents.get(this.extent) {
            if this.within < extent.len {
                break;
            }
            this.extent += 1;
            this.within = 0;
        }
        let Some(&extent) = extents.get(this.extent) else { return Poll::Ready(None) };
        let len = (extent.len - this.within).min(READ as u64);
        match read_at(&state.handle, extent.at + this.within, len) {
            Ok(bytes) => {
                this.within += len;
                Poll::Ready(Some(Ok(Bytes::from(bytes))))
            }
            Err(e) => Poll::Ready(Some(Err(DataFusionError::IoError(e)))),
        }
    }
}

impl State {
    /// Whether the cap is due to be worked out again (`refresh`): not a fixed one, and not within `REFRESH_MS` of the last time or
    /// while a refresh is under way. A due one is marked under way.
    fn due(&mut self) -> bool {
        if self.fixed || js_sys::Date::now() - self.refreshed < REFRESH_MS {
            return false;
        }
        self.refreshed = f64::INFINITY;
        true
    }

    /// Writes `bytes` as one extent of spill file `id`: at the first freed extent of its size or more, else at the tail, which
    /// must stay within the cap. The state changes only once the write has been made, so a refused one leaves it as it was.
    fn place(&mut self, id: u64, bytes: &[u8]) -> io::Result<()> {
        if !self.files.contains_key(&id) {
            return Err(io::Error::other("this spill file was dropped while it was written"));
        }
        let len = bytes.len() as u64;
        let freed = self.free.iter().position(|extent| extent.len >= len);
        let at = match freed {
            Some(i) => self.free[i].at,
            None => self.tail,
        };
        if freed.is_none() && at + len > self.cap {
            return Err(io::Error::other(format!("{FULL}: the page's spills may take {} MB of the site's storage, and this query needs more", self.cap >> 20)));
        }
        write_at(&self.handle, bytes, at)?;
        match freed {
            Some(i) => {
                let extent = self.free[i];
                if extent.len == len {
                    self.free.remove(i);
                } else {
                    self.free[i] = Extent { at: extent.at + len, len: extent.len - len };
                }
            }
            None => self.tail += len,
        }
        if let Some(extents) = self.files.get_mut(&id) {
            extents.push(Extent { at, len });
        }
        SPILLED.fetch_add(len, Relaxed);
        Ok(())
    }

    /// Gives an extent back to the free list, merged with the free extents it touches.
    fn give_back(&mut self, extent: Extent) {
        self.free.push(extent);
        self.free.sort_by_key(|e| e.at);
        let mut merged: Vec<Extent> = Vec::with_capacity(self.free.len());
        for e in self.free.drain(..) {
            match merged.last_mut() {
                Some(last) if last.at + last.len == e.at => last.len += e.len,
                _ => merged.push(e),
            }
        }
        self.free = merged;
    }
}

/// `bytes` written at `at`. A copy goes to the handle (`Uint8Array::from`), since a view of the module's memory is invalid once
/// the memory grows.
fn write_at(handle: &Option<Handle>, bytes: &[u8], at: u64) -> io::Result<()> {
    let Some(Handle(handle)) = handle else { return Err(closed()) };
    let written = js::call_with(handle, "write", &[js_sys::Uint8Array::from(bytes).into(), at_options(at)]).map_err(failure)?;
    match written.as_f64() {
        Some(n) if n == bytes.len() as f64 => Ok(()),
        _ => Err(io::Error::other("the spill file took fewer bytes than it was given")),
    }
}

/// `len` bytes of the file at `at`, in a fresh array (`Uint8Array::new_with_length`, copied out with `to_vec`).
fn read_at(handle: &Option<Handle>, at: u64, len: u64) -> io::Result<Vec<u8>> {
    let Some(Handle(handle)) = handle else { return Err(closed()) };
    let into = js_sys::Uint8Array::new_with_length(len as u32);
    let got = js::call_with(handle, "read", &[into.clone().into(), at_options(at)]).map_err(failure)?;
    match got.as_f64() {
        Some(n) if n == len as f64 => Ok(into.to_vec()),
        _ => Err(io::Error::other("the spill file ended before an extent it holds did")),
    }
}

/// The `{at}` option of a sync access handle's read and write.
fn at_options(at: u64) -> JsValue {
    let options = JsValue::from(js_sys::Object::new());
    js::set(&options, "at", at as f64).expect("a fresh object takes its keys");
    options
}

/// A failed handle call as an io::Error: a browser's refusal of a write past the site's quota (`QuotaExceededError`) is in the
/// page's words (`FULL`), any other in the browser's.
fn failure(e: JsValue) -> io::Error {
    let quota = js::get(&e, "name").ok().and_then(|name| name.as_string()).as_deref() == Some("QuotaExceededError");
    let words = js::js_words(e);
    io::Error::other(if quota { format!("{FULL}: {words}") } else { words })
}

fn closed() -> io::Error {
    io::Error::other("this page's spill file is closed")
}

/// The most the file may hold, and whether that is fixed: `PONDRA_SPILL_GB` when it is given (read as the core reads it), else
/// half of what the origin has free, so a kept page's own files keep the other half (`refresh` works it out again).
async fn cap_of(storage: &JsValue) -> Result<(u64, bool), String> {
    if let Some(gb) = pondra::env::parse::<f64>("PONDRA_SPILL_GB").filter(|gb| *gb >= 0.0) {
        return Ok(((gb * crate::GIB) as u64, true));
    }
    Ok((free_of(storage).await? / 2, false))
}

/// What the origin has free: `navigator.storage.estimate()`'s quota less its usage, which counts this page's spill file too.
async fn free_of(storage: &JsValue) -> Result<u64, String> {
    let estimate = js::awaited(storage, "estimate", &[]).await.map_err(js::js_words)?;
    let quota = js::get(&estimate, "quota")?.as_f64().ok_or("the browser gives the site no storage quota")?;
    let usage = js::get(&estimate, "usage")?.as_f64().unwrap_or(0.0);
    Ok((quota - usage).max(0.0) as u64)
}

/// The cap worked out again as the file grows (`Writer::out`, at most once a second): what the file holds now, and half of what
/// the origin has free now, so a tab that spills later, or a kept page's next write, still finds room. A cap that falls below the
/// file's tail only stops it growing. An estimate that fails keeps the cap as it was.
async fn refresh(shared: Weak<Mutex<State>>) {
    let global = JsValue::from(js_sys::global());
    let free = match js::get(&global, "navigator").and_then(|navigator| js::get(&navigator, "storage")) {
        Ok(storage) => free_of(&storage).await,
        Err(e) => Err(e),
    };
    let Some(shared) = shared.upgrade() else { return }; // (the file has ended)
    let mut state = lock(&shared);
    state.refreshed = js_sys::Date::now();
    if let Ok(free) = free {
        state.cap = state.tail + free / 2;
    }
}

/// A new name for the file: `crypto.randomUUID()`.
fn uuid(global: &JsValue) -> Result<String, String> {
    let crypto = js::get(global, "crypto")?;
    js::call_with(&crypto, "randomUUID", &[]).ok().and_then(|id| id.as_string()).ok_or_else(|| "the browser gives no random name (crypto.randomUUID)".to_string())
}

/// Removes the folder's other files, which a page that ended left. One a live Worker holds refuses its removal (its sync access
/// handle is exclusive), and is left.
async fn sweep(folder: &JsValue, keep: &str) {
    let Ok(entries) = opfs::entries(folder).await else { return };
    for (name, handle) in entries {
        let file = js::get(&handle, "kind").ok().and_then(|kind| kind.as_string()).as_deref() == Some("file");
        if file && name != keep {
            let _ = js::awaited(folder, "removeEntry", &[name.as_str().into()]).await;
        }
    }
}

/// The folder swept by a page that keeps no spill file (`start`): every file goes but one a live Worker holds. (A Worker's page
/// that is between making its file and opening its handle loses it here, and runs with no spill file, as on the main thread.)
async fn sweep_left() {
    let folder = async {
        let global = JsValue::from(js_sys::global());
        let storage = js::get(&js::get(&global, "navigator")?, "storage")?;
        let root = js::awaited(&storage, "getDirectory", &[]).await.map_err(js::js_words)?;
        // (no folder: no page spilled here, and none is made)
        js::awaited(&root, "getDirectoryHandle", &[FOLDER.into(), opfs::create_option(false)?]).await.map_err(js::js_words)
    };
    if let Ok(folder) = folder.await {
        sweep(&folder, "").await;
    }
}

/// Whether this page can keep a spill file: a dedicated Worker, where `FileSystemSyncAccessHandle` is defined, with the origin's
/// private file system.
fn available() -> bool {
    let global = JsValue::from(js_sys::global());
    js::get(&global, "FileSystemSyncAccessHandle").is_ok_and(|class| !class.is_undefined()) && opfs::available()
}

/// A page's spill file as the page holds it: opening in the background from the page's claim (`start`), given to the core once
/// it is open (`settle`), and ended with the page (`detach`, then `OpfsSpills::close`).
pub(crate) struct Spills {
    open: Shared<LocalBoxFuture<'static, Option<Arc<OpfsSpills>>>>,
    ended: Rc<Cell<bool>>, // the page has ended: an open that finishes now closes its own file
    given: Cell<bool>,     // the factory has been given to the core
}

/// The page's spill file, opened in the background. Called once the page's claim has succeeded, so a page refused its claim
/// touches no file. None when the page can't keep one (not a Worker, or `PONDRA_SPILL_GB=0`), and the page runs as on the main
/// thread.
pub(crate) fn start() -> Option<Spills> {
    if !available() || pondra::env::parse::<f64>("PONDRA_SPILL_GB") == Some(0.0) {
        if opfs::available() {
            spawn_local(sweep_left()); // (no file of its own, but what a crashed Worker's page left still goes)
        }
        return None;
    }
    let ended = Rc::new(Cell::new(false));
    let (sent, opened) = oneshot::channel();
    let finished = ended.clone();
    spawn_local(async move {
        let file = match OpfsSpills::open().await {
            Ok(file) if finished.get() => {
                file.close().await; // (the page ended while it opened: the file is none, and is closed here)
                None
            }
            Ok(file) => Some(Arc::new(file)),
            Err(_) => None, // (no file, so no factory: the page runs as on the main thread)
        };
        let _ = sent.send(file);
    });
    let open = async move { opened.await.ok().flatten() }.boxed_local().shared();
    Some(Spills { open, ended, given: Cell::new(false) })
}

impl Spills {
    /// Waits until the file is open, and gives the core its factory (once): a lake that opens after this spills through it.
    pub(crate) async fn settle(&self) {
        let Some(file) = self.open.clone().await else { return };
        if !self.given.replace(true) {
            let factory: Arc<dyn TempFileFactory> = file;
            give_spills(factory);
        }
    }

    /// The page ends, synchronously (its `close` or its `Drop`): its factory is taken back from the core, and the file it had
    /// opened is returned for the page to close once its member has ended. An open still under way closes its own file when it
    /// finishes (`ended`).
    pub(crate) fn detach(self) -> Option<Arc<OpfsSpills>> {
        self.ended.set(true);
        let file = self.open.clone().now_or_never().flatten()?;
        let factory: Arc<dyn TempFileFactory> = file.clone();
        take_spills(&factory);
        Some(file)
    }
}

/// The bytes written to spill files, ever: what a check reads to see that a query spilled (`web_check.mjs`).
#[wasm_bindgen(js_name = spilledBytes)]
pub fn spilled_bytes() -> f64 {
    SPILLED.load(Relaxed) as f64
}

/// The open spill file's size now (its handle's `getSize`), 0 when there is none.
#[wasm_bindgen(js_name = spillFileBytes)]
pub fn spill_file_bytes() -> f64 {
    let open = OPEN.lock().unwrap_or_else(PoisonError::into_inner).upgrade();
    let Some(shared) = open else { return 0.0 };
    let state = lock(&shared);
    match &state.handle {
        Some(Handle(handle)) => js::call_with(handle, "getSize", &[]).ok().and_then(|size| size.as_f64()).unwrap_or(0.0),
        None => 0.0,
    }
}
