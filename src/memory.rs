//! A lake held in this process's memory (`memory://…`, DuckDB's `:memory:`), and its spill to disk.
//!
//! Everything stays in memory below the budget. Past it, the bulk of a lake (its files and its log:
//! `data/` and `log/`) goes to a folder of this process's on disk (`spill_base`), so loading more
//! than the memory holds no longer ends the process. The catalog and `files/` (replaced by
//! their version: `PutMode::Update`) stay in memory: they are small, and a local folder can't take
//! an update.
//!
//! What spills is one budget for everything this process spills: `PONDRA_SPILL_GB`, else the disk's room, shared
//! with the queries' and the shuffles' spills (`store::others_held`). The lakes in memory count their bytes on
//! disk and their reserved bytes together (`Spilled`, one for the process), so the spill disk is counted once for
//! all of them. The room a lake keeps on its disk is kept either way. Past either, the write is refused by name
//! (53100), and a write that would pass it reserves its bytes first, so writes racing for the last of the room
//! can't both take it.
//!
//! A lake moves out of memory with `set_location` (`ALTER DATABASE … SET LOCATION`, `db.save`): its objects
//! are copied to the target while the node serves, the host stops the node, `settle` makes the target equal
//! to the lake as it is then, and the node starts again there. Nothing of the lake is written in place.
use crate::store::{Counted, Lake, Store};
use anyhow::Context;
use futures::future::ready;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{CopyMode, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart, WriteMultipart};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path as FsPath, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use {bytes::Bytes, object_store::GetResultPayload, std::ops::Range};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

/// The lock file in a spill folder. The process that made the folder holds it, so a folder whose
/// lock another process can take is one its process left behind. It is not an object of the lake.
const LOCK_FILE: &str = ".lock";

/// The lakes held in this process's memory, by place: every open of one here is the same store until
/// `forget_memory`, so a node that restarts in place finds its lake as it left it. Nothing of one is
/// on disk, and no other process reaches it. (Not `per_node!`: a restart in place resets those.)
static MEMORY: std::sync::LazyLock<Mutex<HashMap<String, Store>>> = std::sync::LazyLock::new(Default::default);

/// The spill folders this process made and has not yet removed (`Folder`). A process static, as `MEMORY` is: a
/// folder of a static (`store::SPILLS`) outlives a node until the program ends, and `leave` removes what is left.
/// `sweep` skips these (on NFS a lock is per process, so opening a folder's lock again would drop ours). Not the
/// pid: an exec restart keeps it, and the container image runs `pondra` as PID 1 on a shared volume.
static MADE: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());

/// What the lakes in memory hold on disk: the bytes of every lake's files and log that spilled (`on_disk`), and the
/// bytes reserved for spills in flight (`reserved`, see `Reserved`). A reservation counts before its write lands and
/// stops counting once the write's bytes are on disk. One for the process (`PROCESS`), so the spill disk is counted
/// once for all of its lakes; a test makes its own, so tests running in parallel don't see each other's bytes.
#[derive(Debug, Default)]
pub(crate) struct Spilled {
    on_disk: AtomicU64,
    reserved: AtomicU64,
}

impl Spilled {
    /// The bytes on disk and reserved: what the lakes take of the spill disk.
    fn total(&self) -> u64 { self.on_disk.load(SeqCst) + self.reserved.load(SeqCst) }
}

/// The spilled bytes of this process's lakes in memory: the one `Spilled` every lake made by `Held::with_budget` counts in.
static PROCESS: std::sync::LazyLock<Arc<Spilled>> = std::sync::LazyLock::new(Arc::default);

/// What the lakes in this process's memory take of the spill disk, on disk or reserved for spills in flight.
pub(crate) fn spilled() -> u64 { PROCESS.total() }

crate::per_node!(
    /// Why the last move of this node's lake failed (`move_failed`): the host starts the node again on the lake
    /// in memory after it, so the flag belongs to the node that starts next, and goes when that node stops.
    pub static MOVE_FAILED: Mutex<Option<String>> = Default::default());

/// A move that failed (`settle`): said on stderr, and kept for the node that starts again on the lake in memory,
/// which says so in `/stats`. The host calls it after the node stopped (`Node::stop` sets its statics back).
pub fn move_failed(to: &str, e: &anyhow::Error) {
    let why = format!("moving the lake to {to} failed: {e:#}");
    eprintln!("{why}; it stays in memory");
    *MOVE_FAILED.lock().unwrap_or_else(PoisonError::into_inner) = Some(why);
}

/// Is `url` a lake held in this process's memory?
pub fn in_memory(url: &str) -> bool { url.starts_with("memory://") }

/// Is a lake held in this process's memory at `url`? (Opening one there makes it.)
pub fn memory_held(url: &str) -> bool { MEMORY.lock().unwrap_or_else(PoisonError::into_inner).contains_key(url.trim_end_matches('/')) }

/// Is `store` a lake held in this process's memory? Every open of one gives the same store.
pub fn held_in_memory(store: &Store) -> bool { MEMORY.lock().unwrap_or_else(PoisonError::into_inner).values().any(|s| Arc::ptr_eq(s, store)) }

/// A lake's place as given: `:memory:` (DuckDB's word) is a new lake in this process's memory.
pub fn place(lake: &str) -> String {
    match lake {
        ":memory:" => format!("memory://{}", uuid::Uuid::new_v4().simple()),
        _ => lake.to_string(),
    }
}

/// Let a lake in memory go (the library's close, once its node has stopped): its memory with it.
pub fn forget_memory(url: &str) { MEMORY.lock().unwrap_or_else(PoisonError::into_inner).remove(url.trim_end_matches('/')); }

/// The store of the lake held in memory at `url`: the same one for every open of it, made on the first.
pub fn store(url: &str) -> Store {
    MEMORY.lock().unwrap_or_else(PoisonError::into_inner).entry(url.trim_end_matches('/').to_string()).or_insert_with(|| Arc::new(Counted(Arc::new(Held::new())))).clone()
}

/// The bytes past which a lake's files and log go to disk: `PONDRA_MEMORY_LAKE_GB`, else a quarter of
/// the machine's memory (4 GB where it can't say).
fn budget() -> u64 {
    match crate::env::parse::<f64>("PONDRA_MEMORY_LAKE_GB") {
        Some(gb) => (gb * (1u64 << 30) as f64) as u64,
        None => crate::store::ram().map_or(4 << 30, |r| (r / 4) as u64),
    }
}

/// A lake held in memory. Its objects are in an `InMemory` until the lake's memory passes the budget;
/// after that, new files and log objects go to disk (see the module's doc).
#[derive(Debug)]
pub struct Held(Arc<Inner>);

/// What a held lake keeps. It is shared (in an `Arc`) with the streams and uploads it hands out,
/// which outlive the call that made them.
#[derive(Debug)]
struct Inner {
    objects: InMemory,
    tier: OnceLock<Disk>, // the disk tier (made on the first object that spills)
    mem: Tally,           // the objects in `objects`: what the budget is measured against
    disk: Tally,          // the objects in `tier`: this lake's bytes on disk, also counted in `spilled`
    spilled: Arc<Spilled>, // the count this lake's bytes are in: the process's (`PROCESS`), or a test's own
    budget: u64,
    #[cfg(test)]
    cap: Option<u64>, // a test's setting in bytes, in place of `PONDRA_SPILL_GB` (`setting`)
}

/// The objects one tier holds, by path, each with its bytes, and their sum. Every write and delete that moves
/// an object into or out of a tier notes it here, so each sum is the tier's whole. A counted tier (the disk
/// tier) moves its `Spilled` by the same change, under the same lock, so the two never disagree.
#[derive(Debug, Default)]
struct Tally {
    sizes: Mutex<HashMap<Path, u64>>,
    sum: AtomicU64,
    counted: Option<Arc<Spilled>>,
}

impl Tally {
    /// A tally whose sum is also counted in `spilled`'s `on_disk`.
    fn counting(spilled: &Arc<Spilled>) -> Tally { Tally { counted: Some(spilled.clone()), ..Default::default() } }
}

/// The store of a spill folder: a local folder's. A page has no folder, so there a `Disk` is never made (`Disk::make`).
#[cfg(not(target_arch = "wasm32"))]
type FolderStore = object_store::local::LocalFileSystem;
#[cfg(target_arch = "wasm32")]
type FolderStore = InMemory;

/// The folder spilled objects go to: a `Folder` of this process's own, which goes when the lake does.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))] // (a page's `Disk::make` refuses: none is made there)
#[derive(Debug)]
struct Disk {
    store: FolderStore,
    folder: Folder,
}

impl Held {
    /// A lake held in memory, spilling past this process's budget.
    pub fn new() -> Held { Held::with_budget(budget()) }

    /// A lake held in memory, spilling past `budget` bytes of it in memory. Its spills count in this process's `Spilled`.
    pub fn with_budget(budget: u64) -> Held { Held::counting(budget, PROCESS.clone()) }

    /// A lake held in memory whose spills count in `spilled`.
    fn counting(budget: u64, spilled: Arc<Spilled>) -> Held {
        let disk = Tally::counting(&spilled);
        Held(Arc::new(Inner { objects: InMemory::new(), tier: OnceLock::new(), mem: Tally::default(), disk, spilled, budget, #[cfg(test)] cap: None }))
    }

    /// A lake held in memory whose spills count in `spilled` (a test's own, so tests running in parallel never see each
    /// other's bytes) and stand against `cap` bytes in place of the setting, if there is one.
    #[cfg(test)]
    fn sharing(budget: u64, cap: Option<u64>, spilled: Arc<Spilled>) -> Held {
        let mut h = Held::counting(budget, spilled);
        Arc::get_mut(&mut h.0).expect("a lake made here is not shared yet").cap = cap;
        h
    }

    /// A lake held in memory whose spills stand against `cap` bytes in place of the setting, counted on their own.
    #[cfg(test)]
    fn with_cap(budget: u64, cap: u64) -> Held { Held::sharing(budget, Some(cap), Arc::default()) }
}

impl std::fmt::Display for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "memory, spilling to disk past {} MB", self.0.budget >> 20) }
}

/// The objects that may go to disk: a lake's files and its log.
fn spillable(at: &Path) -> bool {
    let key: &str = at.as_ref();
    key.starts_with("data/") || key.starts_with("log/")
}

/// The spill folder's lock file is not one of the lake's objects.
fn hidden(meta: &ObjectMeta) -> bool {
    let key: &str = meta.location.as_ref();
    key == LOCK_FILE
}

fn already(at: &Path) -> object_store::Error { object_store::Error::AlreadyExists { path: at.to_string(), source: "it is already there".into() } }

fn missing(at: &Path) -> object_store::Error { object_store::Error::NotFound { path: at.to_string(), source: "it is in no tier of the lake".into() } }

#[cfg(not(target_arch = "wasm32"))]
fn spill_failed(e: std::io::Error) -> object_store::Error {
    object_store::Error::Generic { store: "memory", source: format!("past its budget, a lake in memory spills to a folder on disk (PONDRA_CACHE_DIR says where), which can't be used: {e}").into() }
}

fn read_failed(e: std::io::Error) -> object_store::Error { object_store::Error::Generic { store: "memory", source: format!("reading a lake's file spilled to disk: {e}").into() } }

/// The error a spill is refused with: `what` says which limit it passed. Every refusal ends the same way out.
fn full(what: String) -> object_store::Error {
    object_store::Error::Generic { store: "memory", source: format!("{what}: raise PONDRA_SPILL_GB or PONDRA_MEMORY_LAKE_GB, or move the lake to a folder (ALTER DATABASE … SET LOCATION)").into() }
}

/// A part's future that fails with a refusal (`Upload::put_part`), without sending the part on.
fn refused(why: String) -> UploadPart { Box::pin(async move { Err(full(why)) }) }

/// Bytes reserved on disk for a spill in flight (`Inner::reserve`). They count against the spill budget until the write
/// lands (the disk's tally takes them over) or the reservation goes; dropping one gives its bytes back.
#[derive(Debug)]
struct Reserved {
    inner: Arc<Inner>,
    bytes: u64,
}

impl Reserved {
    /// Reserves `bytes` more for the same write (a part of a multipart upload), or says why not.
    fn grow(&mut self, bytes: u64) -> Result<(), String> {
        let disk = self.inner.tier.get().ok_or("the disk a lake in memory spills to is gone")?;
        self.inner.claim(disk, bytes)?;
        self.bytes += bytes;
        Ok(())
    }

    /// Gives back what is reserved: once the write has landed (its bytes are in the disk's sum) or has failed.
    fn release(&mut self) { self.inner.spilled.reserved.fetch_sub(std::mem::take(&mut self.bytes), SeqCst); }
}

impl Drop for Reserved {
    fn drop(&mut self) { self.release() }
}

impl Tally {
    fn has(&self, at: &Path) -> bool { self.sizes.lock().unwrap_or_else(PoisonError::into_inner).contains_key(at) }

    fn size(&self, at: &Path) -> Option<u64> { self.sizes.lock().unwrap_or_else(PoisonError::into_inner).get(at).copied() }

    fn sum(&self) -> u64 { self.sum.load(SeqCst) }

    /// Records that `at` now holds `bytes` (`None`: it is not in this tier any more), taking off the size it had.
    /// Returns whether it was there before.
    fn note(&self, at: &Path, bytes: Option<u64>) -> bool {
        let mut sizes = self.sizes.lock().unwrap_or_else(PoisonError::into_inner);
        let old = sizes.remove(at);
        if let Some(old) = old {
            self.sum.fetch_sub(old, SeqCst);
            if let Some(spilled) = &self.counted {
                spilled.on_disk.fetch_sub(old, SeqCst);
            }
        }
        if let Some(bytes) = bytes {
            sizes.insert(at.clone(), bytes);
            self.sum.fetch_add(bytes, SeqCst);
            if let Some(spilled) = &self.counted {
                spilled.on_disk.fetch_add(bytes, SeqCst);
            }
        }
        old.is_some()
    }
}

impl Inner {
    /// Does `at`, of `bytes`, go to disk when it is written now? Only the bulk of a lake, and only what
    /// would take its memory past the budget: a load's first file bigger than the budget went to memory
    /// whole. An upload, whose size isn't known yet, goes to disk once the budget is reached. An update
    /// stays in memory, where its version is checked.
    fn spills(&self, at: &Path, mode: &PutMode, bytes: u64) -> bool {
        spillable(at) && !matches!(mode, PutMode::Update(_)) && self.mem.sum() + bytes.max(1) > self.budget
    }

    /// The disk tier of this lake, made on the first need.
    fn disk_or_make(&self) -> object_store::Result<&Disk> {
        if let Some(disk) = self.tier.get() {
            return Ok(disk);
        }
        let made = Disk::make()?;
        Ok(self.tier.get_or_init(|| made)) // (a racing make loses here, and its folder goes with it)
    }

    /// The disk tier, if it exists and `at` may be on it.
    fn disk_of(&self, at: &Path) -> Option<&Disk> { self.tier.get().filter(|_| spillable(at)) }

    /// The setting the spills of this lake are checked against, in GB (`piece_fits`): `PONDRA_SPILL_GB`, or a test's cap.
    #[cfg(test)]
    fn setting(&self) -> Option<f64> {
        match self.cap {
            Some(cap) => Some(cap as f64 / (1u64 << 30) as f64), // (a power of two: `gb_bytes` gives `cap` back exactly)
            None => crate::store::spill_gb(),
        }
    }

    #[cfg(not(test))]
    fn setting(&self) -> Option<f64> { crate::store::spill_gb() }

    /// Adds `bytes` to what is reserved for spills in flight, if they fit; else says why not. The one check for
    /// `reserve` and `Reserved::grow`. Under the setting, everything this process spills must stay within it: the
    /// others' bytes (`store::others_held`) and the lakes' (`on_disk` and `reserved`). Without it, the bytes in flight
    /// come off the disk's free room, which counts only what has landed. The check and the reservation are one
    /// `fetch_update` on `reserved`, so writes racing for the last of the room can't both take it. The room and
    /// `on_disk` are read after `reserved` (SeqCst): a landing is on disk before its reservation goes, so it is
    /// counted once, in one or the other, never in neither.
    fn claim(&self, disk: &Disk, bytes: u64) -> Result<(), String> {
        if let Some((free, keep)) = crate::store::short_of_room(&disk.folder.path) {
            return Err(format!("the disk a lake in memory spills to has {} MB free, and a lake keeps {} MB", free >> 20, keep >> 20));
        }
        let gb = self.setting();
        let others = gb.map_or(0, |_| crate::store::others_held()); // (the queries' and the shuffles' spills count under the setting only)
        let fits = |reserved: u64| {
            let room = crate::store::space(&disk.folder.path).map(|(free, total)| (free.saturating_sub(reserved), total));
            crate::store::piece_fits(gb, others + self.spilled.on_disk.load(SeqCst) + reserved, bytes, room)
        };
        self.spilled
            .reserved
            .fetch_update(SeqCst, SeqCst, |reserved| fits(reserved).then_some(reserved + bytes))
            .map(|_| ())
            .map_err(|_| match gb {
                Some(gb) => format!("this process's spills may take {:.1} MB of disk and hold {} MB there", gb * 1024.0, (crate::store::others_held() + self.spilled.total()) >> 20),
                None => format!("the disk a lake in memory spills to has no room for {} MB more beside the spills in flight", bytes >> 20),
            })
    }

    /// Reserves `bytes` on disk for a write about to land there, refused by name when the disk has no room or
    /// the cap is passed. The reservation gives them back when it is dropped.
    fn reserve(self: &Arc<Self>, disk: &Disk, bytes: u64) -> object_store::Result<Reserved> {
        self.claim(disk, bytes).map_err(full)?;
        Ok(Reserved { inner: self.clone(), bytes })
    }

    async fn on_disk(&self, at: &Path) -> object_store::Result<bool> {
        let Some(disk) = self.disk_of(at) else { return Ok(false) };
        match disk.store.head(at).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Takes `at` out of memory, if it is there.
    async fn leave_memory(&self, at: &Path) -> object_store::Result<()> {
        if self.mem.note(at, None) {
            self.objects.delete(at).await?;
        }
        Ok(())
    }

    /// Takes `at` off disk, if it is there. An object that isn't there is no error: a store's deletes
    /// succeed for what is missing, as S3's do.
    async fn leave_disk(&self, at: &Path) -> object_store::Result<()> {
        let Some(disk) = self.disk_of(at) else { return Ok(()) };
        match disk.store.delete(at).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                self.disk.note(at, None);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// `at` was just written to memory or to disk (`bytes`): the copy in the other tier goes, so a read finds
    /// the newest one wherever it is.
    async fn landed(&self, at: &Path, on_disk: bool, bytes: u64) -> object_store::Result<()> {
        if on_disk {
            self.disk.note(at, Some(bytes));
            return self.leave_memory(at).await;
        }
        self.mem.note(at, Some(bytes));
        self.leave_disk(at).await
    }

    async fn remove(&self, at: &Path) -> object_store::Result<()> {
        self.leave_memory(at).await?;
        self.leave_disk(at).await
    }

    async fn put_mem(&self, at: &Path, body: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        let bytes = body.content_length() as u64;
        if matches!(opts.mode, PutMode::Create) && self.on_disk(at).await? {
            return Err(already(at));
        }
        let put = self.objects.put_opts(at, body, opts).await?;
        self.landed(at, false, bytes).await?;
        Ok(put)
    }

    async fn put_disk(self: &Arc<Self>, at: &Path, body: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        if matches!(opts.mode, PutMode::Create) && self.mem.has(at) {
            return Err(already(at));
        }
        let disk = self.disk_or_make()?;
        let bytes = body.content_length() as u64;
        let _room = self.reserve(disk, bytes)?; // (held until `landed` has counted the bytes on disk)
        let put = disk.store.put_opts(at, body, opts).await?;
        self.landed(at, true, bytes).await?;
        Ok(put)
    }
}

/// A lake that goes gives its bytes on disk back to the count they were in (its reservations go with it: each holds the lake).
impl Drop for Inner {
    fn drop(&mut self) { self.spilled.on_disk.fetch_sub(self.disk.sum(), SeqCst); }
}

impl Disk {
    /// A lake's spill folder, made on the first object that spills.
    #[cfg(not(target_arch = "wasm32"))]
    fn make() -> object_store::Result<Disk> {
        let folder = Folder::make("memory").map_err(spill_failed)?;
        let store = FolderStore::new_with_prefix(&folder.path)?;
        Ok(Disk { store, folder })
    }

    /// A page's lake in memory doesn't spill: it has no folder, so a write past its budget is refused here, where a node
    /// would make its spill folder.
    #[cfg(target_arch = "wasm32")]
    fn make() -> object_store::Result<Disk> {
        let why = "out of memory: a lake in memory doesn't spill on a page (it has no folder), so a write past its budget is refused";
        Err(object_store::Error::Generic { store: "memory", source: Box::new(crate::codes::Coded("53200", why.into())) })
    }
}

/// A folder of this process's own under the user's spill folder for one kind of spill (a lake held in
/// memory's, or a query's and a shuffle's: `store::spill_folder`), with its lock file held. The folders of
/// processes that ended are swept first (`sweep`); dropping this one removes it.
#[derive(Debug)]
pub(crate) struct Folder {
    pub(crate) path: PathBuf,
    lock: Option<std::fs::File>,
}

impl Folder {
    /// A folder of this process's for `kind`, with its lock file held. It is in `MADE` from the moment it is made.
    pub(crate) fn make(kind: &str) -> std::io::Result<Folder> {
        let parent = own_dir(&spill_base(), &kind_dir(kind))?;
        sweep(&parent);
        let path = parent.join(format!("{}-{}", std::process::id(), uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&path)?;
        MADE.lock().unwrap_or_else(PoisonError::into_inner).insert(path.clone());
        // (until its lock is held the folder is a `Folder` with no lock: a failure below drops it, and its folder goes)
        let mut folder = Folder { path, lock: None };
        let lock = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(folder.path.join(LOCK_FILE))?;
        if lock.try_lock().is_err() {
            return Err(std::io::Error::other("its lock file is held by another process"));
        }
        folder.lock = Some(lock);
        Ok(folder)
    }
}

impl Drop for Folder {
    fn drop(&mut self) {
        drop(self.lock.take()); // (Windows can't delete a folder with an open file in it)
        let _ = std::fs::remove_dir_all(&self.path);
        MADE.lock().unwrap_or_else(PoisonError::into_inner).remove(&self.path); // (after its folder is gone: `sweep` skipped it meanwhile)
    }
}

/// Where a node's spills go: `PONDRA_SPILL_DIR` when set, else `PONDRA_CACHE_DIR` (with the node's other
/// scratch), else the temporary directory, unless that is itself held in memory (tmpfs: /tmp on Fedora
/// and Arch), where a spill would free nothing: then the user's cache folder. A lake held in memory
/// spills here too, and so do a query's spills (`store.rs`) and a shuffle's (`spill.rs`).
pub(crate) fn spill_base() -> PathBuf {
    if let Some(dir) = crate::env::get("PONDRA_SPILL_DIR").or_else(|| crate::env::get("PONDRA_CACHE_DIR")) {
        return PathBuf::from(dir);
    }
    let temp = std::env::temp_dir();
    #[cfg(target_os = "linux")]
    if held_in_ram(&temp) {
        let cache = std::env::var_os("XDG_CACHE_HOME").map(PathBuf::from).or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")));
        if let Some(cache) = cache.filter(|c| c.is_absolute()) {
            if !held_in_ram(&cache) {
                return cache;
            }
        }
    }
    temp
}

/// Is `dir` on a filesystem held in memory (tmpfs, ramfs)?
#[cfg(target_os = "linux")]
fn held_in_ram(dir: &FsPath) -> bool {
    use std::os::unix::ffi::OsStrExt;
    // (a folder not made yet is judged by its nearest existing parent: the filesystem it would be made on)
    let Some(dir) = dir.ancestors().find(|p| p.exists()) else { return false };
    let Ok(path) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else { return false };
    let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
    unsafe { libc::statfs(path.as_ptr(), &mut fs) == 0 && matches!(fs.f_type as u32, 0x0102_1994 | 0x8584_58f6) } // (TMPFS_MAGIC, RAMFS_MAGIC)
}

/// A folder named `name` in `base` that is this user's alone: `{name}-{uid}` on Unix (where /tmp is shared),
/// made 0700 and used only while it is ours alone, so no other user can read what is in it or take a lock
/// away; `name` elsewhere, where the temporary folder is the user's own. `base` is made first, parents and all
/// (a PONDRA_SPILL_DIR not made yet). Refused (PermissionDenied) when it is someone else's or open to others.
pub(crate) fn own_dir(base: &FsPath, name: &str) -> std::io::Result<PathBuf> {
    let dir = own_path(base, name);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::create_dir_all(base)?;
        let _ = std::fs::DirBuilder::new().mode(0o700).create(&dir); // (it may be there already)
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(&dir)?;
    check_alone(&dir)?;
    Ok(dir)
}

/// `name` in `base` as `own_dir` has it, if it is there and this user's alone. Makes nothing: what a node's start
/// asks (`sweep_all`), where nothing may be made.
pub(crate) fn own_dir_if_there(base: &FsPath, name: &str) -> Option<PathBuf> {
    let dir = own_path(base, name);
    check_alone(&dir).ok()?;
    Some(dir)
}

/// The folder `name` is in `base`: `{name}-{uid}` on Unix, where `/tmp` is shared, `name` elsewhere.
fn own_path(base: &FsPath, name: &str) -> PathBuf {
    #[cfg(unix)]
    {
        base.join(format!("{name}-{}", unsafe { libc::getuid() }))
    }
    #[cfg(not(unix))]
    {
        base.join(name)
    }
}

/// The one ownership check of `own_dir` and `own_dir_if_there`: `dir` is a folder, not a link, owned by this user
/// and open to no one else (Unix). Refused (PermissionDenied) otherwise.
fn check_alone(dir: &FsPath) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let m = std::fs::symlink_metadata(dir)?;
        if m.is_dir() && m.uid() == unsafe { libc::getuid() } && m.mode() & 0o077 == 0 {
            return Ok(());
        }
        Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, format!("{} is not this user's alone", dir.display())))
    }
    #[cfg(not(unix))]
    {
        if dir.is_dir() { Ok(()) } else { Err(std::io::Error::from(std::io::ErrorKind::NotFound)) }
    }
}

/// The name of the folders of one kind of spill in the user's spill folder: `pondra-{kind}`, `own_dir`'s.
fn kind_dir(kind: &str) -> String {
    format!("pondra-{kind}")
}

/// Removes the spill folders whose process has ended: a folder whose lock file this process can take
/// is one nobody holds, and a folder with no lock file at all is one whose process died between making
/// the folder and its lock (made at once, so none a minute on). Best effort. A folder made a minute ago is
/// left alone: its lock may not be taken yet (another lake in this process may be making it now). This
/// process's own folders are skipped before their lock is opened (on NFS a lock is per process: opening it
/// again would drop ours).
fn sweep(parent: &FsPath) {
    let Ok(entries) = std::fs::read_dir(parent) else { return };
    for entry in entries.flatten() {
        let dir = entry.path();
        if MADE.lock().unwrap_or_else(PoisonError::into_inner).contains(&dir) {
            continue;
        }
        let lock_path = dir.join(LOCK_FILE);
        match std::fs::metadata(&lock_path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if age(&dir).is_some_and(|a| a > Duration::from_secs(60)) {
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
            Ok(_) if age(&lock_path).is_some_and(|a| a > Duration::from_secs(60)) => {
                let Ok(lock) = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path) else { continue };
                if lock.try_lock().is_ok() {
                    drop(lock); // (Windows can't delete a folder with an open file in it)
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
            _ => {}
        }
    }
}

/// How long ago `path` was last changed, if it can say.
fn age(path: &FsPath) -> Option<Duration> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()?.elapsed().ok()
}

/// Removes the spill folders that processes which ended left, of both kinds (`sweep`). Run beside a node's start
/// (`node.rs`), on a thread of its own: a folder left by a crash may be gigabytes, and nothing is made here.
pub fn sweep_all() {
    for kind in ["memory", "spill"] {
        if let Some(parent) = own_dir_if_there(&spill_base(), &kind_dir(kind)) {
            sweep(&parent);
        }
    }
}

/// The program's end, for the ends that run no destructor (`process::exit`, and `main` returning past its
/// statics): the lakes held in memory go, then every folder this process made that is still there. Best
/// effort, no error. On Windows a folder whose lock file is still open (`store::SPILLS` is a static) can't
/// be removed, and stays for the next process's sweep.
pub fn leave() {
    let lakes = std::mem::take(&mut *MEMORY.lock().unwrap_or_else(PoisonError::into_inner));
    drop(lakes); // (after the lock is let go: a lake's folder goes with its last store, and its drop takes it out of `MADE`)
    let made = std::mem::take(&mut *MADE.lock().unwrap_or_else(PoisonError::into_inner));
    for path in made {
        let _ = std::fs::remove_dir_all(&path);
    }
}

/// What one blocking read of a spilled file takes; and what a move copies at a time, so a spilled file
/// never comes into memory whole (it was spilled because memory was short).
const PIECE: u64 = 8 << 20;

/// `range` of a spilled file, as a stream of pieces of at most `PIECE`, each read in one blocking call.
/// The file stays open while it is read, as a file's own stream keeps it: deleted meanwhile, it still reads.
#[cfg(not(target_arch = "wasm32"))]
fn pieces(file: std::fs::File, range: Range<u64>) -> BoxStream<'static, object_store::Result<Bytes>> {
    futures::stream::try_unfold((file, range.start), move |(mut file, at)| async move {
        if at >= range.end {
            return Ok(None);
        }
        let len = (range.end - at).min(PIECE) as usize;
        let read = tokio::task::spawn_blocking(move || {
            use std::io::{Read, Seek, SeekFrom};
            let mut buf = vec![0; len];
            file.seek(SeekFrom::Start(at))?;
            file.read_exact(&mut buf)?;
            Ok::<_, std::io::Error>((file, buf))
        });
        let (file, buf) = read.await.map_err(|e| read_failed(std::io::Error::other(e)))?.map_err(read_failed)?;
        Ok(Some((Bytes::from(buf), (file, at + len as u64))))
    })
    .boxed()
}

/// A multipart upload into memory or onto disk. When it completes, the object is where its parts went:
/// in memory (its bytes count toward the budget), or on disk (its copy in memory goes). On disk, each part's
/// bytes are reserved before the part is sent on; a refused part ends the upload (`put_part`).
#[derive(Debug)]
struct Upload {
    up: Box<dyn MultipartUpload>,
    inner: Arc<Inner>,
    at: Path,
    on_disk: bool,
    bytes: u64,
    room: Option<Reserved>,  // on disk: the room its parts take, reserved as they come
    refused: Option<String>, // why a part was refused: every later part, and `complete`, fail with it
}

#[async_trait::async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if let Some(why) = &self.refused {
            return refused(why.clone());
        }
        let bytes = data.content_length() as u64;
        if let Some(room) = self.room.as_mut() {
            if let Err(why) = room.grow(bytes) {
                // (a part not sent on would leave the local upload's offset behind, so completing it would land a
                // shifted file: every later part is refused too)
                self.refused = Some(why.clone());
                return refused(why);
            }
        }
        self.bytes += bytes;
        self.up.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if let Some(why) = self.refused.clone() {
            let _ = self.abort().await; // (the parts sent so far go; the refusal is the answer)
            return Err(full(why));
        }
        let result = match self.up.complete().await {
            Ok(done) => self.inner.landed(&self.at, self.on_disk, self.bytes).await.map(|()| done),
            Err(e) => Err(e),
        };
        self.room = None; // (landed: the bytes are the disk's; or the upload failed, and they are not)
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let done = self.up.abort().await;
        self.room = None;
        done
    }
}

#[async_trait::async_trait]
impl ObjectStore for Held {
    async fn put_opts(&self, at: &Path, body: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        if self.0.spills(at, &opts.mode, body.content_length() as u64) {
            self.0.put_disk(at, body, opts).await
        } else {
            self.0.put_mem(at, body, opts).await
        }
    }

    async fn put_multipart_opts(&self, at: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        let on_disk = self.0.spills(at, &PutMode::Overwrite, 0);
        let (up, room) = match on_disk {
            true => {
                let disk = self.0.disk_or_make()?;
                let room = self.0.reserve(disk, 0)?; // (the room is checked before the upload starts)
                (disk.store.put_multipart_opts(at, opts).await?, Some(room))
            }
            false => (self.0.objects.put_multipart_opts(at, opts).await?, None),
        };
        Ok(Box::new(Upload { up, inner: self.0.clone(), at: at.clone(), on_disk, bytes: 0, room, refused: None }))
    }

    async fn get_opts(&self, at: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let got = self.0.objects.get_opts(at, opts.clone()).await;
        let (Err(object_store::Error::NotFound { .. }), Some(disk)) = (&got, self.0.disk_of(at)) else { return got };
        #[cfg_attr(target_arch = "wasm32", allow(unused_mut))]
        let mut found = disk.store.get_opts(at, opts).await?;
        // (a file's own stream reads 8 KB a blocking call, which made queries over a spilled lake 1.4x
        // slower: read it a piece at a time instead, each piece one call)
        #[cfg(not(target_arch = "wasm32"))] // (a page has no files: its spills are refused, `Disk::make`)
        if let GetResultPayload::File(file, _) = found.payload {
            found.payload = GetResultPayload::Stream(pieces(file, found.range.clone()));
        }
        Ok(found)
    }

    fn delete_stream(&self, at: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.0.clone();
        at.then(move |p| {
            let inner = inner.clone();
            async move {
                let p = p?;
                inner.remove(&p).await?;
                Ok(p)
            }
        })
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let mem = self.0.objects.list(prefix);
        match self.0.tier.get() {
            Some(disk) => mem.chain(disk.store.list(prefix).try_filter(|m| ready(!hidden(m)))).boxed(),
            None => mem,
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut listed = self.0.objects.list_with_delimiter(prefix).await?;
        if let Some(disk) = self.0.tier.get() {
            let more = disk.store.list_with_delimiter(prefix).await?;
            listed.objects.extend(more.objects.into_iter().filter(|m| !hidden(m)));
            listed.common_prefixes.extend(more.common_prefixes);
            listed.objects.sort_by(|a, b| a.location.cmp(&b.location));
            listed.common_prefixes.sort();
            listed.common_prefixes.dedup();
        }
        Ok(listed)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> object_store::Result<()> {
        let inner = &*self.0;
        if matches!(opts.mode, CopyMode::Create) && (inner.mem.has(to) || inner.on_disk(to).await?) {
            return Err(already(to));
        }
        // (a copy on disk is a hard link: it keeps its bytes once the object it copied goes, so it counts them)
        match inner.mem.size(from) {
            Some(bytes) => {
                inner.objects.copy_opts(from, to, opts).await?;
                inner.landed(to, false, bytes).await
            }
            None => {
                let disk = inner.disk_of(from).ok_or_else(|| missing(from))?;
                let bytes = inner.disk.size(from).unwrap_or(0);
                disk.store.copy_opts(from, to, opts).await?;
                inner.landed(to, true, bytes).await
            }
        }
    }
}

/// The mark a move writes at its target first and takes away last, so a place it didn't finish is never taken
/// for a lake: a copy of one with no mark would open as a lake that is missing things. A lake's own objects are
/// under `catalog/`, `data/`, `log/`, `files/` and the like, so this name is never one of them.
const MOVING: &str = "_moving";

/// The objects a move copied, by path, each with the e-tag it had in the lake when it was read. Pass 2
/// copies what has changed since: the target's e-tags are its own, so only the lake's can say.
pub type Copied = BTreeMap<String, Option<String>>;

/// Refuses a place a move didn't finish (its `MOVING` mark is there), by name: a move to it, or a node opening
/// it (`store.rs`'s `Lake::open` runs this beside the catalog's own opening, so a bucket pays no extra trip).
pub async fn refuse_torn(store: &Store, place: &str) -> anyhow::Result<()> {
    match store.head(&Path::from(MOVING)).await {
        Ok(_) => Err(crate::codes::coded("55000", format!("a move to {place} didn't finish (its {MOVING} mark is there): remove what is there, or move elsewhere"))),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Is `e` the refusal `refuse_torn` makes of a place a move didn't finish?
pub fn torn(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.downcast_ref::<crate::codes::Coded>().is_some_and(|c| c.0 == "55000" && c.1.contains(MOVING)))
}

/// `ALTER DATABASE b SET LOCATION 'dir'` (ADR-072 §7, `db.save`): a lake held in this process's memory moves
/// to a folder or a bucket and goes on there, the same node and port, its tasks running. Every check passes
/// before anything is copied. Pass 1 copies the objects while the node still serves; then the host
/// (`node::move_lake`) stops the node, runs `settle`, and starts it again on the target.
pub async fn set_location(lake: &Lake, database: &str, to: &str) -> anyhow::Result<serde_json::Value> {
    let this = crate::ddl::lake_name(lake);
    if !in_memory(&lake.url) {
        return Err(crate::codes::coded("0A000", "SET LOCATION moves a lake held in memory; a lake on disk or in a bucket moves with BACKUP DATABASE … TO and RESTORE DATABASE … FROM"));
    }
    anyhow::ensure!(database == this, "ALTER DATABASE {database} SET LOCATION: this database is {this}");
    anyhow::ensure!(to.trim() != ":memory:" && !in_memory(to), "a lake moves to a folder or a bucket, not to memory");
    anyhow::ensure!(crate::branch::pins(lake).await?.is_none(), "a branch is made from this database and keeps its files: it moves once no branch is made from it");
    anyhow::ensure!(crate::node::movable(), "this node can't move its lake: it was started without a host that restarts it");
    let to = crate::ddl::full(to)?;
    crate::ext::owners_only(&to, format!("ALTER DATABASE … SET LOCATION {to}: a path on the server's own disk, which only the node's owner moves a lake into (the `pondra` shell, `pondra sql`, `connect()` of a lake): move it to a bucket (s3://, gs://, az://)"))?; // (the asker's context: `ddl_here` runs in the request)
    crate::backup::apart(lake, &to)?;
    crate::ext::reach_place(lake, &to).await?; // (a bucket: the secret whose scope covers it)
    let dest = crate::store::open_store(&to)?.1;
    refuse_torn(&dest, &to).await?; // (a move that didn't finish there: named, before the place is called holding other things)
    anyhow::ensure!(crate::backup::empty(&to).await?, "{to} holds other things: move into a new place (or an empty folder)");
    let metas: Vec<ObjectMeta> = lake.store.list(None).try_collect().await.context("listing the lake")?; // (before the mark: a failure here leaves the place as it was)
    // The mark goes first of the copies, alone (`Create`: a mark already there is another move's). From here on,
    // whatever stops this move leaves a place refused by name, never a copy that looks like a lake.
    let mark = serde_json::json!({"from": lake.url, "pid": std::process::id(), "started_ms": crate::log::now_ms()});
    dest.put_opts(&Path::from(MOVING), serde_json::to_vec(&mark)?.into(), PutOptions { mode: PutMode::Create, ..Default::default() }).await.context("marking the place")?;
    let done: Vec<anyhow::Result<Option<(String, Option<String>, u64)>>> = futures::stream::iter(metas)
        .map(|meta| {
            let (from, dest) = (lake.store.clone(), dest.clone());
            async move {
                let at = meta.location;
                let got = copy_at(&from, &dest, &at, true).await?;
                Ok(got.map(|(e_tag, n)| (at.to_string(), e_tag, n)))
            }
        })
        .buffer_unordered(16)
        .collect()
        .await;
    let (mut copied, mut objects, mut bytes, mut failed) = (Copied::new(), 0u64, 0u64, None);
    for d in done {
        match d {
            Ok(Some((at, e_tag, n))) => {
                copied.insert(at, e_tag);
                objects += 1;
                bytes += n;
            }
            Ok(None) => {} // (let go since it was listed)
            Err(e) => failed = failed.or(Some(e)),
        }
    }
    if let Some(e) = failed {
        return Err(cleared(&dest, &to, e.context(format!("copying the lake to {to}"))).await);
    }
    if let Err(e) = crate::node::move_lake(crate::node::Move { from: lake.url.clone(), to: to.clone(), copied }) {
        return Err(cleared(&dest, &to, e).await); // (no host took the move: the copies go, and the mark with them)
    }
    // (answered once the host has begun to stop the node: a statement sent next waits for the move, rather
    // than being answered by the node it is about to leave)
    for _ in 0..5000 {
        if crate::drain::draining() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Ok(serde_json::json!({"moving": to, "objects": objects, "bytes": bytes, "notice": "the node starts again on it: statements wait a moment, a Postgres connection reconnects; if the move fails, the lake stays in memory and pondra.databases.move_failed says why"}))
}

/// Copies the object at `at` from `from` to `to`, as it is. `create`: it must not be in `to` yet (pass 1,
/// into an empty place). `None`: it is gone from `from`. Otherwise its e-tag as the read saw it, and its size.
async fn copy_at(from: &Store, to: &Store, at: &Path, create: bool) -> anyhow::Result<Option<(Option<String>, u64)>> {
    let got = match from.get(at).await {
        Ok(got) => got,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let (e_tag, size) = (got.meta.e_tag.clone(), got.meta.size);
    if size <= PIECE {
        let mode = if create { PutMode::Create } else { PutMode::Overwrite };
        to.put_opts(at, got.bytes().await?.into(), PutOptions { mode, ..Default::default() }).await?;
    } else {
        let mut out = WriteMultipart::new_with_chunk_size(to.put_multipart(at).await?, PIECE as usize);
        let mut pieces = got.into_stream();
        while let Some(piece) = pieces.next().await {
            out.wait_for_capacity(1).await?;
            out.write(&piece?);
        }
        out.finish().await?;
    }
    Ok(Some((e_tag, size)))
}

/// Takes away what is at the target, so a move that failed leaves the place as pass 1 found it: empty. A listing,
/// not what the copies named: a failed copy may have written part of an object. The mark goes last, and only once
/// everything else is gone (a listing returns it first, a local folder in any order): a copy without its mark would
/// look like a lake, and one with it is refused by name. A failure keeps the mark and says how many objects are left.
async fn clear(dest: &Store) -> anyhow::Result<()> {
    let listed: Vec<Path> = dest.list(None).map_ok(|meta| meta.location).try_collect().await.context("listing the place to clear it (none of it is taken away)")?;
    let mark = Path::from(MOVING);
    let (others, marked): (Vec<Path>, Vec<Path>) = listed.into_iter().partition(|at| *at != mark);
    let (mut left, mut first): (usize, Option<object_store::Error>) = (0, None);
    for at in &others {
        if let Err(e) = take_away(dest, at).await {
            left += 1;
            first = first.or(Some(e));
        }
    }
    if let Some(e) = first {
        return Err(anyhow::Error::from(e).context(format!("{} of its objects couldn't be taken away (its {MOVING} mark stays)", left + marked.len())));
    }
    for at in &marked {
        take_away(dest, at).await.with_context(|| format!("its {MOVING} mark couldn't be taken away"))?;
    }
    Ok(())
}

/// Takes `at` away. One that is gone already is taken away too: a delete that finds it gone is no failure.
async fn take_away(dest: &Store, at: &Path) -> object_store::Result<()> {
    match dest.delete(at).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

/// The error a move that failed ends with: `e`, and, when the target can't be cleared either, that too. Its mark
/// stays then, so the place is refused by name until someone clears it.
async fn cleared(dest: &Store, to: &str, e: anyhow::Error) -> anyhow::Error {
    match clear(dest).await {
        Ok(()) => e,
        Err(c) => e.context(format!("{to} couldn't be cleared after it ({c:#})")),
    }
}

/// Removes the folders under `dir` that hold nothing, deepest first: what a cleared folder target keeps.
fn prune_empty(dir: &FsPath) {
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            prune_empty(&entry.path());
            let _ = std::fs::remove_dir(entry.path()); // (fails, and so stays, unless it is empty)
        }
    }
}

/// Pass 2, once the host has stopped the node (`node::Move`): the target is made equal to the lake as it is
/// now. What changed since pass 1 read it is copied again, what is new copied, what the lake no longer holds
/// taken away there, and the lake in memory let go.
pub async fn settle(m: &crate::node::Move) -> anyhow::Result<()> {
    let from = MEMORY.lock().unwrap_or_else(PoisonError::into_inner).get(m.from.trim_end_matches('/')).cloned().context("the lake to move is no longer in memory")?;
    let dest = crate::store::open_store(&m.to)?.1;
    if let Err(e) = settle_into(&from, &dest, m).await {
        // (a failed second pass leaves the lake in memory, whole, and nothing of the move at the target: or its
        // mark, when the target can't be cleared either, which refuses the place by name)
        let e = cleared(&dest, &m.to, e).await;
        if !m.to.contains("://") {
            prune_empty(FsPath::new(&m.to)); // (a folder keeps the folders its objects were in)
        }
        return Err(e);
    }
    forget_memory(&m.from);
    Ok(())
}

/// The copies and deletes of pass 2 (`settle`), which a failure of any of them undoes.
async fn settle_into(from: &Store, dest: &Store, m: &crate::node::Move) -> anyhow::Result<()> {
    let now: Vec<ObjectMeta> = from.list(None).try_collect().await?;
    let (mut keep, mut changed) = (BTreeSet::new(), vec![]);
    for meta in now {
        let key = meta.location.to_string();
        let same = matches!(m.copied.get(&key), Some(Some(e)) if meta.e_tag.as_deref() == Some(e.as_str()));
        if !same {
            changed.push(meta.location.clone());
        }
        keep.insert(key);
    }
    futures::stream::iter(changed)
        .map(|at| {
            let (from, dest) = (from.clone(), dest.clone());
            async move { copy_at(&from, &dest, &at, false).await.map(|_| ()) }
        })
        .buffer_unordered(16)
        .try_collect::<Vec<()>>()
        .await?;
    // (test hook: a second pass that fails, as a full disk would. After its copies, not the first one: a pass
    // with nothing changed copies nothing, and the hook must still fail it)
    if crate::store::crash_rolled("move_settle") {
        anyhow::bail!("PONDRA_CRASH at move_settle");
    }
    let stale: Vec<Path> = dest
        .list(None)
        .map_ok(|meta| meta.location)
        .try_filter(|at| {
            let key: &str = at.as_ref();
            ready(!keep.contains(key) && key != MOVING) // (the mark is in no lake's keep: it goes last, below)
        })
        .try_collect()
        .await?;
    for at in &stale {
        dest.delete(at).await?;
    }
    // The mark goes last: until then a copy that hasn't caught up is refused by name, never taken for a lake.
    take_away(dest, &Path::from(MOVING)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, TryStreamExt};

    fn bytes(n: u8) -> PutPayload { vec![n; 10].into() }

    async fn read(h: &Held, at: &str) -> Vec<u8> { h.get(&Path::from(at)).await.unwrap().bytes().await.unwrap().to_vec() }

    #[tokio::test]
    async fn past_the_budget_the_bulk_goes_to_disk_and_reads_back() {
        let h = Held::with_budget(10);
        h.put(&Path::from("catalog/c"), bytes(1)).await.unwrap(); // 10 bytes: in memory
        for (i, name) in ["data/t/a.parquet", "data/t/b.parquet", "data/t/c.parquet"].iter().enumerate() {
            h.put(&Path::from(*name), bytes(i as u8 + 2)).await.unwrap(); // past the budget: on disk
            assert!(!h.0.mem.has(&Path::from(*name)));
        }
        assert_eq!(read(&h, "data/t/b.parquet").await, vec![3; 10]);
        h.put(&Path::from("catalog/d"), bytes(9)).await.unwrap(); // the catalog stays in memory past it
        assert!(h.0.mem.has(&Path::from("catalog/d")));
        assert_eq!(read(&h, "catalog/d").await, vec![9; 10]);
        let mut data: Vec<String> = h.list(Some(&Path::from("data"))).map_ok(|m| m.location.to_string()).try_collect().await.unwrap();
        data.sort();
        assert_eq!(data, ["data/t/a.parquet", "data/t/b.parquet", "data/t/c.parquet"]);
        let all = h.list(None).try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(all.len(), 5, "the lock file is not an object");
        let root = h.list_with_delimiter(None).await.unwrap();
        let mut dirs: Vec<String> = root.common_prefixes.iter().map(|p| p.to_string()).collect();
        dirs.sort();
        assert_eq!(dirs, ["catalog", "data"]);
        assert!(root.objects.is_empty());
    }

    #[tokio::test]
    async fn a_file_that_would_pass_the_budget_goes_to_disk() {
        let h = Held::with_budget(15);
        h.put(&Path::from("data/t/a.parquet"), bytes(1)).await.unwrap(); // 10 of 15: in memory
        h.put(&Path::from("data/t/b.parquet"), bytes(2)).await.unwrap(); // 20 would pass it: on disk
        assert!(h.0.mem.has(&Path::from("data/t/a.parquet")) && !h.0.mem.has(&Path::from("data/t/b.parquet")));
        let first = Held::with_budget(5);
        first.put(&Path::from("data/t/a.parquet"), bytes(1)).await.unwrap(); // a first file bigger than the budget
        assert!(!first.0.mem.has(&Path::from("data/t/a.parquet")));
        assert_eq!(read(&first, "data/t/a.parquet").await, vec![1; 10]);
    }

    #[tokio::test]
    async fn deletes_reach_both_tiers_and_a_missing_one_is_no_error() {
        let h = Held::with_budget(10);
        h.put(&Path::from("catalog/c"), bytes(1)).await.unwrap();
        h.put(&Path::from("data/t/a"), bytes(2)).await.unwrap(); // on disk
        let gone: Vec<object_store::Result<Path>> = vec![Ok(Path::from("data/t/a")), Ok(Path::from("catalog/c")), Ok(Path::from("never/was"))];
        let done: Vec<Path> = h.delete_stream(futures::stream::iter(gone).boxed()).try_collect().await.unwrap();
        assert_eq!(done.len(), 3);
        for at in ["data/t/a", "catalog/c"] {
            assert!(matches!(h.get(&Path::from(at)).await, Err(object_store::Error::NotFound { .. })), "{at} is deleted");
        }
        assert_eq!(h.0.mem.sum(), 0);
    }

    #[tokio::test]
    async fn the_folder_is_there_while_the_lake_is() {
        let path = {
            let h = Held::with_budget(0); // every file spills at once
            h.put(&Path::from("data/t/a"), bytes(1)).await.unwrap();
            let path = h.0.tier.get().unwrap().folder.path.clone();
            assert!(path.join(LOCK_FILE).exists());
            path
        };
        assert!(!path.exists(), "dropping the lake removes its folder");
    }

    #[tokio::test]
    async fn create_refuses_what_is_already_on_disk() {
        let h = Held::with_budget(10);
        h.put(&Path::from("catalog/big"), vec![0u8; 100].into()).await.unwrap(); // past the budget
        let create = PutOptions { mode: PutMode::Create, ..Default::default() };
        h.put_opts(&Path::from("data/t/x"), bytes(1), create.clone()).await.unwrap(); // on disk
        assert!(matches!(h.put_opts(&Path::from("data/t/x"), bytes(2), create.clone()).await, Err(object_store::Error::AlreadyExists { .. })));
        h.delete(&Path::from("catalog/big")).await.unwrap(); // under the budget again: the write goes to memory
        assert!(matches!(h.put_opts(&Path::from("data/t/x"), bytes(3), create).await, Err(object_store::Error::AlreadyExists { .. })));
        assert_eq!(read(&h, "data/t/x").await, vec![1; 10]);
    }

    #[tokio::test]
    async fn an_overwrite_leaves_one_copy() {
        let h = Held::with_budget(10);
        h.put(&Path::from("catalog/c"), bytes(1)).await.unwrap();
        h.put(&Path::from("data/t/a"), bytes(2)).await.unwrap(); // on disk
        h.delete(&Path::from("catalog/c")).await.unwrap(); // under the budget again
        h.put(&Path::from("data/t/a"), bytes(3)).await.unwrap(); // to memory: the disk's copy goes
        assert_eq!(read(&h, "data/t/a").await, vec![3; 10]);
        assert_eq!(h.list(None).try_collect::<Vec<_>>().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_multipart_upload_under_the_budget_is_counted() {
        let h = Held::with_budget(1 << 30);
        let at = Path::from("catalog/m");
        let mut up = h.put_multipart_opts(&at, PutMultipartOptions::default()).await.unwrap();
        up.put_part(bytes(4)).await.unwrap();
        up.put_part(bytes(5)).await.unwrap();
        assert_eq!(h.0.mem.sum(), 0, "counted once it completes");
        up.complete().await.unwrap();
        assert_eq!(h.0.mem.sum(), 20);
        assert_eq!(read(&h, "catalog/m").await, [vec![4; 10], vec![5; 10]].concat());
    }

    #[tokio::test]
    async fn a_spilled_file_reads_whole_and_in_ranges_across_its_pieces() {
        let h = Held::with_budget(0);
        let body: Vec<u8> = (0..PIECE as usize * 2 + 5).map(|i| (i % 251) as u8).collect();
        let at = Path::from("data/t/f.parquet");
        h.put(&at, body.clone().into()).await.unwrap();
        assert!(!h.0.mem.has(&at));
        assert_eq!(read(&h, "data/t/f.parquet").await, body);
        let r = PIECE - 3..PIECE * 2 + 2;
        assert_eq!(h.get_range(&at, r.clone()).await.unwrap(), body[r.start as usize..r.end as usize]);
        let got = h.get_opts(&at, GetOptions { range: Some((PIECE + 1..PIECE + 9).into()), ..Default::default() }).await.unwrap();
        assert_eq!(got.range, PIECE + 1..PIECE + 9);
        assert_eq!(got.bytes().await.unwrap(), body[PIECE as usize + 1..PIECE as usize + 9]);
    }

    #[tokio::test]
    async fn a_spill_past_the_cap_is_refused_by_name_and_a_delete_gives_its_room_back() {
        let h = Held::with_cap(0, 15); // every file spills; the disk may take 15 bytes of them
        h.put(&Path::from("data/t/a"), bytes(1)).await.unwrap(); // 10 bytes: under the cap
        let err = h.put(&Path::from("data/t/b"), bytes(2)).await.unwrap_err(); // 20 bytes would pass it
        let text = err.to_string();
        assert!(text.contains("this process's spills may take") && text.contains("PONDRA_SPILL_GB"), "{text}");
        assert_eq!(crate::codes::of(&anyhow::Error::from(err)), "53100");
        assert!(matches!(h.get(&Path::from("data/t/b")).await, Err(object_store::Error::NotFound { .. })), "a refused write leaves nothing");
        h.delete(&Path::from("data/t/a")).await.unwrap(); // gives its 10 bytes back
        h.put(&Path::from("data/t/b"), bytes(2)).await.unwrap(); // lands now
        assert_eq!(read(&h, "data/t/b").await, vec![2; 10]);
        assert_eq!(h.0.disk.sum(), 10);
    }

    #[tokio::test]
    async fn a_multipart_upload_past_the_cap_fails_at_the_part_that_passes_it() {
        let h = Held::with_cap(0, 15);
        let at = Path::from("data/t/m");
        let mut up = h.put_multipart_opts(&at, PutMultipartOptions::default()).await.unwrap();
        up.put_part(bytes(4)).await.unwrap(); // 10 bytes: under the cap
        assert!(up.put_part(bytes(5)).await.is_err(), "the second part passes the cap");
        assert!(up.put_part(bytes(6)).await.is_err(), "and so does every part after it");
        up.abort().await.unwrap();
        assert!(matches!(h.get(&at).await, Err(object_store::Error::NotFound { .. })), "an aborted upload leaves nothing");
        assert_eq!(h.0.spilled.reserved.load(SeqCst), 0, "and gives its room back");
        let mut again = h.put_multipart_opts(&at, PutMultipartOptions::default()).await.unwrap();
        again.put_part(bytes(4)).await.unwrap();
        assert!(again.put_part(bytes(5)).await.is_err());
        assert!(again.complete().await.is_err(), "a completed upload past the cap is refused too");
        assert!(matches!(h.get(&at).await, Err(object_store::Error::NotFound { .. })), "and leaves nothing");
        assert_eq!(h.0.spilled.reserved.load(SeqCst), 0);
        assert_eq!(h.0.disk.sum(), 0);
    }

    #[tokio::test]
    async fn two_writes_racing_past_the_cap_one_lands_and_one_is_refused() {
        let h = Held::with_cap(0, 15);
        let (pa, pb) = (Path::from("data/t/a"), Path::from("data/t/b"));
        let (a, b) = tokio::join!(h.put(&pa, bytes(1)), h.put(&pb, bytes(2)));
        assert_eq!([a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count(), 1, "one lands, the other is refused");
        assert_eq!(h.0.disk.sum(), 10);
    }

    #[tokio::test]
    async fn a_copy_on_disk_lands_and_is_counted() {
        let h = Held::with_cap(0, 1000);
        h.put(&Path::from("data/t/a"), bytes(1)).await.unwrap(); // on disk: 10 bytes
        h.copy(&Path::from("data/t/a"), &Path::from("data/t/b")).await.unwrap();
        assert_eq!(read(&h, "data/t/b").await, vec![1; 10]);
        assert_eq!(h.0.disk.sum(), 20, "the copy's bytes count as the source's did");
    }

    /// Two lakes sharing one `Spilled` and one cap (what the process's lakes share under the setting): the second's spill
    /// past what is left is refused by name, a delete in the first gives its room back, and racing writes from both
    /// never take the last of it together.
    #[tokio::test]
    async fn two_lakes_share_one_cap_and_a_delete_gives_its_room_back() {
        let shared: Arc<Spilled> = Arc::default();
        let a = Held::sharing(0, Some(15), shared.clone()); // every file spills; the spills of both may take 15 bytes
        let b = Held::sharing(0, Some(15), shared.clone());
        a.put(&Path::from("data/t/a"), bytes(1)).await.unwrap(); // 10 bytes of the 15
        let err = b.put(&Path::from("data/t/b"), bytes(2)).await.unwrap_err(); // 20 would pass it, with a's 10
        let text = err.to_string();
        assert!(text.contains("this process's spills may take") && text.contains("PONDRA_SPILL_GB"), "{text}");
        assert_eq!(crate::codes::of(&anyhow::Error::from(err)), "53100");
        assert!(matches!(b.get(&Path::from("data/t/b")).await, Err(object_store::Error::NotFound { .. })), "a refused write leaves nothing");
        a.delete(&Path::from("data/t/a")).await.unwrap(); // gives its 10 bytes back to b
        b.put(&Path::from("data/t/b"), bytes(2)).await.unwrap(); // lands now
        b.delete(&Path::from("data/t/b")).await.unwrap();
        let (x, y) = tokio::join!(a.put(&Path::from("data/t/x"), bytes(3)), b.put(&Path::from("data/t/y"), bytes(4)));
        assert_eq!([x.is_ok(), y.is_ok()].iter().filter(|ok| **ok).count(), 1, "racing for the last of the room, one lands");
        assert_eq!(shared.total(), 10, "and the two lakes hold what landed");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_folder_held_in_memory_is_told_apart() {
        if FsPath::new("/dev/shm").is_dir() {
            assert!(held_in_ram(FsPath::new("/dev/shm")), "/dev/shm is tmpfs");
        }
        assert!(!held_in_ram(FsPath::new(env!("CARGO_MANIFEST_DIR"))), "the checkout is on disk");
    }

    /// A folder under `parent` with its lock file made, returned open. `old` sets the lock's time two minutes back.
    fn lock_in(parent: &FsPath, name: &str, old: bool) -> std::fs::File {
        let dir = parent.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let lock = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(dir.join(LOCK_FILE)).unwrap();
        if old {
            lock.set_modified(std::time::SystemTime::now() - Duration::from_secs(120)).unwrap();
        }
        lock
    }

    #[test]
    fn sweep_removes_a_folder_whose_process_ended_and_keeps_the_rest() {
        let parent = std::env::temp_dir().join(format!("pondra-sweep-{}", uuid::Uuid::new_v4().simple()));
        drop(lock_in(&parent, "ended", true)); // its lock is free, and it was last touched two minutes ago
        let alive = lock_in(&parent, "alive", true); // as old, but its lock is held
        alive.try_lock().unwrap();
        drop(lock_in(&parent, "new", false)); // free, but made just now: its lock may not be taken yet
        sweep(&parent);
        assert!(!parent.join("ended").exists(), "a folder whose process ended is removed");
        assert!(parent.join("alive").exists(), "a folder whose lock is held is kept");
        assert!(parent.join("new").exists(), "a folder made just now is kept");
        drop(alive);
        let _ = std::fs::remove_dir_all(&parent);
    }

    #[cfg(unix)]
    #[test]
    fn own_dir_is_this_users_alone_and_refused_once_others_can_open_it() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("pondra-own-{}", uuid::Uuid::new_v4().simple()));
        let base = root.join("a").join("b"); // not made yet: `own_dir` makes it, parents and all
        let uid = unsafe { libc::getuid() };
        let dir = own_dir(&base, "x").unwrap();
        assert_eq!(dir, base.join(format!("x-{uid}")));
        assert_eq!(std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777, 0o700);
        assert_eq!(own_dir(&base, "x").unwrap(), dir, "called again, it gives the same folder");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let refused = own_dir(&base, "x").unwrap_err();
        assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn own_dir_if_there_makes_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("pondra-own-if-{}", uuid::Uuid::new_v4().simple()));
        let base = root.join("a");
        assert!(own_dir_if_there(&base, "x").is_none(), "not there: none");
        assert!(!root.exists(), "and nothing is made");
        let dir = own_dir(&base, "x").unwrap();
        assert_eq!(own_dir_if_there(&base, "x"), Some(dir.clone()));
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(own_dir_if_there(&base, "x").is_none(), "open to others: none");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_folder_this_process_made_is_kept_by_sweep() {
        let parent = std::env::temp_dir().join(format!("pondra-sweep-made-{}", uuid::Uuid::new_v4().simple()));
        drop(lock_in(&parent, "mine", true)); // old, and its lock free: only this process's own list keeps it
        let path = parent.join("mine");
        MADE.lock().unwrap_or_else(PoisonError::into_inner).insert(path.clone());
        sweep(&parent);
        assert!(path.exists(), "a folder this process made is kept, whatever its lock");
        MADE.lock().unwrap_or_else(PoisonError::into_inner).remove(&path);
        let _ = std::fs::remove_dir_all(&parent);
    }

    #[cfg(unix)]
    #[test]
    fn a_folder_with_no_lock_file_goes_once_a_minute_old() {
        let parent = std::env::temp_dir().join(format!("pondra-sweep-bare-{}", uuid::Uuid::new_v4().simple()));
        let bare = parent.join("bare");
        std::fs::create_dir_all(&bare).unwrap(); // no lock file: its process died before it made one
        std::fs::File::open(&bare).unwrap().set_modified(std::time::SystemTime::now() - Duration::from_secs(120)).unwrap();
        std::fs::create_dir_all(parent.join("new")).unwrap(); // made just now: its lock may not be made yet
        sweep(&parent);
        assert!(!bare.exists(), "a folder with no lock file, a minute old, is removed");
        assert!(parent.join("new").exists(), "a folder made just now is kept");
        let _ = std::fs::remove_dir_all(&parent);
    }

    /// A folder of this test's, not made yet (`open_store` makes it), and its store: a move's target.
    fn target(name: &str) -> (PathBuf, Store) {
        let folder = std::env::temp_dir().join(format!("pondra-move-{name}-{}", uuid::Uuid::new_v4().simple()));
        let (_, store, _) = crate::store::open_store(folder.to_str().unwrap()).unwrap();
        (folder, store)
    }

    #[tokio::test]
    async fn settle_takes_the_mark_away_last_and_leaves_the_lake_whole() {
        let lake = format!("memory://mc-{}", uuid::Uuid::new_v4().simple());
        let from = store(&lake);
        let objects = ["catalog/manifest", "data/t/a.parquet", "files/notes/a.txt"];
        for at in objects {
            from.put(&Path::from(at), bytes(1)).await.unwrap();
        }
        let (folder, dest) = target("settle");
        dest.put(&Path::from(MOVING), bytes(9)).await.unwrap(); // (as a move's first step leaves it)
        let to = folder.to_str().unwrap().to_string();
        settle(&crate::node::Move { from: lake, to, copied: Copied::new() }).await.unwrap();
        let mut at: Vec<String> = dest.list(None).map_ok(|meta| meta.location.to_string()).try_collect().await.unwrap();
        at.sort();
        assert_eq!(at, objects, "every object of the lake, and no mark");
        let _ = std::fs::remove_dir_all(&folder);
    }

    #[tokio::test]
    async fn a_place_a_move_didnt_finish_is_refused_by_name() {
        let (folder, dest) = target("torn");
        let place = folder.to_str().unwrap().to_string();
        refuse_torn(&dest, &place).await.expect("a place with no mark is no torn place");
        dest.put(&Path::from(MOVING), bytes(1)).await.unwrap();
        let e = refuse_torn(&dest, &place).await.unwrap_err();
        assert_eq!(crate::codes::of(&e), "55000");
        assert!(torn(&e), "the refusal is the torn place's");
        let words = format!("{e:#}");
        assert!(words.contains(&place) && words.contains(MOVING), "{words}");
        assert!(!torn(&anyhow::anyhow!("{place} holds other things")), "another error is not");
        let _ = std::fs::remove_dir_all(&folder);
    }

    #[tokio::test]
    async fn clear_takes_everything_away_the_mark_too() {
        let (folder, dest) = target("clear");
        dest.put(&Path::from(MOVING), bytes(1)).await.unwrap();
        for at in ["catalog/manifest", "data/t/a.parquet"] {
            dest.put(&Path::from(at), bytes(2)).await.unwrap();
        }
        clear(&dest).await.unwrap();
        let left: Vec<String> = dest.list(None).map_ok(|meta| meta.location.to_string()).try_collect().await.unwrap();
        assert!(left.is_empty(), "{left:?}");
        let _ = std::fs::remove_dir_all(&folder);
    }

    #[test]
    fn a_folder_is_in_made_until_it_goes() {
        let folder = Folder::make("test").unwrap();
        let path = folder.path.clone();
        assert!(MADE.lock().unwrap_or_else(PoisonError::into_inner).contains(&path));
        drop(folder);
        assert!(!path.exists(), "dropping it removes it");
        assert!(!MADE.lock().unwrap_or_else(PoisonError::into_inner).contains(&path));
    }
}
