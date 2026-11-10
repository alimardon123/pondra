//! A lake held in this process's memory (`memory://…`, DuckDB's `:memory:`), and its spill to disk.
//!
//! Everything stays in memory below the budget. Past it, the bulk of a lake (its files and its log:
//! `data/` and `log/`) goes to a folder of this process's on disk (`scratch::spill_base`), so loading more
//! than the memory holds no longer ends the process. The catalog and `files/` (replaced by
//! their version: `PutMode::Update`) stay in memory: they are small, and a local folder can't take
//! an update.
//!
//! What spills is one budget for everything this process spills: `PONDRA_SPILL_GB`, else the disk's room, shared
//! with the queries' and the shuffles' spills (`ledger::room`). The lakes in memory count their bytes on
//! disk and their reserved bytes together (`Spilled`, one for the process), so the spill disk is counted once for
//! all of them. The room a lake keeps on its disk is kept either way. Past either, the write is refused by name
//! (53100), and a write that would pass it reserves its bytes first, so writes racing for the last of the room
//! can't both take it.
//!
//! A lake moves out of memory with `memory::set_location` (the host's: `ALTER DATABASE … SET LOCATION`,
//! `db.save`), which is refused at a place a move didn't finish (`refuse_torn`).
use crate::store::Store;
use futures::future::ready;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{CopyMode, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart};
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use {bytes::Bytes, object_store::GetResultPayload, std::ops::Range};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};


/// The lakes held in this process's memory, by place: every open of one here is the same store until
/// `forget_memory`, so a node that restarts in place finds its lake as it left it. Nothing of one is
/// on disk, and no other process reaches it. (The process's, not a node's: a restart in place keeps its lake.)
static MEMORY: std::sync::LazyLock<Mutex<HashMap<String, Store>>> = std::sync::LazyLock::new(Default::default);

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
/// It is on the spill disk's room list (`ledger::holds`) from its first use, so every spill's check counts it.
static PROCESS: std::sync::LazyLock<Arc<Spilled>> = std::sync::LazyLock::new(|| {
    let spilled = Arc::<Spilled>::default();
    crate::ledger::holds(&(spilled.clone() as Arc<dyn crate::ledger::Holds>));
    spilled
});

/// What the lakes in memory take of the spill disk, on disk or reserved for spills in flight (`ledger::room`).
impl crate::ledger::Holds for Spilled {
    fn held(&self) -> u64 { self.total() }
}

/// The program's end (`memory::leave`): every lake held in this process's memory goes, and its spill folder with it.
pub(crate) fn forget_all() {
    let lakes = std::mem::take(&mut *MEMORY.lock().unwrap_or_else(PoisonError::into_inner));
    drop(lakes); // (after the lock is let go: a lake's folder goes with its last store)
}

/// Is `url` a lake held in this process's memory?
pub fn in_memory(url: &str) -> bool { url.starts_with("memory://") }

/// Is a lake held in this process's memory at `url`? (Opening one there makes it.)
pub fn memory_held(url: &str) -> bool { MEMORY.lock().unwrap_or_else(PoisonError::into_inner).contains_key(url.trim_end_matches('/')) }

/// The counted opens of the lakes held here (`store::counted`), each with the store it counts the requests of: an open
/// is its node's, so it is a store of its own, and `held_in_memory` knows it as its lake's by this list. Weak both, so
/// neither stays for it: an entry goes once either has gone. (The process's, as `MEMORY` is: an open outlives no lake
/// here, and a restart in place keeps its lake.)
static COUNTED: std::sync::LazyLock<Mutex<Vec<(std::sync::Weak<dyn object_store::ObjectStore>, std::sync::Weak<dyn object_store::ObjectStore>)>>> = std::sync::LazyLock::new(Default::default);

/// `open`, which counts the requests of `store`, is that store still: a lake held in memory's, if it is one.
pub(crate) fn counts(open: &Store, store: &Store) {
    if held_in_memory(store) {
        let mut counted = COUNTED.lock().unwrap_or_else(PoisonError::into_inner);
        counted.retain(|(o, s)| o.strong_count() > 0 && s.strong_count() > 0);
        counted.push((Arc::downgrade(open), Arc::downgrade(store)));
    }
}

/// Is `store` a lake held in this process's memory? Every open of one gives the same store, and a counted open of it
/// (`counts`) is that store too: a term claimed through one is this process's, with no lock file (`terms::claim`).
pub fn held_in_memory(store: &Store) -> bool {
    let held = |s: &Store| MEMORY.lock().unwrap_or_else(PoisonError::into_inner).values().any(|h| Arc::ptr_eq(h, s));
    let inner = COUNTED.lock().unwrap_or_else(PoisonError::into_inner).iter().find(|(o, _)| std::ptr::addr_eq(o.as_ptr(), Arc::as_ptr(store))).and_then(|(_, s)| s.upgrade());
    held(store) || inner.is_some_and(|s| held(&s))
}

/// A lake's place as given: `:memory:` (DuckDB's word) is a new lake in this process's memory.
pub fn place(lake: &str) -> String {
    match lake {
        ":memory:" => format!("memory://{}", uuid::Uuid::new_v4().simple()),
        _ => lake.to_string(),
    }
}

/// The store of the lake held in memory at `url`, if one is (a move's source: `memory::settle`); none is made.
pub(crate) fn held(url: &str) -> Option<Store> { MEMORY.lock().unwrap_or_else(PoisonError::into_inner).get(url.trim_end_matches('/')).cloned() }

/// Let a lake in memory go (the library's close, once its node has stopped): its memory with it.
pub fn forget_memory(url: &str) { MEMORY.lock().unwrap_or_else(PoisonError::into_inner).remove(url.trim_end_matches('/')); }

/// The store of the lake held in memory at `url`: the same one for every open of it, made on the first.
pub fn store(url: &str) -> Store {
    MEMORY.lock().unwrap_or_else(PoisonError::into_inner).entry(url.trim_end_matches('/').to_string()).or_insert_with(|| Arc::new(Held::new())).clone()
}

/// Holds `given` (a page's private folder, OPFS) as the lake in memory at `url`, so every open of `url` gives it
/// (`store`), as it gives a lake made in memory; `forget_memory` lets it go, its objects staying where they are.
pub fn give(url: &str, given: Store) { MEMORY.lock().unwrap_or_else(PoisonError::into_inner).insert(url.trim_end_matches('/').to_string(), given); }

/// The bytes past which a lake's files and log go to disk: `PONDRA_MEMORY_LAKE_GB`, else a quarter of
/// the machine's memory (4 GB where it can't say).
fn budget() -> u64 {
    match crate::env::parse::<f64>("PONDRA_MEMORY_LAKE_GB") {
        Some(gb) => (gb * (1u64 << 30) as f64) as u64,
        None => crate::machine::process().ram().map_or(4 << 30, |r| (r / 4) as u64),
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

/// The folder spilled objects go to: the platform's `"memory"` space (natively a `Folder` of this process's own), which goes
/// when the lake does.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))] // (a page's `Disk::make` refuses: none is made there)
#[derive(Debug)]
struct Disk {
    store: FolderStore,
    space: crate::scratch::Space,
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

/// The spill folder's lock file is not one of the lake's objects.
fn hidden(meta: &ObjectMeta) -> bool {
    let key: &str = meta.location.as_ref();
    key == crate::scratch::LOCK_FILE
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
    // (coded: a bucket's error is otherwise the machine's, 58030, whatever its words: `codes::of`)
    let why = format!("{what}: raise PONDRA_SPILL_GB or PONDRA_MEMORY_LAKE_GB, or move the lake to a folder (ALTER DATABASE … SET LOCATION)");
    object_store::Error::Generic { store: "memory", source: Box::new(crate::codes::Coded("53100", why)) }
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
        crate::store::lake_file(at.as_ref()) && !matches!(mode, PutMode::Update(_)) && self.mem.sum() + bytes.max(1) > self.budget
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
    fn disk_of(&self, at: &Path) -> Option<&Disk> { self.tier.get().filter(|_| crate::store::lake_file(at.as_ref())) }

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

    /// What the process's other holders keep on its spill disk (`ledger::others_hold`). A test's cap counts its lake
    /// alone: the process's list holds the other tests' lakes, run at once.
    #[cfg(test)]
    fn others(&self) -> u64 {
        match self.cap {
            Some(_) => 0,
            None => crate::ledger::others_hold(&*self.spilled),
        }
    }

    #[cfg(not(test))]
    fn others(&self) -> u64 { crate::ledger::others_hold(&*self.spilled) }

    /// Adds `bytes` to what is reserved for spills in flight, if they fit; else says why not. The one check for
    /// `reserve` and `Reserved::grow`. Under the setting, everything this process spills must stay within it: the
    /// others' bytes (`ledger::others_hold`) and the lakes' (`on_disk` and `reserved`). Without it, the bytes in flight
    /// come off the disk's free room, which counts only what has landed. The check and the reservation are one
    /// `fetch_update` on `reserved`, so writes racing for the last of the room can't both take it. The room and
    /// `on_disk` are read after `reserved` (SeqCst): a landing is on disk before its reservation goes, so it is
    /// counted once, in one or the other, never in neither.
    fn claim(&self, disk: &Disk, bytes: u64) -> Result<(), String> {
        if let Some((free, keep)) = crate::store::short_of_room(&disk.space.dir) {
            return Err(format!("the disk a lake in memory spills to has {} MB free, and a lake keeps {} MB", free >> 20, keep >> 20));
        }
        let gb = self.setting();
        let others = gb.map_or(0, |_| self.others()); // (the queries' and the shuffles' spills count under the setting only)
        let fits = |reserved: u64| {
            let room = crate::machine::process().space(&disk.space.dir).map(|(free, total)| (free.saturating_sub(reserved), total));
            crate::store::piece_fits(gb, others + self.spilled.on_disk.load(SeqCst) + reserved, bytes, room)
        };
        self.spilled
            .reserved
            .fetch_update(SeqCst, SeqCst, |reserved| fits(reserved).then_some(reserved + bytes))
            .map(|_| ())
            .map_err(|_| match gb {
                Some(gb) => format!("this process's spills may take {:.1} MB of disk and hold {} MB there", gb * 1024.0, (self.others() + self.spilled.total()) >> 20),
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
        let space = crate::scratch::process().space("memory").map_err(spill_failed)?;
        let store = FolderStore::new_with_prefix(&space.dir)?;
        Ok(Disk { store, space })
    }

    /// A page's lake in memory doesn't spill: it has no folder, so a write past its budget is refused here, where a node
    /// would make its spill folder.
    #[cfg(target_arch = "wasm32")]
    fn make() -> object_store::Result<Disk> {
        let why = "out of memory: a lake in memory doesn't spill on a page (it has no folder), so a write past its budget is refused";
        Err(object_store::Error::Generic { store: "memory", source: Box::new(crate::codes::Coded("53200", why.into())) })
    }
}

/// What one blocking read of a spilled file takes; and what a move copies at a time, so a spilled file
/// never comes into memory whole (it was spilled because memory was short).
pub(crate) const PIECE: u64 = 8 << 20;

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
pub(crate) const MOVING: &str = "_moving";

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
            let path = h.0.tier.get().unwrap().space.dir.clone();
            assert!(path.join(crate::scratch::LOCK_FILE).exists());
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
        let (px, py) = (Path::from("data/t/x"), Path::from("data/t/y")); // (the puts borrow their paths while they race)
        let (x, y) = tokio::join!(a.put(&px, bytes(3)), b.put(&py, bytes(4)));
        assert_eq!([x.is_ok(), y.is_ok()].iter().filter(|ok| **ok).count(), 1, "racing for the last of the room, one lands");
        assert_eq!(shared.total(), 10, "and the two lakes hold what landed");
    }
}
