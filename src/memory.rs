//! A lake held in this process's memory (`memory://…`, DuckDB's `:memory:`), and its spill to disk.
//!
//! Everything stays in memory below the budget. Past it, the bulk of a lake (its files and its log:
//! `data/` and `log/`) goes to a folder of this process's in the temporary directory, so loading
//! more than the memory holds no longer ends the process. The catalog and `files/` (replaced by
//! their version: `PutMode::Update`) stay in memory: they are small, and a local folder can't take
//! an update.
use crate::store::{Counted, Store};
use futures::future::ready;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{CopyMode, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart};
use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

/// The lock file in a spill folder. The process that made the folder holds it, so a folder whose
/// lock another process can take is one its process left behind. It is not an object of the lake.
const LOCK_FILE: &str = ".lock";

/// The lakes held in this process's memory, by place: every open of one here is the same store until
/// `forget_memory`, so a node that restarts in place finds its lake as it left it. Nothing of one is
/// on disk, and no other process reaches it. (Not `per_node!`: a restart in place resets those.)
static MEMORY: std::sync::LazyLock<Mutex<HashMap<String, Store>>> = std::sync::LazyLock::new(Default::default);

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
    mem: InMemory,
    disk: OnceLock<Disk>, // (made on the first object that spills)
    sizes: Mutex<HashMap<Path, u64>>, // the bytes of each object in `mem`: every object put there is listed
    held: AtomicU64, // their sum: what the budget is measured against
    budget: u64,
}

/// The folder spilled objects go to, with its lock file held. Dropping it removes the folder.
#[derive(Debug)]
struct Disk {
    store: LocalFileSystem,
    path: PathBuf,
    lock: Option<std::fs::File>,
}

impl Held {
    /// A lake held in memory, spilling past this process's budget.
    pub fn new() -> Held { Held::with_budget(budget()) }

    /// A lake held in memory, spilling past `budget` bytes of it in memory.
    pub fn with_budget(budget: u64) -> Held {
        Held(Arc::new(Inner { mem: InMemory::new(), disk: OnceLock::new(), sizes: Default::default(), held: AtomicU64::new(0), budget }))
    }
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

fn spill_failed(e: std::io::Error) -> object_store::Error {
    object_store::Error::Generic { store: "memory", source: format!("past its budget, a lake in memory spills to a folder in the temporary directory, which can't be used: {e}").into() }
}

impl Inner {
    fn in_mem(&self, at: &Path) -> bool { self.sizes.lock().unwrap_or_else(PoisonError::into_inner).contains_key(at) }

    fn size(&self, at: &Path) -> Option<u64> { self.sizes.lock().unwrap_or_else(PoisonError::into_inner).get(at).copied() }

    /// Records that `at` now holds `bytes` in memory (`None`: it is not in memory any more), taking off
    /// the size it had. Returns whether it was in memory before.
    fn note(&self, at: &Path, bytes: Option<u64>) -> bool {
        let mut sizes = self.sizes.lock().unwrap_or_else(PoisonError::into_inner);
        let old = sizes.remove(at);
        if let Some(old) = old {
            self.held.fetch_sub(old, Relaxed);
        }
        if let Some(bytes) = bytes {
            sizes.insert(at.clone(), bytes);
            self.held.fetch_add(bytes, Relaxed);
        }
        old.is_some()
    }

    /// Does `at` go to disk when it is written now? Only the bulk of a lake, and only past the budget.
    /// An update stays in memory, where its version is checked.
    fn spills(&self, at: &Path, mode: &PutMode) -> bool {
        spillable(at) && !matches!(mode, PutMode::Update(_)) && self.held.load(Relaxed) >= self.budget
    }

    /// The disk tier of this lake, made on the first need.
    fn disk_or_make(&self) -> object_store::Result<&Disk> {
        if let Some(disk) = self.disk.get() {
            return Ok(disk);
        }
        let made = Disk::make()?;
        Ok(self.disk.get_or_init(|| made)) // (a racing make loses here, and its folder goes with it)
    }

    /// The disk tier, if it exists and `at` may be on it.
    fn disk_of(&self, at: &Path) -> Option<&Disk> { self.disk.get().filter(|_| spillable(at)) }

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
        if self.note(at, None) {
            self.mem.delete(at).await?;
        }
        Ok(())
    }

    /// Takes `at` off disk, if it is there. An object that isn't there is no error: a store's deletes
    /// succeed for what is missing, as S3's do.
    async fn leave_disk(&self, at: &Path) -> object_store::Result<()> {
        let Some(disk) = self.disk_of(at) else { return Ok(()) };
        match disk.store.delete(at).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// `at` was just written to memory (`bytes`) or to disk: the copy in the other tier goes, so a
    /// read finds the newest one wherever it is.
    async fn landed(&self, at: &Path, on_disk: bool, bytes: u64) -> object_store::Result<()> {
        if on_disk {
            return self.leave_memory(at).await;
        }
        self.note(at, Some(bytes));
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
        let put = self.mem.put_opts(at, body, opts).await?;
        self.landed(at, false, bytes).await?;
        Ok(put)
    }

    async fn put_disk(&self, at: &Path, body: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        if matches!(opts.mode, PutMode::Create) && self.in_mem(at) {
            return Err(already(at));
        }
        let put = self.disk_or_make()?.store.put_opts(at, body, opts).await?;
        self.landed(at, true, 0).await?;
        Ok(put)
    }
}

impl Disk {
    /// A folder of this process's in the temporary directory, with its lock file held. Folders whose
    /// process has ended are removed first.
    fn make() -> object_store::Result<Disk> {
        let parent = spill_parent().map_err(spill_failed)?;
        sweep(&parent);
        let path = parent.join(format!("{}-{}", std::process::id(), uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&path).map_err(spill_failed)?;
        let lock = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(path.join(LOCK_FILE)).map_err(spill_failed)?;
        if lock.try_lock().is_err() {
            return Err(spill_failed(std::io::Error::other("its lock file is held by another process")));
        }
        let store = LocalFileSystem::new_with_prefix(&path)?;
        Ok(Disk { store, path, lock: Some(lock) })
    }
}

impl Drop for Disk {
    fn drop(&mut self) {
        drop(self.lock.take()); // (Windows can't delete a folder with an open file in it)
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// The folder the spill folders go in: this user's, in the temporary directory, made 0700 and used only
/// while it is ours alone (as the cluster's lock folder is), so no other user can take a lock away or
/// read a lake's files.
fn spill_parent() -> std::io::Result<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt};
        let uid = unsafe { libc::getuid() };
        let dir = std::env::temp_dir().join(format!("pondra-memory-{uid}"));
        let _ = std::fs::DirBuilder::new().mode(0o700).create(&dir);
        let m = std::fs::symlink_metadata(&dir)?;
        if m.is_dir() && m.uid() == uid && m.mode() & 0o077 == 0 {
            return Ok(dir);
        }
        Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, format!("{} is not this user's alone", dir.display())))
    }
    #[cfg(not(unix))]
    {
        let dir = std::env::temp_dir().join("pondra-memory");
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }
}

/// Removes the spill folders whose process has ended: a folder whose lock file this process can take
/// is one nobody holds. Best effort. A folder made a minute ago is left alone: its lock may not be
/// taken yet (another lake in this process may be making it now).
fn sweep(parent: &FsPath) {
    let Ok(entries) = std::fs::read_dir(parent) else { return };
    for entry in entries.flatten() {
        let dir = entry.path();
        let lock_path = dir.join(LOCK_FILE);
        let old = std::fs::metadata(&lock_path).and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).is_some_and(|age| age > Duration::from_secs(60));
        if !old {
            continue;
        }
        let Ok(lock) = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path) else { continue };
        if lock.try_lock().is_ok() {
            drop(lock); // (Windows can't delete a folder with an open file in it)
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

/// A multipart upload into memory or onto disk. When it completes, the object is where its parts went:
/// in memory (its bytes count toward the budget), or on disk (its copy in memory goes).
#[derive(Debug)]
struct Upload {
    up: Box<dyn MultipartUpload>,
    inner: Arc<Inner>,
    at: Path,
    on_disk: bool,
    bytes: u64,
}

#[async_trait::async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.bytes += data.content_length() as u64;
        self.up.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let done = self.up.complete().await?;
        self.inner.landed(&self.at, self.on_disk, self.bytes).await?;
        Ok(done)
    }

    async fn abort(&mut self) -> object_store::Result<()> { self.up.abort().await }
}

#[async_trait::async_trait]
impl ObjectStore for Held {
    async fn put_opts(&self, at: &Path, body: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        if self.0.spills(at, &opts.mode) {
            self.0.put_disk(at, body, opts).await
        } else {
            self.0.put_mem(at, body, opts).await
        }
    }

    async fn put_multipart_opts(&self, at: &Path, opts: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        let on_disk = self.0.spills(at, &PutMode::Overwrite);
        let up = match on_disk {
            true => self.0.disk_or_make()?.store.put_multipart_opts(at, opts).await?,
            false => self.0.mem.put_multipart_opts(at, opts).await?,
        };
        Ok(Box::new(Upload { up, inner: self.0.clone(), at: at.clone(), on_disk, bytes: 0 }))
    }

    async fn get_opts(&self, at: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let got = self.0.mem.get_opts(at, opts.clone()).await;
        if let (Err(object_store::Error::NotFound { .. }), Some(disk)) = (&got, self.0.disk_of(at)) {
            return disk.store.get_opts(at, opts).await;
        }
        got
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
        let mem = self.0.mem.list(prefix);
        match self.0.disk.get() {
            Some(disk) => mem.chain(disk.store.list(prefix).try_filter(|m| ready(!hidden(m)))).boxed(),
            None => mem,
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut listed = self.0.mem.list_with_delimiter(prefix).await?;
        if let Some(disk) = self.0.disk.get() {
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
        if matches!(opts.mode, CopyMode::Create) && (inner.in_mem(to) || inner.on_disk(to).await?) {
            return Err(already(to));
        }
        match inner.size(from) {
            Some(bytes) => {
                inner.mem.copy_opts(from, to, opts).await?;
                inner.landed(to, false, bytes).await
            }
            None => {
                let disk = inner.disk_of(from).ok_or_else(|| missing(from))?;
                disk.store.copy_opts(from, to, opts).await?;
                inner.landed(to, true, 0).await
            }
        }
    }
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
            assert!(!h.0.in_mem(&Path::from(*name)));
        }
        assert_eq!(read(&h, "data/t/b.parquet").await, vec![3; 10]);
        h.put(&Path::from("catalog/d"), bytes(9)).await.unwrap(); // the catalog stays in memory past it
        assert!(h.0.in_mem(&Path::from("catalog/d")));
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
        assert_eq!(h.0.held.load(Relaxed), 0);
    }

    #[tokio::test]
    async fn the_folder_is_there_while_the_lake_is() {
        let path = {
            let h = Held::with_budget(0); // every file spills at once
            h.put(&Path::from("data/t/a"), bytes(1)).await.unwrap();
            let path = h.0.disk.get().unwrap().path.clone();
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
        assert_eq!(h.0.held.load(Relaxed), 0, "counted once it completes");
        up.complete().await.unwrap();
        assert_eq!(h.0.held.load(Relaxed), 20);
        assert_eq!(read(&h, "catalog/m").await, [vec![4; 10], vec![5; 10]].concat());
    }
}
