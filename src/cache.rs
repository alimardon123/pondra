//! Two caches in front of the bucket. Lake objects are never overwritten (every write creates a
//! new object), so nothing cached can go stale: no invalidation, ever.
//!
//! * `Disk`, the local SSD tier: whole objects (Parquet files, log segments) on this node's disk.
//!   Filled three ways — a node keeps what it writes, fetches whole what it reads, and prefetches
//!   what every commit brings in (see `Lake::prefetch`) — so recent data is read from local disk
//!   on every node, not from the bucket. The bucket stays the only durable copy.
//! * `CachedStore`, what DataFusion reads through: byte ranges in memory, then the SSD tier, then
//!   the bucket. It also serves DataFusion's object-store interface (object_store 0.13) from our
//!   single S3 client (0.14), so the binary carries one HTTP/TLS stack.
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store_df::path::Path;
use object_store_df::{
    CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use std::collections::HashSet;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// The local SSD tier: whole lake objects under `dir`, at their lake paths, least recently used
/// evicted past `max` bytes.
pub struct Disk {
    pub dir: PathBuf,
    pub max: u64,
    index: Mutex<(lru::LruCache<String, u64>, u64)>, // lake path -> bytes; total
    store: Arc<dyn object_store::ObjectStore>,       // the lake (for background fetches)
    busy: Mutex<HashSet<String>>,                     // fetches in flight
    slots: Arc<tokio::sync::Semaphore>,
}

impl Disk {
    /// Open (and index) the tier; what an earlier run left there is still good.
    pub fn open(dir: PathBuf, max: u64, store: Arc<dyn object_store::ObjectStore>) -> std::io::Result<Arc<Disk>> {
        std::fs::create_dir_all(&dir)?;
        let mut found = vec![];
        let mut todo = vec![dir.clone()];
        while let Some(d) = todo.pop() {
            for e in std::fs::read_dir(d)?.flatten() {
                let (path, meta) = (e.path(), e.metadata()?);
                match meta.is_dir() {
                    true => todo.push(path),
                    false if path.extension().is_some_and(|x| x == "tmp") => drop(std::fs::remove_file(path)),
                    false => found.push((meta.modified()?, path.strip_prefix(&dir).unwrap().to_string_lossy().replace('\\', "/"), meta.len())),
                }
            }
        }
        found.sort(); // oldest first, so they are evicted first
        let index = Mutex::new((lru::LruCache::unbounded(), 0));
        let disk = Disk { dir, max, index, store, busy: Default::default(), slots: Arc::new(tokio::sync::Semaphore::new(4)) };
        found.into_iter().for_each(|(_, key, len)| disk.index_put(key, len));
        Ok(Arc::new(disk))
    }

    /// Where `key` is on disk, and its size — if it's here.
    pub fn get(&self, key: &str) -> Option<(PathBuf, u64)> {
        let len = *self.index.lock().unwrap().0.get(key)?;
        Some((self.dir.join(key), len))
    }

    /// Keep an object (written atomically: a reader never sees half a file; the temporary name
    /// is unique, as two writers — or two nodes sharing the folder — may keep the same object).
    pub fn put(&self, key: &str, bytes: &[u8]) {
        let (path, tmp) = (self.dir.join(key), self.dir.join(format!("{key}.{}.tmp", uuid::Uuid::new_v4().simple())));
        let ok = path.parent().map_or(Ok(()), std::fs::create_dir_all).and_then(|_| std::fs::write(&tmp, bytes)).and_then(|_| std::fs::rename(&tmp, &path));
        if ok.is_ok() {
            self.index_put(key.to_string(), bytes.len() as u64);
        }
    }

    fn index_put(&self, key: String, len: u64) {
        let mut ix = self.index.lock().unwrap();
        ix.1 += len;
        if let Some(old) = ix.0.put(key, len) {
            ix.1 -= old;
        }
        while ix.1 > self.max {
            let Some((old, len)) = ix.0.pop_lru() else { break };
            ix.1 -= len;
            std::fs::remove_file(self.dir.join(old)).ok();
        }
    }

    /// Fetch a whole object into the tier in the background (at most 4 at a time), unless it is
    /// already here or on its way.
    pub fn fetch_later(self: &Arc<Self>, key: String) {
        if self.get(&key).is_some() || !self.busy.lock().unwrap().insert(key.clone()) {
            return;
        }
        let disk = self.clone();
        tokio::spawn(async move {
            let _slot = disk.slots.clone().acquire_owned().await;
            let path = object_store::path::Path::from(key.as_str());
            if let Ok(r) = disk.store.get_opts(&path, Default::default()).await {
                if let Ok(bytes) = r.bytes().await {
                    disk.put(&key, &bytes);
                }
            }
            disk.busy.lock().unwrap().remove(&key);
        });
    }

    /// A byte range of a cached object.
    pub async fn read(&self, path: PathBuf, range: Range<u64>) -> std::io::Result<Bytes> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut f = tokio::fs::File::open(path).await?;
        f.seek(std::io::SeekFrom::Start(range.start)).await?;
        let mut buf = vec![0; (range.end - range.start) as usize];
        f.read_exact(&mut buf).await?;
        Ok(buf.into())
    }
}

type Entry = (Bytes, ObjectMeta, Range<u64>);

#[derive(Debug)]
pub struct CachedStore {
    inner: Arc<dyn object_store::ObjectStore>,
    cache: Mutex<(lru::LruCache<String, Entry>, usize)>, // entries, total bytes
    max_bytes: usize,
    disk: Option<(Arc<Disk>, String)>, // the SSD tier, and the lake's prefix in the bucket
}

impl std::fmt::Debug for Disk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Disk({:?})", self.dir) }
}

impl CachedStore {
    pub fn new(inner: Arc<dyn object_store::ObjectStore>, max_bytes: usize, disk: Option<(Arc<Disk>, String)>) -> Self {
        Self { inner, cache: Mutex::new((lru::LruCache::unbounded(), 0)), max_bytes, disk }
    }

    /// Serve from the SSD tier if the object is there; otherwise have it fetched for next time.
    async fn on_disk(&self, location: &Path, options: &GetOptions) -> Option<Entry> {
        let (disk, prefix) = self.disk.as_ref()?;
        let key = location.as_ref().strip_prefix(prefix.as_str())?.trim_start_matches('/').to_string();
        let Some((file, size)) = disk.get(&key) else {
            disk.fetch_later(key);
            return None;
        };
        let range = match options.range.clone() {
            Some(GetRange::Bounded(r)) => r.start..r.end.min(size),
            Some(GetRange::Offset(o)) => o..size,
            Some(GetRange::Suffix(n)) => size.saturating_sub(n)..size,
            None => 0..size,
        };
        let bytes = if options.head { Bytes::new() } else { disk.read(file, range.clone()).await.ok()? };
        let meta = ObjectMeta { location: location.clone(), last_modified: chrono::DateTime::UNIX_EPOCH, size, e_tag: None, version: None };
        Some((bytes, meta, range))
    }

    fn keep(&self, key: String, entry: Entry) {
        let mut c = self.cache.lock().unwrap();
        c.1 += entry.0.len();
        if let Some((old, ..)) = c.0.put(key, entry) {
            c.1 -= old.len();
        }
        while c.1 > self.max_bytes {
            let Some((_, (old, ..))) = c.0.pop_lru() else { break };
            c.1 -= old.len();
        }
    }

    async fn fetch(&self, location: &Path, range: Option<GetRange>, head: bool) -> Result<Entry> {
        let range = range.map(|r| match r {
            GetRange::Bounded(r) => object_store::GetRange::Bounded(r),
            GetRange::Offset(o) => object_store::GetRange::Offset(o),
            GetRange::Suffix(n) => object_store::GetRange::Suffix(n),
        });
        let opts = object_store::GetOptions { range, head, ..Default::default() };
        let res = self.inner.get_opts(&object_store::path::Path::from(location.as_ref()), opts).await.map_err(generic)?;
        let m = &res.meta;
        let meta = ObjectMeta { location: location.clone(), last_modified: m.last_modified, size: m.size, e_tag: m.e_tag.clone(), version: m.version.clone() };
        let range = res.range.clone();
        let bytes = if head { Bytes::new() } else { res.bytes().await.map_err(generic)? };
        Ok((bytes, meta, range))
    }
}

fn generic(e: object_store::Error) -> Error { Error::Generic { store: "lake", source: Box::new(e) } }

fn unsupported<T>() -> Result<T> { Err(Error::NotImplemented { operation: "write".into(), implementer: "pondra read cache".into() }) }

impl std::fmt::Display for CachedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "CachedStore") }
}

#[async_trait]
impl ObjectStore for CachedStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let key = format!("{location}|{:?}|{}", options.range, options.head);
        let hit = self.cache.lock().unwrap().0.get(&key).cloned();
        let hit = match hit {
            Some(entry) => Some(entry),
            None => self.on_disk(location, &options).await,
        };
        let (bytes, meta, range) = match hit {
            Some(entry) => entry,
            None => {
                let entry = self.fetch(location, options.range, options.head).await?;
                self.keep(key, entry.clone());
                entry
            }
        };
        let payload = GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed());
        Ok(GetResult { payload, meta, range, attributes: Default::default() })
    }

    // DataFusion only reads the lake; Pondra writes through its own client.
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> { unsupported() }
    async fn put_multipart_opts(&self, _: &Path, _: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> { unsupported() }
    fn delete_stream(&self, _: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> { stream::once(async { unsupported() }).boxed() }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> { stream::once(async { unsupported() }).boxed() }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> { unsupported() }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> { unsupported() }
}
