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
//! * Files outside the lakes (ADR-026) can change under their names, so their byte ranges are
//!   kept only as a version (`Outside`): the e-tag and size the statement's own listing saw.
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
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
        crate::panics::spawn(async move {
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

/// Byte ranges of objects outside the lakes, in memory, shared by every store
/// (`PONDRA_FILES_CACHE_MB`, 512): each kept under the version its statement listed — the e-tag
/// (or the time) and size a listing or HEAD gave just before — and fetched as that version
/// (`if_match`), so a newer version's bytes are never kept under an older one's key. The next
/// statement lists again, and a file changed since is a new key.
struct Outside {
    seen: lru::LruCache<String, (String, Option<String>)>, // store|location -> (version, e-tag)
    ranges: lru::LruCache<String, Entry>,                    // store|location|version|range -> bytes
    bytes: usize,
}

static OUTSIDE: std::sync::LazyLock<Mutex<Outside>> = std::sync::LazyLock::new(|| {
    let seen = lru::LruCache::new(std::num::NonZeroUsize::new(100_000).expect("nonzero"));
    Mutex::new(Outside { seen, ranges: lru::LruCache::unbounded(), bytes: 0 })
});

fn files_cache_bytes() -> usize { crate::env::or("PONDRA_FILES_CACHE_MB", 512usize) << 20 }

/// Keeps a range (or a block) of a file outside the lakes, the least recently used going past `files_cache_bytes`.
fn keep_outside(key: String, entry: Entry) {
    let mut o = OUTSIDE.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    o.bytes += entry.0.len();
    if let Some((old, ..)) = o.ranges.put(key, entry) {
        o.bytes -= old.len();
    }
    while o.bytes > files_cache_bytes() {
        let Some((_, (old, ..))) = o.ranges.pop_lru() else { break };
        o.bytes -= old.len();
    }
}

#[derive(Debug)]
pub struct CachedStore {
    inner: Arc<dyn object_store::ObjectStore>,
    cache: Mutex<(lru::LruCache<String, Entry>, usize)>, // entries, total bytes
    max_bytes: usize,
    disk: Option<(Arc<Disk>, String)>, // the SSD tier, and the lake's prefix in the bucket
    lakes: std::sync::RwLock<Vec<String>>, // the prefix of each lake read through it (`a/b/`, or `` at the bucket's root)
    id: String, // (this store, in the keys of `OUTSIDE`)
    blocks: Arc<tokio::sync::Semaphore>, // the blocks of files outside the lakes in flight from it at once (`in_blocks`)
}

/// A file outside the lakes is read in blocks of this size (`in_blocks`): one connection to a bucket carries some tens
/// of MB a second, so a few MB of column chunks fetched as one range waited on one connection. (TPC-H's first runs over
/// Parquet files on R2 took 1.65x DuckDB's, which fetches 2 MB pieces, up to 16 at once.)
const BLOCK: u64 = 2 << 20;
/// A range's blocks in flight at once, and a store's.
const BLOCKS_A_RANGE: usize = 16;
const BLOCKS_A_STORE: usize = 64;

impl std::fmt::Debug for Disk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Disk({:?})", self.dir) }
}

impl CachedStore {
    pub fn new(inner: Arc<dyn object_store::ObjectStore>, max_bytes: usize, disk: Option<(Arc<Disk>, String)>, prefix: &str) -> Self {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let this = Self { inner, cache: Mutex::new((lru::LruCache::unbounded(), 0)), max_bytes, disk, lakes: Default::default(), id, blocks: Arc::new(tokio::sync::Semaphore::new(BLOCKS_A_STORE)) };
        this.add_lake(prefix);
        this
    }

    /// A store of files outside any lake (a bucket `CREATE SECRET` covers, a web server).
    pub fn files(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { inner, cache: Mutex::new((lru::LruCache::unbounded(), 0)), max_bytes: 0, disk: None, lakes: Default::default(), id: uuid::Uuid::new_v4().simple().to_string(), blocks: Arc::new(tokio::sync::Semaphore::new(BLOCKS_A_STORE)) }
    }

    /// The bytes of objects' ranges held in memory now (at most `max_bytes`).
    pub fn held(&self) -> usize { self.cache.lock().unwrap().1 }

    fn saw(&self, m: &ObjectMeta) { saw(&self.id, &self.lakes.read().unwrap(), m) }

    /// An object outside the lakes: a byte range of the version last listed, from memory or
    /// fetched as that version; anything else (a whole object, a HEAD, one never listed) as it is.
    async fn outside_get(&self, location: &Path, mut options: GetOptions) -> Result<GetResult> {
        let bridge = crate::bridge::Bridge(self.inner.clone());
        let at = format!("{}|{location}", self.id);
        let seen = OUTSIDE.lock().unwrap_or_else(std::sync::PoisonError::into_inner).seen.get(&at).cloned();
        let (Some((listed, e_tag)), Some(range), false) = (seen, options.range.clone(), options.head) else {
            let r = bridge.get_opts(location, options).await?;
            self.saw(&r.meta);
            return Ok(r);
        };
        let key = format!("{at}|{listed}|{range:?}");
        if let Some(entry) = OUTSIDE.lock().unwrap_or_else(std::sync::PoisonError::into_inner).ranges.get(&key).cloned() {
            return Ok(result(entry));
        }
        options.if_match = e_tag; // (changed since it was listed: an error, not another version's bytes)
        if let GetRange::Bounded(r) = &range {
            if let Some(entry) = self.in_blocks(&bridge, location, &options, &at, &listed, r.clone()).await? {
                return Ok(result(entry));
            }
        }
        let r = bridge.get_opts(location, options).await?;
        let (meta, range) = (r.meta.clone(), r.range.clone());
        let entry = (r.bytes().await?, meta, range);
        if version(&entry.1) == listed {
            // (a store without e-tags whose file changed meanwhile: read, not kept)
            keep_outside(key, entry.clone());
        }
        Ok(result(entry))
    }

    /// Range `r` of the version `listed` of a file outside the lakes, out of its blocks: the `BLOCK`s of the file, at
    /// multiples of `BLOCK`, each kept on its own (`Outside`), so a range a query reads finds the blocks an earlier
    /// query's ranges brought in, though DataFusion merged those otherwise (a query touching other columns of a file
    /// fetched again all it read). The blocks not here are fetched side by side (`BLOCKS_A_RANGE` at once, and
    /// `BLOCKS_A_STORE` from this store). None for a range under a quarter of a block that its blocks don't all hold:
    /// fetched as it is (a footer's few KB aren't worth a block).
    async fn in_blocks(&self, bridge: &crate::bridge::Bridge, location: &Path, options: &GetOptions, at: &str, listed: &str, r: Range<u64>) -> Result<Option<Entry>> {
        let Some(size) = listed.rsplit_once(':').and_then(|(_, n)| n.parse::<u64>().ok()) else { return Ok(None) }; // (`version`)
        if r.start >= r.end || r.end > size {
            return Ok(None);
        }
        let (first, last) = (r.start / BLOCK, (r.end - 1) / BLOCK);
        let key = |i: u64| format!("{at}|{listed}|#{i}");
        let mut have: Vec<Option<Entry>> = {
            let mut o = OUTSIDE.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (first..=last).map(|i| o.ranges.get(&key(i)).cloned()).collect()
        };
        let missing: Vec<u64> = (first..=last).zip(&have).filter(|(_, h)| h.is_none()).map(|(i, _)| i).collect();
        if !missing.is_empty() && r.end - r.start < BLOCK / 4 {
            return Ok(None);
        }
        let fetched: Vec<Entry> = stream::iter(missing.iter().copied())
            .map(|i| {
                let mut o = options.clone();
                o.range = Some(GetRange::Bounded(i * BLOCK..((i + 1) * BLOCK).min(size)));
                async move {
                    let _turn = self.blocks.acquire().await.map_err(|e| Error::Generic { store: "files", source: Box::new(e) })?;
                    let got = bridge.get_opts(location, o).await?;
                    let (meta, range) = (got.meta.clone(), got.range.clone());
                    Ok::<_, Error>((got.bytes().await?, meta, range))
                }
            })
            .buffered(BLOCKS_A_RANGE)
            .try_collect()
            .await?;
        for (i, e) in missing.iter().zip(&fetched) {
            if version(&e.1) == listed {
                keep_outside(key(*i), e.clone());
            }
        }
        let mut fetched = fetched.into_iter();
        have.iter_mut().filter(|h| h.is_none()).for_each(|h| *h = fetched.next());
        let blocks: Vec<Entry> = have.into_iter().flatten().collect();
        let meta = blocks[0].1.clone();
        if blocks.iter().any(|b| version(&b.1) != version(&meta)) {
            return Err(Error::Generic { store: "files", source: format!("{location} changed while it was read").into() });
        }
        let base = first * BLOCK;
        let bytes = match &blocks[..] {
            [one] => one.0.slice((r.start - base) as usize..((r.end - base) as usize).min(one.0.len())),
            _ => {
                let mut all = bytes::BytesMut::with_capacity((r.end - r.start) as usize);
                for (k, b) in blocks.iter().enumerate() {
                    let at = base + k as u64 * BLOCK;
                    let (from, to) = (r.start.max(at) - at, (r.end.min(at + b.0.len() as u64)).saturating_sub(at));
                    all.extend_from_slice(&b.0[from as usize..to.max(from) as usize]);
                }
                all.freeze()
            }
        };
        Ok(Some((bytes, meta, r)))
    }

    /// Another lake in this bucket (attached): its files are cached too.
    pub fn add_lake(&self, prefix: &str) {
        let p = prefix.trim_matches('/');
        self.lakes.write().unwrap().push(if p.is_empty() { String::new() } else { format!("{p}/") });
    }

    /// Is this object in a lake (`sub`: in that part of it)? A lake's files (`data/`) are written
    /// once and never change: cached as they are. Other objects in the bucket (files outside the
    /// lake, ADR-026) may change under their names: kept only by version (`Outside`), and written
    /// as COPY … TO asks.
    fn in_lake(&self, location: &Path, sub: &str) -> bool { inside(&self.lakes.read().unwrap(), location, sub) }

    /// Not a lake's object: DataFusion may write it (COPY … TO beside a lake, never into one).
    fn outside(&self, location: &Path) -> Result<crate::bridge::Bridge> {
        match self.in_lake(location, "") {
            true => Err(Error::PermissionDenied { path: location.to_string(), source: "a lake's objects are written by Pondra alone".into() }),
            false => Ok(crate::bridge::Bridge(self.inner.clone())),
        }
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
        let res = self.inner.get_opts(&object_store::path::Path::parse(location.as_ref()).map_err(|e| generic(e.into()))?, opts).await.map_err(generic)?;
        let m = &res.meta;
        let meta = ObjectMeta { location: location.clone(), last_modified: m.last_modified, size: m.size, e_tag: m.e_tag.clone(), version: m.version.clone() };
        let range = res.range.clone();
        let bytes = if head { Bytes::new() } else { res.bytes().await.map_err(generic)? };
        Ok((bytes, meta, range))
    }
}

fn result((bytes, meta, range): Entry) -> GetResult {
    let payload = GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed());
    GetResult { payload, meta, range, attributes: Default::default() }
}

fn inside(lakes: &[String], location: &Path, sub: &str) -> bool { lakes.iter().any(|p| location.as_ref().starts_with(&format!("{p}{sub}"))) }

/// An object's version: its e-tag and size, or (a store without e-tags) its time, to the second as
/// every store's headers give it, and size.
fn version(m: &ObjectMeta) -> String {
    match &m.e_tag {
        Some(e) => format!("{e}:{}", m.size),
        None => format!("@{}:{}", m.last_modified.timestamp(), m.size),
    }
}

/// What a listing or HEAD says an object outside the lakes is now (`Outside`).
fn saw(id: &str, lakes: &[String], m: &ObjectMeta) {
    if !inside(lakes, &m.location, "") {
        OUTSIDE.lock().unwrap_or_else(std::sync::PoisonError::into_inner).seen.put(format!("{id}|{}", m.location), (version(m), m.e_tag.clone()));
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
        if !self.in_lake(location, "") {
            return self.outside_get(location, options).await;
        }
        if !self.in_lake(location, "data/") {
            return crate::bridge::Bridge(self.inner.clone()).get_opts(location, options).await; // (the lake's other objects: its own client's)
        }
        let key = format!("{location}|{:?}|{}", options.range, options.head);
        let hit = self.cache.lock().unwrap().0.get(&key).cloned();
        let hit = match hit {
            Some(entry) => Some(entry),
            None => self.on_disk(location, &options).await,
        };
        let entry = match hit {
            Some(entry) => entry,
            None => {
                let entry = self.fetch(location, options.range, options.head).await?;
                self.keep(key, entry.clone());
                entry
            }
        };
        Ok(result(entry))
    }

    // DataFusion only reads the lake; Pondra writes through its own client. Beside it: COPY … TO.
    async fn put_opts(&self, at: &Path, data: PutPayload, o: PutOptions) -> Result<PutResult> { self.outside(at)?.put_opts(at, data, o).await }
    async fn put_multipart_opts(&self, at: &Path, o: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> { self.outside(at)?.put_multipart_opts(at, o).await }
    fn delete_stream(&self, at: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let (inner, lakes) = (self.inner.clone(), self.lakes.read().unwrap().clone());
        at.then(move |p| {
            let (inner, lakes) = (inner.clone(), lakes.clone());
            async move {
                let p = p?;
                if lakes.iter().any(|l| p.as_ref().starts_with(l.as_str())) {
                    return Err(Error::PermissionDenied { path: p.to_string(), source: "a lake's objects are deleted by Pondra alone".into() });
                }
                let one = stream::once(std::future::ready(Ok(p.clone()))).boxed();
                crate::bridge::Bridge(inner).delete_stream(one).next().await.unwrap_or(Ok(p))
            }
        }).boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let (id, lakes) = (self.id.clone(), self.lakes.read().unwrap().clone());
        crate::bridge::Bridge(self.inner.clone()).list(prefix).inspect(move |m| m.iter().for_each(|m| saw(&id, &lakes, m))).boxed() // (files outside the lake)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let r = crate::bridge::Bridge(self.inner.clone()).list_with_delimiter(prefix).await?;
        r.objects.iter().for_each(|m| self.saw(m));
        Ok(r)
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> { unsupported() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file outside the lakes is read in blocks: ranges across blocks, within one, at the file's end, and one too small
    /// for a block, each its bytes; and once the file is gone from the bucket, ranges its blocks hold are still read, from
    /// them, though no read asked for those ranges before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_file_outside_is_read_in_blocks_that_later_ranges_find() -> Result<()> {
        use object_store::ObjectStoreExt as _;
        use object_store_df::ObjectStoreExt as _;
        let inner = Arc::new(object_store::memory::InMemory::new());
        let size = 5 * BLOCK + 12_345;
        let data: Vec<u8> = (0..size).map(|i| (i * 31 % 251) as u8).collect();
        let at = object_store::path::Path::from("files/a.parquet");
        inner.put(&at, data.clone().into()).await.unwrap();
        let store = CachedStore::files(inner.clone());
        let listed: Vec<_> = store.list(None).collect().await; // (a statement's listing: the version its blocks are kept under)
        assert_eq!(listed.len(), 1);
        let path = Path::from("files/a.parquet");
        let want = |r: &Range<u64>| Bytes::copy_from_slice(&data[r.start as usize..r.end as usize]);
        for r in [BLOCK - 10..3 * BLOCK + 7, 0..BLOCK, 4 * BLOCK..size, 2 * BLOCK + 1..2 * BLOCK + 600_000, size - 8..size] {
            assert_eq!(store.get_range(&path, r.clone()).await?, want(&r), "{r:?}");
        }
        inner.delete(&at).await.unwrap();
        for r in [BLOCK..2 * BLOCK + 5, 3 * BLOCK + 100..3 * BLOCK + 200, 4 * BLOCK + 3..size, 0..size] {
            assert_eq!(store.get_range(&path, r.clone()).await?, want(&r), "{r:?}, from its blocks");
        }
        // A small range whose block isn't held is fetched as it is, and kept so, not as a block.
        let b = object_store::path::Path::from("files/b.parquet");
        inner.put(&b, data.clone().into()).await.unwrap();
        let _: Vec<_> = store.list(None).collect().await;
        let (path, footer) = (Path::from("files/b.parquet"), size - 8..size);
        assert_eq!(store.get_range(&path, footer.clone()).await?, want(&footer));
        inner.delete(&b).await.unwrap();
        assert_eq!(store.get_range(&path, footer.clone()).await?, want(&footer), "kept as it was read");
        assert!(store.get_range(&path, size - 9..size).await.is_err(), "no block of b was fetched");
        Ok(())
    }
}
