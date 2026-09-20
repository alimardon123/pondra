//! DataFusion reads the lake through this adapter: it serves DataFusion's object-store interface
//! (object_store 0.13) from our single S3 client (0.14), so the binary carries one HTTP/TLS
//! stack, and it caches what it reads. Lake objects are never overwritten (every write creates
//! a new object), so cached byte ranges and sizes can never go stale: no invalidation needed.
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store_df::path::Path;
use object_store_df::{
    CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use std::ops::Range;
use std::sync::{Arc, Mutex};

type Entry = (Bytes, ObjectMeta, Range<u64>);

#[derive(Debug)]
pub struct CachedStore {
    inner: Arc<dyn object_store::ObjectStore>,
    cache: Mutex<(lru::LruCache<String, Entry>, usize)>, // entries, total bytes
    max_bytes: usize,
}

impl CachedStore {
    pub fn new(inner: Arc<dyn object_store::ObjectStore>, max_bytes: usize) -> Self {
        Self { inner, cache: Mutex::new((lru::LruCache::unbounded(), 0)), max_bytes }
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
