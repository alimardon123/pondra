//! The catalog's key-value store on a node: SlateDB. The leader's writer and a follower's reader of the bucket, their
//! block caches, and the upkeep (the compactor and the garbage collector) that runs beside the writer once the node
//! serves. The only file that names SlateDB; `kv.rs` says what the catalog needs of any store.
use super::{Error, Kv, Written};
use crate::store::Store;
use bytes::Bytes;
use futures::future::BoxFuture;
use slatedb::config::{CompactorOptions, DbReaderOptions, DurabilityLevel, FlushOptions, FlushType, GarbageCollectorDirectoryOptions, GarbageCollectorOptions, ObjectStoreCacheOptions, ReadOptions, ScanOptions, Settings};
use slatedb::{Db, DbReader, DbReaderMode, ErrorKind, WriteBatch, WriteHandle};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

impl From<slatedb::Error> for Error {
    fn from(e: slatedb::Error) -> Self {
        Error::new(matches!(e.kind(), ErrorKind::Closed(_)), e)
    }
}

/// The leader's SlateDB writer, with its upkeep: `close` stops the upkeep first, then the writer.
pub(crate) struct Writer {
    db: Db,
    upkeep: Upkeep,
    blocks: [Blocks; 2],
    flush_now: bool, // a local lake's writer: each write's WAL flushed at once (`write`)
}

/// A follower's view of the bucket: SlateDB's reader, which follows the manifest and never writes.
pub(crate) struct Reader {
    r: DbReader,
    blocks: [Blocks; 2],
}

impl Writer {
    /// The catalog as its leader opens it: the bucket's `catalog` database for writing, with its block caches and its
    /// compactor and garbage collector, started once the node serves. `local`: a folder, where each write's WAL is
    /// flushed at once.
    pub(crate) async fn open(store: Store, cache_dir: Option<PathBuf>, local: bool) -> Result<Writer, Error> {
        // Poll object storage rarely when idle (that's an idle writer's request bill), but often
        // enough that compaction keeps up with the checkpoints.
        let compactor_options = Some(CompactorOptions { poll_interval: Duration::from_secs(5), ..Default::default() });
        // Every commit is a write-ahead-log object: clear those, and the old manifests and compaction
        // records, every minute once a minute old, not SlateDB's every 10 minutes — thousands would sit
        // in the bucket. (On a bucket that is one more LIST a minute for the compaction records.)
        // (What a reader's checkpoint still needs stays.)
        // On a local disk, where listing costs nothing, the WAL goes every 5 s and the compactor's
        // replaced files a minute on, not five: a minute of WAL is a file per commit (100 MB at 400
        // commits a second), and a disk they filled had no room to flush the catalog, so nothing
        // could ever clear it. (A follower reads the files of a manifest it polled 250 ms ago.)
        let every = |interval: u64, min_age: u64| Some(GarbageCollectorDirectoryOptions { interval: Some(Duration::from_secs(interval)), min_age: Duration::from_secs(min_age), dry_run: false });
        let gc = crate::env::parse("PONDRA_GC_SECS");
        let (wal, secs) = (gc.unwrap_or(if local { 5 } else { 60 }), gc.unwrap_or(60));
        let compacted = if local { every(10, 60) } else { GarbageCollectorOptions::default().compacted_options };
        let garbage_collector_options = Some(GarbageCollectorOptions { wal_options: every(wal, wal), manifest_options: every(secs, secs), compactions_options: every(secs, secs), compacted_options: compacted, ..Default::default() });
        // (the compactor and the garbage collector start once the node serves: their first reads
        // were most of a cold start's, C5; `upkeep`)
        let settings = Settings { garbage_collector_options: None, flush_interval: Some(Duration::from_millis(1)), manifest_poll_interval: Duration::from_secs(10), l0_sst_size_bytes: 16 << 20, l0_max_ssts: 64, l0_max_ssts_per_key: 32, max_unflushed_bytes: 64 << 20, compactor_options: None, object_store_cache_options: object_cache(cache_dir), ..Default::default() };
        // (a flush's blocks stay out of the cache: everything committed is in the overlay already)
        let flushed = slatedb::BlockCachePolicy::default().with_flush_targets(&[slatedb::CacheTarget::Index, slatedb::CacheTarget::Filters]);
        let (cache, blocks) = caches();
        let db = Db::builder("catalog", store.clone()).with_settings(settings).with_db_cache(cache).with_block_cache_policy(flushed).build().await?;
        let upkeep = upkeep(store, compactor_options.unwrap_or_default(), garbage_collector_options.unwrap_or_default(), local);
        Ok(Writer { db, upkeep, blocks, flush_now: local })
    }
}

impl Reader {
    /// The catalog as a follower or a reader opens it: the bucket's `catalog` database, followed as it changes every
    /// `poll`. `streamed`: the leader's commit stream lays its changes over this view, so the view skips the WAL.
    pub(crate) async fn open(store: Store, streamed: bool, cache_dir: Option<PathBuf>, poll: Duration) -> Result<Reader, Error> {
        let opts = DbReaderOptions { manifest_poll_interval: poll, skip_wal_replay: streamed, object_store_cache_options: object_cache(cache_dir), ..Default::default() };
        // FollowLatest writes nothing, so readers work with read-only bucket credentials.
        let (cache, blocks) = caches();
        let r = DbReader::builder("catalog", store).with_reader_mode(DbReaderMode::FollowLatest).with_options(opts).with_db_cache(cache).build().await?;
        Ok(Reader { r, blocks })
    }
}

// The writer's reads and writes are the leader's: `remote` picks what the bucket holds over the memtable too.
#[async_trait::async_trait]
impl Kv for Writer {
    async fn get(&self, key: &str, remote: bool) -> Result<Option<Bytes>, Error> {
        Ok(match remote {
            true => self.db.get_with_options(key, &ReadOptions::new().with_durability_filter(DurabilityLevel::Remote)).await?,
            false => self.db.get(key).await?,
        })
    }

    async fn scan(&self, range: std::ops::Range<Vec<u8>>, remote: bool) -> Result<BTreeMap<String, Bytes>, Error> {
        let it = match remote {
            true => self.db.scan_with_options(range, &ScanOptions::new().with_durability_filter(DurabilityLevel::Remote)).await?,
            false => self.db.scan(range).await?,
        };
        collect(it).await
    }

    fn is_writer(&self) -> bool {
        true
    }

    async fn write(&self, puts: &[(String, Bytes)], deletes: &[String]) -> Result<Arc<dyn Written>, Error> {
        // (the values are shared, not copied: `put_bytes`; a key is copied, as `put` copies it)
        let mut batch = WriteBatch::new();
        for (k, v) in puts {
            batch.put_bytes(Bytes::copy_from_slice(k.as_bytes()), v.clone());
        }
        for k in deletes {
            batch.delete(k);
        }
        let handle: Arc<dyn Written> = Arc::new(self.db.write(batch).await?);
        if self.flush_now {
            // (its WAL written now, not at the next tick of `flush_interval`: a commit waited about a
            // millisecond for it. On a disk a write is a file whichever way; a bucket keeps the tick,
            // which lets writes made together share one PUT.)
            let db = self.db.clone();
            crate::panics::task(async move { let _ = db.flush_with_options(FlushOptions { flush_type: FlushType::Wal }).await; }); // (a node stopped in a process ends it: ADR-061)
        }
        Ok(handle)
    }

    async fn checkpoint(&self) -> Result<(), Error> {
        Ok(self.db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?)
    }

    async fn close(&self) -> Result<(), Error> {
        self.upkeep.stop().await;
        Ok(self.db.close().await?)
    }

    fn cached(&self) -> usize {
        self.blocks.iter().map(Blocks::weighted_size).sum()
    }
}

// A follower's reads are its view of the bucket, plain: everything in it is in the bucket already, so `remote` changes nothing.
#[async_trait::async_trait]
impl Kv for Reader {
    async fn get(&self, key: &str, _remote: bool) -> Result<Option<Bytes>, Error> {
        Ok(self.r.get(key).await?)
    }

    async fn scan(&self, range: std::ops::Range<Vec<u8>>, _remote: bool) -> Result<BTreeMap<String, Bytes>, Error> {
        collect(self.r.scan(range).await?).await
    }

    fn is_writer(&self) -> bool {
        false
    }

    async fn write(&self, _puts: &[(String, Bytes)], _deletes: &[String]) -> Result<Arc<dyn Written>, Error> {
        Err(Error::new(false, "read-only node"))
    }

    async fn checkpoint(&self) -> Result<(), Error> {
        Err(Error::new(false, "read-only node"))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(self.r.close().await?)
    }

    fn cached(&self) -> usize {
        self.blocks.iter().map(Blocks::weighted_size).sum()
    }
}

impl Written for WriteHandle {
    fn durable(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move { self.await_durable().await.map_err(Error::from) })
    }
}

/// All entries of a scan. The opening of the scan is SlateDB's error as it is (a closed store says so); an error
/// while iterating is never taken for a fence: it stays an error, as it was before the store was behind a trait.
async fn collect(mut it: slatedb::DbIterator) -> Result<BTreeMap<String, Bytes>, Error> {
    let mut all = BTreeMap::new();
    while let Some(kv) = it.next().await.map_err(|e| Error::new(false, e))? {
        all.insert(String::from_utf8(kv.key.to_vec()).map_err(|e| Error::new(false, e))?, kv.value);
    }
    Ok(all)
}

/// The catalog's own object cache: on the SSD tier beside the lake's objects when there is one (`cache_dir`), and none
/// without it (SlateDB caches nothing with no root folder).
fn object_cache(cache_dir: Option<PathBuf>) -> ObjectStoreCacheOptions {
    ObjectStoreCacheOptions { root_folder: cache_dir, max_cache_size_bytes: Some(2 << 30), cache_on_flush: true, cache_on_compaction: true, ..Default::default() }
}

/// The catalog's blocks kept in memory: 32 MB of data and 32 MB of indexes and filters. SlateDB's
/// own default is 512 and 128 MB, and the leader put every block it flushed in it, so a node under
/// steady writes grew by the catalog's size until it held 640 MB for a cache it hardly reads:
/// everything committed is in the overlay (`mirror`), and an inline segment's rows are read once
/// and kept decoded (`Lake::tail`).
/// The caches are moka's, as SlateDB's own are, but they say what they hold (`Kv::cached`), so
/// a node's own memory doesn't count them (`tools/soak.py`).
fn caches() -> (Arc<dyn slatedb::db_cache::DbCache>, [Blocks; 2]) {
    let (data, meta) = (Blocks::new(32), Blocks::new(32));
    let cache = slatedb::db_cache::SplitCache::new().with_block_cache(Some(Arc::new(data.clone()))).with_meta_cache(Some(Arc::new(meta.clone()))).build();
    (Arc::new(cache), [data, meta])
}

/// One of the catalog's block caches (`caches`): SlateDB's `MokaCache`, whose weight it can say.
#[derive(Clone)]
pub(crate) struct Blocks(moka::future::Cache<slatedb::db_cache::CachedKey, slatedb::db_cache::CachedEntry>);

impl Blocks {
    fn new(mb: u64) -> Self {
        Blocks(moka::future::Cache::builder().weigher(|_, v: &slatedb::db_cache::CachedEntry| v.size() as u32).max_capacity(mb << 20).build())
    }

    /// The bytes it holds now.
    fn weighted_size(&self) -> usize {
        self.0.weighted_size() as usize
    }
}

#[async_trait::async_trait]
impl slatedb::db_cache::DbCache for Blocks {
    async fn get_block(&self, key: &slatedb::db_cache::CachedKey) -> Result<Option<slatedb::db_cache::CachedEntry>, slatedb::Error> { Ok(self.0.get(key).await) }
    async fn get_index(&self, key: &slatedb::db_cache::CachedKey) -> Result<Option<slatedb::db_cache::CachedEntry>, slatedb::Error> { Ok(self.0.get(key).await) }
    async fn get_filter(&self, key: &slatedb::db_cache::CachedKey) -> Result<Option<slatedb::db_cache::CachedEntry>, slatedb::Error> { Ok(self.0.get(key).await) }
    async fn get_stats(&self, key: &slatedb::db_cache::CachedKey) -> Result<Option<slatedb::db_cache::CachedEntry>, slatedb::Error> { Ok(self.0.get(key).await) }
    async fn insert(&self, key: slatedb::db_cache::CachedKey, value: slatedb::db_cache::CachedEntry) { self.0.insert(key, value).await }
    async fn remove(&self, key: &slatedb::db_cache::CachedKey) { self.0.remove(key).await; }
    fn entry_count(&self) -> u64 { self.0.entry_count() }
}

/// The catalog's compactor and garbage collector, as SlateDB runs them beside a writer, started
/// once the node serves (or after 10 s: a `pondra sql` writer never does), so a cold start waits
/// for neither (C5). A failure in either stops the node, as one in the writer would.
fn upkeep(store: Store, compactor: CompactorOptions, gc: GarbageCollectorOptions, local: bool) -> Upkeep {
    let compactor = slatedb::CompactorBuilder::new("catalog", store.clone()).with_options(compactor).build();
    let gc = slatedb::GarbageCollectorBuilder::new("catalog", store.clone()).with_options(gc).build();
    let (c, g) = (compactor.clone(), gc.clone());
    let started = crate::panics::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(10), crate::store::SERVING.subscribe().wait_for(|s| *s)).await;
        if local {
            crate::panics::task(unpin(store));
        }
        // (`try_join`, not `join`: the collector never ends, so `join` waited for it and never saw the compactor stop.
        // The first failure of either one ends the wait here, and it takes the failure path below)
        if let Err(e) = tokio::try_join!(c.run(), g.run()) {
            // (a compactor fenced by a newer leader's is this node's step down, not a failure: it restarts and rejoins
            // as a follower, as a fenced write does. Stopping there left a stale leader that woke gone: harness `fence`)
            let e = crate::store::fatal(e.into());
            crate::node::stop(format!("the catalog's compactor or garbage collector failed: {e}"));
        }
    });
    // (SlateDB doesn't export the collector's type, so how the two stop is kept as a future to run)
    let stop = Box::pin(async move {
        let _ = tokio::join!(compactor.stop(), gc.stop());
    });
    Upkeep(Mutex::new(Some((started, stop))))
}

/// The upkeep, kept so a close can stop it. SlateDB runs the compactor and the collector
/// in tasks of their own, which outlive the future that started them: stopped only with that
/// future, they went on polling a closed lake (or one opened again) for the life of the process.
struct Upkeep(Mutex<Option<(tokio::task::JoinHandle<()>, BoxFuture<'static, ()>)>>);

impl Upkeep {
    /// Stops it, once: the task that starts the two first (so neither starts after this), then both.
    async fn stop(&self) {
        let taken = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some((started, stop)) = taken {
            started.abort();
            let _ = started.await;
            stop.await;
        }
    }
}

/// SlateDB's compactor pins the files each compaction replaces with a checkpoint for 15 minutes
/// (for a scan still reading them), whatever the collector is told: under a stream of small
/// commits that is a GB of a local disk, and a small disk it filled stayed full that long. On a
/// local disk every node polls its view every 250 ms and a scan takes far less than a minute, so
/// such a checkpoint (unnamed, 15 minutes) goes a minute on; the collector then takes its files.
async fn unpin(store: Store) {
    let admin = slatedb::admin::Admin::builder("catalog", store).build();
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let Ok(checkpoints) = admin.list_checkpoints(None).await else { continue };
        let now = chrono::Utc::now();
        for c in checkpoints {
            let compactor = c.name.is_none() && c.expire_time.is_some_and(|e| e - c.create_time >= chrono::Duration::minutes(15));
            if compactor && now - c.create_time > chrono::Duration::minutes(1) {
                let _ = admin.delete_checkpoint(c.id).await;
            }
        }
    }
}
