//! The lake: object storage (a local dir or s3://bucket/prefix on S3, R2, MinIO…) with the
//! catalog (SlateDB) inside it. Nothing else holds state.
//!
//! The leader is the catalog's only writer. It also streams every commit to the followers the
//! moment it is durable, and they lay those changes over their own (slightly behind) view of the
//! catalog: every node sees a commit within milliseconds, while the bucket stays the source of truth.
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::aws::AmazonS3Builder;
use object_store::{local::LocalFileSystem, path::Path, prefix::PrefixStore, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use slatedb::config::{CompactorOptions, DbReaderOptions, DurabilityLevel, FlushOptions, FlushType, ReadOptions, ScanOptions, Settings};
use slatedb::{Db, DbReader, DbReaderMode, ErrorKind, WriteBatch, WriteHandle};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

pub type Store = Arc<dyn ObjectStore>;

/// A table: columns, the Parquet files already tiered, and the last log segment they cover.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct TableMeta {
    pub columns: Vec<(String, String)>, // (name, Arrow type such as "Int64" or "Utf8")
    #[serde(default)]
    pub key: Vec<String>, // primary key: non-empty = upsert table (latest row per key wins)
    #[serde(default)]
    pub merge: BTreeMap<String, String>, // merge table: rows per key combine (column -> sum|min|max)
    pub files: Vec<DataFile>,
    pub tiered: u64, // every segment <= `tiered` is already inside `files`
    #[serde(default)]
    pub garbage: Vec<(String, u64)>, // replaced files + when; deleted after the retention period
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DataFile {
    pub path: String,
    pub rows: u64,
    #[serde(default)]
    pub bytes: u64,
    /// Keyed tables: the last log segment this file covers. A row in a file with a higher `ord`
    /// is a newer version of its key, so several files can hold the same key (LSM-style) and
    /// tiering doesn't have to rewrite the whole table every time.
    #[serde(default)]
    pub ord: u64,
}

/// One log segment = one node's flush, holding rows for many tables. Small segments are stored
/// inside the catalog write itself (one round trip to object storage per flush); big ones as an
/// object written by the node that received the rows.
#[derive(Serialize, Deserialize, Clone)]
pub struct Segment {
    pub path: String,                                // empty = inline, under data_key(seg)
    pub parts: BTreeMap<String, Vec<(u64, u64, u64)>>, // table -> (offset, length, rows) of Arrow IPC streams
    pub ts_ms: u64,
}

pub fn table_key(t: &str) -> String { format!("t/{t}") }
pub fn seg_key(n: u64) -> String { format!("s/{n:020}") }
pub fn producer_key(p: &str) -> String { format!("p/{p}") }
pub fn data_key(seg: u64) -> String { format!("d/{seg:020}") } // small segments live inside the catalog
pub fn json<T: Serialize>(v: &T) -> Vec<u8> { serde_json::to_vec(v).expect("serializable") }

const TAIL_BYTES: usize = 256 << 20; // decoded log segments kept in memory, per node

const RECENT: Duration = Duration::from_secs(30); // replayed to (re)connecting followers…
const RECENT_BYTES: usize = 64 << 20; // …within this much memory (past it, a reconnecting
// follower reads from its own view until it has caught up, instead of the leader buffering more)

/// Query read cache size in MB (env PONDRA_CACHE_MB, default 1024).
fn cache_mb() -> usize { std::env::var("PONDRA_CACHE_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(1024) }

pub struct Lake {
    pub url: String, // absolute local dir or "s3://bucket/prefix"
    pub store: Store,
    pub cat: Catalog,
    pub hwm: watch::Sender<u64>, // the last committed segment this node knows of
    pub backlog: std::sync::atomic::AtomicU64, // leader: rows in the log not yet tiered (all tables)
    rt: Arc<RuntimeEnv>,         // shared by all queries: object store registry + Parquet metadata cache
    tail: Mutex<(lru::LruCache<(u64, String), Rows>, usize)>, // decoded (segment, table) rows; total bytes
}

pub type Rows = Arc<Vec<datafusion::arrow::record_batch::RecordBatch>>;

/// Open a lake's object store. For s3:// URLs, also returns the bucket-level client that query
/// scans use (DataFusion addresses objects by their full path in the bucket).
pub fn open_store(url: &str) -> Result<(String, Store, Option<(String, Store)>)> {
    let Some(rest) = url.strip_prefix("s3://") else {
        std::fs::create_dir_all(url)?;
        let dir = std::fs::canonicalize(url)?.to_string_lossy().trim_start_matches(r"\\?\").to_string(); // (Windows verbatim prefix)
        return Ok((dir.clone(), Arc::new(LocalFileSystem::new_with_prefix(&dir)?), None));
    };
    // Credentials and endpoint come from AWS_* env vars (AWS_ENDPOINT for R2 / MinIO).
    let (bucket, prefix) = rest.trim_end_matches('/').split_once('/').unwrap_or((rest, ""));
    let s3: Store = Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).with_allow_http(true).build()?);
    let store: Store = if prefix.is_empty() { s3.clone() } else { Arc::new(PrefixStore::new(s3.clone(), prefix)) };
    Ok((url.trim_end_matches('/').to_string(), store, Some((format!("s3://{bucket}"), s3))))
}

impl Lake {
    /// `writer`: the leader. `streamed`: a follower, which also gets every commit streamed from
    /// the leader, so its own catalog view only needs the leader's checkpoints (no log replay).
    pub async fn open(url: &str, writer: bool, streamed: bool) -> Result<Arc<Lake>> {
        let (url, store, bucket) = open_store(url)?;
        let rt = RuntimeEnvBuilder::new().build_arc()?;
        if let Some((bucket_url, s3)) = bucket {
            let cached = crate::cache::CachedStore::new(s3, cache_mb() << 20);
            rt.register_object_store(&url::Url::parse(&bucket_url)?, Arc::new(cached));
        }
        let cat = if writer { Catalog::writer(store.clone()).await? } else { Catalog::reader(store.clone(), streamed).await? };
        let hwm = watch::Sender::new(cat.get::<u64>("n").await?.unwrap_or(1) - 1);
        Ok(Arc::new(Lake { url, store, cat, hwm, backlog: Default::default(), rt, tail: Mutex::new((lru::LruCache::unbounded(), 0)) }))
    }

    /// Follower: lay a commit streamed from the leader over our view of the catalog. `gap`: some
    /// commits before it never arrived (see `Catalog::apply`).
    pub fn apply(&self, d: &Delta, gap: bool) {
        self.cat.apply(d, gap);
        self.advance(self.cat.visible_n() - 1);
    }

    /// Non-leaders: catch up with our own catalog view (and prune what it now holds).
    pub async fn refresh(&self) -> Result<()> {
        self.cat.refresh().await?;
        self.advance(self.cat.visible_n() - 1);
        Ok(())
    }

    /// The highest segment this node's reads include right now: on the leader everything that is
    /// durable, on a follower what its own view plus the streamed commits hold. (`hwm` only wakes
    /// readers up; it can be ahead of what a follower can actually read.)
    pub fn visible(&self) -> u64 {
        match self.cat.is_writer() {
            true => *self.hwm.borrow(),
            false => self.cat.visible_n().saturating_sub(1),
        }
    }

    /// Note that segments up to `hwm` are committed (wakes tasks and watchers).
    pub fn advance(&self, hwm: u64) {
        self.hwm.send_if_modified(|h| std::mem::replace(h, (*h).max(hwm)) < hwm);
    }

    /// A fresh SQL session on the shared runtime.
    pub fn session(&self) -> SessionContext {
        // At least 2 partitions, so every node plans aggregations as partial + final (see spmd.rs).
        let partitions = std::thread::available_parallelism().map_or(2, |n| n.get()).max(2);
        self.session_with(partitions)
    }

    /// A session with a fixed number of partitions: 1 for point lookups, where splitting the work
    /// costs more than it saves and many queries run at once.
    pub fn session_with(&self, partitions: usize) -> SessionContext {
        SessionContext::new_with_config_rt(SessionConfig::new().with_information_schema(true).with_target_partitions(partitions), self.rt.clone())
    }

    /// Full URL of an object, for DataFusion.
    pub fn full(&self, path: &str) -> String { format!("{}/{path}", self.url) }

    /// Write an object only if it does not exist yet: data is never overwritten.
    pub async fn put(&self, path: &str, bytes: Vec<u8>) -> Result<()> {
        let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
        self.store.put_opts(&Path::from(path), Bytes::from(bytes).into(), opts).await?;
        Ok(())
    }

    /// The rows of `table` in log segment `n`, decoded once and then served from memory
    /// (segments never change, so the cache never goes stale).
    pub async fn segment_rows(&self, n: u64, seg: &Segment, table: &str) -> Result<Rows> {
        let key = (n, table.to_string());
        if let Some(rows) = self.tail.lock().unwrap().0.get(&key) {
            return Ok(rows.clone());
        }
        let Some(parts) = seg.parts.get(table) else { return Ok(Rows::default()) };
        let bytes = match seg.path.is_empty() {
            true => self.cat.get_raw(&data_key(n)).await?.with_context(|| format!("missing inline segment {n}"))?,
            false => self.store.get(&Path::from(seg.path.as_str())).await?.bytes().await?,
        };
        let mut rows = vec![];
        for &(off, len, _) in parts {
            rows.extend(crate::log::decode(&bytes[off as usize..(off + len) as usize])?);
        }
        let rows: Rows = Arc::new(rows);
        let size = rows.iter().map(|b| b.get_array_memory_size()).sum::<usize>();
        let mut c = self.tail.lock().unwrap();
        c.1 += size;
        c.0.put(key, rows.clone());
        while c.1 > TAIL_BYTES {
            let Some((_, old)) = c.0.pop_lru() else { break };
            c.1 -= old.iter().map(|b| b.get_array_memory_size()).sum::<usize>();
        }
        Ok(rows)
    }

    pub async fn delete(&self, path: &str) {
        self.store.delete(&Path::from(path)).await.ok();
    }
}

/// The catalog is a SlateDB key-value store inside the lake.
/// One process holds the writer (SlateDB fences out any other); any number can read.
pub struct Catalog {
    db: Db_,
    order: tokio::sync::Mutex<u64>, // leader: next commit id; held while writing, so ids follow write order
    durable: Option<mpsc::UnboundedSender<(WriteHandle, Arc<Delta>, oneshot::Sender<()>)>>, // leader: writes awaiting durability, in order
    feed: broadcast::Sender<Arc<Delta>>,                  // leader: commits, as they become durable
    recent: Recent, // leader: the last RECENT of them (and their bytes), for (re)connecting followers
    last_n: AtomicU64, // the lake's "n" (next segment) after the latest commit written / streamed to us
    // Every commit also writes "c", its number. Follower: the leader's streamed changes (None =
    // deleted), each with its commit number, are laid over our own view of the catalog, which is
    // a few seconds behind. A read uses them only if our view isn't ahead of the stream (else
    // they could be older than what the view has) and, after a gap, once the view passed `hold`.
    overlay: Mutex<BTreeMap<String, (u64, Option<Bytes>)>>,
    view: (AtomicU64, AtomicU64), // our own view's "c" and the "n" it had at or before that "c"
    pruned: AtomicU64,            // streamed changes up to this commit were dropped (the view has them)
    pins: Mutex<BTreeMap<u64, usize>>, // scans in progress, by the view "c" they started from
    streamed: AtomicU64,          // the last commit streamed to us; with the view: the whole lake
    hold: AtomicU64,              // commits before this one never arrived: read from the view alone
}

/// A scan in progress: the streamed changes it may still lay over its (older) view stay.
struct Pin<'a>(&'a Catalog, u64);

impl Drop for Pin<'_> {
    fn drop(&mut self) {
        let mut pins = self.0.pins.lock().unwrap();
        if let Some(n) = pins.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                pins.remove(&self.1);
            }
        }
    }
}

type Recent = Arc<Mutex<(VecDeque<(Instant, Arc<Delta>)>, usize)>>;

enum Db_ {
    Writer(Db),
    Reader(DbReader),
}

/// One committed catalog write.
pub struct Delta {
    pub id: u64, // the commit number, "c"
    pub puts: Vec<(String, Bytes)>,
    pub deletes: Vec<String>,
}

impl Delta {
    fn bytes(&self) -> usize { self.puts.iter().map(|(k, v)| k.len() + v.len()).sum() }

    /// Wire format: u32 length | u64 id | puts: u32 count, (key, value)* | deletes: u32 count, key*;
    /// every key and value is a u32 length followed by its bytes.
    pub fn frame(&self) -> Bytes {
        let mut b = vec![0u8; 4];
        let field = |b: &mut Vec<u8>, x: &[u8]| {
            b.extend((x.len() as u32).to_le_bytes());
            b.extend(x);
        };
        b.extend(self.id.to_le_bytes());
        b.extend((self.puts.len() as u32).to_le_bytes());
        for (k, v) in &self.puts {
            field(&mut b, k.as_bytes());
            field(&mut b, v);
        }
        b.extend((self.deletes.len() as u32).to_le_bytes());
        for k in &self.deletes {
            field(&mut b, k.as_bytes());
        }
        let n = (b.len() - 4) as u32;
        b[..4].copy_from_slice(&n.to_le_bytes());
        b.into()
    }

    /// Take one whole frame off the front of `buf`, if it has one.
    pub fn take(buf: &mut bytes::BytesMut) -> Result<Option<Delta>> {
        if buf.len() < 4 || buf.len() < 4 + u32::from_le_bytes(buf[..4].try_into()?) as usize {
            return Ok(None);
        }
        let n = u32::from_le_bytes(buf[..4].try_into()?) as usize;
        let mut f = buf.split_to(4 + n).freeze().slice(4..);
        let id = u64::from_le_bytes(next(&mut f, 8)?[..].try_into()?);
        let puts = (0..count(&mut f)?).map(|_| Ok((String::from_utf8(field(&mut f)?.to_vec())?, field(&mut f)?))).collect::<Result<_>>()?;
        let deletes = (0..count(&mut f)?).map(|_| Ok(String::from_utf8(field(&mut f)?.to_vec())?)).collect::<Result<_>>()?;
        Ok(Some(Delta { id, puts, deletes }))
    }
}

/// All entries of a scan.
async fn collect(mut it: slatedb::DbIterator) -> Result<BTreeMap<String, Bytes>> {
    let mut all = BTreeMap::new();
    while let Some(kv) = it.next().await? {
        all.insert(String::from_utf8(kv.key.to_vec())?, kv.value);
    }
    Ok(all)
}

fn next(f: &mut Bytes, n: usize) -> Result<Bytes> {
    anyhow::ensure!(f.len() >= n, "truncated frame");
    Ok(f.split_to(n))
}
fn count(f: &mut Bytes) -> Result<u32> { Ok(u32::from_le_bytes(next(f, 4)?[..].try_into()?)) }
fn field(f: &mut Bytes) -> Result<Bytes> {
    let n = count(f)? as usize;
    next(f, n)
}

impl Catalog {
    async fn writer(store: Store) -> Result<Self> {
        // Poll object storage rarely when idle (that's an idle writer's request bill), but often
        // enough that compaction keeps up with the checkpoints.
        let compactor_options = Some(CompactorOptions { poll_interval: Duration::from_secs(5), ..Default::default() });
        let settings = Settings { flush_interval: Some(Duration::from_millis(1)), manifest_poll_interval: Duration::from_secs(10), l0_sst_size_bytes: 16 << 20, l0_max_ssts: 64, l0_max_ssts_per_key: 32, max_unflushed_bytes: 64 << 20, compactor_options, ..Default::default() };
        let mut cat = Self::new(Db_::Writer(Db::builder("catalog", store).with_settings(settings).build().await?));
        cat.last_n.store(cat.get::<u64>("n").await?.unwrap_or(1), Relaxed);
        *cat.order.get_mut() = cat.get::<u64>("c").await?.unwrap_or(0) + 1;
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(publish(rx, cat.feed.clone(), cat.recent.clone()));
        cat.durable = Some(tx);
        Ok(cat)
    }

    async fn reader(store: Store, streamed: bool) -> Result<Self> {
        let opts = DbReaderOptions { manifest_poll_interval: Duration::from_millis(250), skip_wal_replay: streamed, ..Default::default() };
        // FollowLatest writes nothing, so readers work with read-only bucket credentials.
        let cat = Self::new(Db_::Reader(DbReader::open("catalog", store, DbReaderMode::FollowLatest, opts).await?));
        cat.refresh().await?;
        Ok(cat)
    }

    fn new(db: Db_) -> Self {
        let (feed, order) = (broadcast::channel(1024).0, tokio::sync::Mutex::new(1));
        let (last_n, view, pruned, pins, streamed, hold) = Default::default();
        Catalog { db, order, durable: None, feed, recent: Default::default(), last_n, overlay: Default::default(), view, pruned, pins, streamed, hold }
    }

    /// Leader: the recent commits plus a receiver for every commit from now on.
    pub fn subscribe(&self) -> (Vec<Arc<Delta>>, broadcast::Receiver<Arc<Delta>>) {
        let rx = self.feed.subscribe();
        (self.recent.lock().unwrap().0.iter().map(|(_, d)| d.clone()).collect(), rx)
    }

    fn apply(&self, d: &Delta, gap: bool) {
        let mut o = self.overlay.lock().unwrap();
        if d.id <= self.pruned.load(Relaxed) {
            return; // our own view has it, and newer changes to its keys may already be dropped
        }
        if gap {
            // Some commits never arrived. What we hold is no longer a prefix of the lake, so
            // drop it and read from our own view alone until the view has passed the gap.
            o.clear();
            self.hold.fetch_max(d.id - 1, Relaxed);
        }
        if let Some((_, v)) = d.puts.iter().find(|(k, _)| k == "n") {
            self.last_n.fetch_max(serde_json::from_slice(v).unwrap_or(1), Relaxed);
        }
        o.extend(d.puts.iter().map(|(k, v)| (k.clone(), (d.id, Some(v.clone())))));
        o.extend(d.deletes.iter().map(|k| (k.clone(), (d.id, None))));
        self.streamed.fetch_max(d.id, Relaxed);
    }

    /// Whether a read that saw our own view at commit `c` may lay the streamed changes over it.
    /// (Call it while holding the overlay lock: `apply` changes both under it.)
    fn overlay_ok(&self, c: u64) -> bool { self.hold.load(Relaxed) <= c && c <= self.streamed.load(Relaxed) }

    /// Our own view's commit number right now.
    async fn view_now(&self) -> Result<u64> {
        let Db_::Reader(r) = &self.db else { return Ok(0) };
        Ok(r.get("c").await?.map(|v| serde_json::from_slice(&v)).transpose()?.unwrap_or(0))
    }

    /// Non-leader: how far our own catalog view is; drop the streamed changes it now has.
    async fn refresh(&self) -> Result<()> {
        let Db_::Reader(r) = &self.db else { return Ok(()) };
        let get = |k: &'static str| async move { Ok::<u64, anyhow::Error>(r.get(k).await?.map(|v| serde_json::from_slice(&v)).transpose()?.unwrap_or(0)) };
        // "n" BEFORE "c": our view only moves forward, so this `n` is one our view already had
        // at commit `c` (the other way round it could be from a newer view than `c`, and reads
        // held back to an older commit would miss segments this node thought it could read).
        let (n, c) = (get("n").await?.max(1), get("c").await?);
        // Drop the streamed copies our view now has, except those a scan still in progress
        // (it started from an older view) may lay over its data.
        let upto = {
            let pins = self.pins.lock().unwrap();
            self.view.0.store(c, Relaxed);
            self.view.1.store(n, Relaxed);
            pins.keys().next().map_or(c, |&p| p.min(c))
        };
        let mut o = self.overlay.lock().unwrap();
        self.pruned.fetch_max(upto, Relaxed);
        o.retain(|_, (id, _)| *id > upto);
        Ok(())
    }

    /// Before a scan: see `Pin`. Its data will come from a view at least this new.
    fn pin(&self) -> Pin<'_> {
        let mut pins = self.pins.lock().unwrap();
        let c = self.view.0.load(Relaxed);
        *pins.entry(c).or_default() += 1;
        Pin(self, c)
    }

    fn overlaid(&self) -> bool {
        let _o = self.overlay.lock().unwrap();
        self.overlay_ok(self.view.0.load(Relaxed))
    }

    /// The "n" of the newest commit this node's reads include.
    pub fn visible_n(&self) -> u64 {
        match (&self.db, self.overlaid()) {
            (Db_::Reader(_), true) => self.view.1.load(Relaxed).max(self.last_n.load(Relaxed)),
            (Db_::Reader(_), false) => self.view.1.load(Relaxed),
            (Db_::Writer(_), _) => self.last_n.load(Relaxed),
        }
    }

    pub fn is_writer(&self) -> bool { matches!(self.db, Db_::Writer(_)) }

    /// The number of the last commit our own view has.
    pub fn view_c(&self) -> u64 { self.view.0.load(Relaxed) }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.get_raw(key).await?.map(|v| serde_json::from_slice(&v).context(key.to_string())).transpose()
    }

    pub async fn get_raw(&self, key: &str) -> Result<Option<Bytes>> {
        Ok(match &self.db {
            // Queries only ever see durable (acknowledged) data, never in-flight writes.
            Db_::Writer(db) => match db.get_with_options(key, &ReadOptions::new().with_durability_filter(DurabilityLevel::Remote)).await.map_err(fatal)? {
                // Inline segment data is only asked for once its segment is visible, so it is
                // durable too; while a checkpoint moves it the durable-only read can miss it.
                None if key.starts_with("d/") => db.get(key).await.map_err(fatal)?,
                v => v,
            },
            Db_::Reader(r) => {
                // A streamed copy is newer than our view's (unless the view is ahead of the
                // stream); inline segment data never changes, so its streamed copy is always good.
                let inline = key.starts_with("d/");
                let c = if inline { 0 } else { self.view_now().await? };
                let copy = {
                    let o = self.overlay.lock().unwrap(); // (under the lock: see `apply`)
                    o.get(key).filter(|_| inline || self.overlay_ok(c)).map(|(_, v)| v.clone())
                };
                match copy {
                    Some(v) => v,
                    None => r.get(key).await?, // (read after the check: at least as new)
                }
            }
        })
    }

    /// All entries with keys in [from, to).
    pub async fn scan<T: DeserializeOwned>(&self, from: &str, to: &str) -> Result<Vec<(String, T)>> {
        if from >= to {
            return Ok(vec![]);
        }
        let range = from.as_bytes().to_vec()..to.as_bytes().to_vec();
        let decode = |all: BTreeMap<String, Bytes>| all.into_iter().map(|(k, v)| Ok((k, serde_json::from_slice(&v)?))).collect();
        let r = match &self.db {
            Db_::Writer(db) => return decode(collect(db.scan_with_options(range, &ScanOptions::new().with_durability_filter(DurabilityLevel::Remote)).await.map_err(fatal)?).await?),
            Db_::Reader(r) => r,
        };
        let _pin = self.pin();
        loop {
            // Our view is somewhere between `before` and `after` while we read it.
            let before = self.view_now().await?;
            let mut all = collect(r.scan(range.clone()).await?).await?;
            let after = self.view_now().await?;
            // Under the lock, so the stream can't move while we decide (see `apply`).
            let streamed = {
                let o = self.overlay.lock().unwrap();
                let streamed = self.streamed.load(Relaxed);
                if self.hold.load(Relaxed) <= before && after <= streamed {
                    for (k, (_, v)) in o.range(from.to_string()..to.to_string()) {
                        match v {
                            Some(v) => all.insert(k.clone(), v.clone()),
                            None => all.remove(k),
                        };
                    }
                    return decode(all); // = the lake as of commit `streamed`
                }
                streamed
            };
            if before >= streamed || before < self.hold.load(Relaxed) {
                return decode(all); // our view alone: newer than the stream (or the stream has a gap)
            }
            tokio::time::sleep(Duration::from_millis(1)).await; // our view passed the stream mid-read: again
        }
    }

    /// Persist the (small) catalog memtable, so a restart replays only the WAL written since.
    pub async fn checkpoint(&self) -> Result<()> {
        if let Db_::Writer(db) = &self.db {
            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
        }
        Ok(())
    }

    /// Write puts and deletes atomically and wait until durable (and streamed to the followers).
    pub async fn commit(&self, puts: Vec<(String, Vec<u8>)>, deletes: &[String]) -> Result<()> {
        Ok(self.write(puts, deletes).await?.await?)
    }

    /// Write puts and deletes atomically. The returned receiver fires once the write is durable;
    /// writes become durable in the order they were made, so a caller can make the next write
    /// without waiting (pipelined commits: one object-store round trip no longer blocks the next).
    pub async fn write(&self, puts: Vec<(String, Vec<u8>)>, deletes: &[String]) -> Result<oneshot::Receiver<()>> {
        let (Db_::Writer(db), Some(durable)) = (&self.db, &self.durable) else { bail!("read-only node") };
        let puts: Vec<(String, Bytes)> = puts.into_iter().map(|(k, v)| (k, v.into())).collect();
        let mut batch = WriteBatch::new();
        for (k, v) in &puts {
            batch.put(k, v);
        }
        for k in deletes {
            batch.delete(k);
        }
        let mut id = self.order.lock().await;
        let c = ("c".to_string(), Bytes::from(json(&*id)));
        batch.put(&c.0, &c.1);
        let handle = db.write(batch).await.map_err(fatal)?;
        if let Some((_, v)) = puts.iter().find(|(k, _)| k == "n") {
            self.last_n.store(serde_json::from_slice(v)?, Relaxed);
        }
        let (done, rx) = oneshot::channel();
        let puts = puts.into_iter().chain([c]).collect();
        let _ = durable.send((handle, Arc::new(Delta { id: *id, puts, deletes: deletes.to_vec() }), done));
        *id += 1;
        Ok(rx)
    }
}

/// Leader: as each write becomes durable (in order), stream it to the followers and release its
/// writer. If one fails (e.g. a new leader fenced us out) its outcome is unknown: restart and
/// rejoin; the restart reloads the state.
async fn publish(mut rx: mpsc::UnboundedReceiver<(WriteHandle, Arc<Delta>, oneshot::Sender<()>)>, feed: broadcast::Sender<Arc<Delta>>, recent: Recent) {
    while let Some((handle, d, done)) = rx.recv().await {
        if let Err(e) = handle.await_durable().await {
            eprintln!("catalog commit failed: {e}");
            crate::cluster::restart();
        }
        let mut r = recent.lock().unwrap();
        r.1 += d.bytes();
        r.0.push_back((Instant::now(), d.clone()));
        while r.0.front().is_some_and(|(t, old)| t.elapsed() > RECENT || (r.1 > RECENT_BYTES && !Arc::ptr_eq(old, &d))) {
            let (_, old) = r.0.pop_front().expect("non-empty");
            r.1 -= old.bytes();
        }
        drop(r);
        let _ = feed.send(d); // no followers listening is fine
        let _ = done.send(());
    }
}

/// A writer that was fenced out (a new leader took over) must stop at once: it restarts and
/// rejoins the cluster as a follower.
fn fatal(e: slatedb::Error) -> anyhow::Error {
    if matches!(e.kind(), ErrorKind::Closed(_)) {
        eprintln!("catalog closed: {e}");
        crate::cluster::restart();
    }
    e.into()
}

/// Test hook: `PONDRA_CRASH=after_seg_put:0.05,after_parquet_put:0.2` aborts the process at
/// that point with that probability, to prove crashes never lose or duplicate rows.
pub fn maybe_crash(point: &str) {
    let Ok(spec) = std::env::var("PONDRA_CRASH") else { return };
    for (name, prob) in spec.split(',').filter_map(|p| p.split_once(':')) {
        let roll = (uuid::Uuid::new_v4().as_u128() % 1_000_000) as f64 / 1e6;
        if name == point && roll < prob.parse().unwrap_or(0.0) {
            eprintln!("PONDRA_CRASH: aborting at {point}");
            std::process::abort();
        }
    }
}
