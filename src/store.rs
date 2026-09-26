//! The lake: object storage (a local dir or s3://bucket/prefix on S3, R2, MinIO…) with the
//! catalog (SlateDB) inside it. Nothing else holds state.
//!
//! The leader is the catalog's only writer. It also streams every commit to the followers the
//! moment it is durable, and they lay those changes over their own (slightly behind) view of the
//! catalog: every node sees a commit within milliseconds, while the bucket stays the source of truth.
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::ClientConfigKey;
use object_store::{local::LocalFileSystem, path::Path, prefix::PrefixStore, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use slatedb::config::{CompactorOptions, DbReaderOptions, DurabilityLevel, FlushOptions, FlushType, ObjectStoreCacheOptions, ReadOptions, ScanOptions, Settings};
use slatedb::{Db, DbReader, DbReaderMode, ErrorKind, WriteBatch, WriteHandle};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
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
    pub files: Vec<DataFile>, // the recent files; older ones of append tables are sealed (`manifest.rs`)
    #[serde(default)]
    pub sealed: Option<crate::manifest::Sealed>,
    pub tiered: u64, // every segment <= `tiered` is already inside `files` (or sealed)
    #[serde(default)]
    pub garbage: Vec<(String, u64)>, // replaced files + when; deleted after the retention period
    #[serde(default)]
    pub publish: Vec<String>, // open formats other engines also read it in: "delta", "iceberg"
    #[serde(default)]
    pub cluster: Vec<String>, // append tables: each file's rows sorted by these (see `tier::clustered`)
    #[serde(default)]
    pub ttl: Option<(String, u64)>, // keyed tables: a row whose (timestamp) column is older than this many seconds is gone
    #[serde(default)]
    pub partition: Option<String>, // append tables: every file holds one value of this ("col", "day(col)", "hour(col)", "month(col)")
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sketch: BTreeMap<String, String>, // append tables: each key-like column's distinct values, sketched (`sketch.rs`)
    #[serde(default)]
    pub ids: bool, // every row has its system columns (`sys.rs`): tables made from round 19 on
    #[serde(default)]
    pub changed: bool, // append tables: UPDATE, DELETE or MERGE has replaced rows (`{t}$deleted` holds the old ones)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub purges: Vec<(u64, u64)>, // changed tables: (commit, when): the old rows of every change up to it are out of the files (`tier::purge`)
}

impl TableMeta {
    /// The changes whose old rows are out of the table's files: reads skip their `{t}$deleted` rows.
    pub fn purged(&self) -> u64 { self.purges.last().map_or(0, |p| p.0) }

    /// The TTL as a SQL condition that keeps live rows ("" if none).
    pub fn ttl_sql(&self) -> String {
        self.ttl.as_ref().map(|(c, s)| format!("\"{c}\" >= now() - INTERVAL '{s} seconds'")).unwrap_or_default()
    }
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
    /// A compaction output: the whole table, one row per key, no delete markers.
    #[serde(default)]
    pub whole: bool,
    /// Append tables: each column's min and max, so queries skip files without opening them.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub stats: crate::manifest::Stats,
    /// Partitioned tables: the one partition value this file holds.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub part: String,
    /// Its rows carry their system columns (`sys.rs`); files written before round 19 don't.
    #[serde(default)]
    pub sys: bool,
    /// Append tables: the columns (of those with `stats`) that hold a NULL. None: not known (files
    /// written before round 15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nulls: Option<Vec<String>>,
    /// A new file's distinct-value sketches (`sketch.rs`), on their way into its table's.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sketch: BTreeMap<String, String>,
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

/// The open formats a new table is published in unless it says otherwise (the node's `--publish`).
pub fn default_publish() -> Vec<String> {
    std::env::var("PONDRA_PUBLISH").unwrap_or_default().split(',').filter(|f| !f.is_empty()).map(String::from).collect()
}

const TAIL_BYTES: usize = 256 << 20; // decoded log segments kept in memory, per node
const AHEAD: u64 = 256; // replicated acks: commits acknowledged before the bucket has them, at most

const RECENT: Duration = Duration::from_secs(30); // replayed to (re)connecting followers…
const RECENT_BYTES: usize = 64 << 20; // …within this much memory (past it, a reconnecting
// follower reads from its own view until it has caught up, instead of the leader buffering more)

/// Query read cache in memory, MB (env PONDRA_CACHE_MB, default 1024).
fn cache_mb() -> usize { std::env::var("PONDRA_CACHE_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(1024) }

/// The local SSD tier for a lake on object storage (`serve --cache-dir/--cache-gb`, or env
/// PONDRA_CACHE_DIR / PONDRA_CACHE_GB): under the temp dir, 20 GB, or a quarter of the free disk
/// if that is less (a notebook's sandbox may have a few GB). 0 GB turns it off.
fn disk_tier(url: &str, store: &Store) -> Option<Arc<crate::cache::Disk>> {
    let dir = std::env::var("PONDRA_CACHE_DIR").map(std::path::PathBuf::from).unwrap_or_else(|_| std::env::temp_dir().join("pondra-cache"));
    let bytes = match std::env::var("PONDRA_CACHE_GB").ok().and_then(|v| v.parse::<u64>().ok()) {
        Some(gb) => gb << 30,
        None => free_bytes(&dir).map_or(20 << 30, |free| (free / 4).min(20 << 30)),
    };
    let dir = dir.join(url.trim_start_matches("s3://").replace(['/', ':', '\\'], "_")); // one folder per lake
    (bytes > 0).then(|| crate::cache::Disk::open(dir, bytes, store.clone()).ok()).flatten()
}

/// Free bytes on the disk that holds `dir` (or its nearest existing parent), where the platform says.
fn free_bytes(dir: &std::path::Path) -> Option<u64> {
    #[cfg(unix)]
    {
        let at = std::ffi::CString::new(dir.ancestors().find(|a| a.exists())?.as_os_str().as_encoded_bytes()).ok()?;
        let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
        (unsafe { libc::statvfs(at.as_ptr(), &mut s) } == 0).then(|| s.f_bavail as u64 * s.f_frsize as u64)
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        None
    }
}

pub struct Lake {
    pub url: String, // absolute local dir or "s3://bucket/prefix"
    pub store: Store,
    pub cat: Catalog,
    pub hwm: watch::Sender<u64>, // the last committed segment this node knows of
    pub backlog: std::sync::atomic::AtomicU64, // leader: rows in the log not yet tiered (all tables)
    rt: Arc<RuntimeEnv>,         // shared by all queries: object store registry + Parquet metadata cache
    tail: Mutex<(lru::LruCache<(u64, String), Rows>, usize)>, // decoded (segment, table) rows; total bytes
    pub disk: Option<Arc<crate::cache::Disk>>, // lakes on object storage: the local SSD tier
    pub groups: crate::serve::Groups,          // decoded row groups for key lookups
    pub hot: Arc<crate::hot::Hot>,             // decoded columns of files queries read lately
    pub attached: std::sync::RwLock<Vec<(String, Arc<Lake>)>>, // other lakes, read as `name.table` (`--attach`)
    pub ids: crate::sys::Ids, // the row ids this process stamps rows with (a block reserved from the leader)
    me: std::sync::Weak<Lake>,
}

impl std::fmt::Debug for Lake {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Lake({})", self.url) }
}

/// How much memory queries may use on this node (`PONDRA_MEMORY_GB`, else a third of RAM). Past
/// it, sorts, aggregations and joins spill to temporary files, or the query stops with an error:
/// a query never takes the node down. (A third, not half: what DataFusion counts here is the big
/// hash tables and sort buffers, not the Parquet decoding and the batches in flight around them,
/// so the node's own memory runs ahead of this number. The hot columns come out of it too.)
pub fn memory_limit() -> usize {
    let gb = std::env::var("PONDRA_MEMORY_GB").ok().and_then(|g| g.parse::<f64>().ok());
    gb.map(|g| (g * (1u64 << 30) as f64) as usize).or_else(|| ram().map(|r| r / 3)).unwrap_or(4 << 30)
}

/// The machine's memory, if it says: `/proc` on Linux, the OS's own call elsewhere.
pub fn ram() -> Option<usize> {
    static RAM: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *RAM.get_or_init(read_ram)
}

fn read_ram() -> Option<usize> {
    let proc = std::fs::read_to_string("/proc/meminfo").ok().and_then(|m| m.lines().find_map(|l| l.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?.trim().parse::<usize>().ok()));
    proc.map(|kb| kb << 10).or_else(|| {
        let s = sysinfo::System::new_with_specifics(sysinfo::RefreshKind::nothing().with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()));
        Some(s.total_memory() as usize).filter(|&b| b > 0)
    })
}

/// This process's resident memory, if the OS says.
pub fn resident() -> Option<usize> {
    let proc = std::fs::read_to_string("/proc/self/statm").ok().and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok());
    proc.map(|pages| pages * 4096).or_else(|| {
        use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
        let (pid, mut s) = (sysinfo::get_current_pid().ok()?, System::new());
        s.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, ProcessRefreshKind::nothing().with_memory());
        Some(s.process(pid)?.memory() as usize).filter(|&b| b > 0)
    })
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
    // Idle connections are dropped after 15 s rather than reused: through proxies and NATs that
    // silently forget idle connections, a reused one hung a PUT for the full 30 s timeout on R2.
    let idle = (AmazonS3ConfigKey::Client(ClientConfigKey::PoolIdleTimeout), "15s");
    let s3: Store = Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).with_allow_http(true).with_config(idle.0, idle.1).build()?);
    let store: Store = if prefix.is_empty() { s3.clone() } else { Arc::new(PrefixStore::new(s3.clone(), prefix)) };
    Ok((url.trim_end_matches('/').to_string(), store, Some((format!("s3://{bucket}"), s3))))
}

impl Lake {
    /// `writer`: the leader. `streamed`: a follower, which also gets every commit streamed from
    /// the leader, so its own catalog view only needs the leader's checkpoints (no log replay).
    pub async fn open(url: &str, writer: bool, streamed: bool) -> Result<Arc<Lake>> {
        let (url, store, bucket) = open_store(url)?;
        let pool = Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(memory_limit()));
        let rt = RuntimeEnvBuilder::new().with_memory_pool(pool).build_arc()?; // (spills go to the OS temp dir)
        let disk = bucket.as_ref().and_then(|_| disk_tier(&url, &store));
        if let Some((bucket_url, s3)) = bucket {
            let prefix = url.trim_start_matches(&bucket_url).trim_start_matches('/').to_string();
            let cached = crate::cache::CachedStore::new(s3, cache_mb() << 20, disk.clone().map(|d| (d, prefix)));
            rt.register_object_store(&url::Url::parse(&bucket_url)?, Arc::new(cached));
        }
        // The catalog's own files go on the SSD tier too (next to the lake's objects), so catalog
        // reads — the leader's, a new node's, a reader's without a live leader — are local.
        let cache = disk.as_ref().map(|d| d.dir.with_extension("catalog"));
        let cache = ObjectStoreCacheOptions { root_folder: cache, max_cache_size_bytes: Some(2 << 30), cache_on_flush: true, cache_on_compaction: true, ..Default::default() };
        let cat = if writer { Catalog::writer(store.clone(), cache).await? } else { Catalog::reader(store.clone(), streamed, cache).await? };
        let hwm = watch::Sender::new(cat.get::<u64>("n").await?.unwrap_or(1) - 1);
        let lake = Arc::new_cyclic(|me| Lake { url, store, cat, hwm, backlog: Default::default(), rt, tail: Mutex::new((lru::LruCache::unbounded(), 0)), disk, groups: crate::serve::Groups::new(cache_mb() << 19), hot: Arc::new(crate::hot::Hot::new()), attached: Default::default(), ids: Default::default(), me: me.clone() });
        lake.hot.watch(); // the decoded columns give memory back when the node needs it
        if let Some(writes) = lake.cat.unstarted.lock().unwrap().take() {
            tokio::spawn(lake.clone().commits(writes));
        }
        Ok(lake)
    }

    /// Leader: commits, in write order. A write is committed — acknowledged, visible here, and
    /// streamed as committed — once it is in the bucket, or (`--ack replicated`) once
    /// `replicas - 1` member followers hold it, whichever comes first. It reaches the bucket a
    /// moment later either way. A write that fails to reach the bucket (a newer leader fenced
    /// us out) has an unknown outcome: restart and rejoin; the restart reloads the state.
    async fn commits(self: Arc<Self>, mut writes: mpsc::UnboundedReceiver<Write>) {
        let (to_bucket, mut in_bucket) = mpsc::unbounded_channel::<(Arc<WriteHandle>, u64)>();
        let lake = self.clone();
        tokio::spawn(async move {
            while let Some((handle, id)) = in_bucket.recv().await {
                if let Err(e) = handle.await_durable().await {
                    eprintln!("catalog commit failed: {e}");
                    crate::cluster::restart("a catalog commit failed");
                }
                lake.cat.durable.send_replace(id);
                lake.cat.send(Frame::Durable(id));
            }
        });
        let cat = &self.cat;
        let need = cat.replicas.saturating_sub(1);
        while let Some((handle, d, done)) = writes.recv().await {
            if need > 0 {
                cat.send(Frame::Change(d.clone())); // followers keep it and say so
            }
            let mut acked = cat.acked.subscribe();
            // (At most AHEAD commits are acknowledged ahead of the bucket: if it stalls, acks wait
            // for it, so what a failover has to recover — and could lose — stays bounded.)
            let close = *cat.durable.borrow() + AHEAD >= d.id;
            loop {
                if need > 0 && close && cat.holders(d.id) >= need {
                    break;
                }
                tokio::select! {
                    r = handle.await_durable() => {
                        if let Err(e) = r {
                            eprintln!("catalog commit failed: {e}");
                            crate::cluster::restart("a catalog commit failed");
                        }
                        break;
                    }
                    _ = acked.changed() => {}
                }
            }
            if need == 0 {
                cat.send(Frame::Change(d.clone()));
            }
            cat.apply(&d); // our in-memory catalog
            cat.committed.store(d.id, Relaxed);
            cat.send(Frame::Committed(d.id));
            let _ = to_bucket.send((handle, d.id));
            let _ = done.send(());
        }
    }

    /// Follower: a change streamed from the leader; it takes effect once committed.
    pub fn hold(&self, d: Arc<Delta>) {
        self.prefetch(&d);
        self.cat.pending.lock().unwrap().insert(d.id, d);
    }

    /// Follower: lay the committed changes (up to `upto`) over our view of the catalog.
    pub fn commit_upto(&self, upto: u64) {
        let ready: Vec<Arc<Delta>> = {
            let mut p = self.cat.pending.lock().unwrap();
            let later = p.split_off(&(upto + 1));
            std::mem::replace(&mut *p, later).into_values().collect()
        };
        ready.iter().for_each(|d| self.cat.apply(d));
        self.advance(self.cat.visible_n() - 1);
    }

    /// Keep the recent lake on this node's SSD: every object a commit brings in (a log segment
    /// another node wrote, a new Parquet file) is fetched in the background, so a query on any
    /// node reads recent data from local disk, not from the bucket.
    pub fn prefetch(&self, d: &Delta) {
        let Some(disk) = &self.disk else { return };
        for (key, value) in &d.puts {
            let paths: Vec<String> = match key.get(..2) {
                Some("s/") => serde_json::from_slice::<Segment>(value).map(|s| vec![s.path]).unwrap_or_default(),
                // (files a tiering round writes; not a bulk load's big files, which only the queries that need them read)
                Some("t/") => serde_json::from_slice::<TableMeta>(value).map(|m| m.files.into_iter().filter(|f| f.bytes <= 256 << 20).map(|f| f.path).collect()).unwrap_or_default(),
                _ => vec![], // (keys like "c" and "n" are one character long)
            };
            paths.into_iter().filter(|p| !p.is_empty()).for_each(|p| disk.fetch_later(p));
        }
    }

    /// A node starting on a lake in object storage: copy the log tail and the newest table
    /// files (up to half the SSD tier) to local disk in the background, so a first query on a
    /// fresh node doesn't wait on the bucket either.
    pub async fn warm(&self) -> Result<()> {
        let Some(disk) = &self.disk else { return Ok(()) };
        let tables = self.cat.scan::<TableMeta>("t/", "t0").await?;
        let tiered = tables.iter().map(|(_, m)| m.tiered).min().unwrap_or(0);
        for (_, seg) in self.cat.scan::<Segment>(&seg_key(tiered + 1), "s0").await? {
            if !seg.path.is_empty() {
                disk.fetch_later(seg.path);
            }
        }
        let mut budget = disk.max / 2;
        for f in tables.iter().flat_map(|(_, m)| m.files.iter().rev()) {
            if f.bytes <= budget {
                budget -= f.bytes;
                disk.fetch_later(f.path.clone());
            }
        }
        Ok(())
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

    /// What a remembered answer to `sql` depends on: this lake's catalog, and those of the attached
    /// lakes it may read, itself or through the views it reads (and which lakes are attached).
    /// None: nothing to remember it by.
    pub async fn version_for(&self, sql: &str) -> Option<u64> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.cat.version()?.hash(&mut h);
        let mut text = sql.to_string();
        if !self.attached.read().unwrap().is_empty() {
            crate::query::stored_views(self, sql, false).await.ok()?.iter().for_each(|(_, v)| text.push_str(&format!(" {v}")));
        }
        for (name, other) in self.attached.read().unwrap().iter().filter(|(n, _)| crate::ddl::mentions(&text, n)) {
            (name, other.cat.version()?).hash(&mut h);
        }
        Some(h.finish())
    }

    /// Read another lake as `name.table` in this one's queries (its bucket's reader goes into our
    /// runtime too: a query here reads its files).
    pub fn attach(&self, name: &str, other: Arc<Lake>) -> Result<()> {
        let name = &name.to_lowercase(); // (as SQL reads an unquoted name)
        crate::ddl::check(name)?;
        anyhow::ensure!(*name != crate::ddl::lake_name(self), "this lake is called {name} already: attach the other under another name");
        if let Some(bucket) = other.url.strip_prefix("s3://").map(|r| format!("s3://{}", r.split('/').next().unwrap_or_default())) {
            let url = url::Url::parse(&bucket)?;
            self.rt.register_object_store(&url, other.rt.object_store(datafusion::execution::object_store::ObjectStoreUrl::parse(&bucket)?)?);
        }
        self.attached.write().unwrap().push((name.to_string(), other));
        Ok(())
    }

    /// A fresh SQL session on the shared runtime (`partitions()` of them).
    pub fn session(&self) -> SessionContext {
        self.session_with(partitions())
    }

    /// A session with a fixed number of partitions: 1 for point lookups, where splitting the work
    /// costs more than it saves and many queries run at once.
    pub fn session_with(&self, partitions: usize) -> SessionContext {
        let config = crate::optimize::config(SessionConfig::new().with_information_schema(true).with_target_partitions(partitions)
            .with_default_catalog_and_schema(crate::ddl::lake_name(self), crate::ddl::PUBLIC));
        let state = SessionStateBuilder::new().with_config(config).with_runtime_env(self.rt.clone()).with_default_features();
        let state = state.with_optimizer_rules(crate::optimize::rules()).with_physical_optimizer_rules(crate::optimize::physical_rules());
        let mut ctx = SessionContext::new_with_state(state.build());
        datafusion_functions_json::register_all(&mut ctx).expect("JSON functions register"); // json_get(…), ->, ->>
        crate::files::register(&ctx, self.arc()); // files('…'), file_read(path)
        crate::ai::register(&ctx); // ai_complete, ai_embed, cosine_similarity, …
        crate::asof::register(&ctx); // (ASOF JOIN's marker)
        crate::fsum::register(&ctx); // sum(DOUBLE): the same answer in any order
        ctx
    }

    /// Query memory in use, and the limit.
    pub fn memory(&self) -> (usize, usize) { (self.rt.memory_pool.reserved(), memory_limit()) }

    /// This lake as an `Arc` (for query plans that outlive the call that made them).
    pub fn arc(&self) -> Arc<Lake> { self.me.upgrade().expect("a lake outlives its queries") }

    /// Full URL of an object, for DataFusion.
    pub fn full(&self, path: &str) -> String { format!("{}/{path}", self.url) }

    /// The store DataFusion reads `url` through (for lakes on object storage: the read cache and
    /// the SSD tier in front of the bucket).
    pub fn object_store(&self, url: &datafusion::datasource::listing::ListingTableUrl) -> Result<Arc<dyn object_store_df::ObjectStore>> {
        Ok(self.rt.object_store(url)?)
    }

    /// Write an object only if it does not exist yet: data is never overwritten.
    pub async fn put(&self, path: &str, bytes: Vec<u8>) -> Result<()> {
        let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
        let bytes = Bytes::from(bytes);
        self.store.put_opts(&Path::from(path), bytes.clone().into(), opts).await?;
        if let (Some(disk), false) = (&self.disk, crate::delta::open_format(path)) {
            disk.put(path, &bytes); // what a node writes, it keeps
        }
        Ok(())
    }

    /// A whole lake object: from the SSD tier if it's there, else from the bucket (and kept).
    pub async fn object(&self, path: &str) -> Result<Bytes> {
        if let Some((file, _)) = self.disk.as_ref().and_then(|d| d.get(path)) {
            if let Ok(bytes) = tokio::fs::read(file).await {
                return Ok(bytes.into());
            }
        }
        let bytes = self.store.get(&Path::from(path)).await?.bytes().await?;
        if let Some(disk) = &self.disk {
            disk.put(path, &bytes);
        }
        Ok(bytes)
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
            false => self.object(&seg.path).await?,
        };
        let mut rows = vec![];
        for &(off, len, _) in parts {
            for b in crate::log::decode(&bytes[off as usize..(off + len) as usize])? {
                rows.push(crate::sys::expand(b)?); // (fresh row ids: one number in the log)
            }
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
    flushed: AtomicU64,             // leader: `order` at the last memtable flush
    writes: Option<mpsc::UnboundedSender<Write>>, // leader: writes waiting to commit, in order
    unstarted: Mutex<Option<mpsc::UnboundedReceiver<Write>>>, // (until `Lake::open` starts `commits`)
    committed: AtomicU64,           // leader: the last committed write (what reads see)
    durable: watch::Sender<u64>,    // leader: the last write that is in the bucket
    pub replicas: usize,            // leader: copies that commit a write (1: the bucket's alone)
    acks: Mutex<(BTreeMap<String, (u64, u64)>, Vec<String>)>, // leader: follower -> the run of changes it holds; the members whose copies count
    acked: watch::Sender<()>,       // leader: an ack arrived
    pending: Mutex<BTreeMap<u64, Arc<Delta>>>, // follower: changes streamed but not yet committed
    feed: broadcast::Sender<Frame>, // leader: the commit stream
    recent: Recent, // leader: its last RECENT (and their bytes), for (re)connecting followers
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
    follows: bool,                // follower / read-only node that gets the commit stream
    mirror: AtomicBool,           // the overlay holds the whole catalog (but inline data): read only it
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

type Recent = Arc<Mutex<(VecDeque<(Instant, Frame)>, usize)>>;
type Write = (Arc<WriteHandle>, Arc<Delta>, oneshot::Sender<()>);

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
}

/// What the leader streams to the other nodes (`GET /cluster/log`), in order.
#[derive(Clone)]
pub enum Frame {
    /// First on every connection: the leader's term, and whether commits are replicated
    /// (followers then keep each change on disk and acknowledge it; see `replica.rs`).
    Start { term: u64, replicated: bool },
    /// A write, in order. It takes effect with the `Committed` notice that covers it.
    Change(Arc<Delta>),
    /// Every change up to here is committed: apply it.
    Committed(u64),
    /// … and is in the bucket: a follower no longer needs its copy.
    Durable(u64),
}

impl Frame {
    /// Wire format: u32 length | u8 kind | body. A change's body: u64 id | puts: u32 count,
    /// (key, value)* | deletes: u32 count, key*; every key and value is a u32 length and bytes.
    pub fn encode(&self) -> Bytes {
        let mut b = vec![0u8; 4];
        let field = |b: &mut Vec<u8>, x: &[u8]| {
            b.extend((x.len() as u32).to_le_bytes());
            b.extend(x);
        };
        match self {
            Frame::Start { term, replicated } => {
                b.push(0);
                b.extend(term.to_le_bytes());
                b.push(*replicated as u8);
            }
            Frame::Change(d) => {
                b.push(1);
                b.extend(d.id.to_le_bytes());
                b.extend((d.puts.len() as u32).to_le_bytes());
                for (k, v) in &d.puts {
                    field(&mut b, k.as_bytes());
                    field(&mut b, v);
                }
                b.extend((d.deletes.len() as u32).to_le_bytes());
                for k in &d.deletes {
                    field(&mut b, k.as_bytes());
                }
            }
            Frame::Committed(id) => (b.push(2), b.extend(id.to_le_bytes())).1,
            Frame::Durable(id) => (b.push(3), b.extend(id.to_le_bytes())).1,
        }
        let n = (b.len() - 4) as u32;
        b[..4].copy_from_slice(&n.to_le_bytes());
        b.into()
    }

    /// Take one whole frame off the front of `buf`, if it has one.
    pub fn take(buf: &mut bytes::BytesMut) -> Result<Option<Frame>> {
        if buf.len() < 4 || buf.len() < 4 + u32::from_le_bytes(buf[..4].try_into()?) as usize {
            return Ok(None);
        }
        let n = u32::from_le_bytes(buf[..4].try_into()?) as usize;
        let mut f = buf.split_to(4 + n).freeze().slice(4..);
        let u64_ = |f: &mut Bytes| -> Result<u64> { Ok(u64::from_le_bytes(next(f, 8)?[..].try_into()?)) };
        Ok(Some(match next(&mut f, 1)?[0] {
            0 => Frame::Start { term: u64_(&mut f)?, replicated: next(&mut f, 1)?[0] == 1 },
            1 => {
                let id = u64_(&mut f)?;
                let puts = (0..count(&mut f)?).map(|_| Ok((String::from_utf8(field(&mut f)?.to_vec())?, field(&mut f)?))).collect::<Result<_>>()?;
                let deletes = (0..count(&mut f)?).map(|_| Ok(String::from_utf8(field(&mut f)?.to_vec())?)).collect::<Result<_>>()?;
                Frame::Change(Arc::new(Delta { id, puts, deletes }))
            }
            2 => Frame::Committed(u64_(&mut f)?),
            3 => Frame::Durable(u64_(&mut f)?),
            k => bail!("unknown frame kind {k}"),
        }))
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
    async fn writer(store: Store, object_store_cache_options: ObjectStoreCacheOptions) -> Result<Self> {
        // Poll object storage rarely when idle (that's an idle writer's request bill), but often
        // enough that compaction keeps up with the checkpoints.
        let compactor_options = Some(CompactorOptions { poll_interval: Duration::from_secs(5), ..Default::default() });
        let settings = Settings { flush_interval: Some(Duration::from_millis(1)), manifest_poll_interval: Duration::from_secs(10), l0_sst_size_bytes: 16 << 20, l0_max_ssts: 64, l0_max_ssts_per_key: 32, max_unflushed_bytes: 64 << 20, compactor_options, object_store_cache_options, ..Default::default() };
        let mut cat = Self::new(Db_::Writer(Db::builder("catalog", store).with_settings(settings).build().await?));
        cat.replicas = std::env::var("PONDRA_REPLICAS").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
        cat.last_n.store(cat.get::<u64>("n").await?.unwrap_or(1), Relaxed);
        let c = cat.get::<u64>("c").await?.unwrap_or(0);
        *cat.order.get_mut() = c + 1;
        let (tx, rx) = mpsc::unbounded_channel();
        (cat.writes, *cat.unstarted.get_mut().unwrap()) = (Some(tx), Some(rx));
        cat.committed.store(c, Relaxed);
        cat.durable.send_replace(c);
        cat.checkpoint().await?; // what we inherited (replayed from the WAL) into every node's view
        // The leader too reads the catalog from memory: everything committed, nothing in flight.
        let Db_::Writer(db) = &cat.db else { unreachable!() };
        let mut all = collect(db.scan(b"".to_vec()..b"d/".to_vec()).await.map_err(fatal)?).await?;
        all.extend(collect(db.scan(b"d0".to_vec()..vec![0xff]).await.map_err(fatal)?).await?);
        cat.overlay.get_mut().unwrap().extend(all.into_iter().map(|(k, v)| (k, (c, Some(v)))));
        cat.streamed.store(c, Relaxed);
        cat.mirror.store(true, Relaxed);
        Ok(cat)
    }

    async fn reader(store: Store, streamed: bool, object_store_cache_options: ObjectStoreCacheOptions) -> Result<Self> {
        let opts = DbReaderOptions { manifest_poll_interval: Duration::from_millis(250), skip_wal_replay: streamed, object_store_cache_options, ..Default::default() };
        // FollowLatest writes nothing, so readers work with read-only bucket credentials.
        let mut cat = Self::new(Db_::Reader(DbReader::open("catalog", store, DbReaderMode::FollowLatest, opts).await?));
        cat.follows = streamed;
        cat.refresh().await?; // (one try at the in-memory catalog before serving; later refreshes retry)
        Ok(cat)
    }

    fn new(db: Db_) -> Self {
        let (feed, order) = (broadcast::channel(4096).0, tokio::sync::Mutex::new(1));
        let (last_n, view, pruned, pins, streamed, hold, mirror, flushed, committed) = Default::default();
        let (durable, acked, acks, pending, unstarted) = (watch::Sender::new(0), watch::Sender::new(()), Default::default(), Default::default(), Default::default());
        Catalog { db, order, flushed, writes: None, unstarted, committed, durable, replicas: 1, acks, acked, pending, feed, recent: Default::default(), last_n, overlay: Default::default(), view, pruned, pins, streamed, hold, follows: false, mirror }
    }

    /// Leader: the recent frames plus a receiver for every frame from now on.
    pub fn subscribe(&self) -> (Vec<Frame>, broadcast::Receiver<Frame>) {
        let rx = self.feed.subscribe();
        (self.recent.lock().unwrap().0.iter().map(|(_, f)| f.clone()).collect(), rx)
    }

    /// Leader: stream a frame (and keep it for followers that connect later).
    fn send(&self, f: Frame) {
        let mut r = self.recent.lock().unwrap();
        r.1 += if let Frame::Change(d) = &f { d.bytes() } else { 0 };
        r.0.push_back((Instant::now(), f.clone()));
        while r.0.len() > 1 && r.0.front().is_some_and(|(t, _)| t.elapsed() > RECENT || r.1 > RECENT_BYTES) {
            if let Some((_, Frame::Change(old))) = r.0.pop_front() {
                r.1 -= old.bytes();
            }
        }
        drop(r);
        let _ = self.feed.send(f); // no followers listening is fine
    }

    /// Leader: a follower holds every change from `first` to `upto` (replicated mode).
    pub fn ack(&self, from: &str, first: u64, upto: u64) {
        self.acks.lock().unwrap().0.insert(from.to_string(), (first, upto));
        self.acked.send_replace(());
    }

    /// Leader: the followers whose copies count towards committing (listed in the catalog as
    /// "m", so a new leader knows whom to ask for them; see `replica::recover`).
    pub fn set_members(&self, members: Vec<String>) { self.acks.lock().unwrap().1 = members; }

    /// Leader: how many members hold change `id`.
    fn holders(&self, id: u64) -> usize {
        let a = self.acks.lock().unwrap();
        a.1.iter().filter(|m| a.0.get(*m).is_some_and(|&(first, upto)| (first..=upto).contains(&id))).count()
    }

    /// Leader: wait until write `id` is in the bucket.
    pub async fn wait_durable(&self, id: u64) {
        let _ = self.durable.subscribe().wait_for(|&d| d >= id).await;
    }

    /// Leader: the last committed write.
    pub fn committed(&self) -> u64 { self.committed.load(Relaxed) }

    fn apply(&self, d: &Delta) {
        let mut o = self.overlay.lock().unwrap();
        if d.id <= self.pruned.load(Relaxed) {
            return; // our own view has it, and newer changes to its keys may already be dropped
        }
        // A gap: commits between what we hold — the mirror, or the streamed commits plus our view
        // — and this one never arrived.
        let streamed = self.streamed.load(Relaxed);
        let covered = if self.mirror.load(Relaxed) { streamed } else { streamed.max(self.view.0.load(Relaxed)) };
        if d.id > covered + 1 {
            // Some commits never arrived. What we hold is no longer a prefix of the lake, so
            // drop it and read from our own view alone until the view has passed the gap.
            o.clear();
            self.mirror.store(false, Relaxed);
            self.hold.fetch_max(d.id - 1, Relaxed);
        }
        if let Some((_, v)) = d.puts.iter().find(|(k, _)| k == "n") {
            self.last_n.fetch_max(serde_json::from_slice(v).unwrap_or(1), Relaxed);
        }
        // (The leader reads inline data from its own store; everyone else keeps it until their
        // view has it. With the whole catalog here, a deleted key is simply gone.)
        let writer = self.is_writer();
        o.extend(d.puts.iter().filter(|(k, _)| !(writer && k.starts_with("d/"))).map(|(k, v)| (k.clone(), (d.id, Some(v.clone())))));
        match self.mirror.load(Relaxed) {
            true => d.deletes.iter().for_each(|k| drop(o.remove(k))),
            false => o.extend(d.deletes.iter().map(|k| (k.clone(), (d.id, None)))),
        }
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
        {
            let mut o = self.overlay.lock().unwrap();
            if self.mirror.load(Relaxed) {
                // The mirror answers every read but inline data without our view, so commits the
                // view already has must still arrive through the stream: `pruned` stays put.
                o.retain(|k, (id, _)| *id > upto || !k.starts_with("d/"));
            } else {
                self.pruned.fetch_max(upto, Relaxed);
                o.retain(|_, (id, _)| *id > upto);
            }
        }
        if self.follows && !self.mirror.load(Relaxed) && self.hold.load(Relaxed) <= c {
            self.seed().await?;
        }
        Ok(())
    }

    /// Follower: load the whole catalog, but inline segment data, into memory. From then on the
    /// commit stream keeps it current and reads are answered from memory, as of the last streamed
    /// commit: the bucket is out of the query path. (Redone after a gap in the stream.)
    async fn seed(&self) -> Result<()> {
        let Db_::Reader(r) = &self.db else { return Ok(()) };
        let _pin = self.pin(); // (the streamed changes after `before` stay while we read)
        let before = self.view_now().await?;
        let mut all = collect(r.scan(b"".to_vec()..b"d/".to_vec()).await?).await?;
        all.extend(collect(r.scan(b"d0".to_vec()..vec![0xff]).await?).await?);
        let after = self.view_now().await?;
        let n = all.get("n").map_or(Ok(1), |v| serde_json::from_slice(v))?;
        let mut o = self.overlay.lock().unwrap();
        // One consistent picture: the scan saw one view, or the streamed commits we hold cover
        // every change it may have picked up after `before` (they win over it, below). A gap
        // after `before`, or neither: try again at the next refresh (one try each, never a loop
        // that a busy lake on slow storage could keep failing).
        let c = before;
        let covered = after <= self.streamed.load(Relaxed);
        if self.hold.load(Relaxed) > c || self.mirror.load(Relaxed) || (after != before && !covered) {
            return Ok(());
        }
        // Streamed changes newer than the snapshot win; everything older gives way to it.
        o.retain(|k, (id, _)| *id > c || k.starts_with("d/"));
        for (k, v) in all {
            o.entry(k).or_insert((c, Some(v)));
        }
        o.retain(|_, (_, v)| v.is_some()); // (deleted keys: absent from the whole catalog)
        self.pruned.fetch_max(c, Relaxed); // older streamed commits are in the snapshot
        self.streamed.fetch_max(c, Relaxed);
        self.last_n.fetch_max(n, Relaxed);
        self.mirror.store(true, Relaxed);
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
        if self.mirror.load(Relaxed) {
            return self.last_n.load(Relaxed); // the mirror is the lake as of the last streamed commit
        }
        match (&self.db, self.overlaid()) {
            (Db_::Reader(_), true) => self.view.1.load(Relaxed).max(self.last_n.load(Relaxed)),
            (Db_::Reader(_), false) => self.view.1.load(Relaxed),
            (Db_::Writer(_), _) => self.last_n.load(Relaxed),
        }
    }

    pub fn is_writer(&self) -> bool { matches!(self.db, Db_::Writer(_)) }

    /// The catalog version every read here reflects right now — on the leader (durable commits)
    /// or a node that holds the whole catalog in memory — or `None` where reads mix our view with
    /// the stream. A read that starts after this sees at least this version (never older).
    pub fn version(&self) -> Option<u64> {
        match &self.db {
            Db_::Writer(_) => Some(self.committed.load(Relaxed)),
            Db_::Reader(_) => self.mirror.load(Relaxed).then(|| self.streamed.load(Relaxed)),
        }
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.get_raw(key).await?.map(|v| serde_json::from_slice(&v).context(key.to_string())).transpose()
    }

    pub async fn get_raw(&self, key: &str) -> Result<Option<Bytes>> {
        Ok(match &self.db {
            // The leader answers from its in-memory catalog: committed writes only, never ones
            // in flight. Inline segment data is only asked for once its segment is committed.
            Db_::Writer(db) if key.starts_with("d/") => db.get(key).await.map_err(fatal)?,
            Db_::Writer(db) => match self.mirror.load(Relaxed) {
                true => self.overlay.lock().unwrap().get(key).and_then(|(_, v)| v.clone()),
                false => db.get_with_options(key, &ReadOptions::new().with_durability_filter(DurabilityLevel::Remote)).await.map_err(fatal)?,
            },
            Db_::Reader(r) => {
                let inline = key.starts_with("d/");
                {
                    let o = self.overlay.lock().unwrap();
                    if self.mirror.load(Relaxed) && !inline {
                        return Ok(o.get(key).and_then(|(_, v)| v.clone())); // the in-memory catalog
                    }
                }
                // A streamed copy is newer than our view's (unless the view is ahead of the
                // stream); inline segment data never changes, so its streamed copy is always good.
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
            Db_::Writer(db) if !self.mirror.load(Relaxed) => return decode(collect(db.scan_with_options(range, &ScanOptions::new().with_durability_filter(DurabilityLevel::Remote)).await.map_err(fatal)?).await?),
            Db_::Writer(_) => None,
            Db_::Reader(r) => Some(r),
        };
        {
            let o = self.overlay.lock().unwrap();
            if self.mirror.load(Relaxed) {
                // The in-memory catalog (scans never cover inline segment data).
                let hits = o.range(from.to_string()..to.to_string()).filter_map(|(k, (_, v))| Some((k.clone(), v.clone()?)));
                return decode(hits.collect());
            }
        }
        let (r, _pin) = (r.expect("a reader: the leader reads its in-memory catalog above"), self.pin());
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

    /// Leader: persist the catalog memtable if anything was committed since the last time. A
    /// restart then replays only the WAL written since, and the other nodes' views — which read
    /// no WAL — catch up (a follower whose commit stream broke reads from its view until then).
    pub async fn checkpoint(&self) -> Result<()> {
        let Db_::Writer(db) = &self.db else { return Ok(()) };
        let next = *self.order.lock().await; // every commit before it is in the memtable
        if self.flushed.load(Relaxed) != next {
            db.flush_with_options(FlushOptions { flush_type: FlushType::MemTable }).await?;
            self.flushed.store(next, Relaxed);
        }
        Ok(())
    }

    /// Write puts and deletes atomically and wait until committed (see `Lake::commits`).
    pub async fn commit(&self, puts: Vec<(String, Vec<u8>)>, deletes: &[String]) -> Result<()> {
        Ok(self.write(puts, deletes).await?.await?)
    }

    /// Write puts and deletes atomically. The returned receiver fires once the write is committed;
    /// writes commit in the order they were made, so a caller can make the next write without
    /// waiting (pipelined commits: one round trip no longer blocks the next).
    pub async fn write(&self, puts: Vec<(String, Vec<u8>)>, deletes: &[String]) -> Result<oneshot::Receiver<()>> {
        let (Db_::Writer(db), Some(writes)) = (&self.db, &self.writes) else { bail!("read-only node") };
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
        let _ = writes.send((Arc::new(handle), Arc::new(Delta { id: *id, puts, deletes: deletes.to_vec() }), done));
        *id += 1;
        Ok(rx)
    }
}


/// A writer that was fenced out (a new leader took over) must stop at once: it restarts and
/// rejoins the cluster as a follower.
fn fatal(e: slatedb::Error) -> anyhow::Error {
    if matches!(e.kind(), ErrorKind::Closed(_)) {
        eprintln!("catalog closed: {e}");
        crate::cluster::restart("the catalog was closed (fenced by a newer leader)");
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

/// How many partitions a query runs in here: one per core (`PONDRA_CORES` stands for another
/// machine's count), at least 2, so every node plans aggregations as partial + final (see
/// spmd.rs), and at most one per 24 MB of query memory. Each partition's sort keeps 10 MB aside to
/// merge what it spilled (DataFusion's default); on 4 cores with 50 MB the reserves took the
/// budget and the merge above them failed, and smaller reserves can't merge at all.
pub fn partitions() -> usize {
    let cores = std::env::var("PONDRA_CORES").ok().and_then(|p| p.parse().ok()).unwrap_or_else(|| std::thread::available_parallelism().map_or(2, |n| n.get()));
    cores.min(memory_limit() / (24 << 20)).max(2)
}
