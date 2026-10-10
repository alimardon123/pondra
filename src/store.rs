//! The lake: object storage (a local dir or s3://bucket/prefix on S3, R2, MinIO…) with the
//! catalog (SlateDB) inside it. Nothing else holds state.
//!
//! The leader is the catalog's only writer. It also streams every commit to the followers the
//! moment it is durable, and they lay those changes over their own (slightly behind) view of the
//! catalog: every node sees a commit within milliseconds, while the bucket stays the source of truth.
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::ClientConfigKey;
use object_store::{local::LocalFileSystem, path::Path, prefix::PrefixStore, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use slatedb::config::{CompactorOptions, DbReaderOptions, GarbageCollectorDirectoryOptions, GarbageCollectorOptions, DurabilityLevel, FlushOptions, FlushType, ObjectStoreCacheOptions, ReadOptions, ScanOptions, Settings};
use slatedb::{Db, DbReader, DbReaderMode, ErrorKind, WriteBatch, WriteHandle};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

pub type Store = Arc<dyn ObjectStore>;

/// A user's file, next to the tables (`PUT /files`): the one kind of lake object that may be
/// replaced or removed, so no cache keeps it (ADR-034). Everything else is written once.
pub fn replaceable(path: &str) -> bool { path.starts_with("files/") }

fn version_of(m: &object_store::ObjectMeta) -> String {
    match &m.e_tag {
        Some(e) => format!("{}:{}", e.trim_matches('"'), m.size),
        None => format!("@{}:{}", m.last_modified.timestamp_micros(), m.size),
    }
}

/// A file replaced since the version an editor read (`If-Match`): `PUT /files` answers 412.
#[derive(Debug)]
pub struct Changed;
impl std::fmt::Display for Changed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "the file changed since it was opened: open it again, or save under another name") }
}
impl std::error::Error for Changed {}

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
    /// The commit that last put rows into its files (tiered from the log, or a file commit): with
    /// its purges, what tells a version that changed its rows from one that only rewrote its files
    /// (`iceberg::publish`: a `replace`, which writers' conflict checks pass over).
    #[serde(default)]
    pub rows_at: u64,
    /// An upsert table whose older versions are positions (`tier::shadow`): each round since it
    /// held one generation of files (made, or compacted), so every generation it has is published.
    #[serde(default)]
    pub shadows: bool,
    /// The position-delete files of replaced files, and when: deleted after the retention period
    /// too, unless another of the table's files still names one (a delete file may name several).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub garbage_deletes: Vec<(String, u64)>,
    /// Those files' records, while they are kept: a writer that read the table before they were
    /// replaced names them, and its change is carried over by their rows' ids (`adopt::carry`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replaced: Vec<DataFile>,
    #[serde(default)]
    pub publish: Vec<String>, // open formats other engines also read it in: "delta", "iceberg"
    #[serde(default)]
    pub cluster: Vec<String>, // append tables: each file's rows sorted by these (see `tier::clustered`)
    #[serde(default)]
    pub ttl: Option<(String, u64)>, // keyed tables: a row whose (timestamp) column is older than this many seconds is gone
    #[serde(default)]
    pub partition: Option<String>, // append tables: every file holds one value of this ("col", "day(col)", "hour(col)", "month(col)")
    /// Keyed tables: of a key's rows, the one with the greatest value of this column is current
    /// (its event time: a late row doesn't replace a newer one), not the one that came last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sketch: BTreeMap<String, String>, // append tables: each key-like column's distinct values, sketched (`sketch.rs`)
    #[serde(default)]
    pub ids: bool, // every row has its system columns (`sys.rs`): tables made from round 19 on
    #[serde(default)]
    pub changed: bool, // append tables: UPDATE, DELETE or MERGE has replaced rows (`{t}$deleted` holds the old ones)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub purges: Vec<(u64, u64)>, // changed tables: (commit, when): the old rows of every change up to it are out of the files (`tier::purge`)
    /// Columns renamed since they were first written: the name in the files and the log (what
    /// `columns` says) -> the name SQL knows it by. Files are never rewritten for a rename.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub names: BTreeMap<String, String>,
    /// Columns dropped (their stored names): older files still hold them; nothing reads them again.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped: Vec<String>,
    /// Where its files, manifests and Delta and Iceberg copies are: `data/{folder}/`. None: its
    /// name, as for every table before round 26; a renamed table keeps the folder it had
    /// (`ALTER TABLE … RENAME TO`, ADR-030).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,
    /// Columns a write must give a value (`NOT NULL`; a key's columns are too, for tables made
    /// from round 26 on), by stored name (`defaults.rs`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub not_null: Vec<String>,
    /// A column's value when a write leaves it out (`DEFAULT expr`): stored name -> the SQL
    /// expression, worked out for each row as it is written (`defaults.rs`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub defaults: BTreeMap<String, String>,
    /// `CHECK` constraints (ADR-036 §3): (name, condition over the columns by their SQL names),
    /// which every row written must not make false (`defaults.rs`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<(String, String)>,
    /// Identity columns (by stored name): each numbered by a sequence it owns, which its default
    /// calls (`seq.rs`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub identity: BTreeMap<String, crate::seq::Identity>,
    /// UNIQUE, and PRIMARY KEY and FOREIGN KEY said NOT ENFORCED, by stored column names
    /// (`constraints.rs`, ADR-057): an enforced UNIQUE has every write checked on the leader.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<crate::constraints::Constraint>,
    /// Enum columns, by stored name: their labels, and the type they are of (`types.rs`). Every
    /// door refuses a value its column's labels don't list.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub enums: BTreeMap<String, crate::types::Enum>,
    /// Not the lake's: files outside it a query reads as a table (`ext.rs`), never in the catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<crate::ext::Spec>,
    /// Another engine's table (`scan.rs`): what matches its files' columns to its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outside: Option<crate::scan::Table>,
    /// Other engines' properties of the table (Iceberg's: `write.delete.mode`, say), kept and
    /// published for them; Pondra's own options are fields above.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
    /// A history view's table (SCD type 2, ADR-036 §8): every version of each key as it came;
    /// reads give each its `__start_at` and `__end_at` (`views::history_view`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<crate::views::History>,
    /// `retention = '7 days'` (ADR-043): how long the table's past is kept (`ddl::KEEP_MS` when not
    /// set): read `AT (…)` (`past.rs`), and undropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_secs: Option<u64>,
    /// From which commit (and time, ms) on the table can be read as it was: a purge let the old
    /// versions of earlier changes go (`tier::purge`). (0, 0): all of it; None: changed before
    /// this was kept, and whole only from its oldest purge kept (`past::kept_since`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub past_from: Option<(u64, u64)>,
    /// A clone's (`CREATE TABLE c CLONE t`, ADR-043): the folders of other tables whose files it
    /// lists too. Files there go only by the orphan sweep, which counts every table listing them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shares: Vec<String>,
}

impl TableMeta {
    /// The changes whose old rows are out of the table's files: reads skip their `{t}$deleted` rows.
    pub fn purged(&self) -> u64 { self.purges.last().map_or(0, |p| p.0) }

    /// Is this column an upsert table's delete markers (`_deleted`)? Others never see it: published,
    /// its rows are positions (`tier::shadow`), and Iceberg keeps the name for its own.
    pub fn marker(&self, column: &str) -> bool { column == "_deleted" && !self.key.is_empty() }

    /// Has any column been renamed or dropped (ADR-022)? If not, SQL sees `columns` as stored.
    pub fn mapped(&self) -> bool { !self.names.is_empty() || !self.dropped.is_empty() }

    /// A stored column's name in SQL.
    pub fn name_of<'a>(&'a self, stored: &'a str) -> &'a str { self.names.get(stored).map_or(stored, String::as_str) }

    /// The folder under `data/` its files are in (`name`'s, unless it was renamed).
    pub fn folder<'a>(&'a self, name: &'a str) -> &'a str { self.folder.as_deref().unwrap_or(name) }

    /// The columns SQL sees, as (stored name, name in SQL, type): all but the dropped.
    pub fn live(&self) -> impl Iterator<Item = (&str, &str, &str)> {
        self.columns.iter().filter(|(c, _)| !self.dropped.contains(c)).map(|(c, t)| (c.as_str(), self.name_of(c), t.as_str()))
    }

    /// The column SQL calls `name`: its stored name.
    pub fn stored(&self, name: &str) -> Option<&str> { self.live().find(|(_, n, _)| *n == name).map(|(s, _, _)| s) }

    /// The table as SQL sees it: columns (and key, clustering, merges, partition, TTL) under their
    /// SQL names, dropped ones gone. Everything that works on SQL names uses this; storage keeps
    /// `self`.
    pub fn logical(&self) -> TableMeta {
        if !self.mapped() {
            return self.clone();
        }
        let n = |c: &String| self.name_of(c).to_string();
        let partition = self.partition.as_ref().map(|p| match p.split_once('(') {
            Some((f, c)) => format!("{f}({})", self.name_of(c.trim_end_matches(')'))),
            None => self.name_of(p).to_string(),
        });
        TableMeta {
            columns: self.live().map(|(_, n, t)| (n.to_string(), t.to_string())).collect(),
            key: self.key.iter().map(n).collect(),
            merge: self.merge.iter().map(|(c, f)| (n(c), f.clone())).collect(),
            cluster: self.cluster.iter().map(n).collect(),
            ttl: self.ttl.as_ref().map(|(c, s)| (n(c), *s)),
            order: self.order.as_ref().map(n),
            partition,
            not_null: self.not_null.iter().filter(|c| !self.dropped.contains(c)).map(n).collect(),
            defaults: self.defaults.iter().filter(|(c, _)| !self.dropped.contains(c)).map(|(c, e)| (n(c), e.clone())).collect(),
            identity: self.identity.iter().filter(|(c, _)| !self.dropped.contains(c)).map(|(c, i)| (n(c), i.clone())).collect(),
            constraints: self.constraints.iter().map(|c| c.named(&n)).collect(),
            enums: self.enums.iter().filter(|(c, _)| !self.dropped.contains(c)).map(|(c, e)| (n(c), e.clone())).collect(),
            names: BTreeMap::new(),
            dropped: vec![],
            ..self.clone()
        }
    }

    /// `b` (columns by their SQL names) as stored: renamed columns under their stored names. A
    /// column under a name SQL no longer knows (a writer from before a rename or drop) is left
    /// out, never taken for the column now stored under it. Every writer into the log gives SQL's
    /// names (`log::pack` calls this).
    pub fn to_stored(&self, b: &RecordBatch) -> Result<RecordBatch> {
        if !self.mapped() {
            return Ok(b.clone());
        }
        let (mut fields, mut columns) = (vec![], vec![]);
        for (f, c) in b.schema().fields().iter().zip(b.columns()) {
            let name = match self.stored(f.name()) {
                Some(s) => s,
                None if self.columns.iter().any(|(n, _)| n == f.name()) => continue, // (an old name)
                None => f.name(), // (not the table's: reads leave it out)
            };
            fields.push(Arc::new(f.as_ref().clone().with_name(name)));
            columns.push(c.clone());
        }
        let options = datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(b.num_rows()));
        Ok(RecordBatch::try_new_with_options(Arc::new(Schema::new_with_metadata(fields, b.schema().metadata().clone())), columns, &options)?)
    }

    /// `b` (as stored) under the names SQL knows its columns by, dropped columns left out.
    pub fn to_logical(&self, b: &RecordBatch) -> Result<RecordBatch> {
        if !self.mapped() {
            return Ok(b.clone());
        }
        let keep: Vec<usize> = (0..b.num_columns()).filter(|&i| !self.dropped.contains(b.schema().field(i).name())).collect();
        let b = b.project(&keep)?;
        let fields: Vec<_> = b.schema().fields().iter().map(|f| Arc::new(f.as_ref().clone().with_name(self.name_of(f.name())))).collect();
        Ok(RecordBatch::try_new(Arc::new(Schema::new_with_metadata(fields, b.schema().metadata().clone())), b.columns().to_vec())?)
    }

    /// The TTL as a SQL condition that keeps live rows ("" if none).
    pub fn ttl_sql(&self) -> String {
        self.ttl.as_ref().map(|(c, s)| format!("\"{c}\" >= now() - INTERVAL '{s} seconds'")).unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
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
    /// Another engine's table's file (`scan.rs`): its partition values and deletes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outside: Option<Box<crate::scan::Outside>>,
    /// A file recorded as another engine wrote it (ADR-029 §1): its rows' system columns, which it
    /// doesn't hold, from here (`scan::adopted`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Lineage>,
    /// Rows deleted from it by position (ADR-029 §4), in Iceberg's position-delete files (paths in
    /// the lake) that name it: another engine's merge-on-read change, or Pondra's own changes
    /// (`tier::positions`). Reads skip them without decoding them (`scan::lake_files`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletes: Vec<crate::scan::Delete>,
    /// How many of its rows those delete (a file mostly deleted is rewritten: `tier::maintain`).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub deleted: u64,
}

fn is_zero(n: &u64) -> bool { *n == 0 }

impl TableMeta {
    /// Files replaced or taken out: they go after the retention period, and their position-delete
    /// files with them (`tier::expire`).
    pub fn discard(&mut self, files: &[DataFile]) {
        let now = crate::log::now_ms();
        self.garbage.extend(files.iter().map(|f| (f.path.clone(), now)));
        self.garbage_deletes.extend(files.iter().flat_map(|f| f.delete_files()).map(|p| (p.clone(), now)));
        self.replaced.extend(files.iter().map(|f| DataFile { stats: Default::default(), sketch: Default::default(), nulls: None, ..f.clone() }));
    }
}

impl DataFile {
    /// The position-delete files that name it (paths in the lake).
    pub fn delete_files(&self) -> impl Iterator<Item = &String> {
        self.deletes.iter().filter_map(|d| match d {
            crate::scan::Delete::Positions { path, .. } => Some(path),
            _ => None,
        })
    }
}

/// Where a file's rows' system columns come from when it doesn't hold them: its first row's id
/// (the next rows' follow, by their place in the file), and the commit that recorded it (their
/// `_version`) and its time (`_created_at`, `_updated_at`). Iceberg v3's row lineage and Delta's
/// row tracking give a row its id the same way.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct Lineage {
    pub first: i64,
    pub version: u64,
    pub ms: u64,
}

/// One log segment = one node's flush, holding rows for many tables. Small segments are stored
/// inside the catalog write itself (one round trip to object storage per flush); big ones as an
/// object written by the node that received the rows.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Segment {
    pub path: String,                                // empty = inline, under data_key(seg)
    pub parts: BTreeMap<String, Vec<(u64, u64, u64)>>, // table -> (offset, length, rows) of Arrow IPC streams
    pub ts_ms: u64,
    /// A file commit (ADR-029 §7): the files it put into tables and took out of them, whose rows
    /// the log's readers read from the files — the change feed, Kafka topics, `/watch`, tasks.
    /// Queries and tiering don't: the files are the tables' own already.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, Filed>,
}

/// One table's files in a file commit, as they were then.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Filed {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<DataFile>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<DataFile>,
    /// Files it deleted rows of by position: each as it was, with the deletes it took.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted: Vec<(DataFile, Vec<crate::scan::Delete>)>,
}

impl Segment {
    /// Rows of `table` this segment gives the log's readers: its parts', then its files'.
    pub fn rows_of(&self, table: &str) -> u64 {
        let parts = self.parts.get(table).map_or(0, |p| p.iter().map(|x| x.2).sum::<u64>());
        parts + self.files.get(table).map_or(0, |f| f.added.iter().map(|d| d.rows).sum::<u64>())
    }
}

pub fn table_key(t: &str) -> String { format!("t/{t}") }

/// The folder table `t`'s files are in, under `data/` (its name, unless it was renamed).
pub async fn folder_of(lake: &Lake, t: &str) -> Result<String> {
    Ok(lake.cat.get::<TableMeta>(&table_key(t)).await?.and_then(|m| m.folder).unwrap_or_else(|| t.to_string()))
}
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
        None => space(&dir).map_or(20 << 30, |(free, _)| (free / 4).min(20 << 30)),
    };
    let dir = dir.join(url.trim_start_matches("s3://").replace(['/', ':', '\\'], "_")); // one folder per lake
    (bytes > 0).then(|| crate::cache::Disk::open(dir, bytes, store.clone()).ok()).flatten()
}

/// Free and total bytes of the disk that holds `dir` (or its nearest existing parent), where the
/// platform says.
fn space(dir: &std::path::Path) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        let at = std::ffi::CString::new(dir.ancestors().find(|a| a.exists())?.as_os_str().as_encoded_bytes()).ok()?;
        let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
        (unsafe { libc::statvfs(at.as_ptr(), &mut s) } == 0).then(|| (s.f_bavail as u64 * s.f_frsize as u64, s.f_blocks as u64 * s.f_frsize as u64))
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        None
    }
}

/// Room a lake on local disk keeps for its own upkeep: a quarter of the disk, 256 MB at most.
/// Commits wait while less is free (`log::Sequencer`), so the catalog can still flush and
/// tiering write the files that let the log go: a disk the lake filled to its last byte had
/// room for neither, and kept a log that never drained.
pub fn short_of_room(dir: &std::path::Path) -> Option<(u64, u64)> {
    space(dir).map(|(free, total)| (free, (total / 4).min(256 << 20))).filter(|(free, keep)| free < keep)
}

pub struct Lake {
    pub url: String, // absolute local dir or "s3://bucket/prefix" (gs://, az://, abfss:// too)
    pub store: Store,
    pub cat: Catalog,
    pub hwm: watch::Sender<u64>, // the last committed segment this node knows of
    pub backlog: std::sync::atomic::AtomicU64, // leader: rows in the log not yet tiered (all tables)
    pub rt: Arc<RuntimeEnv>,         // shared by all queries: object store registry + Parquet metadata cache
    tail: Mutex<Tail>, // decoded log rows
    pub tails: Mutex<(lru::LruCache<String, Arc<crate::query::Tail>>, usize)>, // tables' tails as queries read them (`query::Tail`); total bytes
    pub disk: Option<Arc<crate::cache::Disk>>, // lakes on object storage: the local SSD tier
    pub groups: crate::serve::Groups,          // decoded row groups for key lookups
    pub hot: Arc<crate::hot::Hot>,             // decoded columns of files queries read lately
    pub attached: std::sync::RwLock<Vec<(String, Arc<Lake>)>>, // other lakes, read as `name.table` (`--attach`)
    cached: Option<Arc<crate::cache::CachedStore>>, // what DataFusion reads the bucket through
    pub ids: crate::sys::Ids, // the row ids this process stamps rows with (a block reserved from the leader)
    pub caught: watch::Sender<bool>, // false while a node that just started catches up with its leader (`caught_up`)
    pub to: std::sync::OnceLock<crate::log::To>, // where this process's flushes and sequences' values go: its sequencer, or the leader's
    pub sequences: tokio::sync::Mutex<HashMap<String, crate::seq::Block>>, // the sequences' values this node hands out (`seq::next`)
    bases: std::sync::RwLock<BTreeMap<String, (String, Option<Store>)>>, // a branch's bases (ADR-047): id -> place, and its store once read
    sessions: Mutex<std::collections::HashMap<usize, datafusion::execution::SessionState>>, // (`session_with`'s, made once a partition count)
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

/// The machine's memory, if it says: `/proc` on Linux, the OS's own call elsewhere. Less when a
/// cgroup limits this process (a container, a service with `MemoryMax`): `/proc/meminfo` is the
/// host's, and a node in a 4 GB container on a 64 GB host sized its queries for 64 and was killed.
pub fn ram() -> Option<usize> {
    static RAM: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *RAM.get_or_init(read_ram)
}

fn read_ram() -> Option<usize> {
    let proc = std::fs::read_to_string("/proc/meminfo").ok().and_then(|m| m.lines().find_map(|l| l.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?.trim().parse::<usize>().ok()));
    proc.map(|kb| (kb << 10).min(cgroup_memory().unwrap_or(usize::MAX))).or_else(|| {
        let s = sysinfo::System::new_with_specifics(sysinfo::RefreshKind::nothing().with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()));
        Some(s.total_memory() as usize).filter(|&b| b > 0)
    })
}

/// The least memory limit of this process's cgroups and their parents (v2 `memory.max`, v1
/// `memory.limit_in_bytes`; "max" or none: no limit). In a container the cgroup is its root.
fn cgroup_memory() -> Option<usize> {
    let groups = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let limits = groups.lines().filter_map(|line| {
        let (controllers, path) = line.split_once(':')?.1.split_once(':')?; // "0::/path" (v2), "4:memory:/path" (v1)
        let (base, file) = match controllers {
            "" => ("/sys/fs/cgroup", "memory.max"),
            c if c.split(',').any(|c| c == "memory") => ("/sys/fs/cgroup/memory", "memory.limit_in_bytes"),
            _ => return None,
        };
        let read = |dir: &std::path::Path| std::fs::read_to_string(format!("{base}{}/{file}", dir.display())).ok()?.trim().parse::<usize>().ok();
        std::path::Path::new(path).ancestors().filter_map(read).min()
    });
    limits.min()
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

/// Decoded log rows, each table's by segment, `TAIL_BYTES` at most. A table's log is read only
/// after its `tiered` mark (a read that began before reads its own from the store), so its rows up
/// to the mark go as soon as a read finds it moved, and past the limit the oldest segment goes.
/// Kept by recent use instead, every node held 256 MB of rows long since in files.
#[derive(Default)]
struct Tail {
    rows: std::collections::HashMap<String, BTreeMap<u64, Rows>>,
    bytes: usize,
}

impl Tail {
    fn size(rows: &Rows) -> usize { rows.iter().map(|b| b.get_array_memory_size()).sum() }

    fn forget(&mut self, table: &str, upto: u64) {
        let Some(t) = self.rows.get_mut(table) else { return };
        let kept = t.split_off(&upto.saturating_add(1));
        self.bytes -= std::mem::replace(t, kept).values().map(Self::size).sum::<usize>();
        if t.is_empty() {
            self.rows.remove(table);
        }
    }

    fn keep(&mut self, table: &str, n: u64, rows: Rows) {
        self.bytes += Self::size(&rows);
        if let Some(old) = self.rows.entry(table.to_string()).or_default().insert(n, rows) {
            self.bytes -= Self::size(&old);
        }
        while self.bytes > TAIL_BYTES {
            let Some((first, oldest)) = self.rows.iter().filter_map(|(t, r)| Some((*r.keys().next()?, t.clone()))).min() else { break };
            self.forget(&oldest, first);
        }
    }
}

/// Open a lake's object store. For a bucket (`s3://`, `gs://`, `az://`, `abfss://`), also returns
/// the bucket-level client that query scans use (DataFusion addresses objects by their full path
/// in the bucket). Credentials and endpoints come from the environment, as each cloud's own tools
/// read them: `AWS_*` (`AWS_ENDPOINT` for R2 and MinIO), `GOOGLE_*`, `AZURE_*`.
pub fn open_store(url: &str) -> Result<(String, Store, Option<(String, Store)>)> {
    let Some((scheme, rest)) = url.split_once("://").filter(|(s, _)| ["s3", "gs", "az", "abfs", "abfss"].contains(s)) else {
        std::fs::create_dir_all(url)?;
        let dir = std::fs::canonicalize(url)?.to_string_lossy().trim_start_matches(r"\\?\").to_string(); // (Windows verbatim prefix)
        return Ok((dir.clone(), Arc::new(Counted(Arc::new(LocalFileSystem::new_with_prefix(&dir)?))), None));
    };
    let (bucket, prefix) = rest.trim_end_matches('/').split_once('/').unwrap_or((rest, ""));
    let root = format!("{scheme}://{bucket}");
    // Idle connections are dropped after 15 s rather than reused: through proxies and NATs that
    // silently forget idle connections, a reused one hung a PUT for the full 30 s timeout on R2.
    let (idle, after) = (ClientConfigKey::PoolIdleTimeout, "15s");
    // Every request to the bucket takes a turn of its budget, retries too (`budget.rs`, C5).
    let turns = || crate::budget::Budget::of(&root);
    let whole: Store = match scheme {
        "s3" => Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).with_allow_http(true).with_config(AmazonS3ConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
        "gs" => Arc::new(GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket).with_config(GoogleConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
        _ => Arc::new(MicrosoftAzureBuilder::from_env().with_url(&root).with_config(AzureConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
    };
    let store: Store = if prefix.is_empty() { whole.clone() } else { Arc::new(PrefixStore::new(whole.clone(), prefix)) };
    Ok((url.trim_end_matches('/').to_string(), Arc::new(Counted(store)), Some((root, whole))))
}

/// A lake's bucket, as DataFusion names its store (`s3://bucket`), or None for a folder.
pub fn bucket_url(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    Some(format!("{scheme}://{}", rest.split('/').next().unwrap_or_default()))
}

/// A lake's store, counting the requests that cost the most and meet a bucket's rate limits
/// (S3: 3,500 writes a second per prefix): writes, lists and deletes (`GET /metrics`).
#[derive(Debug)]
struct Counted(Store);

impl std::fmt::Display for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}

#[async_trait::async_trait]
impl ObjectStore for Counted {
    async fn put_opts(&self, at: &Path, body: object_store::PutPayload, opts: PutOptions) -> object_store::Result<object_store::PutResult> {
        let r = self.0.put_opts(at, body, opts).await;
        if !matches!(r, Err(object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented { .. })) {
            crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1); // (a local disk refuses SlateDB's tagged PUT before writing; it tries again untagged)
        }
        r
    }
    async fn put_multipart_opts(&self, at: &Path, opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1);
        self.0.put_multipart_opts(at, opts).await
    }
    async fn get_opts(&self, at: &Path, opts: object_store::GetOptions) -> object_store::Result<object_store::GetResult> { self.0.get_opts(at, opts).await }
    fn delete_stream(&self, at: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        use futures::StreamExt;
        self.0.delete_stream(at.inspect(|_| crate::metrics::add(&crate::metrics::OBJECT_DELETES, 1)).boxed())
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        crate::metrics::add(&crate::metrics::OBJECT_LISTS, 1);
        self.0.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
        crate::metrics::add(&crate::metrics::OBJECT_LISTS, 1);
        self.0.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: object_store::CopyOptions) -> object_store::Result<()> {
        crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1);
        self.0.copy_opts(from, to, opts).await
    }
}

impl Lake {
    /// `writer`: the leader. `streamed`: a follower, which also gets every commit streamed from
    /// the leader, so its own catalog view only needs the leader's checkpoints (no log replay).
    /// What opening a folder with no catalog says (a follower of a leader still making one waits).
    pub const NO_LAKE: &'static str = "holds no lake yet";

    pub async fn open(url: &str, writer: bool, streamed: bool) -> Result<Arc<Lake>> {
        let (url, store, bucket) = open_store(url)?;
        let local = bucket.is_none();
        let pool = Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(memory_limit()));
        // (spills go to the OS temp dir; no listing kept past its statement: DataFusion's own
        // cache of them would hide a file added to a folder outside the lake, ADR-026)
        let caches = datafusion::execution::cache::cache_manager::CacheManagerConfig::default().with_list_files_cache_limit(0);
        let rt = RuntimeEnvBuilder::new().with_memory_pool(pool).with_cache_manager(caches).build_arc()?;
        let disk = bucket.as_ref().and_then(|_| disk_tier(&url, &store));
        let mut cached_store = None;
        if let Some((bucket_url, s3)) = bucket {
            let prefix = url.trim_start_matches(&bucket_url).trim_start_matches('/').to_string();
            let cached = Arc::new(crate::cache::CachedStore::new(s3, cache_mb() << 20, disk.clone().map(|d| (d, prefix.clone())), &prefix));
            rt.register_object_store(&url::Url::parse(&bucket_url)?, cached.clone());
            cached_store = Some(cached);
        }
        // The catalog's own files go on the SSD tier too (next to the lake's objects), so catalog
        // reads — the leader's, a new node's, a reader's without a live leader — are local.
        let cache = disk.as_ref().map(|d| d.dir.with_extension("catalog"));
        let cache = ObjectStoreCacheOptions { root_folder: cache, max_cache_size_bytes: Some(2 << 30), cache_on_flush: true, cache_on_compaction: true, ..Default::default() };
        let cat = match writer {
            true => Catalog::writer(store.clone(), cache, local).await?,
            false => match Catalog::opened(store.clone(), streamed, cache).await {
                Ok(cat) => cat,
                // (no catalog at all: say so, rather than the database's own words for it)
                Err(e) => match futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from("catalog/manifest")))).await {
                    None => anyhow::bail!("{url} {}: start one there (`pondra {url}`, or `pondra serve --dir {url}`), or make a table in it", Lake::NO_LAKE),
                    Some(_) => return Err(e),
                },
            },
        };
        crate::format::check(&cat, &url, writer).await?; // (a lake a newer Pondra wrote: refused before its tables are read, ADR-039)
        let hwm = watch::Sender::new(cat.get::<u64>("n").await?.unwrap_or(1) - 1);
        let lake = Arc::new_cyclic(|me| Lake { url, store, cat, hwm, backlog: Default::default(), rt, tail: Default::default(), tails: Mutex::new((lru::LruCache::unbounded(), 0)), disk, groups: crate::serve::Groups::new(cache_mb() << 19), hot: Arc::new(crate::hot::Hot::new()), attached: Default::default(), ids: Default::default(), cached: cached_store, caught: watch::channel(true).0, to: Default::default(), sequences: Default::default(), bases: Default::default(), sessions: Default::default(), me: me.clone() });
        lake.hot.watch(); // the decoded columns give memory back when the node needs it
        crate::branch::load(&lake).await?; // (a branch's files listed in its bases: ADR-047)
        if let Some(writes) = lake.cat.unstarted.lock().unwrap().take() {
            crate::panics::spawn(lake.clone().commits(writes));
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
        crate::panics::spawn(async move {
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
        self.arrived(&d);
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

    /// A commit seen here. The recent lake is kept on this node's SSD: every object it brings in (a
    /// log segment another node wrote, a new Parquet file) is fetched in the background, so a query
    /// on any node reads recent data from local disk, not from the bucket. And the files it
    /// replaced leave the hot columns.
    pub fn arrived(&self, d: &Delta) {
        for (key, value) in &d.puts {
            match (key.get(..2), &self.disk) {
                (Some("s/"), Some(disk)) => {
                    if let Ok(s) = serde_json::from_slice::<Segment>(value) {
                        if !s.path.is_empty() && !s.path.starts_with("_base/") {
                            disk.fetch_later(s.path);
                        }
                    }
                }
                (Some("t/"), disk) if disk.is_some() || self.hot.on() => {
                    let Ok(m) = serde_json::from_slice::<TableMeta>(value) else { continue };
                    self.hot.forget(&m.garbage);
                    // (files a tiering round writes; not a bulk load's big files, which only the queries that need them read)
                    if let Some(disk) = disk {
                        m.files.into_iter().filter(|f| f.bytes <= 256 << 20 && !f.path.is_empty() && !f.path.starts_with("_base/")).for_each(|f| disk.fetch_later(f.path));
                    }
                }
                _ => {} // (keys like "c" and "n" are one character long)
            }
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
            if f.bytes <= budget && !f.path.starts_with("_base/") {
                budget -= f.bytes;
                disk.fetch_later(f.path.clone());
            }
        }
        Ok(())
    }

    /// Wait, 10 s at most, until this node holds what its leader had committed when it started
    /// (`cluster::catch_up`). A node restarted after a failover would otherwise answer from an older
    /// catalog than it did before: its view reads no WAL, and the new leader flushes what it took
    /// over a moment after it leads (a table just made was "not found").
    pub async fn caught_up(&self) {
        if !*self.caught.borrow() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), self.caught.subscribe().wait_for(|c| *c)).await;
        }
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
        match (bucket_url(&other.url), &self.cached) {
            (Some(bucket), Some(ours)) if bucket_url(&self.url).as_ref() == Some(&bucket) => ours.add_lake(other.url[bucket.len()..].trim_start_matches('/')), // (one bucket: our reader, its files cached too)
            (Some(bucket), _) => {
                let url = url::Url::parse(&bucket)?;
                self.rt.register_object_store(&url, other.rt.object_store(datafusion::execution::object_store::ObjectStoreUrl::parse(&bucket)?)?);
            }
            _ => {}
        }
        // (the catalog's sync and an ATTACH may both open it at once, on a bucket for seconds: one is kept)
        let mut attached = self.attached.write().unwrap();
        if !attached.iter().any(|(n, _)| n == name) {
            attached.push((name.to_string(), other));
        }
        Ok(())
    }

    /// A fresh SQL session on the shared runtime (`partitions()` of them).
    pub fn session(&self) -> SessionContext {
        self.session_with(partitions())
    }

    /// A session with a fixed number of partitions: 1 for point lookups, where splitting the work
    /// costs more than it saves and many queries run at once. Its functions, planners and rules
    /// are the same for every query, so they are made once (`made`) and copied; each copy gets
    /// catalogs of its own, so the tables a query registers are its alone.
    pub fn session_with(&self, partitions: usize) -> SessionContext {
        let mut state = self.sessions.lock().unwrap().entry(partitions).or_insert_with(|| self.made(partitions).state()).clone();
        let catalogs = datafusion::catalog::MemoryCatalogProviderList::new();
        let catalog = datafusion::execution::SessionStateDefaults::default_catalog(state.config(), state.table_factories(), state.runtime_env());
        datafusion::catalog::CatalogProviderList::register_catalog(&catalogs, state.config().options().catalog.default_catalog.clone(), Arc::new(catalog));
        state.register_catalog_list(Arc::new(catalogs));
        let ctx = SessionContext::new_with_state(state);
        crate::files::register(&ctx, self.arc()); // files('…'), file_read(path) (here, not in what is kept: the lake would keep itself)
        crate::seq::register(&ctx, self.arc()); // nextval, currval, setval
        crate::ext::register_secrets(&ctx, self.arc()); // secrets()
        ctx
    }

    /// A session made from nothing, with everything every query's session has.
    fn made(&self, partitions: usize) -> SessionContext {
        let config = crate::optimize::config(SessionConfig::new().with_information_schema(true).with_target_partitions(partitions)
            .with_default_catalog_and_schema(crate::ddl::lake_name(self), crate::ddl::PUBLIC));
        let state = SessionStateBuilder::new().with_config(config).with_runtime_env(self.rt.clone()).with_default_features();
        let mut state = state.with_optimizer_rules(crate::optimize::rules()).with_physical_optimizer_rules(crate::optimize::physical_rules());
        state.expr_planners().get_or_insert_with(Vec::new).insert(0, crate::files::planner());
        state.expr_planners().get_or_insert_with(Vec::new).insert(0, crate::intervals::planner()); // (n * INTERVAL '37 seconds') // (SUBSTRING of bytes too)
        let mut ctx = SessionContext::new_with_state(state.build());
        spark(&ctx); // format_string, pmod, parse_url, …
        datafusion_functions_json::register_all(&mut ctx).expect("JSON functions register"); // json_get(…), ->, ->>
        crate::ai::register(&ctx); // ai_complete, ai_embed, cosine_similarity, …
        crate::asof::register(&ctx); // (ASOF JOIN's marker)
        crate::fsum::register(&ctx); // sum(DOUBLE): the same answer in any order
        crate::optimize::register_zoned(&ctx); // to_timestamp(column): in the zone its type says
        crate::panics::test_function(&ctx); // pondra_panic(), for the tests only
        ctx
    }

    /// Query memory in use, and the limit.
    pub fn memory(&self) -> (usize, usize) { (self.rt.memory_pool.reserved(), memory_limit()) }

    /// This lake as an `Arc` (for query plans that outlive the call that made them).
    pub fn arc(&self) -> Arc<Lake> { self.me.upgrade().expect("a lake outlives its queries") }

    /// Full URL of an object, for DataFusion.
    pub fn full(&self, path: &str) -> String {
        if path.contains("://") {
            return path.to_string(); // (a file outside the lake: `ext.rs`)
        }
        if let Some((id, rest)) = crate::branch::split(path) {
            if let Some((url, _)) = self.bases.read().unwrap().get(id) {
                return format!("{url}/{rest}"); // (a branch's file in its base: ADR-047)
            }
        }
        format!("{}/{path}", self.url)
    }

    /// A branch's base (ADR-047), so the paths it lists there resolve: DataFusion reads its files
    /// through this lake's bucket reader when they share a bucket, or one of its own.
    pub fn add_base(&self, id: &str, url: &str) -> Result<()> {
        if self.bases.read().unwrap().contains_key(id) {
            return Ok(());
        }
        match (bucket_url(url), &self.cached) {
            (Some(bucket), Some(ours)) if bucket_url(&self.url).as_ref() == Some(&bucket) => ours.add_lake(url[bucket.len()..].trim_start_matches('/')),
            (Some(bucket), _) => {
                if let Ok((_, _, Some((root, whole)))) = open_store(url) {
                    let prefix = url[root.len()..].trim_start_matches('/').to_string();
                    self.rt.register_object_store(&url::Url::parse(&bucket)?, Arc::new(crate::cache::CachedStore::new(whole, cache_mb() << 20, None, &prefix)));
                }
            }
            _ => {}
        }
        self.bases.write().unwrap().insert(id.to_string(), (url.to_string(), None));
        Ok(())
    }

    /// A branch's base's store, for the objects of the base it reads whole (manifests, segments).
    fn base_store(&self, id: &str) -> Result<Store> {
        let url = match self.bases.read().unwrap().get(id) {
            Some((_, Some(store))) => return Ok(store.clone()),
            Some((url, None)) => url.clone(),
            None => anyhow::bail!("no base {id} in this branch"),
        };
        anyhow::ensure!(url.contains("://") || std::path::Path::new(&url).exists(), "this branch's base {url} is gone");
        let store = open_store(&url)?.1;
        if let Some(b) = self.bases.write().unwrap().get_mut(id) {
            b.1 = Some(store.clone());
        }
        Ok(store)
    }

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
        if let (Some(disk), false) = (&self.disk, crate::delta::open_format(path) || replaceable(path)) {
            disk.put(path, &bytes); // what a node writes, it keeps
        }
        Ok(())
    }

    /// A whole lake object: from the SSD tier if it's there, else from the bucket (and kept). A
    /// user's file (`files/`) may be replaced, so it is always the bucket's (ADR-034).
    pub async fn object(&self, path: &str) -> Result<Bytes> {
        let disk = self.disk.as_ref().filter(|_| !replaceable(path));
        if let Some((file, _)) = disk.and_then(|d| d.get(path)) {
            if let Ok(bytes) = tokio::fs::read(file).await {
                return Ok(bytes.into());
            }
        }
        let bytes = match crate::branch::split(path) {
            Some((id, rest)) => self.base_store(id)?.get(&Path::from(rest)).await?.bytes().await?, // (a branch's base's: ADR-047)
            None => self.store.get(&Path::from(path)).await?.bytes().await?,
        };
        if let Some(disk) = disk {
            disk.put(path, &bytes);
        }
        Ok(bytes)
    }

    /// A user's file's version, for `If-Match` (`GET /files` gives it): its e-tag and size, or
    /// (a store without e-tags) its time and size.
    pub async fn version(&self, path: &str) -> Result<String> { Ok(version_of(&self.store.head(&Path::from(path)).await?)) }

    /// A user's file and the version of those very bytes (one request: `GET /files`).
    pub async fn file(&self, path: &str) -> Result<(Bytes, String)> {
        let r = self.store.get(&Path::from(path)).await?;
        let v = version_of(&r.meta);
        Ok((r.bytes().await?, v))
    }

    /// Replace a user's file (`files/`) that is still the version `expect` (ADR-034): a newer
    /// one, saved meanwhile, is refused (`Changed`), so two editors never overwrite each other
    /// blind. Where the store can, the check and the write are one request (a conditional put).
    pub async fn replace(&self, path: &str, bytes: Vec<u8>, expect: &str) -> Result<String> {
        anyhow::ensure!(replaceable(path), "only the lake's files (files/…) are replaced");
        static ONE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(()); // (a store with no conditional put: this node's saves one at a time)
        let _one = ONE.lock().await;
        let at = Path::from(path);
        let m = self.store.head(&at).await?;
        if version_of(&m) != expect {
            return Err(Changed.into());
        }
        let (len, bytes, opts) = (bytes.len() as u64, Bytes::from(bytes), |mode| PutOptions { mode, ..Default::default() });
        let update = PutMode::Update(object_store::UpdateVersion { e_tag: m.e_tag.clone(), version: m.version.clone() });
        let put = match self.store.put_opts(&at, bytes.clone().into(), opts(update)).await {
            Ok(r) => r,
            Err(object_store::Error::Precondition { .. }) => return Err(Changed.into()),
            Err(object_store::Error::NotImplemented { .. } | object_store::Error::NotSupported { .. }) => {
                self.store.put_opts(&at, bytes.into(), opts(PutMode::Overwrite)).await? // (a local disk: checked just above)
            }
            Err(e) => return Err(e.into()),
        };
        match put.e_tag {
            Some(e) => Ok(format!("{}:{len}", e.trim_matches('"'))), // (the version written: not one another save made since)
            None => self.version(path).await,
        }
    }

    /// Remove a user's file (`files/`): the tables' own objects are removed by Pondra alone.
    pub async fn remove_file(&self, path: &str) -> Result<()> {
        anyhow::ensure!(replaceable(path), "only the lake's files (files/…) are removed this way");
        self.store.head(&Path::from(path)).await?; // (not there: an error that says so)
        self.store.delete(&Path::from(path)).await?;
        Ok(())
    }

    /// The rows of `table` in log segment `n`, decoded once and then served from memory
    /// (segments never change, so the cache never goes stale).
    /// The log's segments after `after`, to the newest, each decoded once per node (a segment
    /// never changes once committed), the list kept whole from where it was first asked for: a
    /// key lookup reads only what was committed since the last one, however long the log tail.
    pub async fn segments_after(&self, after: u64) -> Result<Vec<(u64, Arc<Segment>)>> {
        struct Known {
            start: u64, // every segment from here…
            whole: u64, // …to here is in `segs`
            segs: BTreeMap<u64, Arc<Segment>>,
        }
        static KNOWN: LazyLock<Mutex<std::collections::HashMap<String, Known>>> = LazyLock::new(Default::default);
        let now = self.visible();
        let from = match KNOWN.lock().unwrap().get(&self.url) {
            Some(k) if k.start <= after + 1 && k.whole >= after => k.whole + 1, // (only what is new)
            _ => after + 1,
        };
        let raw = if from <= now { self.cat.scan_raw(&seg_key(from), "s0").await? } else { BTreeMap::new() };
        let mut all = KNOWN.lock().unwrap();
        let k = all.entry(self.url.clone()).or_insert(Known { start: from, whole: from - 1, segs: BTreeMap::new() });
        if from < k.start || from > k.whole + 1 {
            *k = Known { start: from, whole: from - 1, segs: BTreeMap::new() }; // (asked from further back: begun again there)
        }
        for (key, v) in raw {
            let n = key[2..].parse::<u64>()?;
            if n <= now && !k.segs.contains_key(&n) {
                k.segs.insert(n, Arc::new(serde_json::from_slice::<Segment>(&v)?));
            }
        }
        k.whole = k.whole.max(now);
        while k.segs.len() > 200_000 {
            let Some((first, _)) = k.segs.pop_first() else { break };
            k.start = first + 1; // (the oldest go: asked for again, they are read again)
        }
        let mut out: Vec<(u64, Arc<Segment>)> = k.segs.range(after + 1..).map(|(n, s)| (*n, s.clone())).collect();
        drop(all);
        out.retain(|(n, _)| *n <= now);
        Ok(out)
    }

    pub async fn segment_rows(&self, n: u64, seg: &Segment, table: &str) -> Result<Rows> {
        if let Some(rows) = self.tail.lock().unwrap().rows.get(table).and_then(|t| t.get(&n)) {
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
        #[derive(Deserialize)]
        struct Tiered { tiered: u64 }
        let tiered = self.cat.get::<Tiered>(&table_key(table)).await?.map_or(0, |t| t.tiered);
        let mut c = self.tail.lock().unwrap();
        c.forget(table, tiered);
        if n > tiered {
            c.keep(table, n, rows.clone());
        }
        Ok(rows)
    }

    /// Rows of `table` a file commit gives the log's readers (`Segment::files`): its files' rows as
    /// they were then, with their system columns (stored names), from the `from`-th on and `rows`
    /// of them at most; `removed`: the rows of the files it took out.
    pub async fn filed_rows(&self, seg: &Segment, table: &str, removed: bool, from: u64, rows: u64) -> Result<Vec<RecordBatch>> {
        let (Some(filed), Some(meta)) = (seg.files.get(table), self.cat.get::<TableMeta>(&table_key(table)).await?) else { return Ok(vec![]) };
        let meta = crate::sys::with_sys(&meta);
        let schema = crate::query::schema(&meta.columns)?;
        let (ctx, end, mut at, mut out) = (self.session(), from.saturating_add(rows), 0u64, vec![]);
        for f in if removed { &filed.removed } else { &filed.added } {
            let (lo, hi) = (at.max(from), (at + f.rows).min(end));
            if lo < hi {
                out.extend(crate::scan::file_rows(self, &ctx, f, &meta, &schema, crate::scan::Pick::Range(lo - at, hi - at)).await?.collect().await?);
            }
            at += f.rows;
            if at >= end {
                break;
            }
        }
        out.iter().map(|b| crate::query::conform(b, &schema)).collect()
    }

    /// Rows of `table` a file commit deleted by position (`Filed::deleted`), with their system
    /// columns (stored names): each file's rows at the places its new deletes name.
    pub async fn deleted_rows(&self, seg: &Segment, table: &str) -> Result<Vec<RecordBatch>> {
        let (Some(filed), Some(meta)) = (seg.files.get(table), self.cat.get::<TableMeta>(&table_key(table)).await?) else { return Ok(vec![]) };
        let meta = crate::sys::with_sys(&meta);
        let schema = crate::query::schema(&meta.columns)?;
        let mut out = vec![];
        for (f, deletes) in &filed.deleted {
            out.extend(crate::adopt::rows_deleted(self, &meta, f, deletes, &schema).await?);
        }
        out.iter().map(|b| crate::query::conform(b, &schema)).collect()
    }

    pub async fn delete(&self, path: &str) {
        if path.starts_with("_base/") {
            return; // a branch never deletes its base's files (ADR-047): the base lets them go
        }
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
    loud: AtomicU64,              // the last commit a remembered answer may depend on (`quiet`, `version`)
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

/// A commit's key that says nothing a read sees changed (retention's marks: `tier::expire`).
pub const QUIET: &str = "quiet";

/// A commit no remembered answer depends on (`Catalog::version`): only the statements' history's
/// rows, their producer's progress and its table's entry (`history.rs`, every second on every
/// node), retention letting segments go and marked `QUIET`, or sequences' blocks taken (`seq.rs`,
/// marked too). Counting them, every remembered
/// answer was forgotten every second on a lake nobody wrote to.
fn quiet(d: &Delta) -> bool {
    let history = |k: &str| k == crate::history::KEY || k.strip_prefix("p/history-").is_some_and(|id| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()));
    let marked = d.puts.iter().any(|(k, _)| k == QUIET);
    let only = |s: &Segment| s.files.is_empty() && s.parts.keys().all(|t| t == crate::history::TABLE);
    d.deletes.iter().all(|k| k.starts_with("s/") || k.starts_with("d/"))
        && (marked || d.puts.iter().any(|(k, _)| history(k)))
        && d.puts.iter().all(|(k, v)| {
            matches!(k.as_str(), "c" | "n" | "b" | QUIET) || k.starts_with("d/") || history(k) || (marked && (k.starts_with("t/") || k.starts_with("sq/")))
                || (k.starts_with("s/") && serde_json::from_slice::<Segment>(v).is_ok_and(|s| only(&s)))
        })
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

/// A cold start's steps and their times, with `PONDRA_TRACE_START` (`tools/cold_trace.sh`, C5).
pub fn trace(what: &str, since: std::time::Instant) {
    if std::env::var_os("PONDRA_TRACE_START").is_some() {
        eprintln!("start: {what} {:.2} s", since.elapsed().as_secs_f64());
    }
}

/// Has the node begun to serve? (Then the catalog's upkeep starts: `upkeep`.)
static SERVING: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::Sender::new(false));

/// The node serves now (`main.rs`).
pub fn serving() { SERVING.send_replace(true); }

/// The catalog's blocks kept in memory: 32 MB of data and 32 MB of indexes and filters. SlateDB's
/// own default is 512 and 128 MB, and the leader put every block it flushed in it, so a node under
/// steady writes grew by the catalog's size until it held 640 MB for a cache it hardly reads:
/// everything committed is in the overlay (`mirror`), and an inline segment's rows are read once
/// and kept decoded (`Lake::tail`).
fn blocks() -> Arc<dyn slatedb::db_cache::DbCache> {
    use slatedb::db_cache::{moka::{MokaCache, MokaCacheOptions}, SplitCache};
    let moka = |mb: u64| Some(Arc::new(MokaCache::new_with_opts(MokaCacheOptions { max_capacity: mb << 20, time_to_live: None, time_to_idle: None })) as Arc<dyn slatedb::db_cache::DbCache>);
    Arc::new(SplitCache::new().with_block_cache(moka(32)).with_meta_cache(moka(32)).build())
}

/// The catalog's compactor and garbage collector, as SlateDB runs them beside a writer, started
/// once the node serves (or after 10 s: a `pondra sql` writer never does), so a cold start waits
/// for neither (C5). A failure in either stops the node, as one in the writer would.
fn upkeep(store: Store, compactor: CompactorOptions, gc: GarbageCollectorOptions, local: bool) {
    crate::panics::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(10), SERVING.subscribe().wait_for(|s| *s)).await;
        if local {
            tokio::spawn(unpin(store.clone()));
        }
        let compactor = slatedb::CompactorBuilder::new("catalog", store.clone()).with_options(compactor).build();
        let gc = slatedb::GarbageCollectorBuilder::new("catalog", store).with_options(gc).build();
        let (a, b) = tokio::join!(compactor.run(), gc.run());
        if let Err(e) = a.and(b) {
            eprintln!("stopping: the catalog's compactor or garbage collector failed: {e}");
            std::process::abort();
        }
    });
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

impl Catalog {
    async fn writer(store: Store, object_store_cache_options: ObjectStoreCacheOptions, local: bool) -> Result<Self> {
        // Poll object storage rarely when idle (that's an idle writer's request bill), but often
        // enough that compaction keeps up with the checkpoints.
        let compactor_options = Some(CompactorOptions { poll_interval: Duration::from_secs(5), ..Default::default() });
        // Every commit is a write-ahead-log object: clear those (and old manifests) every minute
        // once a minute old, not SlateDB's every 10 minutes — thousands would sit in the bucket.
        // (What a reader's checkpoint still needs stays.)
        // On a local disk, where listing costs nothing, the WAL goes every 5 s and the compactor's
        // replaced files a minute on, not five: a minute of WAL is a file per commit (100 MB at 400
        // commits a second), and a disk they filled had no room to flush the catalog, so nothing
        // could ever clear it. (A follower reads the files of a manifest it polled 250 ms ago.)
        let every = |interval: u64, min_age: u64| Some(GarbageCollectorDirectoryOptions { interval: Some(Duration::from_secs(interval)), min_age: Duration::from_secs(min_age), dry_run: false });
        let gc = std::env::var("PONDRA_GC_SECS").ok().and_then(|v| v.parse().ok());
        let (wal, secs) = (gc.unwrap_or(if local { 5 } else { 60 }), gc.unwrap_or(60));
        let compacted = if local { every(10, 60) } else { GarbageCollectorOptions::default().compacted_options };
        let garbage_collector_options = Some(GarbageCollectorOptions { wal_options: every(wal, wal), manifest_options: every(secs, secs), compacted_options: compacted, ..Default::default() });
        // (the compactor and the garbage collector start once the node serves: their first reads
        // were most of a cold start's, C5; `upkeep`)
        let settings = Settings { garbage_collector_options: None, flush_interval: Some(Duration::from_millis(1)), manifest_poll_interval: Duration::from_secs(10), l0_sst_size_bytes: 16 << 20, l0_max_ssts: 64, l0_max_ssts_per_key: 32, max_unflushed_bytes: 64 << 20, compactor_options: None, object_store_cache_options, ..Default::default() };
        let t0 = std::time::Instant::now();
        // (a flush's blocks stay out of the cache: everything committed is in the overlay already)
        let flushed = slatedb::BlockCachePolicy::default().with_flush_targets(&[slatedb::CacheTarget::Index, slatedb::CacheTarget::Filters]);
        let db = Db::builder("catalog", store.clone()).with_settings(settings).with_db_cache(blocks()).with_block_cache_policy(flushed);
        let mut cat = Self::new(Db_::Writer(db.build().await?));
        trace("the catalog's writer open", t0);
        upkeep(store, compactor_options.unwrap_or_default(), garbage_collector_options.unwrap_or_default(), local);
        cat.replicas = std::env::var("PONDRA_REPLICAS").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
        cat.last_n.store(cat.get::<u64>("n").await?.unwrap_or(1), Relaxed);
        let c = cat.get::<u64>("c").await?.unwrap_or(0);
        *cat.order.get_mut() = c + 1;
        let (tx, rx) = mpsc::unbounded_channel();
        (cat.writes, *cat.unstarted.get_mut().unwrap()) = (Some(tx), Some(rx));
        cat.committed.store(c, Relaxed);
        cat.durable.send_replace(c);
        // (what we inherited, replayed from the WAL, reaches every node's view at the first
        // checkpoint: right after the node serves, `main.rs`)
        // The leader too reads the catalog from memory: everything committed, nothing in flight.
        let Db_::Writer(db) = &cat.db else { unreachable!() };
        let mut all = collect(db.scan(b"".to_vec()..b"d/".to_vec()).await.map_err(fatal)?).await?;
        all.extend(collect(db.scan(b"d0".to_vec()..vec![0xff]).await.map_err(fatal)?).await?);
        trace("the catalog in memory", t0);
        cat.overlay.get_mut().unwrap().extend(all.into_iter().map(|(k, v)| (k, (c, Some(v)))));
        cat.streamed.store(c, Relaxed);
        cat.loud.store(c, Relaxed);
        cat.mirror.store(true, Relaxed);
        Ok(cat)
    }

    async fn reader(store: Store, streamed: bool, object_store_cache_options: ObjectStoreCacheOptions) -> Result<Self> {
        let opts = DbReaderOptions { manifest_poll_interval: Duration::from_millis(250), skip_wal_replay: streamed, object_store_cache_options, ..Default::default() };
        // FollowLatest writes nothing, so readers work with read-only bucket credentials.
        let reader = DbReader::builder("catalog", store).with_reader_mode(DbReaderMode::FollowLatest).with_options(opts).with_db_cache(blocks());
        let mut cat = Self::new(Db_::Reader(reader.build().await?));
        cat.follows = streamed;
        cat.refresh().await?; // (one try at the in-memory catalog before serving; later refreshes retry)
        Ok(cat)
    }

    /// A reader, or why there can't be one: SlateDB tries again for as long as the bucket says no,
    /// so a bucket that isn't there, or that refuses the credentials, left `pondra sql` waiting
    /// for ever. When opening takes a while, a listing (after the store's own retries) says
    /// whether there is a catalog to wait for.
    async fn opened(store: Store, streamed: bool, cache: ObjectStoreCacheOptions) -> Result<Self> {
        let open = Self::reader(store.clone(), streamed, cache);
        tokio::pin!(open);
        if let Ok(cat) = tokio::time::timeout(Duration::from_secs(3), &mut open).await {
            return cat;
        }
        match futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from("catalog/manifest")))).await {
            Some(Err(e)) => Err(anyhow::Error::new(e).context("the lake's bucket")),
            Some(Ok(_)) => open.await, // (a catalog, slow to open)
            None => bail!("no catalog"), // (the caller says what to do)
        }
    }

    fn new(db: Db_) -> Self {
        let (feed, order) = (broadcast::channel(4096).0, tokio::sync::Mutex::new(1));
        let (last_n, view, pruned, pins, streamed, hold, mirror, flushed, committed, loud) = Default::default();
        let (durable, acked, acks, pending, unstarted) = (watch::Sender::new(0), watch::Sender::new(()), Default::default(), Default::default(), Default::default());
        Catalog { db, order, flushed, writes: None, unstarted, committed, durable, replicas: 1, acks, acked, pending, feed, recent: Default::default(), last_n, overlay: Default::default(), view, pruned, pins, streamed, hold, follows: false, mirror, loud }
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
        if !quiet(d) {
            self.loud.fetch_max(d.id, Relaxed);
        }
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
        self.loud.fetch_max(c, Relaxed);
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
    /// the stream. A read that starts after this sees at least this version (never older). Quiet
    /// commits don't count: every read but the history's is the same across them.
    pub fn version(&self) -> Option<u64> {
        match &self.db {
            Db_::Writer(_) => Some(self.loud.load(Relaxed).min(self.committed.load(Relaxed))), // (`apply` comes just before `committed`)
            Db_::Reader(_) => self.mirror.load(Relaxed).then(|| self.loud.load(Relaxed)),
        }
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.get_raw(key).await?.map(|v| serde_json::from_slice(&v).context(key.to_string())).transpose()
    }

    /// The commit that last wrote `key`, where the catalog is in memory (the leader's always is).
    pub fn written_at(&self, key: &str) -> Option<u64> {
        let o = self.overlay.lock().unwrap();
        self.mirror.load(Relaxed).then(|| o.get(key).map(|(id, _)| *id)).flatten()
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
        self.scan_raw(from, to).await?.into_iter().map(|(k, v)| Ok((k, serde_json::from_slice(&v)?))).collect()
    }

    /// `scan`, the values as they are kept (JSON), for a caller that decodes only what it hasn't.
    pub async fn scan_raw(&self, from: &str, to: &str) -> Result<BTreeMap<String, Bytes>> {
        if from >= to {
            return Ok(BTreeMap::new());
        }
        let range = from.as_bytes().to_vec()..to.as_bytes().to_vec();
        let decode = |all: BTreeMap<String, Bytes>| -> Result<BTreeMap<String, Bytes>> { Ok(all) };
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

    /// A new branch's leader (ADR-047): its commits number after its base's, whose rows it holds
    /// with their `_version`s.
    pub async fn start_after(&self, c: u64) {
        let mut order = self.order.lock().await;
        *order = (*order).max(c + 1);
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
    static PARTS: std::sync::OnceLock<usize> = std::sync::OnceLock::new(); // (every session asks: the cores are read from the cgroup's files)
    *PARTS.get_or_init(|| {
        let cores = std::env::var("PONDRA_CORES").ok().and_then(|p| p.parse().ok()).unwrap_or_else(|| std::thread::available_parallelism().map_or(2, |n| n.get()));
        cores.min(memory_limit() / (24 << 20)).max(2)
    })
}

/// Spark's functions whose names DataFusion doesn't have (`format_string`, `pmod`, `parse_url`,
/// `sha2`, `collect_list`, …: PySpark's vocabulary in SQL). Where both have a name, DataFusion's
/// stays, so no answer changes, and Spark's is there under a name of its own (`spark_floor`):
/// `spark_sql('…')` calls it where Spark SQL names `floor` (`sparksql.rs`).
fn spark(ctx: &SessionContext) {
    let more = SPARK.get_or_init(|| spark_of(ctx));
    more.scalar.iter().chain(&more.shared).for_each(|f| { ctx.register_udf(f.clone()); });
    more.aggregate.iter().for_each(|f| { ctx.register_udaf(f.clone()); });
}

pub struct Spark {
    scalar: Vec<datafusion::logical_expr::ScalarUDF>,
    aggregate: Vec<datafusion::logical_expr::AggregateUDF>,
    shared: Vec<datafusion::logical_expr::ScalarUDF>, // (Spark's of the names both have, renamed)
    pub names: std::collections::HashMap<String, String>, // a name both have (or its alias) → Spark's here
}

static SPARK: std::sync::OnceLock<Spark> = std::sync::OnceLock::new();

/// Spark's functions as every session has them (worked out once, against the first session's
/// names: DataFusion's own, nothing else yet).
pub fn spark_functions() -> &'static Spark {
    SPARK.get_or_init(|| spark_of(&SessionContext::new()))
}

fn spark_of(ctx: &SessionContext) -> Spark {
    use datafusion::execution::FunctionRegistry;
    let new = |name: &str, aliases: &[String]| std::iter::once(name).chain(aliases.iter().map(String::as_str)).all(|n| ctx.udf(n).is_err() && ctx.udaf(n).is_err() && ctx.udwf(n).is_err());
    let (scalar, shared): (Vec<_>, Vec<_>) = datafusion_spark::all_default_scalar_functions().into_iter().partition(|f| new(f.name(), f.aliases()));
    let aggregate = datafusion_spark::all_default_aggregate_functions().into_iter().filter(|f| new(f.name(), f.aliases()));
    let names = shared.iter().flat_map(|f| std::iter::once(f.name().to_string()).chain(f.aliases().iter().cloned()).map(|n| (n, format!("spark_{}", f.name())))).collect();
    Spark {
        scalar: scalar.iter().map(|f| f.as_ref().clone()).collect(),
        aggregate: aggregate.map(|f| f.as_ref().clone()).collect(),
        shared: shared.iter().map(crate::sparksql::renamed).collect(),
        names,
    }
}
