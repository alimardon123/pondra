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
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManager, DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::spill_file::{SpillFile, TempFileFactory};
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::ClientConfigKey;
#[cfg(not(target_arch = "wasm32"))]
use object_store::local::LocalFileSystem;
use object_store::{path::Path, prefix::PrefixStore, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use crate::kv::{self, slate, Kv, Written};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Weak};
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str(crate::lake_files::CHANGED) }
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
    /// Columns that hold text but are of another SQL type, by stored name: "uuid", "json", "jsonb", "varchar(n)"
    /// (`types.rs`). Every door refuses a value that isn't one of the type's, written its one way.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sql_types: BTreeMap<String, String>,
    /// Not the lake's: files outside it a query reads as a table (`ext.rs`), never in the catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ext: Option<crate::ext_names::Spec>,
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
    /// A GROUP BY view's table whose answers need a last step (avg, stddev, HAVING, …: ADR-055):
    /// its columns are what adds up, and reads give the view's columns from them (`finish.rs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish: Option<crate::finish::Finish>,
    /// `retention = '7 days'` (ADR-043): how long the table's past is kept (`ddl::KEEP_MS` when not
    /// set): read `AT (…)` (`past.rs`), and undropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_secs: Option<u64>,
    /// From which commit (and time, ms) on the table can be read as it was: a purge let the old
    /// versions of earlier changes go (`tier::purge`). (0, 0): all of it; None: changed before
    /// this was kept, and whole only from its oldest purge kept (`past::kept_since`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub past_from: Option<(u64, u64)>,
    /// When the table was made: the log's end then and the time (ms). `t AT (…)` before it is
    /// refused (`past::table`); None for a table made before this was kept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub made: Option<(u64, u64)>,
    /// The job of the `CREATE TABLE … AS SELECT` that made it: a definition sent again by that statement is
    /// its own, and another statement's is refused while the table is there (`write::create_table`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub made_by: Option<String>,
    /// A clone's (`CREATE TABLE c CLONE t`, ADR-043): the folders of other tables whose files it
    /// lists too. Files there go only by the orphan sweep, which counts every table listing them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shares: Vec<String>,
    /// The rules made from this parse, once (`rules`): never in the catalog, and a clone starts without them.
    #[serde(skip)]
    pub rules: RulesMemo,
}

/// The rules made from one parse of a table's entry (`TableMeta::rules`), kept once. A clone gets an empty one:
/// it may be changed, so it makes its own.
#[derive(Default)]
pub struct RulesMemo(std::sync::OnceLock<Arc<crate::rules::Table>>);

impl Clone for RulesMemo {
    fn clone(&self) -> Self { RulesMemo::default() }
}

impl TableMeta {
    /// The changes whose old rows are out of the table's files: reads skip their `{t}$deleted` rows.
    pub fn purged(&self) -> u64 { self.purges.last().map_or(0, |p| p.0) }

    /// Is this column an upsert table's delete markers (`_deleted`)? Others never see it: published,
    /// its rows are positions (`tier::shadow`), and Iceberg keeps the name for its own.
    pub fn marker(&self, column: &str) -> bool { column == "_deleted" && !self.key.is_empty() }

    /// Is this column (by the names its entry uses) a uuid, held as text (`types.rs`)?
    pub fn uuid(&self, column: &str) -> bool { self.sql_types.get(column).is_some_and(|t| t == "uuid") }

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
            not_null: self.in_sql_list(&self.not_null),
            defaults: self.in_sql(&self.defaults),
            identity: self.in_sql(&self.identity),
            constraints: self.constraints.iter().map(|c| c.named(&n)).collect(),
            enums: self.in_sql(&self.enums),
            sql_types: self.in_sql(&self.sql_types),
            names: BTreeMap::new(),
            dropped: vec![],
            ..self.clone()
        }
    }

    /// SQL's columns, as `logical()` has them, without copying the rest of the entry (its files).
    pub fn logical_columns(&self) -> Vec<(String, String)> {
        if !self.mapped() {
            return self.columns.clone();
        }
        self.live().map(|(_, n, t)| (n.to_string(), t.to_string())).collect()
    }

    /// A per-column map, keyed by stored name, under the names SQL knows the columns by: the entries of dropped columns
    /// left out. `logical()` and `to_rules()` both map their fields through this, so a rename or a drop is mapped here.
    fn in_sql<V: Clone>(&self, m: &BTreeMap<String, V>) -> BTreeMap<String, V> {
        m.iter().filter(|(c, _)| !self.dropped.contains(c)).map(|(c, v)| (self.name_of(c).to_string(), v.clone())).collect()
    }

    /// A per-column list, by stored name, under SQL's names: the dropped columns left out (`in_sql` for a list).
    fn in_sql_list(&self, l: &[String]) -> Vec<String> {
        l.iter().filter(|c| !self.dropped.contains(c)).map(|c| self.name_of(c).to_string()).collect()
    }

    /// The table a write's rules see (`rules::Table`), made from the rule fields alone: never `files` or the
    /// statistics. Each column SQL sees (`live`, the one mapping of stored names to SQL's) takes its rules by its
    /// stored name, as the entry keeps them.
    pub fn to_rules(&self) -> Result<crate::rules::Table> {
        let columns = self
            .live()
            .map(|(stored, name, _)| crate::rules::Column {
                name: name.to_string(),
                stored: stored.to_string(),
                not_null: self.not_null.iter().any(|c| c == stored),
                default: self.defaults.get(stored).cloned(),
                always: self.identity.get(stored).is_some_and(|i| i.always),
                held: self.sql_types.get(stored).cloned(),
                labels: self.enums.get(stored).map(|e| (e.of.clone(), e.labels.clone())),
            })
            .collect();
        Ok(crate::rules::Table {
            schema: crate::answer::schema(&self.logical_columns())?,
            columns,
            key: self.key.iter().map(|c| self.name_of(c).to_string()).collect(),
            key_checked: crate::constraints::key_checked(self),
            unique: self
                .constraints
                .iter()
                .filter(|c| c.enforced && c.kind == crate::constraints::Kind::Unique)
                .map(|c| (c.name.clone(), c.columns.iter().map(|s| self.name_of(s).to_string()).collect()))
                .collect(),
            checks: self.checks.clone(),
            mapped: self.mapped(),
        })
    }

    /// The rules of this entry, made once per parse and kept beside it: `Catalog::parsed` hands out one `Arc` per
    /// entry's bytes, so a write takes them with no lookup. An owned or changed entry calls `to_rules` instead.
    pub fn rules(self: &Arc<Self>) -> Result<Arc<crate::rules::Table>> {
        if let Some(t) = self.rules.0.get() {
            return Ok(t.clone());
        }
        let t = Arc::new(self.to_rules()?);
        Ok(self.rules.0.get_or_init(|| t).clone())
    }

    /// The table as its readers see it, to describe it (listings, `pg_catalog`, MCP): `logical`, but
    /// a view that finishes its answers (`finish.rs`) shows the columns it answers, not its partial ones.
    pub fn described(&self) -> TableMeta {
        match &self.finish {
            Some(f) => TableMeta { columns: f.columns.clone(), merge: BTreeMap::new(), ..self.logical() },
            None => self.logical(),
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

pub use crate::data_file::{DataFile, Lineage};

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
    Ok(lake.cat.meta(&table_key(t)).await?.and_then(|m| m.folder.clone()).unwrap_or_else(|| t.to_string()))
}
pub fn seg_key(n: u64) -> String { format!("s/{n:020}") }
pub fn producer_key(p: &str) -> String { format!("p/{p}") }

/// How long a producer's mark is kept after its last commit, from its name and the time `now` (ms; 0: kept for
/// good, as every mark was before expiries). Three windows:
/// - the named window, `copy::retention_ms` (64 days): a job a client named (`POST /sql?job=`,
///   `POST /insert/{t}?job=`, the Python client's `job=`, `PONDRA_JOB`) whose name doesn't start `auto-`. The docs
///   promise that a job sent again is applied once, and a daily load may be re-run days later;
/// - the retry window (`job_kept`): an id that starts `auto-` (the clients' own: `auto-until<S>-` ids, which say
///   their last resend time, `client_until`, and the others, `log::auto_job`'s and the older clients'), a
///   transaction's COMMIT and a COPY's chunks, unless a client named the transaction's job;
/// - 0 for the rest: views', tasks', Kafka's and `/append`'s producers, and the records of COPY INTO files
///   (`job:load:`) and of Iceberg snapshots (`job:iceberg:`), which their own code keeps or expires.
///
/// The retry window was checked against the paths that send each kind again, not taken on trust:
/// - an `auto-` id is resent only by its client's own retries (the clients' `retry_secs`, python/pondra/client.py,
///   js/index.js), by a fenced `pondra sql` starting over with its job (write.rs ~2299, `PONDRA_JOB` is passed on),
///   and by the inbox: a leader committed the request and died before it deleted it, and the next writer waits out
///   `alive`'s 30 s (cluster.rs:554, inbox.rs);
/// - a transaction's COMMIT (`txn:{id}:{table}`, a fresh id per transaction: txn.rs `start`) is sent once, by
///   the node that runs it (txn.rs `commit`: `write::post` to the leader, or `commit_here` on the leader), and
///   a failed send goes back to the client as an error with the transaction gone, so nothing sends it again;
///   a named transaction (`txn:job-{j}:`) is sent again by its client, so it takes the named window;
/// - a COPY chunk (`copy:{job}`, pg.rs ~642) is appended once, and its error goes back to the client, which
///   can't resume a COPY: nothing sends a chunk again.
pub fn kept_ms(producer: &str, now: u64) -> u64 {
    let named = |job: &str| job_kept(job, now);
    if let Some(job) = producer.strip_prefix("sql:") {
        return named(job);
    }
    if let Some(job) = producer.strip_prefix("txn:job-") {
        return named(job);
    }
    if producer.starts_with("txn:") || producer.starts_with("copy:") {
        return retry_ms();
    }
    if producer.starts_with("job:load:") || producer.starts_with("job:iceberg:") {
        return 0;
    }
    if let Some(job) = producer.strip_prefix("job:") {
        return named(job);
    }
    0
}

/// The window of a job id, from its name (`kept_ms`):
/// - `auto-until<S>-…` (a client's id, `S` its last resend time in unix seconds: its first send plus its `retry_secs`):
///   the mark is kept until `S` plus the retry window, so a resend that is still allowed finds it. Measured from `now`
///   (a commit): `max(retry_ms, S - now) + retry_ms`, and 24 hours at most, whatever `S` says;
/// - any other `auto-…` id: the clients' own ids (`log::auto_job`, an older client's `auto-{producer}-{n}`, and the
///   Python client's `producer=`, which is the user's own name, so an `auto-` name a user picks lands here too): the
///   retry window, whatever follows the prefix;
/// - any other job: a name a client chose, which is kept for the named window (`copy::retention_ms`).
fn job_kept(job: &str, now: u64) -> u64 {
    if let Some(until_s) = client_until(job) {
        let left = until_s.saturating_mul(1000).saturating_sub(now);
        return left.max(retry_ms()).saturating_add(retry_ms()).min(UNTIL_CAP_MS);
    }
    if job.starts_with("auto-") {
        return retry_ms();
    }
    crate::copy::retention_ms()
}

/// The most a mark is kept for its client's deadline (`job_kept`): a day.
const UNTIL_CAP_MS: u64 = 24 * 60 * 60 * 1000;

/// `S` of a job id `auto-until<S>-…` (digits, then a dash), if it is one: the last moment its client may send it again.
pub fn client_until(job: &str) -> Option<u64> {
    let (digits, _) = job.strip_prefix("auto-until")?.split_once('-')?;
    (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| digits.parse().unwrap_or(u64::MAX))
}

/// The retry window (`job_kept`): twice the lease (`locks::lease`, PONDRA_LOCK_LEASE_SECS, 60 s by
/// default), and at least a minute, so 120 s by default. It has no setting of its own.
pub fn retry_ms() -> u64 {
    static MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| 2 * crate::locks::lease().as_secs().max(60) * 1000)
}
pub fn data_key(seg: u64) -> String { format!("d/{seg:020}") } // small segments live inside the catalog
pub fn json<T: Serialize>(v: &T) -> Vec<u8> { serde_json::to_vec(v).expect("serializable") }

/// The open formats a new table is published in unless it says otherwise (the node's `--publish`).
pub fn default_publish() -> Vec<String> {
    crate::env::get("PONDRA_PUBLISH").unwrap_or_default().split(',').filter(|f| !f.is_empty()).map(String::from).collect()
}

const TAIL_BYTES: usize = 256 << 20; // decoded log segments kept in memory, per node
const AHEAD: u64 = 256; // replicated acks: commits acknowledged before the bucket has them, at most

const RECENT: Duration = Duration::from_secs(30); // replayed to (re)connecting followers…
const RECENT_BYTES: usize = 64 << 20; // …within this much memory (past it, a reconnecting
// follower reads from its own view until it has caught up, instead of the leader buffering more)

/// Query read cache in memory, MB (env PONDRA_CACHE_MB, default 1024).
fn cache_mb() -> usize { crate::env::or("PONDRA_CACHE_MB", 1024) }

/// The local SSD tier for a lake on object storage (`serve --cache-dir/--cache-gb`, or env
/// PONDRA_CACHE_DIR / PONDRA_CACHE_GB): under the temp dir, 20 GB, or a quarter of the free disk
/// if that is less (a notebook's sandbox may have a few GB). 0 GB turns it off.
fn disk_tier(url: &str, store: &Store) -> Option<Arc<crate::cache::Disk>> {
    let dir = crate::env::get("PONDRA_CACHE_DIR").map(std::path::PathBuf::from).unwrap_or_else(|| std::env::temp_dir().join("pondra-cache"));
    let bytes = match crate::env::parse::<u64>("PONDRA_CACHE_GB") {
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

/// Room a lake on local disk keeps for its own upkeep: a quarter of the disk, 256 MB at most (of
/// a disk of `total` bytes). Commits wait while less is free (`log::Sequencer`), so the catalog can
/// still flush and tiering write the files that let the log go: a disk the lake filled to its last
/// byte had room for neither, and kept a log that never drained. Spills keep it too.
fn lake_keeps(total: u64) -> u64 { (total / 4).min(256 << 20) }

pub fn short_of_room(dir: &std::path::Path) -> Option<(u64, u64)> {
    space(dir).map(|(free, total)| (free, lake_keeps(total))).filter(|(free, keep)| free < keep)
}

/// The folder of this process's spills, DataFusion's and a shuffle's (`spill::dir`): a `memory::Folder` of
/// this process's own, made by the first spill that needs one. A static's end runs no destructor, so the
/// folder goes when the program ends through `memory::leave` (which `cli.rs` calls before each exit); where
/// that can't remove it (Windows, while its lock file is open), it stays for the next process's sweep. A
/// folder that can't be made is remembered as the reason why.
static SPILLS: std::sync::OnceLock<Result<crate::memory::Folder, String>> = std::sync::OnceLock::new();

/// The folder of this process's spills (`SPILLS`), made on first use. Its error when there is none: DataFusion's
/// spills then go to the OS temp folder, and a shuffle fails with the reason.
pub(crate) fn spill_folder() -> std::io::Result<&'static std::path::Path> {
    let folder = SPILLS.get_or_init(|| {
        crate::memory::Folder::make("spill").map_err(|e| {
            eprintln!("no spill folder of this process's ({e}): queries spill to the temporary folder, shuffles fail");
            e.to_string()
        })
    });
    match folder {
        Ok(folder) => Ok(&folder.path),
        Err(why) => Err(std::io::Error::other(why.clone())),
    }
}

/// `PONDRA_SPILL_GB`: how many GB of spills the node may keep on disk. None when unset (a negative setting is none).
fn spill_gb() -> Option<f64> { crate::env::parse::<f64>("PONDRA_SPILL_GB").filter(|gb| *gb >= 0.0) }

/// The bytes that may spill under `dir`: `PONDRA_SPILL_GB` when set, else `dir`'s disk's free space less `lake_keeps`; None with no `dir`.
pub(crate) fn spill_cap(dir: Option<&std::path::Path>) -> Option<u64> {
    match spill_gb() {
        Some(gb) => Some(gb_bytes(gb)),
        None => dir.and_then(space).map(|(free, total)| free.saturating_sub(lake_keeps(total))),
    }
}

/// Where DataFusion's spills go, and how much of the disk they may take (`spill_cap`). Not the
/// OS temp folder, which is held in memory on many Linux systems, where a spill frees nothing. The
/// first spill makes the folder and DataFusion's own directory in it (`Spills`): a lake that never
/// spills makes neither, so nothing is written when it opens. The folder is the user's own, under the
/// disk with the room (`memory::spill_base`), and folders of processes that ended are swept from it.
/// The cap is the free space when the first spill makes the disk, less the room a lake keeps, or the
/// setting in GB; it is looked at again as queries start (`spill_room`). `0` turns spilling off: a
/// query that needs it is refused (53200). A negative setting is no setting.
fn spill_disk() -> DiskManagerBuilder {
    if spill_gb() == Some(0.0) {
        return DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
    }
    // (a merge of spilled runs takes at most an eighth of its partition's share, about 4 MB a run read at once: unlimited,
    // as DataFusion has it, each partition's merge took the whole pool and a window over its rows was refused)
    // (`PONDRA_SPILL_FAN_IN` replaces the formula; 0 is DataFusion's own, no limit.)
    let fan_in = crate::env::parse::<usize>("PONDRA_SPILL_FAN_IN").unwrap_or_else(|| (memory_limit() / partitions() / (32 << 20)).clamp(4, 64));
    // (the lake's own manager holds no file: `Spills` makes the one that does, with this fan-in too, since sorts read it here)
    DiskManagerBuilder::default().with_mode(DiskManagerMode::Custom(Arc::new(Spills { fan_in, made: std::sync::OnceLock::new() }))).with_max_spill_merge_fan_in(fan_in)
}

/// DataFusion's spill disk for one lake, made by its first spill (`TempFileFactory`): the same manager `spill_disk`
/// would have built when the lake opened, in the spill folder (or the OS temp folder, where there is none), with
/// `fan_in` and the cap of `spill_cap`. `made` holds it once it is built, or the reason it could not be. It is
/// registered (`register_spiller`) when it is made, so `spill_room` caps it and `spill_fits` counts what it holds.
struct Spills {
    fan_in: usize,
    made: std::sync::OnceLock<Result<Arc<DiskManager>, String>>,
}

impl TempFileFactory for Spills {
    fn create_temp_file(&self, description: &str) -> datafusion::error::Result<Arc<dyn SpillFile>> {
        let made = self.made.get_or_init(|| {
            let folder = spill_folder().ok();
            let builder = match folder {
                Some(path) => DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![path.to_path_buf()])),
                None => DiskManagerBuilder::default(), // (no folder: the OS temp folder, as before)
            };
            let builder = builder.with_max_spill_merge_fan_in(self.fan_in);
            let builder = match spill_cap(folder) {
                Some(cap) => builder.with_max_temp_directory_size(cap), // (where the platform can't say, DataFusion's own 100 GB stays)
                None => builder,
            };
            let disk = Arc::new(builder.build().map_err(|e| e.to_string())?);
            register_spiller(&disk);
            Ok(disk)
        });
        match made {
            Ok(disk) => disk.create_tmp_file(description),
            Err(why) => Err(DataFusionError::ResourcesExhausted(format!("no spill folder to spill to while {description}: {why}"))),
        }
    }
}

/// `gb` gibibytes, in bytes.
fn gb_bytes(gb: f64) -> u64 { (gb * (1u64 << 30) as f64) as u64 }

/// The disk managers that hold this process's lakes' spill files (one for each lake, made by its first spill: `Spills`),
/// held weakly: a lake that goes takes its manager with it. `register_spiller` adds each one (unless spilling is off),
/// `spill_room` sets their caps, `spill_fits` counts what they hold.
static SPILLERS: Mutex<Vec<Weak<DiskManager>>> = Mutex::new(Vec::new());

/// Adds a spill disk to `SPILLERS`, once `Spills` has made it. A disabled one has no cap to set, so it is left out.
fn register_spiller(disk: &Arc<DiskManager>) {
    if spill_gb() != Some(0.0) {
        SPILLERS.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(Arc::downgrade(disk));
    }
}

/// The disk managers still alive, the dead ones dropped from `SPILLERS`.
fn live_spillers() -> Vec<Arc<DiskManager>> {
    let mut spillers = SPILLERS.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    spillers.retain(|w| w.strong_count() > 0);
    spillers.iter().filter_map(Weak::upgrade).collect()
}

/// The cap of one disk manager that holds `used` bytes of spills, given the setting and what shuffles hold
/// (`shuffled`) or the folder's disk (`disk`: free and total bytes, where the platform says). With the setting,
/// the shuffles and the spills together stay within it; without, a spill may take the disk's free room beyond
/// what a lake keeps. None where neither is known: the cap stays as it was.
fn room_cap(gb: Option<f64>, used: u64, shuffled: u64, disk: Option<(u64, u64)>) -> Option<u64> {
    match gb {
        Some(gb) => Some(gb_bytes(gb).saturating_sub(shuffled)),
        None => disk.map(|(free, total)| used + free.saturating_sub(lake_keeps(total))),
    }
}

/// Sets every live lake's cap from the disk as it is now (`room_cap`).
fn set_room(gb: Option<f64>) {
    let shuffled = gb.map_or(0, |_| crate::spill::held()); // (shuffles count only under the setting: without it the disk's free room says it all)
    let disk = space(&crate::memory::spill_base()); // (the disk the folder is, or would be on: not made here, the first spill makes it)
    for manager in live_spillers() {
        if let Some(cap) = room_cap(gb, manager.used_disk_space(), shuffled, disk) {
            let _ = manager.set_max_temp_directory_size(cap); // (a disabled manager refuses: none is registered)
        }
    }
}

/// Looked at again as each query's session is made (`session_with`), at most every 500 ms: a node started on a
/// full disk spills once room is freed, and one started on an empty disk stops taking room that others need.
/// Two queries at once may both look, which is harmless. Several lakes in a process each get their own spills
/// plus the free room, so together they can overshoot between two looks, by what half a second writes.
pub(crate) fn spill_room() {
    static START: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);
    static LOOKED: AtomicU64 = AtomicU64::new(u64::MAX); // when the disk was last looked at, ms since `START` (MAX: never)
    let now = START.elapsed().as_millis() as u64;
    let last = LOOKED.load(Relaxed);
    if last != u64::MAX && now.saturating_sub(last) < 500 {
        return;
    }
    LOOKED.store(now, Relaxed);
    set_room(spill_gb());
}

/// Whether `bytes` more fit on the spill disk: within the setting, with `held` already on it (shuffles' pieces
/// and spills), or in its free room beyond what a lake keeps. Where the platform can't say how much room there
/// is, it fits.
fn piece_fits(gb: Option<f64>, held: u64, bytes: u64, disk: Option<(u64, u64)>) -> bool {
    match gb {
        Some(gb) => held.saturating_add(bytes) <= gb_bytes(gb),
        None => disk.map_or(true, |(free, total)| bytes <= free.saturating_sub(lake_keeps(total))),
    }
}

/// Whether a shuffle's piece of `bytes` fits on the spill disk now: `Spill::write` asks before it writes one.
/// No spill folder: it fits, and the write fails with its own error.
pub(crate) fn spill_fits(bytes: u64) -> Result<()> {
    let Ok(folder) = spill_folder() else { return Ok(()) };
    let gb = spill_gb();
    let held = gb.map_or(0, |_| crate::spill::held() + live_spillers().iter().map(|m| m.used_disk_space()).sum::<u64>());
    match piece_fits(gb, held, bytes, space(folder)) {
        true => Ok(()),
        false => Err(no_room(bytes, gb)),
    }
}

/// The refusal of a piece that doesn't fit (53100): how much more it needed, and the way out. The setting is
/// named only when it is set.
fn no_room(bytes: u64, gb: Option<f64>) -> anyhow::Error {
    let way = match gb {
        Some(gb) => format!("Free space on the spill disk, or raise PONDRA_SPILL_GB (now {gb} GB) to let it spill more."),
        None => "Free space on the spill disk to let it spill more.".to_string(),
    };
    let more = bytes as f64 / (1u64 << 20) as f64;
    crate::codes::coded("53100", format!("The shuffle needed {more:.1} MB more on disk than there is room for. {way}"))
}

pub struct Lake {
    pub url: String, // absolute local dir or "s3://bucket/prefix" (gs://, az://, abfss:// too; memory://id in this process)
    pub store: Store,
    pub cat: Catalog,
    pub hwm: watch::Sender<u64>, // the last committed segment this node knows of
    pub backlog: std::sync::atomic::AtomicU64, // leader: rows in the log not yet tiered (all tables)
    pub rt: Arc<RuntimeEnv>,         // shared by all queries: object store registry + Parquet metadata cache
    tail: Mutex<Tail>, // decoded log rows
    pub tails: Mutex<(lru::LruCache<(String, String), Arc<crate::query::Tail>>, usize)>, // tables' tails as queries read them (`query::Tail`), each keyed by its table and then its form (`of.1`); total bytes
    kept: (Arc<Footers>, Arc<FileStatistics>), // DataFusion's caches of Parquet footers and of files' statistics (`rt`'s)
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
    crate::env::query_memory(|| ram().map_or(4 << 30, |r| r / 3))
}

/// The machine's memory, if it says: `/proc` on Linux, the OS's own call elsewhere. Less when a
/// cgroup limits this process (a container, a service with `MemoryMax`): `/proc/meminfo` is the
/// host's, and a node in a 4 GB container on a 64 GB host sized its queries for 64 and was killed.
pub fn ram() -> Option<usize> {
    static RAM: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *RAM.get_or_init(read_ram)
}

// (a page's memory is its line, which `page.rs` takes from its device: none is the machine's)
#[cfg(target_arch = "wasm32")]
fn read_ram() -> Option<usize> {
    None
}

#[cfg(not(target_arch = "wasm32"))]
fn read_ram() -> Option<usize> {
    let proc = std::fs::read_to_string("/proc/meminfo").ok().and_then(|m| m.lines().find_map(|l| l.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?.trim().parse::<usize>().ok()));
    proc.map(|kb| (kb << 10).min(cgroup_memory().unwrap_or(usize::MAX))).or_else(|| {
        let s = sysinfo::System::new_with_specifics(sysinfo::RefreshKind::nothing().with_memory(sysinfo::MemoryRefreshKind::nothing().with_ram()));
        Some(s.total_memory() as usize).filter(|&b| b > 0)
    })
}

/// The least memory limit of this process's cgroups and their parents (v2 `memory.max`, v1
/// `memory.limit_in_bytes`; "max" or none: no limit). In a container the cgroup is its root.
#[cfg(not(target_arch = "wasm32"))]
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
#[cfg(not(target_arch = "wasm32"))]
pub fn resident() -> Option<usize> {
    let proc = std::fs::read_to_string("/proc/self/statm").ok().and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok());
    proc.map(|pages| pages * page_size()).or_else(|| {
        use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
        let (pid, mut s) = (sysinfo::get_current_pid().ok()?, System::new());
        s.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, ProcessRefreshKind::nothing().with_memory());
        Some(s.process(pid)?.memory() as usize).filter(|&b| b > 0)
    })
}

/// A page's resident memory is not asked of an OS: it has none, and its memory is its line (`page.rs`).
#[cfg(target_arch = "wasm32")]
pub fn resident() -> Option<usize> {
    None
}

/// The size of a page in bytes, which `/proc/self/statm` counts in: 4 KB, or 64 KB on some arm64 builds
/// (where a count of pages read as 4 KB pages was 16 times too little). Asked each time: glibc answers
/// from memory, and a static would be a node's state for nothing.
#[cfg(not(target_arch = "wasm32"))]
fn page_size() -> usize {
    #[cfg(unix)]
    {
        let bytes = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if bytes > 0 {
            return bytes as usize;
        }
    }
    4096
}

pub type Rows = Arc<Vec<datafusion::arrow::record_batch::RecordBatch>>;

/// Decoded log rows, each table's by segment, `TAIL_BYTES` at most. A table's log is read only
/// after its `tiered` mark (a read that began before reads its own from the store), so its rows up
/// to the mark go when the node sees the commit that moved it (`arrived`), or a read finds it
/// moved, and past the limit the oldest segment goes. Kept by recent use instead, every node held
/// 256 MB of rows long since in files; kept until a read, a table read once and then only
/// written held them too (fluss_bench: 267 MB of a node's 740 once it was idle).
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

/// How this process reaches a lake on another server (ADR-058): the settings of the bucket key it
/// is read with (a secret's, `ext::build`; None: the environment's), whether nothing here may write
/// it, and where its leader is asked (a URL, with a TYPE pondra secret's token).
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Reach {
    pub read_only: bool,
    pub params: Option<BTreeMap<String, String>>,
    pub endpoint: Option<String>,
    pub token: Option<String>,
    pub secret: Option<String>, // (the bucket secret's name: who may use it reads the lake here, `users::across`)
}

crate::per_node!(static REACH: std::sync::RwLock<BTreeMap<String, Reach>> = std::sync::RwLock::new(BTreeMap::new()));

/// Say how `url` (a lake) is reached. Asked again (an attachment and a branch of it in one
/// process), what the new one leaves out is kept, and read-only stays read-only.
pub fn reach_by(url: &str, r: Reach) {
    let mut all = REACH.write().unwrap();
    let old = all.remove(url.trim_end_matches('/')).unwrap_or_default();
    let read_only = old.read_only || r.read_only;
    all.insert(url.trim_end_matches('/').to_string(), Reach { read_only, params: r.params.or(old.params), endpoint: r.endpoint.or(old.endpoint), token: r.token.or(old.token), secret: r.secret.or(old.secret) });
}

/// What this process knows of how to reach other places, for a process it starts to lead one of them
/// for a moment (`inbox::lead_once`): it holds the same keys, so a backup in another bucket is written
/// with the secret that reaches it (ADR-070).
pub const REACH_VAR: &str = "PONDRA_REACH";

pub fn reaches() -> String { serde_json::to_string(&*REACH.read().unwrap()).unwrap_or_default() }

/// The places the process that started this one could reach (`REACH_VAR`), as it reached them.
pub fn inherit() {
    let Some(text) = crate::env::get(REACH_VAR) else { return };
    match serde_json::from_str::<BTreeMap<String, Reach>>(&text) {
        Ok(all) => all.into_iter().for_each(|(url, r)| reach_by(&url, r)),
        Err(e) => eprintln!("{REACH_VAR}: {e}"),
    }
}

/// How `url`, or a lake it is in, is reached (the longest registered prefix: `url` itself, or a
/// path under it).
pub fn reach_of(url: &str) -> Option<Reach> {
    let url = url.trim_end_matches('/');
    let all = REACH.read().unwrap();
    all.iter().filter(|(k, _)| url.strip_prefix(k.as_str()).is_some_and(|rest| rest.is_empty() || rest.starts_with('/')))
        .max_by_key(|(k, _)| k.len()).map(|(_, r)| r.clone())
}

/// Open a lake's object store. For a bucket (`s3://`, `gs://`, `az://`, `abfss://`), also returns
/// the bucket-level client that query scans use (DataFusion addresses objects by their full path
/// in the bucket). Credentials and endpoints come from the environment, as each cloud's own tools
/// read them: `AWS_*` (`AWS_ENDPOINT` for R2 and MinIO), `GOOGLE_*`, `AZURE_*`. A lake on another
/// server is read with the key its attachment names (`reach_of`), and a read-only one refuses writes.
pub fn open_store(url: &str) -> Result<(String, Store, Option<(String, Store)>)> {
    if in_memory(url) {
        let url = url.trim_end_matches('/').to_string();
        let held = crate::memory::store(&url);
        return Ok((url.clone(), held.clone(), Some((url, held)))); // (DataFusion reads its files at their URLs, as a bucket's)
    }
    if served(url) {
        // (the store is the node that holds the lake's, read through its door: `served.rs`; nothing is connected yet)
        let at = url.trim_start_matches("pondra://").trim_end_matches('/');
        anyhow::ensure!(!at.is_empty() && !at.contains('/'), "{url}: a node serves one lake, named by pondra://host:port alone");
        let url = format!("pondra://{at}");
        let store: Store = Arc::new(Counted(Arc::new(crate::served::Served::new(at))));
        return Ok((url.clone(), store.clone(), Some((url, store)))); // (the lake and its bucket are one, as in memory)
    }
    let Some((scheme, rest)) = url.split_once("://").filter(|(s, _)| ["s3", "gs", "az", "abfs", "abfss"].contains(s)) else {
        return folder(url);
    };
    let (bucket, prefix) = rest.trim_end_matches('/').split_once('/').unwrap_or((rest, ""));
    let root = format!("{scheme}://{bucket}");
    let reach = reach_of(url);
    let params = reach.as_ref().and_then(|r| r.params.as_ref());
    // Idle connections are dropped after 15 s rather than reused: through proxies and NATs that
    // silently forget idle connections, a reused one hung a PUT for the full 30 s timeout on R2.
    let (idle, after) = (ClientConfigKey::PoolIdleTimeout, "15s");
    // Every request to the bucket takes a turn of its budget, retries too (`budget.rs`, C5).
    let turns = || crate::budget::Budget::of(&root);
    let mut whole: Store = match (params, scheme) {
        (Some(_), _) => crate::ext::build(&url::Url::parse(&root)?, params, crate::ext::For::Lake)?,
        (None, "s3") => Arc::new(AmazonS3Builder::from_env().with_bucket_name(bucket).with_allow_http(true).with_config(AmazonS3ConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
        (None, "gs") => Arc::new(GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket).with_config(GoogleConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
        (None, _) => Arc::new(MicrosoftAzureBuilder::from_env().with_url(&root).with_config(AzureConfigKey::Client(idle), after).with_http_connector(turns()).build()?),
    };
    if reach.as_ref().is_some_and(|r| r.read_only) {
        whole = Arc::new(ReadOnly(whole, url.trim_end_matches('/').to_string())); // (before the prefix: the lake's store and the bucket's refuse alike)
    }
    let store: Store = if prefix.is_empty() { whole.clone() } else { Arc::new(PrefixStore::new(whole.clone(), prefix)) };
    // (the bucket's client, which scans and the SSD tier's misses go through, is counted apart:
    // each request goes through one of the two)
    Ok((url.trim_end_matches('/').to_string(), Arc::new(Counted(store)), Some((root, Arc::new(Counted(whole)) as Store))))
}

/// A folder's store, made if it isn't there.
#[cfg(not(target_arch = "wasm32"))]
fn folder(url: &str) -> Result<(String, Store, Option<(String, Store)>)> {
    std::fs::create_dir_all(url)?;
    let dir = std::fs::canonicalize(url)?.to_string_lossy().trim_start_matches(r"\\?\").to_string(); // (Windows verbatim prefix)
    // (`Local`: SlateDB's collector advances its manifest and compactions boundary files with conditional
    // overwrites, and a folder that refuses them never deletes those files. The soak of one local lake
    // for 5 hours grew its catalog from 206 to 13,373 objects, and commits' p95 from 0.4 to 5.2 ms. The
    // boundary stays on (`boundary_files_enabled`): it is what lets a reader whose manifest's successor was
    // deleted list again. The lock in `Local` orders one process's updates only: a folder has one writer,
    // and a writer fenced by a newer one restarts (`fatal`).)
    Ok((dir.clone(), Arc::new(Counted(Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?)))), None))
}

/// A page has no folders: its lake is in memory or in a bucket.
#[cfg(target_arch = "wasm32")]
fn folder(_: &str) -> Result<(String, Store, Option<(String, Store)>)> {
    Err(crate::codes::coded("0A000", "a page opens no folder: its lake is in memory or in a bucket"))
}

// Lakes held in this process's memory (`memory://`, DuckDB's `:memory:`) live in `memory.rs`, which
// spills their files to disk past a budget.
use crate::memory::in_memory;

/// Is `url` a lake served by the node that holds it (`pondra://host:port`, ADR-072 §8)?
pub fn served(url: &str) -> bool { url.starts_with("pondra://") }

/// A lake's bucket, as DataFusion names its store (`s3://bucket`), or None for a folder.
pub fn bucket_url(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    Some(format!("{scheme}://{}", rest.split('/').next().unwrap_or_default()))
}

/// A lake's store, counting the requests that cost the most and meet a bucket's rate limits
/// (S3: 3,500 writes a second per prefix): writes, lists and deletes (`GET /metrics`).
#[derive(Debug)]
pub(crate) struct Counted(pub(crate) Store);

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
        Ok(Box::new(CountedParts(self.0.put_multipart_opts(at, opts).await?)))
    }
    async fn get_opts(&self, at: &Path, opts: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        crate::metrics::add(&crate::metrics::OBJECT_READS, 1); // (a HEAD too: billed as a GET)
        self.0.get_opts(at, opts).await
    }
    fn delete_stream(&self, at: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        use futures::StreamExt;
        self.0.delete_stream(at.inspect(|_| crate::metrics::add(&crate::metrics::OBJECT_DELETES, 1)).boxed())
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        crate::metrics::add(&crate::metrics::OBJECT_LISTS, 1);
        self.0.list(prefix)
    }
    // (forwarded: the store's own offset, not the trait's default that lists everything)
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        crate::metrics::add(&crate::metrics::OBJECT_LISTS, 1);
        self.0.list_with_offset(prefix, offset)
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

/// A multipart upload's parts and its completion, each a request the bucket bills as a write.
#[derive(Debug)]
struct CountedParts(Box<dyn object_store::MultipartUpload>);

#[async_trait::async_trait]
impl object_store::MultipartUpload for CountedParts {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1);
        self.0.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        crate::metrics::add(&crate::metrics::OBJECT_WRITES, 1);
        self.0.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> { self.0.abort().await }
}

/// A local folder's store. `LocalFileSystem` refuses a conditional overwrite (`PutMode::Update`), and
/// SlateDB's collector advances its boundary files with one: from the collector's second pass on, its
/// manifest and compactions passes failed and deleted nothing. So an `Update` here is a head and an
/// overwrite with the e-tag checked between them (a folder's version is its e-tag alone: `version` is
/// None there); every other call is the folder's own.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub(crate) struct Local(pub(crate) LocalFileSystem);

/// One process's conditional updates of a folder take turns here, so a check and the overwrite after it
/// never interleave. (Not `Lake::replace`'s `ONE`, which is held across its own put.)
#[cfg(not(target_arch = "wasm32"))]
static UPDATES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl ObjectStore for Local {
    async fn put_opts(&self, at: &Path, body: object_store::PutPayload, opts: PutOptions) -> object_store::Result<object_store::PutResult> {
        let PutMode::Update(version) = &opts.mode else { return self.0.put_opts(at, body, opts).await };
        let _turn = UPDATES.lock().await;
        let refused = |why: String| object_store::Error::Precondition { path: at.to_string(), source: why.into() };
        let current = match self.0.head(at).await {
            Ok(meta) => meta.e_tag,
            Err(object_store::Error::NotFound { .. }) => return Err(refused("there is no object to update".into())),
            Err(e) => return Err(e),
        };
        if current != version.e_tag {
            return Err(refused(format!("its e-tag is {current:?}, not {:?}", version.e_tag)));
        }
        // (the attributes stay: LocalFileSystem refuses them by name, before it writes anything)
        self.0.put_opts(at, body, PutOptions { mode: PutMode::Overwrite, ..opts }).await
    }
    async fn put_multipart_opts(&self, at: &Path, opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.put_multipart_opts(at, opts).await
    }
    async fn get_opts(&self, at: &Path, opts: object_store::GetOptions) -> object_store::Result<object_store::GetResult> { self.0.get_opts(at, opts).await }
    async fn get_ranges(&self, at: &Path, ranges: &[std::ops::Range<u64>]) -> object_store::Result<Vec<bytes::Bytes>> { self.0.get_ranges(at, ranges).await }
    fn delete_stream(&self, at: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> { self.0.delete_stream(at) }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> { self.0.list(prefix) }
    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> { self.0.list_with_offset(prefix, offset) }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> { self.0.list_with_delimiter(prefix).await }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: object_store::CopyOptions) -> object_store::Result<()> { self.0.copy_opts(from, to, opts).await }
    async fn rename_opts(&self, from: &Path, to: &Path, opts: object_store::RenameOptions) -> object_store::Result<()> { self.0.rename_opts(from, to, opts).await }
}

/// What a node still catching up with its leader answers (57P03: it ran nothing, ask again or
/// another node; `Lake::caught_up`).
pub const CATCHING_UP: &str = "this node just started and is still catching up with its leader: try again in a moment, or ask another node";

/// A lake read with a read-only key (ADR-058): nothing in this process writes or deletes there,
/// whatever the key allows (the bucket would refuse too; this says why first).
#[derive(Debug)]
struct ReadOnly(Store, String);

/// What a write to a lake read with a read-only key gets back.
fn refused(lake: &str, path: &str) -> object_store::Error {
    object_store::Error::PermissionDenied { path: path.to_string(), source: format!("{lake} is read with a read-only key here: nothing on this server writes it (ADR-058)").into() }
}

impl std::fmt::Display for ReadOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{} (read only)", self.0) }
}

#[async_trait::async_trait]
impl ObjectStore for ReadOnly {
    async fn put_opts(&self, at: &Path, _: object_store::PutPayload, _: PutOptions) -> object_store::Result<object_store::PutResult> { Err(refused(&self.1, &at.to_string())) }
    async fn put_multipart_opts(&self, at: &Path, _: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> { Err(refused(&self.1, &at.to_string())) }
    async fn get_opts(&self, at: &Path, opts: object_store::GetOptions) -> object_store::Result<object_store::GetResult> { self.0.get_opts(at, opts).await }
    fn delete_stream(&self, at: futures::stream::BoxStream<'static, object_store::Result<Path>>) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        use futures::StreamExt;
        let lake = self.1.clone();
        at.map(move |p| Err(refused(&lake, &p.map(|p| p.to_string()).unwrap_or_default()))).boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> { self.0.list(prefix) }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> { self.0.list_with_delimiter(prefix).await }
    async fn copy_opts(&self, _: &Path, to: &Path, _: object_store::CopyOptions) -> object_store::Result<()> { Err(refused(&self.1, &to.to_string())) }
}

/// A session with every function a query's session has but what needs a lake: the engine's (`engine::session`, with
/// Postgres's JSON operators and functions), and the node's own. A node's sessions start from it (`Lake::made`), and so do
/// the expressions a table keeps (`defaults.rs`: CHECK, DEFAULT and expectations), so a condition planned in a query plans
/// there too.
pub(crate) fn functions(config: SessionConfig, rt: Arc<RuntimeEnv>) -> SessionContext {
    let ctx = crate::engine::session(config, rt);
    crate::ai::register(&ctx); // ai_complete, ai_embed, cosine_similarity, …
    crate::panics::test_function(&ctx); // pondra_panic(), for the tests only
    ctx
}

/// How often a reader polls the bucket for newer manifests and WAL: a node's, and the CLI's.
pub const READER_POLL: Duration = Duration::from_millis(250);

/// A guest that reads reads one snapshot, for one statement, and never polls: its life is far shorter than this.
pub const READER_ONE_SNAPSHOT: Duration = Duration::from_secs(3600);

impl Lake {
    /// `writer`: the leader. `streamed`: a follower, which also gets every commit streamed from
    /// the leader, so its own catalog view only needs the leader's checkpoints (no log replay).
    /// What opening a folder with no catalog says (a follower of a leader still making one waits).
    pub const NO_LAKE: &'static str = "holds no lake yet";

    pub async fn open(url: &str, writer: bool, streamed: bool) -> Result<Arc<Lake>> {
        Self::open_polled(url, writer, streamed, READER_POLL).await
    }

    /// `open`, a reader polling the bucket every `poll` (`READER_POLL`, or `READER_ONE_SNAPSHOT` for a guest's one statement).
    pub async fn open_polled(url: &str, writer: bool, streamed: bool, poll: Duration) -> Result<Arc<Lake>> {
        let (url, store, bucket) = open_store(url)?;
        let memory = in_memory(&url); // (no bucket to bill or cache: its catalog's garbage goes as a local disk's)
        let local = bucket.is_none() || memory;
        let pool = Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(memory_limit()));
        // (spills go to `spill_disk`; no listing kept past its statement: DataFusion's own cache of
        // them would hide a file added to a folder outside the lake, ADR-026)
        // (Parquet footers and files' statistics kept by caches of our own, so `cached_bytes` can say
        // what they hold: DataFusion's own, at its own limits)
        use datafusion::execution::cache::cache_manager::{CacheManagerConfig, DEFAULT_FILE_STATISTICS_MEMORY_LIMIT, DEFAULT_METADATA_CACHE_LIMIT};
        let kept = (Arc::new(Footers::new(DEFAULT_METADATA_CACHE_LIMIT).with_name("DefaultFileMetadataCache")), Arc::new(FileStatistics::new(DEFAULT_FILE_STATISTICS_MEMORY_LIMIT).with_name("DefaultFileStatisticsCache")));
        let caches = CacheManagerConfig::default().with_list_files_cache_limit(0).with_file_metadata_cache(Some(kept.0.clone())).with_file_statistics_cache(Some(kept.1.clone()));
        let rt = RuntimeEnvBuilder::new().with_memory_pool(pool).with_cache_manager(caches).with_disk_manager_builder(spill_disk()).build_arc()?;
        let disk = bucket.as_ref().filter(|_| !memory).and_then(|_| disk_tier(&url, &store));
        let mut cached_store = None;
        if let Some((bucket_url, held)) = bucket.as_ref().filter(|_| memory) {
            rt.register_object_store(&url::Url::parse(bucket_url)?, Arc::new(crate::bridge::Bridge(held.clone()))); // (in memory already: read as it is)
        } else if let Some((bucket_url, s3)) = bucket {
            let prefix = url.trim_start_matches(&bucket_url).trim_start_matches('/').to_string();
            let cached = Arc::new(crate::cache::CachedStore::new(s3, cache_mb() << 20, disk.clone().map(|d| (d, prefix.clone())), &prefix));
            rt.register_object_store(&url::Url::parse(&bucket_url)?, cached.clone());
            cached_store = Some(cached);
        }
        // The catalog's own files go on the SSD tier too (next to the lake's objects), so catalog
        // reads — the leader's, a new node's, a reader's without a live leader — are local.
        let cache_dir = disk.as_ref().map(|d| d.dir.with_extension("catalog"));
        let opening = async {
            Ok::<_, anyhow::Error>(match writer {
                true => {
                    let t0 = std::time::Instant::now();
                    let kv = slate::Writer::open(store.clone(), cache_dir, local).await?;
                    trace("the catalog's writer open", t0);
                    Catalog::writer(Arc::new(kv), t0).await?
                }
                false => match Catalog::opened(store.clone(), streamed, slate::Reader::open(store.clone(), streamed, cache_dir, poll)).await {
                    Ok(cat) => cat,
                    // (no catalog at all: say so, rather than the database's own words for it)
                    Err(e) => match futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from("catalog/manifest")))).await {
                        None => anyhow::bail!("{url} {}: start one there (`pondra {url}`, or `pondra serve --dir {url}`), or make a table in it", Lake::NO_LAKE),
                        Some(_) => return Err(e),
                    },
                },
            })
        };
        // (the place's torn check runs beside the catalog's opening, so a bucket pays no extra round trip; `join`
        // and not `try_join`: a failure of one must not drop the other half way, with SlateDB's tasks still running)
        let (torn, cat) = tokio::join!(crate::memory::refuse_torn(&store, &url), opening);
        torn?;
        let cat = cat?;
        crate::format::check(&cat, &url, writer).await?; // (a lake a newer Pondra wrote: refused before its tables are read, ADR-039)
        let hwm = watch::Sender::new(cat.get::<u64>("n").await?.unwrap_or(1) - 1);
        cat.frozen.store(cat.get::<serde_json::Value>(crate::backup::KEY).await?.is_some(), Relaxed); // (a backup: ADR-070)
        let lake = Arc::new_cyclic(|me| Lake { url, store, cat, hwm, backlog: Default::default(), rt, tail: Default::default(), tails: Mutex::new((lru::LruCache::unbounded(), 0)), kept, disk, groups: crate::serve::Groups::new(cache_mb() << 19), hot: Arc::new(crate::hot::Hot::new(memory)), attached: Default::default(), ids: Default::default(), cached: cached_store, caught: watch::channel(true).0, to: Default::default(), sequences: Default::default(), bases: Default::default(), sessions: Default::default(), me: me.clone() });
        crate::oom::gives_back(&lake.hot); // (the decoded columns give memory back when the node needs it: the one watch takes them, `oom.rs`)
        crate::branch::load(&lake).await?; // (a branch's files listed in its bases: ADR-047)
        if let Some(writes) = lake.cat.unstarted.lock().unwrap().take() {
            crate::panics::spawn(Lake::commits(Arc::downgrade(&lake), writes)); // (weak: a lake opened in a process goes when its last handle does)
        }
        Ok(lake)
    }

    /// Leader: commits, in write order. A write is committed — acknowledged, visible here, and
    /// streamed as committed — once it is in the bucket, or (`--ack replicated`) once
    /// `replicas - 1` member followers hold it, whichever comes first. It reaches the bucket a
    /// moment later either way. A write that fails to reach the bucket (a newer leader fenced
    /// us out) has an unknown outcome: restart and rejoin; the restart reloads the state.
    /// Both loops hold the lake weakly: the writes' sender is the lake's own (`Catalog::writes`), so
    /// a strong hold would keep a lake opened in a process (a backup taken in it) for ever.
    async fn commits(me: std::sync::Weak<Self>, mut writes: mpsc::UnboundedReceiver<Write>) {
        let (to_bucket, mut in_bucket) = mpsc::unbounded_channel::<(Arc<dyn Written>, u64)>();
        let lake = me.clone();
        crate::panics::spawn(async move {
            while let Some((handle, id)) = in_bucket.recv().await {
                if let Err(e) = handle.durable().await {
                    eprintln!("catalog commit failed: {e}");
                    crate::cluster::restart("a catalog commit failed");
                }
                let Some(lake) = lake.upgrade() else { return };
                lake.cat.durable.send_replace(id);
                lake.cat.send(Frame::Durable(id));
            }
        });
        while let Some((handle, d, done, sent)) = writes.recv().await {
            let t = commit_stage("queued", sent);
            let Some(lake) = me.upgrade() else { return };
            let cat = &lake.cat;
            let need = cat.replicas.saturating_sub(1);
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
                    r = handle.durable() => {
                        if let Err(e) = r {
                            eprintln!("catalog commit failed: {e}");
                            crate::cluster::restart("a catalog commit failed");
                        }
                        break;
                    }
                    _ = acked.changed() => {}
                }
            }
            let t = commit_stage("durable", t);
            if need == 0 {
                cat.send(Frame::Change(d.clone()));
            }
            cat.apply(&d); // our in-memory catalog
            cat.committed.store(d.id, Relaxed);
            cat.send(Frame::Committed(d.id));
            let _ = to_bucket.send((handle, d.id));
            let _ = done.send(());
            commit_stage("applied", t);
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
                (Some("t/"), disk) => {
                    let decoded = self.tail.lock().unwrap().rows.contains_key(&key[2..]);
                    if !(decoded || disk.is_some() || self.hot.on()) {
                        continue;
                    }
                    let Ok(m) = serde_json::from_slice::<TableMeta>(value) else { continue };
                    self.tail.lock().unwrap().forget(&key[2..], m.tiered); // (rows now in files: no read asks for them again)
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
    /// (`cluster::catch_up`), and refuse (57P03) if it doesn't yet. A node restarted after a
    /// failover would otherwise answer from an older catalog than it did before: its view reads no
    /// WAL, and the new leader flushes what it took over a moment after it leads (a table just
    /// made was "not found").
    pub async fn caught_up(&self) -> Result<()> {
        if !*self.caught.borrow() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), self.caught.subscribe().wait_for(|c| *c)).await;
        }
        anyhow::ensure!(*self.caught.borrow(), "{CATCHING_UP}");
        Ok(())
    }

    /// Table `name`'s entry; None only once this node holds what its leader had committed when it started (`caught_up`):
    /// a node just started, or a follower whose leader was just restarted, may not hold a table yet, and says so (57P03:
    /// a client asks again) rather than that there is none. An entry it holds answers at once.
    pub async fn table_meta_if(&self, name: &str) -> Result<Option<Arc<TableMeta>>> {
        if let Some(meta) = self.cat.meta(&table_key(name)).await? {
            return Ok(Some(meta));
        }
        self.caught_up().await?;
        self.cat.meta(&table_key(name)).await
    }

    /// Table `name`'s entry, or "no table" (42P01), as [`Lake::table_meta_if`] decides it.
    pub async fn table_meta(&self, name: &str) -> Result<Arc<TableMeta>> {
        self.table_meta_if(name).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))
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
        for (name, other) in self.attached.read().unwrap().iter().filter(|(n, _)| crate::text::mentions(&text, n)) {
            (name, other.cat.version()?, crate::auth::operator()).hash(&mut h); // (whoever runs the nodes may read what a user may not: `users::across`)
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
        spill_room(); // (the spill disk's room as it is now: `Spills` looked at it once, when the first spill made the disk)
        let state = self.sessions.lock().unwrap().entry(partitions).or_insert_with(|| self.made(partitions).state()).clone();
        let catalog = datafusion::execution::SessionStateDefaults::default_catalog(state.config(), state.table_factories(), state.runtime_env());
        let ctx = crate::engine::with_catalog(state, Arc::new(catalog));
        crate::files::register(&ctx, self.arc()); // files('…'), file_read(path) (here, not in what is kept: the lake would keep itself)
        crate::seq::register(&ctx, self.arc()); // nextval, currval, setval
        crate::ext::register_secrets(&ctx, self.arc()); // secrets()
        ctx
    }

    /// A session made from nothing, with everything every query's session has: the engine's (`engine::session`) and
    /// the node's own functions.
    fn made(&self, partitions: usize) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(partitions).with_default_catalog_and_schema(crate::ddl::lake_name(self), crate::text::PUBLIC);
        functions(config, self.rt.clone())
    }

    /// Query memory in use, and the limit.
    pub fn memory(&self) -> (usize, usize) { (self.rt.memory_pool.reserved(), memory_limit()) }

    /// What each of the node's bounded caches holds now, in bytes (`/metrics`' `cache_bytes`): what
    /// fills up to its limit is not memory the node is losing (`tools/soak.py`).
    pub fn cached_bytes(&self) -> [(&'static str, usize); 7] {
        [
            ("ranges", self.cached.as_ref().map_or(0, |c| c.held())),
            ("log", self.tail.lock().unwrap().bytes),
            ("tails", self.tails.lock().unwrap().1),
            ("lookups", self.groups.held()),
            ("catalog_blocks", self.cat.cached()),
            ("footers", self.kept.0.memory_used()),
            ("statistics", self.kept.1.memory_used()),
        ]
    }

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
        crate::budget::share(url); // (its objects read with a share of the bucket's turns: prod's own nodes come first)
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
        if self.cat.frozen.load(Relaxed) {
            return Err(crate::backup::refused(Some(&self.url))); // (a backup's objects are its runs': ADR-070)
        }
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

    /// Where `path` is read whole, past the caches: this lake's bucket, or a branch's base's (a
    /// backup copies through it, a piece at a time).
    pub fn reader(&self, path: &str) -> Result<(Store, Path)> {
        Ok(match crate::branch::split(path) {
            Some((id, rest)) => (self.base_store(id)?, Path::from(rest)),
            None => (self.store.clone(), Path::from(path)),
        })
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
        if self.cat.frozen.load(Relaxed) {
            return Err(crate::backup::refused(Some(&self.url))); // (ADR-070)
        }
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
                self.store.put_opts(&at, bytes.into(), opts(PutMode::Overwrite)).await? // (a store with no conditional put: checked just above)
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
        if self.cat.frozen.load(Relaxed) {
            return Err(crate::backup::refused(Some(&self.url))); // (ADR-070)
        }
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
        crate::per_node!(static KNOWN: Mutex<std::collections::HashMap<String, Known>> = Default::default());
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
            true => self.cat.inline(&data_key(n)).await?,
            false => self.object(&seg.path).await?,
        };
        let mut rows = vec![];
        for &(off, len, _) in parts {
            for b in crate::log::decode(&bytes[off as usize..(off + len) as usize])? {
                rows.push(crate::sys::expand(b)?); // (fresh row ids: one number in the log)
            }
        }
        let rows: Rows = Arc::new(rows);
        let tiered = self.cat.meta(&table_key(table)).await?.map_or(0, |t| t.tiered);
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
        let (Some(filed), Some(meta)) = (seg.files.get(table), self.cat.meta(&table_key(table)).await?) else { return Ok(vec![]) };
        let meta = crate::sys::with_sys(&meta);
        let schema = crate::answer::schema(&meta.columns)?;
        let (ctx, end, mut at, mut out) = (self.session(), from.saturating_add(rows), 0u64, vec![]);
        for f in if removed { &filed.removed } else { &filed.added } {
            let (lo, hi) = (at.max(from), (at + f.rows).min(end));
            if lo < hi {
                out.extend(crate::lake_scan::file_rows(self, &ctx, f, &meta, &schema, crate::lake_scan::Pick::Range(lo - at, hi - at)).await?.collect().await?);
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
        let (Some(filed), Some(meta)) = (seg.files.get(table), self.cat.meta(&table_key(table)).await?) else { return Ok(vec![]) };
        let meta = crate::sys::with_sys(&meta);
        let schema = crate::answer::schema(&meta.columns)?;
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

/// Tables' entries as parsed (`Catalog::parsed`), by key, each with the raw bytes it was parsed from.
/// Bounded by count and by those bytes: an entry of 128 files' statistics is large, and every query fills
/// the cache, so the least recently used go first.
struct Parsed {
    by_key: lru::LruCache<String, (Bytes, Arc<TableMeta>)>, // (unbounded: `put` trims it)
    bytes: usize,                                           // the raw bytes the entries hold
}

const PARSED_COUNT: usize = 256;
const PARSED_BYTES: usize = 16 << 20;

impl Parsed {
    fn new() -> Self { Parsed { by_key: lru::LruCache::unbounded(), bytes: 0 } }

    /// The entry parsed from these bytes, if it was parsed from bytes that are these.
    fn get(&mut self, key: &str, raw: &Bytes) -> Option<Arc<TableMeta>> {
        let (held, m) = self.by_key.get(key)?;
        same_bytes(held, raw).then(|| m.clone())
    }

    /// Keeps `m`, parsed from `raw`, unless `raw` alone is larger than the byte bound.
    fn put(&mut self, key: &str, raw: &Bytes, m: &Arc<TableMeta>) {
        if let Some((old, _)) = self.by_key.pop(key) {
            self.bytes -= old.len();
        }
        if raw.len() > PARSED_BYTES {
            return;
        }
        self.by_key.put(key.to_string(), (raw.clone(), m.clone()));
        self.bytes += raw.len();
        while self.by_key.len() > PARSED_COUNT || self.bytes > PARSED_BYTES {
            let Some((_, (old, _))) = self.by_key.pop_lru() else { break };
            self.bytes -= old.len();
        }
    }
}

/// Whether two buffers hold the same bytes. A buffer held by `Parsed` is never freed, and `Bytes` never
/// change, so the same start and length is the same content; any other pair is compared, which is cheap
/// next to a parse.
fn same_bytes(a: &Bytes, b: &Bytes) -> bool {
    (a.as_ptr() == b.as_ptr() && a.len() == b.len()) || a == b
}

/// The keys the node holds, each with the commit that put it and its value (None: deleted, kept as a tombstone where the
/// node doesn't hold the whole catalog). It counts the live producers' marks (`p/`) and expiries (`g/`, not the old marks'
/// `g/~` pass) as they change, under the same lock, so a scrape reads two numbers instead of scanning the range
/// (`Catalog::tallies`). Every change goes through the methods below, which keep the counts.
#[derive(Default)]
struct Overlay {
    map: BTreeMap<String, (u64, Option<Bytes>)>,
    marks: usize,
    expiries: usize,
}

impl std::ops::Deref for Overlay {
    type Target = BTreeMap<String, (u64, Option<Bytes>)>;
    fn deref(&self) -> &Self::Target { &self.map }
}

impl Overlay {
    /// Puts `k` as `v`, whatever was there.
    fn set(&mut self, k: String, v: (u64, Option<Bytes>)) {
        let (was, is) = (self.map.get(&k).is_some_and(|(_, old)| old.is_some()), v.1.is_some());
        self.map.insert(k.clone(), v);
        self.tally(&k, was, is);
    }

    /// Puts `k` as `v` if nothing is there.
    fn insert_absent(&mut self, k: String, v: (u64, Option<Bytes>)) {
        if !self.map.contains_key(&k) {
            self.set(k, v);
        }
    }

    fn remove(&mut self, k: &str) {
        if let Some((_, v)) = self.map.remove(k) {
            self.tally(k, v.is_some(), false);
        }
    }

    /// Keeps the entries `keep` says to keep.
    fn retain(&mut self, mut keep: impl FnMut(&str, &(u64, Option<Bytes>)) -> bool) {
        let (mut marks, mut expiries) = (0, 0);
        self.map.retain(|k, v| {
            let kept = keep(k, v);
            if !kept && v.1.is_some() {
                match Self::family(k) {
                    Some(true) => marks += 1,
                    Some(false) => expiries += 1,
                    None => {}
                }
            }
            kept
        });
        self.marks -= marks;
        self.expiries -= expiries;
    }

    fn clear(&mut self) {
        self.map.clear();
        (self.marks, self.expiries) = (0, 0);
    }

    /// Whether `k` is a producer's mark (`Some(true)`), an expiry (`Some(false)`), or neither.
    fn family(k: &str) -> Option<bool> {
        if k.starts_with("p/") {
            Some(true)
        } else if k.starts_with(crate::log::EXPIRY) && !k.starts_with("g/~") {
            Some(false)
        } else {
            None
        }
    }

    /// A key went from live (`was`) to live or deleted (`is`): counted if it is one of the two families.
    fn tally(&mut self, k: &str, was: bool, is: bool) {
        if was == is {
            return;
        }
        let count = match Self::family(k) {
            Some(true) => &mut self.marks,
            Some(false) => &mut self.expiries,
            None => return,
        };
        if is {
            *count += 1;
        } else {
            *count -= 1;
        }
    }
}

/// The catalog is a key-value store inside the lake (`kv`: SlateDB on a node).
/// One process holds the writer (the store fences out any other); any number can read.
pub struct Catalog {
    kv: Arc<dyn Kv>,
    order: tokio::sync::Mutex<u64>, // leader: next commit id; held while writing, so ids follow write order
    flushed: AtomicU64,             // leader: `order` at the last memtable flush
    flushed_at: Mutex<Instant>,     // leader: when the catalog last had nothing unflushed (`checkpoint`)
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
    overlay: Mutex<Overlay>,
    view: (AtomicU64, AtomicU64), // our own view's "c" and the "n" it had at or before that "c"
    pruned: AtomicU64,            // streamed changes up to this commit were dropped (the view has them)
    pins: Mutex<BTreeMap<u64, usize>>, // scans in progress, by the view "c" they started from
    streamed: AtomicU64,          // the last commit streamed to us; with the view: the whole lake
    hold: AtomicU64,              // commits before this one never arrived: read from the view alone
    follows: bool,                // follower / read-only node that gets the commit stream
    mirror: AtomicBool,           // the overlay holds the whole catalog (but inline data): read only it
    loud: AtomicU64,              // the last commit a remembered answer may depend on (`quiet`, `version`)
    metas: Mutex<Parsed>,         // tables' entries as parsed (`parsed`)
    pub frozen: AtomicBool,       // a backup (ADR-070): written only by its runs (`backup::may_write`)
    closing: AtomicBool,          // its own node's stop closed it (`close`): an error from it is no fence
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
type Write = (Arc<dyn Written>, Arc<Delta>, oneshot::Sender<()>, Instant); // (when it was handed to `commits`: `commit_stage`)

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

/// The node's own records, which its writer appends (`history::writer`) and no remembered answer
/// reads (`App::remembers`): the statements' history and the audit log.
const RECORDS: [&str; 2] = [crate::history::TABLE, crate::audit::TABLE];

/// A commit no remembered answer depends on (`Catalog::version`): only the records' rows, their
/// producers' progress and their tables' entries (`history.rs`, every second on every node; the
/// audit log 50 ms after a DDL statement or a refusal), the lake's own keys (`users::KEYS`, written
/// beside serving when the lake is made, and again when sealed), retention letting segments go and
/// marked `QUIET`, or numbers taken (sequences' and row ids' blocks, a bulk write's commit:
/// `log::commit`, marked too). Counting them, every remembered answer was forgotten every second on
/// a lake nobody wrote to, again 50 ms after each `CREATE TABLE`, and once just after a new lake served.
/// Producers' expiries and the old marks' pass (`log::EXPIRY`), and the marks they delete (`p/`), count
/// neither way: a commit is as quiet as it is without them.
fn quiet(d: &Delta) -> bool {
    let hex = |id: &str| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit());
    let expiry = |k: &str| k.starts_with(crate::log::EXPIRY);
    let unread = |k: &str| {
        k == crate::users::KEYS
            || RECORDS.iter().any(|t| k.strip_prefix("t/") == Some(t) || k.strip_prefix("p/").and_then(|p| p.strip_prefix(t.trim_start_matches("pondra$"))).and_then(|p| p.strip_prefix('-')).is_some_and(hex))
    };
    let marked = d.puts.iter().any(|(k, _)| k == QUIET);
    let only = |s: &Segment| s.files.is_empty() && s.parts.keys().all(|t| RECORDS.contains(&t.as_str()));
    d.deletes.iter().all(|k| k.starts_with("s/") || k.starts_with("d/") || k.starts_with("p/") || expiry(k))
        && (marked || d.puts.iter().any(|(k, _)| unread(k)))
        && d.puts.iter().all(|(k, v)| {
            matches!(k.as_str(), "c" | "n" | "b" | QUIET) || k.starts_with("d/") || unread(k) || expiry(k) || (marked && (k.starts_with("t/") || k.starts_with("sq/")))
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
    if crate::env::is_set("PONDRA_TRACE_START") {
        eprintln!("start: {what} {:.2} s", since.elapsed().as_secs_f64());
    }
}

crate::per_node!(
    /// Has the node begun to serve? (Then the catalog's upkeep starts: `kv::slate`.)
    pub(crate) static SERVING: watch::Sender<bool> = watch::Sender::new(false));

/// The node serves now (`main.rs`).
pub fn serving() { SERVING.send_replace(true); }

/// DataFusion's caches of Parquet footers and of files' statistics (`Lake::kept`).
type Footers = datafusion::execution::cache::default_cache::DefaultCache<object_store_df::path::Path, datafusion::execution::cache::cache_manager::CachedFileMetadataEntry>;
type FileStatistics = datafusion::execution::cache::default_cache::DefaultCache<datafusion::execution::cache::TableScopedPath, datafusion::execution::cache::cache_manager::CachedFileMetadata>;

/// A catalog let go stops what its store runs for it: a reader's polling, a writer's flushing and its
/// upkeep. SlateDB's tasks outlive its handles, so a lake opened for a moment (a branch's base, a
/// lake detached, a node stopped in a host's process) went on reading its manifest four times a
/// second for the life of the process. (`close` does the same and waits for it: `Node::stop`.)
impl Drop for Catalog {
    fn drop(&mut self) {
        if tokio::runtime::Handle::try_current().is_err() {
            return; // (no runtime: it ended, and SlateDB's tasks with it)
        }
        let kv = self.kv.clone();
        crate::panics::task(async move {
            let _ = kv.close().await;
        });
    }
}

impl Catalog {
    /// The catalog as its leader opens it, on `kv` (the leader's store, `slate::Writer::open`). `since`: when its
    /// opening began, for `PONDRA_TRACE_START`.
    async fn writer(kv: Arc<dyn Kv>, since: Instant) -> Result<Self> {
        let mut cat = Self::new(kv);
        cat.replicas = crate::env::or("PONDRA_REPLICAS", 1);
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
        let mut all = cat.kv.scan(b"".to_vec()..b"d/".to_vec(), false).await.map_err(fatal)?;
        all.extend(cat.kv.scan(b"d0".to_vec()..vec![0xff], false).await.map_err(fatal)?);
        trace("the catalog in memory", since);
        let overlay = cat.overlay.get_mut().unwrap();
        for (k, v) in all {
            overlay.set(k, (c, Some(v)));
        }
        cat.streamed.store(c, Relaxed);
        cat.loud.store(c, Relaxed);
        cat.mirror.store(true, Relaxed);
        Ok(cat)
    }

    /// The catalog as a follower or a reader opens it, on `kv` (`slate::Reader::open`).
    async fn reader(kv: Arc<dyn Kv>, streamed: bool) -> Result<Self> {
        let mut cat = Self::new(kv);
        cat.follows = streamed;
        cat.refresh().await?; // (one try at the in-memory catalog before serving; later refreshes retry)
        Ok(cat)
    }

    /// A reader, or why there can't be one: SlateDB tries again for as long as the bucket says no,
    /// so a bucket that isn't there, or that refuses the credentials, left `pondra sql` waiting
    /// for ever. When opening takes a while, a listing (after the store's own retries) says
    /// whether there is a catalog to wait for. `reader_open` is the reader's opening: this waits
    /// for it, and for the catalog's first view of the bucket, under the one timeout.
    async fn opened(store: Store, streamed: bool, reader_open: impl std::future::Future<Output = Result<slate::Reader, kv::Error>>) -> Result<Self> {
        let open = async { Self::reader(Arc::new(reader_open.await?), streamed).await };
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

    fn new(kv: Arc<dyn Kv>) -> Self {
        let (feed, order) = (broadcast::channel(4096).0, tokio::sync::Mutex::new(1));
        let (last_n, view, pruned, pins, streamed, hold, mirror, flushed, committed, loud) = Default::default();
        let (durable, acked, acks, pending, unstarted) = (watch::Sender::new(0), watch::Sender::new(()), Default::default(), Default::default(), Default::default());
        Catalog { kv, order, flushed, flushed_at: Mutex::new(Instant::now()), writes: None, unstarted, committed, durable, replicas: 1, acks, acked, pending, feed, recent: Default::default(), last_n, overlay: Default::default(), view, pruned, pins, streamed, hold, follows: false, mirror, loud, metas: Mutex::new(Parsed::new()), frozen: Default::default(), closing: Default::default() }
    }

    /// What the catalog's block caches hold, in bytes.
    pub fn cached(&self) -> usize { self.kv.cached() }

    /// Leader: the recent frames plus a receiver for every frame from now on.
    pub fn subscribe(&self) -> (Vec<Frame>, broadcast::Receiver<Frame>) {
        let rx = self.feed.subscribe();
        (self.recent.lock().unwrap().0.iter().map(|(_, f)| f.clone()).collect(), rx)
    }

    /// Inline data (`d/`) that one of the last 64 commits wrote: it never changes, so the frame's copy is always good.
    fn recent_inline(&self, key: &str) -> Option<Bytes> {
        let r = self.recent.lock().unwrap();
        let found = r.0.iter().rev().filter_map(|(_, f)| match f {
            Frame::Change(d) => Some(d),
            _ => None,
        }).take(64).find_map(|d| d.puts.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()));
        found
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
        for (k, v) in d.puts.iter().filter(|(k, _)| !(writer && k.starts_with("d/"))) {
            o.set(k.clone(), (d.id, Some(v.clone())));
        }
        match self.mirror.load(Relaxed) {
            true => d.deletes.iter().for_each(|k| o.remove(k)),
            false => d.deletes.iter().for_each(|k| o.set(k.clone(), (d.id, None))),
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
        if self.kv.is_writer() {
            return Ok(0);
        }
        Ok(self.kv.get("c", false).await?.map(|v| serde_json::from_slice(&v)).transpose()?.unwrap_or(0))
    }

    /// Non-leader: move our view's marks to where the reader is now. Returns its commit and the
    /// oldest commit a scan still in progress (it started from an older view) holds.
    async fn mark(&self) -> Result<(u64, u64)> {
        if self.kv.is_writer() {
            return Ok((0, 0));
        }
        let get = |k: &'static str| async move { Ok::<u64, anyhow::Error>(self.kv.get(k, false).await?.map(|v| serde_json::from_slice(&v)).transpose()?.unwrap_or(0)) };
        // "n" BEFORE "c": our view only moves forward, so this `n` is one our view already had
        // at commit `c` (the other way round it could be from a newer view than `c`, and reads
        // held back to an older commit would miss segments this node thought it could read).
        let (n, c) = (get("n").await?.max(1), get("c").await?);
        let pins = self.pins.lock().unwrap();
        self.view.0.fetch_max(c, Relaxed); // (queries move them too, `settle`: an older read never wins)
        self.view.1.fetch_max(n, Relaxed);
        Ok((c, pins.keys().next().map_or(c, |&p| p.min(c))))
    }

    /// Right after a query read table entries, on a node with no in-memory catalog (an attached
    /// lake, a follower between seeds): those entries are the reader as it is now, which moves on
    /// between refreshes, so the log's end the query reads to (`visible`) is brought up to them.
    /// Behind them, a change's old row was left out while its new one wasn't read yet (an attached
    /// lake's changed row missing from half the reads: `harness.py attached`). Marks only: pruning
    /// and seeding stay `refresh`'s, once a period (invariant 8).
    pub async fn settle(&self) -> Result<()> {
        match self.mirror.load(Relaxed) {
            true => Ok(()),
            false => self.mark().await.map(|_| ()),
        }
    }

    /// Non-leader: how far our own catalog view is; drop the streamed changes it now has.
    async fn refresh(&self) -> Result<()> {
        if self.kv.is_writer() {
            return Ok(());
        }
        // Drop the streamed copies our view now has, except those a scan still in progress
        // (it started from an older view) may lay over its data.
        let (c, upto) = self.mark().await?;
        {
            let mut o = self.overlay.lock().unwrap();
            if self.mirror.load(Relaxed) {
                // The mirror answers every read but inline data without our view, so commits the
                // view already has must still arrive through the stream: `pruned` stays put.
                o.retain(|k, v| v.0 > upto || !k.starts_with("d/"));
            } else {
                self.pruned.fetch_max(upto, Relaxed);
                o.retain(|_, v| v.0 > upto);
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
        if self.kv.is_writer() {
            return Ok(());
        }
        let _pin = self.pin(); // (the streamed changes after `before` stay while we read)
        let before = self.view_now().await?;
        let mut all = self.kv.scan(b"".to_vec()..b"d/".to_vec(), false).await?;
        all.extend(self.kv.scan(b"d0".to_vec()..vec![0xff], false).await?);
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
        o.retain(|k, v| v.0 > c || k.starts_with("d/"));
        for (k, v) in all {
            o.insert_absent(k, (c, Some(v)));
        }
        o.retain(|_, v| v.1.is_some()); // (deleted keys: absent from the whole catalog)
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
        if self.kv.is_writer() {
            return self.last_n.load(Relaxed);
        }
        match self.overlaid() {
            true => self.view.1.load(Relaxed).max(self.last_n.load(Relaxed)),
            false => self.view.1.load(Relaxed),
        }
    }

    pub fn is_writer(&self) -> bool { self.kv.is_writer() }

    /// The catalog version every read here reflects right now — on the leader (durable commits)
    /// or a node that holds the whole catalog in memory — or `None` where reads mix our view with
    /// the stream. A read that starts after this sees at least this version (never older). Quiet
    /// commits don't count: every read but the history's is the same across them.
    pub fn version(&self) -> Option<u64> {
        match self.kv.is_writer() {
            true => Some(self.loud.load(Relaxed).min(self.committed.load(Relaxed))), // (`apply` comes just before `committed`)
            false => self.mirror.load(Relaxed).then(|| self.loud.load(Relaxed)),
        }
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.get_raw(key).await?.map(|v| serde_json::from_slice(&v).context(key.to_string())).transpose()
    }

    /// A table's entry (`t/…`) as parsed, for a read that runs per query or request: see `parsed`.
    pub async fn meta(&self, key: &str) -> Result<Option<Arc<TableMeta>>> {
        let raw = if self.mirror.load(Relaxed) {
            self.overlay.lock().unwrap().get(key).and_then(|(_, v)| v.clone()) // (the overlay holds the whole catalog)
        } else {
            self.get_raw(key).await?
        };
        raw.map(|raw| self.parsed(key, &raw)).transpose()
    }

    /// The table entry `raw` (read for `key`) as parsed: the copy kept from the last parse when these
    /// are the bytes it was parsed from, else a new parse, which is kept. An entry of 128 files'
    /// statistics is large, and every query reads the entries of the tables it names.
    pub fn parsed(&self, key: &str, raw: &Bytes) -> Result<Arc<TableMeta>> {
        if let Some(m) = self.metas.lock().unwrap().get(key, raw) {
            return Ok(m);
        }
        let m = Arc::new(serde_json::from_slice::<TableMeta>(raw).context(key.to_string())?);
        self.metas.lock().unwrap().put(key, raw, &m);
        Ok(m)
    }

    /// `key` from this node's own view of the bucket, past the mirror: for what a follower needs
    /// before its commit stream opens, which never changes once written (the lake's own keys, which
    /// signing in to that stream takes: `users::keys`).
    pub async fn from_view(&self, key: &str) -> Result<Option<Bytes>> {
        match self.kv.is_writer() {
            true => self.get_raw(key).await,
            false => Ok(self.kv.get(key, false).await?),
        }
    }

    pub async fn get_raw(&self, key: &str) -> Result<Option<Bytes>> {
        if self.kv.is_writer() {
            return Ok(match key.starts_with("d/") {
                // The leader answers from its in-memory catalog: committed writes only, never ones
                // in flight. Inline segment data is only asked for once its segment is committed.
                true => match self.recent_inline(key) {
                    Some(bytes) => Some(bytes),
                    None => self.kv.get(key, false).await.map_err(|e| self.fenced(e))?,
                },
                false => match self.mirror.load(Relaxed) {
                    true => self.overlay.lock().unwrap().get(key).and_then(|(_, v)| v.clone()),
                    false => self.kv.get(key, true).await.map_err(|e| self.fenced(e))?,
                },
            });
        }
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
        Ok(match copy {
            Some(v) => v,
            None => self.kv.get(key, false).await?, // (read after the check: at least as new)
        })
    }

    /// The inline data (`d/`) of a segment the catalog lists, which always arrives. A reader's view can
    /// hold less than its commit number says for a moment while a new writer opens the lake, so a miss
    /// waits here (20 ms, doubling to 500 ms, 10 s in all) rather than failing the read.
    pub async fn inline(&self, key: &str) -> Result<Bytes> {
        let started = Instant::now();
        let mut wait = Duration::from_millis(20);
        loop {
            if let Some(bytes) = self.get_raw(key).await? {
                return Ok(bytes);
            }
            if self.is_writer() || started.elapsed() >= Duration::from_secs(10) {
                let n = key.strip_prefix("d/").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0);
                bail!("missing inline segment {n}");
            }
            tokio::time::sleep(wait).await;
            self.mark().await?; // (our view's marks move on, so the next read sees what it has now)
            wait = (wait * 2).min(Duration::from_millis(500));
        }
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
        if self.kv.is_writer() && !self.mirror.load(Relaxed) {
            return decode(self.kv.scan(range, true).await.map_err(|e| self.fenced(e))?);
        }
        {
            let o = self.overlay.lock().unwrap();
            if self.mirror.load(Relaxed) {
                // The in-memory catalog (scans never cover inline segment data).
                let hits = o.range(from.to_string()..to.to_string()).filter_map(|(k, (_, v))| Some((k.clone(), v.clone()?)));
                return decode(hits.collect());
            }
        }
        let _pin = self.pin();
        loop {
            // Our view is somewhere between `before` and `after` while we read it.
            let before = self.view_now().await?;
            let mut all = self.kv.scan(range.clone(), false).await?;
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

    /// The first `n` entries with keys in [from, to), in key order: a commit's bounded look at a range
    /// (producers' expiries, the old marks' pass). In memory it takes only those `n` (the overlay's range,
    /// `take`), never a copy of the whole range; without the whole catalog here, it reads all of it.
    pub async fn scan_first(&self, from: &str, to: &str, n: usize) -> Result<Vec<(String, Bytes)>> {
        if self.mirror.load(Relaxed) {
            let o = self.overlay.lock().unwrap();
            return Ok(o.range(from.to_string()..to.to_string()).filter_map(|(k, (_, v))| Some((k.clone(), v.clone()?))).take(n).collect());
        }
        Ok(self.scan_raw(from, to).await?.into_iter().take(n).collect())
    }

    /// The producers' marks (`p/` keys) and the expiries (`g/` keys) the catalog holds, when it holds the whole catalog in
    /// memory (the leader's, and a follower's once seeded); `None` otherwise. Counted as the overlay changes, so this is two
    /// numbers, not a scan under the lock (`Overlay`).
    pub fn tallies(&self) -> Option<(usize, usize)> {
        self.mirror.load(Relaxed).then(|| {
            let o = self.overlay.lock().unwrap();
            (o.marks, o.expiries)
        })
    }

    /// Leader: persist the catalog memtable if anything was committed since the last time. A
    /// restart then replays only the WAL written since, and the other nodes' views — which read
    /// no WAL — catch up (a follower whose commit stream broke reads from its view until then).
    pub async fn checkpoint(&self) -> Result<()> {
        if !self.kv.is_writer() {
            return Ok(());
        }
        let next = *self.order.lock().await; // every commit before it is in the memtable
        let flushed = self.flushed.load(Relaxed);
        if flushed != next {
            let flush = self.kv.checkpoint();
            tokio::pin!(flush);
            // (a flush that takes 30 s is still waited for, not dropped: the line says why it is slow)
            if tokio::time::timeout(Duration::from_secs(30), &mut flush).await.is_err() {
                eprintln!("the catalog's checkpoint has waited 30 s: its level-0 files wait for compaction ({} commits not yet in the bucket's catalog)", next - flushed);
                flush.await?;
            }
            self.flushed.store(next, Relaxed);
        }
        // (a catalog with nothing unflushed counts from here, so a commit after a quiet spell isn't seen as one that waited all of it)
        *self.flushed_at.lock().unwrap() = Instant::now();
        Ok(())
    }

    /// Leader: how long, in seconds, the catalog has held commits that no flush has persisted (0 when it has none).
    /// `/metrics` reads it. It reads `committed` and not `order`, which a commit holds while it writes: a stalled
    /// bucket must not stall the scrape that says so. `order` is `committed` + 1 once no commit is in flight, so
    /// commits are unflushed exactly when `committed` has reached `flushed`.
    pub fn unflushed_seconds(&self) -> f64 {
        // (a reader's `committed` and `flushed` are both 0: without this it would count from its open)
        if !self.is_writer() || self.committed.load(Relaxed) < self.flushed.load(Relaxed) {
            return 0.0;
        }
        self.flushed_at.lock().unwrap().elapsed().as_secs_f64()
    }

    /// Lets go of the catalog: its writer flushes what it holds and stops its background tasks, a
    /// reader its polling (`Node::stop`, ADR-061 §4: one process may open another lake after it).
    pub async fn close(&self) -> Result<()> {
        self.closing.store(true, Relaxed);
        Ok(self.kv.close().await?)
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
        let (true, Some(writes)) = (self.kv.is_writer(), &self.writes) else { bail!("read-only node") };
        if self.frozen.load(Relaxed) && !crate::backup::may_write(puts.iter().map(|(k, _)| k).chain(deletes)) {
            return Err(crate::backup::refused(None)); // (whatever door a write came through: ADR-070)
        }
        let t0 = Instant::now();
        let mut puts: Vec<(String, Bytes)> = puts.into_iter().map(|(k, v)| (k, v.into())).collect();
        let mut id = self.order.lock().await;
        let t1 = commit_stage("order", t0);
        // The commit number is the last put: the store takes the puts as they are, and the `Delta` keeps the same list.
        puts.push(("c".to_string(), Bytes::from(json(&*id))));
        let handle = self.kv.write(&puts, deletes).await.map_err(|e| self.fenced(e))?;
        let t2 = commit_stage("kv write", t1);
        if let Some((_, v)) = puts.iter().find(|(k, _)| k == "n") {
            self.last_n.store(serde_json::from_slice(v)?, Relaxed);
        }
        let (done, rx) = oneshot::channel();
        let _ = writes.send((handle, Arc::new(Delta { id: *id, puts, deletes: deletes.to_vec() }), done, t2));
        *id += 1;
        Ok(rx)
    }
}


/// `PONDRA_TRACE_COMMIT=1`: where a leader's commits spend their time. Each stage is the time since the one before it
/// (`since`); once a minute one line on stderr gives each stage's p50 and p95 over that minute, and how late a 10 ms
/// sleep woke (`lag`: the runtime's own wait, which every stage pays when the node's cores are busy). Returns now, the
/// next stage's start.
pub fn commit_stage(stage: &'static str, since: Instant) -> Instant {
    type Stages = Mutex<(Instant, Vec<(&'static str, Vec<f64>)>)>;
    static ON: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
        let on = crate::env::is_set("PONDRA_TRACE_COMMIT");
        if on {
            crate::panics::spawn(async {
                loop {
                    let t = Instant::now();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    commit_stage("lag", t + Duration::from_millis(10));
                }
            });
        }
        on
    });
    crate::per_node!(static STAGES: Stages = Mutex::new((Instant::now(), vec![])); clear = |s| *s.lock().unwrap_or_else(|e| e.into_inner()) = (Instant::now(), vec![]));
    let now = Instant::now();
    if !*ON {
        return now;
    }
    let mut s = STAGES.lock().unwrap();
    let ms = now.saturating_duration_since(since).as_secs_f64() * 1e3;
    match s.1.iter_mut().find(|(n, _)| *n == stage) {
        Some((_, v)) => v.push(ms),
        None => s.1.push((stage, vec![ms])),
    }
    if now - s.0 >= Duration::from_secs(60) {
        let line: Vec<String> = s.1.iter_mut().map(|(n, v)| {
            v.sort_by(f64::total_cmp);
            let q = |p: f64| v[((v.len() - 1) as f64 * p) as usize];
            format!("{n} {:.3}/{:.3} ({})", q(0.5), q(0.95), v.len())
        }).collect();
        eprintln!("commit stages p50/p95 ms (count): {}", line.join(", "));
        *s = (now, vec![]);
    }
    now
}

impl Catalog {
    /// `fatal`, unless this node's own stop closed the catalog: then the write or read in flight
    /// fails and the stop goes on to give up the term. Restarting there led the term again, and its
    /// followers waited out the lease (`upgrade_check.py rolling`: a batch waited 5.8 s).
    fn fenced(&self, e: kv::Error) -> anyhow::Error {
        if self.closing.load(Relaxed) {
            return e.into();
        }
        fatal(e)
    }
}

/// A writer that was fenced out (a new leader took over) must stop at once: it restarts and
/// rejoins the cluster as a follower. (The upkeep's failures take this too: `kv::slate`.)
pub(crate) fn fatal(e: kv::Error) -> anyhow::Error {
    if e.closed() {
        eprintln!("catalog closed: {e}");
        crate::cluster::restart("the catalog was closed (fenced by a newer leader)");
    }
    e.into()
}

/// Test hook: `PONDRA_CRASH=after_seg_put:0.05,after_parquet_put:0.2` aborts the process at
/// that point with that probability, to prove crashes never lose or duplicate rows.
pub fn maybe_crash(point: &str) {
    if crash_rolled(point) {
        if crate::library::on() {
            panic!("PONDRA_CRASH at {point}"); // (a library's node fails, its host stays: a vital loop's panic)
        }
        eprintln!("PONDRA_CRASH: aborting at {point}");
        std::process::abort();
    }
}

/// Whether `PONDRA_CRASH` names `point` and its roll comes up: `maybe_crash`'s test, for a point that
/// fails an operation (`memory::settle`'s second pass) rather than the process.
pub fn crash_rolled(point: &str) -> bool {
    let Some(spec) = crate::env::get("PONDRA_CRASH") else { return false };
    spec.split(',').filter_map(|p| p.split_once(':')).any(|(name, prob)| {
        let roll = (uuid::Uuid::new_v4().as_u128() % 1_000_000) as f64 / 1e6;
        name == point && roll < prob.parse().unwrap_or(0.0)
    })
}

/// How many partitions a query runs in here: one per core (`PONDRA_CORES` stands for another
/// machine's count), at least 2, so every node plans aggregations as partial + final (see
/// spmd.rs), and at most one per 24 MB of query memory. Each partition's sort keeps 10 MB aside to
/// merge what it spilled (DataFusion's default); on 4 cores with 50 MB the reserves took the
/// budget and the merge above them failed, and smaller reserves can't merge at all.
pub fn partitions() -> usize {
    static PARTS: std::sync::OnceLock<usize> = std::sync::OnceLock::new(); // (every session asks: the cores are read from the cgroup's files)
    *PARTS.get_or_init(|| {
        let cores = crate::env::parse("PONDRA_CORES").unwrap_or_else(|| std::thread::available_parallelism().map_or(2, |n| n.get()));
        cores.min(memory_limit() / (24 << 20)).max(2)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `resident` counts bytes: `/proc/self/statm` counts pages, and the page size is the OS's (64 KB on
    /// some arm64 builds), so it agrees with `VmRSS` in kilobytes. (On 4 KB pages the old constant passed
    /// too; it is the arm64 case this guards.)
    #[cfg(target_os = "linux")]
    #[test]
    fn resident_is_in_bytes_as_the_page_size_says() {
        let block = vec![1u8; 100 << 20]; // (written, so resident)
        std::hint::black_box(&block);
        let ours = resident().expect("resident memory");
        let status = std::fs::read_to_string("/proc/self/status").expect("the process's status");
        let kb: usize = status.lines().find_map(|l| l.strip_prefix("VmRSS:")?.trim().strip_suffix("kB")?.trim().parse().ok()).expect("VmRSS");
        let theirs = kb << 10;
        assert!(ours.abs_diff(theirs) * 4 <= theirs, "resident {ours} bytes, VmRSS {theirs} bytes");
    }

    #[test]
    fn catching_up_is_cannot_connect_now() {
        assert_eq!(crate::codes::of(&anyhow::anyhow!("{}", CATCHING_UP)), "57P03");
    }

    /// What a read never sees is quiet, and anything else in the same commit makes it loud
    /// (invariant 224): `harness.py remembered` asks around the audit log's line and a new lake's keys.
    #[test]
    fn quiet_is_what_no_read_sees() {
        let id = "0123456789abcdef0123456789abcdef";
        let seg = |table: &str| serde_json::json!({"path": "", "parts": {table: [[0, 10, 1]]}, "ts_ms": 1}).to_string();
        let quiet_of = |puts: &[(&str, &str)]| quiet(&Delta { id: 9, puts: puts.iter().map(|(k, v)| (k.to_string(), Bytes::from(v.to_string()))).collect(), deletes: vec![] });
        let audit = seg(crate::audit::TABLE);
        let (history, user) = (seg(crate::history::TABLE), seg("r_t"));
        let (audit_p, history_p) = (format!("p/audit-{id}"), format!("p/history-{id}"));
        assert!(quiet_of(&[("t/pondra$audit", "{}"), ("c", "9")]), "the audit log's table made");
        assert!(quiet_of(&[("d/3", "x"), ("s/3", &audit), (&audit_p, "1"), ("n", "4"), ("c", "9")]), "the audit log's rows");
        assert!(quiet_of(&[("d/4", "x"), ("s/4", &history), (&history_p, "1"), ("n", "5"), ("c", "9")]), "the history's rows");
        assert!(quiet_of(&[("n", "4"), ("b", "2"), (QUIET, "true"), ("c", "9")]), "a row-id block taken");
        assert!(quiet_of(&[(crate::users::KEYS, "{}"), ("c", "9")]), "the lake's own keys");
        assert!(!quiet_of(&[("n", "4"), ("b", "2"), ("c", "9")]), "unmarked numbers are loud");
        assert!(!quiet_of(&[("t/r_t", "{}"), ("c", "9")]), "a table's entry");
        assert!(!quiet_of(&[("d/3", "x"), ("s/3", &user), ("p/sql:1", "1"), ("n", "4"), ("c", "9")]), "a table's rows");
        assert!(!quiet_of(&[("d/3", "x"), ("s/3", &user), (QUIET, "true"), ("c", "9")]), "a table's rows, marked");
        assert!(!quiet_of(&[("p/audit-x", "1"), ("c", "9")]), "a producer only named like the audit log's");
        assert!(!quiet_of(&[("d/3", "x"), ("s/3", &audit), (&audit_p, "1"), ("v/a_view", "{}"), ("c", "9")]), "the audit log's rows with a view's entry");
        // Expiries, the old marks' marker and the marks they delete count neither way (`log::EXPIRY`).
        let expiry = |due: u64, p: &str| format!("{}{due:020}/{p}", crate::log::EXPIRY);
        let quiet_with = |puts: &[(&str, &str)], deletes: &[&str]| quiet(&Delta { id: 9, puts: puts.iter().map(|(k, v)| (k.to_string(), Bytes::from(v.to_string()))).collect(), deletes: deletes.iter().map(|k| k.to_string()).collect() });
        let (expiring, marker) = (expiry(5, "sql:auto-x"), format!("{}~old", crate::log::EXPIRY));
        assert!(quiet_with(&[("d/4", "x"), ("s/4", &history), (&history_p, "1"), ("n", "5"), ("c", "9")], &[&expiring, "p/sql:auto-y"]), "the history's rows, with marks expiring and gone");
        assert!(quiet_with(&[("d/4", "x"), ("s/4", &history), (&history_p, "1"), (&expiring, "1"), (&marker, "\"Done\""), ("n", "5"), ("c", "9")], &[]), "the history's rows, with expiries and the marker put");
        assert!(quiet_with(&[(QUIET, "true"), (&expiring, "1"), ("n", "4"), ("b", "2"), ("c", "9")], &[]), "a row-id block taken, with an expiry put");
        assert!(!quiet_with(&[("d/3", "x"), ("s/3", &user), (&expiring, "1"), ("n", "4"), ("c", "9")], &[]), "a table's rows keep the commit loud");
        assert!(!quiet_with(&[("d/3", "x"), ("s/3", &user), ("c", "9")], &["p/sql:auto-y"]), "a table's rows keep the commit loud, with a mark gone");
        assert!(!quiet_with(&[(&expiring, "1"), (&marker, "\"Done\""), ("n", "4"), ("c", "9")], &[]), "expiries alone are as loud as nothing: unmarked numbers are loud");
    }

    /// `scan_first` is the first keys of a range, in key order, and `count` counts the range as the catalog holds it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_first_takes_the_first_keys_of_a_range() -> Result<()> {
        // (a lake opened to write: its commits are applied by `Lake::open`'s `commits`, which a bare
        // `Catalog::writer` never starts, so its `commit` would wait for ever)
        let dir = std::env::temp_dir().join(format!("pondra-scan-first-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        lake.cat.commit((0..5u8).map(|i| (format!("g/{i}"), vec![i])).chain([("h/x".to_string(), vec![9])]).collect(), &[]).await?;
        let first: Vec<String> = lake.cat.scan_first("g/", "g0", 3).await?.into_iter().map(|(k, _)| k).collect();
        assert_eq!(first, ["g/0", "g/1", "g/2"]);
        assert_eq!(lake.cat.scan_first("g/", "g0", 99).await?.len(), 5);
        assert_eq!(lake.cat.tallies().map(|t| t.1), Some(5), "five expiries; `h/x` is neither");
        lake.cat.close().await?;
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A catalog its own node's stop closed answers an error, never a restart (invariant 300):
    /// taken for a fence, the write below would exec this test again.
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_by_its_own_stop_is_no_fence() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-closed-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?));
        let cat = Catalog::writer(Arc::new(slate::Writer::open(store, None, true).await?), Instant::now()).await?;
        let _ = cat.write(vec![("t/a".into(), b"1".to_vec())], &[]).await?;
        cat.close().await?;
        assert!(cat.write(vec![("t/b".into(), b"2".to_vec())], &[]).await.is_err());
        assert!(cat.get_raw("d/1").await.is_err());
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A folder's store takes the conditional overwrites SlateDB's collector makes (`Local`): an update with the
    /// e-tag a put or a head gave goes through, and the next with the e-tag that one gave too; an old e-tag, a
    /// missing object and two updates racing on one e-tag are refused as S3 refuses them; an update with an
    /// attribute is refused by name and changes nothing; creates and overwrites are as they were.
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_folder_takes_conditional_overwrites() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-local-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?));
        let at = Path::from("catalog/gc/manifest.boundary");
        let bytes = |b: &'static [u8]| object_store::PutPayload::from_static(b);
        let update = |e_tag: &str| PutOptions { mode: PutMode::Update(object_store::UpdateVersion { e_tag: Some(e_tag.into()), version: None }), ..Default::default() };
        let refused = |r: object_store::Result<object_store::PutResult>| matches!(r, Err(object_store::Error::Precondition { .. }));

        let created = store.put_opts(&at, bytes(b"1"), PutOptions { mode: PutMode::Create, ..Default::default() }).await?;
        let again = store.put_opts(&at, bytes(b"x"), PutOptions { mode: PutMode::Create, ..Default::default() }).await;
        assert!(matches!(again, Err(object_store::Error::AlreadyExists { .. })), "a create over an object: already there");
        let first = created.e_tag.expect("an e-tag");
        assert_eq!(store.head(&at).await?.e_tag.as_deref(), Some(first.as_str()), "a put's e-tag is the one a head gives");

        let second = store.put_opts(&at, bytes(b"2"), update(&first)).await?;
        assert_eq!(second.e_tag, store.head(&at).await?.e_tag, "an update's e-tag is the new object's");
        let second = second.e_tag.expect("an e-tag");
        let third = store.put_opts(&at, bytes(b"3"), update(&second)).await?.e_tag.expect("an e-tag");
        assert_eq!(store.get(&at).await?.bytes().await?, Bytes::from_static(b"3"), "the second update went through");

        assert!(refused(store.put_opts(&at, bytes(b"4"), update(&first)).await), "an old e-tag: a precondition");
        let missing = Path::from("catalog/gc/none.boundary");
        assert!(refused(store.put_opts(&missing, bytes(b"5"), update("any")).await), "a missing object: a precondition");
        assert!(store.head(&missing).await.is_err(), "and nothing was created");

        let mut attributes = object_store::Attributes::new();
        attributes.insert(object_store::Attribute::ContentType, "text/plain".into());
        let tagged = PutOptions { attributes, ..update(&third) };
        assert!(matches!(store.put_opts(&at, bytes(b"6"), tagged).await, Err(object_store::Error::NotImplemented { .. })), "an update with an attribute: refused by name");
        assert_eq!(store.get(&at).await?.bytes().await?, Bytes::from_static(b"3"), "and nothing changed");

        let (a, b) = tokio::join!(store.put_opts(&at, bytes(b"7"), update(&third)), store.put_opts(&at, bytes(b"8"), update(&third)));
        assert_eq!([a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count(), 1, "of two updates on one e-tag, one is made");
        assert!(refused(if a.is_err() { a } else { b }), "the other is a precondition");
        store.put_opts(&at, bytes(b"9"), PutOptions::default()).await?;
        assert_eq!(store.get(&at).await?.bytes().await?, Bytes::from_static(b"9"), "an overwrite is as it was");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A table entry is parsed once for its bytes: the same bytes (in the same buffer or another) give the
    /// same parse, changed bytes a new one, and what is kept stays under its count and byte bounds.
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_table_entry_is_parsed_once_per_version() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-parsed-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(Local(LocalFileSystem::new_with_prefix(&dir)?));
        let cat = Catalog::writer(Arc::new(slate::Writer::open(store, None, true).await?), Instant::now()).await?;
        let entry = |folder: &str| -> Result<Bytes> { Ok(Bytes::from(serde_json::to_vec(&TableMeta { folder: Some(folder.to_string()), ..Default::default() })?)) };
        let raw = entry("x")?;
        let first = cat.parsed("t/a", &raw)?;
        assert!(Arc::ptr_eq(&first, &cat.parsed("t/a", &raw)?), "the same bytes: the same parse");
        assert!(Arc::ptr_eq(&first, &cat.parsed("t/a", &Bytes::copy_from_slice(&raw))?), "equal bytes in another buffer: no parse");
        let second = cat.parsed("t/a", &entry("y")?)?;
        assert!(!Arc::ptr_eq(&first, &second) && second.folder.as_deref() == Some("y"), "changed bytes: a new parse");
        assert!(cat.parsed("t/a", &Bytes::from_static(b"not an entry")).is_err(), "bytes that aren't an entry are an error");
        // Entries of 1 MiB: 16 MiB of raw bytes stay at most, the least recently used out first; one of
        // more than 16 MiB is parsed for its caller and not kept.
        let big = |n: usize| -> Result<Bytes> { Ok(Bytes::from(serde_json::to_vec(&TableMeta { folder: Some(format!("{n}{}", "f".repeat(1 << 20))), ..Default::default() })?)) };
        for n in 0..40 {
            cat.parsed(&format!("t/big{n}"), &big(n)?)?;
        }
        let kept = |cat: &Catalog, key: &str| cat.metas.lock().unwrap().by_key.contains(key);
        assert!(cat.metas.lock().unwrap().bytes <= PARSED_BYTES, "the raw bytes kept are within the bound");
        assert!(kept(&cat, "t/big39") && !kept(&cat, "t/big0"), "the least recently used went first");
        let before = cat.metas.lock().unwrap().bytes;
        cat.parsed("t/huge", &Bytes::from(serde_json::to_vec(&TableMeta { folder: Some("h".repeat(PARSED_BYTES + 1)), ..Default::default() })?))?;
        assert!(!kept(&cat, "t/huge") && cat.metas.lock().unwrap().bytes == before, "an entry over the bound is not kept");
        for n in 0..300 {
            cat.parsed(&format!("t/small{n}"), &entry(&n.to_string())?)?;
        }
        assert!(cat.metas.lock().unwrap().by_key.len() <= PARSED_COUNT, "the count bound holds");
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// `recent_inline` answers a `d/` key that one of the last 64 commits wrote, and no other key: the
    /// frames are sent by hand, as the commit loop would send them, so no commit has to wait for it.
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recent_inline_answers_the_last_64_commits() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-recent-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let store: Store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&dir)?);
        let cat = Catalog::writer(Arc::new(slate::Writer::open(store, None, true).await?), Instant::now()).await?;
        let change = |id: u64, key: String, bytes: &'static [u8]| Frame::Change(Arc::new(Delta { id, puts: vec![(key, Bytes::from_static(bytes))], deletes: vec![] }));
        cat.send(change(1, data_key(1), b"rows"));
        assert_eq!(cat.recent_inline(&data_key(1)).as_deref(), Some(&b"rows"[..]));
        assert_eq!(cat.recent_inline(&data_key(2)), None);
        assert_eq!(cat.get_raw(&data_key(1)).await?.as_deref(), Some(&b"rows"[..]));
        for i in 0..64u64 {
            cat.send(change(2 + i, data_key(100 + i), b"other"));
        }
        assert_eq!(cat.recent_inline(&data_key(1)), None, "the window is the last 64 commits");
        assert_eq!(cat.recent_inline(&data_key(163)).as_deref(), Some(&b"other"[..]));
        cat.close().await?;
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A lake a process opens to write (a backup taken in it: `backup::take_here`) is let go with its
    /// last handle: the loops `Lake::open` starts hold it weakly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lake_opened_to_write_goes_with_its_last_handle() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-dropped-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        lake.cat.commit(vec![("t/a".into(), b"1".to_vec())], &[]).await?;
        lake.cat.close().await?;
        let gone = Arc::downgrade(&lake);
        drop(lake);
        for _ in 0..100 {
            if gone.upgrade().is_none() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(gone.upgrade().is_none(), "the lake is still held");
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// A listed segment's inline rows can reach a reader a moment late, while its view is behind the
    /// commit number it has streamed (a new writer opening the lake): the read waits for them. A writer's
    /// miss is an error at once (`Catalog::inline`; a reader's after 10 s, which no test waits out).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_listed_segment_waits_for_its_inline_rows() -> Result<()> {
        use datafusion::arrow::array::{ArrayRef, Int64Array};
        let dir = std::env::temp_dir().join(format!("pondra-inline-{}", uuid::Uuid::new_v4()));
        let writer = Lake::open(dir.to_str().unwrap(), true, false).await?;
        writer.cat.commit(vec![("t/x".into(), b"1".to_vec())], &[]).await?;
        writer.cat.checkpoint().await?; // (a reader's view sees only what is flushed)
        let reader = Lake::open(dir.to_str().unwrap(), false, true).await?;
        assert!(reader.cat.mirror.load(Relaxed), "a streamed reader holds the whole catalog once it opens");

        // The next commit, streamed to the reader with its segment and inline rows.
        let batch = RecordBatch::try_from_iter([("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)])?;
        let ipc = crate::log::encode_ipc(&[batch])?;
        let n = writer.cat.committed() + 1;
        let seg = Segment { parts: BTreeMap::from([("t".to_string(), vec![(0, ipc.len() as u64, 3)])]), ..Default::default() };
        let streamed = vec![
            (seg_key(n), Bytes::from(json(&seg))),
            (data_key(n), Bytes::from(ipc.clone())),
            ("n".to_string(), Bytes::from(json(&(n + 1)))),
            ("c".to_string(), Bytes::from(json(&n))),
        ];
        reader.cat.apply(&Delta { id: n, puts: streamed, deletes: vec![] });

        // Commit n reaches the writer's next flush without its inline rows, so the reader's view takes
        // its commit number and not the rows.
        writer.cat.commit(vec![("t/y".into(), b"2".to_vec())], &[]).await?;
        writer.cat.checkpoint().await?;
        let since = Instant::now();
        while reader.cat.view_now().await? < n {
            anyhow::ensure!(since.elapsed() < Duration::from_secs(10), "the reader's view never reached commit {n}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        reader.cat.refresh().await?; // (the streamed copy of `d/n` goes: the view has its commit)
        assert!(reader.cat.get_raw(&data_key(n)).await?.is_none(), "the view holds no inline rows of commit {n} yet");

        // The rows land 300 ms later. The read below starts first, so it has to wait for them.
        let late = writer.clone();
        let before = Instant::now();
        let landing = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            late.cat.commit(vec![(data_key(n), ipc.clone())], &[]).await?;
            late.cat.checkpoint().await
        });
        let rows = reader.segment_rows(n, &seg, "t").await?;
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 3, "the segment's rows, once they arrive");
        assert!(before.elapsed() >= Duration::from_millis(300), "the read answered before its rows were written");
        landing.await??;

        // A writer has the whole catalog: its miss is an error at once.
        let quick = Instant::now();
        let miss = writer.cat.inline(&data_key(999_999)).await.unwrap_err();
        assert_eq!(miss.to_string(), "missing inline segment 999999");
        assert!(quick.elapsed() < Duration::from_secs(1), "a writer's miss waited");

        reader.cat.close().await?;
        writer.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// A node started on a full disk (its cap 0 at open) spills again once room is freed: `spill_room` gives a
    /// manager what it holds plus the disk's room beyond a lake's keep. The setting is passed in, not read, so
    /// the test doesn't depend on the environment it runs in.
    #[test]
    fn spill_room_follows_the_disk() {
        let folder = spill_folder().expect("this process's spill folder");
        let manager = DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![folder.to_path_buf()])).with_max_temp_directory_size(0).build().expect("a disk manager");
        let manager = Arc::new(manager);
        SPILLERS.lock().unwrap().push(Arc::downgrade(&manager));
        set_room(None);
        let cap = manager.max_temp_directory_size();
        assert!(cap >= manager.used_disk_space() + (1 << 20), "a cap of {cap} with the disk's room");
        set_room(Some(1.0));
        assert!(manager.max_temp_directory_size() <= gb_bytes(1.0), "the setting caps the spills");
    }

    /// A lake's spill disk shows no directory of DataFusion's until something spills: its first spill file is made
    /// in the spill folder, and the folder is the one the spill made (`spill_folder`, this process's).
    #[test]
    fn a_lake_makes_no_spill_folder_until_it_spills() {
        if spill_gb() == Some(0.0) {
            return; // (spilling is off: there is no spill disk to make)
        }
        let m = Arc::new(spill_disk().build().expect("a spill disk"));
        assert!(m.temp_dir_paths().is_empty(), "no directory before a spill");
        assert!(m.tmp_files_enabled());
        let f = m.create_tmp_file("a test").expect("a spill file");
        let path = f.path().expect("a local file").to_path_buf();
        assert!(path.exists(), "{path:?}");
        if let Ok(folder) = spill_folder() {
            assert!(path.starts_with(folder), "{path:?} is not in {folder:?}");
        }
        // (DataFusion counts a write's bytes on the manager that made the file, through the file's own writer: not asserted here)
        let mut writer = f.open_writer().expect("a writer");
        std::io::Write::write_all(&mut writer, &[7u8; 10]).expect("10 bytes written");
        writer.finish().expect("finished");
    }

    /// A shuffle's piece asks for its room first (`spill_fits`): what doesn't fit is refused with 53100, and the
    /// setting is named only when it is set.
    #[test]
    fn a_piece_that_does_not_fit_is_refused_by_name() {
        assert!(!piece_fits(Some(0.5), 0, 1 << 30, None), "a gigabyte past half a gigabyte");
        assert!(piece_fits(Some(0.5), 1 << 20, 1 << 20, None), "a megabyte more under the setting");
        assert!(!piece_fits(None, 0, 1, Some((0, 1 << 30))), "no free room");
        assert!(piece_fits(None, 0, 1 << 20, Some((1 << 30, 1 << 30))), "a gigabyte free, a lake keeps 256 MB");
        let set = no_room(1 << 20, Some(0.5));
        assert_eq!(crate::codes::of(&set), "53100");
        assert!(set.to_string().contains("PONDRA_SPILL_GB") && !set.to_string().contains("datafusion"), "{set}");
        let unset = no_room(1 << 20, None);
        assert_eq!(crate::codes::of(&unset), "53100");
        assert!(!unset.to_string().contains("PONDRA_SPILL_GB") && !unset.to_string().contains("datafusion"), "{unset}");
        let err = spill_fits(u64::MAX / 2).expect_err("no disk holds that");
        assert_eq!(crate::codes::of(&err), "53100");
        assert_eq!(err.to_string().contains("PONDRA_SPILL_GB"), spill_gb().is_some(), "{err}");
    }

    /// A keyed table with every rule kind: a renamed and a dropped column, both identity kinds with their
    /// defaults, an enum, a uuid, a varchar(3), NOT NULL, DEFAULT, CHECK, an enforced and a NOT ENFORCED UNIQUE.
    fn with_rules() -> TableMeta {
        let col = |c: &str, t: &str| (c.to_string(), t.to_string());
        let set = |v: &[(&str, &str)]| v.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<BTreeMap<String, String>>();
        let id = |always: bool, seq: &str| crate::seq::Identity { always, declared: vec![], sequence: seq.into() };
        let unique = |name: &str, c: &str, enforced: bool| crate::constraints::Constraint { name: name.into(), kind: crate::constraints::Kind::Unique, columns: vec![c.into()], references: None, enforced };
        TableMeta {
            columns: vec![col("id", "Int64"), col("seq_no", "Int64"), col("c_code", "Utf8"), col("c_status", "Utf8"), col("owner", "Utf8"), col("total", "Int64"), col("doc", "Utf8"), col("gone", "Utf8"), col("_deleted", "Boolean")],
            key: vec!["id".into()],
            names: set(&[("c_code", "code"), ("c_status", "status")]),
            dropped: vec!["gone".into()],
            not_null: vec!["id".into(), "c_code".into(), "gone".into()],
            defaults: set(&[("id", "nextval('orders_id_seq')"), ("seq_no", "nextval('orders_seq_no_seq')"), ("total", "0"), ("gone", "'x'")]),
            checks: vec![("orders_total_check".into(), "total >= 0".into())],
            identity: [("id".to_string(), id(false, "orders_id_seq")), ("seq_no".to_string(), id(true, "orders_seq_no_seq"))].into(),
            constraints: vec![unique("orders_code_key", "c_code", true), unique("orders_owner_key", "owner", false)],
            enums: [("c_status".to_string(), crate::types::Enum { of: Some("order_status".into()), labels: vec!["open".into(), "shipped".into()] })].into(),
            sql_types: set(&[("owner", "uuid"), ("c_code", "varchar(3)"), ("doc", "json"), ("gone", "json")]),
            ..Default::default()
        }
    }

    /// `logical()` of `with_rules`, as JSON: the golden is the unchanged code's.
    #[test]
    fn logical_is_as_before_rules() {
        let json = serde_json::to_string(&with_rules().logical()).unwrap();
        assert_eq!(json, r#"{"columns":[["id","Int64"],["seq_no","Int64"],["code","Utf8"],["status","Utf8"],["owner","Utf8"],["total","Int64"],["doc","Utf8"],["_deleted","Boolean"]],"key":["id"],"merge":{},"files":[],"sealed":null,"tiered":0,"garbage":[],"rows_at":0,"shadows":false,"publish":[],"cluster":[],"ttl":null,"partition":null,"ids":false,"changed":false,"not_null":["id","code"],"defaults":{"id":"nextval('orders_id_seq')","seq_no":"nextval('orders_seq_no_seq')","total":"0"},"checks":[["orders_total_check","total >= 0"]],"identity":{"id":{"always":false,"sequence":"orders_id_seq"},"seq_no":{"always":true,"sequence":"orders_seq_no_seq"}},"constraints":[{"name":"orders_code_key","kind":"unique","columns":["code"],"enforced":true},{"name":"orders_owner_key","kind":"unique","columns":["owner"]}],"enums":{"status":{"of":"order_status","labels":["open","shipped"]}},"sql_types":{"code":"varchar(3)","doc":"json","owner":"uuid"}}"#);
    }

    /// `to_rules` names everything SQL's way: the dropped column is absent, the renamed one keeps its stored name, and
    /// each rule field is what `logical()` gives under the same names.
    #[test]
    fn to_rules_maps_the_rule_fields_as_logical_does() -> Result<()> {
        let m = with_rules();
        let (t, l) = (m.to_rules()?, m.logical());
        let plain = |name: &str, stored: &str| crate::rules::Column { name: name.into(), stored: stored.into(), not_null: false, default: None, always: false, held: None, labels: None };
        let want = vec![
            crate::rules::Column { not_null: true, default: Some("nextval('orders_id_seq')".into()), ..plain("id", "id") },
            crate::rules::Column { default: Some("nextval('orders_seq_no_seq')".into()), always: true, ..plain("seq_no", "seq_no") },
            crate::rules::Column { not_null: true, held: Some("varchar(3)".into()), ..plain("code", "c_code") },
            crate::rules::Column { labels: Some((Some("order_status".into()), vec!["open".into(), "shipped".into()])), ..plain("status", "c_status") },
            crate::rules::Column { held: Some("uuid".into()), ..plain("owner", "owner") },
            crate::rules::Column { default: Some("0".into()), ..plain("total", "total") },
            crate::rules::Column { held: Some("json".into()), ..plain("doc", "doc") },
            plain("_deleted", "_deleted"),
        ];
        assert_eq!(t.columns, want);
        let names: Vec<&str> = t.schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "seq_no", "code", "status", "owner", "total", "doc", "_deleted"], "SQL's names, _deleted among them");
        assert_eq!(t.key, ["id"]);
        assert!(t.key_checked && t.mapped && t.checked() && t.width() == 7);
        assert_eq!(t.unique, [("orders_code_key".to_string(), vec!["code".to_string()])]);
        assert_eq!(t.checks, [("orders_total_check".to_string(), "total >= 0".to_string())]);
        assert!(t.column("gone").is_none() && t.marker("_deleted") && !t.marker("total"));
        for c in &t.columns {
            assert_eq!(l.not_null.contains(&c.name), c.not_null, "{}", c.name);
            assert_eq!(l.defaults.get(&c.name).cloned(), c.default, "{}", c.name);
            assert_eq!(l.identity.get(&c.name).is_some_and(|i| i.always), c.always, "{}", c.name);
            assert_eq!(l.sql_types.get(&c.name).cloned(), c.held, "{}", c.name);
            assert_eq!(l.enums.get(&c.name).map(|e| (e.of.clone(), e.labels.clone())), c.labels, "{}", c.name);
        }
        assert_eq!(l.key, t.key);
        assert_eq!(l.columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), names);
        assert!(!l.defaults.contains_key("gone") && !l.sql_types.contains_key("gone") && !l.not_null.iter().any(|c| c == "gone"));
        Ok(())
    }

    /// A merge table (a GROUP BY view's) has a key, but its plain INSERTs' keys are not checked.
    #[test]
    fn a_merge_table_is_not_key_checked() -> Result<()> {
        let m = TableMeta { columns: vec![("day".into(), "Int64".into()), ("n".into(), "Int64".into())], key: vec!["day".into()], merge: [("n".to_string(), "sum".to_string())].into(), ..Default::default() };
        let t = m.to_rules()?;
        assert!(!t.key_checked && !t.checked() && !t.mapped);
        assert_eq!(t.key, ["day"]);
        assert_eq!(t.key, m.logical().key);
        Ok(())
    }

    /// `rules()` makes a parse's table once: the same `Arc` gives the same tables, a new parse makes its own, and a
    /// clone starts with none.
    #[test]
    fn the_rules_are_made_once_per_parse() -> Result<()> {
        let a = Arc::new(with_rules());
        let (x, y) = (a.rules()?, a.rules()?);
        assert!(Arc::ptr_eq(&x, &y), "the same parse gives the same tables");
        assert_eq!(*x, a.to_rules()?, "and they are the rules of its entry");
        let b = Arc::new((*a).clone());
        assert!(!Arc::ptr_eq(&b.rules()?, &x), "a new parse makes its own");
        let c = (*a).clone();
        assert!(c.rules.0.get().is_none(), "a clone's memo is empty");
        let made = Arc::new(c).rules()?;
        assert_eq!(*made, *x, "and the clone's rules are the same");
        Ok(())
    }
}
