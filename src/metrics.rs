//! `GET /metrics`: this node's counters and the lake's tables, in Prometheus' text format.
use crate::server::App;
use crate::store::{table_key, TableMeta};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

crate::per_node!(pub static ROWS_IN: AtomicU64 = AtomicU64::new(0)); // rows this node took into the log, by any door
crate::per_node!(pub static QUERIES: AtomicU64 = AtomicU64::new(0)); // SQL queries run here (HTTP, Postgres, MCP, Flight)
crate::per_node!(pub static QUERY_ERRORS: AtomicU64 = AtomicU64::new(0));
crate::per_node!(pub static QUERY_US: AtomicU64 = AtomicU64::new(0)); // their total time
crate::per_node!(pub static SPREAD: AtomicU64 = AtomicU64::new(0)); // queries run across the cluster from here
crate::per_node!(pub static SHUFFLED: AtomicU64 = AtomicU64::new(0)); // …of them with a shuffle
crate::per_node!(pub static RANGED: AtomicU64 = AtomicU64::new(0)); // …of them with their tables sliced by a key's ranges
crate::per_node!(pub static FILES_SCANNED: AtomicU64 = AtomicU64::new(0)); // append-table files queries opened…
crate::per_node!(pub static FILES_SKIPPED: AtomicU64 = AtomicU64::new(0)); // …and skipped by their min/max
crate::per_node!(pub static OBJECT_WRITES: AtomicU64 = AtomicU64::new(0)); // object-store requests this process made (`store::Counted`): writes…
crate::per_node!(pub static OBJECT_LISTS: AtomicU64 = AtomicU64::new(0)); // …lists…
crate::per_node!(pub static OBJECT_DELETES: AtomicU64 = AtomicU64::new(0)); // …deletes…
crate::per_node!(pub static OBJECT_READS: AtomicU64 = AtomicU64::new(0)); // …and reads (GET and HEAD)
crate::per_node!(pub static SPILLED: AtomicU64 = AtomicU64::new(0)); // shuffle bytes written to this node's disk
crate::per_node!(pub static SKEW: AtomicU64 = AtomicU64::new(0)); // the worst bucket/average seen in a shuffle, x100
crate::per_node!(pub static SKEW_SPLITS: AtomicU64 = AtomicU64::new(0)); // hot partitions of shuffled joins shared out
crate::per_node!(pub static RECEIVED: AtomicU64 = AtomicU64::new(0)); // shuffle bytes this node's steps read (its share of the work)
crate::per_node!(pub static WIRE: AtomicU64 = AtomicU64::new(0)); // bytes other nodes sent this node, compressed as sent (shuffles, gathered results)
crate::per_node!(pub static WAIT_US: AtomicU64 = AtomicU64::new(0)); // time this node's steps waited for other nodes' buckets
// (these two are the process's, not a node's: a stopped node's last queries still count themselves
// down, and set back to 0 under them they would wrap)
pub static RUNNING: AtomicU64 = AtomicU64::new(0); // queries running here now, each on one of PONDRA_QUERIES' turns
pub static WAITING: AtomicU64 = AtomicU64::new(0); // queries waiting here for a turn

/// Counts what is going on now: up while this guard lives (`Now::new`), down when it is dropped.
pub struct Now(&'static AtomicU64);

impl Now {
    pub fn new(count: &'static AtomicU64) -> Self {
        count.fetch_add(1, Relaxed);
        Now(count)
    }
}

impl Drop for Now {
    fn drop(&mut self) { self.0.fetch_sub(1, Relaxed); }
}

pub fn add(c: &AtomicU64, n: u64) { c.fetch_add(n, Relaxed); }

crate::per_node!(
    /// Background loops that fail without failing a request (tiering, publishing, window emission,
    /// streaming tasks, checkpoints): counted here, so a stall that otherwise shows only on stderr is
    /// visible in `/metrics` and `/stats`.
    static BG_FAILS: [(&str, AtomicU64); 5] =
    [("tiering", AtomicU64::new(0)), ("publish", AtomicU64::new(0)), ("emission", AtomicU64::new(0)), ("tasks", AtomicU64::new(0)), ("checkpoint", AtomicU64::new(0))];
    clear = |all| all.iter().for_each(|(_, c)| c.store(0, Relaxed)));

pub fn job_failed(job: &str) {
    if let Some((_, c)) = BG_FAILS.iter().find(|(k, _)| *k == job) {
        c.fetch_add(1, Relaxed);
    }
}

/// Each background job and how many times it has failed (for `/metrics` and `/stats`).
pub fn failures() -> Vec<(&'static str, u64)> { BG_FAILS.iter().map(|(k, c)| (*k, c.load(Relaxed))).collect() }

pub async fn render(app: &App) -> anyhow::Result<String> {
    let mut out = String::new();
    let mut metric = |name: &str, kind: &str, help: &str, samples: &[(String, f64)]| {
        let _ = writeln!(out, "# HELP pondra_{name} {help}\n# TYPE pondra_{name} {kind}");
        for (labels, v) in samples {
            let _ = writeln!(out, "pondra_{name}{labels} {v}");
        }
    };
    let one = |v: f64| vec![(String::new(), v)];
    let get = |c: &AtomicU64| c.load(Relaxed) as f64;
    let role = if app.cluster.reader { "reader" } else if app.seq.is_some() { "leader" } else { "follower" };
    metric("role", "gauge", "1 for this node's role", &[(format!("{{role=\"{role}\"}}"), 1.0)]);
    metric("nodes", "gauge", "live nodes in the cluster", &one(app.cluster.nodes().len() as f64));
    metric("visible_segment", "gauge", "newest log segment readable here", &one(app.lake.visible() as f64));
    metric("rows_in_total", "counter", "rows taken into the log here", &one(get(&ROWS_IN)));
    metric("queries_total", "counter", "SQL queries run here", &one(get(&QUERIES)));
    metric("query_errors_total", "counter", "SQL queries that failed", &one(get(&QUERY_ERRORS)));
    metric("queries_running", "gauge", "SQL queries running here now, each on one of PONDRA_QUERIES' turns", &one(get(&RUNNING)));
    metric("queries_waiting", "gauge", "SQL queries waiting here for a turn", &one(get(&WAITING)));
    metric("query_seconds_total", "counter", "time spent in SQL queries", &one(get(&QUERY_US) / 1e6));
    metric("spread_queries_total", "counter", "queries run across the cluster from here", &one(get(&SPREAD)));
    metric("shuffled_queries_total", "counter", "…of them with a shuffle", &one(get(&SHUFFLED)));
    metric("ranged_queries_total", "counter", "…of them with their tables sliced by a key's ranges", &one(get(&RANGED)));
    metric("files_scanned_total", "counter", "Parquet files queries read", &one(get(&FILES_SCANNED)));
    metric("files_skipped_total", "counter", "Parquet files queries skipped by min/max, unopened", &one(get(&FILES_SKIPPED)));
    let requests = [("write", &OBJECT_WRITES), ("list", &OBJECT_LISTS), ("delete", &OBJECT_DELETES), ("read", &OBJECT_READS)].map(|(op, c)| (format!("{{op=\"{op}\"}}"), get(c)));
    metric("object_requests_total", "counter", "requests this process made of its bucket, past the SSD tier: writes (a multipart upload's start, parts and end each one), lists, deletes and reads, as a bucket bills and rate-limits them", &requests);
    metric("background_failures_total", "counter", "background jobs (tiering, publishing, emission, tasks, checkpoints) that failed; a rising count is a stall to look at", &failures().iter().map(|(k, n)| (format!("{{job=\"{k}\"}}"), *n as f64)).collect::<Vec<_>>());
    let turns = crate::budget::turns();
    let label = |b: &str, l: &str| format!("{{bucket=\"{b}\",lake=\"{l}\"}}");
    metric("bucket_turns", "gauge", "requests this node may have at once at a bucket, or at another lake in it it reads (a branch's base: a quarter)", &turns.iter().map(|(b, l, n, _)| (label(b, l), *n as f64)).collect::<Vec<_>>());
    metric("bucket_turns_most", "gauge", "the most requests it had at once there so far", &turns.iter().map(|(b, l, _, m)| (label(b, l), *m as f64)).collect::<Vec<_>>());
    let ai = [("answered", &crate::ai::CALLS), ("failed", &crate::ai::FAILED), ("retried", &crate::ai::RETRIED)].map(|(r, c)| (format!("{{result=\"{r}\"}}"), get(c)));
    metric("ai_requests_total", "counter", "requests ai_complete and ai_embed made to the model's endpoint: answered, failed for good (their rows NULL), tried again", &ai);
    metric("shuffle_spilled_bytes_total", "counter", "shuffle rows written to this node's disk", &one(get(&SPILLED)));
    metric("shuffle_disk_bytes", "gauge", "shuffle buckets on this node's disk now", &one(crate::spill::held() as f64));
    metric("shuffle_skew", "gauge", "worst bucket vs the average one in a shuffle here (1 = even)", &one(get(&SKEW) / 100.0));
    metric("skew_splits_total", "counter", "hot partitions of shuffled joins shared out over the nodes", &one(get(&SKEW_SPLITS)));
    metric("shuffle_received_bytes_total", "counter", "shuffle bytes this node's steps read", &one(get(&RECEIVED)));
    metric("wire_bytes_total", "counter", "bytes other nodes sent this node, compressed as sent", &one(get(&WIRE)));
    metric("shuffle_wait_seconds_total", "counter", "time this node's steps waited for other nodes' buckets", &one(get(&WAIT_US) / 1e6));
    let (reserved, limit) = app.lake.memory();
    metric("memory_limit_bytes", "gauge", "query memory limit (spills beyond it)", &one(limit as f64));
    metric("memory_reserved_bytes", "gauge", "query memory in use", &one(reserved as f64));
    let (hot, hot_max, loading) = app.lake.hot.usage();
    metric("hot_bytes", "gauge", "decoded columns kept in memory (hot.rs)", &one(hot as f64));
    metric("hot_limit_bytes", "gauge", "the most the hot columns may hold (PONDRA_HOT_GB)", &one(hot_max as f64));
    metric("hot_loading_files", "gauge", "files whose columns are being decoded into memory, or wait to be", &one(loading as f64));
    metric("hot_batches_skipped_total", "counter", "batches of hot columns scans skipped by their ranges", &one(crate::hot::SKIPPED.load(Relaxed) as f64));
    let mut caches: Vec<(String, f64)> = app.lake.cached_bytes().iter().map(|(c, b)| (format!("{{cache=\"{c}\"}}"), *b as f64)).collect();
    caches.push(("{cache=\"answers\"}".into(), app.results.held() as f64));
    metric("cache_bytes", "gauge", "what each bounded cache holds now: objects' byte ranges, decoded log rows, tables' tails, row groups for lookups, the catalog's blocks, Parquet footers, files' statistics, remembered answers", &caches);
    metric("resident_bytes", "gauge", "resident memory of the process", &one(crate::store::resident().unwrap_or(0) as f64));
    if let Some(seq) = &app.seq {
        metric("untiered_rows", "gauge", "rows in the log waiting to become Parquet", &one(app.lake.backlog.load(Relaxed) as f64));
        let mut ms = seq.commit_ms.lock().unwrap().clone();
        ms.sort_by(f64::total_cmp);
        let q = |p: f64| ms.get(((ms.len() as f64 * p) as usize).min(ms.len().saturating_sub(1))).copied().unwrap_or(0.0);
        metric("commit_ms", "gauge", "recent catalog commit latency", &[("{quantile=\"0.5\"}".into(), q(0.5)), ("{quantile=\"0.99\"}".into(), q(0.99))]);
    }
    let tables = app.lake.cat.scan::<TableMeta>("t/", "t0").await?;
    let (mut files, mut rows, mut bytes, mut entry) = (vec![], vec![], vec![], vec![]);
    for (key, m) in &tables {
        let t = &key[table_key("").len()..];
        let s = m.sealed.clone().unwrap_or_default();
        files.push((format!("{{table=\"{t}\",where=\"inline\"}}"), m.files.len() as f64));
        files.push((format!("{{table=\"{t}\",where=\"sealed\"}}"), s.files as f64));
        rows.push((format!("{{table=\"{t}\"}}"), (s.rows + m.files.iter().map(|f| f.rows).sum::<u64>()) as f64));
        bytes.push((format!("{{table=\"{t}\"}}"), (s.bytes + m.files.iter().map(|f| f.bytes).sum::<u64>()) as f64));
        entry.push((format!("{{table=\"{t}\"}}"), crate::store::json(m).len() as f64));
    }
    metric("table_files", "gauge", "Parquet files: listed in the catalog entry, or in manifests", &files);
    metric("table_rows", "gauge", "rows in Parquet files", &rows);
    metric("table_bytes", "gauge", "bytes of Parquet files", &bytes);
    metric("table_entry_bytes", "gauge", "size of the table's catalog entry (what every commit to it writes)", &entry);
    // What publishing a table in an open format keeps (it names manifests, not files: ADR-012).
    let mut published = vec![];
    for (format, prefix) in [("delta", "x/"), ("iceberg", "i/")] {
        for (key, v) in app.lake.cat.scan::<serde_json::Value>(prefix, &format!("{prefix}\u{10ffff}")).await? {
            published.push((format!("{{table=\"{}\",format=\"{format}\"}}", &key[prefix.len()..]), crate::store::json(&v).len() as f64));
        }
    }
    metric("published_state_bytes", "gauge", "size of what the catalog keeps about a published table", &published);
    Ok(out)
}
