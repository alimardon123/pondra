//! This node's counters, which every form keeps, and `GET /metrics` (a door: `render`), which says them and the lake's
//! tables in Prometheus' text format.
#[cfg(feature = "doors")]
use crate::app::App;
#[cfg(feature = "doors")]
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
crate::per_node!(pub static CATALOG_READS: AtomicU64 = AtomicU64::new(0)); // catalog records this node read (a lookup, there or not, or each a scan gave)…
crate::per_node!(pub static CATALOG_WRITES: AtomicU64 = AtomicU64::new(0)); // …and put or deleted (the write-set rule: designs/redesign/redesign.md §4)
crate::per_node!(pub static SPILLED: AtomicU64 = AtomicU64::new(0)); // shuffle bytes written to this node's disk
crate::per_node!(pub static SKEW: AtomicU64 = AtomicU64::new(0)); // the worst bucket/average seen in a shuffle, x100
crate::per_node!(pub static SKEW_SPLITS: AtomicU64 = AtomicU64::new(0)); // hot partitions of shuffled joins shared out
crate::per_node!(pub static RECEIVED: AtomicU64 = AtomicU64::new(0)); // shuffle bytes this node's steps read (its share of the work)
crate::per_node!(pub static WIRE: AtomicU64 = AtomicU64::new(0)); // bytes other nodes sent this node, compressed as sent (shuffles, gathered results)
crate::per_node!(pub static WAIT_US: AtomicU64 = AtomicU64::new(0)); // time this node's steps waited for other nodes' buckets
crate::per_node!(pub static STOPPED_FOR_MEMORY: AtomicU64 = AtomicU64::new(0)); // queries the node stopped because it was nearly out of memory (`oom.rs`)
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

/// jemalloc's count of what it holds, when it keeps one (built with its statistics: the `heap-profile` build), else
/// nothing. Beside the process's resident memory it says whether the heap grew or pages were held, and how much of
/// the process is outside jemalloc (C libraries' malloc, thread stacks): reviews/soak-heap-0fa48fb.md.
#[cfg(all(feature = "doors", target_os = "linux", target_env = "gnu"))]
pub fn allocator() -> Vec<(&'static str, u64)> {
    use crate::jemalloc::{read, write};
    if !write(c"epoch", 1u64) {
        return vec![]; // (its statistics are a snapshot, taken when the epoch moves)
    }
    [("allocated", c"stats.allocated"), ("active", c"stats.active"), ("resident", c"stats.resident"), ("metadata", c"stats.metadata"), ("retained", c"stats.retained")]
        .into_iter().filter_map(|(k, name)| read::<usize>(name).map(|v| (k, v as u64))).collect()
}
#[cfg(all(feature = "doors", not(all(target_os = "linux", target_env = "gnu"))))]
pub fn allocator() -> Vec<(&'static str, u64)> { vec![] }

/// Each background job and how many times it has failed (for `/metrics` and `/stats`).
pub fn failures() -> Vec<(&'static str, u64)> { BG_FAILS.iter().map(|(k, c)| (*k, c.load(Relaxed))).collect() }

#[cfg(feature = "doors")]
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
    let role = if app.members.reader() { "reader" } else if app.seq.is_some() { "leader" } else { "follower" };
    metric("role", "gauge", "1 for this node's role", &[(format!("{{role=\"{role}\"}}"), 1.0)]);
    metric("nodes", "gauge", "live nodes in the cluster", &one(app.members.nodes().len() as f64));
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
    metric("catalog_records_total", "counter", "catalog records this node read and wrote", &[("{op=\"read\"}".to_string(), get(&CATALOG_READS)), ("{op=\"write\"}".to_string(), get(&CATALOG_WRITES))]);
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
    metric("queries_stopped_for_memory_total", "counter", "queries stopped (answered 53200) because the node was nearly out of memory (PONDRA_MEMORY_MAX_GB); a first try stopped and run again frugally is not counted, one that can't be run again is", &one(get(&STOPPED_FOR_MEMORY)));
    metric("memory_limit_bytes", "gauge", "query memory limit (spills beyond it)", &one(limit as f64));
    metric("memory_reserved_bytes", "gauge", "query memory in use", &one(reserved as f64));
    let (hot, hot_max, loading) = app.lake.hot.usage();
    metric("hot_bytes", "gauge", "decoded columns kept in memory (hot.rs)", &one(hot as f64));
    metric("hot_limit_bytes", "gauge", "the most the hot columns may hold (PONDRA_HOT_GB)", &one(hot_max as f64));
    metric("hot_loading_files", "gauge", "files whose columns are being decoded into memory, or wait to be", &one(loading as f64));
    metric("hot_batches_skipped_total", "counter", "batches of hot columns scans skipped by their ranges", &one(crate::hot::SKIPPED.load(Relaxed) as f64));
    // (one line a cache the node's line counts, by its name: `Ledger::charge`)
    let caches: Vec<(String, f64)> = app.lake.cached_bytes().iter().map(|(c, b)| (format!("{{cache=\"{c}\"}}"), *b as f64)).collect();
    metric("cache_bytes", "gauge", "what each bounded cache holds now: objects' byte ranges, decoded log rows, tables' tails, row groups for lookups, the catalog's blocks, Parquet footers, files' statistics, remembered answers, planning facts' manifest lists, tables' manifests, Delta logs and Iceberg manifests read, answers kept for their pages, statements worked out as point queries, the log tails' keys lookups index", &caches);
    metric("resident_bytes", "gauge", "resident memory of the process", &one(crate::store::resident().unwrap_or(0) as f64));
    let held: Vec<(String, f64)> = allocator().into_iter().map(|(k, b)| (format!("{{kind=\"{k}\"}}"), b as f64)).collect();
    if !held.is_empty() {
        metric("allocator_bytes", "gauge", "jemalloc's own count: allocated (what the node holds), active (its pages), resident (those, its free pages not yet given back and its metadata), metadata, retained (address space kept, not resident)", &held);
    }
    if let Some(seq) = &app.seq {
        metric("untiered_rows", "gauge", "rows in the log waiting to become Parquet", &one(app.lake.backlog.load(Relaxed) as f64));
        let mut ms = seq.commit_ms.lock().unwrap().clone();
        ms.sort_by(f64::total_cmp);
        let q = |p: f64| ms.get(((ms.len() as f64 * p) as usize).min(ms.len().saturating_sub(1))).copied().unwrap_or(0.0);
        metric("commit_ms", "gauge", "recent catalog commit latency", &[("{quantile=\"0.5\"}".into(), q(0.5)), ("{quantile=\"0.99\"}".into(), q(0.99))]);
        metric("catalog_unflushed_seconds", "gauge", "seconds the catalog has held commits that no flush has persisted yet (0 when it has none): an alarm, since a new leader replays that long a WAL", &one(app.lake.cat.unflushed_seconds()));
        // Producers' marks and the expiry keys that will remove them (`log::Producers`): flat under steady writes.
        if let Some((marks, expiries)) = app.lake.cat.tallies() {
            metric("producer_marks", "gauge", "producers' marks in the catalog (p/): one per producer with a commit kept, a job's or a transaction's or a COPY's", &one(marks as f64));
            metric("mark_expiries", "gauge", "expiry keys (g/) waiting: each goes with its mark once its window is up", &one(expiries as f64));
        }
    }
    // The tables' files, rows and sizes, from what this node knows of each (`tier::refresh`: the tables committed since
    // its last look, and every table once a minute), so a scrape's reads follow what changed, not how many tables the
    // lake holds. The lake's totals here, so a lake of thousands of tables is still a few series; a line for each table
    // is `GET /metrics/tables` (`tables`).
    let (count, [files, rows, bytes, entry]) = app.table_lines(false).await?;
    metric("tables", "gauge", "tables in the catalog", &one(count as f64));
    metric("table_files", "gauge", "Parquet files of the lake's tables: listed in the catalog entries, or in manifests (each table's: GET /metrics/tables)", &files);
    metric("table_rows", "gauge", "rows in the lake's tables' Parquet files", &rows);
    metric("table_bytes", "gauge", "bytes of the lake's tables' Parquet files", &bytes);
    metric("table_entry_bytes", "gauge", "size of the lake's tables' catalog entries (what every commit to one writes)", &entry);
    Ok(out)
}

/// `GET /metrics/tables`: `render`'s table lines for each table, labelled `table` (as `/metrics` gave them before it
/// gave the lake's totals), and what publishing each table in an open format keeps. One scrape is five lines a table,
/// so it is an endpoint of its own, scraped by whoever wants each table's numbers. They come from the same map
/// (`tier::refresh`), except the published state, which reads each published table's record.
#[cfg(feature = "doors")]
pub async fn tables(app: &App) -> anyhow::Result<String> {
    let mut out = String::new();
    let mut metric = |name: &str, help: &str, samples: &[(String, f64)]| {
        let _ = writeln!(out, "# HELP pondra_{name} {help}\n# TYPE pondra_{name} gauge");
        for (labels, v) in samples {
            let _ = writeln!(out, "pondra_{name}{labels} {v}");
        }
    };
    let (_, [files, rows, bytes, entry]) = app.table_lines(true).await?;
    metric("table_files", "Parquet files of each table: listed in the catalog entry, or in manifests", &files);
    metric("table_rows", "rows in each table's Parquet files", &rows);
    metric("table_bytes", "bytes of each table's Parquet files", &bytes);
    metric("table_entry_bytes", "size of each table's catalog entry (what every commit to it writes)", &entry);
    // (publishing names manifests, not files: ADR-012)
    let mut published = vec![];
    for (format, prefix) in [("delta", "x/"), ("iceberg", "i/")] {
        for (key, v) in app.lake.cat.scan::<serde_json::Value>(prefix, &format!("{prefix}\u{10ffff}")).await? {
            published.push((format!("{{table=\"{}\",format=\"{format}\"}}", &key[prefix.len()..]), crate::store::json(&v).len() as f64));
        }
    }
    metric("published_state_bytes", "size of what the catalog keeps about a published table", &published);
    Ok(out)
}
