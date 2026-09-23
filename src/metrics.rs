//! `GET /metrics`: this node's counters and the lake's tables, in Prometheus' text format.
use crate::server::App;
use crate::store::{table_key, TableMeta};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub static ROWS_IN: AtomicU64 = AtomicU64::new(0); // rows this node took into the log, by any door
pub static QUERIES: AtomicU64 = AtomicU64::new(0); // SQL queries run here (HTTP, Postgres, MCP, Flight)
pub static QUERY_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static QUERY_US: AtomicU64 = AtomicU64::new(0); // their total time
pub static SPREAD: AtomicU64 = AtomicU64::new(0); // queries run across the cluster from here
pub static SHUFFLED: AtomicU64 = AtomicU64::new(0); // …of them with a shuffle
pub static FILES_SCANNED: AtomicU64 = AtomicU64::new(0); // append-table files queries opened…
pub static FILES_SKIPPED: AtomicU64 = AtomicU64::new(0); // …and skipped by their min/max

pub fn add(c: &AtomicU64, n: u64) { c.fetch_add(n, Relaxed); }

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
    metric("query_seconds_total", "counter", "time spent in SQL queries", &one(get(&QUERY_US) / 1e6));
    metric("spread_queries_total", "counter", "queries run across the cluster from here", &one(get(&SPREAD)));
    metric("shuffled_queries_total", "counter", "…of them with a shuffle", &one(get(&SHUFFLED)));
    metric("files_scanned_total", "counter", "Parquet files queries read", &one(get(&FILES_SCANNED)));
    metric("files_skipped_total", "counter", "Parquet files queries skipped by min/max, unopened", &one(get(&FILES_SKIPPED)));
    let (reserved, limit) = app.lake.memory();
    metric("memory_limit_bytes", "gauge", "query memory limit (spills beyond it)", &one(limit as f64));
    metric("memory_reserved_bytes", "gauge", "query memory in use", &one(reserved as f64));
    let (hot, hot_max) = app.lake.hot.usage();
    metric("hot_bytes", "gauge", "decoded columns kept in memory (hot.rs)", &one(hot as f64));
    metric("hot_limit_bytes", "gauge", "the most the hot columns may hold (PONDRA_HOT_GB)", &one(hot_max as f64));
    let rss = std::fs::read_to_string("/proc/self/statm").ok().and_then(|s| s.split_whitespace().nth(1)?.parse::<f64>().ok());
    metric("resident_bytes", "gauge", "resident memory of the process", &one(rss.unwrap_or(0.0) * 4096.0));
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
