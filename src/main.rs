//! Pondra: a streamhouse in one binary (see ADR-002 to ADR-005).
//! Object storage — a local dir or s3://bucket/prefix (S3, R2, MinIO) — is the only state.
mod cache;
mod delta;
mod cluster;
mod log;
mod query;
mod server;
mod spmd;
mod store;
mod tasks;
mod tier;
mod views;

use clap::Parser;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc; // returns freed memory to the OS promptly (glibc malloc holds on to it)
use datafusion::arrow::util::pretty::pretty_format_batches;
use std::{future::Future, sync::Arc, time::Duration};

#[derive(Parser)]
enum Cmd {
    /// Run a node. Nodes started on the same lake form a cluster: the first leads (ingest,
    /// tiering, commits), the rest follow (SQL, task shards, writes forwarded to the leader) and
    /// take over if the leader dies. `--reader`: SQL only, never leads.
    Serve {
        /// Local directory or s3://bucket/prefix (credentials/endpoint from AWS_* env vars).
        #[arg(long)]
        dir: String,
        /// Address other nodes reach this one at (also the listen address).
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: String,
        /// Read-only node: any number can run next to the single writer.
        #[arg(long)]
        reader: bool,
        /// Minimum milliseconds between a node's flushes. 0: flush as soon as the previous flush
        /// is committed (what queued up meanwhile goes together), for the lowest latency.
        #[arg(long, default_value_t = 0)]
        flush_ms: u64,
        /// Tiering starts as soon as rows commit, at most once per this many seconds (fractions
        /// allowed): how soon new rows are Parquet, and a Delta version other engines can read.
        /// Each run costs a few object-store writes per busy table (0 = only via POST /tier).
        #[arg(long, default_value_t = 2.0)]
        tier_secs: f64,
        /// Streaming tasks run as soon as new rows commit, and at least this often (milliseconds).
        #[arg(long, default_value_t = 1000)]
        task_ms: u64,
        /// Seconds to keep consumed log segments and replaced files before deleting them.
        #[arg(long, default_value_t = 60)]
        retain_secs: u64,
        /// Rows allowed to wait in the log for tiering before commits pause (backpressure).
        /// Lower it for less memory, raise it to absorb longer bursts.
        #[arg(long, default_value_t = 10_000_000)]
        backlog: u64,
        /// Lakes on object storage: where this node keeps its local SSD copy of recent objects
        /// (default: <temp dir>/pondra-cache).
        #[arg(long)]
        cache_dir: Option<String>,
        /// Size of that SSD tier in GB (0 turns it off).
        #[arg(long, default_value_t = 20)]
        cache_gb: u64,
    },
    /// Print catalog entries whose keys start with `prefix` (t/ tables, s/ segments, p/ producers…).
    Catalog {
        #[arg(long)]
        dir: String,
        prefix: String,
    },
    /// Run SQL straight against the lake: read-only, no server needed.
    Sql {
        #[arg(long)]
        dir: String,
        query: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cmd::parse() {
        Cmd::Serve { dir, addr, reader, flush_ms, tier_secs, task_ms, retain_secs, backlog, cache_dir, cache_gb } => {
            std::env::set_var("PONDRA_CACHE_GB", cache_gb.to_string()); // read by Lake::open
            if let Some(d) = cache_dir {
                std::env::set_var("PONDRA_CACHE_DIR", d);
            }
            let (_, store, _) = store::open_store(&dir)?;
            let cluster = cluster::Cluster::join(&store, &addr, reader).await?;
            let leader = cluster.is_leader();
            // Read-only nodes follow the leader's commit stream too (when a live one is there to ask),
            // so their reads are as fresh as a follower's instead of waiting for catalog polls.
            let streamed = !leader && !cluster.leader.addr.is_empty() && (!reader || cluster.leader_alive().await);
            let lake = match store::Lake::open(&dir, leader, streamed).await {
                Err(e) if leader => {
                    eprintln!("opening the lake as leader failed: {e:#}"); // e.g. a newer leader fenced us
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    cluster::restart()
                }
                lake => lake?,
            };
            let l = lake.clone();
            tokio::spawn(async move { l.warm().await.map_err(|e| eprintln!("warming the SSD tier: {e:#}")) });
            let max_backlog = (tier_secs > 0.0).then_some(backlog); // rows waiting to be tiered
            let seq = if leader { Some(log::Sequencer::start(lake.clone(), max_backlog).await?) } else { None };
            let log = (!reader).then(|| {
                let to = match &seq {
                    Some(s) => log::To::Local(s.clone()),
                    None => log::To::Leader(cluster.leader.addr.clone()),
                };
                Arc::new(log::Log::start(lake.clone(), Duration::from_millis(flush_ms), to))
            });
            let app = server::App { lake: lake.clone(), cluster: cluster.clone(), log, seq, lock: Default::default(), retain_ms: retain_secs * 1000 };
            if leader {
                // The leader's SSD tier learns of objects other nodes wrote from its own commits.
                let l = lake.clone();
                tokio::spawn(async move {
                    let (_, mut commits) = l.cat.subscribe();
                    loop {
                        match commits.recv().await {
                            Ok(d) => l.prefetch(&d),
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue, // (just fewer prefetches)
                            Err(_) => break,
                        }
                    }
                });
                // Tier as soon as new rows commit — other engines see them in the Delta log a moment
                // later — but at most once every `tier_secs`, and at least every 5 × that (bulk
                // inserts, compaction, retention).
                if tier_secs > 0.0 {
                    let (a, mut hwm, period) = (app.clone(), lake.hwm.subscribe(), Duration::from_secs_f64(tier_secs));
                    tokio::spawn(async move {
                        loop {
                            let start = tokio::time::Instant::now();
                            hwm.borrow_and_update();
                            if let Err(e) = a.tier_all(0).await {
                                eprintln!("background job failed: {e:#}");
                            }
                            tokio::time::sleep_until(start + period).await;
                            let _ = tokio::time::timeout(period * 4, hwm.changed()).await; // (right away if rows came meanwhile)
                        }
                    });
                }
                // …and any table as soon as a million rows wait in the log (bounds memory and read cost).
                let a = app.clone();
                every(if tier_secs == 0.0 { Duration::ZERO } else { Duration::from_secs(1) }, move || { let a = a.clone(); async move { a.tier_all(1_000_000).await.map(|_| ()) } });
                // Persist the catalog's memtable every few seconds when something changed, so a
                // restart (or a new leader, reader or `pondra sql`) replays only a few seconds of
                // catalog WAL. Not more often: each is a level-0 file that SlateDB's compactor
                // has to keep up with, and writes stall when too many pile up.
                let l = lake.clone();
                every(Duration::from_secs(5), move || { let l = l.clone(); async move { l.cat.checkpoint().await } });
            } else {
                match reader {
                    false => cluster.clone().follow(store),
                    true => cluster.clone().watch_leader(store, streamed), // a reader never votes or leads
                }
                if streamed {
                    cluster::mirror(lake.clone(), cluster.leader.addr.clone());
                }
                // How far our own catalog view is: all a reader has, and a follower's fallback.
                every(Duration::from_millis(250), move || { let l = lake.clone(); async move { l.refresh().await } });
            }
            if !reader {
                let (a, max_wait) = (app.clone(), Duration::from_millis(task_ms));
                tokio::spawn(async move {
                    let mut commits = a.lake.hwm.subscribe();
                    loop {
                        let _ = tokio::time::timeout(max_wait, commits.changed()).await; // new rows, or time's up
                        if let Err(e) = a.run_tasks().await {
                            eprintln!("streaming tasks failed: {e:#}");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                });
            }
            let role = if reader { "reader" } else if leader { "leader" } else { "follower" };
            eprintln!("pondra {role} (term {}) serving {dir} on {addr}", cluster.leader.n);
            axum::serve(tokio::net::TcpListener::bind(&addr).await?, server::router(app)).await?;
        }
        Cmd::Catalog { dir, prefix } => {
            let lake = store::Lake::open(&dir, false, false).await?;
            match prefix.starts_with("d/") {
                true => println!("{prefix}: {:?} bytes", lake.cat.get_raw(&prefix).await?.map(|b| b.len())), // inline data: binary
                false => {
                    for (k, v) in lake.cat.scan::<serde_json::Value>(&prefix, &format!("{prefix}\u{10ffff}")).await? {
                        println!("{k} {v}");
                    }
                }
            }
        }
        Cmd::Sql { dir, query } => {
            let lake = store::Lake::open(&dir, false, false).await?;
            let batches = query::session(&lake, &query, "").await?.sql(&query).await?.collect().await?;
            println!("{}", pretty_format_batches(&batches)?);
        }
    }
    Ok(())
}

/// Run `job` forever, starting every `period` (right away if a run took longer; a zero period
/// disables it).
fn every<F, Fut>(period: Duration, job: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send,
{
    if period.is_zero() {
        return;
    }
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            if let Err(e) = job().await {
                eprintln!("background job failed: {e:#}");
            }
        }
    });
}
