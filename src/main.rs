//! Pondra: a streamhouse in one binary (see ADR-002 to ADR-005).
//! Object storage — a local dir or s3://bucket/prefix (S3, R2, MinIO) — is the only state.
mod ai;
mod auth;
mod cache;
mod delta;
mod files;
mod flight;
mod hot;
mod iceberg;
mod inbox;
mod kafka;
mod serve;
mod cluster;
mod log;
mod manifest;
mod metrics;
mod optimize;
mod mcp;
mod pg;
mod query;
mod replica;
mod server;
mod spill;
mod spmd;
mod store;
mod tasks;
mod tier;
mod udf;
mod views;
mod write;

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
        /// Memory for queries, in GB (also PONDRA_MEMORY_GB; default: half the machine's).
        /// Sorts, joins and aggregations that need more spill to the temp dir.
        #[arg(long)]
        memory_gb: Option<f64>,
        /// Seconds to keep consumed log segments and replaced files before deleting them.
        #[arg(long, default_value_t = 60)]
        retain_secs: u64,
        /// Keep the log this many seconds as a change feed: `/watch/{t}?after=N` replays every
        /// change since N (upserts and deletes of keyed tables included). 0: only `--retain-secs`.
        #[arg(long, default_value_t = 0)]
        changelog_secs: u64,
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
        /// When a write is acknowledged. `durable`: once it is in the bucket (one object-store
        /// write: a millisecond on local disk, 100s of ms on S3 or R2). `replicated`: once
        /// `--replicas` nodes hold it — the leader in memory, followers on local disk — which
        /// takes milliseconds on any storage; it reaches the bucket a moment later. Give every
        /// node the same setting (any of them may lead).
        #[arg(long, default_value = "durable", value_parser = ["durable", "replicated"])]
        ack: String,
        /// With `--ack replicated`: how many nodes hold a write before it's acknowledged (the
        /// leader is one). Until enough followers are up, writes are acknowledged once durable.
        #[arg(long, default_value_t = 2)]
        replicas: usize,
        /// With `--ack replicated`: followers flush each copy to disk before acknowledging it, so
        /// an acked write survives a power loss of the follower too (costs a disk flush per change).
        #[arg(long)]
        fsync: bool,
        /// Open formats new tables are also published in, for engines that don't know Pondra:
        /// `delta`, `iceberg` or `delta,iceberg` (default: none; Pondra and `pondra sql` read the
        /// lake natively, fresher). Per table: `"publish"` when creating it.
        #[arg(long, default_value = "")]
        publish: String,
        /// Also speak the Postgres wire protocol here (e.g. 0.0.0.0:5432): psql, drivers, BI tools.
        #[arg(long)]
        pg: Option<String>,
        /// Also speak the Kafka protocol here (e.g. 0.0.0.0:9092): Kafka producers write to tables
        /// (a topic is a table), consumers read their log.
        #[arg(long)]
        kafka: Option<String>,
        /// Where Kafka clients are told to connect to this node (default: the host of `--addr`, the
        /// port of `--kafka`).
        #[arg(long)]
        kafka_advertise: Option<String>,
        /// Also speak Arrow Flight and Flight SQL here (e.g. 0.0.0.0:8815): ADBC, JDBC and
        /// pyarrow clients, Arrow in and out; a table's log as a columnar stream.
        #[arg(long)]
        flight: Option<String>,
        /// Access tokens (also PONDRA_READ_TOKEN, PONDRA_WRITE_TOKEN, PONDRA_ADMIN_TOKEN): reading
        /// needs any, writing rows write or admin, tables/views/tasks admin. Give every node the
        /// same ones; none set = no checks.
        #[arg(long)]
        read_token: Option<String>,
        #[arg(long)]
        write_token: Option<String>,
        #[arg(long)]
        admin_token: Option<String>,
        /// Another lake to read as `name.table`, and write to through its own leader (repeatable):
        /// `--attach sales=s3://bucket/sales`. Several clusters, each leading its own lake, share
        /// one bucket this way.
        #[arg(long)]
        attach: Vec<String>,
    },
    /// Print catalog entries whose keys start with `prefix` (t/ tables, s/ segments, p/ producers…).
    Catalog {
        #[arg(long)]
        dir: String,
        prefix: String,
    },
    /// Run SQL straight against the lake, no server needed. Queries read the bucket (and local
    /// files: `SELECT * FROM 'x.parquet'`). Writes (CREATE TABLE, INSERT, UPDATE, DELETE) run
    /// here too; the leader records them — over HTTP, or through the bucket if this machine can't
    /// reach it — or this process does if nobody leads.
    Sql {
        #[arg(long)]
        dir: String,
        /// Other lakes to read as `name.table` (`name=dir`, repeatable).
        #[arg(long)]
        attach: Vec<String>,
        query: String,
    },
}

/// Ctrl-C, or SIGTERM (how schedulers and `kill` stop a process).
async fn stopped() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cmd::parse() {
        Cmd::Serve { dir, addr, reader, flush_ms, tier_secs, task_ms, memory_gb, retain_secs, changelog_secs, backlog, cache_dir, cache_gb, ack, replicas, fsync, publish, pg, kafka, kafka_advertise, flight, read_token, write_token, admin_token, attach: attached } => {
            let env = |flag: Option<String>, var: &str| flag.or_else(|| std::env::var(var).ok()).filter(|t| !t.is_empty());
            let auth = Arc::new(auth::Auth::new(env(read_token, "PONDRA_READ_TOKEN"), env(write_token, "PONDRA_WRITE_TOKEN"), env(admin_token.clone(), "PONDRA_ADMIN_TOKEN")));
            if let Some(t) = env(admin_token, "PONDRA_ADMIN_TOKEN") {
                std::env::set_var("PONDRA_ADMIN_TOKEN", t); // (nodes call each other with it: cluster::http)
            }
            std::env::set_var("PONDRA_CACHE_GB", cache_gb.to_string()); // read by Lake::open
            if let Some(gb) = memory_gb {
                std::env::set_var("PONDRA_MEMORY_GB", gb.to_string()); // read by Lake::open
            }
            std::env::set_var("PONDRA_PUBLISH", publish); // read by POST /tables
            std::env::set_var("PONDRA_CHANGELOG_SECS", changelog_secs.to_string()); // read by tier::expire
            std::env::set_var("PONDRA_FSYNC", fsync.to_string()); // read by replica::ReplicaLog::hold
            std::env::set_var("PONDRA_REPLICAS", if ack == "replicated" { replicas.max(1) } else { 1 }.to_string());
            if let Some(d) = cache_dir {
                std::env::set_var("PONDRA_CACHE_DIR", d);
            }
            let (_, store, _) = store::open_store(&dir)?;
            let cluster = cluster::Cluster::join(&store, &addr, reader).await?;
            let leader = cluster.is_leader();
            if leader {
                // "Still here", in the bucket, from the start: for machines outside the cluster.
                let (s, n) = (store.clone(), cluster.leader.n);
                cluster::mark_alive(&s, n).await?;
                every(Duration::from_secs(10), move || { let s = s.clone(); async move { cluster::mark_alive(&s, n).await } });
                // Stopped (Ctrl-C, or SIGTERM from a scheduler scaling down): the next node leads at
                // once instead of waiting out the lease. Acknowledged writes are already durable.
                let s = store.clone();
                tokio::spawn(async move {
                    stopped().await;
                    cluster::release(&s, n).await;
                    std::process::exit(0);
                });
            }
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
            for spec in &attached {
                attach(&lake, spec, &addr, true).await?;
            }
            let l = lake.clone();
            tokio::spawn(async move { l.warm().await.map_err(|e| eprintln!("warming the SSD tier: {e:#}")) });
            // Followers keep the leader's changes that aren't in the bucket yet (replicated acks);
            // a new leader first commits what they hold from the previous one.
            let replica = if reader { None } else { Some(replica::ReplicaLog::open(replica::dir(&dir, &addr))?) };
            if let (true, Some(own)) = (leader, &replica) {
                replica::recover(&lake, &addr, cluster.leader.n, Some(own)).await?;
            }
            let max_backlog = (tier_secs > 0.0).then_some(backlog); // rows waiting to be tiered
            let seq = if leader { Some(log::Sequencer::start(lake.clone(), max_backlog).await?) } else { None };
            let log = (!reader).then(|| {
                let to = match &seq {
                    Some(s) => log::To::Local(s.clone()),
                    None => log::To::Leader(cluster.leader.addr.clone()),
                };
                Arc::new(log::Log::start(lake.clone(), Duration::from_millis(flush_ms), to))
            });
            let app = server::App { lake: lake.clone(), cluster: cluster.clone(), log, seq, lock: Default::default(), retain_ms: retain_secs * 1000, results: Default::default(), replica: replica.clone(), auth };
            if let Some(pg_addr) = pg {
                let a = app.clone();
                tokio::spawn(async move { pg::serve(a, pg_addr).await.map_err(|e| eprintln!("postgres protocol: {e:#}")) });
            }
            if let Some(kafka_addr) = kafka {
                let port = kafka_addr.rsplit_once(':').map_or("9092", |(_, p)| p).to_string();
                let advertise = kafka_advertise.unwrap_or_else(|| format!("{}:{port}", addr.rsplit_once(':').map_or("127.0.0.1", |(h, _)| h)));
                let a = app.clone();
                tokio::spawn(async move { kafka::serve(a, kafka_addr, advertise).await.map_err(|e| eprintln!("kafka protocol: {e:#}")) });
            }
            if let Some(flight_addr) = flight {
                let a = app.clone();
                tokio::spawn(async move { flight::serve(a, flight_addr).await.map_err(|e| eprintln!("flight: {e:#}")) });
            }
            if let (Some(_), Some(log)) = (&app.seq, &app.log) {
                let (lake, log) = (lake.clone(), log.clone()); // event-time windows past the watermark, emitted once
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        if let Err(e) = views::emit_all(&lake, &log).await {
                            eprintln!("window emission: {e:#}");
                        }
                    }
                });
            }
            // Finished shuffles are forgotten here, and what they spilled deleted.
            every(Duration::from_secs(30), || async { spmd::gc(); Ok(()) });
            if let Some(seq) = &app.seq {
                inbox::serve(lake.clone(), seq.clone(), app.lock.clone()); // writers that can't reach us
            }
            if leader {
                // The leader's SSD tier learns of objects other nodes wrote from its own commits.
                let l = lake.clone();
                tokio::spawn(async move {
                    let (_, mut commits) = l.cat.subscribe();
                    loop {
                        match commits.recv().await {
                            Ok(store::Frame::Change(d)) => l.prefetch(&d),
                            Ok(_) => {}
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
                if lake.cat.replicas > 1 {
                    replica::members(lake.clone(), cluster.clone());
                }
            } else {
                match reader {
                    false => cluster.clone().follow(store),
                    true => cluster.clone().watch_leader(store, streamed), // a reader never votes or leads
                }
                if streamed {
                    cluster::mirror(lake.clone(), cluster.leader.addr.clone(), addr.clone(), replica);
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
        Cmd::Sql { dir, query, attach: attached } => match write::parse(&query) {
            Some(stmt) => println!("{}", write::from_cli(&dir, stmt).await?),
            None => {
                let lake = store::Lake::open(&dir, false, false).await?;
                for spec in &attached {
                    attach(&lake, spec, "", false).await?;
                }
                let batches = query::session(&lake, &query, "").await?.enable_url_table().sql(&query).await?.collect().await?;
                println!("{}", pretty_format_batches(&batches)?);
            }
        },
    }
    Ok(())
}

/// `--attach name=dir`: read another lake as `name.table`. A node keeps it fresh the way a
/// read-only node does: its leader's commit stream when that answers, and its own catalog view.
async fn attach(home: &store::Lake, spec: &str, me: &str, follow: bool) -> anyhow::Result<()> {
    let (name, dir) = spec.split_once('=').ok_or_else(|| anyhow::anyhow!("--attach takes name=dir"))?;
    let leader = cluster::latest(&store::open_store(dir)?.1).await?.map(|t| t.addr).filter(|a| !a.is_empty());
    let live = match (&leader, follow) {
        (Some(a), true) => cluster::http().get(format!("http://{a}/cluster/leader")).timeout(Duration::from_secs(2)).send().await.is_ok(),
        _ => false,
    };
    let other = store::Lake::open(dir, false, live).await?;
    if let (true, Some(a)) = (live, leader) {
        cluster::mirror(other.clone(), a, me.to_string(), None);
    }
    if follow {
        let l = other.clone();
        every(Duration::from_millis(250), move || { let l = l.clone(); async move { l.refresh().await } });
    }
    home.attach(name, other)
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
