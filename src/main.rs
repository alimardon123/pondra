//! Pondra: a streamhouse in one binary (see ADR-002 to ADR-005).
//! Object storage — a local dir or s3://bucket/prefix (S3, R2, MinIO) — is the only state.
#![recursion_limit = "256"] // (the Send check of a query's future, many awaits deep)
mod adopt;
mod audit;
mod ai;
mod avro;
mod branch;
mod bridge;
mod asof;
mod auth;
mod budget;
mod cache;
mod change;
mod codes;
mod copy;
mod guard;
mod hilbert;
mod history;
mod ddl;
mod defaults;
mod delta;
mod ext;
mod feeds;
mod files;
mod format;
mod friendly;
mod flight;
mod fresh;
mod fsum;
mod hot;
mod index;
mod constraints;
mod intervals;
mod layout;
mod iceberg;
mod inbox;
mod kafka;
mod kafka_client;
mod live;
mod serve;
mod settings;
mod shares;
mod sharing;
mod shell;
mod cluster;
mod console;
mod xlsx;
mod dbserver;
mod drain;
mod log;
mod manifest;
mod objects;
mod once;
mod metrics;
mod optimize;
mod pages;
mod panics;
mod past;
mod tls;
mod txn;
mod mcp;
mod pg;
mod pg_catalog;
mod query;
mod read_delta;
mod read_iceberg;
mod ranges;
mod pyfn;
mod python;
mod runs;
mod sketch;
mod skew;
mod replica;
mod routines;
mod scan;
mod script;
mod seq;
mod server;
mod service;
mod spill;
mod sparksql;
mod spmd;
mod store;
mod sys;
mod tasks;
mod temp;
mod tier;
mod udf;
mod users;
mod vend;
mod vars;
mod views;
mod write;
mod write_outside;
mod workspace;

use clap::Parser;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc; // returns freed memory to the OS promptly (glibc malloc holds on to it)
use datafusion::arrow::util::pretty::pretty_format_batches;
use std::{future::Future, sync::Arc, time::Duration};

/// Pondra: a streamhouse in one binary. With no command, a SQL shell on a lake.
#[derive(Parser)]
#[command(version, args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
    /// With no command: the lake the SQL shell opens (a folder or s3://bucket/prefix; default ./lake).
    lake: Option<String>,
}

#[derive(clap::Subcommand)]
enum Cmd {
    /// Serve a lake, or a folder of lakes as databases: `pondra serve lake`, `pondra serve data`.
    ///
    /// A lake: nodes started on the same lake form a cluster; the first leads (ingest, tiering,
    /// commits), the rest follow (SQL, task shards, writes forwarded to the leader) and take over
    /// if the leader dies. `--reader`: SQL only, never leads.
    ///
    /// A folder whose subfolders hold lakes: each is a database by its folder's name. Postgres
    /// clients pick one by name (`psql -d sales`), HTTP clients by `/db/sales/…`, and the console
    /// at `/` lists them. Each runs as a node of its own, started when first used and stopped when
    /// idle (PONDRA_DATABASE_IDLE_SECS, 600); `CREATE DATABASE` makes another.
    Serve {
        /// The lake (a folder, or s3://bucket/prefix, its credentials from the AWS_* variables), or
        /// a folder of lakes. Default: this folder. A new or empty one becomes a lake.
        path: Option<String>,
        /// For scripts and services, nothing guessed: exactly this lake (made if it isn't there).
        #[arg(long = "lake", alias = "dir", value_name = "LAKE", conflicts_with_all = ["path", "databases"])]
        dir: Option<String>,
        /// For scripts and services, nothing guessed: exactly this folder of lakes, each a database.
        #[arg(long = "lakes", alias = "databases", value_name = "FOLDER", conflicts_with = "path")]
        databases: Option<String>,
        /// Serving a folder of lakes: the database a client that names none gets (default: `lake`,
        /// else the only one).
        #[arg(long)]
        default: Option<String>,
        /// Address other nodes reach this one at (also the listen address).
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: String,
        /// Address other nodes reach this one at, if not `--addr`: a database's node, behind the
        /// process serving its folder, `host:port/db/name` (it passes it through).
        #[arg(long)]
        advertise: Option<String>,
        /// Read-only node: any number can run next to the single writer.
        #[arg(long)]
        reader: bool,
        /// Minimum milliseconds between a node's flushes. 0: flush as soon as the previous flush
        /// is committed (what queued up meanwhile goes together), for the lowest latency.
        #[arg(long, default_value_t = 0)]
        flush_ms: u64,
        /// Tiering starts as soon as rows commit, at most once per this many seconds (fractions
        /// allowed): how soon new rows are Parquet, and a Delta or Iceberg version other engines
        /// can read (Pondra's own reads see every commit at once). Each run costs a few
        /// object-store writes per busy table, so not more often than that by default; a table
        /// with a million rows waiting is tiered within a second anyway (0 = only via POST /tier).
        #[arg(long, default_value_t = 10.0)]
        tier_secs: f64,
        /// Streaming tasks run as soon as new rows commit, and at least this often (milliseconds).
        #[arg(long, default_value_t = 1000)]
        task_ms: u64,
        /// Memory for queries, in GB (also PONDRA_MEMORY_GB; default: a third of the machine's).
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
        /// Size of that SSD tier in GB (0 turns it off; default: 20, or a quarter of the free
        /// disk if that is less).
        #[arg(long)]
        cache_gb: Option<u64>,
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
        /// TLS on every door (also PONDRA_TLS_CERT, PONDRA_TLS_KEY): a PEM certificate and its key.
        /// Plain connections are then taken only from this machine (PONDRA_TLS=optional: from
        /// anywhere), and nodes call each other over HTTPS.
        #[arg(long)]
        tls_cert: Option<String>,
        #[arg(long)]
        tls_key: Option<String>,
        /// The authority that signs the nodes' certificates (also PONDRA_TLS_CA): nodes show theirs
        /// to each other (mutual TLS), and the nodes' key is taken only with one.
        #[arg(long)]
        tls_ca: Option<String>,
        /// Another lake to read as `name.table`, and write to through its own leader (repeatable):
        /// `--attach sales=s3://bucket/sales`. Several clusters, each leading its own lake, share
        /// one bucket this way.
        #[arg(long)]
        attach: Vec<String>,
        /// Attach, for as long as this node runs, the lakes in this folder (its subfolders that hold
        /// one) under their folder names: the shell passes its own folder, so the lakes side by
        /// side are its databases. Nothing is saved in the catalog.
        #[arg(long)]
        attach_found: Option<String>,
        /// Stop, as on Ctrl-C, when standard input closes: when the program that started this
        /// node ends, however it ends (`pondra.local()` in Python and JavaScript, the shell).
        #[arg(long)]
        stop_with_stdin: bool,
        /// Run Python functions and procedures (`LANGUAGE python`) with this Python, which has the
        /// `pondra` package and pyarrow (`auto`: the first found), on warm workers beside the node,
        /// gone after a minute idle. A routine runs any code on this machine, so only an admin
        /// token makes one, and a node without tokens takes this only when it listens on 127.0.0.1.
        #[arg(long)]
        python: Option<String>,
    },
    /// Keep a node running on this machine: started at boot and again if it stops (systemd,
    /// launchd or Windows's service manager). `pondra service install --lake s3://bucket/lake`.
    Service {
        #[command(subcommand)]
        cmd: service::Command,
    },
    /// Print catalog entries whose keys start with `prefix` (t/ tables, s/ segments, p/ producers…).
    Catalog {
        #[arg(long, visible_alias = "lake")]
        dir: String,
        prefix: String,
    },
    /// Run SQL straight against the lake, no server needed. Queries read the bucket (and local
    /// files: `SELECT * FROM 'x.parquet'`). Writes (CREATE TABLE, INSERT, UPDATE, DELETE) run
    /// here too; the leader records them — over HTTP, or through the bucket if this machine can't
    /// reach it — or this process does if nobody leads.
    Sql {
        #[arg(long, visible_alias = "lake")]
        dir: String,
        /// Other lakes to read as `name.table` (`name=dir`, repeatable).
        #[arg(long)]
        attach: Vec<String>,
        query: String,
    },
    /// Run a SQL file — its statements in order, `$name` taking the value of `--name` — on a node
    /// of the lake started for it (`pondra run load.sql lake --day 2026-09-27`), or on a node
    /// already running (`--url http://host:8080`). `pondra run load.sql --help` lists its parameters.
    #[command(disable_help_flag = true)]
    Run {
        file: Option<String>,
        /// This help, and the file's parameters (its DECLAREs: type, default, what each means).
        #[arg(short, long)]
        help: bool,
        #[arg(long)]
        url: Option<String>,
        /// A token for that node (also PONDRA_TOKEN).
        #[arg(long)]
        token: Option<String>,
        /// The lake (a folder or s3://bucket/prefix; default ./lake), unless `--url`; then the file's
        /// parameters, `--name value` … (`--url` and `--token` may come among them too).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, value_name = "LAKE] [--NAME VALUE")]
        rest: Vec<String>,
    },
}

/// `pondra run --help`, and with a file its parameters, as `--name` options (ADR-037).
fn run_help(file: Option<&str>) -> anyhow::Result<()> {
    use clap::CommandFactory;
    let mut cmd = Cli::command();
    let run = cmd.find_subcommand_mut("run").expect("the run command");
    let Some(file) = file else { return Ok(run.print_help()?) };
    let text = std::fs::read_to_string(file).map_err(|e| anyhow::anyhow!("{file}: {e}"))?;
    let params = crate::workspace::parameters(file, &text)?.unwrap_or_default();
    let arg = |p: &vars::Param| format!("--{} {}", p.name, p.ty.as_deref().unwrap_or("VALUE").to_uppercase());
    println!("Usage: pondra run {file} [LAKE] {}", params.iter().map(|p| match p.required { true => arg(p), false => format!("[{}]", arg(p)) }).collect::<Vec<_>>().join(" "));
    println!("\n{}", match params.is_empty() { true => format!("{file} takes no parameters."), false => format!("Parameters of {file}:") });
    let width = params.iter().map(|p| arg(p).len()).max().unwrap_or(0);
    for p in &params {
        let said = [p.description.clone(), Some(p.default.as_ref().map_or("required".into(), |d| format!("default: {d}")))];
        println!("  {:width$}  {}", arg(p), said.into_iter().flatten().collect::<Vec<_>>().join(" · "));
    }
    println!("\nOptions: --url URL (a node already running), --token TOKEN (also PONDRA_TOKEN)");
    Ok(())
}

/// Ctrl-C, SIGTERM (how schedulers and `kill` stop a process), or with `stdin`, standard input
/// closing (on every OS alike, and even when the program that started this one was killed).
pub(crate) async fn stopped(stdin: bool) {
    let closed = async move {
        match stdin {
            true => drop(tokio::task::spawn_blocking(|| std::io::copy(&mut std::io::stdin(), &mut std::io::sink())).await),
            false => std::future::pending().await,
        }
    };
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {}, _ = closed => {} }
    }
    #[cfg(not(unix))]
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = closed => {} }
}

/// The lake this node serves (its catalog checkpointed when it stops).
static MAIN: std::sync::OnceLock<Arc<store::Lake>> = std::sync::OnceLock::new();

fn main() {
    if std::env::args().nth(1).as_deref() == Some("service") && std::env::args().nth(2).as_deref() == Some("run") {
        service::run_from_manager(); // (what a service manager starts: becomes the node)
    }
    // The work runs on threads with Linux's main stack, 8 MB: Windows gives its main thread 1 MB,
    // and planning (DataFusion's, recursive) and a session's making can need more; tokio's workers
    // get 2 MB, which procedures calling procedures 16 deep overflowed in the release build (only
    // the pages a thread touches are memory).
    let work = std::thread::Builder::new().name("pondra".into()).stack_size(8 << 20).spawn(|| {
        // Blocking threads (file reads and writes, the SSD tier) go after a second idle, not tokio's
        // ten: tiering every 10 s kept ~70 of them alive on 4 cores, and each kept the memory its
        // biggest piece of work had used. A node under steady writes held 1 GB of its own after ten
        // minutes, growing 60 MB a minute; with this, 250 MB, growing 7 (soak.py).
        let keep = std::time::Duration::from_secs(1);
        tokio::runtime::Builder::new_multi_thread().enable_all().thread_stack_size(8 << 20).thread_keep_alive(keep).build().expect("a runtime").block_on(async {
            // An error is said in words, its causes after it: a backtrace (RUST_BACKTRACE) is for panics.
            if let Err(e) = run().await {
                eprintln!("Error: {}", ext::said(&e));
                std::process::exit(1);
            }
        })
    });
    if work.expect("a thread to work on").join().is_err() {
        std::process::exit(101); // (a panic, said already: as Rust's main says one)
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let Some(cmd) = cli.cmd else { return shell::run(&cli.lake.unwrap_or_else(|| "lake".into())).await };
    match cmd {
        Cmd::Serve { path, dir, databases, default, addr, advertise, reader, flush_ms, tier_secs, task_ms, memory_gb, retain_secs, changelog_secs, backlog, cache_dir, cache_gb, ack, replicas, fsync, publish, pg, kafka, kafka_advertise, flight, read_token, write_token, admin_token, tls_cert, tls_key, tls_ca, attach: attached, attach_found, stop_with_stdin, python } => {
            for (flag, var) in [(tls_cert, "PONDRA_TLS_CERT"), (tls_key, "PONDRA_TLS_KEY"), (tls_ca, "PONDRA_TLS_CA")] {
                if let Some(v) = flag {
                    std::env::set_var(var, v); // (read by `tls.rs`)
                }
            }
            tls::init()?; // (a certificate that can't be read: said now)
            // A lake, or a folder of lakes (each a database: `dbserver.rs`), by what the path holds.
            let (dir, many) = match (dir, databases) {
                (Some(d), _) => {
                    // (one letter from --lakes: a mix-up is said, never served)
                    anyhow::ensure!(!dbserver::holds_lakes(&d).await?, "{d} holds lakes, each a database: serve them with --lakes {d} (or one of them: --lake {d}/<name>)");
                    (d, false)
                }
                (None, Some(f)) => {
                    anyhow::ensure!(!dbserver::is_lake(&f).await?, "{f} is a lake: serve it with --lake {f} (--lakes is for a folder of lakes)");
                    (f, true)
                }
                (None, None) => {
                    let given = path.is_some();
                    let p = path.unwrap_or_else(|| ".".into());
                    let many = dbserver::holds_lakes(&p).await?;
                    // (this folder is made a lake only when named: `pondra serve .`; and a folder with
                    // other things in it, never: a mistyped path doesn't become a lake)
                    let lake = std::path::Path::new(&p).join("catalog").is_dir();
                    anyhow::ensure!(given || many || lake, "no lake in this folder, and no lakes in its folders: `pondra serve lake` makes one at ./lake (or name yours: pondra serve <folder>)");
                    let empty = std::fs::read_dir(&p).map_or(true, |mut d| d.next().is_none());
                    anyhow::ensure!(many || lake || empty || ext::scheme(&p).is_some_and(|s| s != "file"), "{p} holds other things than lakes: serve a lake in it (pondra serve {p}/lake), or make one there anyway with --lake {p}");
                    (p, many)
                }
            };
            if many {
                let one = [(flight.is_some(), "--flight"), (kafka.is_some(), "--kafka"), (!attached.is_empty(), "--attach"), (advertise.is_some(), "--advertise"), (attach_found.is_some(), "--attach-found")];
                if let Some((_, flag)) = one.iter().find(|(given, _)| *given) {
                    anyhow::bail!("{dir} holds several lakes, served as databases; {flag} is one lake's (serve that lake: pondra serve {dir}/<name> {flag} …)");
                }
                let env = |flag: Option<String>, var: &str| flag.or_else(|| std::env::var(var).ok()).filter(|t| !t.is_empty());
                let (r, w, a) = (env(read_token, "PONDRA_READ_TOKEN"), env(write_token, "PONDRA_WRITE_TOKEN"), env(admin_token, "PONDRA_ADMIN_TOKEN"));
                for (var, v) in [("PONDRA_READ_TOKEN", &r), ("PONDRA_WRITE_TOKEN", &w), ("PONDRA_ADMIN_TOKEN", &a)] {
                    if let Some(v) = v {
                        std::env::set_var(var, v); // (each database's node takes them from here)
                    }
                }
                let auth = Arc::new(auth::Auth::new(r, w, a));
                let local = ["127.0.0.1:", "localhost:", "[::1]:"].iter().any(|x| addr.starts_with(x));
                anyhow::ensure!(python.is_none() || auth.on() || local, "--python lets whoever makes a procedure run code on this machine: set --admin-token, or listen on 127.0.0.1");
                // Each database's node gets the rest, as `pondra serve <lake>` would take them.
                let mut node: Vec<String> = ["--flush-ms", &flush_ms.to_string(), "--task-ms", &task_ms.to_string(), "--retain-secs", &retain_secs.to_string(),
                    "--changelog-secs", &changelog_secs.to_string(), "--backlog", &backlog.to_string(), "--ack", &ack, "--replicas", &replicas.to_string()].map(String::from).to_vec();
                for (flag, v) in [("--memory-gb", memory_gb.map(|m| m.to_string())), ("--cache-dir", cache_dir), ("--cache-gb", cache_gb.map(|g| g.to_string())), ("--publish", Some(publish).filter(|p| !p.is_empty()))] {
                    if let Some(v) = v {
                        node.extend([flag.to_string(), v]);
                    }
                }
                if fsync {
                    node.push("--fsync".into());
                }
                let options = dbserver::Options { python, tier_secs: Some(tier_secs), reader, node };
                return dbserver::serve(dir, addr, pg, default, options, auth).await;
            }
            let env = |flag: Option<String>, var: &str| flag.or_else(|| std::env::var(var).ok()).filter(|t| !t.is_empty());
            let auth = Arc::new(auth::Auth::new(env(read_token, "PONDRA_READ_TOKEN"), env(write_token, "PONDRA_WRITE_TOKEN"), env(admin_token.clone(), "PONDRA_ADMIN_TOKEN")));
            if let Some(t) = env(admin_token, "PONDRA_ADMIN_TOKEN") {
                std::env::set_var("PONDRA_ADMIN_TOKEN", t); // (nodes call each other with it: cluster::http)
            }
            let local = ["127.0.0.1:", "localhost:", "[::1]:"].iter().any(|a| addr.starts_with(a));
            anyhow::ensure!(python.is_none() || auth.on() || local, "--python lets whoever makes a procedure run code on this machine: set --admin-token, or listen on 127.0.0.1");
            if let Some(gb) = cache_gb {
                std::env::set_var("PONDRA_CACHE_GB", gb.to_string()); // read by Lake::open
            }
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
            let t_start = std::time::Instant::now();
            let tr = |w: &str| store::trace(w, t_start);
            let listen = addr.clone();
            let addr = advertise.unwrap_or(addr); // (how others reach it: its cluster name)
            sharing::set_endpoint(&addr); // (where a recipient's profile points: `sharing.rs`)
            let cluster = cluster::Cluster::join(&store, &addr, reader).await?;
            tr("the lease");
            let leader = cluster.is_leader();
            if leader {
                // "Still here", in the bucket, from the start: for machines outside the cluster.
                cluster.clone().keep_alive(store.clone(), &dir); // (beside the catalog's opening, not before it: C5)
            }
            // Stopped (Ctrl-C, SIGTERM from a scheduler scaling down, or the program that started
            // this node ending): drained first (`drain.rs`; a second signal stops at once), then the
            // next node leads at once instead of waiting out the lease.
            let (s, n) = (store.clone(), leader.then_some(cluster.leader.n));
            panics::spawn(async move {
                stopped(stop_with_stdin).await;
                tokio::select! { _ = drain::drain() => {}, _ = stopped(false) => {} }
                if let Some(n) = n {
                    if let Some(l) = MAIN.get() {
                        // (what it committed in the bucket, with replicated acks too; the catalog's
                        // memtable written out: the next leader replays no WAL, C5)
                        let _ = tokio::time::timeout(Duration::from_secs(10), l.cat.wait_durable(l.cat.committed())).await;
                        let _ = tokio::time::timeout(Duration::from_secs(5), l.cat.checkpoint()).await;
                    }
                    cluster::step_down(&s, n).await;
                }
                std::process::exit(0);
            });
            // Read-only nodes follow the leader's commit stream too (when a live one is there to ask),
            // so their reads are as fresh as a follower's instead of waiting for catalog polls.
            let streamed = !leader && !cluster.leader.addr.is_empty() && (!reader || cluster.leader_alive().await);
            let lake = match store::Lake::open(&dir, leader, streamed).await {
                // (a lake a newer Pondra wrote: this binary must not lead or serve it, nor hold the term)
                Err(e) if e.downcast_ref::<format::Newer>().is_some() => {
                    if leader {
                        cluster::release(&store, cluster.leader.n).await;
                    }
                    return Err(e);
                }
                Err(e) if leader => {
                    eprintln!("opening the lake as leader failed: {e:#}"); // e.g. a newer leader fenced us
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    cluster::restart("opening the lake as leader failed")
                }
                // A new lake whose first leader hasn't made the catalog yet (nodes started
                // together), or never will (it died first): look again, until it has or its
                // mark goes stale and this node takes over.
                Err(e) if ["failed to find latest transactional object", store::Lake::NO_LAKE].iter().any(|m| format!("{e:#}").contains(m)) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    cluster::restart("the lake has no catalog yet: its leader is still making it")
                }
                lake => lake?,
            };
            let _ = MAIN.set(lake.clone());
            tr("the lake open");
            for spec in &attached {
                attach(&lake, spec, &addr, true).await?;
            }
            if let Some(folder) = &attach_found {
                let found = ddl::attach_found(&lake, folder, &addr).await;
                if !found.is_empty() {
                    eprintln!("attached the lakes in {folder}: {}", found.join(", "));
                }
            }
            // The lakes attached in SQL (`ATTACH … AS …`), and later ATTACHes and DETACHes.
            tr("lakes attached");
            let (l, me) = (lake.clone(), addr.clone());
            ddl::sync(&l, &me, true).await?;
            tr("attachments synced");
            every(Duration::from_secs(1), move || { let (l, me) = (l.clone(), me.clone()); async move { ddl::sync(&l, &me, true).await } });
            let l = lake.clone();
            tokio::spawn(async move { l.warm().await.map_err(|e| eprintln!("warming the SSD tier: {e:#}")) });
            // Followers keep the leader's changes that aren't in the bucket yet (replicated acks);
            // a new leader first commits what they hold from the previous one.
            tr("warming");
            let replica = if reader { None } else { Some(replica::ReplicaLog::open(replica::dir(&dir, &addr))?) };
            if let (true, Some(own)) = (leader, &replica) {
                replica::recover(&lake, &addr, cluster.leader.n, Some(own)).await?;
            }
            tr("followers' changes recovered");
            let max_backlog = (tier_secs > 0.0).then_some(backlog); // rows waiting to be tiered
            let seq = if leader { Some(log::Sequencer::start(lake.clone(), max_backlog).await?) } else { None };
            let to = match &seq {
                Some(s) => log::To::Local(s.clone()),
                None => log::To::Leader(cluster.leader.addr.clone()),
            };
            let _ = lake.to.set(to.clone()); // (a follower's sequences' values: the leader's sequencer)
            let log = (!reader).then(|| Arc::new(log::Log::start(lake.clone(), Duration::from_millis(flush_ms), to)));
            tr("the sequencer");
            python::init(python);
            let app = server::App { lake: lake.clone(), cluster: cluster.clone(), log, seq, lock: Default::default(), retain_ms: retain_secs * 1000, results: Default::default(), replica: replica.clone(), auth };
            if leader {
                let l = app.lake.clone(); // (beside serving, not before it: C5)
                tokio::spawn(async move {
                    if let Err(e) = users::make_keys(&l).await {
                        eprintln!("the lake's keys (sessions', the nodes'): {e:#}"); // (sessions' signing key, and the nodes' own: `users.rs`)
                    }
                    match ext::rewrap(&l).await {
                        Ok(0) => {}
                        Ok(n) => eprintln!("rewrapped {n} secret{} with the master key in use now", if n == 1 { "" } else { "s" }),
                        Err(e) => eprintln!("secrets not rewrapped: {e:#}"),
                    }
                });
            }
            tr("the app");
            let l = app.lake.clone();
            panics::spawn(async move {
                // (a follower's catalog shows the leader's keys once the leader has flushed them)
                for _ in 0..600 {
                    if let Ok(k) = users::node_key(&l).await {
                        std::env::set_var("PONDRA_NODE_KEY", k); // (nodes call each other with it when no admin token is set: cluster::http)
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                eprintln!("the lake's key for its nodes isn't readable here: with users and no --admin-token, this node can't call the others");
            });
            if cluster.is_leader() && !cluster.reader {
                runs::schedule(app.clone()); // (tasks: the leader runs their ticks)
            }
            if let Some(pg_addr) = pg {
                let a = app.clone();
                panics::spawn(async move { pg::serve(a, pg_addr).await.map_err(|e| eprintln!("postgres protocol: {e:#}")) });
            }
            if let Some(kafka_addr) = kafka {
                let port = kafka_addr.rsplit_once(':').map_or("9092", |(_, p)| p).to_string();
                let advertise = kafka_advertise.unwrap_or_else(|| format!("{}:{port}", addr.rsplit_once(':').map_or("127.0.0.1", |(h, _)| h)));
                let a = app.clone();
                panics::spawn(async move { kafka::serve(a, kafka_addr, advertise).await.map_err(|e| eprintln!("kafka protocol: {e:#}")) });
            }
            if let Some(flight_addr) = flight {
                let a = app.clone();
                panics::spawn(async move { flight::serve(a, flight_addr).await.map_err(|e| eprintln!("flight: {e:#}")) });
            }
            if app.log.is_some() {
                feeds::start(app.clone()); // (other clusters' topics feeding views: none, no work)
            }
            if let (Some(_), Some(log)) = (&app.seq, &app.log) {
                let (lake, log) = (lake.clone(), log.clone()); // windows and sessions past the watermark, emitted once
                panics::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        if let Err(e) = views::emit_all(&lake, &log).await {
                            eprintln!("emission: {e:#}");
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
                // The leader's SSD tier learns of objects other nodes wrote from its own commits (and its hot
                // columns of the files they replaced).
                let l = lake.clone();
                panics::spawn(async move {
                    let (_, mut commits) = l.cat.subscribe();
                    loop {
                        match commits.recv().await {
                            Ok(store::Frame::Change(d)) => l.arrived(&d),
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
                    panics::spawn(async move {
                        loop {
                            let start = tokio::time::Instant::now();
                            hwm.borrow_and_update();
                            let failed = a.tier_all(0).await.inspect_err(|e| eprintln!("background job failed: {e:#}")).is_err();
                            tokio::time::sleep_until(start + period).await;
                            // (right away if rows came meanwhile; a round that failed, say on a full
                            // disk, is tried again then too, not only once more rows come)
                            if !failed {
                                let _ = tokio::time::timeout(period * 4, hwm.changed()).await;
                            }
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
                format::raise(lake.clone(), cluster.clone()); // (once every node knows this build's format: ADR-039)
            } else {
                format::watch(lake.clone());
                cluster::catch_up(lake.clone(), cluster.leader.addr.clone()); // (answers wait until this node holds what the leader had)
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
                panics::spawn(async move {
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
            // No Nagle: a small answer goes out at once, not after the client's delayed ACK (the
            // Postgres, Kafka and Flight ports do the same).
            tr("listening");
            let listener = tls::Doors::bind(&listen, tls::Door::Http).await?; // (TLS too: `tls.rs`)
            store::serving(); // (the catalog's compactor and garbage collector start now: C5)
            runs::mark_stopped(app.clone()); // (runs whose node stopped under them: `stopped`)
            if leader {
                let l = app.lake.clone(); // (what it inherited, replayed from the WAL, into every node's view)
                tokio::spawn(async move { l.cat.checkpoint().await.map_err(|e| eprintln!("checkpoint: {e:#}")) });
            }
            axum::serve(listener, server::router(app).into_make_service_with_connect_info::<tls::Peer>()).await?; // (who asks: `console::save_settings`)
        }
        Cmd::Run { file, url, token, help, rest } => {
            if help || file.is_none() || rest.iter().any(|a| a == "--help" || a == "-h") {
                return run_help(file.as_deref());
            }
            let file = file.unwrap_or_default();
            // (the lake first if it is there; `--url` and `--token` wherever they are; the rest parameters)
            let (mut lake, mut url, mut token, mut params, mut it) = (None, url, token, vec![], rest.into_iter());
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--url" => url = it.next(),
                    "--token" => token = it.next(),
                    _ if !a.starts_with("--") && lake.is_none() && params.is_empty() => lake = Some(a),
                    _ => params.push(a),
                }
            }
            anyhow::ensure!(lake.is_none() || url.is_none(), "run a script on a lake folder or on a node (--url), not both");
            let sql = std::fs::read_to_string(&file).map_err(|e| anyhow::anyhow!("{file}: {e}"))?;
            let body = serde_json::json!({"sql": sql, "params": shell::params(&params)?});
            let token = token.or_else(|| std::env::var("PONDRA_TOKEN").ok());
            print!("{}", shell::script(lake.as_deref().unwrap_or("lake"), url.as_deref(), token.as_deref(), body).await?);
        }
        Cmd::Service { cmd } => service::command(cmd).await?,
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
        Cmd::Sql { dir, query, attach: attached } if write::checkpoint(&query) => {
            // (the leader's work; with nobody leading, the next node to start tiers the log)
            let store = store::open_store(&dir)?.1;
            match cluster::latest(&store).await? {
                Some(t) if !t.addr.is_empty() && cluster::alive(&store, &t).await => {
                    println!("{}", cluster::http().post(crate::tls::url(&format!("{}/sql", t.addr))).body("CHECKPOINT").send().await?.error_for_status()?.text().await?)
                }
                _ => println!("{}", serde_json::json!({"checkpoint": false, "why": "no node runs this lake: the next one to start tiers its log"})),
            }
            let _ = attached;
        }
        Cmd::Sql { dir, query, attach: attached } => {
            // As a node takes SQL: functions and files (`read_csv(…)`, `'x.parquet'`) expanded, then
            // a write or a query. A write to a folder with no lake yet makes one.
            let lake = match write::parse(&query) {
                Some(_) => write::made(&dir).await?,
                None => store::Lake::open(&dir, false, false).await?,
            };
            for spec in &attached {
                attach(&lake, spec, "", false).await?;
            }
            ddl::sync(&lake, "", false).await?;
            let query = routines::expand(&lake, &query).await?;
            match write::parse(&query) {
                Some(stmt) => println!("{}", ext::scope(true, write::from_cli(&dir, stmt)).await?), // (its user's own machine: its files, its credentials)
                None => {
                    let planned = asof::rewrite(&query)?; // (as every door: ASOF JOIN, and `*` without the system columns a query names)
                    let run = async { anyhow::Ok(query::session(&lake, &query, "").await?.enable_url_table().sql(&planned).await?.collect().await?) };
                    let batches = ext::scope(true, run).await?;
                    println!("{}", pretty_format_batches(&batches)?);
                }
            }
        }
    }
    Ok(())
}

/// `--attach name=dir`: read another lake as `name.table`. A node keeps it fresh the way a
/// read-only node does: its leader's commit stream when that answers, and its own catalog view.
async fn attach(home: &store::Lake, spec: &str, me: &str, follow: bool) -> anyhow::Result<()> {
    let (name, dir) = spec.split_once('=').ok_or_else(|| anyhow::anyhow!("--attach takes name=dir"))?;
    ddl::attach(home, name, dir, me, follow, follow).await
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
    panics::spawn(async move {
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
