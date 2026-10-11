//! The `pondra` command: its subcommands and options, and what each one starts.
use crate::*;
use clap::Parser;
use datafusion::arrow::util::pretty::pretty_format_batches;
use std::sync::Arc;

/// Pondra: a streamhouse in one binary. With no command, a SQL shell on a lake.
#[derive(Parser)]
#[command(version, args_conflicts_with_subcommands = true)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) cmd: Option<Cmd>,
    /// With no command: the lake the SQL shell opens (a folder or s3://bucket/prefix; default ./lake).
    lake: Option<String>,
}

#[derive(clap::Subcommand)]
pub(crate) enum Cmd {
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
    /// The lake's files kept in a folder of your own, for git and your editor: `pondra workspace
    /// pull ./ws` writes them there, `pondra workspace push ./ws` sends back what changed. A file
    /// changed on both sides is listed and left as it is.
    Workspace {
        #[command(subcommand)]
        cmd: sync::Command,
    },
    #[command(flatten)]
    Project(project::Command),
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
    /// Lead a lake for a moment: make it if nothing is there yet, answer what waits in its inbox,
    /// let go. A node runs it for a lake nobody leads (`inbox::lead_once`, invariant 62).
    #[command(hide = true)]
    Lead {
        #[arg(long)]
        dir: String,
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

pub fn main() {
    if std::env::args().nth(1).as_deref() == Some("service") && std::env::args().nth(2).as_deref() == Some("run") {
        service::run_from_manager(); // (what a service manager starts: becomes the node)
    }
    // The work runs on threads with Linux's main stack, 8 MB: Windows gives its main thread 1 MB,
    // and planning (DataFusion's, recursive) and a session's making can need more; tokio's workers
    // get 2 MB, which procedures calling procedures 16 deep overflowed in the release build (only
    // the pages a thread touches are memory).
    let work = std::thread::Builder::new().name("pondra".into()).stack_size(8 << 20).spawn(|| {
        // Few blocking threads, gone after a second idle (`panics::runtime`): kept alive, each held
        // the memory its work had used, and a node under steady writes grew for as long as it ran.
        panics::runtime().build().expect("a runtime").block_on(async {
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
                    let lake = dbserver::is_lake(&p).await?; // (one being made too: nodes started at once)
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
            // Stopped (Ctrl-C, SIGTERM from a scheduler scaling down, or the program that started
            // this node ending): drained first (`drain.rs`; a second signal stops at once), then the
            // next node leads at once instead of waiting out the lease. Watched from before the node
            // starts, so a stop while it starts is seen too (invariant 46).
            let mut stop = panics::spawn(stopped(stop_with_stdin));
            let mut node = tokio::select! {
                // (a node stopped while it starts ends there: nothing serves yet to drain, and a
                // leader's term runs out with its lease rather than being given up)
                started = node::Node::start(node::Options { dir, listen: addr, advertise, reader, flush_ms, tier_secs, task_ms, retain_secs, backlog, pg, kafka, kafka_advertise, flight, attach: attached, attach_found, python, auth }) => started?,
                _ = &mut stop => std::process::exit(0),
            };
            tokio::select! {
                served = node.serving() => served?,
                _ = &mut stop => {
                    tokio::select! { _ = drain::drain() => {}, _ = stopped(false) => {} }
                    node.stop().await;
                    std::process::exit(0);
                }
            }
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
        Cmd::Workspace { cmd } => sync::command(cmd).await?,
        Cmd::Project(cmd) => project::command(cmd).await?,
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
        Cmd::Lead { dir } => write::lead_only(&dir).await?,
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
            // `USE` sets a session's path, and this command has no session (`path.rs`).
            anyhow::ensure!(!path::is_use(&query) && path::set_schema(&query).is_none(), "{}", settings::NO_SESSION);
            // As a node takes SQL: functions and files (`read_csv(…)`, `'x.parquet'`) expanded, then
            // a write or a query. A write to a folder with no lake yet makes one.
            let lake = match write::parse(&query) {
                Some(_) => write::made(&dir).await?,
                None => store::Lake::open(&dir, false, false).await?,
            };
            for spec in &attached {
                node::attach(&lake, spec, "", false).await?;
            }
            ddl::sync(&lake, "", false).await?;
            let query = routines::expand(&lake, &query).await?;
            match write::parse(&query) {
                Some(stmt) => println!("{}", ext::scope(true, write::from_cli(&dir, stmt)).await?), // (its user's own machine: its files, its credentials)
                None => {
                    let planned = asof::rewrite(&query)?; // (as every door: ASOF JOIN, and `*` without the system columns a query names)
                    let run = async { anyhow::Ok(query::session(&lake, &query, "").await?.enable_url_table().sql(&planned).await?.collect().await?) };
                    let batches = ext::scope(true, auth::WHO.scope(auth::Principal::token(auth::Role::Admin), run)).await?; // (and its storage: attached lakes too)
                    println!("{}", pretty_format_batches(&batches)?);
                }
            }
        }
    }
    Ok(())
}
