//! The `pondra` command: its subcommands and options, and what each one starts.
use crate::*;
use std::io::Write;
use std::sync::Arc;

/// Pondra: a streamhouse in one binary. With no command, a SQL shell on a lake.
#[derive(clap::Parser)]
#[command(version, args_conflicts_with_subcommands = true)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) cmd: Option<Cmd>,
    /// With no command: the lake the SQL shell opens (a folder, s3://bucket/prefix, or :memory: for one held in
    /// memory until the shell ends; default ./lake).
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
        /// a folder of lakes. Default: this folder. A new or empty one becomes a lake. `:memory:`:
        /// a lake held in this node's memory, gone when it stops. `pondra://host:port`: a lake whose
        /// store another node serves (`--serve-store`), which leads it.
        path: Option<String>,
        // (the flags that are node settings, `--memory-gb` and the others, are their entries' in `settings.rs`: `command`)
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
        #[arg(long, default_value_t = node::Start::default().flush_ms)]
        flush_ms: u64,
        /// Tiering starts as soon as rows commit, at most once per this many seconds (fractions
        /// allowed): how soon new rows are Parquet, and a Delta or Iceberg version other engines
        /// can read (Pondra's own reads see every commit at once). Each run costs a few
        /// object-store writes per busy table, so not more often than that by default; a table
        /// with a million rows waiting is tiered within a second anyway (0 = only via POST /tier).
        #[arg(long, default_value_t = node::Start::default().tier_secs)]
        tier_secs: f64,
        /// Streaming tasks run as soon as new rows commit, and at least this often (milliseconds).
        #[arg(long, default_value_t = node::Start::default().task_ms)]
        task_ms: u64,
        /// Seconds to keep consumed log segments and replaced files before deleting them.
        #[arg(long, default_value_t = node::Start::default().retain_secs)]
        retain_secs: u64,
        /// Rows allowed to wait in the log for tiering before commits pause (backpressure).
        /// Lower it for less memory, raise it to absorb longer bursts.
        #[arg(long, default_value_t = node::Start::default().backlog)]
        backlog: u64,
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
        /// node ends, however it ends (a node `connect()` starts beside a Python or JavaScript program, the shell).
        #[arg(long)]
        stop_with_stdin: bool,
        /// End, as on SIGTERM, once nobody has used this node for this many seconds and it has
        /// nothing left to tier (also PONDRA_IDLE_EXIT_SECS): no HTTP or Flight request on its way,
        /// no Postgres or Kafka connection open, no run. A platform that starts it again on the next
        /// request (Cloud Run, Fly, Knative, a systemd socket) then bills only while there is work.
        /// A lake with tasks on a schedule keeps its leader up: they run only while a node does.
        #[arg(long, value_name = "SECS")]
        idle_exit: Option<f64>,
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
        /// The lake: a folder, s3://bucket/prefix, or `:memory:` for one held in this process's memory for its statements.
        #[arg(long, visible_alias = "lake")]
        dir: String,
        /// Other lakes to read as `name.table` (`name=dir`, repeatable).
        #[arg(long)]
        attach: Vec<String>,
        query: String,
    },
    /// Back a lake up into a lake of its own: `pondra backup s3://b/prod s3://b/backups/prod`. Run
    /// again, it copies only what changed since, and keeps what earlier runs need for
    /// `--retention-days` (7 by default), so a restore can go back that far. The backup is read
    /// like any lake, and written only by its next run. Works with no node running.
    Backup {
        /// The lake to back up (a folder or s3://bucket/prefix).
        lake: String,
        /// Where its backup is (a new place the first time, the same one after).
        to: String,
        /// How many days earlier runs stay restorable (7 the first time; the backup keeps the
        /// last one given).
        #[arg(long)]
        retention_days: Option<u64>,
    },
    /// Make a lake again from its backup, in a new place: `pondra restore s3://b/backups/prod
    /// s3://b/prod2 [--at '2026-10-12 03:00:00']` (its newest run at or before that time, in UTC).
    /// Its tasks come back suspended (ALTER TASK name RESUME).
    Restore {
        /// The backup.
        from: String,
        /// The new lake's place: nothing there yet.
        to: String,
        /// Restore the newest run at or before this time: '2026-10-12 03:00:00' (UTC), RFC 3339,
        /// or milliseconds since 1970. The newest run when left out.
        #[arg(long)]
        at: Option<String>,
    },
    /// Make a token for a user of a lake, and print it (once: the lake keeps only its hash). Through
    /// the bucket, so no token is needed: whoever can write the lake's folder owns it. The lake's first
    /// user is `pondra` (made by a node on the network with no token, which prints its token too).
    /// `T=$(pondra token --dir lake)`.
    Token {
        #[arg(long, visible_alias = "lake")]
        dir: String,
        /// The user the token is for.
        #[arg(long, default_value = "pondra")]
        user: String,
        /// The token's name, to drop it by (`DROP TOKEN name FOR user`); default `token-<unix seconds>`.
        #[arg(long)]
        name: Option<String>,
    },
    /// Lead a lake for a moment: make it if nothing is there yet, answer what waits in its inbox,
    /// let go. A node runs it for a lake nobody leads (`node::lead_once`, invariant 62).
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

/// The `pondra` command and its subcommands, as `run` parses them. `serve` also takes the node settings whose entries give
/// them a flag (`settings.rs`), each shown after the flag its entry names.
pub(crate) fn command() -> clap::Command {
    <Cli as clap::CommandFactory>::command().mut_subcommand("serve", |mut serve| {
        let flags: Vec<(&settings::Setting, settings::Flag)> = settings::all().iter().filter_map(|s| Some((s, s.flag?))).collect();
        for &(s, f) in &flags {
            // (its help as a doc comment's is: clap ends one without a period)
            let help = if f.help.ends_with('.') && !f.help.ends_with("..") { &f.help[..f.help.len() - 1] } else { f.help };
            let arg = clap::Arg::new(s.var).long(long(s)).help(Some(help).filter(|h| !h.is_empty()));
            serve = serve.arg(match s.kind {
                settings::Kind::Bool | settings::Kind::Switch => arg.action(clap::ArgAction::SetTrue),
                _ => arg.action(clap::ArgAction::Set).value_name(&s.var["PONDRA_".len()..]).default_value(f.shows).value_parser(move |v: &str| match s.reads(v) {
                    true => Ok(v.to_string()),
                    false => Err(if s.kind == settings::Kind::Int { "not a whole number of 0 or more" } else { "not a number of 0 or more" }),
                }),
            });
        }
        let mut order = Vec::new();
        for (id, at) in serve.get_arguments().filter(|a| !a.is_positional() && flags.iter().all(|(s, _)| a.get_id() != s.var)).map(|a| (a.get_id().to_string(), a.get_long().unwrap_or_default().to_string())).collect::<Vec<_>>() {
            order.push(id);
            after(&at, &flags, &mut order);
        }
        order.extend(flags.iter().map(|(s, _)| s.var.to_string()).filter(|v| !order.contains(v)).collect::<Vec<_>>()); // (after a flag there isn't: last)
        for (i, id) in order.into_iter().enumerate() {
            serve = serve.mut_arg(id, |a| a.display_order(i));
        }
        serve
    })
}

/// `args` parsed as `run` parses its command line (`pondra service install` checks a service's options with it).
pub(crate) fn parse_from(args: impl IntoIterator<Item = String>) -> Result<Cli, clap::Error> {
    let m = command().try_get_matches_from(args)?;
    <Cli as clap::FromArgMatches>::from_arg_matches(&m).map_err(|e| e.format(&mut command()))
}

/// A setting's flag: `--memory-gb` for `PONDRA_MEMORY_GB`.
fn long(s: &settings::Setting) -> String {
    s.name().replace('_', "-")
}

/// Puts in `order` the setting flags shown after the flag `at`, each followed by the ones after it.
fn after(at: &str, flags: &[(&settings::Setting, settings::Flag)], order: &mut Vec<String>) {
    for (s, _) in flags.iter().filter(|(_, f)| f.after == at) {
        order.push(s.var.to_string());
        after(&long(s), flags, order);
    }
}

/// The settings `pondra serve` was given as flags on its command line (`command`), by their names in the environment.
fn flags(serve: &clap::ArgMatches) -> Vec<(&'static str, String)> {
    let on = |s: &&settings::Setting| s.flag.is_some() && serve.value_source(s.var) == Some(clap::parser::ValueSource::CommandLine);
    settings::all().iter().filter(on).map(|s| (s.var, match s.kind {
        settings::Kind::Bool | settings::Kind::Switch => "true".to_string(),
        _ => serve.get_one::<String>(s.var).cloned().unwrap_or_default(),
    })).collect()
}

/// `pondra run --help`, and with a file its parameters, as `--name` options (ADR-037).
fn run_help(file: Option<&str>) -> anyhow::Result<()> {
    let mut cmd = command();
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

/// `pondra serve`'s node failed (`node::stop`, which said why): it is stopped, `stop` (a leader's term given back, so
/// the next node leads at once, invariant 210), within 30 s as a library's host bounds it, and the process ends with 1.
async fn failed_stop(stop: impl std::future::Future<Output = ()>) -> ! {
    if tokio::time::timeout(std::time::Duration::from_secs(30), stop).await.is_err() {
        eprintln!("the node's stop took over 30 s: it ends now");
    }
    crate::memory::leave(); // (what this process made goes first: `process::exit` runs no destructor)
    std::process::exit(1)
}

/// Does a node listen beyond this machine: `--addr`, or `--pg`, `--kafka` or `--flight` when given, on
/// anything but 127.0.0.1, localhost or [::1]?
fn on_network(addr: &str, others: [&Option<String>; 3]) -> bool {
    let loopback = |a: &str| ["127.0.0.1:", "localhost:", "[::1]:"].iter().any(|x| a.starts_with(x));
    !loopback(addr) || others.iter().any(|a| a.as_deref().is_some_and(|a| !loopback(a)))
}

/// SlateDB reports what its background work cannot do (the collector's passes, the compactor's) as `log`
/// records, and the program installed no logger: those errors went nowhere. The command says them on
/// stderr. A library's host owns its own logging, so `pondra.open()` and the C ABI set none.
struct ErrorLines;

impl ::log::Log for ErrorLines {
    fn enabled(&self, record: &::log::Metadata) -> bool { record.level() == ::log::Level::Error }
    fn log(&self, record: &::log::Record) {
        if self.enabled(record.metadata()) {
            // (not `eprintln!`: a closed stderr must not panic a SlateDB task)
            let _ = writeln!(std::io::stderr(), "{}: {}", record.target(), record.args());
        }
    }
    fn flush(&self) {}
}

static ERROR_LINES: ErrorLines = ErrorLines;

pub fn main() {
    crate::machine::install(std::sync::Arc::new(crate::os::Os::default())); // (the program's machine and network, before anything reads them)
    crate::net::install(std::sync::Arc::new(crate::os::Net::default()));
    if ::log::set_logger(&ERROR_LINES).is_ok() {
        ::log::set_max_level(::log::LevelFilter::Error);
    }
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
        // (the command is the program's own work, its operator's: `pondra sql`'s statements, and a node's start before
        // its loops and doors begin, run as the node's: `auth::as_node`. A request a door serves is its caller's)
        panics::runtime().build().expect("a runtime").block_on(crate::auth::as_node(async {
            // An error is said in words, its causes after it: a backtrace (RUST_BACKTRACE) is for panics.
            if let Err(e) = run().await {
                eprintln!("Error: {}", ext::said(&e));
                crate::memory::leave(); // (what this process made goes first: `process::exit` runs no destructor)
                std::process::exit(1);
            }
        }))
    });
    let panicked = work.expect("a thread to work on").join().is_err();
    crate::memory::leave(); // (`main` returns, or exits below, past statics no destructor runs for: what it made goes now)
    if panicked {
        std::process::exit(101); // (a panic, said already: as Rust's main says one)
    }
}

/// The shell's lake, guessed as `serve PATH` guesses one (invariant 142): a word that isn't a folder
/// and is a command with a letter left out or two swapped is that command mistyped, and a folder
/// holding other things is never made a lake. (A word longer than a command is a name: `test2`.)
async fn opened(lake: &str, given: bool) -> anyhow::Result<()> {
    let path = std::path::Path::new(lake);
    if lake.contains("://") || (!given && !path.exists()) {
        return Ok(());
    }
    anyhow::ensure!(!path.is_file(), "{lake} is a file, and pondra opens a lake (a folder): run pondra, then read it with FROM '{lake}'");
    if !path.exists() {
        let commands = command();
        if let Some(c) = commands.get_subcommands().filter(|c| !c.is_hide_set()).map(|c| c.get_name()).find(|c| mistyped(lake, c)) {
            anyhow::bail!("no command {lake}: did you mean `pondra {c}`? (a new lake named {lake}: pondra ./{lake})");
        }
        return Ok(());
    }
    if dbserver::holds_lakes(lake).await? {
        anyhow::bail!("{lake} holds lakes, each a database: open one (pondra {lake}/<name>), or serve them all (pondra serve --lakes {lake})");
    }
    let empty = std::fs::read_dir(lake).map_or(true, |mut d| d.next().is_none());
    anyhow::ensure!(empty || dbserver::is_lake(lake).await?, "{lake} holds other things than a lake: open a lake in it (pondra {lake}/lake)");
    Ok(())
}

/// Is `word` the command `c` with one letter left out, changed, or swapped with the next?
fn mistyped(word: &str, c: &str) -> bool {
    let (w, c): (Vec<char>, Vec<char>) = (word.chars().collect(), c.chars().collect());
    if c.len() < 4 {
        return false; // (`sq`, `fun`: a short name is as likely a lake's)
    }
    match c.len().checked_sub(w.len()) {
        Some(0) => {
            let off: Vec<usize> = (0..w.len()).filter(|&i| w[i] != c[i]).collect();
            off.len() == 1 || (off.len() == 2 && off[1] == off[0] + 1 && w[off[0]] == c[off[1]] && w[off[1]] == c[off[0]])
        }
        Some(1) => (0..c.len()).any(|i| c[..i] == w[..i] && c[i + 1..] == w[i..]),
        _ => false,
    }
}

async fn run() -> anyhow::Result<()> {
    let matches = command().get_matches();
    let cli = <Cli as clap::FromArgMatches>::from_arg_matches(&matches).unwrap_or_else(|e| e.format(&mut command()).exit());
    store::inherit(); // (a lake led for a moment, reached as the process that asked reaches it)
    let Some(cmd) = cli.cmd else {
        let given = cli.lake.is_some();
        let lake = crate::memory::place(&cli.lake.unwrap_or_else(|| "lake".into())); // (`pondra :memory:`: a lake in the node's memory)
        opened(&lake, given).await?;
        return shell::run(&lake).await;
    };
    match cmd {
        Cmd::Serve { path, dir, databases, default, addr, advertise, reader, flush_ms, tier_secs, task_ms, retain_secs, backlog, ack, replicas, pg, kafka, kafka_advertise, flight, attach: attached, attach_found, stop_with_stdin, idle_exit, python } => {
            // The flags that are settings (`settings.rs`) are given to the process at once, before anything reads them, and
            // its children get them too (a folder of lakes' databases, `pondra lead`). A setting not on the command line is
            // the environment's, else its default, as anywhere.
            let mut given = flags(matches.subcommand_matches("serve").expect("serve's own"));
            given.push(("PONDRA_REPLICAS", if ack == "replicated" { replicas.max(1) } else { 1 }.to_string()));
            given.extend(idle_exit.map(|s| ("PONDRA_IDLE_EXIT_SECS", s.to_string())));
            env::give(given)?;
            // A lake, or a folder of lakes (each a database: `dbserver.rs`), by what the path holds.
            let (path, dir) = (path.map(|p| crate::memory::place(&p)), dir.map(|d| crate::memory::place(&d))); // (`:memory:`: in this process's memory)
            // (a node that serves its lake's store: checked before anything is opened; a folder of lakes is never
            // served that way, and `--lakes` names none: `served.rs`)
            let serving = env::flag("PONDRA_SERVE_STORE");
            if databases.is_none() {
                let lake = dir.clone().or_else(|| path.clone()).unwrap_or_else(|| ".".into());
                served::checked(&lake, reader, env::get("PONDRA_ADMIN_TOKEN").is_some(), serving)?;
            }
            let (dir, many) = match (dir, databases) {
                // (a lake another node serves: its place is its URL, and nothing here is made or counted as a folder)
                (Some(d), _) if store::served(&d) => (d, false),
                (Some(d), _) => {
                    // (one letter from --lakes: a mix-up is said, never served)
                    anyhow::ensure!(!dbserver::holds_lakes(&d).await?, "{d} holds lakes, each a database: serve them with --lakes {d} (or one of them: --lake {d}/<name>)");
                    (d, false)
                }
                (None, Some(f)) => {
                    anyhow::ensure!(!store::served(&f), "{f} is a lake served by its node: serve it with --lake {f}");
                    anyhow::ensure!(!dbserver::is_lake(&f).await?, "{f} is a lake: serve it with --lake {f} (--lakes is for a folder of lakes)");
                    (f, true)
                }
                (None, None) if path.as_deref().is_some_and(store::served) => (path.unwrap_or_default(), false),
                (None, None) => {
                    let given = path.is_some();
                    let p = path.unwrap_or_else(|| ".".into());
                    let many = dbserver::holds_lakes(&p).await?;
                    // (this folder is made a lake only when named: `pondra serve .`; and a folder with
                    // other things in it, never: a mistyped path doesn't become a lake)
                    let lake = dbserver::is_lake(&p).await?; // (one being made too: nodes started at once)
                    anyhow::ensure!(given || many || lake, "no lake in this folder, and no lakes in its folders: `pondra serve lake` makes one at ./lake (or name yours: pondra serve <folder>)");
                    let empty = std::fs::read_dir(&p).map_or(true, |mut d| d.next().is_none());
                    anyhow::ensure!(many || lake || empty || crate::ext_names::scheme(&p).is_some_and(|s| s != "file"), "{p} holds other things than lakes: serve a lake in it (pondra serve {p}/lake), or make one there anyway with --lake {p}");
                    (p, many)
                }
            };
            if many {
                anyhow::ensure!(!serving, "--serve-store serves one lake's store: serve a lake (--lake), not a folder of them");
                anyhow::ensure!(idle_exit.is_none() && env::secs("PONDRA_IDLE_EXIT_SECS").is_zero(), "{dir} holds several lakes, served as databases, and each database's node already stops once idle (PONDRA_DATABASE_IDLE_SECS): --idle-exit is for one lake's node");
                let one = [(flight.is_some(), "--flight"), (kafka.is_some(), "--kafka"), (kafka_advertise.is_some(), "--kafka-advertise"), (!attached.is_empty(), "--attach"), (advertise.is_some(), "--advertise"), (attach_found.is_some(), "--attach-found")];
                if let Some((_, flag)) = one.iter().find(|(given, _)| *given) {
                    anyhow::bail!("{dir} holds several lakes, served as databases; {flag} is one lake's (serve that lake: pondra serve {dir}/<name> {flag} …)");
                }
                tls::init()?; // (a certificate that can't be read: said now)
                let auth = Arc::new(auth::Auth::new(env::get("PONDRA_READ_TOKEN"), env::get("PONDRA_WRITE_TOKEN"), env::get("PONDRA_ADMIN_TOKEN")));
                let on_network = on_network(&addr, [&pg, &kafka, &flight]);
                anyhow::ensure!(auth.on() || !on_network, "a folder of lakes served on {addr} answers anyone there: set --admin-token (each database's node takes it), or listen on 127.0.0.1");
                anyhow::ensure!(python.is_none() || auth.on() || !on_network, "--python lets whoever makes a procedure run code on this machine: set --admin-token, or listen on 127.0.0.1");
                // Each database's node gets the rest, as `pondra serve <lake>` would take them (and the settings given here, the
                // tokens too, as its environment: `dbserver.rs`).
                let node: Vec<String> = ["--flush-ms", &flush_ms.to_string(), "--task-ms", &task_ms.to_string(), "--retain-secs", &retain_secs.to_string(),
                    "--backlog", &backlog.to_string(), "--ack", &ack, "--replicas", &replicas.to_string()].map(String::from).to_vec();
                let options = dbserver::Options { python, tier_secs: Some(tier_secs), reader, node };
                return dbserver::serve(dir, addr, pg, default, options, auth).await;
            }
            let owner = env::get("PONDRA_OWNER_KEY").filter(|k| k.len() >= 16); // (16 characters at least, as a library's: `library.rs`)
            if owner.is_some() && !dir.contains("://") && std::fs::read_dir(&dir).is_ok_and(|mut d| d.next().is_none()) {
                // (a lake the shell or `local()` just made is its user's alone, as a Postgres data folder is: its catalog holds the nodes' key)
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
                }
            }
            // (on the network a node is never open: with no token, its leader makes the lake's first user before it serves, `node.rs`)
            let on_network = on_network(&addr, [&pg, &kafka, &flight]);
            let auth = auth::Auth::new(env::get("PONDRA_READ_TOKEN"), env::get("PONDRA_WRITE_TOKEN"), env::get("PONDRA_ADMIN_TOKEN"));
            let auth = Arc::new(auth.on_network(on_network).owned_by(owner)); // (nodes call each other with the admin one: cluster::http)
            tls::init()?; // (a certificate that can't be read: said now)
            anyhow::ensure!(python.is_none() || auth.on() || !on_network, "--python lets whoever makes a procedure run code on this machine: set --admin-token, or listen on 127.0.0.1");
            // Stopped (Ctrl-C, SIGTERM from a scheduler scaling down, or the program that started
            // this node ending): drained first (`drain.rs`; a second signal stops at once), then the
            // next node leads at once instead of waiting out the lease. Watched from before the node
            // starts, so a stop while it starts is seen too (invariant 46).
            // (a pinned future, not a task: `Node::stop` ends every task a node started, and a moved node
            // starts again, so the selects below poll this one alone)
            let mut stop = Box::pin(stopped(stop_with_stdin));
            // (`--idle-exit`: once nobody has used the node for that long and it has nothing left to do, it stops as on
            // SIGTERM: `idle.rs`)
            let quiet = env::secs("PONDRA_IDLE_EXIT_SECS");
            let mut moves = node::mover(); // (a lake moving to another place: `memory::set_location`)
            // (the node's own work failing, `node::stop`: the node is stopped as on SIGTERM but with no drain, as a library's
            // host stops it, so a leader gives up its term and the next node leads at once; then the process ends with 1)
            let mut failures = node::supervise();
            let mut options = node::Options { dir, listen: addr, advertise, start: node::Start { reader, flush_ms, tier_secs, task_ms, retain_secs, backlog, python, auth }, pg, kafka, kafka_advertise, flight, attach: attached, attach_found, guest: None };
            loop {
                let held = Arc::new(node::Held::default()); // (what the start takes, given back by a stop: `node.rs`)
                let mut node = tokio::select! {
                    started = node::Node::start(options.clone(), held.clone()) => match started {
                        Ok(node) => node,
                        Err(e) => {
                            held.stop().await; // (a start that fails gives back what it took too, as `Node::open` does: the next node leads at once)
                            return Err(e);
                        }
                    },
                    // (a node stopped while it starts gives back what it took so far, a leader its term, so
                    // the next node leads at once; there is no drain here, since nothing serves yet)
                    _ = &mut stop => {
                        held.stop().await;
                        crate::memory::leave();
                        std::process::exit(0);
                    }
                    Some(node::Event::Failed(_)) = failures.recv() => {
                        failed_stop(held.stop()).await;
                    }
                };
                let mut idle = Box::pin(node.idle(quiet.as_secs_f64())); // (this node's uses, counted from its start)
                tokio::select! {
                    served = node.serving() => {
                        served?;
                        break;
                    }
                    _ = async { tokio::select! { _ = &mut stop => {}, _ = &mut idle => eprintln!("nobody has used this node for {quiet:?} and it has nothing left to do: it stops (--idle-exit)") } } => {
                        tokio::select! { _ = node.drain() => {}, _ = stopped(false) => node.hurry() }
                        node.stop().await;
                        crate::memory::leave();
                        std::process::exit(0);
                    }
                    // The lake moves: its requests answered and the node stopped, its objects settled on the
                    // target, and the node started again there, on the same port. A move that fails leaves
                    // the lake in memory: the node starts again on it, on the same port, and says why.
                    Some(node::Event::Failed(_)) = failures.recv() => {
                        failed_stop(node.stop()).await;
                    }
                    Some(m) = moves.recv() => {
                        node.drain().await;
                        node.stop().await;
                        while failures.try_recv().is_ok() {} // (what the stopped node's work said is no failure of the next one's)
                        if let Err(e) = crate::memory::settle(&m).await {
                            crate::memory::move_failed(&m.to, &e);
                            continue;
                        }
                        options.dir = m.to;
                    }
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
            let token = project::token(token, url.as_deref());
            print!("{}", shell::script(lake.as_deref().unwrap_or("lake"), url.as_deref(), token.as_deref(), body).await?);
        }
        Cmd::Service { cmd } => service::command(cmd).await?,
        Cmd::Workspace { cmd } => sync::command(cmd).await?,
        Cmd::Project(cmd) => project::command(cmd).await?,
        Cmd::Catalog { dir, prefix } => {
            let lake = store::Lake::open(&dir, false, false, node::native(false)).await?;
            match prefix.starts_with("d/") {
                true => match lake.cat.get_raw(&prefix).await? {
                    Some(b) => println!("{prefix}: {} bytes", b.len()), // inline data: binary
                    None => println!("{prefix}: not in the catalog"),
                },
                false => {
                    for (k, v) in lake.cat.scan_raw(&prefix, &format!("{prefix}\u{10ffff}")).await? {
                        match serde_json::from_slice::<serde_json::Value>(&v) {
                            Ok(v) => println!("{k} {v}"),
                            Err(_) => println!("{k} ({} bytes)", v.len()), // (inline data, under every prefix's scan: binary)
                        }
                    }
                }
            }
        }
        Cmd::Lead { dir } => write::lead_only(&dir, &node::native(false)).await?,
        Cmd::Backup { lake, to, retention_days } => sql_on_guest(&lake, &backup_sql(&to, retention_days), &[]).await?,
        Cmd::Restore { from, to, at } => {
            let at = at.as_deref().map(backup::instant).transpose()?;
            println!("{}", serde_json::to_string_pretty(&backup::restore(&from, &to, at, &node::native(false)).await?)?);
        }
        Cmd::Sql { dir, query, attach: attached } if write::checkpoint(&query) => {
            // (the leader's work: a node that runs the lake is asked; with nobody leading, this process leads for the
            // moment, as a one-off write does, tiers the log, writes the catalog down, and ends, so a lake no node
            // serves can be kept tidy by a scheduled `pondra sql --dir … CHECKPOINT`)
            let dir = crate::memory::place(&dir);
            let store = store::open_store(&dir)?.1;
            match terms::latest(&store).await? {
                Some(t) if !t.addr.is_empty() && terms::alive(&store, &t).await => {
                    println!("{}", crate::net::process().client(crate::net::Policy::Cluster).post(crate::tls::url(&format!("{}/sql", t.addr))).body("CHECKPOINT").send().await?.error_for_status()?.text().await?)
                }
                _ => sql_on_guest(&dir, &query, &attached).await?,
            }
        }
        Cmd::Sql { dir, query, attach: attached } => sql_on_guest(&crate::memory::place(&dir), &query, &attached).await?, // (`:memory:`: in this process's memory, as `serve`'s)
        Cmd::Token { dir, user, name } => {
            let here = node::native(false); // (a command of its own, not a guest: as `pondra lead`)
            let Ok(lake) = store::Lake::open(&dir, false, false, here.clone()).await else { anyhow::bail!("no lake at {dir} (pondra token takes a lake's folder, or its s3:// prefix)") };
            let no_user = lake.cat.scan_raw("u/", "u0").await?.is_empty();
            drop(lake); // (what follows opens it as it needs: through the inbox, or leading for a moment)
            if user == "pondra" && no_user {
                // (a lake with no user yet, as a follower on the network finds it: its first user, as a leader makes it)
                let first = users::Change::Create { name: user.clone(), login: true, if_not_exists: true, verifier: None, superuser: true, limits: Default::default() };
                write::through_bucket(&dir, write::Request::Ddl(ddl::Ddl::Users(first)), &here).await?;
            }
            let (token, hash) = users::new_token(&user);
            let name = name.unwrap_or_else(|| format!("token-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs())));
            let change = users::Change::CreateToken { name: name.clone(), user: user.clone(), expires_secs: None, hash: Some(hash) };
            write::through_bucket(&dir, write::Request::Ddl(ddl::Ddl::Users(change)), &here).await?; // (only the hash goes)
            println!("{token}"); // (alone on stdout: `T=$(pondra token --dir lake)`)
            eprintln!("a token for {user} (shown once; DROP TOKEN {name} FOR {user} takes it back)");
        }
    }
    Ok(())
}

/// `pondra sql`, and `pondra backup` (its one statement, `backup_sql`): one statement, asked of a guest node of the lake
/// (`node::Options::guest`), which answers it as `POST /sql` does, and ends. A plain query or an export (`COPY … TO`) reads;
/// anything else may write, and leads the lake for the moment if nobody does; a write to a lake whose leader is alive but
/// doesn't answer from here goes through the bucket's inbox (`write::by_inbox`). The output is what the node gives: the
/// notices its procedures printed, then the answer.
async fn sql_on_guest(dir: &str, query: &str, attached: &[String]) -> anyhow::Result<()> {
    // `USE` sets a session's path, and this command has no session (`path.rs`).
    anyhow::ensure!(!path::is_use(query) && path::set_schema(query).is_none(), "{}", set::NO_SESSION);
    // DuckDB's INSTALL and LOAD answer a row naming what happened, as a page answers them (`extension.rs`).
    let query = match extension::row(query) { Some(row) => row?, None => query.to_string() };
    let reads = statement::single_statement(&query).is_some_and(|one| statement::plain_statement(&one)) || matches!(write::parse(&query), Some(write::Stmt::CopyTo(..)));
    let guest = if reads { node::Guest::Reads } else { node::Guest::Writes };
    let life = node::native(true).life.clone(); // (a guest's, as its node's: a run again gives up after five)
    let node = match node::Node::open(node::Options::guest(dir, attached.to_vec(), guest)).await {
        Ok(node) => node,
        // (fenced out by a newer writer while it started, before its statement ran: run again, as a write fenced later is)
        Err(e) if write::fenced(&e) && write::tries() < 5 => write::start_over("fenced while starting", write::job(), &*life),
        Err(e) => return Err(e),
    };
    // (the answer is read, and the node finished, on every path: a leader's term is given back whatever the answer)
    let job = url::form_urlencoded::byte_serialize(write::job().as_bytes()).collect::<String>();
    let answer = async {
        let res = node.ask("POST", &format!("/sql?format=table&job={job}"), query.into_bytes()).await?;
        let (ok, heard, sqlstate) = (res.status().is_success(), shell::notices(res.headers()), res.headers().get("x-pondra-sqlstate").cloned());
        let body = axum::body::to_bytes(res.into_body(), usize::MAX).await?;
        anyhow::Ok((ok, heard, sqlstate, String::from_utf8_lossy(&body).into_owned()))
    }
    .await;
    let finished = node.finish().await; // (before anything prints: a guest that led waits for what it wrote to be durable)
    let (ok, heard, sqlstate, body) = answer?;
    if !ok {
        let err = write::refusal(sqlstate.as_ref().and_then(|v| v.to_str().ok()), body);
        // (fenced out by a newer writer: run again with this job, so whoever records it records it once, as `write::deliver` does)
        if write::fenced(&err) && write::tries() < 5 {
            if let Err(e) = finished {
                eprintln!("the guest didn't end cleanly before the run again: {e:#}");
            }
            write::start_over("fenced while writing", write::job(), &*life);
        }
        return Err(err);
    }
    // (fenced out as it finished, before what it wrote was durable: run again, and the job says whether it was recorded)
    if finished.as_ref().is_err_and(|e| write::fenced(e) && write::tries() < 5) {
        write::start_over("fenced while finishing", write::job(), &*life);
    }
    finished?;
    print!("{heard}");
    println!("{body}");
    Ok(())
}

/// The statement `pondra backup` runs on the lake: `BACKUP DATABASE TO 'to' [WITH (retention_days = n)]`, its place quoted
/// the way SQL quotes a string (a `'` doubled), which `write::parse` reads back.
fn backup_sql(to: &str, retention_days: Option<u64>) -> String {
    let keep = retention_days.map(|d| format!(" WITH (retention_days = {d})")).unwrap_or_default();
    format!("BACKUP DATABASE TO '{}'{keep}", to.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_backup_names_its_place_as_sql_quotes_it() {
        assert_eq!(super::backup_sql("backups/till", None), "BACKUP DATABASE TO 'backups/till'");
        let sql = super::backup_sql("it's/here", Some(3));
        assert_eq!(sql, "BACKUP DATABASE TO 'it''s/here' WITH (retention_days = 3)");
        let Some(crate::write::Stmt::Ddl(ds)) = crate::write::parse(&sql) else { panic!("a backup statement: {sql}") };
        assert!(matches!(&ds[..], [crate::ddl::Ddl::BackupDatabase { database: None, to, retention_days: Some(3) }] if to == "it's/here"), "{sql}");
    }

    /// `pondra --help`, `pondra serve --help` and their `-h`, as `tests/fixtures/help/` holds them: a change to them is
    /// meant (copy the one it writes under target/help/ there), never a side effect of where a flag is defined.
    #[test]
    fn the_help_is_as_it_was() {
        let at = |p: &str| std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(p);
        let mut changed = Vec::new();
        for (file, args) in [("pondra.txt", &["pondra", "--help"][..]), ("pondra-h.txt", &["pondra", "-h"]), ("serve.txt", &["pondra", "serve", "--help"]), ("serve-h.txt", &["pondra", "serve", "-h"])] {
            let Err(e) = super::command().try_get_matches_from(args) else { panic!("{args:?} shows help") };
            assert_eq!(e.kind(), clap::error::ErrorKind::DisplayHelp, "{args:?}");
            let shown = e.render().to_string();
            if std::fs::read_to_string(at(&format!("tests/fixtures/help/{file}"))).ok().as_ref() != Some(&shown) {
                let _ = std::fs::create_dir_all(at("target/help"));
                std::fs::write(at(&format!("target/help/{file}")), &shown).unwrap();
                changed.push(format!("`{}` (target/help/{file})", args.join(" ")));
            }
        }
        assert!(changed.is_empty(), "{} changed: diff each with tests/fixtures/help/, and copy it there if that is meant", changed.join(", "));
    }

    /// A setting's flag is given only when it is on the command line, as it was written there; else the environment holds.
    #[test]
    fn serve_gives_the_settings_on_its_command_line() {
        let given = |args: &[&str]| {
            let m = super::command().try_get_matches_from([&["pondra", "serve"][..], args].concat()).unwrap();
            super::flags(m.subcommand_matches("serve").unwrap())
        };
        assert_eq!(given(&[]), vec![]);
        assert!(super::parse_from(["pondra", "serve", "--memory-gb", "4", "--tls-cert", "c.pem"].map(String::from)).is_ok(), "as a service's options are checked");
        assert_eq!(given(&["--publish", "delta", "--fsync", "--memory-gb", "1.5", "--admin-token", ""]),
            [("PONDRA_ADMIN_TOKEN", ""), ("PONDRA_FSYNC", "true"), ("PONDRA_MEMORY_GB", "1.5"), ("PONDRA_PUBLISH", "delta")].map(|(v, x)| (v, x.to_string())).to_vec());
        for wrong in [&["--memory-gb", "-1"][..], &["--cache-gb", "lots"], &["--changelog-secs", "inf"]] {
            assert!(super::command().try_get_matches_from([&["pondra", "serve"][..], wrong].concat()).is_err(), "{wrong:?}");
        }
    }

    /// Each setting's flag comes after a flag `serve` has, and is a node's.
    #[test]
    fn each_setting_flag_has_its_place() {
        let mut cmd = super::command();
        let serve = cmd.find_subcommand_mut("serve").unwrap();
        let longs: Vec<String> = serve.get_arguments().filter_map(|a| a.get_long()).map(String::from).collect();
        for s in crate::settings::all().iter().filter(|s| s.flag.is_some()) {
            assert!(longs.contains(&s.flag.unwrap().after.to_string()), "{} comes after --{}, which serve hasn't", s.var, s.flag.unwrap().after);
            assert_eq!(s.who, crate::settings::Who::Node, "{}", s.var);
        }
    }

    #[test]
    fn a_command_mistyped() {
        for (word, c) in [("serv", "serve"), ("sevre", "serve"), ("sorve", "serve"), ("aply", "apply"), ("stauts", "status")] {
            assert!(super::mistyped(word, c), "{word} for {c}");
        }
        for (word, c) in [("serve", "serve"), ("test2", "test"), ("tests", "test"), ("sales", "apply"), ("sq", "sql"), ("lake", "plan")] {
            assert!(!super::mistyped(word, c), "{word} isn't {c}");
        }
    }
}
