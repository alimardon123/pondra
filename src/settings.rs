//! Every setting, one entry each (redesign §3, step 1c): its name, what kind of value it takes, what it is when unset,
//! where it holds (a node, a session, a lake) and when it may change. `env.rs` reads a node's through this table, and
//! `tools/per_node_check.py` fails a `PONDRA_*` name read that isn't in a table. The tables are the registry's settings section
//! (`Settings`): the node's own are `CORE` below, added by the `settings` extension, and an extension adds its own beside its
//! code (`ai::SETTINGS`), so a build lists only the settings of what it has.
//!
//! A default is here only where its readers take it from here (`env::value`, `env::secs`, `env::text`): a reader that does
//! more with an unset value (a 0 that means the default, a value only it can work out) keeps its own until it moves here.

/// Who a name is for, which says where it may come from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Who {
    /// A node's setting: an option of `open`, a flag of the command line, or the environment.
    Node,
    /// The command line's own (`pondra sql`, the project commands): the environment only.
    Command,
    /// Set by the program that starts a node (the shell, `connect()`, a folder of lakes, a service
    /// manager): its environment, or the line a Pondra process hands the node it starts (`Line`).
    Starter,
    /// A test's hook into the node: the environment only, and left out of the docs.
    Test,
}

/// What a value is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    /// On when set to anything (the debug switches, the trace switches, a test's hooks).
    Switch,
    /// On for `1`, `true`, `on` or `yes` (`env::flag`).
    Bool,
    Int,
    Float,
    /// Seconds, read through `env::secs`, and the others: the unit is the name's ending. Seconds, hours and GB take
    /// fractions; milliseconds, days, KB and MB are whole, as their readers read them (`Setting::reads`).
    Secs,
    Ms,
    Hours,
    Days,
    Kb,
    Mb,
    Gb,
    Text,
    /// Several, separated by commas.
    List,
    Path,
    /// Several paths, separated as the system separates them (`:`, `;` on Windows).
    Paths,
    Url,
    /// A token, a password or a key: never shown.
    Secret,
}

/// Where a value holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Scope {
    /// The process's: what its first open was given, else its environment, for what a process has one of (its memory
    /// root and watch, its runtime): every node in the process reads the same value.
    Process,
    /// A node's: what it was given, else its process's, read once as it starts (`Values`).
    Node,
    /// A session's: `SET`.
    Session,
    /// The lake's, kept in its catalog so every member agrees.
    Lake,
}

/// When a value may change.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Change {
    /// Fixed by the process's first open (`env::give`). Every node setting, until a node's state is off the statics
    /// (redesign step 2).
    Start,
    /// By an admin while it runs.
    Admin,
    /// By the session itself.
    Own,
}

/// What a setting is when it isn't set.
#[derive(Clone, Copy, Debug)]
pub enum Unset {
    /// Nothing here: unset, or its readers' own default.
    No,
    /// This value.
    Is(&'static str),
    /// One worked out when read (from the cores, or another setting).
    Of(fn() -> String),
}

/// One setting.
#[derive(Clone, Copy, Debug)]
pub struct Setting {
    /// Its name in the environment: `PONDRA_MEMORY_GB`, which is `memory_gb` as an option of `open` and `--memory-gb` as a
    /// flag.
    pub var: &'static str,
    pub who: Who,
    pub kind: Kind,
    pub unset: Unset,
    pub scope: Scope,
    pub change: Change,
    /// `pondra serve` takes it as a flag too (`--memory-gb`).
    pub flag: Option<Flag>,
}

/// A node setting's flag of `pondra serve`, named as the setting is (`--memory-gb` for `memory_gb`). One given on the command line
/// is given to the node (`env::give`) and wins; else the environment holds, else the default, as for any setting.
#[derive(Clone, Copy, Debug)]
pub struct Flag {
    /// What it does, one paragraph (`pondra serve --help`, and the reference page), or empty for one that the flag before it
    /// says (`--write-token` after `--read-token`).
    pub help: &'static str,
    /// The flag it comes after in `pondra serve --help`, by its long name.
    pub after: &'static str,
    /// The default the help shows, where it shows one.
    pub shows: Option<&'static str>,
}

impl Setting {
    /// Its short name: `memory_gb`.
    pub fn name(&self) -> String {
        self.var["PONDRA_".len()..].to_ascii_lowercase()
    }

    /// What it is when unset, as text, or None.
    pub fn default(&self) -> Option<String> {
        match self.unset {
            Unset::No => None,
            Unset::Is(v) => Some(v.to_string()),
            Unset::Of(f) => Some(f()),
        }
    }

    /// Does `v` read as a value of its kind, as its readers read it: a number for a number, 0 or more (seconds, hours and GB
    /// take fractions, the other units are whole)?
    pub fn reads(&self, v: &str) -> bool {
        match self.kind {
            Kind::Int | Kind::Ms | Kind::Days | Kind::Kb | Kind::Mb => v.trim().parse::<u64>().is_ok(),
            Kind::Float | Kind::Secs | Kind::Hours | Kind::Gb => v.trim().parse::<f64>().is_ok_and(|n| n >= 0.0 && n.is_finite()),
            _ => true,
        }
    }

    /// The same setting, which `pondra serve` also takes as a flag, shown after the flag `after`.
    pub const fn flag(self, after: &'static str, help: &'static str) -> Setting {
        Setting { flag: Some(Flag { help, after, shows: None }), ..self }
    }

    /// The same setting, read by what the process has one of (`Scope::Process`): every node in it takes the same value.
    pub const fn process(self) -> Setting { Setting { scope: Scope::Process, ..self } }

    /// Its flag's help shows this default.
    pub const fn shows(self, default: &'static str) -> Setting {
        match self.flag {
            Some(f) => Setting { flag: Some(Flag { shows: Some(default), ..f }), ..self },
            None => panic!("only a flag shows a default"),
        }
    }
}

pub const fn of(var: &'static str, who: Who, kind: Kind, unset: Unset) -> Setting {
    Setting { var, who, kind, unset, scope: Scope::Node, change: Change::Start, flag: None }
}
pub const fn node(var: &'static str, kind: Kind, unset: Unset) -> Setting { of(var, Who::Node, kind, unset) }
pub const fn command(var: &'static str, kind: Kind, unset: Unset) -> Setting { of(var, Who::Command, kind, unset) }
pub const fn starter(var: &'static str, kind: Kind, unset: Unset) -> Setting { of(var, Who::Starter, kind, unset) }
pub const fn test(var: &'static str, kind: Kind, unset: Unset) -> Setting { of(var, Who::Test, kind, unset) }

// The defaults worked out when read.
fn blocking_threads() -> String { // (tokio's own on Windows, where a child's pipes are read on blocking threads: `spawns::runtime`)
    if cfg!(windows) { 512 } else { 16.max(4 * crate::env::cores()) }.to_string()
}
fn cores() -> String { crate::env::cores().to_string() }
fn inline_kb() -> String { // (a megabyte where commits are replicated: the followers' copies, `log.rs`)
    if crate::env::value::<usize>("PONDRA_REPLICAS") > 1 { "1024" } else { "64" }.to_string()
}
fn procedures() -> String { (4 * crate::env::cores()).to_string() } // (`python.rs`)
fn queries() -> String { (4 * crate::env::cores()).max(16).to_string() } // (`app.rs`)
fn view_runs() -> String { (crate::env::cores() / 2).max(2).to_string() } // (`views.rs`)

use Kind::*;
use Unset::{Is, No, Of};

/// The node's own settings, which the `settings` extension adds (an extension's own are beside its code).
pub const CORE: &[Setting] = &[
    node("PONDRA_ADMIN_TOKEN", Secret, No).flag("write-token", ""),
    node("PONDRA_ADMIN_TOKEN_PREVIOUS", Secret, No),
    node("PONDRA_ALLOW_UNSIGNED_EXTENSIONS", Bool, No),
    node("PONDRA_AUDIT", List, Is("role,ddl,function")),
    node("PONDRA_AUDIT_DAYS", Days, Is("90")),
    node("PONDRA_BLOCKING_THREADS", Int, Of(blocking_threads)).process(), // (the runtime's: `spawns::runtime`)
    node("PONDRA_BROADCAST_MB", Mb, Is("64")),
    node("PONDRA_BUCKET_REQUESTS", Int, Is("256")),
    node("PONDRA_CACHE_DIR", Path, No).flag("backlog", "Lakes on object storage: where this node keeps its local SSD copy of recent \
        objects (default: <temp dir>/pondra-cache-<uid>, the user's own)."),
    node("PONDRA_CACHE_GB", Gb, No).flag("cache-dir", "Size of that SSD tier in GB (0 turns it off; default: 20, or a quarter of \
        the free disk if that is less)."),
    node("PONDRA_CACHE_MB", Mb, Is("1024")),
    node("PONDRA_CHANGELOG_SECS", Secs, Is("0")).flag("retain-secs", "Keep the log this many seconds as a change feed: \
        `/watch/{t}?after=N` replays every change since N (upserts and deletes of keyed tables included). 0: only `--retain-secs`.").shows("0"),
    node("PONDRA_CODEC", Text, No),
    node("PONDRA_CONFIG_DIR", Path, No),
    node("PONDRA_CONSOLE_DIR", Path, No),
    node("PONDRA_CONSOLE_EXTENSIONS", Paths, No),
    node("PONDRA_CORES", Int, Of(cores)),
    node("PONDRA_CURSORS_MB", Mb, No),
    node("PONDRA_DATABASE_IDLE_SECS", Secs, Is("600")),
    node("PONDRA_IDLE_EXIT_SECS", Secs, Is("0")),
    node("PONDRA_DEBUG_FAIL_STEP", Switch, No),
    node("PONDRA_DEBUG_FAIL_STEP_AFTER", Switch, No),
    node("PONDRA_DEBUG_JOIN_ORDER", Switch, No),
    node("PONDRA_DEBUG_OPEN_TWICE", Switch, No),
    node("PONDRA_DEBUG_PG", Switch, No),
    node("PONDRA_DEBUG_SPREAD", Switch, No),
    node("PONDRA_DEFAULT_FILE_FORMAT", Text, Is("parquet")).flag("default-table-format", "The file format a new table's files are \
        written in when its CREATE TABLE names none (also PONDRA_DEFAULT_FILE_FORMAT; default: parquet). A session's \
        `SET default_file_format` comes first.").shows("parquet"),
    node("PONDRA_DEFAULT_TABLE_FORMAT", Text, Is("pondra")).flag("publish", "The table format a new table is made in when its \
        CREATE TABLE names none (`USING`): `pondra` (also PONDRA_DEFAULT_TABLE_FORMAT; default: pondra; `iceberg` comes with this \
        lake's own Iceberg tables). A session's `SET default_table_format` comes first. A table keeps the format it was made in.").shows("pondra"),
    node("PONDRA_DRAIN_GRACE_SECS", Secs, Is("0")),
    node("PONDRA_DRAIN_SECS", Secs, Is("30")),
    node("PONDRA_EXTENSION_FILE_MB", Mb, Is("64")),
    node("PONDRA_EXTENSION_MB", Mb, Is("256")),
    node("PONDRA_EXTENSION_SECS", Secs, Is("30")),
    node("PONDRA_FILES_CACHE_MB", Mb, Is("512")),
    node("PONDRA_FILE_VERSIONS", Int, Is("50")),
    node("PONDRA_FILE_VERSIONS_DAYS", Days, Is("90")),
    node("PONDRA_FSYNC", Bool, No).flag("replicas", "With `--ack replicated`: followers flush each copy to disk before acknowledging \
        it, so an acked write survives a power loss of the follower too (costs a disk flush per change)."),
    node("PONDRA_FUNCTION_CACHE_MB", Mb, Is("256")),
    node("PONDRA_GC_SECS", Secs, No),
    node("PONDRA_HISTORY", Text, No),
    node("PONDRA_HISTORY_DAYS", Days, Is("7")),
    node("PONDRA_HISTORY_RATE", Int, Is("500")),
    node("PONDRA_HOT_GB", Gb, No),
    node("PONDRA_INLINE_KB", Kb, Of(inline_kb)),
    node("PONDRA_JOIN_ORDER", Int, Is("2")),
    node("PONDRA_KAFKA_GROUP_DAYS", Days, Is("7")),
    test("PONDRA_KEEP_AT_ONCE", Int, No),
    test("PONDRA_KEEP_PARQUET", Text, No),
    node("PONDRA_KMS_COMMAND", Text, No),
    test("PONDRA_LATE_ROWS", Text, No),
    node("PONDRA_LAUNCHER", Path, No),
    node("PONDRA_LEARN", Bool, No),
    node("PONDRA_LEARN_MS", Ms, Is("100")),
    node("PONDRA_LINK", List, No),
    node("PONDRA_LOAD_RETENTION_SECS", Secs, Is("5529600")), // (64 days, as Snowflake keeps its load metadata)
    node("PONDRA_LOCK_LEASE_SECS", Secs, Is("60")),
    node("PONDRA_MATCH_HELD_ROWS", Int, Is("1000000")),
    node("PONDRA_MATCH_RUN_ROWS", Int, Is("1000000")),
    node("PONDRA_MEMORY_GB", Gb, No).flag("task-ms", "Memory for queries and every cache together, in GB (also \
        PONDRA_MEMORY_GB; default: 72% of the machine's, the caches giving way to queries), except a cache given its own budget (PONDRA_HOT_GB, \
        PONDRA_CACHE_MB, PONDRA_PAGES_MB, PONDRA_FILES_CACHE_MB, PONDRA_FUNCTION_CACHE_MB). A query that needs more takes it from the caches, then sorts, joins and aggregations spill."),
    node("PONDRA_MEMORY_LAKE_GB", Gb, No),
    node("PONDRA_MEMORY_MAX_GB", Gb, No).process(), // (the process's line: its memory root and its watch, `ledger::Root`, `oom`)
    node("PONDRA_PAGES_MB", Mb, Is("256")),
    node("PONDRA_PIN_LEASE_SECS", Secs, Is("1209600")),
    node("PONDRA_PIN_RENEW_SECS", Secs, Is("3600")),
    node("PONDRA_PROCEDURES", Int, Of(procedures)),
    node("PONDRA_PUBLISH", List, No).flag("fsync", "Open formats new tables are also published in, for engines that don't know Pondra: \
        `delta`, `iceberg` or `delta,iceberg` (default: none; Pondra and `pondra sql` read the lake natively, fresher). Per table: \
        `\"publish\"` when creating it.").shows(""),
    node("PONDRA_PURGE_ROWS", Int, Is("100000")),
    node("PONDRA_PYTHON", Path, No),
    node("PONDRA_PYTHON_PROBE_SECS", Secs, Is("15")),
    node("PONDRA_QUERIES", Int, Of(queries)),
    node("PONDRA_QUEUE_SECS", Secs, Is("60")),
    node("PONDRA_READ_TOKEN", Secret, No).flag("flight", "Access tokens (also PONDRA_READ_TOKEN, PONDRA_WRITE_TOKEN, \
        PONDRA_ADMIN_TOKEN): reading needs any, writing rows write or admin, tables/views/tasks admin. Give every node the same \
        ones; none set = no checks."),
    node("PONDRA_REPLICAS", Int, Is("1")),
    node("PONDRA_RUNS_DAYS", Days, Is("30")),
    node("PONDRA_SECRET_KEY", Secret, No),
    node("PONDRA_SECRET_KEY_PREVIOUS", Secret, No),
    node("PONDRA_SERVE_STORE", Bool, No).flag("reader", "Serve this lake's store to nodes on other machines, which open it as \
        pondra://host:port. Needs an admin token, the same on every node; this node leads the lake. A folder or :memory: only."),
    node("PONDRA_SESSION_HOURS", Hours, Is("12")),
    node("PONDRA_SESSION_IDLE_SECS", Secs, Is("3600")),
    node("PONDRA_SHARED_KEY_FILE", Path, No),
    node("PONDRA_SHARE_FILES_PER_MINUTE", Int, Is("10000")),
    node("PONDRA_SHARE_URL_SECS", Secs, Is("900")),
    node("PONDRA_SHARING_URL", Url, No),
    node("PONDRA_SKEW_MB", Mb, Is("64")),
    node("PONDRA_SLOW_MS", Ms, Is("1000")),
    node("PONDRA_SPILL_DIR", Path, No),
    node("PONDRA_SPILL_FAN_IN", Int, No),
    node("PONDRA_SPILL_GB", Gb, No),
    node("PONDRA_SPILL_MB", Mb, Is("64")),
    node("PONDRA_SPREAD_MB", Mb, Is("256")),
    node("PONDRA_SQL_OPTIONS", List, No),
    node("PONDRA_STAGED_SECS", Secs, Is("86400")),
    node("PONDRA_STATEMENT_TIMEOUT", Text, No),
    node("PONDRA_STREAM_MB", Mb, Is("8")),
    node("PONDRA_TEMP_MB", Mb, Is("1024")),
    node("PONDRA_TLS", Text, No),
    node("PONDRA_TLS_CA", Path, No).flag("tls-key", "The authority that signs the nodes' certificates (also PONDRA_TLS_CA): nodes \
        show theirs to each other (mutual TLS), and the nodes' key is taken only with one."),
    node("PONDRA_TLS_CERT", Path, No).flag("admin-token", "TLS on every door (also PONDRA_TLS_CERT, PONDRA_TLS_KEY): a PEM certificate \
        and its key. Plain connections are then taken only from this machine (PONDRA_TLS=optional: from anywhere), and nodes call \
        each other over HTTPS."),
    node("PONDRA_TLS_KEY", Path, No).flag("tls-cert", ""),
    node("PONDRA_TRACE_COMMIT", Switch, No),
    node("PONDRA_TRACE_PG", Switch, No),
    node("PONDRA_TRACE_QUERY", Switch, No),
    node("PONDRA_TRACE_RERUN", Switch, No),
    node("PONDRA_TRACE_START", Switch, No),
    node("PONDRA_TRUSTED_PROXIES", List, No),
    node("PONDRA_TXN_IDLE_SECS", Secs, Is("600")),
    node("PONDRA_USER_QUERIES", Int, No),
    node("PONDRA_USER_TIMEOUT", Secs, No),
    node("PONDRA_VIEW_RUNS", Int, Of(view_runs)),
    node("PONDRA_VIEW_RUN_SECS", Secs, Is("3600")),
    node("PONDRA_WEB_ORIGINS", List, No),
    node("PONDRA_WEB_SEND_MB", Mb, Is("64")),
    node("PONDRA_WORKER_IDLE_SECS", Secs, Is("60")),
    node("PONDRA_WORKER_MB", Mb, Is("2048")),
    node("PONDRA_WORKER_START_SECS", Secs, Is("60")),
    node("PONDRA_WRITE_TOKEN", Secret, No).flag("read-token", ""),
    command("PONDRA_BROWSER", Text, No),
    command("PONDRA_ENV", Text, No),
    command("PONDRA_HOME", Path, No),
    command("PONDRA_JOB", Text, No),
    command("PONDRA_PASSWORD", Secret, No),
    command("PONDRA_TOKEN", Secret, No),
    command("PONDRA_TRIES", Int, Is("0")),
    starter("PONDRA_NODE_KEY", Secret, No),
    starter("PONDRA_OWNER_KEY", Secret, No),
    starter("PONDRA_REACH", Secret, No),
    starter("PONDRA_SERVER_URL", Url, No),
    starter("PONDRA_SUPERVISED", Switch, No),
    test("PONDRA_CRASH", Text, No),
    test("PONDRA_DROP_BEATS", Path, No),
    test("PONDRA_NO_DIRECT", Switch, No),
    test("PONDRA_TEST_AUTOCOMMIT_PAUSE_MS", Ms, No),
    test("PONDRA_TEST_FILL_PAUSE_MS", Ms, No),
    test("PONDRA_TEST_FORMAT", Int, No),
    test("PONDRA_TEST_NO_GATE", Switch, No),
    test("PONDRA_TEST_CHECKS", Switch, No),
    test("PONDRA_TEST_PANICS", Bool, No),
];

/// The registry's settings section: every setting this build's extensions add, each name once.
#[derive(Default)]
pub struct Settings {
    all: Vec<Setting>,
    by_var: std::collections::HashMap<&'static str, usize>,
}

impl crate::registry::Section for Settings {
    const NAME: &'static str = "settings";
}

impl Settings {
    /// An extension's settings; a name another already added is refused when the registry is built.
    pub fn add(&mut self, settings: &[Setting]) {
        for s in settings {
            assert!(self.by_var.insert(s.var, self.all.len()).is_none(), "two settings are named {}", s.var);
            self.all.push(*s);
        }
    }
}

/// The node's own settings, as an extension: first in `crate::built_in`, so every other extension's come after. It lists
/// them all as `pondra.settings`.
pub static SETTINGS: crate::registry::BuiltIn = crate::registry::BuiltIn {
    name: "settings",
    requires: &[],
    register: |r| {
        r.section_mut::<Settings>().add(CORE);
        r.section_mut::<crate::lists::Lists>().add(crate::lists::List { name: "settings", columns: COLUMNS, rows });
    },
};

/// `pondra.settings`' columns: each name in the environment and as an option, what it takes, what it is when unset here
/// (worked out on this machine, where it is), where it holds, when it may change and who it is for.
const COLUMNS: &[(&str, crate::lists::Cell)] = {
    use crate::lists::Cell::Text;
    &[("name", Text), ("variable", Text), ("type", Text), ("default_value", Text), ("scope", Text), ("change", Text), ("who", Text)]
};

/// One row per setting, a test's hooks left out as the docs leave them out. Each word is its variant's name in lower case.
fn rows(r: &crate::registry::Registry) -> Vec<serde_json::Value> {
    let word = |d: &dyn std::fmt::Debug| format!("{d:?}").to_lowercase();
    r.section::<Settings>().all.iter().filter(|s| s.who != Who::Test).map(|s| serde_json::json!({
        "name": s.name(), "variable": s.var, "type": word(&s.kind), "default_value": s.default(), "scope": word(&s.scope), "change": word(&s.change), "who": word(&s.who),
    })).collect()
}

/// A node's settings (`Scope::Node`), read once as it starts (`Values::now`), so one set in the process's environment
/// later never reaches a node that started without it. A reader with the node at hand reads its own here (`node.settings`);
/// `env.rs` reads the process's, for a reader with none.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Values(std::collections::BTreeMap<&'static str, String>);

impl std::fmt::Debug for Values {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.debug_tuple("Values").field(&Shown(&self.0)).finish() }
}

/// Settings as `Debug` shows them: a secret's value never (`Kind::Secret`: the admin's and the read and write tokens, the
/// secret and owner keys), nor that of a name this build doesn't know, whatever prints them (11-09 audit L6).
struct Shown<'a>(&'a std::collections::BTreeMap<&'static str, String>);

impl std::fmt::Debug for Shown<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let secret = |var: &str| find(var).is_none_or(|s| s.kind == Kind::Secret);
        f.debug_map().entries(self.0.iter().map(|(var, v)| (var, if secret(var) { "(secret)" } else { v.as_str() }))).finish()
    }
}

impl Values {
    /// The process's now: what it was given, else its environment (`env::get`), for every node setting of this build that a
    /// node reads (not the command line's own).
    pub fn now() -> Values {
        let read = |s: &&'static Setting| s.scope == Scope::Node && s.who != Who::Command;
        Values(all().iter().filter(read).filter_map(|s| crate::env::get(s.var).map(|v| (s.var, v))).collect())
    }

    /// A setting of the node's, or None when it isn't set (an empty value is none).
    pub fn get(&self, var: &str) -> Option<&str> {
        debug_assert!(find(var).is_some_and(|s| s.scope == Scope::Node), "{var} isn't a node's setting in settings.rs");
        self.0.get(var).map(String::as_str)
    }

    /// A switch: on for `1`, `true`, `on` or `yes`, as `env::flag` reads one.
    pub fn flag(&self, var: &str) -> bool { self.get(var).is_some_and(|v| ["1", "true", "on", "yes"].contains(&v.trim().to_ascii_lowercase().as_str())) }

    /// Is it set at all? For the debug switches and test hooks, which any value turns on (`env::is_set`).
    pub fn is_set(&self, var: &str) -> bool { self.get(var).is_some() }

    /// A setting read as a `T`, or None when it isn't set or doesn't read as one (`env::parse`).
    pub fn parse<T: std::str::FromStr>(&self, var: &str) -> Option<T> { self.get(var)?.trim().parse().ok() }

    /// A setting in GB (fractions too) as bytes of 2^30, as every memory setting is read (`env::gib`).
    pub fn gib(&self, var: &str) -> Option<usize> { self.parse::<f64>(var).map(crate::env::gb_bytes) }

    /// With a setting of the node's own that its host gives it (a lakes server's address, for its database's node).
    pub fn with(mut self, var: &'static str, value: impl Into<String>) -> Values {
        debug_assert!(find(var).is_some_and(|s| s.scope == Scope::Node), "{var} isn't a node's setting in settings.rs");
        self.0.insert(var, value.into());
        self
    }

    /// Without a setting of the process's that isn't the node's (`--idle-exit`, for a node its host stops once idle).
    pub fn without(mut self, var: &str) -> Values {
        self.0.remove(var);
        self
    }

    /// These values, for a test's node.
    #[cfg(test)]
    pub(crate) fn of(values: &[(&'static str, &str)]) -> Values { Values(values.iter().map(|(k, v)| (*k, v.to_string())).collect()) }
}

/// What a Pondra process hands one it starts as its settings (`pondra serve --settings-stdin`, `pondra lead`): one JSON
/// object of names and values, the first line of the child's standard input, taken before anything in it reads a setting
/// (`env::hand`). It holds every setting but the command line's: the node's `Values`, and the process's own
/// (`Scope::Process`), which the child, a process of its own, takes as its first open. The child's environment holds none of
/// these names (`hand`), so a setting the line leaves out reaches it no other way.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Line(std::collections::BTreeMap<&'static str, String>);

impl std::fmt::Debug for Line {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.debug_tuple("Line").field(&Shown(&self.0)).finish() }
}

impl Line {
    /// A node's values and its process's own settings now.
    pub fn of(values: &Values) -> Line {
        let process = all().iter().filter(|s| s.scope == Scope::Process).filter_map(|s| crate::env::get(s.var).map(|v| (s.var, v)));
        Line(values.0.clone().into_iter().chain(process).collect())
    }

    /// With a setting of the child's own: the shell's key for its node, the places `pondra lead` reaches.
    pub fn with(mut self, var: &'static str, value: impl Into<String>) -> Line {
        debug_assert!(find(var).is_some_and(|s| s.who != Who::Command), "{var} isn't a setting a line carries");
        self.0.insert(var, value.into());
        self
    }

    /// Readies a process about to start (its `serve` or `lead` arguments given) to take this line, and gives the line to
    /// write to its standard input once it has started: `--settings-stdin`, its input a pipe, and none of a line's names in
    /// its environment (`env_clear` would take PATH and the rest with them).
    pub fn hand(&self, cmd: &mut std::process::Command) -> String {
        cmd.arg("--settings-stdin").stdin(std::process::Stdio::piped());
        for s in all().iter().filter(|s| s.who != Who::Command) {
            cmd.env_remove(s.var);
        }
        serde_json::to_string(&self.0).expect("names and text") + "\n"
    }

    /// A line as its child reads it, by full names: refused, naming why, unless it is one object of text values whose
    /// names are all a line's.
    pub fn read(text: &str) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        let values: std::collections::BTreeMap<String, String> = serde_json::from_str(text.trim_end())
            .map_err(|e| anyhow::anyhow!("--settings-stdin: the first line of standard input isn't one JSON object of names and text values ({e})"))?;
        for name in values.keys() {
            match find(name).map(|s| s.who) {
                Some(Who::Command) => anyhow::bail!("--settings-stdin: {name} is the command line's, not handed to a node"),
                None => anyhow::bail!("--settings-stdin: {name} isn't a setting of this build"),
                Some(_) => {}
            }
        }
        Ok(values)
    }
}

/// Every setting of this build, in the order its extensions added them.
pub fn all() -> &'static [Setting] {
    &crate::registry::get().section::<Settings>().all
}

/// The setting of a name in the environment (`PONDRA_MEMORY_GB`), or None for one nothing in this build reads.
pub fn find(var: &str) -> Option<&'static Setting> {
    let s = crate::registry::get().section::<Settings>();
    s.by_var.get(var).map(|&i| &s.all[i])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node's settings never show a secret's value, whatever prints them (11-09 audit L6).
    #[test]
    fn settings_show_no_secret() {
        let held = |pairs: &[(&'static str, &str)]| pairs.iter().map(|(k, v)| (*k, v.to_string())).collect::<std::collections::BTreeMap<_, _>>();
        let m = held(&[("PONDRA_ADMIN_TOKEN", "tok-1234"), ("PONDRA_SECRET_KEY", "key-5678"), ("PONDRA_MEMORY_GB", "4")]);
        for shown in [format!("{:?}", Values(m.clone())), format!("{:?}", Line(m))] {
            assert!(!shown.contains("tok-1234") && !shown.contains("key-5678"), "{shown}");
            assert!(shown.contains("PONDRA_ADMIN_TOKEN") && shown.contains("\"4\""), "the names, and what isn't secret: {shown}");
        }
    }

    /// A node's values are its node settings only: never the command line's, nor one the process has one of (its
    /// memory line, its runtime's threads), which every node in a process shares.
    #[test]
    fn a_nodes_values_are_its_node_settings_alone() {
        for var in Values::now().0.keys() {
            let s = find(var).expect("a value of the table's");
            assert!(s.scope == Scope::Node && s.who != Who::Command, "{var} isn't a node's to read");
        }
        for var in ["PONDRA_MEMORY_MAX_GB", "PONDRA_BLOCKING_THREADS"] {
            assert_eq!(find(var).map(|s| s.scope), Some(Scope::Process), "{var} is the process's");
        }
    }

    /// A node reads the values it started with, not its process's: two nodes in one process can differ.
    #[test]
    fn a_node_reads_the_values_it_started_with() {
        let on = crate::node_state::NodeState::starting(crate::lifecycle::unhosted(), Values::of(&[("PONDRA_SERVE_STORE", "yes")]));
        let off = crate::node_state::NodeState::starting(crate::lifecycle::unhosted(), Values::of(&[("PONDRA_SERVE_STORE", "0")]));
        assert!(on.settings.flag("PONDRA_SERVE_STORE") && !off.settings.flag("PONDRA_SERVE_STORE"));
        assert!(off.settings.is_set("PONDRA_SERVE_STORE") && !Values::default().is_set("PONDRA_SERVE_STORE"));
    }

    /// A line holds a node's values and a setting of the child's own, reads back as one object by full names, and
    /// leaves the child's environment none of a line's names.
    #[test]
    fn a_line_is_the_childs_settings_alone() {
        let line = Line::of(&Values::of(&[("PONDRA_MEMORY_GB", "2")])).with("PONDRA_OWNER_KEY", "a-key-of-sixteen-chars");
        let mut cmd = std::process::Command::new("pondra");
        cmd.args(["serve", "--lake", "l"]).env("PONDRA_MEMORY_GB", "9").env("PONDRA_SPILL_GB", "1").env("PONDRA_JOB", "j").env("PATH", "/bin");
        let text = line.hand(&mut cmd);
        assert!(text.ends_with('\n') && text.trim_end().lines().count() == 1, "one line: {text}");
        let read = Line::read(&text).unwrap();
        assert_eq!(read.get("PONDRA_MEMORY_GB").map(String::as_str), Some("2"));
        assert_eq!(read.get("PONDRA_OWNER_KEY").map(String::as_str), Some("a-key-of-sixteen-chars"));
        assert!(read.keys().all(|k| find(k).is_some_and(|s| s.who != Who::Command)));
        assert_eq!(cmd.get_args().last().and_then(|a| a.to_str()), Some("--settings-stdin"));
        let envs: std::collections::BTreeMap<_, _> = cmd.get_envs().map(|(k, v)| (k.to_string_lossy().into_owned(), v.map(|v| v.to_string_lossy().into_owned()))).collect();
        assert_eq!(envs["PONDRA_MEMORY_GB"], None, "a line's names leave the child's environment");
        assert_eq!(envs["PONDRA_SPILL_GB"], None, "even one the line leaves out");
        assert_eq!((envs["PONDRA_JOB"].as_deref(), envs["PATH"].as_deref()), (Some("j"), Some("/bin")), "the rest stay");
    }

    /// A child refuses a line that isn't one, naming why: not an object, a value that isn't text, the command line's
    /// setting, a name nothing reads.
    #[test]
    fn a_line_refuses_what_isnt_a_childs_setting() {
        let why = |text: &str| Line::read(text).expect_err(text).to_string();
        assert!(why("not json\n").contains("isn't one JSON object"));
        assert!(why("{\"PONDRA_MEMORY_GB\": 2}\n").contains("text values"));
        assert!(why("{\"PONDRA_JOB\": \"1\"}\n").contains("PONDRA_JOB is the command line's"));
        assert!(why("{\"PONDRA_NOTHING_AT_ALL\": \"1\"}\n").contains("PONDRA_NOTHING_AT_ALL isn't a setting"));
        assert_eq!(Line::read("{}\n").unwrap().len(), 0);
    }

    #[test]
    fn each_once() {
        let mut seen = std::collections::HashSet::new();
        assert!(all().iter().all(|s| s.var.starts_with("PONDRA_") && seen.insert(s.var)), "each name once");
    }

    /// The section holds the node's own settings first, then each extension's, and finds every one by its name.
    #[test]
    fn a_builds_settings_are_its_extensions() {
        assert_eq!(all()[..CORE.len()].iter().map(|s| s.var).collect::<Vec<_>>(), CORE.iter().map(|s| s.var).collect::<Vec<_>>());
        assert!(all().iter().all(|s| find(s.var).is_some_and(|f| f.var == s.var)));
        assert!(find("PONDRA_NOTHING_AT_ALL").is_none());
    }

    #[test]
    #[should_panic(expected = "two settings are named PONDRA_MEMORY_GB")]
    fn a_name_is_one_settings() {
        let mut s = Settings::default();
        s.add(CORE);
        s.add(&[node("PONDRA_MEMORY_GB", Gb, No)]);
    }

    #[test]
    fn a_value_reads_as_its_kind() {
        let (gb, n, path) = (find("PONDRA_MEMORY_GB").unwrap(), find("PONDRA_REPLICAS").unwrap(), find("PONDRA_CACHE_DIR").unwrap());
        assert!(gb.reads("1.5") && gb.reads(" 0 ") && !gb.reads("-1") && !gb.reads("inf") && !gb.reads("NaN") && !gb.reads("lots"));
        assert!(n.reads("3") && !n.reads("1.5") && !n.reads("-1"));
        assert!(path.reads("-1") && path.reads(""));
        let (secs, mb) = (find("PONDRA_TXN_IDLE_SECS").unwrap(), find("PONDRA_CACHE_MB").unwrap());
        assert!(secs.reads("0.5") && mb.reads("512") && !mb.reads("0.5"), "seconds take fractions, MB are whole, as their readers read them");
    }

    /// A default reads as its kind, as its readers read it (`reads`), so `env::value` and `env::secs` never meet one they
    /// can't (11-06 audit L10: a fraction for a whole kind).
    #[test]
    fn defaults_read() {
        for s in all() {
            let Some(v) = s.default() else { continue };
            let ok = match s.kind {
                Int | Float | Secs | Ms | Hours | Days | Kb | Mb | Gb => s.reads(&v) && v == v.trim(),
                Switch | Bool | Secret | Path | Paths => false, // (unset is off, or none)
                Text | List | Url => !v.is_empty(),
            };
            assert!(ok, "{}'s default {v:?} isn't a {:?}", s.var, s.kind);
            assert!(matches!((s.scope, s.change), (Scope::Node | Scope::Process, Change::Start)), "{}: every node setting is fixed at its start, until step 2", s.var);
        }
    }
}
