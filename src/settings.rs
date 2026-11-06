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
    /// manager): the environment only.
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
    /// Seconds (fractions too), and the others: the unit is the name's ending.
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
    /// The process's: what it was given, else its environment.
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
}

pub const fn of(var: &'static str, who: Who, kind: Kind, unset: Unset) -> Setting {
    Setting { var, who, kind, unset, scope: Scope::Node, change: Change::Start }
}
pub const fn node(var: &'static str, kind: Kind, unset: Unset) -> Setting { of(var, Who::Node, kind, unset) }
pub const fn command(var: &'static str, kind: Kind, unset: Unset) -> Setting { of(var, Who::Command, kind, unset) }
pub const fn starter(var: &'static str, kind: Kind, unset: Unset) -> Setting { of(var, Who::Starter, kind, unset) }
pub const fn test(var: &'static str, kind: Kind, unset: Unset) -> Setting { of(var, Who::Test, kind, unset) }

// The defaults worked out when read.
fn blocking_threads() -> String { // (tokio's own on Windows, where a child's pipes are read on blocking threads: `panics::runtime`)
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
    node("PONDRA_ADMIN_TOKEN", Secret, No),
    node("PONDRA_ADMIN_TOKEN_PREVIOUS", Secret, No),
    node("PONDRA_ALLOW_UNSIGNED_EXTENSIONS", Bool, No),
    node("PONDRA_AUDIT", List, Is("role,ddl,function")),
    node("PONDRA_AUDIT_DAYS", Days, Is("90")),
    node("PONDRA_BLOCKING_THREADS", Int, Of(blocking_threads)),
    node("PONDRA_BROADCAST_MB", Mb, Is("64")),
    node("PONDRA_BUCKET_REQUESTS", Int, Is("256")),
    node("PONDRA_CACHE_DIR", Path, No),
    node("PONDRA_CACHE_GB", Gb, No),
    node("PONDRA_CACHE_MB", Mb, Is("1024")),
    node("PONDRA_CHANGELOG_SECS", Secs, Is("0")),
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
    node("PONDRA_DRAIN_GRACE_SECS", Secs, Is("0")),
    node("PONDRA_DRAIN_SECS", Secs, Is("30")),
    node("PONDRA_EXTENSION_FILE_MB", Mb, Is("64")),
    node("PONDRA_EXTENSION_MB", Mb, Is("256")),
    node("PONDRA_EXTENSION_SECS", Secs, Is("30")),
    node("PONDRA_FILES_CACHE_MB", Mb, Is("512")),
    node("PONDRA_FILE_VERSIONS", Int, Is("50")),
    node("PONDRA_FILE_VERSIONS_DAYS", Days, Is("90")),
    node("PONDRA_FSYNC", Bool, No),
    node("PONDRA_FUNCTION_CACHE_MB", Mb, Is("256")),
    node("PONDRA_GC_SECS", Secs, No),
    node("PONDRA_HISTORY", Text, No),
    node("PONDRA_HISTORY_DAYS", Days, Is("7")),
    node("PONDRA_HISTORY_RATE", Int, Is("500")),
    node("PONDRA_HOT_GB", Gb, No),
    node("PONDRA_INLINE_KB", Kb, Of(inline_kb)),
    node("PONDRA_JOIN_ORDER", Int, Is("2")),
    test("PONDRA_KEEP_AT_ONCE", Int, No),
    test("PONDRA_KEEP_PARQUET", Text, No),
    node("PONDRA_KMS_COMMAND", Text, No),
    test("PONDRA_LATE_ROWS", Text, No),
    node("PONDRA_LAUNCHER", Path, No),
    node("PONDRA_LEARN", Bool, No),
    node("PONDRA_LEARN_MS", Ms, Is("100")),
    node("PONDRA_LINK", List, No),
    node("PONDRA_LOAD_RETENTION_SECS", Secs, No),
    node("PONDRA_LOCK_LEASE_SECS", Secs, Is("60")),
    node("PONDRA_MATCH_HELD_ROWS", Int, Is("1000000")),
    node("PONDRA_MATCH_RUN_ROWS", Int, Is("1000000")),
    node("PONDRA_MEMORY_GB", Gb, No),
    node("PONDRA_MEMORY_LAKE_GB", Gb, No),
    node("PONDRA_MEMORY_MAX_GB", Gb, No),
    node("PONDRA_PAGES_MB", Mb, Is("256")),
    node("PONDRA_PIN_LEASE_SECS", Secs, Is("1209600")),
    node("PONDRA_PIN_RENEW_SECS", Secs, Is("3600")),
    node("PONDRA_PROCEDURES", Int, Of(procedures)),
    node("PONDRA_PUBLISH", List, No),
    node("PONDRA_PURGE_ROWS", Int, Is("100000")),
    node("PONDRA_PYTHON", Path, No),
    node("PONDRA_PYTHON_PROBE_SECS", Secs, Is("15")),
    node("PONDRA_QUERIES", Int, Of(queries)),
    node("PONDRA_QUEUE_SECS", Secs, Is("60")),
    node("PONDRA_READ_TOKEN", Secret, No),
    node("PONDRA_REPLICAS", Int, Is("1")),
    node("PONDRA_RUNS_DAYS", Days, Is("30")),
    node("PONDRA_SECRET_KEY", Secret, No),
    node("PONDRA_SECRET_KEY_PREVIOUS", Secret, No),
    node("PONDRA_SERVE_STORE", Bool, No),
    node("PONDRA_SESSION_HOURS", Hours, No),
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
    node("PONDRA_TLS_CA", Path, No),
    node("PONDRA_TLS_CERT", Path, No),
    node("PONDRA_TLS_KEY", Path, No),
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
    node("PONDRA_WRITE_TOKEN", Secret, No),
    command("PONDRA_BROWSER", Text, No),
    command("PONDRA_ENV", Text, No),
    command("PONDRA_HOME", Path, No),
    command("PONDRA_JOB", Text, No),
    command("PONDRA_PASSWORD", Secret, No),
    command("PONDRA_TOKEN", Secret, No),
    command("PONDRA_TRIES", Int, Is("0")),
    starter("PONDRA_CLIENT_KEY", Secret, No),
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

/// The node's own settings, as an extension: first in `crate::built_in`, so every other extension's come after.
pub static SETTINGS: crate::registry::BuiltIn = crate::registry::BuiltIn {
    name: "settings",
    requires: &[],
    register: |r| r.section_mut::<Settings>().add(CORE),
};

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

    /// A default reads as its kind, so `env::value` never meets one it can't.
    #[test]
    fn defaults_read() {
        for s in all() {
            let Some(v) = s.default() else { continue };
            let ok = match s.kind {
                Int => v.parse::<u64>().is_ok(),
                Float | Secs | Ms | Hours | Days | Kb | Mb | Gb => v.parse::<f64>().is_ok_and(|n| n >= 0.0),
                Switch | Bool | Secret | Path | Paths => false, // (unset is off, or none)
                Text | List | Url => !v.is_empty(),
            };
            assert!(ok, "{}'s default {v:?} isn't a {:?}", s.var, s.kind);
            assert_eq!((s.scope, s.change), (Scope::Node, Change::Start), "{}: every node setting is fixed at its start, until step 2", s.var);
        }
    }
}
