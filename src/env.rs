//! Every setting a node reads, through one function from one table (ADR-061 §8). A setting is what
//! the process was given (`open`'s options, the command line's flags), else its environment, and an
//! empty value is no value. Nothing here sets the environment: in a library the host runs threads of
//! its own (Jupyter, any Python), and `set_var` beside them is a data race. The browser has no
//! environment and reads the same table. `tools/per_node_check.py` fails a `PONDRA_*` name read
//! anywhere else, and a name read that isn't here.
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

/// Who a name is for, which says where it may come from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Who {
    /// A node's setting: an option of `open`, a flag of the command line, or the environment.
    Node,
    /// The command line's own (`pondra sql`, the project commands): the environment only.
    Command,
    /// Set by the program that starts a node (the shell, `local()`, a folder of lakes, a service
    /// manager): the environment only.
    Starter,
    /// A test's hook into the node: the environment only, and left out of the docs.
    Test,
}

use Who::*;

/// Every `PONDRA_*` name a node or the command line reads, and who it is for.
pub const NAMES: &[(&str, Who)] = &[
    ("PONDRA_ADMIN_TOKEN", Node),
    ("PONDRA_ADMIN_TOKEN_PREVIOUS", Node),
    ("PONDRA_AI_CALLS", Node),
    ("PONDRA_AI_KEY", Node),
    ("PONDRA_AI_MODEL", Node),
    ("PONDRA_AI_TIMEOUT_SECS", Node),
    ("PONDRA_AI_URL", Node),
    ("PONDRA_AUDIT", Node),
    ("PONDRA_AUDIT_DAYS", Node),
    ("PONDRA_BLOCKING_THREADS", Node),
    ("PONDRA_BROADCAST_MB", Node),
    ("PONDRA_BUCKET_REQUESTS", Node),
    ("PONDRA_CACHE_DIR", Node),
    ("PONDRA_CACHE_GB", Node),
    ("PONDRA_CACHE_MB", Node),
    ("PONDRA_CHANGELOG_SECS", Node),
    ("PONDRA_CODEC", Node),
    ("PONDRA_CONFIG_DIR", Node),
    ("PONDRA_CONSOLE_DIR", Node),
    ("PONDRA_CONSOLE_EXTENSIONS", Node),
    ("PONDRA_CORES", Node),
    ("PONDRA_CURSORS_MB", Node),
    ("PONDRA_DATABASE_IDLE_SECS", Node),
    ("PONDRA_DEBUG_JOIN_ORDER", Node),
    ("PONDRA_DEBUG_PG", Node),
    ("PONDRA_DEBUG_SPREAD", Node),
    ("PONDRA_DRAIN_GRACE_SECS", Node),
    ("PONDRA_DRAIN_SECS", Node),
    ("PONDRA_EMBED_MODEL", Node),
    ("PONDRA_FILES_CACHE_MB", Node),
    ("PONDRA_FILE_VERSIONS", Node),
    ("PONDRA_FILE_VERSIONS_DAYS", Node),
    ("PONDRA_FSYNC", Node),
    ("PONDRA_FUNCTION_CACHE_MB", Node),
    ("PONDRA_GC_SECS", Node),
    ("PONDRA_HISTORY", Node),
    ("PONDRA_HISTORY_DAYS", Node),
    ("PONDRA_HISTORY_RATE", Node),
    ("PONDRA_HOT_GB", Node),
    ("PONDRA_INLINE_KB", Node),
    ("PONDRA_JOIN_ORDER", Node),
    ("PONDRA_KMS_COMMAND", Node),
    ("PONDRA_LEARN", Node),
    ("PONDRA_LEARN_MS", Node),
    ("PONDRA_LINK", Node),
    ("PONDRA_LOAD_RETENTION_SECS", Node),
    ("PONDRA_MEMORY_GB", Node),
    ("PONDRA_PAGES_MB", Node),
    ("PONDRA_PIN_LEASE_SECS", Node),
    ("PONDRA_PIN_RENEW_SECS", Node),
    ("PONDRA_PROCEDURES", Node),
    ("PONDRA_PUBLISH", Node),
    ("PONDRA_PURGE_ROWS", Node),
    ("PONDRA_PYTHON", Node),
    ("PONDRA_PYTHON_PROBE_SECS", Node),
    ("PONDRA_QUERIES", Node),
    ("PONDRA_QUEUE_SECS", Node),
    ("PONDRA_READ_TOKEN", Node),
    ("PONDRA_REPLICAS", Node),
    ("PONDRA_RUNS_DAYS", Node),
    ("PONDRA_SECRET_KEY", Node),
    ("PONDRA_SECRET_KEY_PREVIOUS", Node),
    ("PONDRA_SESSION_HOURS", Node),
    ("PONDRA_SESSION_IDLE_SECS", Node),
    ("PONDRA_SHARE_FILES_PER_MINUTE", Node),
    ("PONDRA_SHARE_URL_SECS", Node),
    ("PONDRA_SHARING_URL", Node),
    ("PONDRA_SKEW_MB", Node),
    ("PONDRA_SLOW_MS", Node),
    ("PONDRA_SPILL_MB", Node),
    ("PONDRA_SPREAD_MB", Node),
    ("PONDRA_SQL_OPTIONS", Node),
    ("PONDRA_STATEMENT_TIMEOUT", Node),
    ("PONDRA_STREAM_MB", Node),
    ("PONDRA_TEMP_MB", Node),
    ("PONDRA_TLS", Node),
    ("PONDRA_TLS_CA", Node),
    ("PONDRA_TLS_CERT", Node),
    ("PONDRA_TLS_KEY", Node),
    ("PONDRA_TRACE_PG", Node),
    ("PONDRA_TRACE_START", Node),
    ("PONDRA_TRUSTED_PROXIES", Node),
    ("PONDRA_TXN_IDLE_SECS", Node),
    ("PONDRA_USER_QUERIES", Node),
    ("PONDRA_USER_TIMEOUT", Node),
    ("PONDRA_WORKER_IDLE_SECS", Node),
    ("PONDRA_WORKER_MB", Node),
    ("PONDRA_WORKER_START_SECS", Node),
    ("PONDRA_WRITE_TOKEN", Node),
    ("PONDRA_ENV", Command),
    ("PONDRA_HOME", Command),
    ("PONDRA_JOB", Command),
    ("PONDRA_PASSWORD", Command),
    ("PONDRA_TOKEN", Command),
    ("PONDRA_TRIES", Command),
    ("PONDRA_CLIENT_KEY", Starter),
    ("PONDRA_NODE_KEY", Starter),
    ("PONDRA_OWNER_KEY", Starter),
    ("PONDRA_REACH", Starter),
    ("PONDRA_SERVER_URL", Starter),
    ("PONDRA_SUPERVISED", Starter),
    ("PONDRA_CRASH", Test),
    ("PONDRA_DROP_BEATS", Test),
    ("PONDRA_NO_DIRECT", Test),
    ("PONDRA_TEST_FORMAT", Test),
    ("PONDRA_TEST_PANICS", Test),
];

/// What the process was given, by its full names, fixed by the first `give`.
static GIVEN: OnceLock<BTreeMap<String, String>> = OnceLock::new();

/// Who a full name is for, or None for a name nothing reads.
pub fn who(name: &str) -> Option<Who> {
    NAMES.iter().find(|(n, _)| *n == name).map(|(_, w)| *w)
}

/// Gives the process its settings: `open`'s options, or the command line's flags. The first call
/// fixes them. A later one may repeat a setting but never change one, since what the process read
/// at its start stays read; each it would change is named. A name may be written `memory_gb` or
/// `PONDRA_MEMORY_GB`; one that isn't a node's setting is refused by name, with the nearest one.
pub fn give<K: AsRef<str>, V: Into<String>>(options: impl IntoIterator<Item = (K, V)>) -> anyhow::Result<()> {
    let mut named: BTreeMap<String, (String, String)> = BTreeMap::new();
    for (key, value) in options {
        let (key, name) = (key.as_ref().to_string(), full(key.as_ref()));
        match who(&name) {
            Some(Node) => {}
            Some(Command) => anyhow::bail!("{key} is the command line's, not a node's setting"),
            Some(Starter) => anyhow::bail!("{key} is set by the program that starts a node, not given"),
            Some(Test) | None => anyhow::bail!("{key} isn't a setting{}", nearest(&key).map(|n| format!(" (did you mean {n}?)")).unwrap_or_default()),
        }
        named.insert(name, (key, value.into()));
    }
    let values = named.iter().map(|(name, (_, value))| (name.clone(), value.clone())).collect();
    if GIVEN.set(values).is_ok() {
        return Ok(());
    }
    let changed: Vec<String> = named.iter().filter(|(name, (_, value))| get(name).unwrap_or_default() != *value)
        .map(|(name, (key, _))| format!("{key} ({})", get(name).map_or("not set".into(), |v| format!("{v} now")))).collect();
    anyhow::ensure!(changed.is_empty(), "a process's settings are fixed by its first open, so {} can't change: open in a new process for others", changed.join(", "));
    Ok(())
}

/// A setting, or None: what the process was given (empty there is None too, the environment unread),
/// else the environment.
pub fn get(name: &str) -> Option<String> {
    debug_assert!(who(name).is_some(), "{name} isn't in env::NAMES");
    match GIVEN.get().and_then(|g| g.get(name)) {
        Some(v) => Some(v.clone()),
        None => std::env::var(name).ok(),
    }
    .filter(|v| !v.is_empty())
}

/// A setting that may be a path in the environment's own encoding (a folder, a list of files).
pub fn os(name: &str) -> Option<OsString> {
    debug_assert!(who(name).is_some(), "{name} isn't in env::NAMES");
    match GIVEN.get().and_then(|g| g.get(name)) {
        Some(v) => Some(OsString::from(v)),
        None => std::env::var_os(name),
    }
    .filter(|v| !v.is_empty())
}

/// A setting read as a `T`, or None when it isn't set or doesn't read as one.
pub fn parse<T: FromStr>(name: &str) -> Option<T> {
    get(name)?.trim().parse().ok()
}

/// A setting read as a `T`, else `default`.
pub fn or<T: FromStr>(name: &str, default: T) -> T {
    parse(name).unwrap_or(default)
}

/// A setting in seconds (fractions too), else `default`: one that is negative or not a number is the
/// default too.
pub fn secs(name: &str, default: f64) -> Duration {
    parse::<f64>(name).and_then(|s| Duration::try_from_secs_f64(s).ok()).unwrap_or_else(|| Duration::from_secs_f64(default))
}

/// A switch: on for `1`, `true`, `on` or `yes`.
pub fn flag(name: &str) -> bool {
    get(name).is_some_and(|v| ["1", "true", "on", "yes"].contains(&v.trim().to_ascii_lowercase().as_str()))
}

/// Is it set at all (any value but empty)? For the debug switches and test hooks, which any value
/// turns on.
pub fn is_set(name: &str) -> bool {
    get(name).is_some()
}

/// What the process was given, for the Pondra processes it starts (a folder of lakes' databases,
/// `pondra lead`, the program run again to rejoin): as their environment, they read what it reads.
pub fn given() -> impl Iterator<Item = (&'static String, &'static String)> {
    GIVEN.get().into_iter().flatten()
}

/// `memory_gb`, `memory-gb` and `PONDRA_MEMORY_GB` are one name: the last.
fn full(key: &str) -> String {
    let upper = key.trim().replace('-', "_").to_ascii_uppercase();
    match upper.starts_with("PONDRA_") {
        true => upper,
        false => format!("PONDRA_{upper}"),
    }
}

/// The node's setting nearest a name not known, written as the name was (`memory_gb` or
/// `PONDRA_MEMORY_GB`), when one is near enough to be what was meant.
fn nearest(key: &str) -> Option<String> {
    let name = full(key);
    let short = &name["PONDRA_".len()..];
    let (far, best) = NAMES.iter().filter(|(_, w)| *w == Node).map(|(n, _)| (distance(short, &n["PONDRA_".len()..]), *n)).min()?;
    (far <= (short.len() / 3).max(1)).then(|| match key.trim().to_ascii_uppercase().starts_with("PONDRA_") {
        true => best.to_string(),
        false => best["PONDRA_".len()..].to_ascii_lowercase(),
    })
}

/// Levenshtein's distance: the letters to add, take away or change to make one name the other.
fn distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, x) in a.chars().enumerate() {
        let mut last = row[0];
        row[0] = i + 1;
        for (j, y) in b.iter().enumerate() {
            let next = (row[j + 1] + 1).min(row[j] + 1).min(last + usize::from(x != *y));
            last = row[j + 1];
            row[j + 1] = next;
        }
    }
    row[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    // (one test: GIVEN is the process's, fixed by the first give, so the steps run in order)
    #[test]
    fn given_then_fixed() {
        assert!(give([("PONDRA_TOKEN", "x")]).unwrap_err().to_string().contains("the command line's"));
        assert!(give([("owner_key", "x")]).unwrap_err().to_string().contains("starts a node"));
        let wrong = give([("memry_gb", "4")]).unwrap_err().to_string();
        assert!(wrong.contains("memry_gb isn't a setting (did you mean memory_gb?)"), "{wrong}");
        // (written in lower case, so the docs' check of PONDRA_* names doesn't take it for one)
        let wrong = give([("pondra_cache_gbs", "4")]).unwrap_err().to_string();
        assert!(wrong.contains("did you mean PONDRA_CACHE_GB?"), "{wrong}");
        assert!(!give([("color", "red")]).unwrap_err().to_string().contains("did you mean"));
        assert!(GIVEN.get().is_none(), "a refused give fixes nothing");

        // Given wins over the environment, and given empty is unset (the environment unread).
        std::env::set_var("PONDRA_SKEW_MB", "9");
        std::env::set_var("PONDRA_SPILL_MB", "7");
        std::env::set_var("PONDRA_PUBLISH", "delta");
        give([("memory-gb", "4"), ("PONDRA_SKEW_MB", "12"), ("publish", "")]).unwrap();
        assert_eq!(or("PONDRA_MEMORY_GB", 0.0), 4.0);
        assert_eq!(or("PONDRA_SKEW_MB", 0u64), 12);
        assert_eq!(or("PONDRA_SPILL_MB", 0u64), 7, "what wasn't given is the environment's");
        assert_eq!(get("PONDRA_PUBLISH"), None);
        assert_eq!(given().count(), 3);

        // A second give may repeat what is in effect, never change it.
        give([("memory_gb", "4"), ("spill_mb", "7")]).unwrap();
        let changed = give([("memory_gb", "8"), ("cache_gb", "2"), ("skew_mb", "12")]).unwrap_err().to_string();
        assert!(changed.contains("cache_gb (not set), memory_gb (4 now) can't change"), "{changed}");
    }

    #[test]
    fn values() {
        std::env::set_var("PONDRA_DRAIN_GRACE_SECS", "-1");
        assert_eq!(secs("PONDRA_DRAIN_GRACE_SECS", 2.5), Duration::from_millis(2500), "a negative time is the default");
        std::env::set_var("PONDRA_DRAIN_GRACE_SECS", " 0.25 ");
        assert_eq!(secs("PONDRA_DRAIN_GRACE_SECS", 2.5), Duration::from_millis(250));
        std::env::set_var("PONDRA_LEARN", "On");
        assert!(flag("PONDRA_LEARN"));
        std::env::set_var("PONDRA_LEARN", "off");
        assert!(!flag("PONDRA_LEARN") && is_set("PONDRA_LEARN"));
        std::env::set_var("PONDRA_DEBUG_PG", "");
        assert!(!is_set("PONDRA_DEBUG_PG"), "empty is unset");
    }

    #[test]
    fn names_once() {
        let mut seen = std::collections::HashSet::new();
        assert!(NAMES.iter().all(|(n, _)| n.starts_with("PONDRA_") && seen.insert(*n)), "each name once");
    }
}
