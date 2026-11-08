//! Every setting a node reads, through one function from one table (ADR-061 §8; the table is `settings.rs`). A setting is what
//! the process was given (`open`'s options, the command line's flags), else its environment, and an
//! empty value is no value. Nothing here sets the environment: in a library the host runs threads of
//! its own (Jupyter, any Python), and `set_var` beside them is a data race. The browser has no
//! environment and reads the same table. `tools/per_node_check.py` fails a `PONDRA_*` name read
//! anywhere else, and a name read that isn't in the table.
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

pub use crate::settings::Who;
use Who::*;

/// What the process was given, by its full names, fixed by the first `give`.
static GIVEN: OnceLock<BTreeMap<String, String>> = OnceLock::new();

/// Who a full name is for, or None for a name nothing reads.
pub fn who(name: &str) -> Option<Who> {
    crate::settings::find(name).map(|s| s.who)
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
    debug_assert!(who(name).is_some(), "{name} isn't in settings.rs");
    match GIVEN.get().and_then(|g| g.get(name)) {
        Some(v) => Some(v.clone()),
        None => std::env::var(name).ok(),
    }
    .filter(|v| !v.is_empty())
}

/// A setting that may be a path in the environment's own encoding (a folder, a list of files).
pub fn os(name: &str) -> Option<OsString> {
    debug_assert!(who(name).is_some(), "{name} isn't in settings.rs");
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

/// The cores this process has, as its machine says (`Machine::cores`: its cgroup's, as the standard library reads them,
/// else 2; 1 on a page, which has one thread). Every count of threads, turns or partitions starts from it (`PONDRA_CORES`
/// is the query planner's own, `store::partitions`).
pub fn cores() -> usize { crate::machine::process().cores() }

/// A setting read as a `T`, else its default in the table (`settings.rs`): one that isn't set, or doesn't read as a `T`.
pub fn value<T: FromStr>(name: &str) -> T {
    parse(name).unwrap_or_else(|| default_as(name))
}

/// A setting's default in the table, read as a `T`.
fn default_as<T: FromStr>(name: &str) -> T {
    default(name).trim().parse().unwrap_or_else(|_| panic!("{name}'s default doesn't read as its kind"))
}

/// A setting as it was given, else its default in the table.
pub fn text(name: &str) -> String {
    get(name).unwrap_or_else(|| default(name))
}

/// A setting's default in the table.
fn default(name: &str) -> String {
    crate::settings::find(name).and_then(|s| s.default()).unwrap_or_else(|| panic!("{name} has no default in settings.rs"))
}

/// A setting read as a `T`, else `default`: for a default only its reader can work out (from what is above this, as the
/// format a lake is written in), which the table doesn't hold.
pub fn or<T: FromStr>(name: &str, default: T) -> T {
    parse(name).unwrap_or(default)
}

/// A setting in GB (fractions too) as bytes of 2^30, or None when it isn't set: the one conversion every memory setting
/// shares, so `PONDRA_MEMORY_GB=2` is the same 2 GiB wherever it is read.
pub fn gib(name: &str) -> Option<usize> {
    parse::<f64>(name).map(|gb| (gb * (1u64 << 30) as f64) as usize)
}

/// The memory line in bytes, past which the watch stops the query that grew memory: `PONDRA_MEMORY_MAX_GB`, else the form's
/// own (`own`: 85% of the machine's or container's memory on a node, its device's line on a page), and None when it is 0 (the
/// watch is off). Every form's line is this one rule; only `own` is the form's.
pub fn memory_line(own: Option<usize>) -> Option<usize> {
    line_of(gib("PONDRA_MEMORY_MAX_GB"), own)
}

/// `memory_line` of the line `set` and the form's own.
fn line_of(set: Option<usize>, own: Option<usize>) -> Option<usize> {
    set.or(own).filter(|&bytes| bytes > 0)
}

/// How much memory queries may hold in DataFusion's pool: `PONDRA_MEMORY_GB`, else the form's share (`own`: the memory
/// watch's trim line on a node, `store::memory_limit`, two thirds of its line on a page). What the pool counts is the big
/// hash tables and sort buffers, not the decoding and the batches in flight around them, so the share leaves the rest of
/// the line for those. Past it a
/// node's pool spills to disk; a page's, with no disk, refuses the query.
pub fn query_memory(own: impl FnOnce() -> usize) -> usize {
    memory_whole().unwrap_or_else(own)
}

/// The whole line for the queries and the caches together, when `PONDRA_MEMORY_GB` sets it (`ledger::Ledger`): the queries
/// hold no line of their own then, and a grow past it takes cache bytes back before it spills, as DuckDB's `memory_limit`
/// holds its buffers and its operators to one number. A cache given a budget of its own (`PONDRA_HOT_GB`, `PONDRA_CACHE_MB`)
/// keeps it beside the line (`ledger::Charged::inside`). Unset, the whole line is the memory watch's trim line, and the queries
/// may hold all of it too, the caches giving way (`store::whole_line`), but a spiller isn't refused for a reader's moment in
/// a cache.
pub fn memory_whole() -> Option<usize> {
    gib("PONDRA_MEMORY_GB")
}

/// What a refusal tells a user to raise for a query's memory: a page's query pool (`memory_gb`), else a node's (`--memory-gb`).
pub fn raise_memory() -> &'static str {
    if cfg!(target_arch = "wasm32") { "raise memory_gb" } else { "give the node more memory (--memory-gb)" }
}

/// A page's memory line when `PONDRA_MEMORY_MAX_GB` isn't set and its device doesn't say: 3 GiB, a quarter below the 4 GiB a
/// wasm32 memory ends at. The live bytes are what the program asked for, so that quarter is the room for the allocator's own
/// footprint and for a last step that grows within one poll.
pub const PAGE_LINE: usize = 3 << 30;

/// The most query memory a page holds (`query_memory`): two thirds of its line (`memory_line` of its device's, else
/// `PAGE_LINE`; of `PAGE_LINE` when the watch is off, so the pool is never empty), as a page's lake takes it
/// (`store::memory_limit`) with its device's line (`Machine::line`).
pub fn page_query_memory(device: Option<usize>) -> usize {
    query_memory(|| memory_line(Some(device.unwrap_or(PAGE_LINE))).unwrap_or(PAGE_LINE) / 3 * 2)
}

/// A setting in seconds (fractions too), else its default in the table: one that is negative or not a number is the
/// default too. Every `Secs` setting is read through this, or `secs_given` (`settings::Kind::Secs`).
pub fn secs(name: &str) -> Duration {
    secs_given(name).unwrap_or_else(|| Duration::from_secs_f64(default_as(name)))
}

/// A setting in seconds (fractions too), or None when it isn't set or isn't a time: for one with no default in the table,
/// whose reader says what unset is.
pub fn secs_given(name: &str) -> Option<Duration> {
    parse::<f64>(name).and_then(|s| Duration::try_from_secs_f64(s).ok())
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
    let (far, best) = crate::settings::all().iter().filter(|s| s.who == Node).map(|s| (distance(short, &s.var["PONDRA_".len()..]), s.var)).min()?;
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

    /// The line is the setting's, else the form's own; 0 turns the watch off.
    #[test]
    fn a_line_is_the_settings_then_the_forms() {
        assert_eq!((line_of(None, None), line_of(None, Some(1 << 30))), (None, Some(1 << 30)));
        assert_eq!((line_of(Some(2 << 30), Some(1 << 30)), line_of(Some(0), Some(1 << 30))), (Some(2 << 30), None));
        assert_eq!((line_of(Some(2 << 30), None), line_of(None, Some(0))), (Some(2 << 30), None));
    }

    #[test]
    fn values() {
        std::env::set_var("PONDRA_DRAIN_GRACE_SECS", "-1");
        assert_eq!(secs("PONDRA_DRAIN_GRACE_SECS"), Duration::ZERO, "a negative time is the default");
        std::env::set_var("PONDRA_DRAIN_GRACE_SECS", " 0.25 ");
        assert_eq!(secs("PONDRA_DRAIN_GRACE_SECS"), Duration::from_millis(250));
        std::env::set_var("PONDRA_TEMP_MB", "lots");
        assert_eq!(value::<u64>("PONDRA_STREAM_MB") + value::<u64>("PONDRA_TEMP_MB"), 8 + 1024, "unset, or not a number: the table's");
        std::env::set_var("PONDRA_AI_MODEL", " m ");
        assert_eq!((text("PONDRA_AI_MODEL"), text("PONDRA_EMBED_MODEL")), (" m ".to_string(), "nomic-embed-text".to_string()), "as given");
        std::env::set_var("PONDRA_LEARN", "On");
        assert!(flag("PONDRA_LEARN"));
        std::env::set_var("PONDRA_LEARN", "off");
        assert!(!flag("PONDRA_LEARN") && is_set("PONDRA_LEARN"));
        std::env::set_var("PONDRA_DEBUG_PG", "");
        assert!(!is_set("PONDRA_DEBUG_PG"), "empty is unset");
    }
}
