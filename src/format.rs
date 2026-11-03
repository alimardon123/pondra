//! The lake's format (ADR-039): what a binary must understand to read and write a lake.
//!
//! A lake's format is a number in its catalog (`format`: the number and the release that set it).
//! A lake without one is format 0: every lake made before ADR-039, which this build opens as it is.
//! A binary knows the formats up to its `FORMAT` and opens nothing newer (`check`): a lake a newer
//! Pondra wrote is refused, by name, before anything is read or written there, so an older binary
//! started by mistake (a rollback, a machine left behind) never reads one wrongly.
//!
//! A lake moves past format 1 only when something written needs it (`require`), as a Delta table's
//! protocol moves only for a feature it uses: a lake this build opened but used nothing new in
//! still opens in the release before, so going back after an upgrade works for every lake that
//! didn't use what came with it. Even then the format moves only to what every node knows (`raise`
//! keeps it): each node says its release and format on every call to another (`headers`), and the
//! leader keeps what its followers' heartbeats and commit streams said. In a rolling upgrade
//! nothing that needs the new format is written until the last older node has gone; asked for
//! before that, it is refused by name.
use crate::members::Members;
use crate::store::{Catalog, Lake};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// The newest format this build reads and writes. 2: a materialized view that finishes its answers
/// as it is read (`TableMeta::finish`, ADR-055) or runs its query again (`View::rerun`, ADR-056, 057).
/// 3: the lake's own keys sealed by its master key (`users::Kept`, ADR-058).
/// 4: a grant on another database (`users::On::Database`, `GRANT CLONE ON DATABASE`), which a node
/// from before can't read: it took the lake for one nobody signs in to.
/// 5: an identity provider (`sso::Provider`, ADR-074), which a node from before ignores: it takes the
/// provider's tokens for nobody's and, with no other user, the lake for one nobody signs in to.
/// 6: a table's columns kept as one list, each column with its facts (`store::Stored`, redesign 1b,
/// piece 6b), which a node from before can't read (`LISTS`).
pub const FORMAT: u32 = 6;

/// The format whose tables keep their columns as one list (`store::Stored`). The leader raises a lake to it by itself once
/// every node knows it (`raise`), in one commit that rewrites every table's entry so (`lists`): ADR-039's migration in the
/// commit that sets its format. From then on an entry is written so wherever it was made (`Catalog::write`).
pub const LISTS: u32 = 6;

/// The format every lake gets without asking: the mark itself (`raise`).
const BASE: u32 = 1;

/// `FORMAT`, unless a test says otherwise (`PONDRA_TEST_FORMAT`: a build of a later or an earlier format). The
/// process's, not a node's: the build is the process's, and a test pretends to be another build.
pub fn known() -> u32 {
    static KNOWN: std::sync::LazyLock<u32> = std::sync::LazyLock::new(|| crate::env::or("PONDRA_TEST_FORMAT", FORMAT));
    *KNOWN
}
/// This build's release.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// The catalog key that holds it.
const KEY: &str = "format";
/// What a node says of itself on every call to another: its release, and the newest format it knows.
pub fn headers() -> [(&'static str, String); 2] { [("x-pondra-version", VERSION.into()), ("x-pondra-format", known().to_string())] }

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Stamp {
    pub format: u32,
    #[serde(default)]
    pub by: String, // the release that set it
}

/// The lake's format (0: made before there was one).
pub async fn of(cat: &Catalog) -> Result<Stamp> { Ok(cat.get::<Stamp>(KEY).await?.unwrap_or_default()) }

/// A lake written by a newer Pondra than this one.
#[derive(Debug)]
pub struct Newer {
    pub url: String,
    pub stamp: (u32, String),
}

impl std::fmt::Display for Newer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (n, by) = &self.stamp;
        write!(f, "{} is a lake of format {n}, written by Pondra {by}; this is Pondra {VERSION}, which knows formats up to {}: run Pondra {by} or newer here", self.url, known())
    }
}

impl std::error::Error for Newer {}

crate::per_node!(
    /// Did this process make its lake (open it to write with nothing ever committed there)?
    static MADE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false));

/// Did this process make its lake? (No release older than this build has read it.)
pub fn made() -> bool { MADE.load(std::sync::atomic::Ordering::Relaxed) }

/// Refuse a lake newer than this build; its format otherwise.
pub async fn check(cat: &Catalog, url: &str, writer: bool) -> Result<u32> {
    let s = of(cat).await?;
    anyhow::ensure!(s.format <= known(), Newer { url: url.into(), stamp: (s.format, s.by) });
    if writer && cat.get::<u64>("c").await?.is_none() {
        MADE.store(true, std::sync::atomic::Ordering::Relaxed); // (every commit writes `c`)
    }
    Ok(s.format)
}

/// Commit the lake's format, one at a time and never back: a lake this process made gets the mark
/// (`raise`) while its keys may already have moved it on (`require`).
async fn set(lake: &Lake, to: u32) -> Result<()> {
    static ONE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _one = ONE.lock().await;
    let before = of(&lake.cat).await?.format;
    if before >= to {
        return Ok(());
    }
    anyhow::ensure!(before >= LISTS || to < LISTS, "format {to} comes after the tables' column lists (format {LISTS}), which the lake's leader writes first, once every node knows it");
    let stamp = Stamp { format: to, by: VERSION.into() };
    lake.cat.commit(vec![(KEY.into(), serde_json::to_vec(&stamp).expect("json"))], &[]).await?;
    crate::node::say(format!("the lake is format {to} now (Pondra {VERSION})"));
    Ok(())
}

crate::per_node!(
    /// What `raise` saw: 0 when it doesn't run in this process, 1 while it takes its first look, then
    /// the newest format every node knows, plus 2.
    static READY: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0));

/// Leader: before writing something an older release would read wrongly (`what`), move the lake to
/// format `n`, or refuse by name while a node that doesn't know `n` runs. A leader that only just
/// took over looks first (`raise`, 10 s at most); a process that leads without `raise` (a `pondra
/// sql` leading for a moment: nobody else is there) moves it as far as this build knows.
pub async fn require(lake: &Lake, n: u32, what: &str) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    if of(&lake.cat).await?.format >= n {
        return Ok(());
    }
    let started = datafusion::common::instant::Instant::now();
    while READY.load(Relaxed) == 1 && started.elapsed() < Duration::from_secs(15) {
        tokio::time::sleep(Duration::from_millis(100)).await; // (the first look: `raise`)
    }
    let ready = match READY.load(Relaxed) {
        0 => known(),
        1 => 0, // (no look in 15 s: refused, never guessed)
        r => r - 2,
    };
    anyhow::ensure!(
        n <= ready,
        "{what} needs the lake at format {n}, and a node of this cluster runs a Pondra that knows only format {ready}: once every node runs Pondra {VERSION} or newer, ask again (the lake stays as it is, so the release before still opens it)"
    );
    set(lake, n).await
}

/// The newest format a node knows, from what it said (`headers`; a node that says nothing is from
/// before formats: 0).
pub fn said(headers: &http::HeaderMap) -> (String, u32) {
    let get = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
    (get("x-pondra-version").unwrap_or_default(), get("x-pondra-format").and_then(|f| f.parse().ok()).unwrap_or(0))
}

crate::per_node!(
    /// Commit streams open to other nodes, by the format each knows (read-only nodes don't
    /// heartbeat: their stream is how the leader knows they are there).
    static STREAMS: std::sync::Mutex<std::collections::BTreeMap<u32, usize>> = std::sync::Mutex::new(std::collections::BTreeMap::new()));

/// Held by a commit stream for as long as it is open.
pub struct Streamed(u32);

impl Streamed {
    pub fn to(format: u32) -> Streamed {
        *STREAMS.lock().unwrap().entry(format).or_default() += 1;
        Streamed(format)
    }
}

impl Drop for Streamed {
    fn drop(&mut self) {
        let mut s = STREAMS.lock().unwrap();
        if let Some(n) = s.get_mut(&self.0) {
            *n -= 1;
            if *n == 0 {
                s.remove(&self.0);
            }
        }
    }
}

/// Leader: keep what every node knows for `require`, and give a lake without a format the mark
/// (`BASE`) once every node knows it. It first waits out two leases, so that every follower alive
/// has said what it knows, then looks every 10 s. A lake this process made has the mark at once,
/// and every node knows this build's format until one says otherwise: no node older than it has
/// read it. A migration, when a format needs one, goes in the commit that sets it.
pub fn raise(lake: Arc<Lake>, members: Arc<dyn Members>, lock: Arc<tokio::sync::Mutex<()>>) {
    use std::sync::atomic::Ordering::Relaxed;
    crate::panics::spawn(async move {
        let mut marked = false;
        if MADE.load(Relaxed) {
            READY.store(known() + 2, Relaxed);
            marked = set(&lake, BASE).await.map_err(|e| eprintln!("the lake's format: {e:#}")).is_ok();
        } else {
            READY.store(1, Relaxed); // (looking: `require` waits for it)
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        loop {
            let streamed = STREAMS.lock().unwrap().keys().next().copied();
            let ready = [Some(known()), members.least_known(), streamed].into_iter().flatten().min().unwrap_or_default();
            READY.store(ready + 2, Relaxed);
            if !marked && ready >= BASE {
                let done = match of(&lake.cat).await {
                    Ok(s) if s.format >= BASE => Ok(()),
                    Ok(_) => set(&lake, BASE).await,
                    Err(e) => Err(e),
                };
                marked = done.map_err(|e| eprintln!("the lake's format: {e:#}")).is_ok();
            }
            if ready >= LISTS {
                if let Err(e) = lists(&lake, &lock).await {
                    eprintln!("the tables' column lists (format {LISTS}): {e:#}");
                }
            }
            if known() > FORMAT {
                // (a build posing as a later one, `PONDRA_TEST_FORMAT`: it writes what needs it)
                let _ = require(&lake, known(), "this test build").await;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}

/// Leader: format 6 (`LISTS`), in one commit with every table's entry rewritten as its column list. Under the lake's lock,
/// which every writer of an entry takes (`App::lock`), so no change to one is lost between this read and the commit.
/// Returns how many entries it rewrote and their bytes.
pub async fn lists(lake: &Lake, lock: &tokio::sync::Mutex<()>) -> Result<(usize, usize)> {
    if of(&lake.cat).await?.format >= LISTS {
        return Ok((0, 0)); // (the leader's every look after: no lock taken)
    }
    let _guard = lock.lock().await;
    if of(&lake.cat).await?.format >= LISTS {
        return Ok((0, 0));
    }
    let t0 = std::time::Instant::now();
    let mut puts: Vec<(String, Vec<u8>)> = vec![];
    for (k, t) in lake.cat.scan::<crate::store::TableMeta>("t/", "t0").await? {
        if !t.listed() {
            puts.push((k, crate::store::json(&t.shaped(true))));
        }
    }
    let (n, bytes) = (puts.len(), puts.iter().map(|(_, v)| v.len()).sum::<usize>());
    puts.push((KEY.into(), serde_json::to_vec(&Stamp { format: LISTS, by: VERSION.into() })?));
    lake.cat.commit(puts, &[]).await?;
    crate::node::say(format!("the lake is format {LISTS} now (Pondra {VERSION}): {n} tables keep their columns as one list ({bytes} bytes, {:.1} s)", t0.elapsed().as_secs_f64()));
    Ok((n, bytes))
}

/// A node that isn't the leader: stop if the lake moves past what this build knows. (It can't,
/// unless this node was cut off while the others agreed, `raise`, or it started on such a lake
/// before its catalog view held the format: it holds it once caught up, invariant 197.)
pub fn watch(lake: Arc<Lake>) {
    crate::panics::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Err(e) = check(&lake.cat, &lake.url, false).await {
                if e.downcast_ref::<Newer>().is_some() {
                    return crate::node::stop(format!("{e}"));
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{json, table_key, TableMeta};

    /// A lake of format 5 with `tables` tables in format 5's shape, raised to format 6 (`lists`): how many entries the
    /// raise rewrote, their bytes and its seconds. Every entry is then its list, answering as before, and an entry made
    /// in format 5's shape after it is written as its list too (`Catalog::write`).
    async fn raised(tables: usize) -> Result<(usize, usize, f64)> {
        let dir = std::env::temp_dir().join(format!("pondra-lists-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().expect("a path"), true, false).await?;
        lake.cat.commit(vec![(KEY.into(), serde_json::to_vec(&Stamp { format: 5, by: "0.80.0".into() })?)], &[]).await?;
        let entry = |i: usize| {
            let mut t = TableMeta::with_columns((0..8).map(|c| (format!("c{c}"), if c % 2 == 0 { "Int64" } else { "Utf8" }.to_string())).collect());
            t.rename_column("c1", "renamed");
            t.drop_column("c2");
            t.set_not_null("c0", true);
            t.set_default("c3", Some(format!("'{i}'")));
            t.set_order(vec!["c7".into()]);
            t
        };
        for from in (0..tables).step_by(1000) {
            lake.cat.commit((from..tables.min(from + 1000)).map(|i| (table_key(&format!("t{i}")), json(&entry(i)))).collect(), &[]).await?;
        }
        let at5 = lake.cat.scan_raw("t/", "t0").await?;
        assert!(at5.len() == tables && at5.values().all(|v| v.starts_with(br#"{"columns":"#)), "below format 6, each as 0.80 wrote it");
        let t0 = std::time::Instant::now();
        let (n, bytes) = lists(&lake, &tokio::sync::Mutex::new(())).await?;
        let secs = t0.elapsed().as_secs_f64();
        assert_eq!((n, of(&lake.cat).await?.format), (tables, LISTS));
        for (k, v) in lake.cat.scan_raw("t/", "t0").await? {
            assert!(v.starts_with(br#"{"list":"#), "{k}: its list");
            let (t, was): (TableMeta, TableMeta) = (serde_json::from_slice(&v)?, entry(k[3..].parse().expect("a number")));
            assert_eq!((t.sql_columns(), t.column_list(), json(&t.logical())), (was.sql_columns(), was.column_list(), json(&was.logical())), "{k}: as before");
        }
        assert_eq!(lists(&lake, &tokio::sync::Mutex::new(())).await?, (0, 0), "once");
        lake.cat.commit(vec![(table_key("made"), json(&entry(0)))], &[]).await?;
        assert!(lake.cat.scan_raw("t/made", "t/made\u{1}").await?.values().all(|v| v.starts_with(br#"{"list":"#)), "a new entry: its list");
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok((n, bytes, secs))
    }

    /// Format 6's raise rewrites every table's entry as its list, in the commit that sets it.
    #[tokio::test]
    async fn format_6_keeps_every_tables_columns_as_one_list() -> Result<()> { raised(300).await.map(|_| ()) }

    /// The raise's commit on 10,000 tables: its size and time (`cargo test --release -- --ignored --nocapture`).
    #[tokio::test]
    #[ignore]
    async fn the_raise_of_10000_tables() -> Result<()> {
        let (n, bytes, secs) = raised(10_000).await?;
        println!("format 6's raise: {n} entries rewritten, {bytes} bytes in one commit, {secs:.2} s");
        Ok(())
    }
}
