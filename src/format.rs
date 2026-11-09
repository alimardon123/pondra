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
use std::sync::atomic::AtomicU32;
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
/// 7: a materialized view that keeps an average or a variance as its query's own state (`fvar::MEAN_PARTS`, 11-09 audit
/// M11), which a node from before has no function to merge.
pub const FORMAT: u32 = 7;

/// The format whose tables keep their columns as one list (`store::Stored`). A lake moves to it by itself: its leader raises
/// it once every node knows it (`raise`), and a form that leads a lake in memory or a folder alone as it opens (`alone`). The commit sets the
/// format alone: each table's entry becomes its list at its next write (`Catalog::write`), and an entry still in format
/// 5's shape reads as it always did, as every format-5 lake's do.
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

/// The format's record in the catalog (`records`).
pub const PREFIXES: &[crate::records::Prefix] = &[crate::records::Prefix { prefix: KEY, owner: "format", holds: "the lake's format: what a node must read to open it" }];
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

/// What this process holds of a lake's format beside its record: the lake's own (`Lake::format`), never one for the node
/// or the process, since a process holds lakes side by side (an attached one, `serve --lakes`, a backup it takes, a
/// program's two), and what one lake is says nothing of another (invariant 301).
#[derive(Default)]
pub struct Held {
    /// Did this process make the lake (open it to write with nothing ever committed there)? No release older than this
    /// build has read it.
    made: bool,
    /// What `raise` saw: 0 when it doesn't run for the lake here, 1 while it takes its first look, then the newest format
    /// every node knows, plus 2.
    ready: AtomicU32,
    /// One commit of the format at a time (`set`).
    one: tokio::sync::Mutex<()>,
    /// The lake's commit streams open to other nodes, by the format each knows (`Streamed`; read-only nodes don't
    /// heartbeat: their stream is how the leader knows they are there).
    streams: Streams,
}

/// Open commit streams by the format each knows: a count of each.
type Streams = Arc<std::sync::Mutex<std::collections::BTreeMap<u32, usize>>>;

/// Did this process make the lake? (No release older than this build has read it.)
pub fn made(lake: &Lake) -> bool { lake.format.made }

impl Held {
    /// The oldest format a node streaming the lake's commits knows (`Streamed`), if one streams them now.
    fn streamed(&self) -> Option<u32> { self.streams.lock().unwrap().keys().next().copied() }
}

/// Refuse a lake newer than this build; what this process holds of its format otherwise, made here or not.
pub async fn check(cat: &Catalog, url: &str, writer: bool) -> Result<Held> {
    let s = of(cat).await?;
    anyhow::ensure!(s.format <= known(), Newer { url: url.into(), stamp: (s.format, s.by) });
    let made = writer && cat.get::<u64>("c").await?.is_none(); // (every commit writes `c`)
    Ok(Held { made, ..Held::default() })
}

/// Commit the lake's format, one at a time and never back: a lake this process made gets the mark
/// (`raise`) while its keys may already have moved it on (`require`).
async fn set(lake: &Lake, to: u32) -> Result<()> {
    let _one = lake.format.one.lock().await;
    if of(&lake.cat).await?.format >= to {
        return Ok(());
    }
    let stamp = Stamp { format: to, by: VERSION.into() };
    lake.cat.commit(vec![(KEY.into(), serde_json::to_vec(&stamp).expect("json"))], &[]).await?;
    lake.node.life.say(&format!("the lake is format {to} now (Pondra {VERSION})"));
    Ok(())
}

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
    while lake.format.ready.load(Relaxed) == 1 && started.elapsed() < Duration::from_secs(15) {
        tokio::time::sleep(Duration::from_millis(100)).await; // (the first look: `raise`)
    }
    let ready = match lake.format.ready.load(Relaxed) {
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

/// Held by a commit stream of a lake for as long as it is open: the node at its other end knows `format` (`Held::streams`).
pub struct Streamed(Streams, u32);

impl Streamed {
    pub fn to(lake: &Lake, format: u32) -> Streamed {
        let streams = lake.format.streams.clone();
        *streams.lock().unwrap().entry(format).or_default() += 1;
        Streamed(streams, format)
    }
}

impl Drop for Streamed {
    fn drop(&mut self) {
        let mut s = self.0.lock().unwrap();
        if let Some(n) = s.get_mut(&self.1) {
            *n -= 1;
            if *n == 0 {
                s.remove(&self.1);
            }
        }
    }
}

/// Leader: keep what every node knows for `require`, and give a lake without a format the mark
/// (`BASE`) once every node knows it. It first waits out two leases, so that every follower alive
/// has said what it knows, then looks every 10 s. A lake this process made has the mark at once,
/// and every node knows this build's format until one says otherwise: no node older than it has
/// read it. A migration, when a format needs one, goes in the commit that sets it.
pub fn raise(lake: Arc<Lake>, members: Arc<dyn Members>) {
    use std::sync::atomic::Ordering::Relaxed;
    crate::panics::spawn(async move {
        let mut marked = false;
        if lake.format.made {
            lake.format.ready.store(known() + 2, Relaxed);
            marked = set(&lake, first()).await.map_err(|e| eprintln!("the lake's format: {e:#}")).is_ok();
        } else {
            lake.format.ready.store(1, Relaxed); // (looking: `require` waits for it)
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
        loop {
            let streamed = lake.format.streamed();
            let ready = [Some(known()), members.least_known(), streamed].into_iter().flatten().min().unwrap_or_default();
            lake.format.ready.store(ready + 2, Relaxed);
            if !marked && ready >= BASE {
                let done = match of(&lake.cat).await {
                    Ok(s) if s.format >= BASE => Ok(()),
                    Ok(_) => set(&lake, BASE).await,
                    Err(e) => Err(e),
                };
                marked = done.map_err(|e| eprintln!("the lake's format: {e:#}")).is_ok();
            }
            if ready >= LISTS {
                if let Err(e) = set(&lake, LISTS).await {
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

/// The format a lake this build makes starts at: its tables' column lists (`LISTS`) at once, since no release older than
/// this build has read it (the mark alone, `BASE`, for a test posing as a build from before them).
fn first() -> u32 { if known() >= LISTS { LISTS } else { BASE } }

/// A form that leads its lake alone, with no leader's loop (`pondra sql`, a program's node): a lake in memory, or one this
/// process made in a local folder, moves to this build's tables' column lists (`LISTS`) as it opens, in one small commit.
/// No release before this build has read either. A folder's lake made before doesn't, nor a bucket's: a node of a release
/// before may serve it again (a folder's after a restart, a bucket's cluster at once: its followers, which this form can't
/// hear in a leader gap), and it would stop (11-07 audit L2: one `pondra sql` raised a folder for good, so a 0.80 server
/// was refused it). Such a lake moves on only by its server's leader, which knows its nodes' releases (`raise`), or by
/// `require`, for what needs the lists; until then its entries keep format 5's shape, which every reader reads.
pub async fn alone(lake: &Lake) -> Result<()> {
    if known() >= LISTS && moves_alone(&lake.url, lake.format.made) {
        set(lake, LISTS).await?;
    }
    Ok(())
}

/// Does a form alone move the lake at `url` on (`alone`): one in this process's memory, or in a local folder when this
/// process made it (`made`), never one in a bucket?
fn moves_alone(url: &str, made: bool) -> bool {
    match crate::store::place(url) {
        Ok(crate::store::Place::Memory) => true,
        Ok(crate::store::Place::Folder) => made,
        _ => false,
    }
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
                    return lake.node.life.stop(format!("{e}"));
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{json, table_key, TableMeta};

    /// A form alone raises a lake in memory, and one it made in a folder; never a folder's made before, nor a bucket's
    /// (`alone`).
    #[test]
    fn a_form_alone_raises_only_a_lake_no_release_before_has_read() {
        assert!(moves_alone("memory://7", false) && moves_alone("/home/me/lake", true) && moves_alone("C:\\lakes\\one", true));
        assert!(!moves_alone("/home/me/lake", false) && !moves_alone("C:\\lakes\\one", false));
        for bucket in ["s3://bucket/lake", "gs://b/l", "az://c/l", "abfss://c@a.dfs.core.windows.net/l"] {
            assert!(!moves_alone(bucket, true) && !moves_alone(bucket, false), "{bucket}");
        }
    }

    /// What this process holds of a lake's format is the lake's own (invariant 301): a lake made here says so, and a
    /// folder's lake made before, opened beside it, doesn't, so a form alone moves the first on and leaves the second as a
    /// release before left it (11-07 audit L2). Under one static for the process, the second was taken for one made here.
    #[tokio::test]
    async fn a_lake_made_here_says_so_and_one_made_before_beside_it_doesnt() -> Result<()> {
        let dir = |n: &str| std::env::temp_dir().join(format!("pondra-made-{n}-{}", uuid::Uuid::new_v4()));
        let (before, here) = (dir("before"), dir("here"));
        let (b, h) = (before.to_str().expect("a path"), here.to_str().expect("a path"));
        let old = Lake::open(b, true, false, crate::node_state::NodeState::unhosted()).await?;
        old.cat.commit(vec![(KEY.into(), serde_json::to_vec(&Stamp { format: 5, by: "0.80.0".into() })?)], &[]).await?; // (as 0.80 left it)
        old.cat.close().await?;
        drop(old);
        let new = Lake::open(h, true, false, crate::node_state::NodeState::unhosted()).await?;
        let old = Lake::open(b, true, false, crate::node_state::NodeState::unhosted()).await?;
        assert!(made(&new) && !made(&old));
        alone(&new).await?;
        alone(&old).await?;
        assert_eq!((of(&new.cat).await?.format, of(&old.cat).await?.format), (LISTS, 5), "the lake made here moves on, the other stays");
        for (lake, d) in [(new, here), (old, before)] {
            lake.cat.close().await?;
            let _ = std::fs::remove_dir_all(d);
        }
        Ok(())
    }

    /// A lake's commit streams count for its own format alone (invariant 301): a node of format 5 streaming one lake holds
    /// back that lake's raise (`raise`), not a lake beside it, and it counts no more once its stream ends. Under one static
    /// for the node, a follower of either lake held back both.
    #[tokio::test]
    async fn a_lakes_commit_streams_are_its_own() -> Result<()> {
        let dir = |n: &str| std::env::temp_dir().join(format!("pondra-streams-{n}-{}", uuid::Uuid::new_v4()));
        let (one, two) = (dir("one"), dir("two"));
        let a = Lake::open(one.to_str().expect("a path"), true, false, crate::node_state::NodeState::unhosted()).await?;
        let b = Lake::open(two.to_str().expect("a path"), true, false, crate::node_state::NodeState::unhosted()).await?;
        let (old, newer) = (Streamed::to(&a, 5), Streamed::to(&a, 7));
        assert_eq!((a.format.streamed(), b.format.streamed()), (Some(5), None), "a's streams are a's alone");
        drop(old);
        assert_eq!(a.format.streamed(), Some(7), "an ended stream counts no more");
        drop(newer);
        assert_eq!(a.format.streamed(), None);
        for (lake, d) in [(a, one), (b, two)] {
            lake.cat.close().await?;
            let _ = std::fs::remove_dir_all(d);
        }
        Ok(())
    }

    /// Format 6 is one small commit: a format-5 lake's entries stay in their shape and answer as before, and each becomes
    /// its list at its next write (`Catalog::write`), as does an entry made in format 5's shape after it.
    #[tokio::test]
    async fn format_6_turns_each_entry_into_its_list_at_its_next_write() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-lists-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().expect("a path"), true, false, crate::node_state::NodeState::unhosted()).await?;
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
        // (one entry read as its list, from another lake: below format 6 it is written as format 5's fields too)
        let mut puts: Vec<_> = (0..300).map(|i| (table_key(&format!("t{i}")), json(&entry(i)))).collect();
        puts[299].1 = json(&entry(299).as_list());
        lake.cat.commit(puts, &[]).await?;
        let shapes = |raw: &std::collections::BTreeMap<String, bytes::Bytes>| raw.values().filter(|v| v.starts_with(br#"{"list":"#)).count();
        let at5 = lake.cat.scan_raw("t/", "t0").await?;
        assert!(at5.len() == 300 && at5.values().all(|v| v.starts_with(br#"{"columns":"#)), "below format 6, each as 0.80 wrote it");
        let mut t3: TableMeta = serde_json::from_slice(&at5[&table_key("t3")])?;
        t3.set_default("c5", Some("'y'".into())); // (a write of an entry read in format 5's shape, below format 6)
        lake.cat.commit(vec![(table_key("t3"), json(&t3))], &[]).await?;
        assert!(lake.cat.get_raw(&table_key("t3")).await?.is_some_and(|v| v.starts_with(br#"{"columns":"#)), "and written in it again");
        set(&lake, LISTS).await?;
        assert_eq!(of(&lake.cat).await?.format, LISTS);
        let raised = lake.cat.scan_raw("t/", "t0").await?;
        assert_eq!(shapes(&raised), 0, "the raise sets the format alone");
        let again = |k: &str| -> Result<TableMeta> { Ok(serde_json::from_slice(&raised[k])?) };
        let mut t7 = again(&table_key("t7"))?;
        t7.set_not_null("c4", true); // (a write of an entry read in format 5's shape, after the raise)
        lake.cat.commit(vec![(table_key("t7"), json(&t7)), (table_key("made"), json(&entry(0)))], &[]).await?;
        let after = lake.cat.scan_raw("t/", "t0").await?;
        assert_eq!(shapes(&after), 2, "the entry written, and the one made: each its list");
        assert!(after[&table_key("t7")].starts_with(br#"{"list":"#) && after[&table_key("made")].starts_with(br#"{"list":"#));
        for (k, v) in &after {
            let (t, was): (TableMeta, TableMeta) = match k.as_str() {
                "t/t7" => (serde_json::from_slice(v)?, t7.clone()),
                "t/t3" => (serde_json::from_slice(v)?, t3.clone()),
                _ => (serde_json::from_slice(v)?, entry(k[3..].parse().unwrap_or(0))),
            };
            assert_eq!((t.sql_columns(), t.column_list(), t.logical().list()), (was.sql_columns(), was.column_list(), was.logical().list()), "{k}: as before");
        }
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
