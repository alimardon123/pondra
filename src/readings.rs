//! A long query keeps the files it reads (redesign step 5, piece 13c): a kind of reader of a lake's old versions (`pins`).
//! A statement's view of the catalog lists its tables' files as they were when it began; a merge that replaces them puts
//! them in the table's garbage, and expiry deletes them once they have been there `retain_secs` (`tier::expire`). A
//! statement still reading then would find them gone. So each statement says it reads (`Reading`, from its start), and
//! expiry keeps every file let go of since the oldest one still running began.
//!
//! - A node's statements: `query::session_for` begins one per statement and puts it in the session's settings, so the
//!   session, its plans' tasks and everything planned from it hold it. The last of them dropped, it ends.
//! - A follower's: it says the oldest start of its own in its beat (`HEADER`), and the leader keeps it with the beat
//!   (`heard`) for as long as the follower's beats come (`LEASE`). A follower's view, with the changes streamed over it,
//!   can be a moment behind its leader's, and the two clocks can differ: its start counts `SLACK` earlier.
//! - A new leader needs no word from before it led: a node starts again when its leader changes (`cluster::follow`,
//!   `watch_leader`), so no statement planned against the old one still runs.
//! - A statement past a day (`CAP`) holds no longer, so a session kept by mistake can't hold the lake's files for ever:
//!   `/metrics` counts each (`readings_past_a_day_total`).
//!
//! It holds files only: a statement reads the files its tables list, never objects nothing lists, so it keeps none from
//! the orphan sweep (its newest is 0, `pins::Held`). A statement's tail of the log is read as it is planned.
use crate::log::now_ms;
use crate::store::Lake;
use anyhow::Result;
use datafusion::common::instant::Instant;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// The header a follower's beat says its oldest running statement's start in (ms, its clock). Old leaders ignore it, and
/// old followers send none: their statements hold nothing, as before.
pub const HEADER: &str = "x-pondra-reading";

/// How much earlier a follower's start counts: its view's lag behind its leader's, and the two clocks' difference.
pub const SLACK: Duration = Duration::from_secs(10);

/// How long a follower's word lasts without a beat to renew it: the cluster's lease.
const LEASE: Duration = crate::terms::LEASE;

/// A statement past this holds no longer (as a lock's `until` is capped: `store::UNTIL_CAP_MS`).
pub const CAP: Duration = Duration::from_secs(24 * 3600);

/// A lake's running statements on this node, and its followers' (on its leader).
pub struct Readings {
    /// Start (ms) -> the statements that began then and still read.
    mine: Mutex<BTreeMap<u64, usize>>,
    /// Leader: follower -> when its beat last came, and the oldest start it said.
    theirs: Mutex<BTreeMap<String, (Instant, u64)>>,
    /// The newest start counted past `CAP` so far (each is counted once).
    capped: AtomicU64,
}

impl crate::lake_state::LakeState for Readings {
    const NAME: &'static str = "readings";
    const ABOUT: &'static str = "the start of each statement running here, and of its followers' (said in their beats): the files they read stay";
    fn make() -> Self {
        Readings { mine: Default::default(), theirs: Default::default(), capped: AtomicU64::new(0) }
    }
}

/// One statement reading its lake, from its start (`Readings::begin`) until it is dropped. In a session's settings, so
/// what is planned from the session holds it (`query::session_for`).
pub struct Reading(Option<(Arc<Readings>, u64)>);

impl Reading {
    /// One that holds nothing: what a plan kept to run again holds (`fresh::kept_task`), which reads no file of its lake.
    pub fn none() -> Reading { Reading(None) }
}

impl Drop for Reading {
    fn drop(&mut self) {
        if let Some((of, at)) = self.0.take() {
            let mut mine = of.mine.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(n) = mine.get_mut(&at) {
                *n -= 1;
                if *n == 0 {
                    mine.remove(&at);
                }
            }
        }
    }
}

impl Readings {
    /// A statement begins reading the lake now.
    pub fn begin(self: &Arc<Self>) -> Reading {
        let at = now_ms();
        *self.mine.lock().unwrap_or_else(PoisonError::into_inner).entry(at).or_default() += 1;
        Reading(Some((self.clone(), at)))
    }

    /// The oldest start of this node's running statements, within `CAP`: what its beat says (None: none runs).
    pub fn oldest(&self) -> Option<u64> {
        let floor = now_ms().saturating_sub(CAP.as_millis() as u64);
        self.mine.lock().unwrap_or_else(PoisonError::into_inner).range(floor..).next().map(|(at, _)| *at)
    }

    /// Leader: a follower's beat said this (None: it said nothing, so none of its statements runs, or it is older).
    pub fn heard(&self, from: &str, oldest: Option<u64>) {
        let mut theirs = self.theirs.lock().unwrap_or_else(PoisonError::into_inner);
        match oldest {
            Some(at) => theirs.insert(from.to_string(), (Instant::now(), at)),
            None => theirs.remove(from),
        };
    }

    /// The oldest start any statement still reading began at, here or on a live follower (`SLACK` earlier).
    fn held(&self, past_a_day: &AtomicU64) -> Option<u64> {
        let (now, slack) = (now_ms(), SLACK.as_millis() as u64);
        let floor = now.saturating_sub(CAP.as_millis() as u64);
        let mine = {
            let mine = self.mine.lock().unwrap_or_else(PoisonError::into_inner);
            let (old, rest) = (mine.range(..floor), mine.range(floor..).next().map(|(at, _)| *at));
            let newest_old = old.clone().next_back().map(|(at, _)| *at);
            if let Some(newest) = newest_old.filter(|n| *n > self.capped.load(Relaxed)) {
                let seen = self.capped.swap(newest, Relaxed);
                past_a_day.fetch_add(old.filter(|(at, _)| **at > seen).map(|(_, n)| *n as u64).sum(), Relaxed); // (each once)
            }
            rest
        };
        let theirs = {
            let mut theirs = self.theirs.lock().unwrap_or_else(PoisonError::into_inner);
            theirs.retain(|_, (heard, _)| heard.elapsed() < LEASE);
            theirs.values().map(|(_, at)| at.saturating_sub(slack)).filter(|at| *at >= floor).min()
        };
        mine.into_iter().chain(theirs).min()
    }

    #[cfg(test)]
    fn heard_at(&self, from: &str, at: u64, when: Instant) { self.theirs.lock().unwrap().insert(from.to_string(), (when, at)); }
}

/// What this node says to its leader of its lake's running statements (`cluster::Said`): the oldest one's start, if one runs.
pub fn said(lake: &Lake) -> Vec<(&'static str, String)> {
    lake.state::<Readings>().ok().and_then(|r| r.oldest()).map(|at| (HEADER, at.to_string())).into_iter().collect()
}

/// Leader: what a follower's beat, or a reader's word, said of its running statements (`said`).
pub fn heard(lake: &Lake, from: &str, headers: &http::HeaderMap) -> Result<()> {
    let at = headers.get(HEADER).and_then(|v| v.to_str().ok()?.parse().ok());
    lake.state::<Readings>()?.heard(from, at);
    Ok(())
}

/// Statements as readers of their lake (`pins`): the oldest start holds the files let go of since (`tier::expire`), and
/// none from the orphan sweep (newest 0). A reading ends with its statement, so none lapses or is swept.
pub fn readers() -> crate::pins::Pin {
    crate::pins::Pin { name: "statements", held: |l| Box::pin(held(l)), lapse: None, sweep: None, floor: None }
}

async fn held(lake: &Lake) -> Result<Option<(u64, u64)>> {
    Ok(lake.state::<Readings>()?.held(&lake.node.metrics.readings_past_a_day).map(|at| (at, 0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn readings() -> Arc<Readings> { Arc::new(<Readings as crate::lake_state::LakeState>::make()) }

    /// A statement holds from its start until it is dropped, the oldest of several holds, and one past a day holds no
    /// longer and is counted once.
    #[test]
    fn the_oldest_running_statement_holds_until_it_ends() {
        let (r, past) = (readings(), AtomicU64::new(0));
        let first = r.begin();
        let at = first.0.as_ref().unwrap().1;
        let second = r.begin();
        assert_eq!(r.oldest(), Some(at));
        assert_eq!(r.held(&past), Some(at), "the oldest holds");
        drop(first);
        assert!(r.oldest() >= Some(at), "the second still runs");
        drop(second);
        assert_eq!(r.oldest(), None, "none runs");
        let none = Reading::none();
        drop(none);
        let day = CAP.as_millis() as u64;
        *r.mine.lock().unwrap().entry(now_ms() - day - 1).or_default() += 2; // (two kept by mistake, a day on)
        assert_eq!(r.oldest(), None, "past a day: no longer said");
        r.held(&past);
        r.held(&past);
        assert_eq!(past.load(Relaxed), 2, "each counted once");
    }

    /// The gate: a statement planned before a merge, still reading as expiry runs with no grace, keeps the files the merge
    /// let go of, and reads its answer whole; once it is dropped, they go.
    #[tokio::test]
    async fn a_statement_reading_across_a_merge_keeps_its_files_until_it_is_done() -> Result<()> {
        use crate::store::{json, table_key, TableMeta};
        use datafusion::arrow::array::{AsArray, RecordBatch};
        use datafusion::arrow::datatypes::Int64Type;
        use futures::TryStreamExt;
        let dir = std::env::temp_dir().join(format!("pondra-readings-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false, crate::node_state::NodeState::unhosted()).await?;
        crate::write::create_table(&lake, "t", r#"{"columns": [["x", "Int64"]]}"#).await?;
        for x in [1, 2] {
            let ctx = lake.session();
            let (_, files) = crate::write::write_rows(&lake, &ctx, "t", ctx.sql(&format!("SELECT CAST({x} AS BIGINT) AS x")).await?, None).await?;
            let mut meta = lake.cat.get::<TableMeta>(&table_key("t")).await?.unwrap();
            meta.files.extend(files);
            lake.cat.commit(vec![(table_key("t"), json(&meta))], &[]).await?;
        }
        let paths: Vec<String> = lake.cat.get::<TableMeta>(&table_key("t")).await?.unwrap().files.iter().map(|f| f.path.clone()).collect();
        assert_eq!(paths.len(), 2);
        let there = |p: &String| {
            let (store, at) = (lake.store.clone(), object_store::path::Path::from(p.as_str()));
            async move { object_store::ObjectStoreExt::head(&store, &at).await.is_ok() }
        };
        let ctx = crate::query::session_for(&lake, "SELECT x FROM t", "", None, &crate::auth::Principal::node().scope).await?;
        let stream = crate::engine::sql(&ctx, "SELECT x FROM t ORDER BY x").await?.execute_stream().await?;
        // (the merge: both files out of the table, into its garbage; nothing else reads them)
        let mut meta = lake.cat.get::<TableMeta>(&table_key("t")).await?.unwrap();
        let old = std::mem::take(&mut meta.files);
        meta.discard(&old);
        lake.cat.commit(vec![(table_key("t"), json(&meta))], &[]).await?;
        tokio::time::sleep(Duration::from_millis(20)).await; // (let go of a while ago: with no grace, expiry would take them now)
        crate::tier::expire(&lake, 0).await?;
        for p in &paths {
            assert!(there(p).await, "{p}: kept while the statement reads");
        }
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let rows: Vec<i64> = batches.iter().flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec()).collect();
        assert_eq!(rows, [1, 2], "its answer is whole");
        drop(ctx);
        crate::tier::expire(&lake, 0).await?;
        for p in &paths {
            assert!(!there(p).await, "{p}: gone once the statement is done");
        }
        lake.cat.close().await?;
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A follower's word holds `SLACK` earlier while its beats come, and goes when they stop or it says nothing.
    #[test]
    fn a_followers_word_holds_while_its_beats_come() {
        let (r, past) = (readings(), AtomicU64::new(0));
        let slack = SLACK.as_millis() as u64;
        assert_eq!(r.held(&past), None, "none heard: none reads");
        let t = now_ms() - 5_000;
        r.heard("b:1", Some(t));
        assert_eq!(r.held(&past), Some(t - slack));
        r.heard("c:1", Some(t + 1_000));
        assert_eq!(r.held(&past), Some(t - slack), "the oldest of them");
        r.heard("b:1", None);
        assert_eq!(r.held(&past), Some(t + 1_000 - slack), "b said nothing: none of its statements runs");
        r.heard_at("c:1", t, Instant::now() - LEASE);
        assert_eq!(r.held(&past), None, "c's beats stopped");
        let mine = r.begin();
        assert_eq!(r.held(&past), Some(mine.0.as_ref().unwrap().1), "its own count as they are");
    }
}
