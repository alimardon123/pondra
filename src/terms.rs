//! A term is a lake's leadership: its number names the one catalog writer, until a newer term is claimed.
//! This module keeps the term records in the bucket (`cluster/term`, `cluster/alive`, `cluster/left`), the lock
//! files that show whether a claimer on this machine is still here, and the calls that claim, mark and let go
//! of a term, so the lake modules need not name the cluster.

use crate::store::Store;
use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

const KEEP: u64 = 16; // (the newest terms whose leadership records a lake keeps: `prune`)

#[derive(Serialize, Deserialize, Clone)]
pub struct Term {
    pub n: u64,
    pub addr: String, // the leader of this term
    /// The claim's lock file on the machine that claimed it (`HELD`), or empty (older releases, or a
    /// temp folder that couldn't take one): 32 hex characters, never a path.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub lock: String,
}

/// The newest term, if any.
pub async fn latest(store: &Store) -> Result<Option<Term>> {
    let metas: Vec<_> = store.list(Some(&Path::from("cluster/term"))).try_collect().await?;
    let Some(m) = metas.iter().max_by_key(|m| m.location.to_string()) else { return Ok(None) };
    Ok(Some(serde_json::from_slice(&store.get(&m.location).await?.bytes().await?)?))
}

/// Try to become leader of term `n`: put-if-absent, so at most one node succeeds. (`addr` is
/// empty for a `pondra sql` INSERT recording its files: nothing to follow, just wait for it.)
pub async fn claim(store: &Store, n: u64, addr: &str) -> Result<Option<Term>> {
    // (its id goes into the record: a process here can tell when we're gone; none reaches a lake in our memory)
    let lock = if crate::memory::held_in_memory(store) { None } else { new_lock() };
    let term = Term { n, addr: addr.into(), lock: lock.as_ref().map(|(id, ..)| id.clone()).unwrap_or_default() };
    let lock = lock.map(|(_, path, f)| (path, f));
    let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
    match store.put_opts(&Path::from(format!("cluster/term/{n:020}")), serde_json::to_vec(&term)?.into(), opts).await {
        Ok(_) => {
            // (the term listing and our alive mark go at once, together: one round trip, not two. A newer term in the
            // listing means this claim was made from an older view, or it raced the next number and lost: a newer leader
            // holds the lake, so our mark goes, the lock with it, and we step aside. Our record stays: a later prune removes it.)
            let (listed, marked) = futures::join!(numbered(store, "cluster/term"), mark_alive(store, n));
            let listed = match (listed, marked) {
                (Ok(listed), Ok(())) => listed,
                (Err(e), _) | (_, Err(e)) => {
                    discard(lock);
                    return Err(e);
                }
            };
            if listed.iter().any(|(k, _)| *k > n) {
                let _ = store.delete(&Path::from(format!("cluster/alive/{n:020}"))).await;
                discard(lock);
                return Ok(None);
            }
            if let Some(held) = lock {
                HELD.lock().unwrap().insert(n, held);
            }
            // (capped: a bucket that stalls can't hold the claim; the next claim prunes what this one didn't)
            let _ = tokio::time::timeout(Duration::from_secs(10), prune(store, n, &listed)).await;
            Ok(Some(term))
        }
        Err(object_store::Error::AlreadyExists { .. }) => {
            discard(lock); // someone else won the term
            Ok(None)
        }
        Err(e) => {
            discard(lock);
            Err(e.into())
        }
    }
}

/// The numbered records under one leadership prefix (`cluster/term`, `cluster/alive` or `cluster/left`), each as its
/// number and location. A name that isn't a number is left out.
async fn numbered(store: &Store, prefix: &str) -> Result<Vec<(u64, Path)>> {
    let metas: Vec<_> = store.list(Some(&Path::from(prefix))).try_collect().await?;
    Ok(metas.into_iter().filter_map(|m| Some((m.location.filename()?.parse::<u64>().ok()?, m.location))).collect())
}

/// Deletes the leadership records below the newest `KEEP` terms (`n` is the newest). The claim's own listing of
/// `cluster/term` decides: only a lake holding more than twice `KEEP` terms lists `alive` and `left` (one round trip, both
/// at once) and deletes everything below the floor in one call (S3 deletes in batches). So most claims pay nothing more
/// than the listing they make. Nothing reads a superseded record (`superseded`). Errors are ignored: a record that didn't
/// go goes at a later claim.
async fn prune(store: &Store, n: u64, terms: &[(u64, Path)]) {
    if terms.len() <= 2 * KEEP as usize {
        return;
    }
    let floor = (n + 1).saturating_sub(KEEP); // (the newest KEEP terms stay: floor ..= n)
    let (alive, left) = futures::join!(numbered(store, "cluster/alive"), numbered(store, "cluster/left"));
    let rest = alive.unwrap_or_default().into_iter().chain(left.unwrap_or_default());
    let gone: Vec<object_store::Result<Path>> = terms.iter().cloned().chain(rest).filter(|(k, _)| *k < floor).map(|(_, at)| Ok(at)).collect();
    store.delete_stream(futures::stream::iter(gone).boxed()).collect::<Vec<_>>().await;
}

/// Is the leader of term `m` superseded? A newer term exists, or its own record is gone: `prune` keeps
/// the newest `KEEP`, so a term with `KEEP` newer ones after it has no record. Only a record that isn't
/// there counts (NotFound): a bucket that can't be reached says nothing, and a leader isn't restarted for it.
pub(crate) async fn superseded(store: &Store, m: u64) -> bool {
    if store.head(&Path::from(format!("cluster/term/{:020}", m + 1))).await.is_ok() {
        return true;
    }
    matches!(store.head(&Path::from(format!("cluster/term/{m:020}"))).await, Err(object_store::Error::NotFound { .. }))
}

/// The holder of term `n` is still here (the leader: every 10 s).
pub async fn mark_alive(store: &Store, n: u64) -> Result<()> {
    store.put(&Path::from(format!("cluster/alive/{n:020}")), Vec::<u8>::new().into()).await?;
    Ok(())
}

/// The leader of term `n` marks it alive every 10 s, until the task ends (`write::lead`, and a guest that leads: the
/// node's stop ends it with its other tasks, before it gives its term back).
pub fn mark_every(store: Store, n: u64) -> tokio::task::JoinHandle<()> {
    crate::panics::task(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let _ = mark_alive(&store, n).await;
        }
    })
}

/// Is the holder of term `t` alive? A claim made on this machine is alive while its process holds
/// its lock file; anywhere else, it is alive if it marked itself alive in the last 30 s.
pub async fn alive(store: &Store, t: &Term) -> bool {
    if gone_here(t) {
        return false; // a claimer on this machine that ended can't be alive, whatever its mark says
    }
    match store.head(&Path::from(format!("cluster/alive/{:020}", t.n))).await {
        // (a leader that stepped down isn't alive, whatever its mark says: its last mark, in flight
        // as it stepped down, can land after `release` took it away)
        Ok(m) => (crate::log::now_ms() as i64 - m.last_modified.timestamp_millis()) < 30_000 && !stepped_down(store, t.n).await,
        Err(_) => false,
    }
}

/// A one-off writer is done: whoever comes next doesn't wait for its mark to go stale, nor for its
/// lock file to be let go.
pub async fn release(store: &Store, n: u64) {
    let _ = store.delete(&Path::from(format!("cluster/alive/{n:020}"))).await;
    let held = HELD.lock().unwrap().remove(&n);
    discard(held);
}

crate::per_node!(
    /// The lock files of the terms this node claimed (or took back after a restart), each locked until
    /// the term is let go (`release`), the node's state is dropped as it stops (`reset`), or the process
    /// ends, however it ends: the OS lets go at whichever comes first. Keyed by term number. (A node
    /// holds one claim per term number: one lake at a time.)
    pub(crate) static HELD: Mutex<BTreeMap<u64, (PathBuf, std::fs::File)>> = Mutex::new(BTreeMap::new()));

/// This user's folder of claim lock files, in the temp folder: per user on macOS and Windows, and
/// named by the user's id on Linux, where `/tmp` is shared. Made 0700, and used only while it is
/// ours alone, so no other user can take a lock away (and make a live leader look gone): `memory::own_dir`.
/// Its base is the temp folder, never the spill folder: every process on the machine must find the
/// same one (invariant 370). None: the claims go on without locks, and the mark decides.
/// A page (wasm32) has no temp folder (`temp_dir` panics there), so there are none.
pub(crate) fn lock_dir() -> Option<PathBuf> {
    if cfg!(target_arch = "wasm32") {
        return None;
    }
    crate::memory::own_dir(&std::env::temp_dir(), "pondra-terms").ok()
}

/// Where term `t`'s lock file would be here: its id is trusted only as 32 hex characters (it comes
/// from the bucket, so it must never become a path).
pub(crate) fn lock_path(t: &Term) -> Option<PathBuf> {
    let ok = t.lock.len() == 32 && t.lock.bytes().all(|b| b.is_ascii_hexdigit());
    ok.then(|| lock_dir().map(|d| d.join(format!("{}.lock", t.lock)))).flatten()
}

pub(crate) fn open_lock(path: &std::path::Path) -> std::io::Result<std::fs::File> { std::fs::OpenOptions::new().read(true).write(true).open(path) }

/// A new claim's lock file: a random id (written into the term record) and the file, locked by this
/// process. None when the folder can't take one: the claim goes on without it, and the next writer
/// waits for the mark, as before.
fn new_lock() -> Option<(String, PathBuf, std::fs::File)> {
    let dir = lock_dir()?;
    sweep_locks(&dir);
    let id = uuid::Uuid::new_v4().simple().to_string();
    let path = dir.join(format!("{id}.lock"));
    let f = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&path).ok()?;
    if f.try_lock().is_ok() {
        return Some((id, path, f));
    }
    discard(Some((path, f)));
    None
}

/// Removes the lock files of claims whose process has ended: files a day old that nobody holds. Best
/// effort. (The age keeps the sweep off any claim made just now.)
fn sweep_locks(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let path = e.path();
        let old = e.metadata().and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).is_some_and(|age| age > Duration::from_secs(24 * 3600));
        if !old || !path.extension().is_some_and(|x| x == "lock") {
            continue;
        }
        let Ok(f) = open_lock(&path) else { continue };
        if f.try_lock().is_ok() {
            drop(f); // (Windows can't delete a file that's open)
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Lets go of a claim's lock file and removes it (best effort).
fn discard(held: Option<(PathBuf, std::fs::File)>) {
    if let Some((path, f)) = held {
        drop(f); // (Windows can't delete a file that's open)
        let _ = std::fs::remove_file(path);
    }
}

/// What a claim's lock file says: nothing while its process runs, `restarting <ms>` from when it
/// restarts itself (`restart`) until it takes the lock back (`relock`).
pub(crate) fn say(f: &std::fs::File, what: &str) {
    use std::io::{Seek, Write};
    let mut f = f;
    let _ = f.set_len(0).and_then(|_| f.seek(std::io::SeekFrom::Start(0))).and_then(|_| f.write_all(what.as_bytes()));
}

/// Has the process that claimed term `t` on this machine ended? Its lock file is here, nobody holds
/// it, and it didn't say it was restarting in the last 30 s (an exec lets go of the lock, and so does
/// a restart on Windows: the claimer is coming back, so it isn't gone). A claim made on another
/// machine, or by another user, has no file here: its mark decides, as invariant 17 asks.
pub(crate) fn gone_here(t: &Term) -> bool {
    let Some(path) = lock_path(t) else { return false };
    if HELD.lock().unwrap().contains_key(&t.n) {
        return false; // this very process holds it
    }
    let Ok(mut f) = open_lock(&path) else { return false };
    if f.try_lock().is_err() {
        return false; // its claimer holds it (a lock taken here ends with `f`)
    }
    let mut said = String::new();
    let _ = std::io::Read::read_to_string(&mut f, &mut said);
    let restarting = said.strip_prefix("restarting ").and_then(|ms| ms.parse::<u64>().ok());
    !restarting.is_some_and(|ms| crate::log::now_ms().saturating_sub(ms) < 30_000)
}

/// The leader of term `n` stops, drained (`drain.rs`): it says so (`cluster/left/{n}`), so its
/// followers take over at once instead of waiting out the lease, then releases its term.
pub async fn step_down(store: &Store, n: u64) {
    let _ = store.put_opts(&Path::from(format!("cluster/left/{n:020}")), Vec::<u8>::new().into(), PutOptions { mode: PutMode::Create, ..Default::default() }).await;
    release(store, n).await;
}

/// Has the leader of term `n` stepped down?
pub(crate) async fn stepped_down(store: &Store, n: u64) -> bool { store.head(&Path::from(format!("cluster/left/{n:020}"))).await.is_ok() }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// The numbers of the records under `prefix`, sorted.
    async fn numbers(store: &Store, prefix: &str) -> Vec<u64> {
        let metas: Vec<_> = store.list(Some(&Path::from(prefix))).try_collect().await.unwrap();
        let mut out: Vec<u64> = metas.iter().filter_map(|m| m.location.filename()?.parse().ok()).collect();
        out.sort();
        out
    }

    #[tokio::test]
    async fn a_claim_keeps_the_newest_sixteen_once_the_lake_has_more_than_thirty_two() {
        let dir = std::env::temp_dir().join(format!("pondra-prune-{}", uuid::Uuid::new_v4().simple()));
        let folder = crate::store::open_store(&dir.to_string_lossy()).unwrap().1;
        let url = format!("memory://prune-{}", uuid::Uuid::new_v4().simple());
        let memory = crate::memory::store(&url);
        for store in [folder.clone(), memory.clone()] {
            // (40 terms; each leader marked alive and never let go, a crash's marks too; three terms stepped down)
            for n in 1..=40u64 {
                let term = Term { n, addr: String::new(), lock: String::new() };
                store.put(&Path::from(format!("cluster/term/{n:020}")), serde_json::to_vec(&term).unwrap().into()).await.unwrap();
                store.put(&Path::from(format!("cluster/alive/{n:020}")), Vec::<u8>::new().into()).await.unwrap();
            }
            for n in [2u64, 9, 30] {
                store.put(&Path::from(format!("cluster/left/{n:020}")), Vec::<u8>::new().into()).await.unwrap();
            }
            assert_eq!(numbers(&store, "cluster/term").await.len(), 40);
            let won = claim(&store, 41, "").await.unwrap().expect("term 41 is none's");
            assert_eq!(won.n, 41);
            // (41 is more than 32: the claim's own listing deletes what is below the newest 16, before it returns)
            let (want, kept_left) = ((26..=41).collect::<Vec<u64>>(), vec![30u64]);
            assert_eq!(numbers(&store, "cluster/term").await, want, "the newest 16 terms stay");
            assert_eq!(numbers(&store, "cluster/alive").await, want, "the crashed term's mark is gone with its term");
            assert_eq!(numbers(&store, "cluster/left").await, kept_left, "the stepped-down terms below the newest 16 are gone");
            assert_eq!(latest(&store).await.unwrap().map(|t| t.n), Some(41), "latest answers the newest");
            release(&store, 41).await;
        }
        crate::memory::forget_memory(&url);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_claim_behind_a_newer_term_steps_aside() {
        let store: Store = Arc::new(object_store::memory::InMemory::new());
        for n in 30..=50u64 {
            let term = Term { n, addr: String::new(), lock: String::new() };
            store.put(&Path::from(format!("cluster/term/{n:020}")), serde_json::to_vec(&term).unwrap().into()).await.unwrap();
        }
        assert!(claim(&store, 20, "").await.unwrap().is_none(), "term 50 is newer: a claim of 20 steps aside");
        assert_eq!(latest(&store).await.unwrap().map(|t| t.n), Some(50), "latest stays the newest");
        assert!(numbers(&store, "cluster/alive").await.is_empty(), "a claim that steps aside marks nothing alive");
    }

    #[tokio::test]
    async fn a_leader_is_superseded_by_a_newer_term_or_by_its_own_going() {
        async fn terms_at(ns: &[u64]) -> Store {
            let store: Store = Arc::new(object_store::memory::InMemory::new());
            for n in ns {
                store.put(&Path::from(format!("cluster/term/{n:020}")), Vec::<u8>::new().into()).await.unwrap();
            }
            store
        }
        assert!(superseded(&terms_at(&[5, 6]).await, 5).await, "term 6 is there: 5 is superseded");
        assert!(superseded(&terms_at(&[20]).await, 5).await, "5 and 6 are both pruned: 5's own record is gone");
        assert!(!superseded(&terms_at(&[4, 5]).await, 5).await, "5 is the newest: its record is there and 6 isn't");
        assert!(!superseded(&terms_at(&[5]).await, 5).await, "only 5 is there: nothing newer");
    }
}
