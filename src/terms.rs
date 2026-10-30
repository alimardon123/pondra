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
use datafusion::common::instant::Instant;
use std::time::Duration;

pub(crate) const LEASE: Duration = Duration::from_secs(5);

/// How long a claimer has to mark its term after its bell rang (`deaf`). It looks every second, and a mark is one
/// request: the rest is a slow bucket's margin, as long as the lease a follower gives a leader it stopped hearing.
const HEARD: Duration = LEASE;
const KEEP: u64 = 16; // (the newest terms whose leadership records a lake keeps: `prune`)

#[derive(Serialize, Deserialize, Clone)]
pub struct Term {
    pub n: u64,
    pub addr: String, // the leader of this term
    /// The claim's lock file on the machine that claimed it (`HELD`), or empty (older releases, or a
    /// temp folder that couldn't take one): 32 hex characters, never a path.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub lock: String,
    /// Its claimer answers the inbox's bell with a mark (`listen`), so a writer elsewhere that rang it and saw no mark
    /// knows it is gone (`deaf`). An older release's claimer doesn't; and a replicating node's silence is never taken for
    /// its end (`answers_bell`): its mark alone decides.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub bell: bool,
}

/// Is a claim's silence when rung its end (`Term::bell`)? Not a node's that replicates (`--ack replicated`): its followers
/// hold acknowledged changes the bucket doesn't have yet, and only a leader that reaches them may commit past them
/// (`replica::recover`). A writer that can't reach them waits for its mark to go stale, as its followers take over in a
/// lease (invariant 17).
fn answers_bell(addr: &str, replicas: usize) -> bool { addr.is_empty() || replicas <= 1 }

/// How long ago the newest claimer of a term before `n` marked itself (None: no mark is left, every one let its
/// term go): what `replica::recover` gives the members it can't reach.
pub(crate) async fn marked_before(store: &Store, n: u64) -> Option<Duration> {
    let metas: Vec<_> = store.list(Some(&Path::from("cluster/alive"))).try_collect().await.ok()?;
    let newest = metas.iter().filter(|m| m.location.filename().and_then(|f| f.parse::<u64>().ok()).is_some_and(|k| k < n)).map(|m| m.last_modified).max()?;
    Some(Duration::from_millis((crate::log::now_ms() as i64 - newest.timestamp_millis()).max(0) as u64))
}

/// A claimer that let term `n` go to members that may lead (`replica::Unreached`) waits for one of them to claim a newer
/// one, as long as a mark takes to go stale (30 s), so that it claims nothing in their way meanwhile.
pub async fn newer_claimed(store: &Store, n: u64) {
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if latest(store).await.ok().flatten().is_some_and(|t| t.n > n) {
            return;
        }
    }
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
    let bell = answers_bell(addr, crate::env::or("PONDRA_REPLICAS", 1));
    let term = Term { n, addr: addr.into(), lock: lock.as_ref().map(|(id, ..)| id.clone()).unwrap_or_default(), bell };
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

/// The term a one-off writer claims: the next term with no address, once the newest term is not alive. While it is alive,
/// `live` decides: an `Err` refuses (no claim is made), and an `Ok` waits a second and looks again. (A backup waits for
/// another one-off writer this way; a page's lake alone refuses one, `members::Alone::lead`.)
pub async fn claim_one_off(store: &Store, live: impl Fn(&Term) -> Result<()>) -> Result<Term> {
    loop {
        match latest(store).await? {
            Some(t) if alive(store, &t).await => {
                live(&t)?;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            t => {
                if let Some(term) = claim(store, t.map_or(1, |t| t.n + 1), "").await? {
                    return Ok(term);
                }
            }
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

/// The leader of term `n` marks it alive every 10 s, and whenever the bell rings (`listen`), until the task ends
/// (`write::lead`, a backup taken here, and a guest that leads: the node's stop ends it with its other tasks, before it gives its term back).
pub fn mark_every(store: Store, n: u64) -> tokio::task::JoinHandle<()> {
    crate::panics::task(async move {
        let (mut due, mut bell) = (Instant::now() + Duration::from_secs(10), None);
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if listen(&store, &mut bell).await || Instant::now() >= due {
                let ok = mark_alive(&store, n).await.is_ok();
                due = Instant::now() + Duration::from_secs(if ok { 10 } else { 0 });
            }
        }
    })
}

/// A claimer's ear: has the inbox's bell rung since the last look (`heard`)? One HEAD a second, a cheap request class
/// on S3 and R2, and its own turns of the bucket (`budget::urgent`), so a node busy with a scan still hears it. A first
/// look counts as a ring: one may have come while the claimer started, and the writer that rang waits for its mark.
pub(crate) async fn listen(store: &Store, heard: &mut Option<Option<(chrono::DateTime<chrono::Utc>, Option<String>)>>) -> bool {
    let bell = store.head(&Path::from(crate::inbox::BELL)).await.ok().map(|m| (m.last_modified, m.e_tag));
    heard.replace(bell.clone()) != Some(bell)
}

crate::per_node!(
    /// What this process heard of claimers on other machines (`deaf`): None, one that didn't mark its term when rung (gone,
    /// whatever its mark says: `alive`); Some, when one last did (it isn't rung again for `RUNG_AGAIN`). Keyed by the
    /// store, the term and its lock file's id.
    static RUNG: Mutex<BTreeMap<(String, u64, String), Option<Instant>>> = Mutex::new(BTreeMap::new()));

/// A claimer that answered its bell isn't rung again by `deaf` for this long: its mark, every 10 s, says it is alive
/// meanwhile, and goes stale in 30 s if it isn't. (A waiter or a follower outside the cluster's network rings this often,
/// no more. A request left in the inbox rings whatever: `inbox::send`.)
const RUNG_AGAIN: Duration = Duration::from_secs(30);

fn rung_key(store: &Store, t: &Term) -> (String, u64, String) { (store.to_string(), t.n, t.lock.clone()) }

/// A writer's watch on a claimer's mark, from the moment it rang the bell (`deaf`, `inbox::send`).
pub struct Ear {
    key: (String, u64, String),
    mark: Path,
    before: (chrono::DateTime<chrono::Utc>, Option<String>),
    rang: tokio::time::Instant,
}

impl Ear {
    /// Ring the inbox's bell (`id` its body), and watch whether term `t`'s claimer marks itself after it. None when its
    /// answer can't tell (see `deaf`), or the bucket refused the ring (the claimer may hear nothing new); the bell rings
    /// anyway, for the request just left in the inbox (`inbox::send`).
    pub async fn ring(store: &Store, t: Option<&Term>, id: &str) -> Option<Ear> { Ear::rung(store, t, id, true).await }

    /// `ring`; `always` false rings only when there is an answer to watch for (`deaf`).
    async fn rung(store: &Store, t: Option<&Term>, id: &str, always: bool) -> Option<Ear> {
        let watch = match t {
            Some(t) => Ear::before(store, t).await,
            None => None,
        };
        if watch.is_none() && !always {
            return None;
        }
        let rung = crate::inbox::ring(store, id).await;
        let (key, mark, before) = watch.filter(|_| rung)?;
        Some(Ear { key, mark, before, rang: tokio::time::Instant::now() })
    }

    async fn before(store: &Store, t: &Term) -> Option<((String, u64, String), Path, (chrono::DateTime<chrono::Utc>, Option<String>))> {
        // (its own claim, or one made on this machine by this user: the lock file decides, `gone_here`. A lake in this
        // process's memory has no claimer elsewhere)
        let here = HELD.lock().unwrap().contains_key(&t.n) || lock_path(t).is_some_and(|p| p.exists()) || crate::memory::held_in_memory(store);
        let key = rung_key(store, t);
        let lately = RUNG.lock().unwrap().get(&key).copied().flatten().is_some_and(|at| at.elapsed() < RUNG_AGAIN);
        if !t.bell || here || lately {
            return None;
        }
        let mark = Path::from(format!("cluster/alive/{:020}", t.n));
        let m = store.head(&mark).await.ok()?; // (no mark: `alive` says it's gone already)
        Some((key, mark, (m.last_modified, m.e_tag)))
    }

    /// Since the ring: Some(true), the claimer is gone (no mark in `HEARD`); Some(false), it marked itself, or let its
    /// term go; None, not yet.
    pub async fn gone(&self, store: &Store) -> Option<bool> {
        let now = match store.head(&self.mark).await {
            Ok(m) => (m.last_modified, m.e_tag),
            Err(_) => return Some(false), // (it let its term go, or stepped down: `alive` sees that itself)
        };
        let gone = match now != self.before {
            true => false,
            false if self.rang.elapsed() < HEARD => return None,
            false => true,
        };
        RUNG.lock().unwrap().insert(self.key.clone(), (!gone).then(Instant::now));
        Some(gone)
    }
}

/// A writer elsewhere waits for term `t`'s claimer: is it gone, though its mark is fresh? Rings the bell, and a claimer
/// that doesn't mark itself in `HEARD` is (killed, or its machine is gone: nothing marks its term then), and `alive` says
/// so from then on, here (invariant 17). False at once where its answer can't tell: an older release's claimer
/// (`Term::bell`), one on this machine (`gone_here` decides), or one that answered in the last `RUNG_AGAIN`: then the bell
/// isn't rung. False too when the bucket refused the ring.
pub async fn deaf(store: &Store, t: &Term) -> bool {
    let Some(ear) = Ear::rung(store, Some(t), &uuid::Uuid::new_v4().to_string(), false).await else { return false };
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Some(gone) = ear.gone(store).await {
            return gone;
        }
    }
}

/// `deaf` said so of term `t`'s claimer, in this process.
fn deaf_here(store: &Store, t: &Term) -> bool { RUNG.lock().unwrap().get(&rung_key(store, t)).is_some_and(Option::is_none) }

/// Is the holder of term `t` alive? A claim made on this machine is alive while its process holds
/// its lock file; anywhere else, it is alive if it marked itself alive in the last 30 s, and didn't
/// leave its bell unanswered when this process rang it (`deaf`).
pub async fn alive(store: &Store, t: &Term) -> bool {
    if gone_here(t) || deaf_here(store, t) {
        return false; // a claimer on this machine that ended can't be alive, whatever its mark says; nor one elsewhere that didn't answer
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

    fn elsewhere(n: u64, bell: bool) -> Term { Term { n, addr: String::new(), lock: String::new(), bell } }

    #[tokio::test]
    async fn a_claimer_that_marks_when_rung_is_alive() {
        let store: Store = Arc::new(object_store::memory::InMemory::new());
        let t = elsewhere(9_001, true);
        mark_alive(&store, 9_001).await.unwrap();
        let marks = mark_every(store.clone(), 9_001); // (its first look counts as a ring: a mark a second in)
        assert!(!deaf(&store, &t).await);
        assert!(alive(&store, &t).await);
        let rang = (tokio::time::Instant::now(), bell(&store).await);
        assert!(!deaf(&store, &t).await, "answered a moment ago: not rung again");
        assert!(rang.0.elapsed() < Duration::from_millis(100));
        assert_eq!(bell(&store).await, rang.1, "the bell isn't rung");
        marks.abort();
    }

    /// The bell's last ring (its body), if it rang.
    async fn bell(store: &Store) -> Option<bytes::Bytes> { Some(store.get(&Path::from(crate::inbox::BELL)).await.ok()?.bytes().await.unwrap()) }

    #[test]
    fn a_replicating_node_s_silence_isnt_its_end() {
        assert!(answers_bell("", 1) && answers_bell("", 2), "a one-off writer has no followers");
        assert!(answers_bell("10.0.0.1:8080", 1));
        assert!(!answers_bell("10.0.0.1:8080", 2), "its followers hold acknowledged changes: its mark alone decides");
    }

    #[tokio::test]
    async fn a_claimer_that_doesnt_mark_when_rung_is_gone() {
        let store: Store = Arc::new(object_store::memory::InMemory::new());
        let t = elsewhere(9_002, true);
        mark_alive(&store, 9_002).await.unwrap(); // (fresh: its process was killed a moment after)
        assert!(alive(&store, &t).await);
        let rang = tokio::time::Instant::now();
        assert!(deaf(&store, &t).await);
        assert!(rang.elapsed() >= HEARD);
        assert!(!alive(&store, &t).await, "gone, whatever its mark says");
        // (an older release's claimer doesn't answer the bell: its mark alone decides)
        let old = elsewhere(9_003, false);
        mark_alive(&store, 9_003).await.unwrap();
        let rung = bell(&store).await;
        assert!(!deaf(&store, &old).await && alive(&store, &old).await);
        assert_eq!(bell(&store).await, rung, "nothing to watch for: no ring");
        assert!(marked_before(&store, 9_003).await.is_some_and(|d| d < Duration::from_secs(30)), "9,002's mark, a moment ago");
        assert!(marked_before(&store, 9_002).await.is_none(), "no term before it was marked");
    }

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
                let term = Term { n, addr: String::new(), lock: String::new(), bell: false };
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
            let term = Term { n, addr: String::new(), lock: String::new(), bell: false };
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
