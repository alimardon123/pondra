//! A lake in this process's memory moving out of it, and the program's end. The lake itself, its budget and its spill
//! past it are the catalog's (`memory_lake`); the folders it spills to are the base's (`scratch`).
//!
//! A lake moves out of memory with `set_location` (`ALTER DATABASE … SET LOCATION`, `db.save`): its objects
//! are copied to the target while the node serves, the host stops the node, `settle` makes the target equal
//! to the lake as it is then, and the node starts again there. Nothing of the lake is written in place.
use crate::memory_lake::{forget_memory, in_memory, refuse_torn, MOVING, PIECE};
use crate::store::{Lake, Store};
use anyhow::Context;
use futures::future::ready;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, WriteMultipart};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path as FsPath;
use std::time::Duration;

/// A move that failed (`settle`), said on stderr: why, for the host to start the node again on the lake in memory
/// with (`node::Options::move_failed`), which says so in `/stats` and `pondra.databases` until a move lands.
pub fn move_failed(to: &str, e: &anyhow::Error) -> String {
    let why = format!("moving the lake to {to} failed: {e:#}");
    eprintln!("{why}; it stays in memory");
    why
}

/// The program's end, for the ends that run no destructor (`process::exit`, and `main` returning past its
/// statics): the lakes held in memory go, then every folder this process made that is still there. Best
/// effort, no error. On Windows a folder whose lock file is still open (`store::SPILLS` is a static) can't
/// be removed, and stays for the next process's sweep.
pub fn leave() {
    crate::memory_lake::forget_all(); // (a lake's folder goes with its last store, and its drop takes it out of the made ones)
    crate::scratch::leave();
}

/// The objects a move copied, by path, each with the e-tag it had in the lake when it was read. Pass 2
/// copies what has changed since: the target's e-tags are its own, so only the lake's can say.
pub type Copied = BTreeMap<String, Option<String>>;

/// `ALTER DATABASE b SET LOCATION 'dir'` (ADR-072 §7, `db.save`): a lake held in this process's memory moves
/// to a folder or a bucket and goes on there, the same node and port, its tasks running. Every check passes
/// before anything is copied. Pass 1 copies the objects while the node still serves; then the host
/// (`Lifecycle::move_lake`) stops the node, runs `settle`, and starts it again on the target.
pub async fn set_location(lake: &Lake, database: &str, to: &str, scope: &crate::rights::Scope) -> anyhow::Result<serde_json::Value> {
    let this = crate::ddl::lake_name(lake);
    if !in_memory(&lake.url) {
        return Err(crate::codes::coded("0A000", "SET LOCATION moves a lake held in memory; a lake on disk or in a bucket moves with BACKUP DATABASE … TO and RESTORE DATABASE … FROM"));
    }
    anyhow::ensure!(database == this, "ALTER DATABASE {database} SET LOCATION: this database is {this}");
    anyhow::ensure!(to.trim() != ":memory:" && !in_memory(to), "a lake moves to a folder or a bucket, not to memory");
    anyhow::ensure!(crate::branch::pins(lake).await?.is_none(), "a branch is made from this database and keeps its files: it moves once no branch is made from it");
    anyhow::ensure!(lake.node.life.movable(), crate::lifecycle::NO_MOVE);
    let to = crate::ddl::full(to)?;
    crate::ext::owners_only(&to, format!("ALTER DATABASE … SET LOCATION {to}: a path on the server's own disk, which only the node's owner moves a lake into (the `pondra` shell, `pondra sql`, `connect()` of a lake): move it to a bucket (s3://, gs://, az://)"))?; // (the asker's context: `ddl_here` runs in the request)
    crate::backup::apart(lake, &to)?;
    crate::ext::reach_place(lake, &to, scope).await?; // (a bucket: the secret whose scope covers it)
    let dest = crate::store::counted(crate::store::open_store(&to, Some(&lake.node.reaches))?.1, &lake.node.metrics); // (its requests the node's: `Counted`)
    refuse_torn(&dest, &to).await?; // (a move that didn't finish there: named, before the place is called holding other things)
    anyhow::ensure!(crate::backup::empty(&to, Some(&lake.node.reaches)).await?, "{to} holds other things: move into a new place (or an empty folder)");
    let metas: Vec<ObjectMeta> = lake.store.list(None).try_collect().await.context("listing the lake")?; // (before the mark: a failure here leaves the place as it was)
    // The mark goes first of the copies, alone (`Create`: a mark already there is another move's). From here on,
    // whatever stops this move leaves a place refused by name, never a copy that looks like a lake.
    let mark = serde_json::json!({"from": lake.url, "pid": std::process::id(), "started_ms": crate::log::now_ms()});
    dest.put_opts(&Path::from(MOVING), serde_json::to_vec(&mark)?.into(), PutOptions { mode: PutMode::Create, ..Default::default() }).await.context("marking the place")?;
    let done: Vec<anyhow::Result<Option<(String, Option<String>, u64)>>> = futures::stream::iter(metas)
        .map(|meta| {
            let (from, dest) = (lake.store.clone(), dest.clone());
            async move {
                let at = meta.location;
                let got = copy_at(&from, &dest, &at, true).await?;
                Ok(got.map(|(e_tag, n)| (at.to_string(), e_tag, n)))
            }
        })
        .buffer_unordered(16)
        .collect()
        .await;
    let (mut copied, mut objects, mut bytes, mut failed) = (Copied::new(), 0u64, 0u64, None);
    for d in done {
        match d {
            Ok(Some((at, e_tag, n))) => {
                copied.insert(at, e_tag);
                objects += 1;
                bytes += n;
            }
            Ok(None) => {} // (let go since it was listed)
            Err(e) => failed = failed.or(Some(e)),
        }
    }
    if let Some(e) = failed {
        return Err(cleared(&dest, &to, e.context(format!("copying the lake to {to}"))).await);
    }
    if let Err(e) = lake.node.life.move_lake(crate::lifecycle::Move { from: lake.url.clone(), to: to.clone(), copied }) {
        return Err(cleared(&dest, &to, e).await); // (no host took the move: the copies go, and the mark with them)
    }
    // (answered once the host has begun to stop the node: a statement sent next waits for the move, rather
    // than being answered by the node it is about to leave)
    for _ in 0..5000 {
        if lake.node.drain.draining() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Ok(serde_json::json!({"moving": to, "objects": objects, "bytes": bytes, "notice": "the node starts again on it: statements wait a moment, a Postgres connection reconnects; if the move fails, the lake stays in memory and pondra.databases.move_failed says why"}))
}

/// Copies the object at `at` from `from` to `to`, as it is. `create`: it must not be in `to` yet (pass 1,
/// into an empty place). `None`: it is gone from `from`. Otherwise its e-tag as the read saw it, and its size.
async fn copy_at(from: &Store, to: &Store, at: &Path, create: bool) -> anyhow::Result<Option<(Option<String>, u64)>> {
    let got = match from.get(at).await {
        Ok(got) => got,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let (e_tag, size) = (got.meta.e_tag.clone(), got.meta.size);
    if size <= PIECE {
        let mode = if create { PutMode::Create } else { PutMode::Overwrite };
        to.put_opts(at, got.bytes().await?.into(), PutOptions { mode, ..Default::default() }).await?;
    } else {
        let mut out = WriteMultipart::new_with_chunk_size(to.put_multipart(at).await?, PIECE as usize);
        let mut pieces = got.into_stream();
        while let Some(piece) = pieces.next().await {
            out.wait_for_capacity(1).await?;
            out.write(&piece?);
        }
        out.finish().await?;
    }
    Ok(Some((e_tag, size)))
}

/// Takes away what is at the target, so a move that failed leaves the place as pass 1 found it: empty. A listing,
/// not what the copies named: a failed copy may have written part of an object. The mark goes last, and only once
/// everything else is gone (a listing returns it first, a local folder in any order): a copy without its mark would
/// look like a lake, and one with it is refused by name. A failure keeps the mark and says how many objects are left.
async fn clear(dest: &Store) -> anyhow::Result<()> {
    let listed: Vec<Path> = dest.list(None).map_ok(|meta| meta.location).try_collect().await.context("listing the place to clear it (none of it is taken away)")?;
    let mark = Path::from(MOVING);
    let (others, marked): (Vec<Path>, Vec<Path>) = listed.into_iter().partition(|at| *at != mark);
    let (mut left, mut first): (usize, Option<object_store::Error>) = (0, None);
    for at in &others {
        if let Err(e) = take_away(dest, at).await {
            left += 1;
            first = first.or(Some(e));
        }
    }
    if let Some(e) = first {
        return Err(anyhow::Error::from(e).context(format!("{} of its objects couldn't be taken away (its {MOVING} mark stays)", left + marked.len())));
    }
    for at in &marked {
        take_away(dest, at).await.with_context(|| format!("its {MOVING} mark couldn't be taken away"))?;
    }
    Ok(())
}

/// Takes `at` away. One that is gone already is taken away too: a delete that finds it gone is no failure.
async fn take_away(dest: &Store, at: &Path) -> object_store::Result<()> {
    match dest.delete(at).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

/// The error a move that failed ends with: `e`, and, when the target can't be cleared either, that too. Its mark
/// stays then, so the place is refused by name until someone clears it.
async fn cleared(dest: &Store, to: &str, e: anyhow::Error) -> anyhow::Error {
    match clear(dest).await {
        Ok(()) => e,
        Err(c) => e.context(format!("{to} couldn't be cleared after it ({c:#})")),
    }
}

/// Removes the folders under `dir` that hold nothing, deepest first: what a cleared folder target keeps.
fn prune_empty(dir: &FsPath) {
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            prune_empty(&entry.path());
            let _ = std::fs::remove_dir(entry.path()); // (fails, and so stays, unless it is empty)
        }
    }
}

/// Pass 2, once the host has stopped the node (`lifecycle::Move`): the target is made equal to the lake as it is
/// now. What changed since pass 1 read it is copied again, what is new copied, what the lake no longer holds
/// taken away there, and the lake in memory let go.
pub async fn settle(m: &crate::lifecycle::Move) -> anyhow::Result<()> {
    let from = crate::memory_lake::held(&m.from).context("the lake to move is no longer in memory")?;
    let dest = crate::store::open_store(&m.to, None)?.1; // (the node is stopped: its place is reached as the environment says, as its next start reaches it)
    if let Err(e) = settle_into(&from, &dest, m).await {
        // (a failed second pass leaves the lake in memory, whole, and nothing of the move at the target: or its
        // mark, when the target can't be cleared either, which refuses the place by name)
        let e = cleared(&dest, &m.to, e).await;
        if !m.to.contains("://") {
            prune_empty(FsPath::new(&m.to)); // (a folder keeps the folders its objects were in)
        }
        return Err(e);
    }
    forget_memory(&m.from);
    Ok(())
}

/// The copies and deletes of pass 2 (`settle`), which a failure of any of them undoes.
async fn settle_into(from: &Store, dest: &Store, m: &crate::lifecycle::Move) -> anyhow::Result<()> {
    let now: Vec<ObjectMeta> = from.list(None).try_collect().await?;
    let (mut keep, mut changed) = (BTreeSet::new(), vec![]);
    for meta in now {
        let key = meta.location.to_string();
        let same = matches!(m.copied.get(&key), Some(Some(e)) if meta.e_tag.as_deref() == Some(e.as_str()));
        if !same {
            changed.push(meta.location.clone());
        }
        keep.insert(key);
    }
    futures::stream::iter(changed)
        .map(|at| {
            let (from, dest) = (from.clone(), dest.clone());
            async move { copy_at(&from, &dest, &at, false).await.map(|_| ()) }
        })
        .buffer_unordered(16)
        .try_collect::<Vec<()>>()
        .await?;
    // (test hook: a second pass that fails, as a full disk would. After its copies, not the first one: a pass
    // with nothing changed copies nothing, and the hook must still fail it)
    if crate::store::crash_rolled("move_settle") {
        anyhow::bail!("PONDRA_CRASH at move_settle");
    }
    let stale: Vec<Path> = dest
        .list(None)
        .map_ok(|meta| meta.location)
        .try_filter(|at| {
            let key: &str = at.as_ref();
            ready(!keep.contains(key) && key != MOVING) // (the mark is in no lake's keep: it goes last, below)
        })
        .try_collect()
        .await?;
    for at in &stale {
        dest.delete(at).await?;
    }
    // The mark goes last: until then a copy that hasn't caught up is refused by name, never taken for a lake.
    take_away(dest, &Path::from(MOVING)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_lake::{store, torn};
    use std::path::PathBuf;
    use object_store::PutPayload;

    fn bytes(n: u8) -> PutPayload { vec![n; 10].into() }

    /// A folder of this test's, not made yet (`open_store` makes it), and its store: a move's target.
    fn target(name: &str) -> (PathBuf, Store) {
        let folder = std::env::temp_dir().join(format!("pondra-move-{name}-{}", uuid::Uuid::new_v4().simple()));
        let (_, store, _) = crate::store::open_store(folder.to_str().unwrap(), None).unwrap();
        (folder, store)
    }

    #[tokio::test]
    async fn settle_takes_the_mark_away_last_and_leaves_the_lake_whole() {
        let lake = format!("memory://mc-{}", uuid::Uuid::new_v4().simple());
        let from = store(&lake);
        let objects = ["catalog/manifest", "data/t/a.parquet", "files/notes/a.txt"];
        for at in objects {
            from.put(&Path::from(at), bytes(1)).await.unwrap();
        }
        let (folder, dest) = target("settle");
        dest.put(&Path::from(MOVING), bytes(9)).await.unwrap(); // (as a move's first step leaves it)
        let to = folder.to_str().unwrap().to_string();
        settle(&crate::lifecycle::Move { from: lake, to, copied: Copied::new() }).await.unwrap();
        let mut at: Vec<String> = dest.list(None).map_ok(|meta| meta.location.to_string()).try_collect().await.unwrap();
        at.sort();
        assert_eq!(at, objects, "every object of the lake, and no mark");
        let _ = std::fs::remove_dir_all(&folder);
    }

    #[tokio::test]
    async fn a_place_a_move_didnt_finish_is_refused_by_name() {
        let (folder, dest) = target("torn");
        let place = folder.to_str().unwrap().to_string();
        refuse_torn(&dest, &place).await.expect("a place with no mark is no torn place");
        dest.put(&Path::from(MOVING), bytes(1)).await.unwrap();
        let e = refuse_torn(&dest, &place).await.unwrap_err();
        assert_eq!(crate::codes::of(&e), "55000");
        assert!(torn(&e), "the refusal is the torn place's");
        let words = format!("{e:#}");
        assert!(words.contains(&place) && words.contains(MOVING), "{words}");
        assert!(!torn(&anyhow::anyhow!("{place} holds other things")), "another error is not");
        let _ = std::fs::remove_dir_all(&folder);
    }

    #[tokio::test]
    async fn clear_takes_everything_away_the_mark_too() {
        let (folder, dest) = target("clear");
        dest.put(&Path::from(MOVING), bytes(1)).await.unwrap();
        for at in ["catalog/manifest", "data/t/a.parquet"] {
            dest.put(&Path::from(at), bytes(2)).await.unwrap();
        }
        clear(&dest).await.unwrap();
        let left: Vec<String> = dest.list(None).map_ok(|meta| meta.location.to_string()).try_collect().await.unwrap();
        assert!(left.is_empty(), "{left:?}");
        let _ = std::fs::remove_dir_all(&folder);
    }
}
