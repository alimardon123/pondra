//! A page's catalog store: the keys in memory, logged into the lake's own object store (designs/page-same-core.md P2).
//! Each write is one object, `page-catalog/log/{seq}`, holding one batch. A checkpoint writes
//! `page-catalog/snapshot/{seq}`, every key and value as of its last batch, then deletes the logs and snapshots below it.
//! A checkpoint folds the logs into a snapshot only once they have grown (`due`): every batch is in the lake once its
//! write returns, so a checkpoint only compacts. A reader catches up with the logs as it reads, at most once per poll.
//! The lake's store holds all of it, and every object is created, never overwritten: a second writer on the same lake
//! (another tab) finds a log it cannot create, or a snapshot above its batch, and is fenced out, as SlateDB fences one.
use super::{Error, Kv, Written};
use crate::store::Store;
use bytes::Bytes;
use datafusion::common::instant::Instant;
use futures::future::BoxFuture;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

const LOGS: &str = "page-catalog/log";
const SNAPSHOTS: &str = "page-catalog/snapshot";
/// The fewest bytes of logs above the last snapshot that make a checkpoint due (1 MiB): small writes are not folded.
const FLOOR: u64 = 1 << 20;
/// The most logs above the last snapshot before a checkpoint is due, so a reopen reads a bounded number of objects.
const MOST_LOGS: u64 = 1024;

/// The page's catalog: its keys in memory and the lake's objects that hold them. A reader opens the same objects
/// and takes no writes.
pub(crate) struct Log {
    store: Store,
    keys: Mutex<BTreeMap<String, Bytes>>, // (never held across an await)
    next: AtomicU64,                      // the next batch's sequence number; moved under `writes` (a reader's under `catching`)
    writer: bool,
    writes: tokio::sync::Mutex<()>, // one write at a time, or a checkpoint taking the state it folds
    logged: AtomicU64,              // the bytes of the logs above the last snapshot: a write adds, a checkpoint takes off
    logs: AtomicU64,                // how many logs are above the last snapshot, counted as `logged` is
    snapshot: AtomicU64,            // the last snapshot's bytes (0 when there is none), set by a checkpoint
    floor: u64,                     // `FLOOR`, or a test's (see `writer_with_floor`)
    checkpoints: tokio::sync::Mutex<()>, // one checkpoint at a time; taken before `writes`
    poll: Duration,                 // how long a reader may go without catching up
    seen: Mutex<Instant>,           // when a reader last caught up (datafusion's, as std's panics on wasm32)
    catching: tokio::sync::Mutex<()>, // one catch-up at a time
}

impl Log {
    /// The catalog as a writer opens it: it takes writes and checkpoints.
    pub(crate) async fn writer(store: Store) -> Result<Log, Error> {
        Log::open(store, true, Duration::ZERO).await
    }

    /// The catalog as a reader opens it: no writes, and it catches up with the logs at most once per `poll`.
    pub(crate) async fn reader(store: Store, poll: Duration) -> Result<Log, Error> {
        Log::open(store, false, poll).await
    }

    /// A writer whose checkpoints fold at `floor` bytes, so a test can make a small log due.
    #[cfg(test)]
    async fn writer_with_floor(store: Store, floor: u64) -> Result<Log, Error> {
        let mut kv = Log::writer(store).await?;
        kv.floor = floor;
        Ok(kv)
    }

    /// The catalog as the lake's objects hold it now: the snapshot, then the logs above it, in order.
    async fn open(store: Store, writer: bool, poll: Duration) -> Result<Log, Error> {
        let state = replayed(&store).await?;
        Ok(Log {
            store,
            keys: Mutex::new(state.keys),
            next: AtomicU64::new(state.last + 1),
            writer,
            writes: tokio::sync::Mutex::new(()),
            logged: AtomicU64::new(state.logged),
            logs: AtomicU64::new(state.logs),
            snapshot: AtomicU64::new(state.snapshot),
            floor: FLOOR,
            checkpoints: tokio::sync::Mutex::new(()),
            poll,
            seen: Mutex::new(Instant::now()),
            catching: tokio::sync::Mutex::new(()),
        })
    }

    /// Has a reader caught up less than `poll` ago? (A writer's poll is zero, and it never reads `seen`.)
    fn fresh(&self) -> bool {
        self.seen.lock().unwrap_or_else(PoisonError::into_inner).elapsed() < self.poll
    }

    /// A reader's view catches up with the lake: the batches above its last, or the whole catalog when a checkpoint
    /// has folded some of them. A writer does nothing, and so does a reader that caught up less than `poll` ago.
    async fn catch_up(&self) -> Result<(), Error> {
        if self.writer || self.fresh() {
            return Ok(());
        }
        let _turn = self.catching.lock().await;
        if self.fresh() {
            return Ok(()); // (another read caught up while this one waited)
        }
        let last = self.next.load(Relaxed) - 1;
        match after(&self.store, last).await? {
            Some(batches) => {
                // (all decoded before any is applied, so a damaged one leaves the view as it was)
                let mut decoded = Vec::with_capacity(batches.len());
                for (i, bytes) in batches.iter().enumerate() {
                    let path = format!("{LOGS}/{:020}", last + 1 + i as u64);
                    decoded.push(decode_batch(bytes).ok_or_else(|| damaged(&path))?);
                }
                let mut keys = self.keys.lock().unwrap_or_else(PoisonError::into_inner);
                for (puts, deletes) in &decoded {
                    apply(&mut keys, puts, deletes);
                }
                self.next.store(last + 1 + batches.len() as u64, Relaxed);
            }
            None => {
                let state = replayed(&self.store).await?;
                *self.keys.lock().unwrap_or_else(PoisonError::into_inner) = state.keys;
                self.next.store(state.last + 1, Relaxed);
            }
        }
        *self.seen.lock().unwrap_or_else(PoisonError::into_inner) = Instant::now();
        Ok(())
    }
}

/// The catalog as the lake's objects hold it now: the last sequence number seen, the keys, and what the snapshot and
/// the logs above it weigh (a writer keeps those counts from here on).
struct Replayed {
    last: u64,
    keys: BTreeMap<String, Bytes>,
    snapshot: u64,
    logged: u64,
    logs: u64,
}

/// `replay` until it is not raced: `replay` says None when a checkpoint deleted an object it had listed.
async fn replayed(store: &Store) -> Result<Replayed, Error> {
    loop {
        if let Some(state) = replay(store).await? {
            return Ok(state);
        }
    }
}

/// The catalog as the lake's objects hold it now (see `Replayed`). The logs are listed before the snapshots, and a
/// checkpoint writes its snapshot before it deletes, so a checkpoint between the two listings can only add a snapshot
/// to what the first saw, never leave a gap in the logs. None: an object listed was deleted since, so list again.
async fn replay(store: &Store) -> Result<Option<Replayed>, Error> {
    let logs = list(store, LOGS).await?;
    let (covered, mut keys, snapshot): (u64, BTreeMap<String, Bytes>, u64) = match list(store, SNAPSHOTS).await?.pop() {
        Some((covered, path)) => {
            let Some(bytes) = read(store, &path).await? else { return Ok(None) };
            let (said, pairs) = decode_snapshot(&bytes).ok_or_else(|| damaged(path.as_ref()))?;
            if said != covered {
                return Err(damaged(path.as_ref()));
            }
            (covered, pairs.into_iter().collect(), bytes.len() as u64)
        }
        None => (0, BTreeMap::new(), 0),
    };
    let mut last = covered;
    let (mut logged, mut count): (u64, u64) = (0, 0);
    for (seq, path) in logs {
        last = last.max(seq);
        if seq <= covered {
            continue; // (the snapshot has it; a checkpoint's delete of it may not have finished)
        }
        let bytes = match read(store, &path).await? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let (puts, deletes) = decode_batch(&bytes).ok_or_else(|| damaged(path.as_ref()))?;
        apply(&mut keys, &puts, &deletes);
        logged += bytes.len() as u64;
        count += 1;
    }
    Ok(Some(Replayed { last, keys, snapshot, logged, logs: count }))
}

/// The batches above `last`, in order, each with its bytes; None when a checkpoint has folded some of them into a
/// snapshot (their logs may be gone), so the reader replays from the snapshot instead. The logs are listed before the
/// snapshots, as `replay` lists them, so a checkpoint between the two can only add a snapshot this sees.
async fn after(store: &Store, last: u64) -> Result<Option<Vec<Bytes>>, Error> {
    let mut batches = Vec::new();
    for (seq, path) in list(store, LOGS).await? {
        if seq <= last {
            continue;
        }
        if seq != last + 1 + batches.len() as u64 {
            return Ok(None); // (a gap: a checkpoint deleted the log between)
        }
        match read(store, &path).await? {
            Some(bytes) => batches.push(bytes),
            None => return Ok(None), // (deleted since it was listed)
        }
    }
    // (a snapshot above `last` may cover a fenced writer's leftover log among these batches: `replay` skips it)
    if passed(store, last + 1).await? {
        return Ok(None);
    }
    Ok(Some(batches))
}

/// The logs or the snapshots in the lake, in sequence order, each with its sequence number.
async fn list(store: &Store, folder: &str) -> Result<Vec<(u64, Path)>, Error> {
    let listed: Vec<ObjectMeta> = store.list(Some(&Path::from(folder))).try_collect().await.map_err(store_error)?;
    let mut logs = Vec::with_capacity(listed.len());
    for meta in listed {
        let seq: u64 = meta.location.filename().and_then(|name| name.parse().ok()).ok_or_else(|| damaged(meta.location.as_ref()))?;
        logs.push((seq, meta.location));
    }
    logs.sort_by_key(|(seq, _)| *seq);
    Ok(logs)
}

/// An object's bytes, or None when the lake has no such object.
async fn read(store: &Store, path: &Path) -> Result<Option<Bytes>, Error> {
    match store.get(path).await {
        Ok(got) => Ok(Some(got.bytes().await.map_err(store_error)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(store_error(e)),
    }
}

fn store_error(e: object_store::Error) -> Error {
    Error::new(false, e)
}

/// Has a newer writer checkpointed past `seq`? Then this one's batch at `seq` is below its snapshot, which `open` reads
/// in its place: this writer is fenced out.
async fn passed(store: &Store, seq: u64) -> Result<bool, Error> {
    Ok(list(store, SNAPSHOTS).await?.last().is_some_and(|(covered, _)| *covered >= seq))
}

/// Is a snapshot due? Once the logs above the last one hold as many bytes as it does (and at least `floor`), so a
/// snapshot costs no more than the writes it folds in, and a reopen reads at most about twice the catalog; or once
/// there are `MOST_LOGS` of them, so a reopen reads a bounded number of objects. Never with no log above it.
fn due(logged: u64, logs: u64, snapshot: u64, floor: u64) -> bool {
    logs > 0 && (logged >= floor.max(snapshot) || logs >= MOST_LOGS)
}

fn fenced() -> Error {
    Error::new(true, "another writer took the page's catalog")
}

fn damaged(path: &str) -> Error {
    Error::new(false, format!("the page's catalog object {path} is damaged"))
}

/// Puts, then deletes: a key both put and deleted ends deleted.
fn apply(keys: &mut BTreeMap<String, Bytes>, puts: &[(String, Bytes)], deletes: &[String]) {
    for (key, value) in puts {
        keys.insert(key.clone(), value.clone());
    }
    for key in deletes {
        keys.remove(key);
    }
}

/// A batch's object: the puts, then the deletes. Lengths are `u32`s, little-endian.
fn encode_batch(puts: &[(String, Bytes)], deletes: &[String]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    put_pairs(&mut out, puts.iter().map(|(key, value)| (key.as_str(), &value[..])))?;
    put_len(&mut out, deletes.len())?;
    for key in deletes {
        put_bytes(&mut out, key.as_bytes())?;
    }
    Ok(out)
}

/// A snapshot's object: the last sequence number it covers (a `u64`), then every key and value.
fn encode_snapshot(covered: u64, keys: &BTreeMap<String, Bytes>) -> Result<Vec<u8>, Error> {
    let mut out = covered.to_le_bytes().to_vec();
    put_pairs(&mut out, keys.iter().map(|(key, value)| (key.as_str(), &value[..])))?;
    Ok(out)
}

fn put_pairs<'a>(out: &mut Vec<u8>, pairs: impl ExactSizeIterator<Item = (&'a str, &'a [u8])>) -> Result<(), Error> {
    put_len(out, pairs.len())?;
    for (key, value) in pairs {
        put_bytes(out, key.as_bytes())?;
        put_bytes(out, value)?;
    }
    Ok(())
}

fn put_len(out: &mut Vec<u8>, n: usize) -> Result<(), Error> {
    let n = u32::try_from(n).map_err(|_| Error::new(false, "a page's catalog entry is over 4 GB"))?;
    out.extend_from_slice(&n.to_le_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), Error> {
    put_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

/// An object's bytes, read in order. A read says None past the end; a damaged object is one that is short or has
/// bytes left over.
struct Parts<'a>(&'a [u8]);

impl<'a> Parts<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if n > self.0.len() {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }

    fn u32(&mut self) -> Option<usize> {
        let n: [u8; 4] = self.take(4)?.try_into().ok()?;
        Some(u32::from_le_bytes(n) as usize)
    }

    fn slice(&mut self) -> Option<&'a [u8]> {
        let n = self.u32()?;
        self.take(n)
    }

    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.slice()?.to_vec()).ok()
    }

    fn pairs(&mut self) -> Option<Vec<(String, Bytes)>> {
        let n = self.u32()?;
        let mut pairs = Vec::new(); // (not sized from `n`: a count the bytes can't back fails at its first short read)
        for _ in 0..n {
            let key = self.string()?;
            pairs.push((key, Bytes::copy_from_slice(self.slice()?)));
        }
        Some(pairs)
    }

    fn done(&self) -> bool {
        self.0.is_empty()
    }
}

fn decode_batch(bytes: &[u8]) -> Option<(Vec<(String, Bytes)>, Vec<String>)> {
    let mut parts = Parts(bytes);
    let puts = parts.pairs()?;
    let n = parts.u32()?;
    let mut deletes = Vec::new();
    for _ in 0..n {
        deletes.push(parts.string()?);
    }
    parts.done().then_some((puts, deletes))
}

fn decode_snapshot(bytes: &[u8]) -> Option<(u64, Vec<(String, Bytes)>)> {
    let mut parts = Parts(bytes);
    let covered: [u8; 8] = parts.take(8)?.try_into().ok()?;
    let pairs = parts.pairs()?;
    parts.done().then_some((u64::from_le_bytes(covered), pairs))
}

// A page's reads are the map's, after a reader catches up with the lake. `remote` changes nothing: every batch is in the
// store once `write` returns, so a checkpoint folds the logs into a snapshot only once they have grown (`due`).
#[async_trait::async_trait]
impl Kv for Log {
    async fn get(&self, key: &str, _remote: bool) -> Result<Option<Bytes>, Error> {
        self.catch_up().await?;
        Ok(self.keys.lock().unwrap_or_else(PoisonError::into_inner).get(key).cloned())
    }

    async fn scan(&self, range: std::ops::Range<Vec<u8>>, _remote: bool) -> Result<BTreeMap<String, Bytes>, Error> {
        self.catch_up().await?;
        // (the bounds are key bytes, and not every one is UTF-8, so the map is filtered rather than ranged)
        let keys = self.keys.lock().unwrap_or_else(PoisonError::into_inner);
        let found: BTreeMap<String, Bytes> = keys.iter().filter(|(key, _)| range.start.as_slice() <= key.as_bytes() && key.as_bytes() < range.end.as_slice()).map(|(key, value)| (key.clone(), value.clone())).collect();
        Ok(found)
    }

    fn is_writer(&self) -> bool {
        self.writer
    }

    async fn write(&self, puts: &[(String, Bytes)], deletes: &[String]) -> Result<Arc<dyn Written>, Error> {
        if !self.writer {
            return Err(Error::new(false, "a reader takes no writes"));
        }
        let _turn = self.writes.lock().await;
        let seq = self.next.load(Relaxed);
        let batch = encode_batch(puts, deletes)?;
        let length = batch.len() as u64;
        let create = PutOptions { mode: PutMode::Create, ..Default::default() };
        match self.store.put_opts(&Path::from(format!("{LOGS}/{seq:020}")), PutPayload::from(batch), create).await {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => return Err(fenced()),
            Err(e) => return Err(store_error(e)),
        }
        // (a newer writer that checkpointed past `seq` deleted its log there, so the create alone proves nothing)
        if passed(&self.store, seq).await? {
            return Err(fenced());
        }
        apply(&mut self.keys.lock().unwrap_or_else(PoisonError::into_inner), puts, deletes);
        self.next.store(seq + 1, Relaxed);
        self.logged.fetch_add(length, Relaxed);
        self.logs.fetch_add(1, Relaxed);
        Ok(Arc::new(Stored))
    }

    async fn checkpoint(&self) -> Result<(), Error> {
        if !self.writer {
            return Err(Error::new(false, "a reader takes no writes"));
        }
        let _checkpoint = self.checkpoints.lock().await;
        let (covered, keys, logged, logs) = {
            let _turn = self.writes.lock().await;
            let (logged, logs) = (self.logged.load(Relaxed), self.logs.load(Relaxed));
            if !due(logged, logs, self.snapshot.load(Relaxed), self.floor) {
                return Ok(()); // (nothing to fold: no store call at all)
            }
            // The last batch applied: the snapshot covers every batch so far.
            let covered = self.next.load(Relaxed) - 1;
            let keys = self.keys.lock().unwrap_or_else(PoisonError::into_inner).clone();
            (covered, keys, logged, logs)
        };
        // (`writes` is free from here, so commits go on while the snapshot is encoded, stored and the old objects deleted.
        // That is safe: a write in flight is at `covered + 1` or above, so its `passed` check sees this snapshot below it
        // and is not fenced, and the deletes below take only objects `<= covered`.)
        let object = encode_snapshot(covered, &keys)?;
        let length = object.len() as u64;
        let create = PutOptions { mode: PutMode::Create, ..Default::default() };
        match self.store.put_opts(&Path::from(format!("{SNAPSHOTS}/{covered:020}")), PutPayload::from(object), create).await {
            // (one there already is the same keys: the state after a batch is the logs up to it, and each is one object)
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(e) => return Err(store_error(e)),
        }
        if passed(&self.store, covered + 1).await? {
            return Err(fenced());
        }
        // What it covers goes after it. A crash between the two leaves objects that `open` skips.
        for (seq, path) in list(&self.store, LOGS).await? {
            if seq <= covered {
                self.store.delete(&path).await.map_err(store_error)?;
            }
        }
        for (seq, path) in list(&self.store, SNAPSHOTS).await? {
            if seq < covered {
                self.store.delete(&path).await.map_err(store_error)?;
            }
        }
        // (the logs above `covered` are the writes that landed meanwhile, so these are subtractions, not a store of 0)
        self.logged.fetch_sub(logged, Relaxed);
        self.logs.fetch_sub(logs, Relaxed);
        self.snapshot.store(length, Relaxed);
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }

    fn cached(&self) -> usize {
        self.keys.lock().unwrap_or_else(PoisonError::into_inner).iter().map(|(key, value)| key.len() + value.len()).sum()
    }
}

/// A batch's handle. A page's batch is in the lake's store once `write` returns, so it is durable already.
struct Stored;

impl Written for Stored {
    fn durable(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn lake() -> Store {
        Arc::new(InMemory::new())
    }

    fn pair(key: &str, value: &str) -> (String, Bytes) {
        (key.to_string(), Bytes::from(value.to_string()))
    }

    /// Every key and value the store holds.
    async fn all(kv: &Log) -> BTreeMap<String, Bytes> {
        kv.scan(Vec::new()..vec![0xff], false).await.unwrap()
    }

    /// The sequence numbers of the log objects in the lake.
    async fn log_seqs(store: &Store) -> Vec<u64> {
        list(store, LOGS).await.unwrap().into_iter().map(|(seq, _)| seq).collect()
    }

    /// How many snapshot objects the lake holds.
    async fn snapshots(store: &Store) -> usize {
        list(store, SNAPSHOTS).await.unwrap().len()
    }

    #[tokio::test]
    async fn put_get_scan_and_delete() {
        let kv = Log::writer(lake()).await.unwrap();
        kv.write(&[pair("a", "1"), pair("b", "2"), pair("c", "3"), pair("d", "4")], &[]).await.unwrap().durable().await.unwrap();
        assert_eq!(kv.get("b", false).await.unwrap(), Some(Bytes::from("2")));
        assert_eq!(kv.get("z", false).await.unwrap(), None);
        let range = kv.scan(b"b".to_vec()..b"d".to_vec(), false).await.unwrap();
        assert_eq!(range.keys().collect::<Vec<_>>(), ["b", "c"]);
        assert_eq!(all(&kv).await.len(), 4);
        kv.write(&[pair("b", "9")], &["c".to_string()]).await.unwrap();
        assert_eq!(kv.get("b", false).await.unwrap(), Some(Bytes::from("9")));
        assert_eq!(kv.get("c", false).await.unwrap(), None);
        kv.write(&[pair("e", "5")], &["e".to_string()]).await.unwrap();
        assert_eq!(kv.get("e", false).await.unwrap(), None, "a key put and deleted in one batch ends deleted");
        // (a, b and d are left: each key and value is one byte)
        assert_eq!(kv.cached(), 3 * 2);
    }

    #[tokio::test]
    async fn reopen_replays_the_logs() {
        let store = lake();
        let kv = Log::writer(store.clone()).await.unwrap();
        kv.write(&[pair("a", "1")], &[]).await.unwrap();
        kv.write(&[pair("b", "2"), pair("a", "3")], &[]).await.unwrap();
        kv.write(&[], &["b".to_string()]).await.unwrap();
        drop(kv);
        let again = Log::writer(store.clone()).await.unwrap();
        assert_eq!(all(&again).await, BTreeMap::from([("a".to_string(), Bytes::from("3"))]));
        again.write(&[pair("c", "4")], &[]).await.unwrap();
        assert_eq!(log_seqs(&store).await, vec![1, 2, 3, 4], "the next batch follows the last one replayed");
    }

    #[tokio::test]
    async fn checkpoint_then_writes_then_reopen() {
        let store = lake();
        // (floor 0: a checkpoint is due once any log is above the last snapshot, as every checkpoint was before `due`)
        let kv = Log::writer_with_floor(store.clone(), 0).await.unwrap();
        kv.write(&[pair("a", "1"), pair("b", "2")], &[]).await.unwrap();
        kv.write(&[pair("a", "3")], &[]).await.unwrap();
        kv.checkpoint().await.unwrap();
        assert_eq!(log_seqs(&store).await, Vec::<u64>::new(), "the logs the snapshot covers are gone");
        // (c's value is 64 bytes: with one byte its log is 23 bytes, below the 32-byte snapshot, and the checkpoint
        // below would fold nothing)
        kv.write(&[pair("c", &"4".repeat(64))], &["b".to_string()]).await.unwrap();
        assert_eq!(log_seqs(&store).await, vec![3]);
        drop(kv);

        let again = Log::writer_with_floor(store.clone(), 0).await.unwrap();
        assert_eq!(all(&again).await, BTreeMap::from([("a".to_string(), Bytes::from("3")), ("c".to_string(), Bytes::from("4".repeat(64)))]));
        again.checkpoint().await.unwrap();
        assert_eq!(log_seqs(&store).await, Vec::<u64>::new());
        drop(again);

        // (the snapshot counts as seen: with no log after it, the next batch is one past it, not 1)
        let third = Log::writer(store.clone()).await.unwrap();
        third.write(&[pair("d", "5")], &[]).await.unwrap();
        assert_eq!(log_seqs(&store).await, vec![4]);
        assert_eq!(third.get("a", false).await.unwrap(), Some(Bytes::from("3")));
    }

    #[tokio::test]
    async fn a_second_writer_is_fenced_out() {
        let store = lake();
        let first = Log::writer(store.clone()).await.unwrap();
        let second = Log::writer(store.clone()).await.unwrap();
        first.write(&[pair("a", "1")], &[]).await.unwrap();
        let err = second.write(&[pair("b", "2")], &[]).await.err().expect("the second writer's batch collides");
        assert!(err.closed());
        assert_eq!(second.get("b", false).await.unwrap(), None, "a refused batch changes nothing");
        first.write(&[pair("c", "3")], &[]).await.unwrap();
        assert_eq!(all(&first).await.len(), 2);
    }

    #[tokio::test]
    async fn a_writer_a_newer_one_checkpointed_past_is_fenced_out() {
        let store = lake();
        let stale = Log::writer(store.clone()).await.unwrap();
        let newer = Log::writer_with_floor(store.clone(), 0).await.unwrap();
        newer.write(&[pair("a", "1")], &[]).await.unwrap();
        newer.checkpoint().await.unwrap();
        // (the newer writer's log 1 is gone, so the stale one creates it again, below the newer snapshot)
        assert!(stale.write(&[pair("b", "2")], &[]).await.err().expect("a batch below a newer snapshot").closed());
        // (nothing to fold: fencing shows at its next write. Its refused batch was never counted, so nothing is due, and
        // no store call is made)
        assert!(stale.checkpoint().await.is_ok());
        let again = Log::writer_with_floor(store.clone(), 0).await.unwrap();
        assert_eq!(all(&again).await, BTreeMap::from([("a".to_string(), Bytes::from("1"))]));
        // (16 bytes, not 1: a one-byte batch is 18 bytes, below the 22-byte snapshot, so this checkpoint would fold nothing)
        again.write(&[pair("c", &"3".repeat(16))], &[]).await.unwrap();
        again.checkpoint().await.unwrap();
        assert_eq!(list(&store, SNAPSHOTS).await.unwrap().into_iter().map(|(seq, _)| seq).collect::<Vec<_>>(), vec![2], "the snapshots below the last are gone");
    }

    #[tokio::test]
    async fn a_reader_skips_a_fenced_writers_log() {
        let store = lake();
        let newer = Log::writer_with_floor(store.clone(), 0).await.unwrap();
        for i in 1..=5 {
            newer.write(&[pair(&format!("k{i}"), "v")], &[]).await.unwrap();
        }
        let reader = Log::reader(store.clone(), Duration::ZERO).await.unwrap(); // (last 5)
        let stale = Log::writer(store.clone()).await.unwrap(); // (next 6)
        newer.write(&[pair("n", "6")], &[]).await.unwrap();
        newer.checkpoint().await.unwrap(); // (a snapshot at 6: log 6 is gone)
        assert!(stale.write(&[pair("s", "x")], &[]).await.err().expect("a batch below a newer snapshot").closed());
        assert_eq!(log_seqs(&store).await, vec![6], "the fenced writer's log 6 is left behind");
        assert_eq!(reader.get("n", false).await.unwrap(), Some(Bytes::from("6")));
        assert_eq!(reader.get("s", false).await.unwrap(), None, "a fenced writer's batch is not in the catalog");
    }

    #[tokio::test]
    async fn a_checkpoint_waits_for_the_log_to_grow() {
        let store = lake();
        let kv = Log::writer(store.clone()).await.unwrap();
        kv.write(&[pair("a", "1")], &[]).await.unwrap();
        kv.write(&[pair("b", "2")], &[]).await.unwrap();
        kv.write(&[pair("c", "3")], &[]).await.unwrap();
        kv.checkpoint().await.unwrap();
        assert_eq!(snapshots(&store).await, 0, "three small logs are below the floor: nothing is folded");
        assert_eq!(log_seqs(&store).await, vec![1, 2, 3]);
        kv.checkpoint().await.unwrap();
        assert_eq!(snapshots(&store).await, 0, "still below the floor: a second checkpoint changes nothing");
        assert_eq!(log_seqs(&store).await, vec![1, 2, 3]);

        kv.write(&[pair("d", &"4".repeat(FLOOR as usize))], &[]).await.unwrap();
        kv.checkpoint().await.unwrap();
        assert_eq!(snapshots(&store).await, 1);
        assert_eq!(log_seqs(&store).await, Vec::<u64>::new());
        drop(kv);

        let again = Log::writer(store.clone()).await.unwrap();
        assert_eq!(all(&again).await.keys().collect::<Vec<_>>(), ["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn a_checkpoint_after_many_logs() {
        let store = lake();
        // (no byte floor: only the number of logs makes a checkpoint due)
        let kv = Log::writer_with_floor(store.clone(), u64::MAX).await.unwrap();
        for i in 0..MOST_LOGS - 1 {
            kv.write(&[pair(&format!("k{i}"), "v")], &[]).await.unwrap();
        }
        kv.checkpoint().await.unwrap();
        assert_eq!(snapshots(&store).await, 0, "one log short of MOST_LOGS: nothing is folded");
        assert_eq!(log_seqs(&store).await.len() as u64, MOST_LOGS - 1);
        kv.write(&[pair("last", "v")], &[]).await.unwrap();
        kv.checkpoint().await.unwrap();
        assert_eq!(snapshots(&store).await, 1);
        assert_eq!(log_seqs(&store).await, Vec::<u64>::new());
    }

    #[tokio::test]
    async fn a_reader_catches_up() {
        let store = lake();
        let kv = Log::writer_with_floor(store.clone(), 0).await.unwrap();
        kv.write(&[pair("a", "1")], &[]).await.unwrap();
        let reader = Log::reader(store.clone(), Duration::ZERO).await.unwrap();
        assert_eq!(reader.get("a", false).await.unwrap(), Some(Bytes::from("1")));
        kv.write(&[pair("b", "2")], &[]).await.unwrap();
        assert_eq!(reader.get("b", false).await.unwrap(), Some(Bytes::from("2")), "the batch above it is read");
        kv.write(&[pair("c", "3")], &[]).await.unwrap();
        kv.checkpoint().await.unwrap(); // (a snapshot of a, b and c: their logs are gone)
        kv.write(&[pair("d", "4")], &["a".to_string()]).await.unwrap();
        // (the reader's last is below the snapshot, so it replays the whole catalog)
        assert_eq!(reader.get("c", false).await.unwrap(), Some(Bytes::from("3")));
        assert_eq!(reader.get("d", false).await.unwrap(), Some(Bytes::from("4")));
        assert_eq!(reader.get("a", false).await.unwrap(), None, "a key deleted in a later batch stays deleted");

        // (opened before the write below: a reader that polls once an hour does not see it yet)
        let slow = Log::reader(store.clone(), Duration::from_secs(3600)).await.unwrap();
        kv.write(&[pair("e", "5")], &[]).await.unwrap();
        assert_eq!(slow.get("e", false).await.unwrap(), None);
        assert_eq!(reader.get("e", false).await.unwrap(), Some(Bytes::from("5")));
    }

    #[tokio::test]
    async fn a_reader_sees_what_was_written_and_takes_no_writes() {
        let store = lake();
        let kv = Log::writer(store.clone()).await.unwrap();
        kv.write(&[pair("a", "1")], &[]).await.unwrap();
        let reader = Log::reader(store.clone(), Duration::ZERO).await.unwrap();
        assert!(!reader.is_writer());
        assert_eq!(reader.get("a", false).await.unwrap(), Some(Bytes::from("1")));
        let err = reader.write(&[pair("b", "2")], &[]).await.err().expect("a reader refuses a write");
        assert!(!err.closed());
        assert_eq!(err.to_string(), "a reader takes no writes");
        assert_eq!(reader.checkpoint().await.err().expect("a reader refuses a checkpoint").to_string(), "a reader takes no writes");
    }
}
