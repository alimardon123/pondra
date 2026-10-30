//! A page's catalog store: the keys in memory, logged into the lake's own object store (designs/page-same-core.md P2).
//! Each write is one object, `page-catalog/log/{seq}`, holding one batch. A checkpoint writes
//! `page-catalog/snapshot/{seq}`, every key and value as of its last batch, then deletes the logs and snapshots below it.
//! The lake's store holds all of it, and every object is created, never overwritten: a second writer on the same lake
//! (another tab) finds a log it cannot create, or a snapshot above its batch, and is fenced out, as SlateDB fences one.
use super::{Error, Kv, Written};
use crate::store::Store;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};

const LOGS: &str = "page-catalog/log";
const SNAPSHOTS: &str = "page-catalog/snapshot";

/// The page's catalog: its keys in memory and the lake's objects that hold them. A reader opens the same objects
/// and takes no writes.
pub(crate) struct Log {
    store: Store,
    keys: Mutex<BTreeMap<String, Bytes>>, // (never held across an await)
    next: AtomicU64,                      // the next batch's sequence number; moved under `writes` only
    writer: bool,
    writes: tokio::sync::Mutex<()>, // one write or checkpoint at a time
}

impl Log {
    /// The catalog as a writer or a reader opens it: the snapshot, then the logs above it, in order.
    pub(crate) async fn open(store: Store, writer: bool) -> Result<Log, Error> {
        let (last, keys) = loop {
            // (`replay` says None when a checkpoint deleted a log it had listed: it lists again)
            if let Some(state) = replay(&store).await? {
                break state;
            }
        };
        Ok(Log { store, keys: Mutex::new(keys), next: AtomicU64::new(last + 1), writer, writes: tokio::sync::Mutex::new(()) })
    }
}

/// The catalog as the lake's objects hold it now: the last sequence number seen (the snapshot's counts), and the keys.
/// The logs are listed before the snapshots, and a checkpoint writes its snapshot before it deletes, so a checkpoint
/// between the two listings can only add a snapshot to what the first saw, never leave a gap in the logs. None: an
/// object listed was deleted since, so list again.
async fn replay(store: &Store) -> Result<Option<(u64, BTreeMap<String, Bytes>)>, Error> {
    let logs = list(store, LOGS).await?;
    let (covered, mut keys) = match list(store, SNAPSHOTS).await?.pop() {
        Some((covered, path)) => {
            let Some(bytes) = read(store, &path).await? else { return Ok(None) };
            let (said, pairs) = decode_snapshot(&bytes).ok_or_else(|| damaged(path.as_ref()))?;
            if said != covered {
                return Err(damaged(path.as_ref()));
            }
            (covered, pairs.into_iter().collect())
        }
        None => (0, BTreeMap::new()),
    };
    let mut last = covered;
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
    }
    Ok(Some((last, keys)))
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

// A page's reads are the map's. `remote` changes nothing: every batch is in the store once `write` returns.
#[async_trait::async_trait]
impl Kv for Log {
    async fn get(&self, key: &str, _remote: bool) -> Result<Option<Bytes>, Error> {
        Ok(self.keys.lock().unwrap_or_else(PoisonError::into_inner).get(key).cloned())
    }

    async fn scan(&self, range: std::ops::Range<Vec<u8>>, _remote: bool) -> Result<BTreeMap<String, Bytes>, Error> {
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
        Ok(Arc::new(Stored))
    }

    async fn checkpoint(&self) -> Result<(), Error> {
        if !self.writer {
            return Err(Error::new(false, "a reader takes no writes"));
        }
        let _turn = self.writes.lock().await;
        // The last batch applied: the snapshot covers every batch so far (0 before the first).
        let covered = self.next.load(Relaxed) - 1;
        let snapshot = encode_snapshot(covered, &self.keys.lock().unwrap_or_else(PoisonError::into_inner))?;
        let create = PutOptions { mode: PutMode::Create, ..Default::default() };
        match self.store.put_opts(&Path::from(format!("{SNAPSHOTS}/{covered:020}")), PutPayload::from(snapshot), create).await {
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

    #[tokio::test]
    async fn put_get_scan_and_delete() {
        let kv = Log::open(lake(), true).await.unwrap();
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
        let kv = Log::open(store.clone(), true).await.unwrap();
        kv.write(&[pair("a", "1")], &[]).await.unwrap();
        kv.write(&[pair("b", "2"), pair("a", "3")], &[]).await.unwrap();
        kv.write(&[], &["b".to_string()]).await.unwrap();
        drop(kv);
        let again = Log::open(store.clone(), true).await.unwrap();
        assert_eq!(all(&again).await, BTreeMap::from([("a".to_string(), Bytes::from("3"))]));
        again.write(&[pair("c", "4")], &[]).await.unwrap();
        assert_eq!(log_seqs(&store).await, vec![1, 2, 3, 4], "the next batch follows the last one replayed");
    }

    #[tokio::test]
    async fn checkpoint_then_writes_then_reopen() {
        let store = lake();
        let kv = Log::open(store.clone(), true).await.unwrap();
        kv.write(&[pair("a", "1"), pair("b", "2")], &[]).await.unwrap();
        kv.write(&[pair("a", "3")], &[]).await.unwrap();
        kv.checkpoint().await.unwrap();
        assert_eq!(log_seqs(&store).await, Vec::<u64>::new(), "the logs the snapshot covers are gone");
        kv.write(&[pair("c", "4")], &["b".to_string()]).await.unwrap();
        assert_eq!(log_seqs(&store).await, vec![3]);
        drop(kv);

        let again = Log::open(store.clone(), true).await.unwrap();
        assert_eq!(all(&again).await, BTreeMap::from([("a".to_string(), Bytes::from("3")), ("c".to_string(), Bytes::from("4"))]));
        again.checkpoint().await.unwrap();
        assert_eq!(log_seqs(&store).await, Vec::<u64>::new());
        drop(again);

        // (the snapshot counts as seen: with no log after it, the next batch is one past it, not 1)
        let third = Log::open(store.clone(), true).await.unwrap();
        third.write(&[pair("d", "5")], &[]).await.unwrap();
        assert_eq!(log_seqs(&store).await, vec![4]);
        assert_eq!(third.get("a", false).await.unwrap(), Some(Bytes::from("3")));
    }

    #[tokio::test]
    async fn a_second_writer_is_fenced_out() {
        let store = lake();
        let first = Log::open(store.clone(), true).await.unwrap();
        let second = Log::open(store.clone(), true).await.unwrap();
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
        let stale = Log::open(store.clone(), true).await.unwrap();
        let newer = Log::open(store.clone(), true).await.unwrap();
        newer.write(&[pair("a", "1")], &[]).await.unwrap();
        newer.checkpoint().await.unwrap();
        // (the newer writer's log 1 is gone, so the stale one creates it again, below the newer snapshot)
        assert!(stale.write(&[pair("b", "2")], &[]).await.err().expect("a batch below a newer snapshot").closed());
        assert!(stale.checkpoint().await.err().expect("a snapshot below a newer one").closed());
        let again = Log::open(store.clone(), true).await.unwrap();
        assert_eq!(all(&again).await, BTreeMap::from([("a".to_string(), Bytes::from("1"))]));
        again.write(&[pair("c", "3")], &[]).await.unwrap();
        again.checkpoint().await.unwrap();
        assert_eq!(list(&store, SNAPSHOTS).await.unwrap().into_iter().map(|(seq, _)| seq).collect::<Vec<_>>(), vec![2], "the snapshots below the last are gone");
    }

    #[tokio::test]
    async fn a_reader_sees_what_was_written_and_takes_no_writes() {
        let store = lake();
        let kv = Log::open(store.clone(), true).await.unwrap();
        kv.write(&[pair("a", "1")], &[]).await.unwrap();
        let reader = Log::open(store.clone(), false).await.unwrap();
        assert!(!reader.is_writer());
        assert_eq!(reader.get("a", false).await.unwrap(), Some(Bytes::from("1")));
        let err = reader.write(&[pair("b", "2")], &[]).await.err().expect("a reader refuses a write");
        assert!(!err.closed());
        assert_eq!(err.to_string(), "a reader takes no writes");
        assert_eq!(reader.checkpoint().await.err().expect("a reader refuses a checkpoint").to_string(), "a reader takes no writes");
    }
}
