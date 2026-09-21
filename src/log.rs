//! Ingest. Every node batches the appends it receives; the leader only puts them in order.
//!
//! * The batcher (every node): each flush window, every producer's batch becomes one Arrow IPC
//!   stream (ZSTD) at a byte range of one buffer, followed by the rows the inline views derive
//!   from them. Up to 64 KB travels inside the commit request; a bigger buffer is written to
//!   object storage by this node itself, so the data path grows with the number of nodes.
//! * The sequencer (the leader): skips retried batches (each producer's last seq: exactly-once),
//!   numbers every flush as a log segment and commits them all, with the producers' seqs, in ONE
//!   catalog write. Producers are acked only after that.
use crate::cluster::http;
use crate::store::*;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::ipc::{reader::StreamReader, CompressionType};
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

/// Small flushes ride inside the commit; big ones (high volume) are objects. With replicated
/// acks up to 1 MB rides inside: an object write first would cost the latency they save.
fn inline_bytes() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let env = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<usize>().ok());
    *N.get_or_init(|| env("PONDRA_INLINE_KB").unwrap_or(if env("PONDRA_REPLICAS").unwrap_or(1) > 1 { 1024 } else { 64 }) << 10)
}
const FLUSHES_IN_FLIGHT: usize = 4; // per node: object-store latency overlaps instead of adding up
const COMMITS_IN_FLIGHT: usize = 4; // leader: catalog writes not yet committed

/// Who sent a batch: its producer and sequence number (and, optionally, the seq it expects to
/// follow: a compare-and-swap that streaming tasks use).
#[derive(Serialize, Deserialize, Clone)]
pub struct Src {
    pub producer: String,
    pub seq: u64,
    pub prev: Option<u64>,
}

/// A byte range of a flush holding one table's rows: a producer's batch, or a view's output.
#[derive(Serialize, Deserialize, Clone)]
pub struct Part {
    pub table: String,
    pub off: u64,
    pub len: u64,
    pub rows: u64,
    pub src: Option<Src>, // None = derived by a view
}

/// One node's flush: its parts, whose bytes are at `path` (written by the node) or in `data`.
#[derive(Serialize, Deserialize, Default)]
pub struct Flush {
    pub path: String,
    pub parts: Vec<Part>,
    #[serde(skip)]
    pub data: Bytes,
}

#[derive(Serialize, Deserialize, Clone, Copy, Default)]
pub struct Ack {
    pub seg: u64,
    pub duplicate: bool, // this seq was already committed: nothing to do
    pub conflict: bool,  // `prev` didn't match: someone else committed first; re-read and retry
}

/// The sequencer's answer: an ack per part, or (rarely) "these parts are retries of committed
/// batches: send the flush again without them", when views derived rows from them.
#[derive(Serialize, Deserialize)]
pub enum Outcome {
    Acks(Vec<Ack>),
    Retry(Vec<(usize, Ack)>),
}

// ---------------------------------------------------------------- the batcher (every node)

pub struct Append {
    pub table: String,
    pub src: Src,
    pub batch: RecordBatch, // may be empty: then only the producer's seq advances
    pub ack: oneshot::Sender<Result<Ack, String>>,
}

pub struct Log {
    tx: mpsc::Sender<Append>,
}

/// Where flushes go: the sequencer in this process (the leader), or the leader over HTTP.
pub enum To {
    Local(Arc<Sequencer>),
    Leader(String),
}

impl Log {
    pub fn start(lake: Arc<Lake>, flush: Duration, to: To) -> Log {
        let (tx, mut rx) = mpsc::channel::<Append>(100_000);
        let (to, slots) = (Arc::new(to), Arc::new(tokio::sync::Semaphore::new(FLUSHES_IN_FLIGHT)));
        tokio::spawn(async move {
            let mut last = tokio::time::Instant::now();
            while let Some(first) = rx.recv().await {
                // While all flush slots are busy, appends queue up; then they all go in one flush.
                let slot = slots.clone().acquire_owned().await.expect("never closed");
                let (deadline, mut pending) = (last + flush, vec![first]);
                while let Ok(Some(a)) = tokio::time::timeout_at(deadline, rx.recv()).await {
                    pending.push(a);
                }
                last = tokio::time::Instant::now();
                let (lake, to) = (lake.clone(), to.clone());
                tokio::spawn(async move {
                    send(&lake, &to, pending).await;
                    drop(slot);
                });
            }
        });
        Log { tx }
    }

    /// Append and wait for the ack (the write is committed).
    pub async fn append(&self, table: String, src: Src, batch: RecordBatch) -> Result<Ack> {
        let (ack, rx) = oneshot::channel();
        self.tx.send(Append { table, src, batch, ack }).await.map_err(|_| anyhow!("log closed"))?;
        rx.await?.map_err(|e| anyhow!(e))
    }
}

async fn send(lake: &Lake, to: &To, mut pending: Vec<Append>) {
    while !pending.is_empty() {
        let outcome = async {
            let f = pack(lake, &pending).await?;
            match to {
                To::Local(seq) => seq.submit(f).await,
                To::Leader(addr) => Ok(http().post(format!("http://{addr}/cluster/commit")).body(encode_flush(&f)?).send().await?.error_for_status()?.json().await?),
            }
        };
        match outcome.await {
            Ok(Outcome::Acks(acks)) => {
                for (a, ack) in pending.drain(..).zip(acks) {
                    let _ = a.ack.send(Ok(ack));
                }
            }
            Ok(Outcome::Retry(retried)) => {
                for (i, ack) in retried.into_iter().rev() {
                    let _ = pending.remove(i).ack.send(Ok(ack)); // and go again with the rest
                }
            }
            Err(e) => {
                for a in pending.drain(..) {
                    let _ = a.ack.send(Err(format!("{e:#}"))); // the producer retries, maybe elsewhere
                }
            }
        }
    }
}

/// Encode the producers' batches and their views' output into one flush.
pub async fn pack(lake: &Lake, pending: &[Append]) -> Result<Flush> {
    let (mut data, mut parts) = (vec![], vec![]);
    let mut add = |table: &str, batch: &RecordBatch, src: Option<Src>| -> Result<()> {
        let off = data.len() as u64;
        if batch.num_rows() > 0 {
            data.extend(encode_ipc(&[batch.clone()])?);
        }
        parts.push(Part { table: table.into(), off, len: data.len() as u64 - off, rows: batch.num_rows() as u64, src });
        Ok(())
    };
    let mut by_table: BTreeMap<String, Vec<RecordBatch>> = BTreeMap::new();
    for a in pending {
        add(&a.table, &a.batch, Some(a.src.clone()))?;
        by_table.entry(a.table.clone()).or_default().push(a.batch.clone());
    }
    for (table, batch) in crate::views::derive(lake, &by_table).await? {
        add(&table, &batch, None)?;
    }
    let mut f = Flush { parts, ..Default::default() };
    if data.len() > inline_bytes() {
        f.path = format!("log/{:015}-{}.seg", now_ms(), uuid::Uuid::new_v4());
        lake.put(&f.path, data).await?;
        maybe_crash("after_seg_put");
    } else {
        f.data = data.into();
    }
    Ok(f)
}

/// The body of POST /cluster/commit: u32 header length | JSON header | inline data.
pub fn encode_flush(f: &Flush) -> Result<Vec<u8>> {
    let head = serde_json::to_vec(f)?;
    Ok([&(head.len() as u32).to_le_bytes()[..], &head, &f.data].concat())
}

pub fn decode_flush(body: Bytes) -> Result<Flush> {
    let n = u32::from_le_bytes(body.get(..4).ok_or_else(|| anyhow!("empty flush"))?.try_into()?) as usize;
    let mut f: Flush = serde_json::from_slice(&body[4..4 + n])?;
    f.data = body.slice(4 + n..);
    Ok(f)
}

// ---------------------------------------------------------------- the sequencer (leader)

pub struct Sequencer {
    tx: mpsc::Sender<(Flush, oneshot::Sender<Outcome>)>,
    pub commit_ms: Arc<Mutex<Vec<f64>>>, // catalog commit latencies, for /stats
}

impl Sequencer {
    /// `max_backlog`: while more rows than this wait in the log to be tiered, commits pause
    /// (so producers slow down to what the cluster sustains, instead of memory growing).
    pub async fn start(lake: Arc<Lake>, max_backlog: Option<u64>) -> Result<Arc<Sequencer>> {
        let mut next: u64 = lake.cat.get("n").await?.unwrap_or(1);
        for (key, meta) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
            let rows = crate::tier::backlog(&lake, meta.tiered, None).await?.get(&key[2..]).copied().unwrap_or(0);
            lake.backlog.fetch_add(rows, Ordering::Relaxed);
        }
        let (tx, mut rx) = mpsc::channel::<(Flush, oneshot::Sender<Outcome>)>(10_000);
        let commit_ms = Arc::new(Mutex::new(vec![]));
        let ms = commit_ms.clone();
        tokio::spawn(async move {
            let mut last_seq = HashMap::new(); // producer -> last committed seq (cache of p/ keys)
            let in_flight = Arc::new(tokio::sync::Semaphore::new(COMMITS_IN_FLIGHT));
            while let Some(first) = rx.recv().await {
                let slot = in_flight.clone().acquire_owned().await.expect("never closed");
                while max_backlog.is_some_and(|m| lake.backlog.load(Ordering::Relaxed) > m) {
                    tokio::time::sleep(Duration::from_millis(20)).await; // backpressure: let tiering catch up
                }
                // Everything that queued up during the previous commit goes into this one.
                let mut batch = vec![first];
                while let Ok(f) = rx.try_recv() {
                    batch.push(f);
                }
                if let Err(e) = commit(&lake, &mut next, &mut last_seq, batch, &ms, slot).await {
                    // Committed or not, we can't tell: restart and reload the state from the catalog.
                    eprintln!("sequencer failed: {e:#}");
                    crate::cluster::restart();
                }
            }
        });
        Ok(Arc::new(Sequencer { tx, commit_ms }))
    }

    pub async fn submit(&self, f: Flush) -> Result<Outcome> {
        let (reply, rx) = oneshot::channel();
        self.tx.send((f, reply)).await.map_err(|_| anyhow!("sequencer stopped"))?;
        Ok(rx.await?)
    }
}

/// Sequence a batch of flushes and write them as one catalog commit. Without waiting for it to
/// be committed, the next batch can follow; acks go out once this one is.
async fn commit(lake: &Arc<Lake>, next: &mut u64, last_seq: &mut HashMap<String, u64>, batch: Vec<(Flush, oneshot::Sender<Outcome>)>, ms: &Arc<Mutex<Vec<f64>>>, slot: tokio::sync::OwnedSemaphorePermit) -> Result<()> {
    let (mut puts, mut seqs, mut replies) = (vec![], HashMap::<String, u64>::new(), vec![]);
    let (mut inline, mut inline_parts) = (vec![], BTreeMap::<String, Vec<(u64, u64, u64)>>::new()); // all inline flushes: one segment
    for (f, reply) in batch {
        // 1. Skip retries of committed batches, and batches whose `prev` no longer holds.
        let (mut acks, mut bad, mut mine) = (vec![Ack::default(); f.parts.len()], vec![], HashMap::new());
        for (i, src) in f.parts.iter().enumerate().filter_map(|(i, p)| Some((i, p.src.as_ref()?))) {
            let last = match mine.get(&src.producer).or(seqs.get(&src.producer)).or(last_seq.get(&src.producer)) {
                Some(s) => *s,
                None => lake.cat.get(&producer_key(&src.producer)).await?.unwrap_or(0),
            };
            if src.seq <= last {
                acks[i].duplicate = true;
            } else if src.prev.is_some_and(|p| p != last) {
                acks[i].conflict = true;
            } else {
                mine.insert(src.producer.clone(), src.seq);
                continue;
            }
            bad.push(i);
        }
        if !bad.is_empty() && f.parts.iter().any(|p| p.src.is_none()) {
            // Views derived rows from the skipped batches too: the node must redo the flush.
            replies.push((reply, Outcome::Retry(bad.iter().map(|&i| (i, acks[i])).collect())));
            continue;
        }
        seqs.extend(mine);
        // 2. An object flush becomes the next segment; inline flushes all go into one (below).
        let base = if f.path.is_empty() { inline.len() as u64 } else { 0 };
        let mut parts: BTreeMap<String, Vec<(u64, u64, u64)>> = BTreeMap::new();
        for (_, p) in f.parts.iter().enumerate().filter(|(i, p)| p.rows > 0 && !bad.contains(i)) {
            parts.entry(p.table.clone()).or_default().push((base + p.off, p.len, p.rows));
            lake.backlog.fetch_add(p.rows, Ordering::Relaxed);
        }
        let seg = match (parts.is_empty(), f.path.is_empty()) {
            (true, _) => 0,
            (false, true) => {
                inline.extend_from_slice(&f.data);
                parts.into_iter().for_each(|(t, p)| inline_parts.entry(t).or_default().extend(p));
                u64::MAX // the inline segment's number, set below
            }
            (false, false) => {
                *next += 1;
                puts.push((seg_key(*next - 1), json(&Segment { path: f.path, parts, ts_ms: now_ms() })));
                *next - 1
            }
        };
        acks.iter_mut().enumerate().filter(|(i, _)| !bad.contains(i)).for_each(|(_, a)| a.seg = seg);
        replies.push((reply, Outcome::Acks(acks)));
    }
    if !inline_parts.is_empty() {
        let seg = *next;
        *next += 1;
        puts.push((data_key(seg), inline));
        puts.push((seg_key(seg), json(&Segment { path: String::new(), parts: inline_parts, ts_ms: now_ms() })));
        for (_, o) in replies.iter_mut() {
            if let Outcome::Acks(acks) = o {
                acks.iter_mut().filter(|a| a.seg == u64::MAX).for_each(|a| a.seg = seg);
            }
        }
    }
    // 3. One catalog write for everything; acks once it's committed (see `Lake::commits`).
    puts.extend(seqs.iter().map(|(p, s)| (producer_key(p), json(s))));
    puts.push(("n".into(), json(next)));
    let (t0, durable) = (Instant::now(), lake.cat.write(puts, &[]).await?);
    last_seq.extend(seqs);
    let (lake, ms, hwm) = (lake.clone(), ms.clone(), *next - 1);
    tokio::spawn(async move {
        if durable.await.is_ok() {
            let mut ms = ms.lock().unwrap();
            if ms.len() >= 10_000 {
                ms.drain(..5_000); // keep recent samples only
            }
            ms.push(t0.elapsed().as_secs_f64() * 1e3);
            drop(ms);
            maybe_crash("after_commit");
            lake.advance(hwm);
            for (reply, outcome) in replies {
                let _ = reply.send(outcome);
            }
        }
        drop(slot);
    });
    Ok(())
}

// ---------------------------------------------------------------- encoding

/// Batches as one ZSTD-compressed Arrow IPC stream.
pub fn encode_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = vec![];
    let opts = IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;
    let mut w = StreamWriter::try_new_with_options(&mut buf, &batches[0].schema(), opts)?;
    for b in batches {
        w.write(b)?;
    }
    w.finish()?;
    drop(w);
    Ok(buf)
}

/// The batches of one Arrow IPC stream.
pub fn decode(ipc: &[u8]) -> Result<Vec<RecordBatch>> {
    Ok(StreamReader::try_new(ipc, None)?.collect::<Result<_, _>>()?)
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}
