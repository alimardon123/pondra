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

/// One node's flush: its parts, whose bytes are at `path` (written by the node) or in `data`. Or,
/// with `reserve` or `block`, no rows: a request for that many commit numbers, whose rows a writer
/// stamps itself (the number as their `_version`), and for a block of row ids (`sys.rs`).
#[derive(Serialize, Deserialize, Default)]
pub struct Flush {
    pub path: String,
    pub parts: Vec<Part>,
    #[serde(skip)]
    pub data: Bytes,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub reserve: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub block: bool,
    /// A file commit (the leader's own, under the lake's lock: `adopt::file`): files into tables
    /// and out of them, in this commit, as its segment names them (`Segment::files`).
    #[serde(skip)]
    pub filed: Vec<Filing>,
}

/// One table's part of a file commit: the table as it is to be, the files it takes out already
/// taken, and the files it takes in (whose lineage, if they need one, the sequencer completes:
/// their rows' `_version` and times are this commit's).
pub struct Filing {
    pub table: String,
    pub meta: TableMeta,
    pub added: Vec<DataFile>,
    pub removed: Vec<DataFile>,
    pub deleted: Vec<(DataFile, Vec<crate::scan::Delete>)>,
}

fn is_zero(n: &u64) -> bool { *n == 0 }

/// A log row's place (`_ord`, a Kafka offset): its segment's number, then its position among the
/// table's rows there. A segment holding more of one table's rows than the low bits count takes
/// the numbers after it too (`commit`), so the places never meet, and 40 bits of segment numbers
/// last 34 years at 1,000 commits a second (ADR-029 §11).
pub const ORD_BITS: u32 = 24;

pub fn ord(seg: u64, row: u64) -> u64 { (seg << ORD_BITS) + row }

#[derive(Serialize, Deserialize, Clone, Copy, Default)]
pub struct Ack {
    pub seg: u64,
    pub duplicate: bool, // this seq was already committed: nothing to do
    pub conflict: bool,  // `prev` didn't match: someone else committed first; re-read and retry
    #[serde(default)]
    pub row: u64, // where the rows start among the table's rows in `seg` (their `_ord`: `ord(seg, row)`)
    #[serde(default)]
    pub ms: u64, // a reservation's commit time
    #[serde(default)]
    pub block: u64, // a reservation's block of row ids
}

/// A reservation: a commit number of its own and its time (the `_version` and times of rows a
/// writer stamps itself), and a block of row ids for them (`sys.rs`).
#[derive(Clone, Copy, Debug)]
pub struct Reserved {
    pub version: u64,
    pub ms: u64,
    pub block: u64,
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
    lake: Arc<Lake>,
}

/// A producer's batches are numbered from 1: its last seq is 0 until one commits.
pub const SEQ_FROM_1: &str = "a producer's batches count from seq=1 (seq 0 would be taken for a batch already written)";

/// Where flushes go: the sequencer in this process (the leader), or the leader over HTTP.
#[derive(Clone)]
pub enum To {
    Local(Arc<Sequencer>),
    Leader(String),
}

impl To {
    /// A commit number of its own, its time, and a block of row ids (`Sequencer::reserve`).
    pub async fn reserve(&self) -> Result<Reserved> { self.ask(Flush { reserve: 1, block: true, ..Default::default() }).await }

    /// A block of row ids only.
    pub async fn block(&self) -> Result<u64> { Ok(self.ask(Flush { block: true, ..Default::default() }).await?.block) }

    async fn ask(&self, f: Flush) -> Result<Reserved> {
        reserved(match self {
            To::Local(seq) => seq.submit(f).await?,
            To::Leader(addr) => http().post(format!("http://{addr}/cluster/commit")).body(encode_flush(&f)?).send().await?.error_for_status()?.json().await?,
        })
    }
}

/// A reservation's answer.
fn reserved(o: Outcome) -> Result<Reserved> {
    match o {
        Outcome::Acks(a) if !a.is_empty() => Ok(Reserved { version: a[0].seg, ms: a[0].ms, block: a[0].block }),
        _ => Err(anyhow!("no reservation")),
    }
}

/// `rows` new row ids for rows this process stamps (a new block from `to` when its own runs out).
pub async fn ids(lake: &Lake, to: &To, rows: u64) -> Result<i64> {
    loop {
        if let Some(first) = lake.ids.take(rows) {
            return Ok(first);
        }
        lake.ids.refill(to.block().await?);
    }
}

/// Every row of these appends with its `_row_id`, as they go into the log (`sys.rs`).
async fn stamp(lake: &Lake, to: &To, pending: &mut [Append]) -> Result<()> {
    for a in pending.iter_mut().filter(|a| a.batch.num_rows() > 0) {
        a.batch = crate::sys::stamp(&a.batch, ids(lake, to, a.batch.num_rows() as u64).await?)?;
    }
    Ok(())
}

impl Log {
    pub fn start(lake: Arc<Lake>, flush: Duration, to: To) -> Log {
        let (tx, mut rx) = mpsc::channel::<Append>(100_000);
        let me = lake.clone();
        let (to, slots) = (Arc::new(to), Arc::new(tokio::sync::Semaphore::new(FLUSHES_IN_FLIGHT)));
        tokio::spawn(async move {
            let (mut last, mut turn) = (tokio::time::Instant::now(), None);
            while let Some(first) = rx.recv().await {
                // While all flush slots are busy, appends queue up; then they all go in one flush.
                let slot = slots.clone().acquire_owned().await.expect("never closed");
                let (deadline, mut pending) = (last + flush, vec![first]);
                while let Ok(Some(a)) = tokio::time::timeout_at(deadline, rx.recv()).await {
                    pending.push(a);
                }
                last = tokio::time::Instant::now();
                let (lake, to) = (lake.clone(), to.clone());
                let (done, next) = oneshot::channel();
                let turn = std::mem::replace(&mut turn, Some(next));
                tokio::spawn(async move {
                    send(&lake, &to, pending, turn, done).await;
                    drop(slot);
                });
            }
        });
        Log { tx, lake: me }
    }

    /// Append and wait for the ack (the write is committed).
    pub async fn append(&self, table: String, src: Src, batch: RecordBatch) -> Result<Ack> {
        self.queue(table, src, batch).await?.await
    }

    /// Queue rows now and get their ack later: rows queued one after another commit in that
    /// order, so a client can have several batches in flight (the Kafka protocol does).
    pub async fn queue(&self, table: String, src: Src, batch: RecordBatch) -> Result<impl std::future::Future<Output = Result<Ack>> + use<>> {
        if let Some(m) = self.lake.cat.get::<TableMeta>(&table_key(&table)).await? {
            crate::defaults::check(&m, &table, &batch)?; // (NOT NULL, whichever door the rows came in by)
        }
        let (ack, rx) = oneshot::channel();
        crate::metrics::add(&crate::metrics::ROWS_IN, batch.num_rows() as u64);
        self.tx.send(Append { table, src, batch, ack }).await.map_err(|_| anyhow!("log closed"))?;
        Ok(async move { rx.await?.map_err(|e| anyhow!(e)) })
    }
}

/// One flush, to the sequencer. Flushes are encoded side by side, but reach this node's
/// sequencer in the order they were cut (`turn`: the previous one is there; `done`: this one
/// is), so a producer's pipelined batches commit in order.
async fn send(lake: &Lake, to: &To, mut pending: Vec<Append>, mut turn: Option<oneshot::Receiver<()>>, done: oneshot::Sender<()>) {
    let mut done = Some(done);
    while !pending.is_empty() {
        let outcome = async {
            stamp(lake, to, &mut pending).await?;
            let f = pack(lake, &pending).await?;
            if let Some(turn) = turn.take() {
                let _ = turn.await; // (an error: the previous one gave up; its turn is over either way)
            }
            match to {
                To::Local(seq) => {
                    let outcome = seq.enqueue(f).await?;
                    drop(done.take());
                    anyhow::Ok(outcome.await?)
                }
                To::Leader(addr) => {
                    let request = http().post(format!("http://{addr}/cluster/commit")).body(encode_flush(&f)?).send();
                    drop(done.take()); // (sent in order; over HTTP they may still arrive out of order, rarely)
                    Ok(request.await?.error_for_status()?.json().await?)
                }
            }
        };
        match outcome.await {
            Ok(Outcome::Acks(acks)) => {
                for (a, ack) in pending.drain(..).zip(acks) {
                    let _ = a.ack.send(Ok(ack));
                }
            }
            Ok(Outcome::Retry(retried)) => {
                if retried.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await; // (packed with views this node didn't know of yet: again)
                }
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
pub async fn pack(lake: &Lake, pending: &[Append]) -> Result<Flush> { pack_with(lake, pending, BTreeMap::new()).await }

/// `pack`, the views deriving from `followed` too: rows a file commit puts into tables (and a
/// change's old ones, under `{t}$deleted`), which the flush doesn't carry itself.
pub async fn pack_with(lake: &Lake, pending: &[Append], followed: BTreeMap<String, Vec<RecordBatch>>) -> Result<Flush> {
    let (mut data, mut parts) = (vec![], vec![]);
    let mut add = |table: &str, batch: &RecordBatch, src: Option<Src>| -> Result<()> {
        let off = data.len() as u64;
        if batch.num_rows() > 0 {
            data.extend(encode_ipc(&[crate::sys::compact(batch)?])?);
        }
        parts.push(Part { table: table.into(), off, len: data.len() as u64 - off, rows: batch.num_rows() as u64, src });
        Ok(())
    };
    let mut by_table = followed;
    let mut metas: BTreeMap<String, Option<TableMeta>> = BTreeMap::new(); // (the log keeps columns under their stored names: ADR-022)
    for a in pending {
        if !metas.contains_key(&a.table) {
            metas.insert(a.table.clone(), lake.cat.get::<TableMeta>(&table_key(&a.table)).await?);
        }
        let stored = match &metas[&a.table] {
            Some(m) => m.to_stored(&a.batch)?,
            None => a.batch.clone(),
        };
        add(&a.table, &stored, Some(a.src.clone()))?;
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
        // Row ids' blocks (ADR-029 §11): a counter of their own, which starts above every commit
        // number (the blocks before it had), and moves only as blocks are taken.
        let mut block: u64 = lake.cat.get("b").await?.unwrap_or(next);
        crate::views::forget(&lake); // (the views as this leader finds them)
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
                if let Err(e) = commit(&lake, &mut next, &mut block, &mut last_seq, batch, &ms, slot).await {
                    // Committed or not, we can't tell: restart and reload the state from the catalog.
                    eprintln!("sequencer failed: {e:#}");
                    crate::cluster::restart("the sequencer failed");
                }
            }
        });
        Ok(Arc::new(Sequencer { tx, commit_ms }))
    }

    pub async fn submit(&self, f: Flush) -> Result<Outcome> { Ok(self.enqueue(f).await?.await?) }

    /// A commit number of its own and its time, here (no block of ids: `block`).
    pub async fn number(&self) -> Result<Reserved> { reserved(self.submit(Flush { reserve: 1, ..Default::default() }).await?) }

    /// `To::block`, here.
    pub async fn block(&self) -> Result<u64> { Ok(reserved(self.submit(Flush { block: true, ..Default::default() }).await?)?.block) }

    /// Queue a flush; its outcome comes once it's committed.
    pub async fn enqueue(&self, f: Flush) -> Result<oneshot::Receiver<Outcome>> {
        let (reply, rx) = oneshot::channel();
        self.tx.send((f, reply)).await.map_err(|_| anyhow!("sequencer stopped"))?;
        Ok(rx)
    }
}

/// Producers whose progress left the catalog (a view dropped: `emit:`, `join:`, `fill:`): the
/// sequencer forgets what it remembered of them, so a view made again under the name starts over.
static FORGOTTEN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

pub fn forget_producers(names: impl IntoIterator<Item = String>) { FORGOTTEN.lock().unwrap().extend(names); }

/// Sequence a batch of flushes and write them as one catalog commit. Without waiting for it to
/// be committed, the next batch can follow; acks go out once this one is.
#[allow(clippy::too_many_arguments)]
async fn commit(lake: &Arc<Lake>, next: &mut u64, block: &mut u64, last_seq: &mut HashMap<String, u64>, batch: Vec<(Flush, oneshot::Sender<Outcome>)>, ms: &Arc<Mutex<Vec<f64>>>, slot: tokio::sync::OwnedSemaphorePermit) -> Result<()> {
    let (mut puts, mut seqs, mut replies) = (vec![], HashMap::<String, u64>::new(), vec![]);
    let (mut inline, mut inline_parts) = (vec![], BTreeMap::<String, Vec<(u64, u64, u64)>>::new()); // all inline flushes: one segment
    for p in std::mem::take(&mut *FORGOTTEN.lock().unwrap()) {
        last_seq.remove(&p);
    }
    let (views, blocks) = (crate::views::inline(lake).await?, *block);
    puts.extend(crate::views::bound(lake, &views, *next)); // (views made since: their filling ends before this commit)
    for (mut f, reply) in batch {
        if f.reserve > 0 || f.block {
            // (numbers no segment will take: gaps in the log's sequence, which nothing minds)
            let ack = Ack { seg: *next, ms: now_ms(), block: *block, ..Default::default() };
            *next += f.reserve;
            *block += f.block as u64;
            replies.push((reply, Outcome::Acks(vec![ack])));
            continue;
        }
        // 0. A flush packed with other views than the ones its tables have now goes back to be
        // packed again (`views::Inline`).
        let derived: std::collections::HashSet<&str> = f.parts.iter().filter(|p| p.src.is_none()).map(|p| p.table.as_str()).collect();
        let filed = f.filed.iter().filter(|x| !x.added.is_empty() || !x.removed.is_empty() || !x.deleted.is_empty()).map(|x| x.table.as_str()); // (a file commit's rows: its views derived theirs too)
        let mut owed = f.parts.iter().filter(|p| p.src.is_some() && p.rows > 0).map(|p| p.table.as_str()).chain(filed).flat_map(|t| views.by_source.get(t).into_iter().flatten());
        if owed.any(|v| !derived.contains(v.as_str())) || derived.iter().any(|t| !views.tables.contains(*t)) {
            replies.push((reply, Outcome::Retry(vec![])));
            continue;
        }
        // 1. Skip retries of committed batches, and batches whose `prev` no longer holds.
        let (mut acks, mut bad, mut mine) = (vec![Ack::default(); f.parts.len()], vec![], HashMap::new());
        // (A part without a producer name is at-least-once: nothing to check.)
        for (i, src) in f.parts.iter().enumerate().filter_map(|(i, p)| Some((i, p.src.as_ref().filter(|s| !s.producer.is_empty())?))) {
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
        if !bad.is_empty() && !f.filed.is_empty() {
            replies.push((reply, Outcome::Acks(acks))); // (a file commit already in: its job's seq)
            continue;
        }
        if !bad.is_empty() && f.parts.iter().any(|p| p.src.is_none()) {
            // Views derived rows from the skipped batches too: the node must redo the flush.
            replies.push((reply, Outcome::Retry(bad.iter().map(|&i| (i, acks[i])).collect())));
            continue;
        }
        seqs.extend(mine);
        // 2. An object flush becomes the next segment, and so does a file commit; inline flushes
        // all go into one (below).
        let filings = std::mem::take(&mut f.filed);
        let inlined = f.path.is_empty() && filings.is_empty();
        let base = if inlined { inline.len() as u64 } else { 0 };
        let mut parts: BTreeMap<String, Vec<(u64, u64, u64)>> = BTreeMap::new();
        let before = |t: &str| if inlined { inline_parts.get(t).map_or(0, |v| v.iter().map(|p| p.2).sum()) } else { 0 };
        for (i, p) in f.parts.iter().enumerate().filter(|(i, p)| p.rows > 0 && !bad.contains(i)) {
            let rows = parts.entry(p.table.clone()).or_default();
            acks[i].row = before(&p.table) + rows.iter().map(|p| p.2).sum::<u64>();
            rows.push((base + p.off, p.len, p.rows));
            lake.backlog.fetch_add(p.rows, Ordering::Relaxed);
        }
        let seg = match (parts.is_empty() && filings.is_empty(), inlined) {
            (true, _) => 0,
            (false, true) => {
                inline.extend_from_slice(&f.data);
                parts.into_iter().for_each(|(t, p)| inline_parts.entry(t).or_default().extend(p));
                u64::MAX // the inline segment's number, set below
            }
            (false, false) => {
                let (seg, ts) = (*next, now_ms());
                let mut files = BTreeMap::new();
                for Filing { table, mut meta, mut added, removed, deleted } in filings {
                    added.iter_mut().for_each(|d| stamp_file(d, seg, ts));
                    meta.files.extend(added.iter().cloned());
                    meta.rows_at = seg;
                    puts.push((table_key(&table), json(&meta))); // (the leader's, under the lake's lock)
                    files.insert(table, Filed { added, removed, deleted });
                }
                *next += span(&parts, &files);
                if f.path.is_empty() && !f.data.is_empty() {
                    puts.push((data_key(seg), f.data.to_vec()));
                }
                puts.push((seg_key(seg), json(&Segment { path: f.path, parts, ts_ms: ts, files })));
                seg
            }
        };
        acks.iter_mut().enumerate().filter(|(i, _)| !bad.contains(i)).for_each(|(_, a)| a.seg = seg);
        replies.push((reply, Outcome::Acks(acks)));
    }
    if !inline_parts.is_empty() {
        let seg = *next;
        *next += span(&inline_parts, &BTreeMap::new());
        puts.push((data_key(seg), inline));
        puts.push((seg_key(seg), json(&Segment { path: String::new(), parts: inline_parts, ts_ms: now_ms(), files: BTreeMap::new() })));
        for (_, o) in replies.iter_mut() {
            if let Outcome::Acks(acks) = o {
                acks.iter_mut().filter(|a| a.seg == u64::MAX).for_each(|a| a.seg = seg);
            }
        }
    }
    // 3. One catalog write for everything; acks once it's committed (see `Lake::commits`).
    puts.extend(seqs.iter().map(|(p, s)| (producer_key(p), json(s))));
    puts.push(("n".into(), json(next)));
    if *block != blocks {
        puts.push(("b".into(), json(block)));
    }
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

/// The numbers a segment takes: one, and one more for each `1 << ORD_BITS` rows of a table in it,
/// its files' rows after its parts' (its rows' places, `ord`, run on into them).
fn span(parts: &BTreeMap<String, Vec<(u64, u64, u64)>>, files: &BTreeMap<String, Filed>) -> u64 {
    let rows = |t: &String| parts.get(t).map_or(0, |p| p.iter().map(|x| x.2).sum::<u64>()) + files.get(t).map_or(0, |f| f.added.iter().map(|d| d.rows).sum());
    1 + parts.keys().chain(files.keys()).map(|t| rows(t).saturating_sub(1) >> ORD_BITS).max().unwrap_or(0)
}

/// A file a commit takes in, as of that commit (`seg`, at `ms`): its place in the table's files,
/// and the lineage it was given without them completed (its rows' `_version` and times).
fn stamp_file(d: &mut DataFile, seg: u64, ms: u64) {
    d.ord = seg;
    if let Some(l) = d.lineage.as_mut().filter(|l| l.version == 0) {
        (l.version, l.ms) = (seg, ms);
        d.stats.extend(crate::adopt::system_stats(l, d.rows));
    }
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
