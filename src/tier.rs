//! Tiering, compaction and retention.
//! Each round folds a table's log tail into new Parquet files (keyed tables: one row per key per
//! file, the newest file winning), and commits the new file list and the new `tiered` mark in ONE
//! catalog write, so a crash never loses or doubles rows (an uncommitted Parquet file is just an
//! unreferenced object). Then, separately, `maintain` keeps the file count down: small append
//! files are merged 8 at a time, and keyed tables are compacted into one file once 8 pile up.
use crate::query::{latest_sql, raw, schema, tail};
use datafusion::prelude::ParquetReadOptions;
use crate::store::*;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeMap;

/// Rows per table in log segments (after, upto] (`None`: to the end), from segment metadata only.
pub async fn backlog(lake: &Lake, after: u64, upto: Option<u64>) -> Result<BTreeMap<String, u64>> {
    let mut rows = BTreeMap::new();
    let end = upto.map_or("s0".to_string(), |u| seg_key(u + 1));
    for (_, seg) in lake.cat.scan::<Segment>(&seg_key(after + 1), &end).await? {
        for (table, parts) in seg.parts {
            *rows.entry(table).or_default() += parts.iter().map(|p| p.2).sum::<u64>();
        }
    }
    Ok(rows)
}

/// Tier one table's log up to segment `hwm`, at most ~4M rows at a time (bounded memory; the
/// caller repeats). The leader only decides and commits: the data work (reading the log and
/// files, writing Parquet) is dealt out to the live `nodes` as jobs, SPMD style. Returns the
/// rows tiered and whether it's done.
pub async fn tier_table(lake: &Lake, table: &str, hwm: u64, nodes: &[String], me: &str) -> Result<(u64, bool)> {
    const MAX_ROWS: u64 = 4_000_000;
    let Some(mut meta) = lake.cat.get::<TableMeta>(&table_key(table)).await? else { return Ok((0, true)) };
    if hwm <= meta.tiered {
        return Ok((0, true));
    }
    let (mut segs, mut n) = (vec![], 0); // (segment, this table's rows in it), up to MAX_ROWS
    for (key, seg) in lake.cat.scan::<Segment>(&seg_key(meta.tiered + 1), &seg_key(hwm + 1)).await? {
        let rows = seg.parts.get(table).map_or(0, |p| p.iter().map(|p| p.2).sum());
        segs.push((key[2..].parse::<u64>()?, rows));
        n += rows;
        if n >= MAX_ROWS {
            break;
        }
    }
    if n == 0 {
        return Ok((0, true)); // nothing new for this table (`expire` moves its mark along)
    }
    // The log's rows as new files, one range of segments per node. Keyed tables keep one row per
    // key per file; older files still hold older versions until a compaction folds them.
    let upto = segs.last().map_or(hwm, |s| s.0);
    let (mut jobs, mut from, mut acc, mut done) = (vec![], meta.tiered, 0, 0);
    for &(seg, r) in &segs {
        acc += r;
        if acc * nodes.len() as u64 >= n * (jobs.len() as u64 + 1) || seg == upto {
            jobs.push(Job::new(table, &meta, Kind::Fold { after: from, upto: seg, rows: acc - done }));
            (from, done) = (seg, acc);
        }
    }
    let files = deal(lake, jobs, nodes, me).await?;
    let rows = files.iter().map(|f| f.rows).sum();
    meta.files.extend(files);
    meta.tiered = upto;
    lake.cat.commit(vec![(table_key(table), json(&meta))], &[]).await?;
    lake.backlog.fetch_sub(n.min(lake.backlog.load(std::sync::atomic::Ordering::Relaxed)), std::sync::atomic::Ordering::Relaxed);
    Ok((rows, n < MAX_ROWS))
}

/// Keep a table's file count down, once its new rows are committed (so fresh rows never wait
/// for this). Append tables: once 8+ files are small, merge them, a group of 8 per node; a file
/// that is already 64 MB or 4M rows (what a merge writes at most) is left alone, so rows are
/// never merged twice. Keyed tables fold their versions away once 8 files pile up. Returns
/// whether anything changed.
pub async fn maintain(lake: &Lake, table: &str, nodes: &[String], me: &str) -> Result<bool> {
    let Some(mut meta) = lake.cat.get::<TableMeta>(&table_key(table)).await? else { return Ok(false) };
    let small: Vec<DataFile> = meta.files.iter().filter(|f| f.bytes < 64 << 20 && f.rows < 4_000_000).cloned().collect();
    if !meta.key.is_empty() && meta.files.len() >= 8 {
        let merged = deal(lake, vec![Job::new(table, &meta, Kind::Compact { upto: meta.tiered, rows: 0 })], nodes, me).await?;
        let old = meta.files.clone();
        replace(&mut meta, &old, merged);
    } else if meta.key.is_empty() && small.len() >= 8 {
        let jobs = small.chunks(8).map(|g| Job::new(table, &meta, Kind::Merge { files: g.to_vec() })).collect();
        let merged = deal(lake, jobs, nodes, me).await?;
        replace(&mut meta, &small, merged);
    } else {
        return Ok(false);
    }
    lake.cat.commit(vec![(table_key(table), json(&meta))], &[]).await?;
    Ok(true)
}

/// Swap `old` files for `new` ones; the old ones are deleted after the retention period.
fn replace(meta: &mut TableMeta, old: &[DataFile], new: Vec<DataFile>) {
    let now = crate::log::now_ms();
    meta.files.retain(|f| !old.iter().any(|o| o.path == f.path));
    meta.garbage.extend(old.iter().map(|f| (f.path.clone(), now)));
    meta.files.extend(new);
}

/// Data work the leader deals out. It names its inputs exactly (the table as the leader sees
/// it), so a node whose catalog view lags can't work from an older file list.
#[derive(Serialize, Deserialize)]
pub struct Job {
    table: String,
    meta: TableMeta,
    kind: Kind,
}

impl Job {
    fn new(table: &str, meta: &TableMeta, kind: Kind) -> Job { Job { table: table.into(), meta: meta.clone(), kind } }
}

#[derive(Serialize, Deserialize)]
enum Kind {
    // `rows`: this table's rows in those log segments, as the leader counts them (see `caught_up`)
    Fold { after: u64, upto: u64, rows: u64 }, // log segments (after, upto] -> a file (merge tables: one row per key)
    Merge { files: Vec<DataFile> },            // small files -> one
    Compact { upto: u64, rows: u64 },          // files + log up to `upto` -> one row per key
}

/// Run jobs round-robin on the live nodes (each round starts where the last one stopped, so
/// single jobs rotate too) and collect the files they wrote.
async fn deal(lake: &Lake, jobs: Vec<Job>, nodes: &[String], me: &str) -> Result<Vec<DataFile>> {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let runs = jobs.into_iter().map(|job| {
        let node = &nodes[NEXT.fetch_add(1, Relaxed) % nodes.len()];
        async move {
            if node == me {
                return run_job(lake, job).await;
            }
            let r = crate::cluster::http().post(format!("http://{node}/cluster/job")).json(&job).send().await?;
            anyhow::ensure!(r.status().is_success(), "job on {node}: {}", r.text().await?);
            Ok(r.json().await?)
        }
    });
    Ok(futures::future::try_join_all(runs).await?.into_iter().flatten().collect())
}

/// Do one job here; returns the Parquet files written.
pub async fn run_job(lake: &Lake, Job { table, meta, kind }: Job) -> Result<Vec<DataFile>> {
    // (Clustered append tables get what keyed tables get for their key: small row groups, bloom filters.)
    let (keys, whole) = (if meta.key.is_empty() { meta.cluster.clone() } else { meta.key.clone() }, matches!(kind, Kind::Compact { .. }));
    let (batches, ord) = match kind {
        Kind::Fold { after, upto, rows } if meta.key.is_empty() => {
            caught_up(lake, &table, after, upto, rows).await?;
            let rows = tail(lake, &table, after, Some(upto), false).await?;
            let rows = match rows.is_empty() || meta.cluster.is_empty() {
                true => rows,
                false => clustered(&meta, lake.session().read_batches(rows)?)?.collect().await?,
            };
            (rows, upto)
        }
        // Keyed tables: one row per key for this range of the log, delete markers included (they
        // still have to shadow what older files hold for that key).
        Kind::Fold { after, upto, rows } => {
            caught_up(lake, &table, after, upto, rows).await?;
            let part = TableMeta { files: vec![], tiered: after, ..meta };
            (latest(lake, &table, &part, upto, true).await?, upto)
        }
        Kind::Compact { upto, rows } => {
            anyhow::ensure!(!meta.key.is_empty(), "only keyed tables have versions to compact");
            caught_up(lake, &table, meta.tiered, upto, rows).await?;
            (latest(lake, &table, &meta, upto, false).await?, upto)
        }
        Kind::Merge { files } => {
            let ctx = lake.session();
            let paths: Vec<String> = files.iter().map(|f| lake.full(&f.path)).collect();
            let schema = schema(&meta.columns)?;
            let df = clustered(&meta, ctx.read_parquet(paths, ParquetReadOptions::default().schema(&schema)).await?)?;
            let ord = files.iter().map(|f| f.ord).max().unwrap_or(0);
            let mut merged = write_stream(lake, &table, df.execute_stream().await?, 4_000_000, &keys).await?;
            merged.iter_mut().for_each(|f| f.ord = ord);
            return Ok(merged);
        }
    };
    let mut files: Vec<DataFile> = write_file(lake, &table, &batches, &keys).await?.into_iter().collect();
    files.iter_mut().for_each(|f| (f.ord, f.whole) = (ord, whole));
    Ok(files)
}

/// Append tables with `cluster_by`: every file's rows sorted by those columns (folds and merges
/// alike), so each row group covers a narrow range of them and a filter on them skips the rest
/// by min/max statistics and bloom filters — in Pondra and in any engine reading the Parquet.
fn clustered(meta: &TableMeta, df: datafusion::prelude::DataFrame) -> Result<datafusion::prelude::DataFrame> {
    if meta.cluster.is_empty() {
        return Ok(df);
    }
    Ok(df.sort(meta.cluster.iter().map(|c| datafusion::prelude::ident(c).sort(true, false)).collect())?)
}

/// One row per key: `meta`'s files plus the log up to `upto`, sorted by key.
async fn latest(lake: &Lake, table: &str, meta: &TableMeta, upto: u64, keep_deleted: bool) -> Result<Vec<RecordBatch>> {
    let ctx = lake.session();
    ctx.register_table("__raw", raw(lake, &ctx, table, meta, Some(upto)).await?.into_view())?;
    Ok(ctx.sql(&latest_sql(meta, "__raw", true, keep_deleted)).await?.collect().await?)
}

/// Wait until this node sees log segment `upto` (a follower's view may lag the leader a bit),
/// then check it sees exactly the rows the leader counted in (after, upto]. Never work from
/// less: a missing segment would mean missing rows (the job fails; the next round retries).
async fn caught_up(lake: &Lake, table: &str, after: u64, upto: u64, rows: u64) -> Result<()> {
    if upto <= after {
        return Ok(()); // no log to read (a compaction of the files alone)
    }
    let mut hwm = lake.hwm.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(30), hwm.wait_for(|h| *h >= upto)).await.context("this node is behind the leader")??;
    anyhow::ensure!(lake.cat.get::<Segment>(&seg_key(upto)).await?.is_some(), "segment {upto} isn't visible here yet");
    let here = backlog(lake, after, Some(upto)).await?.get(table).copied().unwrap_or(0);
    anyhow::ensure!(here == rows, "{table}: {here} rows in log segments {after}..={upto} here, {rows} on the leader");
    Ok(())
}

/// Retention: drop log segments that every table and every streaming task has consumed, and
/// files replaced by compaction, once older than `grace_ms` (readers holding an older snapshot
/// may still use them). Catalog first, then the objects: a crash leaves only unreferenced objects.
pub async fn expire(lake: &Lake, grace_ms: u64) -> Result<()> {
    let cutoff = crate::log::now_ms().saturating_sub(grace_ms);
    let mut tables = lake.cat.scan::<TableMeta>("t/", "t0").await?;
    // A table with nothing new in the log moves its mark to the end of it here (tiering skips it,
    // rather than commit nothing), so an idle table doesn't hold retention back.
    let hwm = lake.visible();
    let pending = backlog(lake, tables.iter().map(|(_, m)| m.tiered).min().unwrap_or(hwm), Some(hwm)).await?;
    let idle: Vec<bool> = tables.iter().map(|(k, m)| m.tiered < hwm && !pending.contains_key(&k[2..])).collect();
    tables.iter_mut().zip(&idle).filter(|(_, idle)| **idle).for_each(|((_, m), _)| m.tiered = hwm);
    let mut floor = tables.iter().map(|(_, m)| m.tiered).min().unwrap_or(0);
    for (key, task) in lake.cat.scan::<crate::tasks::Task>("k/", "k0").await? {
        for producer in crate::tasks::producers(&key[2..], &task) {
            let done = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
            if done < floor && !tail(lake, &task.source, done, Some(floor), false).await?.is_empty() {
                floor = done; // the task still has to read these (tasks skip runs with nothing new)
            }
        }
    }
    // Only segments that were ALREADY below the floor `grace_ms` ago: a query that read a table's
    // metadata just before it was tiered may still be reading them.
    static FLOORS: std::sync::Mutex<std::collections::VecDeque<(u64, u64)>> = std::sync::Mutex::new(std::collections::VecDeque::new());
    let floor = {
        let mut floors = FLOORS.lock().unwrap();
        floors.push_back((crate::log::now_ms(), floor));
        while floors.len() > 1 && floors[1].0 <= cutoff {
            floors.pop_front();
        }
        if floors[0].0 <= cutoff { floors[0].1 } else { 0 }
    };
    let segs: Vec<(String, Segment)> = lake.cat.scan::<Segment>(&seg_key(1), &seg_key(floor + 1)).await?
        .into_iter().filter(|(_, s)| s.ts_ms < cutoff).collect();
    let mut dead: Vec<String> = segs.iter().filter(|(_, s)| !s.path.is_empty()).map(|(_, s)| s.path.clone()).collect();
    let mut deletes: Vec<String> = segs.iter().map(|(k, _)| k.clone()).collect();
    deletes.extend(segs.iter().filter(|(_, s)| s.path.is_empty()).map(|(k, _)| data_key(k[2..].parse().unwrap_or(0))));
    let mut puts = vec![];
    for ((key, mut meta), idle) in tables.into_iter().zip(idle) {
        let (old, keep): (Vec<_>, Vec<_>) = meta.garbage.drain(..).partition(|(_, ts)| *ts < cutoff);
        if !old.is_empty() || idle {
            dead.extend(old.into_iter().map(|(p, _)| p));
            meta.garbage = keep;
            puts.push((key, json(&meta)));
        }
    }
    if !puts.is_empty() || !deletes.is_empty() {
        lake.cat.commit(puts, &deletes).await?;
    }
    lake.cat.wait_durable(lake.cat.committed()).await; // (replicated acks: forget objects only once the bucket has)
    futures::stream::iter(dead).for_each_concurrent(16, |path| async move { lake.delete(&path).await }).await; // (one round trip each)
    collect_orphans(lake).await
}

/// Hourly: delete objects that no catalog entry points to and that are a day old: segments a
/// node wrote whose commit never happened, Parquet files from crashed tiering or insert jobs.
/// (A day, because a huge bulk insert writes its files long before it commits them.)
async fn collect_orphans(lake: &Lake) -> Result<()> {
    static LAST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    const HOUR: u64 = 3_600_000;
    let now = crate::log::now_ms();
    if now - LAST.load(std::sync::atomic::Ordering::Relaxed) < HOUR {
        return Ok(());
    }
    LAST.store(now, std::sync::atomic::Ordering::Relaxed);
    let mut used: std::collections::HashSet<String> = lake.cat.scan::<Segment>("s/", "s0").await?.into_iter().map(|(_, s)| s.path).collect();
    for (_, m) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        used.extend(m.files.into_iter().map(|f| f.path).chain(m.garbage.into_iter().map(|(p, _)| p)));
    }
    for prefix in ["log", "data"] {
        let objects: Vec<_> = lake.store.list(Some(&object_store::path::Path::from(prefix))).try_collect().await?;
        for o in objects {
            let old = now as i64 - o.last_modified.timestamp_millis() > 24 * HOUR as i64;
            if old && !used.contains(o.location.as_ref()) && !crate::delta::open_format(o.location.as_ref()) {
                lake.delete(o.location.as_ref()).await;
            }
        }
    }
    Ok(())
}

/// Parquet writer: ZSTD everywhere. Keyed tables are written for lookups as well as scans —
/// sorted by key (see `latest_sql`), with a bloom filter per key column, and in small row groups
/// and pages, so reading one key touches one page instead of a million rows.
fn writer<'a>(buf: &'a mut Vec<u8>, batch: &RecordBatch, keys: &[String]) -> Result<ArrowWriter<&'a mut Vec<u8>>> {
    let mut props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?));
    if !keys.is_empty() {
        props = props.set_max_row_group_row_count(Some(256 << 10));
    }
    for k in keys {
        props = props.set_column_bloom_filter_enabled(k.as_str().into(), true);
    }
    Ok(ArrowWriter::try_new(buf, batch.schema(), Some(props.build()))?)
}

/// Write batches as one Parquet file (none if there are no rows).
pub async fn write_file(lake: &Lake, table: &str, batches: &[RecordBatch], keys: &[String]) -> Result<Option<DataFile>> {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if rows == 0 {
        return Ok(None);
    }
    let mut buf = vec![];
    let mut w = writer(&mut buf, &batches[0], keys)?;
    for b in batches {
        w.write(b)?;
    }
    w.close()?;
    let (path, bytes) = (format!("data/{table}/{}.parquet", uuid::Uuid::new_v4()), buf.len() as u64);
    lake.put(&path, buf).await?;
    maybe_crash("after_parquet_put");
    Ok(Some(DataFile { path, rows: rows as u64, bytes, ord: 0, whole: false }))
}

/// Stream a query result into Parquet files of up to `max_rows` each (bulk INSERT … SELECT).
pub async fn write_stream(lake: &Lake, table: &str, mut stream: SendableRecordBatchStream, max_rows: usize, keys: &[String]) -> Result<Vec<DataFile>> {
    let (mut files, mut pending, mut n) = (vec![], vec![], 0);
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        n += batch.num_rows();
        pending.push(batch);
        if n >= max_rows {
            files.extend(write_file(lake, table, &std::mem::take(&mut pending), keys).await?);
            n = 0;
        }
    }
    files.extend(write_file(lake, table, &pending, keys).await?);
    Ok(files)
}
