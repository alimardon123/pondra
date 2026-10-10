//! Tiering, compaction and retention.
//! Each round folds a table's log tail into new Parquet files (keyed tables: one row per key per
//! file, the newest file winning), and commits the new file list and the new `tiered` mark in ONE
//! catalog write, so a crash never loses or doubles rows (an uncommitted Parquet file is just an
//! unreferenced object). Then, separately, `maintain` keeps the file count down: small append
//! files are merged 8 at a time, and keyed tables are compacted into one file once 8 pile up.
use crate::query::{latest_sql, raw, schema};
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
use futures::StreamExt;
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
    let mut files = deal(lake, jobs, nodes, me).await?;
    let rows = files.iter().map(|f| f.rows).sum();
    crate::sketch::add(&mut meta, &mut files);
    let one = meta.files.windows(2).all(|w| w[0].ord == w[1].ord); // (one generation, or none: nothing shadowed yet)
    meta.shadows = shadowing(&meta) && (meta.shadows || one);
    let new: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    meta.files.extend(files);
    (meta.tiered, meta.rows_at) = (upto, upto);
    shadow(lake, table, &mut meta, &new, true).await?;
    // (Counted off before the commit is awaited: a round that fails on another table drops this
    // future while it waits, the commit lands all the same, and a count taken off after it was
    // never taken off, so the log looked forever undrained. Fewer counted is only a gauge's lag.)
    lake.backlog.fetch_sub(n.min(lake.backlog.load(std::sync::atomic::Ordering::Relaxed)), std::sync::atomic::Ordering::Relaxed);
    lake.cat.commit(vec![(table_key(table), json(&meta))], &[]).await?;
    Ok((rows, n < MAX_ROWS))
}

/// Keep a table's file count down, once its new rows are committed (so fresh rows never wait
/// for this). Append tables: once 8+ files are small, merge them, a group of 8 per node; a file
/// that is already 64 MB or 4M rows (what a merge writes at most) is left alone, so rows are
/// never merged twice. Keyed tables, once 8 files pile up, merge their newest run of files of
/// similar size (`run`); only when that run reaches the oldest file is the whole table rewritten.
/// Returns whether anything changed.
const MERGE_BYTES: u64 = 256 << 20; // input a merge job takes at most

/// `now_anyway` (`CHECKPOINT`): a keyed table that publishes is compacted whatever it holds, so
/// other engines see its rows as they are now.
pub async fn maintain(lake: &Lake, table: &str, nodes: &[String], me: &str, now_anyway: bool) -> Result<bool> {
    let Some(mut meta) = lake.cat.get::<TableMeta>(&table_key(table)).await? else { return Ok(false) };
    let small: Vec<DataFile> = meta.files.iter().filter(|f| f.bytes < 64 << 20 && f.rows < 4_000_000).cloned().collect();
    let generations = meta.files.iter().map(|f| f.ord).collect::<std::collections::BTreeSet<_>>().len();
    let stale = now_anyway && !meta.publish.is_empty() && (generations > 1 || meta.files.iter().any(|f| !f.whole));
    let changed = if !meta.key.is_empty() && (generations >= 8 || stale) {
        // Tables other engines read compact fully (they see keyed tables as of their last full
        // compaction); the rest merge size-tiered runs.
        let run = if meta.publish.is_empty() { run(&meta.files) } else { meta.files.clone() };
        let job = match run.len() == meta.files.len() {
            true => Kind::Compact { upto: meta.tiered, rows: 0 }, // everything: one row per key, deletes dropped
            false => Kind::Squash { files: run.clone() },
        };
        let merged = deal(lake, vec![Job::new(table, &meta, job)], nodes, me).await?;
        let new: Vec<String> = merged.iter().map(|f| f.path.clone()).collect();
        replace(&mut meta, &run, merged);
        shadow(lake, table, &mut meta, &new, false).await?; // (its delete markers, kept to shadow what's older)
        true
    } else if meta.key.is_empty() && (small.len() >= 2 || meta.files.iter().any(mostly_deleted)) {
        // Small files of one partition merge together (so each file keeps one): 8 of about one
        // size at a time (`class`), and all of them before they're sealed (manifests never change,
        // so they'd stay small).
        let sealing: std::collections::HashSet<&str> = crate::manifest::to_seal(&meta).iter().map(|f| f.path.as_str()).collect();
        let mut by_part: BTreeMap<(&str, bool, u32), Vec<DataFile>> = BTreeMap::new();
        small.iter().for_each(|f| {
            let sealing = sealing.contains(f.path.as_str());
            by_part.entry((f.part.as_str(), sealing, if sealing { 0 } else { class(f.bytes) })).or_default().push(f.clone())
        });
        let mut groups: Vec<Vec<DataFile>> = by_part.into_iter().flat_map(|((_, sealing, _), g)| {
            let (least, most) = if sealing { (2, 32) } else { (8, 8) };
            if g.len() < least {
                return vec![];
            }
            // …and at most MERGE_BYTES of input per job, whatever the count: what a job merges is
            // what it holds (its rows, and the Parquet file it is writing).
            let mut groups: Vec<Vec<DataFile>> = vec![];
            for f in g {
                match groups.last_mut() {
                    Some(last) if last.len() < most && last.iter().map(|f| f.bytes).sum::<u64>() + f.bytes <= MERGE_BYTES => last.push(f),
                    _ => groups.push(vec![f]),
                }
            }
            groups.retain(|g| g.len() > 1);
            groups
        }).collect();
        // A file a tenth or more of whose rows were deleted by position is rewritten without
        // them (a merge reads it so), alone if no merge took it: the purge that used to rewrite
        // every changed file every round, now for the files it pays for (ADR-029 §4).
        let merged: std::collections::HashSet<String> = groups.iter().flatten().map(|f| f.path.clone()).collect();
        groups.extend(meta.files.iter().filter(|f| mostly_deleted(f) && !merged.contains(&f.path)).map(|f| vec![f.clone()]));
        if !groups.is_empty() {
            let jobs = groups.iter().map(|g| Job::new(table, &meta, Kind::Merge { files: g.clone() })).collect();
            let merged = deal(lake, jobs, nodes, me).await?;
            replace(&mut meta, &groups.concat(), merged);
        }
        !groups.is_empty()
    } else {
        false
    };
    // The oldest inline files go into manifests once there are too many.
    if !crate::manifest::seal(lake, table, &mut meta).await? && !changed {
        return Ok(false);
    }
    lake.cat.commit(vec![(table_key(table), json(&meta))], &[]).await?;
    Ok(true)
}

/// A file a tenth or more of whose rows were deleted by position: `maintain` rewrites it.
/// An upsert table that others read, each round (ADR-029 §4): the rows its new files shadow in
/// older ones — older versions of their keys — and the new files' delete markers become positions,
/// so other engines read one row per key between compactions (`delta::publishable`). Pondra's own
/// reads pass over them: the newest row per key is theirs anyway (`query::files_once`). `older`:
/// look for the new keys in the older files (a squash's output shadows nothing new).
async fn shadow(lake: &Lake, table: &str, meta: &mut TableMeta, new: &[String], older: bool) -> Result<()> {
    use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
    use datafusion::prelude::{col, ident};
    meta.shadows &= shadowing(meta); // (published no longer, say: positions stop, and start again after a compaction)
    if !meta.shadows || new.is_empty() {
        return Ok(());
    }
    let (ctx, marked) = (lake.session(), meta.columns.iter().any(|(c, _)| c == "_deleted"));
    let cols: Vec<(String, String)> = meta.columns.iter().filter(|(c, _)| meta.key.contains(c) || c == "_deleted").cloned().collect();
    let schema = schema(&cols)?;
    let places = |b: Vec<RecordBatch>| b.iter().flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().iter().map(|p| *p as u64).collect::<Vec<_>>()).collect::<Vec<u64>>();
    let mut hit: Vec<(String, Vec<u64>)> = vec![];
    let mut fresh: Vec<(u64, datafusion::prelude::DataFrame)> = vec![]; // (each new file's keys, by its generation)
    for f in meta.files.iter().filter(|f| new.contains(&f.path)) {
        let df = crate::scan::placed(lake, &ctx, f, &schema).await?;
        fresh.push((f.ord, df.clone().select(meta.key.iter().map(|k| ident(k).alias(format!("__k_{k}"))).collect::<Vec<_>>())?));
        if marked {
            hit.push((f.path.clone(), places(df.filter(col("_deleted").is_true())?.select_columns(&["__row"])?.collect().await?)));
        }
    }
    for f in meta.files.iter().filter(|_| older) {
        let Some(newer) = fresh.iter().filter(|(ord, _)| *ord > f.ord).map(|(_, k)| k.clone()).reduce(|a, b| a.union(b).expect("the same columns")) else { continue };
        let (left, right): (Vec<String>, Vec<String>) = meta.key.iter().map(|k| (k.clone(), format!("__k_{k}"))).unzip();
        let (left, right) = (left.iter().map(String::as_str).collect::<Vec<_>>(), right.iter().map(String::as_str).collect::<Vec<_>>());
        let df = crate::scan::placed(lake, &ctx, f, &schema).await?.join(newer, datafusion::common::JoinType::LeftSemi, &left, &right, None)?;
        hit.push((f.path.clone(), places(df.select_columns(&["__row"])?.collect().await?)));
    }
    let folder = meta.folder(table).to_string();
    for (path, at) in hit.into_iter().filter(|(_, at)| !at.is_empty()) {
        let (p, rows, bytes) = write_positions(lake, &folder, at.iter().map(|i| (lake.full(&path), *i)).collect()).await?;
        let f = meta.files.iter_mut().find(|f| f.path == path).expect("the table's");
        f.deletes.push(crate::scan::Delete::Positions { path: p, file: lake.full(&path), rows, bytes });
        f.deleted += at.len() as u64;
    }
    Ok(())
}

/// An upsert table (newest wins: no merge functions, no `order_by`) that others read.
fn shadowing(meta: &TableMeta) -> bool { !meta.key.is_empty() && meta.merge.is_empty() && meta.order.is_none() && !meta.publish.is_empty() }

fn mostly_deleted(f: &DataFile) -> bool { f.deleted > 0 && f.deleted * 10 >= f.rows }

/// UPDATE, DELETE and MERGE leave an append table's old rows in its files, and reads leave them
/// out (`query::current`, against `{t}$deleted`). A purge turns them into positions (ADR-029 §4):
/// each file holding one gets an Iceberg position-delete file naming its rows' places
/// (`DataFile::deletes`), which reads skip unread and other engines see (Iceberg as delete files,
/// Delta as deletion vectors) — no file rewritten; a file mostly deleted is rewritten later
/// (`maintain`). Every round for a table that is published, else once `PONDRA_PURGE_ROWS`
/// (100,000) old rows or a tenth of the table wait. It covers the changes whose old rows are all
/// in files and whose tombstones are too (commits up to both tables' `tiered`), and says so
/// (`TableMeta::purges`). `now_anyway`: whatever waits (`CHECKPOINT`).
/// A `{t}$deleted` file whose changes were all purged goes once every reader has passed that
/// purge (`retain_ms` later: a reader with an older entry for the table still needs it).
pub async fn purge(lake: &Lake, table: &str, nodes: &[String], me: &str, retain_ms: u64, now_anyway: bool) -> Result<bool> {
    use crate::sys::{deleted, with_sys, VERSION};
    let Some(mut meta) = lake.cat.get::<TableMeta>(&table_key(table)).await?.filter(|m| m.changed && m.key.is_empty()) else { return Ok(false) };
    let Some(mut dmeta) = lake.cat.get::<TableMeta>(&table_key(&deleted(table))).await? else { return Ok(false) };
    let (after, upto, now) = (meta.purged(), meta.tiered.min(dmeta.tiered), crate::log::now_ms());
    let within = datafusion::prelude::col(VERSION).between(datafusion::prelude::lit(after as i64 + 1), datafusion::prelude::lit(upto as i64));
    let gone = match upto > after {
        true => crate::manifest::pruned(lake, &dmeta, None, &[within], &schema(&with_sys(&dmeta).columns)?).await?,
        false => vec![],
    };
    let waiting: u64 = gone.iter().map(|f| f.rows).sum();
    let rows = meta.files.iter().map(|f| f.rows).sum::<u64>() + meta.sealed.as_ref().map_or(0, |s| s.rows);
    let at_least = std::env::var("PONDRA_PURGE_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
    if upto <= after || (!now_anyway && meta.publish.is_empty() && waiting < at_least && waiting * 10 < rows) {
        // (nothing to purge yet; the old rows past the table's retention may still go)
        if settle(&mut meta, &mut dmeta, now, retain_ms) {
            lake.cat.commit(vec![(table_key(table), json(&meta)), (table_key(&deleted(table)), json(&dmeta))], &[]).await?;
        }
        return Ok(false);
    }
    // The files that can hold an old row: by their `_row_id` and `_version` ranges.
    let mut dead = dead(lake, &gone, after, upto).await?;
    dead.sort_unstable();
    let holds = |s: &crate::manifest::Stats| crate::sys::holds(&dead, s);
    let (list, mut sealed, mut hit) = (crate::manifest::list(lake, &meta).await?, vec![], vec![]);
    for (i, m) in list.iter().enumerate().filter(|(_, m)| holds(&m.stats)) {
        let files: Vec<DataFile> = crate::manifest::files(lake, m).await?.into_iter().filter(|f| holds(&f.stats)).collect();
        if !files.is_empty() {
            sealed.push(i);
            hit.extend(files);
        }
    }
    hit.extend(meta.files.iter().filter(|f| holds(&f.stats)).cloned());
    if !hit.is_empty() {
        crate::manifest::unseal(lake, table, &mut meta, list, &sealed).await?; // (a sealed file's record changes: it comes back inline)
        // A job per partition (a delete file holds one, as Iceberg asks), of bounded size.
        let mut groups: Vec<Vec<DataFile>> = vec![];
        hit.sort_by(|a, b| a.part.cmp(&b.part));
        for f in hit.iter().cloned() {
            match groups.last_mut() {
                Some(g) if g[0].part == f.part && g.iter().map(|f| f.bytes).sum::<u64>() + f.bytes <= MERGE_BYTES => g.push(f),
                _ => groups.push(vec![f]),
            }
        }
        let jobs = groups.into_iter().map(|files| Job::new(table, &meta, Kind::Positions { files, gone: gone.clone(), after, upto })).collect();
        for f in deal(lake, jobs, nodes, me).await? {
            if let Some(mine) = meta.files.iter_mut().find(|m| m.path == f.path) {
                *mine = f; // (the same file, with the positions deleted from it)
            }
        }
        // (sealed again by `maintain`, once it has rewritten the files mostly deleted)
    }
    meta.purges.push((upto, now));
    settle(&mut meta, &mut dmeta, now, retain_ms);
    lake.cat.commit(vec![(table_key(table), json(&meta)), (table_key(&deleted(table)), json(&dmeta))], &[]).await?;
    Ok(true)
}

/// Drops `{t}$deleted`'s files once every reader has passed the purge that covered them and they
/// are older than the table's retention, the past `AT (…)` reads (ADR-043), and says from where the
/// table is still whole (`past_from`). The newest purge every reader has passed, and those after it,
/// are kept. Whether anything changed.
fn settle(meta: &mut TableMeta, dmeta: &mut TableMeta, now: u64, retain_ms: u64) -> bool {
    let Some(i) = meta.purges.iter().rposition(|p| now - p.1 >= retain_ms) else { return false };
    let settled = meta.purges[i].0 as i64;
    let kept = now.saturating_sub(meta.retention_secs.map_or(crate::ddl::KEEP_MS, |s| s * 1000)) as i64 * 1000;
    let version = |s: &crate::manifest::Stats| s.get(crate::sys::VERSION).and_then(|(_, hi)| hi.parse::<i64>().ok());
    let newest = |s: &crate::manifest::Stats| match datafusion::common::ScalarValue::try_from_string(s.get(crate::sys::UPDATED)?.1.clone(), &crate::sys::time()) {
        Ok(datafusion::common::ScalarValue::TimestampMicrosecond(Some(us), _)) => Some(us),
        _ => None,
    };
    let done: Vec<DataFile> = dmeta.files.iter().filter(|f| version(&f.stats).is_some_and(|hi| hi <= settled) && newest(&f.stats).is_some_and(|us| us < kept)).cloned().collect();
    if i == 0 && done.is_empty() {
        return false;
    }
    meta.purges.drain(..i);
    if let (Some(v), Some(us)) = (done.iter().filter_map(|f| version(&f.stats)).max(), done.iter().filter_map(|f| newest(&f.stats)).max()) {
        meta.past_from = Some(meta.past_from.unwrap_or_default().max((v as u64, us as u64 / 1000)));
    }
    replace(dmeta, &done, vec![]);
    true
}

/// The places in file `f` of the rows `dead` names (sorted (`_row_id`, `_version`) pairs): from its
/// lineage (a row's id is its first plus its place), else from its rows' own system columns, read
/// in order, every row (the ones deleted before too, whose places count).
async fn positions_of(lake: &Lake, meta: &TableMeta, f: &DataFile, dead: &[(i64, i64)]) -> Result<Vec<u64>> {
    use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
    if let Some(l) = f.lineage {
        let ids = l.first..l.first + f.rows as i64;
        return Ok(dead.iter().filter(|(id, v)| *v == l.version as i64 && ids.contains(id)).map(|(id, _)| (id - l.first) as u64).collect());
    }
    let s = schema(&[(crate::sys::ROW_ID.into(), "Int64".into()), (crate::sys::VERSION.into(), "Int64".into())])?;
    let every = DataFile { deletes: vec![], ..f.clone() };
    let (mut out, mut at) = (vec![], 0u64);
    for b in crate::query::files_once(lake, &lake.session_with(1), &[&every], meta, &s).await?.collect().await? {
        let (ids, versions) = (b.column(0).as_primitive::<Int64Type>(), b.column(1).as_primitive::<Int64Type>());
        for i in 0..b.num_rows() {
            if dead.binary_search(&(ids.value(i), versions.value(i))).is_ok() {
                out.push(at + i as u64);
            }
        }
        at += b.num_rows() as u64;
    }
    Ok(out)
}

/// Every row id of a file, by place (its deletes aside): from its lineage, or its `_row_id` column.
pub async fn ids_of(lake: &Lake, meta: &TableMeta, f: &DataFile) -> Result<Vec<i64>> {
    use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
    if let Some(l) = f.lineage {
        return Ok((l.first..l.first + f.rows as i64).collect());
    }
    anyhow::ensure!(f.sys, "{}: its rows have no ids (written before Pondra 0.19)", f.path);
    let (s, every) = (schema(&[(crate::sys::ROW_ID.into(), "Int64".into())])?, DataFile { deletes: vec![], ..f.clone() });
    let mut out = vec![];
    for b in crate::query::files_once(lake, &lake.session_with(1), &[&every], meta, &s).await?.collect().await? {
        out.extend(b.column(0).as_primitive::<Int64Type>().values());
    }
    Ok(out)
}

/// Iceberg's position deletes (format v2): each row a data file's name, as the table's Iceberg
/// metadata gives it, and a row's place in that file, sorted, in one Parquet file in the table's
/// folder (`_deletes/`). Pondra's own deletes are kept so (`purge`); its reads skip them, and other
/// engines read them as they read their own. The file's path in the lake, rows and bytes.
pub async fn write_positions(lake: &Lake, folder: &str, mut rows: Vec<(String, u64)>) -> Result<(String, u64, u64)> {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    rows.sort();
    let field = |n: &str, t: DataType, id: i64| Field::new(n, t, false).with_metadata([("PARQUET:field_id".to_string(), id.to_string())].into());
    let schema = std::sync::Arc::new(Schema::new(vec![field("file_path", DataType::Utf8, 2147483546), field("pos", DataType::Int64, 2147483545)]));
    let columns: Vec<datafusion::arrow::array::ArrayRef> = vec![std::sync::Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0.as_str()))), std::sync::Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1 as i64)))];
    let mut buf = vec![];
    let props = WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default())).build();
    let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))?;
    w.write(&RecordBatch::try_new(schema, columns)?)?;
    w.close()?;
    let (path, bytes) = (format!("data/{folder}/_deletes/{}.parquet", uuid::Uuid::new_v4()), buf.len() as u64);
    lake.put(&path, buf).await?;
    Ok((path, rows.len() as u64, bytes))
}

/// The old rows of the changes in (after, upto]: (`_row_id`, `_version`) pairs, from `{t}$deleted` files.
async fn dead(lake: &Lake, gone: &[DataFile], after: u64, upto: u64) -> Result<Vec<(i64, i64)>> {
    use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
    let mut out = vec![];
    for b in dead_rows(lake, gone, after, upto).await?.collect().await? {
        let (ids, versions) = (b.column(0).as_primitive::<Int64Type>(), b.column(1).as_primitive::<Int64Type>());
        out.extend(ids.iter().zip(versions.iter()).filter_map(|(i, v)| Some((i?, v?))));
    }
    Ok(out)
}

async fn dead_rows(lake: &Lake, gone: &[DataFile], after: u64, upto: u64) -> Result<datafusion::prelude::DataFrame> {
    use datafusion::prelude::{col, lit};
    let paths: Vec<String> = gone.iter().map(|f| lake.full(&f.path)).collect();
    let ctx = lake.session();
    let within = col(crate::sys::VERSION).between(lit(after as i64 + 1), lit(upto as i64));
    Ok(match paths.is_empty() {
        true => ctx.read_empty()?.select(vec![lit(0i64).alias("__id"), lit(0i64).alias("__v")])?.limit(0, Some(0))?,
        false => ctx.read_parquet(paths, ParquetReadOptions::default()).await?.filter(within)?.select(vec![col(crate::sys::ROW_ID).alias("__id"), col("_old_version").alias("__v")])?,
    })
}

/// A keyed table's files to merge next: the newest generations (a tiering round's files, one per
/// partition, share an `ord`), going back while each older one is at most twice the size of
/// everything newer (so a merge rewrites data of similar size, and the big base is only rewritten
/// once the rest reaches half of it). A run must be consecutive in `ord`: merging around a
/// generation would let an older version jump over a newer one. At least 2 generations; the newest
/// 8 when sizes are too uneven to form a run.
fn run(files: &[DataFile]) -> Vec<DataFile> {
    let mut gens: Vec<(u64, u64)> = vec![]; // (ord, rows), oldest first
    let mut by_age = files.to_vec();
    by_age.sort_by_key(|f| f.ord);
    for f in &by_age {
        match gens.last_mut() {
            Some((ord, rows)) if *ord == f.ord => *rows += f.rows,
            _ => gens.push((f.ord, f.rows)),
        }
    }
    let (mut n, mut rows) = (1, gens.last().map_or(0, |g| g.1));
    while n < gens.len() && gens[gens.len() - n - 1].1 <= 2 * rows.max(1) {
        rows += gens[gens.len() - n - 1].1;
        n += 1;
    }
    let n = if n >= 2 { n } else { 8.min(gens.len()) };
    let from = gens.get(gens.len().saturating_sub(n)).map_or(0, |g| g.0);
    by_age.into_iter().filter(|f| f.ord >= from).collect()
}

/// A small file's size class, each 4× the last (under 4 MB, under 16, under 64). A merge's file
/// moves up a class, so it waits for others of its size instead of being merged again with every
/// new one: merging the newest files into everything before them rewrote a streamed table every
/// minute or so, and every query read it cold from Parquet until the hot columns had it again.
fn class(bytes: u64) -> u32 { (bytes >> 20).max(1).ilog2() / 2 }

/// Swap `old` files for `new` ones; the old ones are deleted after the retention period.
fn replace(meta: &mut TableMeta, old: &[DataFile], new: Vec<DataFile>) {
    meta.files.retain(|f| !old.iter().any(|o| o.path == f.path));
    meta.discard(old);
    meta.files.extend(new.into_iter().map(|f| DataFile { sketch: Default::default(), ..f })); // (rows the table's sketches already saw)
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
    Squash { files: Vec<DataFile> },           // keyed: a run of newer files -> one (delete markers kept)
    Positions { files: Vec<DataFile>, gone: Vec<DataFile>, after: u64, upto: u64 }, // the places in these files (one partition's) of the old rows `gone` (`{t}$deleted`'s) holds for changes in (after, upto], as a delete file: the files' records with it
}

/// Run jobs round-robin on the live nodes (each round starts where the last one stopped, so
/// single jobs rotate too) and collect the files they wrote. A round with a job that failed
/// commits nothing, so what its other jobs wrote goes at once: left to the orphan sweep (a day
/// on), a disk the lake had filled filled again with the files of the rounds that failed on it.
async fn deal(lake: &Lake, jobs: Vec<Job>, nodes: &[String], me: &str) -> Result<Vec<DataFile>> {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let paths = |f: &DataFile| std::iter::once(f.path.clone()).chain(f.delete_files().cloned()).collect::<Vec<_>>();
    // (a `Positions` job hands its files back with a delete file more: never theirs to delete)
    let given: std::collections::HashSet<String> = jobs.iter().flat_map(|j| match &j.kind {
        Kind::Merge { files } | Kind::Squash { files } | Kind::Positions { files, .. } => files.iter().flat_map(paths).collect(),
        Kind::Fold { .. } | Kind::Compact { .. } => vec![],
    }).collect();
    let runs = jobs.into_iter().map(|job| {
        let node = &nodes[NEXT.fetch_add(1, Relaxed) % nodes.len()];
        async move {
            if node == me {
                return run_job(lake, job).await;
            }
            let r = crate::cluster::http().post(crate::tls::url(&format!("{node}/cluster/job"))).json(&job).send().await?;
            anyhow::ensure!(r.status().is_success(), "job on {node}: {}", r.text().await?);
            Ok(r.json().await?)
        }
    });
    let (written, failed): (Vec<_>, Vec<_>) = futures::future::join_all(runs).await.into_iter().partition(|r| r.is_ok());
    let written: Vec<DataFile> = written.into_iter().flat_map(Result::unwrap).collect();
    if let Some(Err(e)) = failed.into_iter().next() {
        let new = written.iter().flat_map(paths).filter(|p| !given.contains(p));
        futures::stream::iter(new).for_each_concurrent(16, |p| async move { lake.delete(&p).await }).await;
        return Err(e);
    }
    Ok(written)
}

/// Do one job here; returns the Parquet files written.
pub async fn run_job(lake: &Lake, Job { table, meta, kind }: Job) -> Result<Vec<DataFile>> {
    let meta = crate::sys::with_sys(&meta); // (files carry every row's system columns)
    // A job holds its rows and the Parquet file it is writing, so what a node runs at once is
    // bounded by the data behind them, not by their number: small merges go side by side, big
    // ones take turns (however many the leader deals out — it waits for them all anyway).
    static SLOTS: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    let budget = (crate::store::memory_limit() / 4 >> 20).max(256);
    let slots = SLOTS.get_or_init(|| tokio::sync::Semaphore::new(budget));
    let mb = |files: &[DataFile]| (files.iter().map(|f| f.bytes).sum::<u64>() >> 20) as usize;
    let takes = match &kind {
        Kind::Merge { files } | Kind::Squash { files } | Kind::Positions { files, .. } => mb(files),
        Kind::Compact { .. } => mb(&meta.files),
        Kind::Fold { .. } => 32,
    };
    let _slot = slots.acquire_many(takes.clamp(1, budget) as u32).await?;
    // (Clustered append tables get what keyed tables get for their key: small row groups, bloom filters.)
    // A keyed table's first file has nothing older to shadow: it drops delete markers (and expired
    // rows, and a view's emptied groups) like a full compaction, and is as complete as one. Only
    // the round's first job writes it: the others' older rows are in that file (invariant 93).
    let first = !meta.key.is_empty() && meta.files.is_empty() && matches!(kind, Kind::Fold { after, .. } if after == meta.tiered);
    let (keys, whole) = (if meta.key.is_empty() { meta.cluster.clone() } else { meta.key.clone() }, first || matches!(kind, Kind::Compact { .. }));
    let (batches, ord) = match kind {
        Kind::Fold { after, upto, rows } if meta.key.is_empty() => {
            caught_up(lake, &table, after, upto, rows).await?;
            let rows = crate::query::tail_of(lake, &table, after, Some(upto), false, true, false).await?;
            let rows = match (rows.is_empty(), meta.cluster.len()) {
                (true, _) | (_, 0) => rows,
                (_, 1) => clustered(&meta, lake.session().read_batches(rows)?)?.collect().await?,
                _ => crate::hilbert::sort(&rows, &meta.cluster)?,
            };
            (rows, upto)
        }
        // Keyed tables: one row per key for this range of the log, delete markers included (they
        // still have to shadow what older files hold for that key).
        Kind::Fold { after, upto, rows } => {
            caught_up(lake, &table, after, upto, rows).await?;
            let part = TableMeta { files: vec![], tiered: after, ..meta.clone() };
            (latest(lake, &table, &part, upto, !first).await?, upto)
        }
        Kind::Compact { upto, rows } => {
            anyhow::ensure!(!meta.key.is_empty(), "only keyed tables have versions to compact");
            caught_up(lake, &table, meta.tiered, upto, rows).await?;
            (latest(lake, &table, &meta, upto, false).await?, upto)
        }
        // Newer versions of some keys: one row per key over just these files, delete markers kept
        // (they still shadow the older files left out), as new as the newest of them.
        Kind::Squash { files } => {
            let ord = files.iter().map(|f| f.ord).max().unwrap_or(0);
            let part = TableMeta { files, ..meta.clone() };
            (latest(lake, &table, &part, meta.tiered, true).await?, ord)
        }
        // The places of the old versions these files hold, in one delete file.
        Kind::Positions { files, gone, after, upto } => {
            let mut dead = dead(lake, &gone, after, upto).await?;
            dead.sort_unstable();
            let (mut rows, mut found) = (vec![], vec![]);
            for f in files {
                let at = positions_of(lake, &meta, &f, &dead).await?;
                rows.extend(at.iter().map(|p| (lake.full(&f.path), *p)));
                found.push((f, at.len() as u64));
            }
            if rows.is_empty() {
                return Ok(vec![]);
            }
            let (path, n, bytes) = write_positions(lake, meta.folder(&table), rows).await?;
            return Ok(found.into_iter().filter(|(_, k)| *k > 0).map(|(mut f, k)| {
                f.deletes.push(crate::scan::Delete::Positions { path: path.clone(), file: lake.full(&f.path), rows: n, bytes });
                f.deleted += k;
                f
            }).collect());
        }
        Kind::Merge { files } => {
            let schema = schema(&meta.columns)?;
            let session = lake.session();
            let read = || async { crate::query::files_once(lake, &session, &files.iter().collect::<Vec<_>>(), &meta, &schema).await };
            let rows = match meta.cluster.is_empty() {
                false if meta.cluster.len() > 1 => {
                    let rows = crate::hilbert::sort(&read().await?.collect().await?, &meta.cluster)?; // (what a merge takes fits: MERGE_BYTES)
                    let stream = futures::stream::iter(rows.into_iter().map(Ok));
                    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema.clone(), stream))
                }
                false => clustered(&meta, read().await?)?.execute_stream().await?,
                // One file after another, each in one partition: files that each held a narrow
                // range of a key (data that arrived in order) merge into one that still does.
                // (Several files in one read come in whatever order they're listed.)
                true => {
                    let (ctx, s, lake, m) = (lake.session_with(1), schema.clone(), lake.arc(), std::sync::Arc::new(meta.clone()));
                    let each = futures::stream::iter(files.clone()).then(move |f| {
                        let (ctx, s, lake, m) = (ctx.clone(), s.clone(), lake.clone(), m.clone());
                        async move {
                            let rows = crate::query::files_once(&lake, &ctx, &[&f], &m, &s).await.map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
                            rows.execute_stream().await
                        }
                    });
                    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema.clone(), futures::TryStreamExt::try_flatten(each)))
                }
            };
            let (ord, part) = (files.iter().map(|f| f.ord).max().unwrap_or(0), files[0].part.clone());
            let mut merged = write_stream(lake, &table, rows, 4_000_000, &keys, meta.key.is_empty(), None).await?;
            merged.iter_mut().for_each(|f| (f.ord, f.part) = (ord, part.clone()));
            return Ok(merged);
        }
    };
    write_parts(lake, &table, &meta, batches, &keys, ord, whole).await
}

/// A job's rows as files, one a partition.
async fn write_parts(lake: &Lake, table: &str, meta: &TableMeta, batches: Vec<RecordBatch>, keys: &[String], ord: u64, whole: bool) -> Result<Vec<DataFile>> {
    let parts = match &meta.partition {
        Some(spec) => split(spec, batches).await?,
        None => vec![(String::new(), batches)],
    };
    let mut files = vec![];
    for (part, batches) in parts {
        if let Some(f) = write_file(lake, table, &batches, keys, meta.key.is_empty()).await? {
            files.push(DataFile { ord, whole, part, ..f });
        }
    }
    Ok(files)
}

/// What a fold job of `from`'s log segments (after, upto] writes, written as files of `to`: a
/// branch's REFRESH takes its base's rows not yet in files so (`branch::refresh`), with their
/// system columns; a keyed table's one row per key, delete markers kept.
pub async fn fold_into(from: &Lake, to: &Lake, table: &str, meta: &TableMeta, after: u64, upto: u64) -> Result<Vec<DataFile>> {
    let meta = crate::sys::with_sys(meta);
    let (keys, batches) = match meta.key.is_empty() {
        true => (meta.cluster.clone(), crate::query::tail_of(from, table, after, Some(upto), false, true, false).await?),
        false => (meta.key.clone(), latest(from, table, &TableMeta { files: vec![], tiered: after, ..meta.clone() }, upto, true).await?),
    };
    write_parts(to, table, &meta, batches, &keys, upto, false).await
}

/// A partitioned table's rows, one group per partition value (`day(ts)`: the day, `col`: the value).
pub async fn split(spec: &str, batches: Vec<RecordBatch>) -> Result<Vec<(String, Vec<RecordBatch>)>> {
    use datafusion::arrow::{array::{AsArray, UInt32Array}, compute::{concat_batches, take_record_batch}};
    let Some(first) = batches.first() else { return Ok(vec![]) };
    let all = concat_batches(&first.schema(), &batches)?;
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_batch("t", all.clone())?;
    let values = ctx.sql(&format!("SELECT {} FROM t", partition_expr(spec))).await?.collect().await?;
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut i = 0u32;
    for b in &values {
        let text = datafusion::arrow::compute::cast(b.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
        for v in text.as_string::<i32>().iter() {
            groups.entry(v.unwrap_or("null").to_string()).or_default().push(i);
            i += 1;
        }
    }
    groups.into_iter().map(|(p, rows)| Ok((p, vec![take_record_batch(&all, &UInt32Array::from(rows))?]))).collect()
}

/// A partition spec as SQL: `day(ts)` → `date_trunc('day', "ts")`; a plain column → itself.
pub fn partition_expr(spec: &str) -> String {
    match spec.split_once('(').map(|(f, c)| (f.trim(), c.trim_end_matches(')').trim())) {
        Some((unit @ ("year" | "month" | "day" | "hour"), col)) => format!("date_trunc('{unit}', \"{col}\")"),
        _ => format!("\"{}\"", spec.trim()),
    }
}

/// A partition spec is a column, or year/month/day/hour of a timestamp or date column.
pub fn check_partition(spec: &str, columns: &[(String, String)]) -> Result<()> {
    let (unit, col) = match spec.split_once('(') {
        Some((f, c)) => (Some(f.trim()), c.trim_end_matches(')').trim()),
        None => (None, spec.trim()),
    };
    let t = columns.iter().find(|(n, _)| n == col).map(|(_, t)| t.as_str());
    let time = t.is_some_and(|t| t.starts_with("Timestamp") || t.starts_with("Date"));
    anyhow::ensure!(t.is_some() && (unit.is_none() || (matches!(unit, Some("year" | "month" | "day" | "hour")) && time)), "PARTITION BY: a column, or year/month/day/hour(a timestamp column)");
    Ok(())
}

/// Append tables with `cluster_by`: every file's rows sorted by that column (folds and merges
/// alike), so each row group covers a narrow range of it and a filter on it skips the rest by
/// min/max statistics and bloom filters — in Pondra and in any engine reading the Parquet. (Two
/// or more columns: along a Hilbert curve through them, `hilbert.rs`.)
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
    let rows = ctx.sql(&latest_sql(meta, "__raw", true, keep_deleted)).await?.collect().await?;
    if meta.cluster.len() > 1 { crate::hilbert::sort(&rows, &meta.cluster) } else { Ok(rows) }
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
            let unread = lake.cat.scan::<Segment>(&seg_key(done + 1), &seg_key(floor + 1)).await?.iter().any(|(_, s)| s.rows_of(&task.source) > 0);
            if done < floor && unread {
                floor = done; // the task still has to read these (tasks skip runs with nothing new)
            }
        }
    }
    // (a view that runs its query again learns from the log what changed since its last run, and a
    // view kept by key reads what other engines' commits took out of its source there)
    for (key, v) in lake.cat.scan::<crate::views::View>("v/", "v0").await?.into_iter().filter(|(_, v)| v.rerun.is_some()) {
        let done = lake.cat.get(&producer_key(&crate::rerun::producer(&key[2..]))).await?.unwrap_or(0);
        let follows = crate::rerun::follows(&v);
        if done < floor && lake.cat.scan::<Segment>(&seg_key(done + 1), &seg_key(floor + 1)).await?.iter().any(|(_, s)| follows.iter().any(|t| crate::rerun::moved(s, t))) {
            floor = done;
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
    // (`--changelog-secs`: the log is also a change feed, kept that long for `/watch?after=` replays.)
    let changelog = std::env::var("PONDRA_CHANGELOG_SECS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) * 1000;
    let log_cutoff = cutoff.min(crate::log::now_ms().saturating_sub(changelog));
    let (segs, kept): (Vec<(String, Segment)>, Vec<(String, Segment)>) = lake.cat.scan::<Segment>(&seg_key(1), &seg_key(floor + 1)).await?
        .into_iter().partition(|(_, s)| s.ts_ms < log_cutoff);
    // A replaced file stays while a segment kept from before its replacement may name it: a file
    // commit's files are read from them by the log's readers (a task behind, `/watch?after=`).
    let next = match kept.first() {
        Some((_, s)) => Some(s.ts_ms),
        None => lake.cat.scan::<Segment>(&seg_key(floor + 1), &seg_key(floor + 1001)).await?.first().map(|(_, s)| s.ts_ms),
    };
    // Files live when a branch was made stay while its pin does (ADR-047).
    let pins = crate::branch::pins(lake).await?;
    let files_cutoff = next.map_or(cutoff, |ts| cutoff.min(ts)).min(pins.map_or(u64::MAX, |p| p.0));
    let mut dead: Vec<String> = segs.iter().filter(|(_, s)| !s.path.is_empty()).map(|(_, s)| s.path.clone()).collect();
    let mut deletes: Vec<String> = segs.iter().map(|(k, _)| k.clone()).collect();
    deletes.extend(segs.iter().filter(|(_, s)| s.path.is_empty()).map(|(k, _)| data_key(k[2..].parse().unwrap_or(0))));
    // Dropped tables kept past their time (ADR-043) go: their files are then orphans, swept a day on.
    #[derive(serde::Deserialize)]
    struct Kept { at_ms: u64, keep_ms: u64 }
    let now = crate::log::now_ms();
    // (one dropped since a branch was made stays: the branch may read its files)
    deletes.extend(lake.cat.scan::<Kept>("dt/", "dt0").await?.into_iter().filter(|(_, d)| now >= d.at_ms + d.keep_ms && pins.is_none_or(|p| d.at_ms < p.0)).map(|(k, _)| k));
    // A folder a clone shares (ADR-043): files there go only by the orphan sweep, which counts
    // every table listing them, never as one table's garbage.
    let mut shared: std::collections::HashSet<String> = tables.iter().flat_map(|(_, m)| m.shares.iter().map(|f| format!("data/{f}/"))).collect();
    for (_, d) in crate::ddl::dropped(lake).await? {
        shared.extend(d.meta.shares.iter().chain(d.deleted.iter().flat_map(|m| &m.shares)).map(|f| format!("data/{f}/")));
    }
    let mut puts = vec![];
    for ((key, mut meta), idle) in tables.into_iter().zip(idle) {
        let (old, keep): (Vec<_>, Vec<_>) = meta.garbage.drain(..).partition(|(_, ts)| *ts < files_cutoff);
        let (deletes, kept): (Vec<_>, Vec<_>) = meta.garbage_deletes.drain(..).partition(|(_, ts)| *ts < files_cutoff);
        if !old.is_empty() || !deletes.is_empty() || idle {
            let named = if deletes.is_empty() { Default::default() } else { named_deletes(lake, &meta).await? };
            dead.extend(old.into_iter().chain(deletes).map(|(p, _)| p).filter(|p| !named.contains(p) && !shared.iter().any(|f| p.starts_with(f))));
            (meta.garbage, meta.garbage_deletes) = (keep, kept);
            let garbage: std::collections::HashSet<&String> = meta.garbage.iter().map(|(p, _)| p).collect();
            meta.replaced.retain(|f| garbage.contains(&f.path));
            puts.push((key, json(&meta)));
        }
    }
    if !puts.is_empty() || !deletes.is_empty() {
        if deletes.iter().all(|k| k.starts_with("s/") || k.starts_with("d/")) {
            puts.push((crate::store::QUIET.into(), json(&true))); // (marks moved, segments and garbage let go: no read sees it, so remembered answers stay)
        }
        lake.cat.commit(puts, &deletes).await?;
    }
    lake.cat.wait_durable(lake.cat.committed()).await; // (replicated acks: forget objects only once the bucket has)
    futures::stream::iter(dead).for_each_concurrent(16, |path| async move { lake.delete(&path).await }).await; // (one round trip each)
    collect_orphans(lake).await
}

/// The position-delete files the table's files name: a replaced file's stay while another names
/// them, and go with the last (the orphan sweep's).
async fn named_deletes(lake: &Lake, meta: &TableMeta) -> Result<std::collections::HashSet<String>> {
    let mut named: std::collections::HashSet<String> = meta.files.iter().flat_map(|f| f.delete_files().cloned()).collect();
    for m in crate::manifest::list(lake, meta).await? {
        named.extend(crate::manifest::files(lake, &m).await?.iter().flat_map(|f| f.delete_files().cloned()));
    }
    Ok(named)
}

/// Delete objects that no catalog entry points to and that are a day old: segments a node wrote
/// whose commit never happened, Parquet files from crashed tiering or insert jobs. (A day, because
/// a huge bulk insert writes its files long before it commits them.) One part of the lake at a
/// time, never the whole bucket at once (C5): `log/`, each table's folder, and the folders of
/// tables gone, each looked at once a day at most, and in each hourly round only as many as a day
/// of rounds needs to cover them all.
async fn collect_orphans(lake: &Lake) -> Result<()> {
    use futures::TryStreamExt;
    const HOUR: u64 = 3_600_000;
    const DAY: u64 = 24 * HOUR;
    static SWEPT: std::sync::LazyLock<std::sync::Mutex<(u64, std::collections::HashMap<String, u64>)>> = std::sync::LazyLock::new(Default::default);
    let now = crate::log::now_ms();
    if now - SWEPT.lock().unwrap().0 < HOUR {
        return Ok(());
    }
    SWEPT.lock().unwrap().0 = now;
    crate::branch::sweep(lake).await?; // (pins of branches gone)
    // An object that was there when a branch was made may be one of its files (a table dropped
    // since, purged): it stays while that branch's pin does (ADR-047).
    let pinned = crate::branch::pins(lake).await?.map_or(0, |p| p.1) as i64;
    use object_store::path::Path;
    let key = |raw: &str| Path::from(raw).to_string(); // (a path as the store lists it: a few characters escaped)
    let mut tables = lake.cat.scan::<TableMeta>("t/", "t0").await?;
    for (n, d) in crate::ddl::dropped(lake).await? {
        tables.push((format!("t/{n}"), d.meta)); // (kept to be undropped: its files are in use)
        tables.extend(d.deleted.map(|m| (format!("t/{}", crate::sys::deleted(&n)), m)));
    }
    // (a folder's files are listed by its table, a table kept under its old name's, and its clones)
    let mut folders: std::collections::HashMap<String, Vec<&TableMeta>> = Default::default();
    for (k, m) in &tables {
        for f in std::iter::once(m.folder(&k[2..])).chain(m.shares.iter().map(String::as_str)) {
            folders.entry(key(&format!("data/{f}"))).or_default().push(m);
        }
    }
    let listed = lake.store.list_with_delimiter(Some(&Path::from("data"))).await?; // (one request a thousand folders)
    let gone = listed.common_prefixes.iter().map(|p| p.to_string()).filter(|p| !folders.contains_key(p));
    let parts: Vec<String> = std::iter::once("log".to_string()).chain(folders.keys().cloned()).chain(gone).collect();
    let due: Vec<String> = {
        let swept = &SWEPT.lock().unwrap().1;
        let mut due: Vec<(u64, String)> = parts.iter().map(|p| (swept.get(p).copied().unwrap_or(0), p.clone())).filter(|(at, _)| now - at >= DAY).collect();
        due.sort();
        due.into_iter().take(parts.len().div_ceil(24).max(1)).map(|(_, p)| p).collect()
    };
    for part in due {
        // A Bloom filter of the paths in use there, sized for them (a few megabytes for a million files: ADR-013).
        let named = |f: &DataFile| std::iter::once(f.path.clone()).chain(f.delete_files().cloned()).collect::<Vec<_>>();
        let used = match (part.as_str(), folders.get(&part)) {
            ("log", _) => {
                let segments = lake.cat.scan::<Segment>("s/", "s0").await?;
                let mut used = Seen::of(segments.len());
                segments.iter().for_each(|(_, s)| used.add(&key(&s.path)));
                used
            }
            (_, Some(all)) => {
                let mut used = Seen::of(all.iter().map(|m| m.files.len() + m.garbage.len() + m.garbage_deletes.len() + m.sealed.as_ref().map_or(0, |s| s.files as usize) + 64).sum());
                for m in all {
                    for manifest in crate::manifest::list(lake, m).await? {
                        crate::manifest::files(lake, &manifest).await?.iter().flat_map(named).for_each(|p| used.add(&key(&p)));
                        used.add(&key(&manifest.path));
                    }
                    m.sealed.iter().for_each(|s| used.add(&key(&s.list)));
                    m.files.iter().flat_map(named).for_each(|p| used.add(&key(&p)));
                    m.garbage.iter().chain(&m.garbage_deletes).for_each(|(p, _)| used.add(&key(p)));
                }
                used
            }
            _ => Seen::of(0), // (a table gone: nothing there is in use)
        };
        let at = Path::parse(&part)?; // (as listed: already escaped)
        let objects: Vec<object_store::ObjectMeta> = lake.store.list(Some(&at)).try_collect().await?;
        for o in objects {
            let old = now as i64 - o.last_modified.timestamp_millis() > DAY as i64;
            if old && o.last_modified.timestamp_millis() > pinned && !used.has(o.location.as_ref()) && !crate::delta::open_format(o.location.as_ref()) {
                let _ = object_store::ObjectStoreExt::delete(&lake.store, &o.location).await; // (by the listed path itself)
            }
        }
        SWEPT.lock().unwrap().1.insert(part, now);
    }
    Ok(())
}

/// A Bloom filter of paths in use: a path it doesn't have is certainly not in use; one it has may
/// or may not be, and simply survives to the next round (about one in a hundred does).
struct Seen(Vec<u64>);

impl Seen {
    /// Room for `n` paths at ten bits each.
    fn of(n: usize) -> Seen { Seen(vec![0; (n.max(4096) * 10 / 64).next_power_of_two()]) }

    fn bits(&self, path: &str) -> [usize; 7] {
        let h = std::hash::BuildHasher::hash_one(&std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default(), path);
        let (a, b) = ((h >> 32) as usize, (h as u32 | 1) as usize); // (double hashing: b is odd, so it strides)
        std::array::from_fn(|i| (a.wrapping_add(i.wrapping_mul(b))) % (self.0.len() * 64))
    }

    fn add(&mut self, path: &str) {
        for bit in self.bits(path) {
            self.0[bit / 64] |= 1 << (bit % 64);
        }
    }

    fn has(&self, path: &str) -> bool { self.bits(path).into_iter().all(|bit| self.0[bit / 64] >> (bit % 64) & 1 == 1) }
}

/// Parquet writer. Keyed tables are written for lookups as well as scans — sorted by key (see
/// `latest_sql`), with a bloom filter per key column, and in small row groups and pages, so reading
/// one key touches one page instead of a million rows.
fn writer<'a>(buf: &'a mut Vec<u8>, batch: &RecordBatch, keys: &[String]) -> Result<ArrowWriter<&'a mut Vec<u8>>> {
    let mut props = WriterProperties::builder().set_compression(codec());
    if !keys.is_empty() {
        props = props.set_max_row_group_row_count(Some(256 << 10));
    }
    for k in keys {
        props = props.set_column_bloom_filter_enabled(k.as_str().into(), true);
    }
    // System columns: row ids count up, deltas of a few bits each; versions and times repeat per
    // commit, and plain values the codec squeezes are what costs least to write (the files come
    // out the size they were without them).
    for c in crate::sys::NAMES.iter().filter(|c| batch.schema().index_of(c).is_ok()) {
        let path: datafusion::parquet::schema::types::ColumnPath = (*c).into();
        let encoding = if *c == crate::sys::ROW_ID { datafusion::parquet::basic::Encoding::DELTA_BINARY_PACKED } else { datafusion::parquet::basic::Encoding::PLAIN };
        props = props.set_column_dictionary_enabled(path.clone(), false).set_column_encoding(path, encoding);
    }
    Ok(ArrowWriter::try_new_with_options(buf, batch.schema(), plain(props.build()))?)
}

/// Writer options with the Parquet file's own types only: no Arrow schema in its footer, which would
/// have other engines' Arrow read strings as views (`string_view`), which PyArrow can't yet take
/// rows of — PyIceberg applying a position delete, say.
pub fn plain(props: WriterProperties) -> datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions {
    datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions::new().with_properties(props).with_skip_arrow_metadata(true)
}

/// The codec data files are written with (`PONDRA_CODEC`). LZ4 by default: it decodes fastest,
/// so scans are CPU-cheap (TPC-H runs 13-20% faster than on ZSTD, with files a third bigger).
/// `zstd` where storage or bandwidth costs more than CPU; `snappy` or `none` also work.
fn codec() -> Compression {
    match std::env::var("PONDRA_CODEC").unwrap_or_default().to_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::try_new(1).expect("a valid level")),
        "snappy" => Compression::SNAPPY,
        "none" => Compression::UNCOMPRESSED,
        _ => Compression::LZ4_RAW,
    }
}

/// Write batches as one Parquet file (none if there are no rows).
/// One Parquet file of these rows; with `stats`, it carries its columns' min/max (append tables).
pub async fn write_file(lake: &Lake, table: &str, batches: &[RecordBatch], keys: &[String], stats: bool) -> Result<Option<DataFile>> {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if rows == 0 {
        return Ok(None);
    }
    let meta = lake.cat.get::<TableMeta>(&table_key(table)).await?;
    let ids = field_ids(meta.as_ref(), &batches[0].schema());
    let folder = meta.as_ref().map_or(table, |m| m.folder(table));
    let mut buf = vec![];
    let mut w = writer(&mut buf, &batches[0].clone().with_schema(ids.clone())?, keys)?;
    for b in batches {
        w.write(&b.clone().with_schema(ids.clone())?)?;
    }
    let footer = w.close()?; // (its statistics: the file's min and max, without a second pass)
    let (path, bytes) = (format!("data/{folder}/{}.parquet", uuid::Uuid::new_v4()), buf.len() as u64);
    lake.put(&path, buf).await?;
    maybe_crash("after_parquet_put");
    let (stats, nulls, sketch) = match stats {
        true => (crate::manifest::stats(batches, Some(&footer)), Some(crate::manifest::nulls(batches)), crate::sketch::of(batches)),
        false => Default::default(),
    };
    let sys = batches[0].schema().index_of(crate::sys::ROW_ID).is_ok();
    Ok(Some(DataFile { path, rows: rows as u64, bytes, ord: 0, whole: false, stats, part: String::new(), nulls, sketch, sys, ..Default::default() }))
}

/// The columns as Iceberg knows them (`iceberg.rs`), each with its field id: a column's place
/// among the stored ones (a renamed column keeps it), the rows' system columns 1,000,001 on.
/// Readers that match a file's columns to the table's by id (Polars' `scan_iceberg`) need them.
fn field_ids(meta: Option<&TableMeta>, schema: &datafusion::arrow::datatypes::SchemaRef) -> datafusion::arrow::datatypes::SchemaRef {
    use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY as ID;
    // (a table an INSERT makes: its columns will be these, in this order)
    let columns: Vec<String> = match meta {
        Some(m) => m.columns.iter().map(|(c, _)| c.clone()).collect(),
        None => schema.fields().iter().map(|f| f.name().clone()).filter(|c| !crate::sys::NAMES.contains(&c.as_str())).collect(),
    };
    let id = |name: &str| match columns.iter().position(|c| c == name) {
        Some(i) => Some(i + 1),
        None => crate::sys::NAMES.iter().position(|c| *c == name).map(|i| 1_000_001 + i),
    };
    let fields: Vec<_> = schema.fields().iter().map(|f| match id(f.name()) {
        Some(i) => {
            let mut m = f.metadata().clone();
            m.insert(ID.to_string(), i.to_string());
            std::sync::Arc::new(f.as_ref().clone().with_metadata(m))
        }
        None => f.clone(),
    }).collect();
    std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Stream a query result into Parquet files of up to `max_rows` each (bulk INSERT … SELECT).
pub async fn write_stream(lake: &Lake, table: &str, mut stream: SendableRecordBatchStream, max_rows: usize, keys: &[String], stats: bool, partition: Option<&str>) -> Result<Vec<DataFile>> {
    let (mut files, mut pending, mut n) = (vec![], vec![], 0);
    loop {
        let batch = stream.next().await.transpose()?;
        let end = batch.is_none();
        if let Some(b) = batch {
            n += b.num_rows();
            pending.push(b);
        }
        if n >= max_rows || (end && n > 0) {
            let groups = match partition {
                Some(spec) => split(spec, std::mem::take(&mut pending)).await?,
                None => vec![(String::new(), std::mem::take(&mut pending))],
            };
            for (part, batches) in groups {
                files.extend(write_file(lake, table, &batches, keys, stats).await?.map(|f| DataFile { part, ..f }));
            }
            n = 0;
        }
        if end {
            return Ok(files);
        }
    }
}
