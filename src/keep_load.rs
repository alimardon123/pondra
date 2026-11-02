//! `CREATE TABLE t AS FROM 'f.parquet'` keeps the file's row groups: each one is copied, undecoded, into our own
//! files (`keep_parquet`), and each copy's ranges, NULLs and sketches are read back from its first columns, as the
//! writer of a written file keeps them (`tier::Encoding`). What doesn't qualify is written as today
//! (`write::write_files`): `kept` answers None, and a reason is not an error the user sees. Design:
//! designs/l2-load-keeps-parquet.md. `PONDRA_KEEP_PARQUET=0` turns it off.
use crate::adopt::fits_as_written;
use crate::answer;
use crate::expander;
use crate::ext;
use crate::keep_parquet::{self, Cut, Kept, Limits, Sys};
use crate::log::Reserved;
use crate::manifest::Seen;
use crate::sketch::Sketches;
use crate::store::{DataFile, Lake, TableMeta};
use crate::text::{NAMES, ROW_ID};
use crate::tier;
use crate::write::{recorded, Files};
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatchReader;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use futures::{StreamExt, TryStreamExt};
use object_store_df::path::Path;
use object_store_df::{ObjectMeta, ObjectStore, ObjectStoreExt as _};
use std::sync::Arc;

/// The files of a `CREATE TABLE … AS FROM 'f.parquet'`'s INSERT into `table` (`job` made the table, `stored` is it):
/// Some when every check holds (`attempt`), else None and the INSERT is written as today. An error before the
/// commit is None too, and says why. `stamp` is the INSERT's (`write::stamp`): the kept files carry their rows'
/// system columns, as a written file does, so they are plain files of the table.
pub async fn kept(query: &Lake, target: &Lake, table: &str, sql: &str, job: &str, stored: Option<&Arc<TableMeta>>, stamp: Option<Reserved>) -> Option<Files> {
    match attempt(query, target, table, sql, job, stored, stamp).await {
        Ok(Ok(files)) => Some(files),
        Ok(Err(_reason)) => None,
        Err(e) => {
            eprintln!("{table}: written as today, not kept: {e:#}");
            None
        }
    }
}

/// Keeps the files, or says why not (`Err`, in words). Each check comes before anything is copied, except the
/// file's own: every file is planned from its footer first, so a reason in the last one copies nothing.
async fn attempt(query: &Lake, target: &Lake, table: &str, sql: &str, job: &str, stored: Option<&Arc<TableMeta>>, stamp: Option<Reserved>) -> Result<Result<Files, String>> {
    if crate::env::get("PONDRA_KEEP_PARQUET").as_deref() == Some("0") {
        return Ok(Err("PONDRA_KEEP_PARQUET=0".into()));
    }
    let Some(stamp) = stamp else { return Ok(Err("no commit number reserved: the rows take a lineage".into())) };
    // The table: this statement's own, with nothing in it, and nothing a file as it is can't keep.
    let Some(meta) = stored else { return Ok(Err("no table".into())) };
    let plain = meta.made_by.as_deref() == Some(job) && meta.files.is_empty() && meta.sealed.is_none() && fits_as_written(meta) && meta.partition.is_none() && meta.cluster.is_empty() && meta.constraints.is_empty() && meta.identity.is_empty();
    if !plain || meta.rules()?.ruled() {
        return Ok(Err("the table isn't this statement's, or has a partition, a key, a rule or identity".into()));
    }
    if recorded(target, job).await? {
        return Ok(Err("the job is recorded: written as today answers it".into()));
    }
    // The query: a bare scan of one file, with no options.
    let Some(name) = bare_scan(sql) else { return Ok(Err("not a bare scan of a file".into())) };
    let Some(spec) = crate::ext_names::spec(&name) else { return Ok(Err("not a file".into())) };
    if spec.format != "parquet" || !spec.options.is_empty() {
        return Ok(Err("not a Parquet file with no options".into()));
    }
    let Some(listed) = ext::meta(query, &name).await? else { return Ok(Err("no file".into())) };
    if listed.files.is_empty() {
        return Ok(Err("no file matches".into()));
    }
    if listed.files.iter().any(|f| !f.part.is_empty() || f.outside.is_some()) {
        return Ok(Err("a file in a folder of partitions, or another engine's".into()));
    }
    // Every file planned from its footer before any is copied, in cuts (a kept file each) of a share of the whole, so the
    // copies keep every core busy: a load of one 230 MB file in two cuts used two cores. A share is at most `FILE_BYTES`
    // and at least `SHARE_FLOOR`; a file whose row groups don't fit a share's file is cut at `FILE_BYTES`.
    let schema = answer::schema(&meta.columns)?;
    let at_once = copies(target);
    let total: u64 = listed.files.iter().map(|f| f.bytes).sum();
    let share = usize::try_from(total / at_once as u64).unwrap_or(usize::MAX).clamp(SHARE_FLOOR, tier::FILE_BYTES);
    let limits = Limits { file: tier::FILE_BYTES, group: tier::ROW_GROUP_BYTES };
    let shared = Limits { file: share, ..limits };
    let mut jobs = vec![];
    for f in &listed.files {
        let (store, location) = ext::store(query, &f.path).await?;
        let object = ObjectMeta { location: location.clone(), last_modified: Default::default(), size: f.bytes, e_tag: None, version: None };
        let md = DFParquetMetadata::new(store.as_ref(), &object).with_page_index_policy(Some(PageIndexPolicy::Optional)).fetch_metadata().await.with_context(|| format!("{}: its Parquet footer", f.path))?;
        let cuts = match keep_parquet::plan(&md, &schema, &shared).or_else(|_| keep_parquet::plan(&md, &schema, &limits)) {
            Ok(cuts) => cuts,
            Err(why) => return Ok(Err(why)),
        };
        jobs.extend(cuts.into_iter().map(|cut| Job { store: store.clone(), location: location.clone(), md: md.clone(), cut }));
    }
    // Each cut's rows take ids from the INSERT's block on, in order, as `sys::stamp_stream` gives them.
    let rows: u64 = jobs.iter().map(|j| j.cut.groups.clone().map(|g| j.md.row_group(g).num_rows() as u64).sum::<u64>()).sum();
    if rows > 1 << 32 {
        return Ok(Err("more than 4,294,967,296 rows".into()));
    }
    let mut next = stamp.block << 32;
    let sys: Vec<Sys> = jobs.iter().map(|j| {
        let first = next as i64;
        next += j.cut.groups.clone().map(|g| j.md.row_group(g).num_rows() as u64).sum::<u64>();
        Sys { first, version: stamp.version as i64, at_us: (stamp.ms * 1000) as i64 }
    }).collect();
    // Then each cut is copied, `at_once` at a time, in order.
    let folder = meta.folder(table).to_string();
    let files = futures::stream::iter(jobs.into_iter().zip(sys)).map(|(cut, sys)| copy_one(target, &folder, schema.clone(), cut, sys)).buffered(at_once).try_collect::<Vec<DataFile>>().await?;
    let columns = meta.live().map(|(s, _, t)| Ok((s.to_string(), answer::dtype(t)?.to_string()))).collect::<Result<Vec<_>>>()?;
    Ok(Ok(Files::new(table, job, columns, files)))
}

/// The smallest share of a load a cut is made (`attempt`): a file of a few row groups is one cut.
const SHARE_FLOOR: usize = 32 << 20;

/// How many cuts are copied at once: one a core, each holding its piece of the source and the file kept of it (up to
/// `FILE_BYTES` each, as both are), within a quarter of the query memory, and at least two (`PONDRA_KEEP_AT_ONCE` for a
/// test: the copies' speed against their number).
fn copies(target: &Lake) -> usize {
    if let Some(n) = crate::env::parse::<usize>("PONDRA_KEEP_AT_ONCE") {
        return n.max(1);
    }
    let (_, memory) = target.memory();
    crate::env::cores().min(memory / (8 * tier::FILE_BYTES)).max(2)
}

/// The name of the file a query is a bare scan of (`SELECT * FROM "ext:…"`, as `front::expand` writes `FROM 'f'`):
/// None for any other query, one this parser doesn't read included.
fn bare_scan(sql: &str) -> Option<String> {
    let text = expander::parse_query(sql).ok()?.to_string();
    text.strip_prefix("SELECT * FROM \"").and_then(|t| t.strip_suffix('"')).filter(|n| crate::ext_names::is(n)).map(str::to_string)
}

/// One cut of one of the query's files: the range to read, and the footer its copy is made from.
struct Job {
    store: Arc<dyn ObjectStore>,
    location: Path,
    md: Arc<ParquetMetaData>,
    cut: Cut,
}

/// One cut read by range (never the whole file), copied, read back, and put as one of the table's files in `folder`.
async fn copy_one(target: &Lake, folder: &str, schema: Arc<Schema>, job: Job, sys: Sys) -> Result<DataFile> {
    let Job { store, location, md, cut } = job;
    let piece = store.get_range(&location, cut.bytes.clone()).await?;
    let (bytes, file) = tier::blocking(move || {
        let kept = keep_parquet::copy(&md, &schema, &cut, piece, Some(sys))?;
        entry(kept)
    })
    .await?;
    let path = format!("data/{folder}/{}.parquet", uuid::Uuid::new_v4());
    target.put(&path, bytes).await?;
    Ok(DataFile { path, ..file })
}

/// A kept file's bytes and its entry but for where it goes: its rows, and the ranges, NULLs and sketches a written file
/// of them would have (`tier::Encoding`'s, which see every row as it is written). Read back from its first 32 columns
/// and its system columns (`manifest::Seen` ranges both), decoded but not encoded, with no schema hint, so strings are
/// Utf8 as a written file's are.
fn entry(kept: Kept) -> Result<(Bytes, DataFile)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(kept.bytes.clone())?;
    let fields = builder.schema().fields().clone();
    let read = fields.iter().enumerate().filter(|(i, f)| *i < 32 || NAMES.contains(&f.name().as_str())).map(|(i, _)| i);
    let mask = ProjectionMask::roots(builder.parquet_schema(), read);
    let reader = builder.with_projection(mask).with_batch_size(8192).build()?;
    let schema = reader.schema();
    let (mut seen, mut sketches, mut rows) = (Seen::new(schema.clone()), Sketches::new(&schema), 0u64);
    for batch in reader {
        let batch = batch?;
        seen.add(&batch)?;
        sketches.add(&batch);
        rows += batch.num_rows() as u64;
    }
    ensure!(rows == kept.rows, "the copy reads {rows} rows, its footer says {}", kept.rows);
    let file = DataFile {
        rows,
        bytes: kept.bytes.len() as u64,
        stats: seen.stats(&kept.footer),
        nulls: Some(seen.nulls()),
        sketch: sketches.done(),
        sys: schema.index_of(ROW_ID).is_ok(),
        ..Default::default()
    };
    Ok((kept.bytes, file))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Date32Array, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::arrow::record_batch::RecordBatch;

    #[test]
    fn only_a_bare_scan_of_a_file_is_kept() {
        assert_eq!(bare_scan("SELECT * FROM \"ext:abc\"").as_deref(), Some("ext:abc"));
        assert_eq!(bare_scan("SELECT a FROM \"ext:abc\""), None, "a projection");
        assert_eq!(bare_scan("SELECT * FROM \"ext:abc\" WHERE true"), None, "a filter");
        assert_eq!(bare_scan("SELECT * FROM t"), None, "a table");
        assert_eq!(bare_scan("SELECT * FROM \"abc\""), None, "a quoted name that isn't a file");
        assert_eq!(bare_scan("SELECT ( FROM"), None, "a query this parser doesn't read");
    }

    /// The entry a written file of these rows gets, and the same entry read back from the bytes of a copy of it:
    /// ranges, NULLs and sketches equal, rows and bytes as the file's.
    #[test]
    fn a_kept_files_entry_is_a_written_files() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, false),
            Field::new("f", DataType::Float64, false),
            Field::new("d", DataType::Date32, false),
        ]));
        let batches: Vec<RecordBatch> = (0..3i64)
            .map(|k| {
                let n: Vec<i64> = (k * 1000..k * 1000 + 1000).collect();
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(Int64Array::from_iter(n.iter().map(|&v| (v != 5).then_some(v)))),
                    Arc::new(StringArray::from_iter_values(n.iter().map(|v| format!("s{}", v % 97)))),
                    Arc::new(Float64Array::from_iter_values(n.iter().map(|&v| v as f64 * 0.5))),
                    Arc::new(Date32Array::from_iter_values(n.iter().map(|&v| (v % 20_000) as i32))),
                ];
                RecordBatch::try_new(schema.clone(), columns).unwrap()
            })
            .collect();
        // What the writer keeps as it goes (`tier::Encoding`), and its footer.
        let (mut w, mut seen, mut sketches) = (tier::writer(schema.clone(), &[]).unwrap(), Seen::new(schema.clone()), Sketches::new(&schema));
        for b in &batches {
            seen.add(b).unwrap();
            sketches.add(b);
            w.write(b).unwrap();
        }
        let footer = w.finish().unwrap();
        let want = (seen.stats(&footer), seen.nulls(), sketches.done());
        let bytes = Bytes::from(std::mem::take(w.inner_mut()));
        let (_, got) = entry(Kept { bytes: bytes.clone(), footer, rows: 3000 }).unwrap();
        assert_eq!(got.rows, 3000);
        assert_eq!(got.bytes, bytes.len() as u64);
        assert!(!got.sys, "a file of no system columns");
        assert_eq!(got.stats, want.0, "ranges");
        assert_eq!(got.nulls, Some(want.1), "NULLs");
        assert_eq!(got.sketch, want.2, "sketches");
    }
}
