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
use datafusion::arrow::array::{Array, ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchReader};
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::common::ScalarValue;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
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
    let plain = meta.made_by.as_deref() == Some(job) && meta.files.is_empty() && meta.sealed.is_none() && fits_as_written(meta) && meta.partition.is_none() && meta.cluster.is_empty() && meta.constraints.is_empty() && !meta.list().columns.iter().any(|c| c.identity.is_some());
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
    let schema = answer::schema(&meta.columns())?;
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
/// `FILE_BYTES` each, as both are, and reserved from the query memory: `copy_one`), within a quarter of the query memory
/// free now, and at least two (`PONDRA_KEEP_AT_ONCE` for a test: the copies' speed against their number).
fn copies(target: &Lake) -> usize {
    if let Some(n) = crate::env::parse::<usize>("PONDRA_KEEP_AT_ONCE") {
        return n.max(1);
    }
    let (reserved, memory) = target.memory();
    at_once(crate::env::cores(), memory.saturating_sub(reserved))
}

/// `copies` on this many cores with this much query memory free.
fn at_once(cores: usize, free: usize) -> usize { cores.min(free / (8 * tier::FILE_BYTES)).max(2) }

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
    // (its piece and the file kept of it, reserved while they are held, so the pool counts them as it admits other work:
    // 11-03 audit M11)
    let held = MemoryConsumer::new("a Parquet load's copy").register(&target.rt.memory_pool);
    held.grow(2 * (cut.bytes.end - cut.bytes.start) as usize);
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
/// of them would have (`tier::Encoding`'s, which see every row as it is written: `manifest::Seen` ranges the first 32
/// columns and the system columns, `Sketches` the first 32). Read back whole, decoded but not encoded, with no schema
/// hint, so strings are Utf8 as a written file's are: every value is decoded (a string that isn't UTF-8 is refused), and
/// what its footer and page index say of its rows is checked against them (`Claims`), since a reader skips rows by
/// them. Any of that failing is an error, and the load is written as today.
fn entry(kept: Kept) -> Result<(Bytes, DataFile)> {
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(kept.bytes.clone(), options)?;
    let mut claims = Claims::new(builder.metadata(), builder.schema())?;
    let reader = builder.with_batch_size(8192).build()?;
    let schema = reader.schema();
    let (mut seen, mut sketches, mut rows) = (Seen::new(schema.clone()), Sketches::new(&schema), 0u64);
    for batch in reader {
        let batch = batch?;
        claims.add(&batch)?;
        seen.add(&batch)?;
        sketches.add(&batch);
        rows += batch.num_rows() as u64;
    }
    claims.done()?;
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


/// Whether what a kept file's footer and page index say of its rows holds: each row group's and each page's least and
/// greatest value bound its rows (NaN aside, as Parquet's writer leaves it out), and its NULL count is theirs. The
/// source's writer made those claims (its pages are copied as they are), and a reader skips rows by them: a file whose
/// `id` chunk said max 10 but held 500 answered `WHERE id = 500` with nothing (11-03 audit M6). A claim that doesn't
/// hold is an error, and the load is written as today, with claims of our own (`kept`). Fed every row in order (`add`),
/// then `done`. Each value is held to its page's bounds once, and each page's bounds to its row group's (a page or a file
/// that says none is held to its row group's).
struct Claims {
    columns: Vec<Runs>,
    row: u64,
}

/// One column's claims (the `index`th): its pages' (`fine`, each with the bounds its rows are held to; its row groups',
/// in a file of no page index) and its row groups', each over consecutive rows. The NULLs of the page under way so far,
/// and of the row group under way.
struct Runs {
    name: String,
    index: usize,
    floating: bool,
    fine: Vec<Claim>,
    groups: Vec<Claim>,
    at: (usize, usize),
    nulls: (u64, u64),
}

/// What a footer or a page index says of a run of rows that ends before row `end`: its least and greatest value and its
/// NULL count, each None where it says nothing.
struct Claim {
    end: u64,
    lo: Option<ScalarValue>,
    hi: Option<ScalarValue>,
    nulls: Option<u64>,
}

impl Claims {
    fn new(md: &ParquetMetaData, schema: &Schema) -> Result<Self> {
        use std::cmp::Ordering::{Greater, Less};
        let parquet = md.file_metadata().schema_descr();
        let ends: Vec<u64> = md.row_groups().iter().scan(0u64, |end, g| {
            *end += g.num_rows() as u64;
            Some(*end)
        }).collect();
        let mut columns = vec![];
        for (index, f) in schema.fields().iter().enumerate() {
            let at = |a: &ArrayRef, k: usize| ScalarValue::try_from_array(a, k).ok().filter(|v| !v.is_null());
            let counted = |n: &UInt64Array, k: usize| n.is_valid(k).then(|| n.value(k));
            // (the system columns' claims are the copy's own, of the rows' ids, versions and times it wrote)
            if NAMES.contains(&f.name().as_str()) {
                continue;
            }
            // (a column of no leaf of its own, a list or a struct, has no claims a reader goes by)
            let s = StatisticsConverter::try_new(f.name(), schema, parquet)?;
            let Some(leaf) = s.parquet_column_index() else { continue };
            let (lo, hi, nulls) = (s.row_group_mins(md.row_groups().iter())?, s.row_group_maxes(md.row_groups().iter())?, s.row_group_null_counts(md.row_groups().iter())?);
            let groups: Vec<Claim> = (0..ends.len()).map(|g| Claim { end: ends[g], lo: at(&lo, g), hi: at(&hi, g), nulls: counted(&nulls, g) }).collect();
            let mut fine = vec![];
            for (g, group) in groups.iter().enumerate() {
                let first = if g == 0 { 0 } else { ends[g - 1] };
                let pages = match (md.column_index(), md.offset_index()) {
                    (Some(index), Some(offsets)) => {
                        let (lo, hi, nulls) = (s.data_page_mins(index, offsets, [&g])?, s.data_page_maxes(index, offsets, [&g])?, s.data_page_null_counts(index, offsets, [&g])?);
                        let pages = offsets[g].get(leaf).map_or(&[][..], |o| o.page_locations());
                        let end = |k: usize| pages.get(k + 1).map_or(group.end, |p| first + p.first_row_index as u64);
                        (0..pages.len()).map(|k| Claim { end: end(k), lo: at(&lo, k), hi: at(&hi, k), nulls: counted(&nulls, k) }).collect()
                    }
                    _ => vec![Claim { end: group.end, lo: None, hi: None, nulls: None }],
                };
                // (each page's rows held to the tighter of its bounds and its row group's: what holds of them holds of both)
                let start = fine.len().saturating_sub(1);
                for mut page in pages {
                    page.lo = tighter(f.name(), page.lo, &group.lo, Greater)?;
                    page.hi = tighter(f.name(), page.hi, &group.hi, Less)?;
                    fine.push(page);
                }
                // (pages in order, the last ending where its row group does)
                ensure!(fine.last().is_some_and(|p| p.end == group.end) && fine[start..].windows(2).all(|w| w[0].end <= w[1].end), "{}: its page index doesn't follow its row groups", f.name());
            }
            let says = |c: &Claim| c.lo.is_some() || c.hi.is_some() || c.nulls.is_some();
            if groups.iter().chain(&fine).any(says) {
                columns.push(Runs { name: f.name().clone(), index, floating: f.data_type().is_floating(), fine, groups, at: (0, 0), nulls: (0, 0) });
            }
        }
        Ok(Claims { columns, row: 0 })
    }

    /// The next rows of the file, in order.
    fn add(&mut self, b: &RecordBatch) -> Result<()> {
        for runs in &mut self.columns {
            let column = b.column(runs.index);
            let values = if runs.floating { without_nan(column)? } else { column.clone() };
            runs.add(self.row, &values, column)?;
        }
        self.row += b.num_rows() as u64;
        Ok(())
    }

    /// Every claim was of rows the file holds (pages and row groups of no rows at its end too).
    fn done(&mut self) -> Result<()> {
        for runs in &mut self.columns {
            while runs.fine.get(runs.at.0).is_some_and(|p| p.end == self.row) {
                runs.close()?;
            }
            ensure!(runs.at == (runs.fine.len(), runs.groups.len()), "{}: its footer claims rows past its {} rows", runs.name, self.row);
        }
        Ok(())
    }
}

impl Runs {
    /// Rows `row..` of the column: its `values` (a float's with each NaN as NULL, `without_nan`) and the `column` as it
    /// is, whose NULLs are counted. Each claim whose run they end is checked.
    fn add(&mut self, row: u64, values: &ArrayRef, column: &ArrayRef) -> Result<()> {
        let (len, mut off) = (column.len(), 0usize);
        while off < len && self.at.0 < self.fine.len() {
            let page = &self.fine[self.at.0];
            let take = usize::try_from(page.end.saturating_sub(row + off as u64)).unwrap_or(usize::MAX).min(len - off);
            if (page.lo.is_some() || page.hi.is_some()) && !bounded(&values.slice(off, take), page.lo.as_ref(), page.hi.as_ref())? {
                let shown = |v: &Option<ScalarValue>| v.as_ref().map_or("anything".to_string(), |v| v.to_string());
                anyhow::bail!("{}: its footer says its rows run from {} to {}, and some of rows {}..{} don't", self.name, shown(&page.lo), shown(&page.hi), row + off as u64, row + (off + take) as u64);
            }
            self.nulls.0 += column.slice(off, take).null_count() as u64;
            off += take;
            if row + off as u64 == page.end {
                self.close()?;
            }
        }
        ensure!(off == len, "{}: the file holds rows past what its footer claims", self.name);
        Ok(())
    }

    /// The page under way ends: its NULLs held to its claim, and those of each row group it ends (one of no rows too).
    fn close(&mut self) -> Result<()> {
        let page = &self.fine[self.at.0];
        let end = page.end;
        held(&self.name, page.nulls, self.nulls.0)?;
        self.nulls = (0, self.nulls.1 + self.nulls.0);
        self.at.0 += 1;
        while let Some(group) = self.groups.get(self.at.1).filter(|g| g.end == end) {
            held(&self.name, group.nulls, self.nulls.1)?;
            self.nulls.1 = 0;
            self.at.1 += 1;
        }
        Ok(())
    }
}

/// The tighter of a page's bound and its row group's: of two least values the greater (`way` Greater), of two greatest
/// the lesser (Less); either one where the other says none. Rows held to it are held to both. An honest page's bound can
/// be the looser: parquet-mr (Spark) cuts a page index's strings to 64 bytes and keeps its row groups' whole.
fn tighter(name: &str, page: Option<ScalarValue>, group: &Option<ScalarValue>, way: std::cmp::Ordering) -> Result<Option<ScalarValue>> {
    Ok(match (page, group) {
        (Some(p), Some(g)) => match p.partial_cmp(g) {
            Some(o) if o == way => Some(p),
            Some(_) => Some(g.clone()),
            None => anyhow::bail!("{name}: its page index's {p} and its footer's {g} don't compare"),
        },
        (page, group) => page.or_else(|| group.clone()),
    })
}

/// A claimed NULL count holds.
fn held(name: &str, claimed: Option<u64>, nulls: u64) -> Result<()> {
    ensure!(claimed.is_none_or(|n| n == nulls), "{name}: its footer says {} NULLs, and its rows hold {nulls}", claimed.unwrap_or(0));
    Ok(())
}

/// Whether each of the values (NULLs aside) is `lo` or above and `hi` or below, each None for no bound. Strings by their
/// bytes (as Parquet orders them); whole numbers, dates, times and decimals of no NULL by their least and greatest
/// value, in one pass; the rest by Arrow's comparison of each with the bound.
fn bounded(values: &ArrayRef, lo: Option<&ScalarValue>, hi: Option<&ScalarValue>) -> Result<bool> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::compute::kernels::cmp::{gt, lt};
    use datafusion::arrow::datatypes::*;
    fn text(v: Option<&ScalarValue>) -> Result<Option<&[u8]>> {
        match v {
            None => Ok(None),
            Some(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => Ok(Some(s.as_bytes())),
            Some(v) => Err(anyhow::anyhow!("a string column's bound {v} isn't a string")),
        }
    }
    match values.data_type() {
        DataType::Utf8 => return Ok(strings_bounded(values.as_string::<i32>(), text(lo)?, text(hi)?)),
        DataType::LargeUtf8 => return Ok(strings_bounded(values.as_string::<i64>(), text(lo)?, text(hi)?)),
        _ => {}
    }
    if values.null_count() == 0 {
        let spanned = match values.data_type() {
            DataType::Int8 => Some(spans::<Int8Type>(values, lo, hi)?),
            DataType::Int16 => Some(spans::<Int16Type>(values, lo, hi)?),
            DataType::Int32 => Some(spans::<Int32Type>(values, lo, hi)?),
            DataType::Int64 => Some(spans::<Int64Type>(values, lo, hi)?),
            DataType::UInt8 => Some(spans::<UInt8Type>(values, lo, hi)?),
            DataType::UInt16 => Some(spans::<UInt16Type>(values, lo, hi)?),
            DataType::UInt32 => Some(spans::<UInt32Type>(values, lo, hi)?),
            DataType::UInt64 => Some(spans::<UInt64Type>(values, lo, hi)?),
            DataType::Date32 => Some(spans::<Date32Type>(values, lo, hi)?),
            DataType::Date64 => Some(spans::<Date64Type>(values, lo, hi)?),
            DataType::Decimal128(..) => Some(spans::<Decimal128Type>(values, lo, hi)?),
            DataType::Timestamp(TimeUnit::Second, _) => Some(spans::<TimestampSecondType>(values, lo, hi)?),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Some(spans::<TimestampMillisecondType>(values, lo, hi)?),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Some(spans::<TimestampMicrosecondType>(values, lo, hi)?),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(spans::<TimestampNanosecondType>(values, lo, hi)?),
            _ => None,
        };
        if let Some(spanned) = spanned {
            return Ok(spanned);
        }
    }
    let past = |bound: Option<&ScalarValue>, op: fn(&dyn datafusion::arrow::array::Datum, &dyn datafusion::arrow::array::Datum) -> std::result::Result<datafusion::arrow::array::BooleanArray, datafusion::arrow::error::ArrowError>| -> Result<bool> {
        Ok(match bound {
            Some(b) => op(values, &b.to_scalar()?)?.true_count() > 0,
            None => false,
        })
    };
    Ok(!past(lo, lt)? && !past(hi, gt)?)
}

/// `bounded` for a column of no NULL whose values order as their native ones do: its least and greatest value, against the bounds.
fn spans<T: datafusion::arrow::datatypes::ArrowPrimitiveType>(values: &ArrayRef, lo: Option<&ScalarValue>, hi: Option<&ScalarValue>) -> Result<bool>
where
    T::Native: Ord,
{
    use datafusion::arrow::array::AsArray;
    let native = |s: Option<&ScalarValue>| -> Result<Option<T::Native>> { s.map(|s| Ok(s.to_array()?.as_primitive::<T>().value(0))).transpose() };
    let v = values.as_primitive::<T>().values();
    let Some(&first) = v.first() else { return Ok(true) };
    // (eight at a time, each its own least and greatest, so the compares don't wait on each other)
    let (mut least, mut most) = ([first; 8], [first; 8]);
    let eights = v.chunks_exact(8);
    let rest = eights.remainder();
    for c in eights {
        for k in 0..8 {
            least[k] = least[k].min(c[k]);
            most[k] = most[k].max(c[k]);
        }
    }
    let (least, most) = rest.iter().fold((least.into_iter().min().unwrap_or(first), most.into_iter().max().unwrap_or(first)), |(l, m), &x| (l.min(x), m.max(x)));
    Ok(native(lo)?.is_none_or(|b| b <= least) && native(hi)?.is_none_or(|b| most <= b))
}

/// `bounded` for strings: a value's first eight bytes decide it against a bound whose first eight differ (most do), and
/// only one that shares them is compared whole.
fn strings_bounded<O: datafusion::arrow::array::OffsetSizeTrait>(a: &datafusion::arrow::array::GenericStringArray<O>, lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
    #[inline]
    fn head(v: &[u8]) -> u64 {
        match v.first_chunk::<8>() {
            Some(b) => u64::from_be_bytes(*b),
            None => v.iter().enumerate().fold(0u64, |h, (k, &x)| h | (x as u64) << (56 - 8 * k)),
        }
    }
    let (data, offsets) = (a.value_data(), a.value_offsets());
    let (lo, hi) = (lo.map(|b| (head(b), b)), hi.map(|b| (head(b), b)));
    // (two strings of up to eight bytes and one head differ only in the zero bytes the longer ends with)
    let at_least = |v: &[u8], h: u64, (bh, b): (u64, &[u8])| h > bh || (h == bh && if v.len() <= 8 && b.len() <= 8 { v.len() >= b.len() } else { v >= b });
    let fits = |v: &[u8]| {
        let h = head(v);
        lo.is_none_or(|l| at_least(v, h, l)) && hi.is_none_or(|(bh, b)| h < bh || (h == bh && if v.len() <= 8 && b.len() <= 8 { v.len() <= b.len() } else { v <= b }))
    };
    let value = |w: &[O]| &data[w[0].as_usize()..w[1].as_usize()];
    match a.nulls().filter(|n| n.null_count() > 0) {
        None => offsets.windows(2).all(|w| fits(value(w))),
        Some(nulls) => offsets.windows(2).zip(nulls.iter()).all(|(w, valid)| !valid || fits(value(w))),
    }
}

/// A float column with each NaN as NULL: Parquet's writer leaves NaN out of a chunk's min and max. A column of no NaN
/// is itself.
fn without_nan(column: &ArrayRef) -> Result<ArrayRef> {
    use datafusion::arrow::array::{AsArray, BooleanArray, PrimitiveArray};
    use datafusion::arrow::buffer::BooleanBuffer;
    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Float16Type, Float32Type, Float64Type};
    fn nan<T: ArrowPrimitiveType>(a: &PrimitiveArray<T>, is_nan: impl Fn(T::Native) -> bool) -> Option<BooleanArray> {
        let v = a.values();
        v.iter().any(|x| is_nan(*x)).then(|| BooleanArray::from(BooleanBuffer::collect_bool(v.len(), |i| is_nan(v[i]))))
    }
    let nan = match column.data_type() {
        DataType::Float64 => nan(column.as_primitive::<Float64Type>(), f64::is_nan),
        DataType::Float32 => nan(column.as_primitive::<Float32Type>(), f32::is_nan),
        DataType::Float16 => nan(column.as_primitive::<Float16Type>(), |x| x.is_nan()),
        _ => None,
    };
    Ok(match nan {
        Some(nan) => datafusion::arrow::compute::nullif(column, &nan)?,
        None => column.clone(),
    })
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

    /// A load copies a cut a core within a quarter of the query memory free now, two at least (11-03 audit M11: other
    /// work's memory leaves the copies less room, and each copy's is reserved).
    #[test]
    fn copies_at_once_fit_the_free_query_memory() {
        let f = tier::FILE_BYTES;
        assert_eq!(at_once(16, 64 * f), 8);
        assert_eq!(at_once(4, 64 * f), 4, "a core each");
        assert_eq!(at_once(16, 24 * f), 3, "a quarter of what is free");
        assert_eq!(at_once(16, 0), 2, "two at least");
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

    /// Two row groups of 1,000 rows, pages of 100: `i` (every tenth one NULL from the fourth), `f` (a NaN at row 7) and
    /// `s`, as a file of them is written.
    fn honest(n: usize) -> (Vec<Option<i64>>, Vec<Option<f64>>, Vec<Option<String>>) {
        let i = (0..n as i64).map(|v| (v % 10 != 3).then_some(v)).collect();
        let f = (0..n).map(|v| Some(if v == 7 { f64::NAN } else { v as f64 })).collect();
        let s = (0..n).map(|v| Some(format!("s{v:05}"))).collect();
        (i, f, s)
    }

    fn of(schema: &Arc<Schema>, (i, f, s): (Vec<Option<i64>>, Vec<Option<f64>>, Vec<Option<String>>)) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(i)), Arc::new(Float64Array::from(f)), Arc::new(StringArray::from(s))]).unwrap()
    }

    /// An honest file's claims hold, a NaN among a float's values too. Rows that aren't what a row group's or a page's
    /// claims say (a value past a page's max but in its row group's range, past a row group's, below a string's min, a
    /// NULL not counted) are refused, so such a load is written as today (11-03 audit M6).
    #[test]
    fn a_kept_files_claims_are_checked_against_its_rows() {
        use datafusion::parquet::file::metadata::ParquetMetaDataReader;
        use datafusion::parquet::file::properties::WriterProperties;
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true), Field::new("f", DataType::Float64, true), Field::new("s", DataType::Utf8, true)]));
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(1000)).set_data_page_row_count_limit(100).set_write_batch_size(100).build();
        let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
        w.write(&of(&schema, honest(2000))).unwrap();
        let bytes = Bytes::from(w.into_inner().unwrap());
        let md = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Required).parse_and_finish(&bytes).unwrap();
        assert_eq!(md.num_row_groups(), 2);
        assert!(md.offset_index().unwrap()[0][0].page_locations().len() >= 10, "pages of 100 rows");
        // (fed in pieces that end inside a page and inside a row group)
        let check = |rows: RecordBatch| -> Result<()> {
            let mut c = Claims::new(&md, &schema)?;
            for (at, n) in [(0, 450), (450, 1100), (1550, 450)] {
                c.add(&rows.slice(at, n))?;
            }
            c.done()
        };
        check(of(&schema, honest(2000))).unwrap();
        let lie = |edit: &dyn Fn(&mut (Vec<Option<i64>>, Vec<Option<f64>>, Vec<Option<String>>))| {
            let mut rows = honest(2000);
            edit(&mut rows);
            check(of(&schema, rows)).unwrap_err().to_string()
        };
        assert!(lie(&|r| r.0[250] = Some(900)).starts_with("i: its footer says"), "past its page's max, in its row group's range");
        assert!(lie(&|r| r.0[1010] = Some(5000)).starts_with("i: its footer says"), "past its row group's max");
        assert!(lie(&|r| r.2[500] = Some("a".into())).starts_with("s: its footer says"), "below a string's min");
        assert!(lie(&|r| r.0[1] = None).contains("NULLs"), "a NULL its footer doesn't count");
        assert!(lie(&|r| r.1[1999] = Some(-1.0)).starts_with("f: its footer says"), "a float below its min");
        // (a file of no page index: its rows held to its row groups' claims)
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(1000)).set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Chunk).build();
        let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props)).unwrap();
        w.write(&of(&schema, honest(2000))).unwrap();
        let chunks = Bytes::from(w.into_inner().unwrap());
        let md = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Skip).parse_and_finish(&chunks).unwrap();
        assert!(md.column_index().is_none() && md.offset_index().is_none(), "no page index");
        let mut c = Claims::new(&md, &schema).unwrap();
        c.add(&of(&schema, honest(2000))).unwrap();
        c.done().unwrap();
        let mut c = Claims::new(&md, &schema).unwrap();
        let mut rows = honest(2000);
        rows.0[250] = Some(900);
        c.add(&of(&schema, rows)).expect("in its row group's range");
        let mut rows = honest(2000);
        rows.0[1010] = Some(5000);
        assert!(Claims::new(&md, &schema).unwrap().add(&of(&schema, rows)).unwrap_err().to_string().starts_with("i: its footer says"), "past its row group's max");
        // (rows past what the footer claims, or short of it)
        let mut c = Claims::new(&md, &schema).unwrap();
        c.add(&of(&schema, honest(2000))).unwrap();
        assert!(c.add(&of(&schema, honest(1))).is_err());
        let mut c = Claims::new(&md, &schema).unwrap();
        c.add(&of(&schema, honest(1999))).unwrap();
        assert!(c.done().is_err());
    }

    /// Of a page's bound and its row group's the tighter is held (a page's string cut short is looser than its row
    /// group's whole one), and pages and row groups of no rows at a file's end are its rows' too.
    #[test]
    fn a_page_is_held_to_the_tighter_bound_and_empty_runs_end_a_file() {
        use std::cmp::Ordering::{Greater, Less};
        let t = |v: &str| Some(ScalarValue::Utf8(Some(v.into())));
        assert_eq!(tighter("s", t("ab"), &t("abc"), Greater).unwrap(), t("abc"), "a least value cut short");
        assert_eq!(tighter("s", t("abd"), &t("abc"), Greater).unwrap(), t("abd"));
        assert_eq!(tighter("s", t("abd"), &t("abc~"), Less).unwrap(), t("abc~"), "a greatest value cut short and raised");
        assert_eq!(tighter("s", None, &t("abc"), Less).unwrap(), t("abc"));
        assert_eq!(tighter("s", t("x"), &None, Less).unwrap(), t("x"));
        assert!(tighter("s", t("x"), &Some(ScalarValue::Int64(Some(1))), Less).is_err());
        let claim = |end: u64, nulls: Option<u64>| Claim { end, lo: None, hi: None, nulls };
        let runs = Runs { name: "i".into(), index: 0, floating: false, fine: vec![claim(5, Some(1)), claim(5, Some(0))], groups: vec![claim(5, Some(1)), claim(5, Some(0))], at: (0, 0), nulls: (0, 0) };
        let mut c = Claims { columns: vec![runs], row: 0 };
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        c.add(&RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)])) as ArrayRef]).unwrap()).unwrap();
        c.done().unwrap();
    }

    /// A bound holds of strings by their bytes (a value sharing a bound's first eight bytes is compared whole, a zero
    /// byte past a string's end orders after it), and of whole numbers and decimals by value, NULLs aside.
    #[test]
    fn a_bound_is_held_by_bytes_and_by_value() {
        use datafusion::arrow::array::Decimal128Array;
        let s = |v: &[Option<&str>]| -> ArrayRef { Arc::new(StringArray::from(v.to_vec())) };
        let t = |v: &str| ScalarValue::Utf8(Some(v.into()));
        let words = s(&[Some("abcdefgh1"), Some("abcdefgh9"), None, Some("b"), Some("")]);
        assert!(bounded(&words, Some(&t("")), Some(&t("b"))).unwrap());
        assert!(!bounded(&words, Some(&t("abcdefgh2")), None).unwrap(), "shares eight bytes, below");
        assert!(!bounded(&s(&[Some("abcdefgh1"), Some("abcdefgh9")]), None, Some(&t("abcdefgh5"))).unwrap(), "shares eight bytes, above");
        assert!(!bounded(&words, Some(&t("a")), None).unwrap(), "the empty string is below a");
        assert!(bounded(&s(&[Some("a\0"), Some("a")]), Some(&t("a")), Some(&t("a\0"))).unwrap());
        assert!(!bounded(&s(&[Some("a")]), Some(&t("a\0")), None).unwrap(), "a zero byte past a string's end");
        let n = |v: Vec<Option<i64>>| -> ArrayRef { Arc::new(Int64Array::from(v)) };
        let i = |v: i64| ScalarValue::Int64(Some(v));
        assert!(bounded(&n(vec![Some(3), Some(-2), Some(7)]), Some(&i(-2)), Some(&i(7))).unwrap());
        assert!(!bounded(&n(vec![Some(3), Some(-2), Some(7)]), Some(&i(-1)), None).unwrap());
        assert!(!bounded(&n(vec![Some(3), Some(-2), Some(7)]), None, Some(&i(6))).unwrap());
        assert!(bounded(&n(vec![Some(3), None, Some(7)]), Some(&i(3)), Some(&i(7))).unwrap(), "a NULL is no value");
        assert!(!bounded(&n(vec![Some(3), None, Some(8)]), Some(&i(3)), Some(&i(7))).unwrap());
        let d: ArrayRef = Arc::new(Decimal128Array::from(vec![150i128, -20]).with_precision_and_scale(10, 2).unwrap());
        let dec = |v: i128| ScalarValue::Decimal128(Some(v), 10, 2);
        assert!(bounded(&d, Some(&dec(-20)), Some(&dec(150))).unwrap());
        assert!(!bounded(&d, Some(&dec(-19)), Some(&dec(150))).unwrap());
    }
}
