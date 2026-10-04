//! Files committed where they were written (ADR-029 §1, §2, §7): another engine's append through
//! the Iceberg catalog, a bulk INSERT's files, costs the leader a commit, never a copy of the rows.
//! The node that takes another engine's commit reads each file's Parquet footer once, to check it
//! holds what the writer's manifest says and fits the table, and to keep its columns' ranges for
//! skipping. The leader commits the files through the log (`file`): recorded with their lineage
//! where they need one (a block of row ids, the commit, its time), from which reads give their
//! rows' system columns (`scan::adopted`) until a merge writes them out; named by the commit's
//! segment, for the log's readers; and followed by the table's views in the same commit.
use crate::store::{json, table_key, DataFile, Lake, Lineage, TableMeta};
use anyhow::{ensure, Context, Result};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use futures::{StreamExt, TryStreamExt};

/// Can another engine's files be the table's as they are? Not a table with renamed or dropped
/// columns: Delta readers match a file's columns by name, and the table's files keep the stored
/// names (ADR-022). Those appends are copied, as round 25 did. A partitioned table's files must
/// each hold one partition value, which a writer does when the table publishes its spec
/// (`iceberg::Layout`). (A keyed table's: not yet.)
pub fn fits_as_written(meta: &TableMeta) -> bool { !meta.mapped() && meta.key.is_empty() && crate::iceberg::Layout::followable(meta) }

/// The files' footers, read once each (a ranged read), checked against the manifest's counts and
/// the table's columns (a column's type as the table's; a NOT NULL column never NULL), and their
/// columns' ranges and NULLs kept on each, under the table's stored names.
pub async fn footers(lake: &Lake, table: &str, meta: &TableMeta, files: &mut [DataFile]) -> Result<()> {
    let ctx = lake.session();
    let checked: Vec<DataFile> = futures::stream::iter(files.iter().cloned()).map(|f| footer(lake, &ctx, table, meta, f)).buffered(16).try_collect().await?;
    files.clone_from_slice(&checked);
    Ok(())
}

async fn footer(lake: &Lake, ctx: &datafusion::prelude::SessionContext, table: &str, meta: &TableMeta, mut f: DataFile) -> Result<DataFile> {
    use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
    let url = datafusion::datasource::listing::ListingTableUrl::parse(lake.full(&f.path))?;
    let store = ctx.runtime_env().object_store(&url)?;
    let object = object_store_df::ObjectMeta { location: url.prefix().clone(), last_modified: Default::default(), size: f.bytes, e_tag: None, version: None };
    let md = datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata::new(store.as_ref(), &object).fetch_metadata().await.with_context(|| format!("{}: its Parquet footer", f.path))?;
    let rows = md.file_metadata().num_rows() as u64;
    ensure!(rows == f.rows, "{}: its manifest says {} rows, the file holds {rows}", f.path, f.rows);
    let schema = datafusion::parquet::arrow::parquet_to_arrow_schema(md.file_metadata().schema_descr(), md.file_metadata().key_value_metadata())?;
    let (mut stats, mut nulls) = (crate::manifest::Stats::new(), vec![]);
    for (i, (stored, t)) in meta.columns.iter().enumerate().filter(|(_, (c, _))| !meta.dropped.contains(c)) {
        let (name, want) = (meta.name_of(stored), crate::query::dtype(t)?);
        let Some(field) = in_file(&schema, i as i64 + 1, name) else {
            ensure!(!meta.not_null.contains(stored), "{}: it has no column {name}, which is NOT NULL in {table}", f.path);
            nulls.push(stored.clone());
            continue;
        };
        ensure!(same(field.data_type(), &want), "{}: its column {name} is {}, {table}'s is {}", f.path, field.data_type(), crate::query::type_name(&want));
        let c = StatisticsConverter::try_new(field.name(), &schema, md.file_metadata().schema_descr())?;
        let counts = c.row_group_null_counts(md.row_groups().iter())?;
        let (known, held) = (counts.null_count() == 0, counts.iter().flatten().sum::<u64>());
        ensure!(!meta.not_null.contains(stored) || (known && held == 0), "{}: {name} is NOT NULL in {table}, and the file {}", f.path, if known { "holds a NULL there" } else { "doesn't say it holds none" });
        if !known || held > 0 {
            nulls.push(stored.clone());
        }
        if stats.len() < 32 && !want.is_floating() {
            if let Some((lo, hi)) = crate::manifest::from_footer(&md, &schema, field.name()) {
                if let (Some(lo), Some(hi)) = (crate::manifest::text(&lo), crate::manifest::text(&hi)) {
                    stats.insert(stored.clone(), (lo, hi));
                }
            }
        }
    }
    if let Some(spec) = &meta.partition {
        f.part = partition(&md, &schema, meta, spec, &f.path, rows).await?;
    }
    (f.stats, f.nulls) = (stats, Some(nulls));
    Ok(f)
}

/// The one partition value a file's rows hold (as `tier::split` names it), by its footer's range
/// of the partition's column; a file holding more than one is refused by name.
async fn partition(md: &datafusion::parquet::file::metadata::ParquetMetaData, schema: &Schema, meta: &TableMeta, spec: &str, path: &str, rows: u64) -> Result<String> {
    use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
    let col = spec.split_once('(').map_or(spec, |(_, c)| c.trim_end_matches(')')).trim();
    let i = meta.columns.iter().position(|(c, _)| c == col).context("the partition's column")?;
    let (name, t) = (meta.name_of(col), crate::query::dtype(&meta.columns[i].1)?);
    let field = in_file(schema, i as i64 + 1, name).with_context(|| format!("{path}: it has no column {name}, which {spec} partitions by"))?;
    let c = StatisticsConverter::try_new(field.name(), schema, md.file_metadata().schema_descr())?;
    let nulls: u64 = c.row_group_null_counts(md.row_groups().iter())?.iter().flatten().sum();
    if nulls == rows {
        return Ok("null".into());
    }
    let (lo, hi) = crate::manifest::from_footer(md, schema, field.name()).with_context(|| format!("{path}: its footer doesn't say which partition ({spec}) it holds"))?;
    let values = ScalarValue::iter_to_array([lo, hi])?;
    let values = datafusion::arrow::compute::cast(&values, &t)?;
    let batch = datafusion::arrow::array::RecordBatch::try_new(std::sync::Arc::new(Schema::new(vec![Field::new(col, t, true)])), vec![values])?;
    let parts: Vec<String> = crate::tier::split(spec, vec![batch]).await?.into_iter().map(|(p, _)| p).collect();
    ensure!(parts.len() == 1 && nulls == 0, "{path}: it holds rows of more than one partition ({spec}: {}{}); the table's partition spec, as it publishes it, puts each in a file of its own", parts.join(", "), if nulls > 0 { ", null" } else { "" });
    Ok(parts.into_iter().next().expect("one"))
}

/// The file's column for the table's column: by its field id (Iceberg's: the stored column's
/// place), else by the name SQL knows it by.
fn in_file<'a>(schema: &'a Schema, id: i64, name: &str) -> Option<&'a Field> {
    let by_id = schema.fields().iter().find(|f| f.metadata().get("PARQUET:field_id").and_then(|i| i.parse::<i64>().ok()) == Some(id));
    by_id.or_else(|| schema.fields().iter().find(|f| f.name() == name)).map(|f| f.as_ref())
}

/// The same type, as far as a reader of the table can tell: strings, binaries and lists however
/// they are laid out, a time zone's name aside.
fn same(file: &DataType, table: &DataType) -> bool {
    fn plain(t: &DataType) -> DataType {
        match t {
            DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
            DataType::BinaryView | DataType::LargeBinary => DataType::Binary,
            DataType::Timestamp(u, tz) => DataType::Timestamp(*u, tz.as_ref().map(|_| "UTC".into())),
            DataType::List(f) | DataType::LargeList(f) | DataType::ListView(f) | DataType::LargeListView(f) => DataType::List(Field::new("item", plain(f.data_type()), true).into()),
            t => t.clone(),
        }
    }
    plain(file) == plain(table)
}

/// One table's part of a file commit (`file`): the files it puts in, the files it takes out, and
/// the rows of its files it deletes by position (a file's path, the delete that names it, how
/// many rows), once (`job`). `new`: the table, if the commit makes it (a bulk INSERT's).
#[derive(Clone, Default)]
pub struct FileCommit {
    pub table: String,
    pub job: String,
    pub added: Vec<DataFile>,
    pub removed: Vec<String>,
    pub deleted: Vec<(String, crate::scan::Delete, u64)>,
    pub new: Option<TableMeta>,
}

/// Leader, under the lake's lock: files into tables, files out of them and rows of their files
/// deleted by position, as one commit through the log (ADR-029 §7), all of it or none (another
/// engine's transaction over several tables is one). The files are recorded as the tables' where
/// they were written (one without its rows' system columns gets a lineage: ids from this node's
/// block, its rows' `_version` and times this commit's); the commit's segment names them for the
/// log's readers (the change feed, Kafka topics, `/watch`, tasks); and the views that follow a
/// table derive their rows from the new files' rows, and take back the rows taken out or deleted
/// (those files must still be the table's: else 409), in the same catalog write. The commit's
/// number, or None if its jobs were in already.
pub async fn file(lake: &Lake, seq: &crate::log::Sequencer, commits: &[FileCommit]) -> Result<Option<u64>> {
    use crate::log::{Append, Filing, Outcome, Src};
    let mut added = vec![];
    for c in commits {
        added.push(with_ids(lake, seq, &c.added).await?);
    }
    loop {
        let (mut filings, mut marks, mut followed) = (vec![], vec![], std::collections::BTreeMap::new());
        let inline = crate::views::inline(lake).await?;
        for (c, added) in commits.iter().zip(&added) {
            let mut meta = match lake.cat.get::<TableMeta>(&table_key(&c.table)).await? {
                Some(m) => m,
                None => c.new.clone().with_context(|| format!("no table {}", c.table))?,
            };
            let (removed, deleted) = carry(lake, &c.table, &meta, c).await?;
            let gone = take_out(lake, &c.table, &mut meta, &removed).await?;
            let hit = mark(lake, &c.table, &mut meta, &deleted).await?;
            let mut files = added.clone();
            crate::sketch::add(&mut meta, &mut files);
            if inline.by_source.contains_key(&c.table) {
                followed.insert(c.table.clone(), rows(lake, &meta, &files, false).await?);
                let mut old = rows(lake, &meta, &gone, true).await?;
                for (f, deletes) in &hit {
                    for b in rows_deleted(lake, &meta, f, deletes, &sys_schema(&meta, true)?).await? {
                        old.push(crate::change::as_deleted(&meta.to_logical(&b)?)?);
                    }
                }
                if !old.is_empty() {
                    followed.insert(crate::sys::deleted(&c.table), old);
                    for (view, vmeta) in crate::views::row_views(lake, &c.table).await? {
                        crate::change::companion(lake, &view, &vmeta).await?; // (its rows of the old ones go there)
                    }
                }
            }
            let src = Src { producer: format!("job:{}", c.job), seq: 1, prev: None }; // (the job, once)
            let none = RecordBatch::new_empty(crate::query::schema(&meta.logical().columns)?);
            marks.push(Append { table: c.table.clone(), src, batch: none, ack: tokio::sync::oneshot::channel().0 });
            filings.push(Filing { table: c.table.clone(), meta, added: files, removed: gone, deleted: hit });
        }
        let mut flush = crate::log::pack_with(lake, &marks, followed).await?;
        flush.filed = filings;
        match seq.submit(flush).await? {
            Outcome::Acks(a) if a.iter().any(|a| a.duplicate) => return Ok(None),
            Outcome::Acks(a) => {
                for c in commits {
                    let mut meta: TableMeta = lake.cat.get(&table_key(&c.table)).await?.context("no table")?;
                    if meta.files.len() > 4 * crate::manifest::INLINE && crate::manifest::seal(lake, &c.table, &mut meta).await? {
                        lake.cat.commit(vec![(table_key(&c.table), json(&meta))], &[]).await?; // (a big INSERT's many files: sealed at once)
                    }
                }
                return Ok(Some(a[0].seg));
            }
            Outcome::Retry(r) if r.is_empty() => tokio::time::sleep(std::time::Duration::from_millis(10)).await, // (views changed: derive again)
            Outcome::Retry(_) => return Ok(None),
            Outcome::Refused(why) => anyhow::bail!(why),
        }
    }
}

/// Leader: another engine's change to a keyed table as the table's rows (`iceberg::keyed`: upserts,
/// delete markers before them) through the log, once (its job): new row ids from this node's block.
/// Whether they went in now (else they were in already).
pub async fn upserts(lake: &Lake, seq: &crate::log::Sequencer, table: &str, rows: RecordBatch, job: &str) -> Result<bool> {
    use crate::log::{Append, Outcome, Src};
    let first = loop {
        if let Some(first) = lake.ids.take(rows.num_rows() as u64) {
            break first;
        }
        lake.ids.refill(seq.block().await?);
    };
    let batch = crate::sys::stamp(&rows, first)?;
    let append = Append { table: table.into(), src: Src { producer: format!("job:{job}"), seq: 1, prev: None }, batch, ack: tokio::sync::oneshot::channel().0 };
    loop {
        match seq.submit(crate::log::pack(lake, std::slice::from_ref(&append)).await?).await? {
            Outcome::Retry(r) if r.is_empty() => tokio::time::sleep(std::time::Duration::from_millis(10)).await, // (views changed: derive again)
            Outcome::Acks(a) => return Ok(a.iter().all(|a| !a.duplicate)),
            Outcome::Retry(_) => return Ok(false),
            Outcome::Refused(why) => anyhow::bail!(why),
        }
    }
}

/// A table's columns and, for a file commit's rows, the system columns known before it commits:
/// `_row_id` (the new rows'); and, for the old rows it takes out, `_created_at` and `_version` too.
fn sys_schema(meta: &TableMeta, old: bool) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    let sys = crate::sys::columns();
    let mut columns = meta.columns.clone();
    columns.extend(sys.iter().filter(|(c, _)| c == crate::sys::ROW_ID || (old && (c == crate::sys::CREATED || c == crate::sys::VERSION))).cloned());
    crate::query::schema(&columns)
}

/// A file commit's rows as the views that follow its table take them: under the names SQL knows,
/// with their `_row_id` (its `_version` and times are the commit's, not known yet); `old`, the
/// rows of files taken out, with the version each had (`_old_version`) and `_created_at` too.
async fn rows(lake: &Lake, meta: &TableMeta, files: &[DataFile], old: bool) -> Result<Vec<RecordBatch>> {
    if files.is_empty() {
        return Ok(vec![]);
    }
    let read = crate::query::files_once(lake, &lake.session(), &files.iter().collect::<Vec<_>>(), meta, &sys_schema(meta, old)?).await?;
    let batches = read.collect().await?;
    batches.iter().map(|b| {
        let b = meta.to_logical(b)?;
        if old { crate::change::as_deleted(&b) } else { Ok(b) }
    }).collect()
}

/// The rows of file `f`, as it was, at the places `deletes` name (the rows they delete), as
/// `schema`: its columns and system columns, by their stored names.
pub async fn rows_deleted(lake: &Lake, meta: &TableMeta, f: &DataFile, deletes: &[crate::scan::Delete], schema: &datafusion::arrow::datatypes::SchemaRef) -> Result<Vec<RecordBatch>> {
    let mut at = crate::scan::deleted_rows(lake, &DataFile { deletes: deletes.to_vec(), ..f.clone() }).await?;
    if !f.deletes.is_empty() {
        let before: std::collections::HashSet<u64> = crate::scan::deleted_rows(lake, f).await?.into_iter().collect();
        at.retain(|p| !before.contains(p)); // (deleted already)
    }
    if at.is_empty() {
        return Ok(vec![]);
    }
    Ok(crate::scan::file_rows(lake, &lake.session(), f, meta, schema, crate::scan::Pick::At(std::sync::Arc::new(at))).await?.collect().await?)
}

/// A writer's change to files Pondra rewrote since it read the table (merged, or rewritten once
/// mostly deleted: the same rows, their ids kept) is carried over to the files that hold those
/// rows now, as positions there: the rows of a file it takes out, or those its positions name.
/// Pondra's own upkeep then never fails another engine's commit; a row changed since does (409).
/// The files it takes out and the positions it deletes, as the table has them now.
async fn carry(lake: &Lake, table: &str, meta: &TableMeta, c: &FileCommit) -> Result<(Vec<String>, Vec<(String, crate::scan::Delete, u64)>)> {
    use crate::scan::{deleted_rows, Delete};
    let mut live = meta.files.clone();
    for m in crate::manifest::list(lake, meta).await? {
        live.extend(crate::manifest::files(lake, &m).await?.iter().cloned());
    }
    let here = |p: &str| live.iter().any(|f| f.path == p);
    let old = |p: &str| meta.replaced.iter().find(|f| f.path == p).cloned().with_context(|| format!("{}: {p} is no longer {table}'s (removed since the writer read it)", crate::iceberg::CONFLICT));
    let (mut removed, mut deleted, mut ids) = (vec![], vec![], vec![]);
    for p in c.removed.iter().filter(|p| !here(p)) {
        let f = old(p)?;
        let gone: std::collections::HashSet<u64> = deleted_rows(lake, &f).await?.into_iter().collect();
        ids.extend(crate::tier::ids_of(lake, meta, &f).await?.into_iter().enumerate().filter(|(i, _)| !gone.contains(&(*i as u64))).map(|(_, id)| id));
    }
    for (p, d, _) in c.deleted.iter().filter(|d| !here(&d.0)) {
        let f = old(p)?;
        let all = crate::tier::ids_of(lake, meta, &f).await?;
        ids.extend(deleted_rows(lake, &DataFile { deletes: vec![d.clone()], ..f }).await?.into_iter().filter_map(|i| all.get(i as usize).copied()));
    }
    removed.extend(c.removed.iter().filter(|p| here(p)).cloned());
    deleted.extend(c.deleted.iter().filter(|d| here(&d.0)).cloned());
    if ids.is_empty() {
        return Ok((removed, deleted));
    }
    ids.sort_unstable();
    ids.dedup();
    let range = |f: &DataFile| f.stats.get(crate::sys::ROW_ID).and_then(|(lo, hi)| Some((lo.parse::<i64>().ok()?, hi.parse::<i64>().ok()?)));
    let mut found = 0;
    for f in live.iter().filter(|f| range(f).is_none_or(|(lo, hi)| lo <= ids[ids.len() - 1] && ids[0] <= hi)) {
        let gone: std::collections::HashSet<u64> = deleted_rows(lake, f).await?.into_iter().collect();
        let all = crate::tier::ids_of(lake, meta, f).await?;
        let at: Vec<u64> = (0..all.len() as u64).filter(|i| !gone.contains(i) && ids.binary_search(&all[*i as usize]).is_ok()).collect();
        if at.is_empty() {
            continue;
        }
        found += at.len();
        let (path, rows, bytes) = crate::tier::write_positions(lake, meta.folder(table), at.iter().map(|i| (lake.full(&f.path), *i)).collect()).await?;
        deleted.push((f.path.clone(), Delete::Positions { path, file: lake.full(&f.path), rows, bytes }, at.len() as u64));
    }
    ensure!(found == ids.len(), "{}: {} of the rows it changes were changed since the writer read it", crate::iceberg::CONFLICT, ids.len() - found);
    Ok((removed, deleted))
}

/// Rows of the table's files deleted by position (merge-on-read: ADR-029 §4): each file named,
/// sealed ones too (their manifests unsealed), takes its deletes, and must be the table's now (else
/// the writer read an older table: 409). The files as they were, with the deletes each took.
async fn mark(lake: &Lake, table: &str, meta: &mut TableMeta, deleted: &[(String, crate::scan::Delete, u64)]) -> Result<Vec<(DataFile, Vec<crate::scan::Delete>)>> {
    if deleted.is_empty() {
        return Ok(vec![]);
    }
    let named: std::collections::BTreeSet<&str> = deleted.iter().map(|d| d.0.as_str()).collect();
    let list = crate::manifest::list(lake, meta).await?;
    let mut sealed = vec![];
    for (i, m) in list.iter().enumerate() {
        if crate::manifest::files(lake, m).await?.iter().any(|f| named.contains(f.path.as_str())) {
            sealed.push(i);
        }
    }
    crate::manifest::unseal(lake, table, meta, list, &sealed).await?;
    let missing: Vec<&&str> = named.iter().filter(|p| !meta.files.iter().any(|f| f.path == **p)).collect();
    ensure!(missing.is_empty(), "{}: {} is no longer {table}'s (merged or removed since the writer read it)", crate::iceberg::CONFLICT, missing[0]);
    let mut out = vec![];
    for f in meta.files.iter_mut().filter(|f| named.contains(f.path.as_str())) {
        let before = f.clone();
        let mine: Vec<&(String, crate::scan::Delete, u64)> = deleted.iter().filter(|d| d.0 == f.path).collect();
        for (_, d, n) in &mine {
            f.deletes.push(d.clone());
            f.deleted += n;
        }
        out.push((before, mine.iter().map(|d| d.1.clone()).collect()));
    }
    Ok(out)
}

/// The data files an Iceberg position-delete file names (its `file_path` column: each file's name as
/// the table's metadata gives it), and how many of each one's rows it deletes.
pub async fn named(lake: &Lake, path: &str) -> Result<Vec<(String, u64)>> {
    use datafusion::arrow::array::AsArray;
    let ctx = lake.session();
    let sql = "SELECT file_path AS f, count(DISTINCT pos) AS n FROM d GROUP BY 1";
    ctx.register_parquet("d", lake.full(path), Default::default()).await?;
    let mut out = vec![];
    for b in ctx.sql(sql).await?.collect().await? {
        let f = datafusion::arrow::compute::cast(b.column(0), &DataType::Utf8)?;
        let (f, n) = (f.as_string::<i32>(), b.column(1).as_primitive::<datafusion::arrow::datatypes::Int64Type>());
        out.extend((0..b.num_rows()).map(|i| (f.value(i).to_string(), n.value(i) as u64)));
    }
    Ok(out)
}

/// The table's files `removed` out of it, sealed ones too (their manifests unsealed), to go after
/// the retention period as merged files do; every one must be the table's now, or the writer read
/// an older table (409). Their records, as they were.
async fn take_out(lake: &Lake, table: &str, meta: &mut TableMeta, removed: &[String]) -> Result<Vec<DataFile>> {
    if removed.is_empty() {
        return Ok(vec![]);
    }
    let list = crate::manifest::list(lake, meta).await?;
    let mut sealed = vec![];
    for (i, m) in list.iter().enumerate() {
        if crate::manifest::files(lake, m).await?.iter().any(|f| removed.contains(&f.path)) {
            sealed.push(i);
        }
    }
    crate::manifest::unseal(lake, table, meta, list, &sealed).await?;
    let gone: Vec<&String> = removed.iter().filter(|p| !meta.files.iter().any(|f| f.path == **p)).collect();
    ensure!(gone.is_empty(), "{}: {} is no longer {table}'s (merged or removed since the writer read it)", crate::iceberg::CONFLICT, gone[0]);
    let out: Vec<DataFile> = meta.files.iter().filter(|f| removed.contains(&f.path)).cloned().collect();
    meta.files.retain(|f| !removed.contains(&f.path));
    meta.discard(&out);
    Ok(out)
}

/// Does the table hold rows another engine couldn't have read in its last published version:
/// rows in the log (or changes to its rows) not yet in its files? (A change that removes files is
/// made against the table as Pondra has it: ADR-029 §3.)
pub async fn stale(lake: &Lake, table: &str) -> Result<bool> {
    let meta: TableMeta = lake.cat.get(&table_key(table)).await?.context("no table")?;
    let waiting = crate::tier::backlog(lake, meta.tiered, None).await?;
    let deleted = crate::sys::deleted(table);
    Ok(waiting.get(table).is_some_and(|n| *n > 0) || waiting.get(&deleted).is_some_and(|n| *n > 0) || (meta.changed && meta.purged() < meta.tiered))
}

/// Files that don't hold their rows' system columns, with the lineage that gives them: ids from
/// this node's block (the commit that records them completes it: `log::stamp_file`). Another
/// engine's files, and Pondra's own written without them (a table something follows, `pondra
/// sql`, the inbox: `write::record`).
async fn with_ids(lake: &Lake, seq: &crate::log::Sequencer, files: &[DataFile]) -> Result<Vec<DataFile>> {
    let mut out = vec![];
    for f in files {
        if f.sys || f.lineage.is_some() {
            out.push(f.clone());
            continue;
        }
        let first = loop {
            if let Some(first) = lake.ids.take(f.rows) {
                break first;
            }
            lake.ids.refill(seq.block().await?);
        };
        out.push(DataFile { lineage: Some(Lineage { first, version: 0, ms: 0 }), ..f.clone() });
    }
    Ok(out)
}

/// The system columns' ranges a file's lineage gives (a purge finds changed rows by them).
pub fn system_stats(l: &Lineage, rows: u64) -> Vec<(String, (String, String))> {
    let at = ScalarValue::TimestampMicrosecond(Some(l.ms as i64 * 1000), Some("UTC".into()));
    let text = |v: &ScalarValue| crate::manifest::text(v).unwrap_or_default();
    let (at, version) = (text(&at), l.version.to_string());
    let last = l.first + rows.saturating_sub(1) as i64;
    vec![
        (crate::sys::ROW_ID.into(), (l.first.to_string(), last.to_string())),
        (crate::sys::VERSION.into(), (version.clone(), version)),
        (crate::sys::CREATED.into(), (at.clone(), at.clone())),
        (crate::sys::UPDATED.into(), (at.clone(), at)),
    ]
}
