//! Other engines' files recorded where they are (ADR-029 §1, §2): an append through the Iceberg
//! catalog costs the node one ranged read per file and a commit, never a copy of the rows. The
//! node that takes the commit reads each file's Parquet footer once, to check it holds what the
//! writer's manifest says and fits the table, and to keep its columns' ranges for skipping; the
//! leader records the files with their lineage (a block of row ids, the commit, its time), from
//! which reads give their rows' system columns (`scan::adopted`) until a merge writes them out.
use crate::store::{json, producer_key, table_key, DataFile, Lake, Lineage, TableMeta};
use anyhow::{ensure, Context, Result};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use futures::{StreamExt, TryStreamExt};

/// Can another engine's files be the table's as they are? Not while views or tasks follow it
/// (they take its rows through the log), and not a table with renamed or dropped columns: Delta
/// readers match a file's columns by name, and the table's files keep the stored names
/// (ADR-022). Those appends are copied, as round 25 did. A partitioned table's files must each
/// hold one partition value, which a writer does when the table publishes its spec
/// (`iceberg::Layout`).
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

/// Leader: record the files as the table's, with their lineage: ids from this node's block, the
/// commit number reserved for them (their rows' `_version`) and its time. False, recording
/// nothing, when a view of the table was made since the files were checked: their rows go
/// through the log instead (`iceberg::through_log`).
pub async fn record(lake: &Lake, seq: &crate::log::Sequencer, table: &str, job: &str, files: &[DataFile]) -> Result<bool> {
    if crate::views::inline(lake).await?.by_source.contains_key(table) || crate::write::follows(lake, table).await? {
        return Ok(false);
    }
    let key = table_key(table);
    let mut meta: TableMeta = lake.cat.get(&key).await?.context("no table")?;
    if !fits_as_written(&meta) {
        return Ok(false); // (altered since)
    }
    let mut recorded = with_lineage(lake, seq, files).await?;
    crate::sketch::add(&mut meta, &mut recorded);
    meta.files.extend(recorded);
    if meta.files.len() > 4 * crate::manifest::INLINE {
        crate::manifest::seal(lake, table, &mut meta).await?;
    }
    lake.cat.commit(vec![(key, json(&meta)), (producer_key(&format!("job:{job}")), json(&1u64))], &[]).await?;
    Ok(true)
}

/// Files that don't hold their rows' system columns, with the lineage that gives them: ids from
/// this node's block, a commit number reserved for them (their rows' `_version`) and its time.
/// Another engine's files, and Pondra's own written where no leader could be asked (`pondra sql`,
/// the inbox: `write::record`).
pub async fn with_lineage(lake: &Lake, seq: &crate::log::Sequencer, files: &[DataFile]) -> Result<Vec<DataFile>> {
    let at = seq.number().await?;
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
        let lineage = Lineage { first, version: at.version, ms: at.ms };
        let mut stats = f.stats.clone();
        stats.extend(system_stats(&lineage, f.rows));
        out.push(DataFile { lineage: Some(lineage), stats, ord: lake.visible(), ..f.clone() });
    }
    Ok(out)
}

/// The system columns' ranges a file's lineage gives (a purge finds changed rows by them).
fn system_stats(l: &Lineage, rows: u64) -> Vec<(String, (String, String))> {
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
