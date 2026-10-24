//! A lake's own files read with their lineage and deletes (`scan.rs` is the reader under them).
use crate::scan::{field, gone, key_of, statistics, url_of, ById, Delete, File, Files};
use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::execution::object_store::ObjectStoreUrl;
use std::sync::Arc;

/// A lake's files as another engine wrote them, recorded where they are (ADR-029 §1), read as
/// `schema` (stored names; the system columns too, if it has them): the table's columns matched by
/// field id (a stored column's place, as the table's Iceberg schema numbers them) or else by name;
/// the system columns from each file's lineage: `_row_id` its first id plus the row's place in the
/// file (Parquet's row number, right under skipped row groups and pages), `_version` and the times
/// its commit's.
pub async fn adopted(lake: &crate::store::Lake, ctx: &datafusion::prelude::SessionContext, files: &[&crate::store::DataFile], meta: &crate::store::TableMeta, schema: &SchemaRef) -> Result<datafusion::prelude::DataFrame> {
    adopted_in(lake, ctx, files, meta, schema, Pick::All).await
}

/// Which of a file's rows a read takes: all (but those deleted), a range of them, or those at
/// some places (by their place in the file, deleted or not).
#[derive(Clone)]
pub enum Pick {
    All,
    Range(u64, u64),
    At(Arc<Vec<u64>>),
}

impl Pick {
    fn of(&self) -> (Option<(u64, u64)>, Option<Arc<Vec<u64>>>) {
        match self {
            Pick::All => (None, None),
            Pick::Range(a, b) => (Some((*a, *b)), None),
            Pick::At(at) => (None, Some(at.clone())),
        }
    }
}

/// `adopted`, only the rows `pick` names of each file.
async fn adopted_in(lake: &crate::store::Lake, ctx: &datafusion::prelude::SessionContext, files: &[&crate::store::DataFile], meta: &crate::store::TableMeta, schema: &SchemaRef, pick: Pick) -> Result<datafusion::prelude::DataFrame> {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::prelude::{cast, col, ident};
    let first = files.first().context("no files")?;
    let url = url_of(&lake.full(&first.path))?;
    let store = ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?;
    let (mut ids, mut mapping) = (std::collections::BTreeMap::new(), std::collections::HashMap::new());
    let own = meta.columns.iter().enumerate().filter(|(_, (c, _))| !crate::text::NAMES.contains(&c.as_str()));
    for (i, (c, _)) in own.clone() {
        ids.insert(c.clone(), i as i64 + 1); // (`iceberg::fields`)
        mapping.insert(c.clone(), i as i64 + 1); // (Pondra's own files carry the stored name…)
    }
    for (i, (c, _)) in own {
        mapping.insert(meta.name_of(c).to_string(), i as i64 + 1); // (…others' the name SQL knows, which wins)
    }
    let file_schema = Arc::new(Schema::new(schema.fields().iter().filter(|f| ids.contains_key(f.name())).cloned().collect::<Vec<_>>())); // (the ids go to `ById`)
    let time = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let partition = vec![field("__first", DataType::Int64), field("__version", DataType::Int64), field("__at", time.clone())];
    let mut out = vec![];
    for (f, deleted) in files.iter().zip(lake_gone(lake, ctx, files).await?) {
        let l = f.lineage.context("a file without lineage")?;
        let values = vec![ScalarValue::Int64(Some(l.first)), ScalarValue::Int64(Some(l.version as i64)), ScalarValue::TimestampMicrosecond(Some(l.ms as i64 * 1000), Some("UTC".into()))];
        let stats = Arc::new(statistics(f, f.rows, true, deleted.as_ref().map_or(0, |d| d.len()), true, &file_schema));
        let (range, only) = pick.of();
        out.push(File { key: key_of(&lake.full(&f.path), &url)?, size: f.bytes, rows: f.rows, partition: values, deleted, range, only, stats });
    }
    let adapter = Some(Arc::new(ById { ids, mapping }) as _);
    let row = Arc::new(Field::new("__row", DataType::Int64, false).with_extension_type(datafusion::parquet::arrow::RowNumber));
    let df = ctx.read_table(Arc::new(Files { store, file_schema, partition, files: out, adapter, row: Some(row) }))?;
    Ok(df.select(schema.fields().iter().map(|f| match f.name().as_str() {
        crate::text::ROW_ID => (col("__first") + col("__row")).alias(f.name()),
        crate::text::VERSION => col("__version").alias(f.name()),
        crate::text::CREATED | crate::text::UPDATED => col("__at").alias(f.name()),
        n => cast(ident(n), f.data_type().clone()).alias(n),
    }).collect::<Vec<_>>())?)
}

/// One of a lake's own files as `schema` (stored names), each row with its place in the file
/// (`__row`), the rows deleted by position skipped unread.
pub async fn placed(lake: &crate::store::Lake, ctx: &datafusion::prelude::SessionContext, f: &crate::store::DataFile, schema: &SchemaRef) -> Result<datafusion::prelude::DataFrame> {
    use datafusion::arrow::datatypes::DataType;
    let url = url_of(&lake.full(&f.path))?;
    let store = ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?;
    let deleted = lake_gone(lake, ctx, &[f]).await?.pop().flatten();
    let stats = Arc::new(statistics(f, f.rows, true, deleted.as_ref().map_or(0, |d| d.len()), true, schema));
    let file = File { key: key_of(&lake.full(&f.path), &url)?, size: f.bytes, rows: f.rows, partition: vec![], deleted, range: None, only: None, stats };
    let row = Arc::new(Field::new("__row", DataType::Int64, false).with_extension_type(datafusion::parquet::arrow::RowNumber));
    Ok(ctx.read_table(Arc::new(Files { store, file_schema: schema.clone(), partition: vec![], files: vec![file], adapter: None, row: Some(row) }))?)
}

/// A lake's own files that hold their rows' system columns, some of their rows deleted by
/// position (`DataFile::deletes`), read as `schema` (stored names): those rows skipped unread.
pub async fn with_deletes(lake: &crate::store::Lake, ctx: &datafusion::prelude::SessionContext, files: &[&crate::store::DataFile], schema: &SchemaRef) -> Result<datafusion::prelude::DataFrame> {
    let first = files.first().context("no files")?;
    let url = url_of(&lake.full(&first.path))?;
    let store = ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?;
    let mut out = vec![];
    for (f, deleted) in files.iter().zip(lake_gone(lake, ctx, files).await?) {
        let stats = Arc::new(statistics(f, f.rows, true, deleted.as_ref().map_or(0, |d| d.len()), true, schema));
        out.push(File { key: key_of(&lake.full(&f.path), &url)?, size: f.bytes, rows: f.rows, partition: vec![], deleted, range: None, only: None, stats });
    }
    Ok(ctx.read_table(Arc::new(Files { store, file_schema: schema.clone(), partition: vec![], files: out, adapter: None, row: None }))?)
}

/// A lake's files' deleted rows (positions), from their deletes (whose paths are in the lake).
async fn lake_gone(lake: &crate::store::Lake, ctx: &datafusion::prelude::SessionContext, files: &[&crate::store::DataFile]) -> Result<Vec<Option<Arc<Vec<u64>>>>> {
    let full = |d: &Delete| match d {
        Delete::Positions { path, file, rows, bytes } => Delete::Positions { path: lake.full(path), file: file.clone(), rows: *rows, bytes: *bytes },
        Delete::Blob { path, offset, size } => Delete::Blob { path: lake.full(path), offset: *offset, size: *size },
        Delete::Vector { path, offset, size } => Delete::Vector { path: lake.full(path), offset: *offset, size: *size },
        d => d.clone(),
    };
    gone(ctx, &files.iter().map(|f| f.deletes.iter().map(full).collect()).collect::<Vec<_>>()).await
}

/// Rows `from..to` (by their place) of one of a lake's own files, as `schema` (stored names, the
/// system columns too): from its lineage (`adopted`), or from the file itself. What the log's
/// readers read of a file commit, a piece at a time (a Kafka fetch of a big one).
pub async fn file_rows(lake: &crate::store::Lake, ctx: &datafusion::prelude::SessionContext, f: &crate::store::DataFile, meta: &crate::store::TableMeta, schema: &SchemaRef, pick: Pick) -> Result<datafusion::prelude::DataFrame> {
    let f = &crate::store::DataFile { deletes: vec![], ..f.clone() }; // (rows by their place: deleted since or not)
    if f.lineage.is_some() {
        return adopted_in(lake, ctx, &[f], meta, schema, pick).await;
    }
    let url = url_of(&lake.full(&f.path))?;
    let store = ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?;
    let stats = Arc::new(datafusion::common::Statistics::new_unknown(schema));
    let (range, only) = pick.of();
    let file = File { key: key_of(&lake.full(&f.path), &url)?, size: f.bytes, rows: f.rows, partition: vec![], deleted: None, range, only, stats };
    Ok(ctx.read_table(Arc::new(Files { store, file_schema: schema.clone(), partition: vec![], files: vec![file], adapter: None, row: None }))?)
}

/// A lake's file's deleted rows (positions, sorted), from its deletes.
pub async fn deleted_rows(lake: &crate::store::Lake, f: &crate::store::DataFile) -> Result<Vec<u64>> {
    let gone = lake_gone(lake, &lake.session(), &[f]).await?;
    Ok(gone.into_iter().flatten().next().map(|g| g.to_vec()).unwrap_or_default())
}
