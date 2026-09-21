//! Serving reads without SQL. The current row of one key in an upsert table is found the way an
//! LSM finds it: the log tail newest-first, then the files newest-first. Each file is narrowed to
//! the row groups whose key range can hold the key (Parquet statistics), and a row group is
//! decoded once and then kept in memory. A lookup costs a scan of one cached row group — tens of
//! microseconds — instead of planning and running a query.
use crate::store::*;
use anyhow::{Context, Result};
use datafusion::arrow::array::{make_comparator, Array, BooleanArray, RecordBatch};
use datafusion::arrow::compute::kernels::cmp::{eq, lt_eq};
use datafusion::arrow::compute::{and, cast, concat_batches, SortOptions};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use datafusion::parquet::file::statistics::Statistics;
use datafusion::scalar::ScalarValue;
use object_store_df::ObjectStoreExt;
use futures::future::{BoxFuture, FutureExt};
use futures::TryStreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Decoded row groups (least recently used out past `max` bytes) and Parquet footers, per node.
pub struct Groups {
    rows: Mutex<(lru::LruCache<(String, usize), (RecordBatch, bool)>, usize)>, // (rows, sorted by key)
    footers: Mutex<HashMap<String, ArrowReaderMetadata>>,
    max: usize,
}

impl Groups {
    pub fn new(max: usize) -> Self { Groups { rows: Mutex::new((lru::LruCache::unbounded(), 0)), footers: Default::default(), max } }
}

/// The newest row of `key` (comma-separated values, in key order) in upsert table `meta`, if it
/// exists and isn't deleted. Table metadata first, then the log: one consistent snapshot.
pub async fn lookup(lake: &Lake, table: &str, meta: &TableMeta, key: &str) -> Result<Option<RecordBatch>> {
    let types: HashMap<&str, &str> = meta.columns.iter().map(|(c, t)| (c.as_str(), t.as_str())).collect();
    let keys: Vec<(&str, ScalarValue)> = meta.key.iter().zip(key.split(',')).map(|(c, v)| {
        Ok((c.as_str(), ScalarValue::try_from_string(v.to_string(), &types[c.as_str()].parse()?)?))
    }).collect::<Result<_>>()?;
    anyhow::ensure!(keys.len() == meta.key.len(), "the key of {table} has {} parts", meta.key.len());
    let row = match newest_in_log(lake, table, meta, &keys).await? {
        Some(row) => Some(row),
        None => newest_in_files(lake, meta, &keys).await?,
    };
    let deleted = |r: &RecordBatch| r.column_by_name("_deleted").and_then(|c| c.as_any().downcast_ref::<BooleanArray>().map(|b| b.value(0)));
    Ok(row.filter(|r| deleted(r) != Some(true)))
}

/// A SQL point query answered like `/lookup`: exactly `SELECT <columns or *> FROM <upsert table>
/// WHERE <key> = <literal> [AND …]` naming every key column, and nothing else. Returns the rows
/// as JSON, or `None` for any other query (it runs as SQL).
pub async fn point_sql(lake: &Lake, sql: &str) -> Result<Option<Vec<u8>>> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return Ok(None) };
    let (Some(stmt), true) = (stmts.pop(), stmts.is_empty()) else { return Ok(None) };
    let Statement::Query(q) = &stmt else { return Ok(None) };
    let SetExpr::Select(s) = q.body.as_ref() else { return Ok(None) };
    let (Some(TableWithJoins { relation: TableFactor::Table { name, .. }, .. }), Some(filter)) = (s.from.first(), &s.selection) else { return Ok(None) };
    // Nothing but these parts: the statement must print back as exactly them.
    let items = s.projection.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ");
    if stmt.to_string() != format!("SELECT {items} FROM {name} WHERE {filter}") {
        return Ok(None);
    }
    let mut eqs = vec![];
    let mut stack = vec![filter];
    while let Some(e) = stack.pop() {
        match e {
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => stack.extend([left.as_ref(), right.as_ref()]),
            Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(c), Expr::Value(v)) => eqs.push((c.value.clone(), v.to_string().trim_matches('\'').to_string())),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        }
    }
    let table = name.to_string().trim_matches('"').to_string();
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&table)).await? else { return Ok(None) };
    let mut cols: Vec<&String> = eqs.iter().map(|(c, _)| c).collect();
    cols.sort();
    let mut key: Vec<&String> = meta.key.iter().collect();
    key.sort();
    if meta.key.is_empty() || !meta.merge.is_empty() || cols != key {
        return Ok(None);
    }
    let names: Vec<String> = match s.projection.as_slice() {
        [SelectItem::Wildcard(_)] => meta.columns.iter().map(|(c, _)| c.clone()).collect(),
        items => items.iter().map(|i| match i {
            SelectItem::UnnamedExpr(Expr::Identifier(c)) if meta.columns.iter().any(|(n, _)| *n == c.value) => Some(c.value.clone()),
            _ => None,
        }).collect::<Option<_>>().unwrap_or_default(),
    };
    if names.is_empty() {
        return Ok(None);
    }
    let key = meta.key.iter().map(|k| eqs.iter().find(|(c, _)| c == k).map(|(_, v)| v.as_str()).unwrap_or("")).collect::<Vec<_>>().join(",");
    let row = lookup(lake, &table, &meta, &key).await?.map(|r| r.project(&names.iter().map(|n| r.schema().index_of(n)).collect::<Result<Vec<_>, _>>()?)).transpose()?;
    let mut w = datafusion::arrow::json::ArrayWriter::new(Vec::new());
    w.write_batches(&row.iter().collect::<Vec<_>>())?;
    w.finish()?;
    Ok(Some(if row.is_none() { b"[]".to_vec() } else { w.into_inner() }))
}

/// The log tail, newest segment and row first (decoded segments are cached per node).
async fn newest_in_log(lake: &Lake, table: &str, meta: &TableMeta, keys: &[(&str, ScalarValue)]) -> Result<Option<RecordBatch>> {
    for (k, seg) in lake.cat.scan::<Segment>(&seg_key(meta.tiered + 1), "s0").await?.into_iter().rev() {
        let rows = lake.segment_rows(k[2..].parse()?, &seg, table).await?;
        for batch in rows.iter().rev() {
            if let Some(i) = last_match(batch, keys, false)? {
                return Ok(Some(batch.slice(i, 1)));
            }
        }
    }
    Ok(None)
}

/// The files, newest first (each holds one row per key).
async fn newest_in_files(lake: &Lake, meta: &TableMeta, keys: &[(&str, ScalarValue)]) -> Result<Option<RecordBatch>> {
    let mut files: Vec<&DataFile> = meta.files.iter().collect();
    files.sort_by_key(|f| std::cmp::Reverse(f.ord));
    for f in files {
        let footer = footer(lake, f).await?;
        let schema = footer.metadata().file_metadata().schema_descr();
        let col = schema.columns().iter().position(|c| c.name() == keys[0].0).context("key column not in file")?;
        for g in 0..footer.metadata().num_row_groups() {
            if !may_hold(footer.metadata().row_group(g).column(col).statistics(), &keys[0].1) {
                continue;
            }
            let (batch, sorted) = row_group(lake, f, &footer, g, keys[0].0).await?;
            if let Some(i) = last_match(&batch, keys, sorted)? {
                return Ok(Some(batch.slice(i, 1)));
            }
        }
    }
    Ok(None)
}

/// Index of the last row whose key columns equal `keys`. In a batch `sorted` by key, a binary
/// search on the first key column narrows it to the matching rows first.
fn last_match(batch: &RecordBatch, keys: &[(&str, ScalarValue)], sorted: bool) -> Result<Option<usize>> {
    let (from, to) = match sorted {
        true => {
            let col = batch.column_by_name(keys[0].0).context("key column missing")?;
            let cmp = make_comparator(col.as_ref(), keys[0].1.to_array()?.as_ref(), SortOptions::default())?;
            let n = batch.num_rows();
            (first(n, |i| cmp(i, 0).is_lt()), first(n, |i| cmp(i, 0).is_le()))
        }
        false => (0, batch.num_rows()),
    };
    if from >= to {
        return Ok(None);
    }
    let batch = batch.slice(from, to - from);
    let mut hits: Option<BooleanArray> = None;
    for (c, v) in keys {
        let col = batch.column_by_name(c).context("key column missing")?;
        let col = if col.data_type() == &v.data_type() { col.clone() } else { cast(col, &v.data_type())? };
        let m = eq(&col, &v.to_scalar()?)?;
        hits = Some(match hits { Some(h) => and(&h, &m)?, None => m });
    }
    let hits = hits.context("no key")?;
    Ok((0..hits.len()).rev().find(|&i| hits.is_valid(i) && hits.value(i)).map(|i| i + from))
}

/// The first index in 0..n where `before` is false (it is true on a prefix): a binary search.
fn first(n: usize, before: impl Fn(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0, n);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if before(mid) { lo = mid + 1 } else { hi = mid }
    }
    lo
}

/// Whether a row group's min/max can contain `key` (unknown types and missing statistics: yes).
fn may_hold(stats: Option<&Statistics>, key: &ScalarValue) -> bool {
    match (stats, key) {
        (Some(Statistics::Int64(s)), ScalarValue::Int64(Some(k))) => s.min_opt().is_none_or(|m| m <= k) && s.max_opt().is_none_or(|m| k <= m),
        (Some(Statistics::Int32(s)), ScalarValue::Int32(Some(k))) => s.min_opt().is_none_or(|m| m <= k) && s.max_opt().is_none_or(|m| k <= m),
        (Some(Statistics::ByteArray(s)), ScalarValue::Utf8(Some(k))) => {
            s.min_opt().is_none_or(|m| m.data() <= k.as_bytes()) && s.max_opt().is_none_or(|m| k.as_bytes() <= m.data())
        }
        _ => true,
    }
}

/// A file's Parquet footer, read once per node (files never change).
async fn footer(lake: &Lake, f: &DataFile) -> Result<ArrowReaderMetadata> {
    if let Some(m) = lake.groups.footers.lock().unwrap().get(&f.path) {
        return Ok(m.clone());
    }
    let m = ArrowReaderMetadata::load_async(&mut reader(lake, f)?, ArrowReaderOptions::new()).await?;
    lake.groups.footers.lock().unwrap().insert(f.path.clone(), m.clone());
    Ok(m)
}

/// Row group `g` of a file, decoded (through the SSD tier and read cache, like any query's reads),
/// and whether it is sorted by column `key` (files are written sorted by key; older ones may not be).
async fn row_group(lake: &Lake, f: &DataFile, footer: &ArrowReaderMetadata, g: usize, key: &str) -> Result<(RecordBatch, bool)> {
    let id = (f.path.clone(), g);
    if let Some(b) = lake.groups.rows.lock().unwrap().0.get(&id) {
        return Ok(b.clone());
    }
    let rows = footer.metadata().row_group(g).num_rows() as usize;
    let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(reader(lake, f)?, footer.clone()).with_row_groups(vec![g]).with_batch_size(rows.max(1)).build()?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    let batch = concat_batches(&footer.schema().clone(), &batches)?;
    let col = batch.column_by_name(key).context("key column missing")?;
    let n = col.len();
    let sorted = n < 2 || lt_eq(&col.slice(0, n - 1), &col.slice(1, n - 1))?.true_count() == n - 1;
    let mut c = lake.groups.rows.lock().unwrap();
    c.1 += batch.get_array_memory_size();
    if let Some((old, _)) = c.0.put(id, (batch.clone(), sorted)) {
        c.1 -= old.get_array_memory_size();
    }
    while c.1 > lake.groups.max {
        let Some((_, (old, _))) = c.0.pop_lru() else { break };
        c.1 -= old.get_array_memory_size();
    }
    Ok((batch, sorted))
}

/// A file, read by byte range through the store DataFusion uses for it.
struct Reader(Arc<dyn object_store_df::ObjectStore>, object_store_df::path::Path, u64);

fn reader(lake: &Lake, f: &DataFile) -> Result<Reader> {
    let url = ListingTableUrl::parse(lake.full(&f.path))?;
    Ok(Reader(lake.object_store(&url)?, url.prefix().clone(), f.bytes))
}

impl AsyncFileReader for Reader {
    fn get_bytes(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, datafusion::parquet::errors::Result<bytes::Bytes>> {
        async move { self.0.get_range(&self.1, range).await.map_err(|e| ParquetError::External(Box::new(e))) }.boxed()
    }

    fn get_metadata<'a>(&'a mut self, _: Option<&'a ArrowReaderOptions>) -> BoxFuture<'a, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let size = self.2;
        async move { Ok(Arc::new(ParquetMetaDataReader::new().load_and_finish(self, size).await?)) }.boxed()
    }
}
