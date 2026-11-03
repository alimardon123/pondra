//! Serving reads without SQL. The current row of one key in an upsert table is found the way an
//! LSM finds it: the log tail newest-first, then the files newest-first. Each file is narrowed to
//! the row groups whose key range can hold the key (Parquet statistics), and a row group is
//! decoded once and then kept in memory. A lookup costs a scan of one cached row group — tens of
//! microseconds — instead of planning and running a query.
use crate::store::*;
use crate::ledger::Charged;
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

/// Decoded row groups (least recently used out past `new`'s bytes) and Parquet footers, per node.
pub struct Groups {
    rows: Mutex<crate::ledger::ByteLru<(String, usize), (RecordBatch, bool)>>, // (rows, sorted by key), each charged its rows' memory
    footers: Mutex<HashMap<String, ArrowReaderMetadata>>,
}

/// The row groups held for lookups (at most `max` bytes), the least recently used given back first.
impl Charged for Groups {
    fn held(&self) -> usize { self.rows.held() }

    fn give_back(&self, bytes: usize) -> usize { self.rows.give_back(bytes) }
}

impl Groups {
    pub fn new(max: usize) -> Self { Groups { rows: Mutex::new(crate::ledger::ByteLru::new(max)), footers: Default::default() } }
}

/// The newest row of `key` (comma-separated values, in key order) in upsert table `meta`, if it
/// exists, isn't deleted and isn't past the table's TTL. Table metadata first, then the log: one consistent snapshot.
pub async fn lookup(lake: &Lake, table: &str, meta: &TableMeta, key: &str) -> Result<Option<RecordBatch>> {
    let types: HashMap<&str, &str> = meta.columns.iter().map(|(c, t)| (c.as_str(), t.as_str())).collect();
    let keys: Vec<(&str, ScalarValue)> = meta.key.iter().zip(key.split(',')).map(|(c, v)| {
        Ok((c.as_str(), ScalarValue::try_from_string(v.to_string(), &types[c.as_str()].parse()?)?))
    }).collect::<Result<_>>()?;
    anyhow::ensure!(keys.len() == meta.key.len(), "the key of {table} has {} parts", meta.key.len());
    let row = match newest_in_log(lake, table, meta, &keys).await? {
        Some((row, _, _)) => Some(row),
        None => newest_in_files(lake, meta, &keys).await?,
    };
    // (a live row's `_deleted` is NULL, whose value bit is whatever the decoder left: check it's valid)
    let deleted = |r: &RecordBatch| r.column_by_name("_deleted").and_then(|c| c.as_any().downcast_ref::<BooleanArray>().map(|b| b.is_valid(0) && b.value(0)));
    let Some(r) = row else { return Ok(None) };
    let now = crate::log::now_us() as i64;
    Ok((deleted(&r) != Some(true) && !meta.past_ttl(&r, now)?).then_some(r))
}

/// A SQL point query: exactly `SELECT <columns or *> FROM <upsert table> WHERE <key> = <value>
/// [AND …]` naming every key column, and nothing else. The value may be a literal or a parameter
/// (`$1`: a prepared statement's, whose columns and parameter types are known without planning).
pub struct Point {
    pub table: String,
    pub meta: Arc<TableMeta>,
    pub names: Vec<String>,      // the columns it returns
    pub key: Vec<Option<String>>, // each key column's value, in key order (None: a parameter)
    params: HashMap<String, usize>, // a key column a parameter stands for -> its number ($n)
}

impl Point {
    /// Its answer's columns.
    pub fn schema(&self) -> Result<datafusion::arrow::datatypes::SchemaRef> {
        let types: HashMap<&str, &str> = self.meta.columns.iter().map(|(c, t)| (c.as_str(), t.as_str())).collect();
        crate::answer::schema(&self.names.iter().map(|n| (n.clone(), types[n.as_str()].to_string())).collect::<Vec<_>>())
    }

    /// Which types its columns are (`pg_types`: a uuid, json or jsonb): its table's own, by name.
    pub fn types(&self) -> Vec<Option<crate::pg_types::Ty>> { self.names.iter().map(|n| self.meta.sql_types.get(n).and_then(|t| crate::pg_types::Ty::of(t))).collect() }

    /// Its parameters' types (`$1`…, in order), as the key columns they stand for.
    pub fn parameters(&self, sql: &str) -> Vec<datafusion::arrow::datatypes::DataType> {
        let n = (1..).take_while(|i| sql.contains(&format!("${i}"))).count();
        let types: HashMap<&str, &str> = self.meta.columns.iter().map(|(c, t)| (c.as_str(), t.as_str())).collect();
        let mut out = vec![datafusion::arrow::datatypes::DataType::Utf8; n];
        for (k, v) in self.meta.key.iter().zip(&self.key) {
            if v.is_none() {
                if let Some(i) = self.params.get(k).copied().filter(|i| (1..=n).contains(i)) {
                    out[i - 1] = crate::answer::dtype(types[k.as_str()]).unwrap_or(datafusion::arrow::datatypes::DataType::Utf8);
                }
            }
        }
        out
    }

    /// The row, or none: as its columns (no planning).
    pub async fn rows(&self, lake: &Lake) -> Result<Option<RecordBatch>> {
        let Some(key) = self.key.iter().cloned().collect::<Option<Vec<String>>>() else { return Ok(None) };
        let schema = self.schema()?;
        let row = lookup(lake, &self.table, &self.meta, &key.join(",")).await?;
        Ok(Some(match row {
            Some(r) => crate::query::conform(&r, &schema)?, // (by name: a row from before an ADD COLUMN lacks it)
            None => RecordBatch::new_empty(schema),
        }))
    }
}

/// `sql` as a point query, if it is one (`Point`): worked out once per catalog version. Not per
/// `visible()`, which only the log's segments move: an `ALTER TABLE` or a tiering round left a kept
/// `Point` with the table's old columns and files. A node whose catalog has no version (a follower
/// between seeds) works it out each time.
pub async fn point(lake: &Lake, sql: &str) -> Result<Option<Arc<Point>>> {
    type Seen = lru::LruCache<(String, String), (u64, Option<Arc<Point>>)>;
    crate::per_node!(static SEEN: Mutex<Seen> = Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(4096).expect("non-zero"))); clear = |m| m.lock().unwrap_or_else(|e| e.into_inner()).clear());
    let head = sql.trim_start();
    if !head.get(..6).is_some_and(|w| w.eq_ignore_ascii_case("select")) {
        return Ok(None); // (most statements: no parse)
    }
    let Some(now) = lake.cat.version() else { return Ok(point_of(lake, sql).await?.map(Arc::new)) };
    let key = (lake.url.clone(), sql.to_string());
    if let Some((v, p)) = SEEN.lock().unwrap().get(&key) {
        if *v == now {
            return Ok(p.clone());
        }
    }
    let p = point_of(lake, sql).await?.map(Arc::new);
    SEEN.lock().unwrap().put(key, (now, p.clone()));
    Ok(p)
}

async fn point_of(lake: &Lake, sql: &str) -> Result<Option<Point>> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    let head = sql.trim_start();
    if !head.get(..6).is_some_and(|w| w.eq_ignore_ascii_case("select")) || !sql.to_lowercase().contains(" where ") {
        return Ok(None); // (most statements: no parse)
    }
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
    let table = name.to_string().trim_matches('"').to_string();
    let Some(meta) = lake.cat.meta(&table_key(&table)).await? else { return Ok(None) };
    if !pointable(&meta) {
        return Ok(None);
    }
    let Some((key, params)) = key_of(&meta, filter) else { return Ok(None) };
    let names: Vec<String> = match s.projection.as_slice() {
        [SelectItem::Wildcard(_)] => meta.columns.iter().map(|(c, _)| c.clone()).filter(|c| c != "_deleted").collect(), // (as `SELECT *` shows it)
        items => items.iter().map(|i| match i {
            SelectItem::UnnamedExpr(Expr::Identifier(c)) if meta.columns.iter().any(|(n, _)| *n == c.value) => Some(c.value.clone()),
            _ => None,
        }).collect::<Option<_>>().unwrap_or_default(),
    };
    if names.is_empty() {
        return Ok(None);
    }
    Ok(Some(Point { table, meta, names, key, params }))
}

/// A table a key lookup answers without SQL: keyed, with no merge, no renamed or moved columns, no event-time order and no
/// TTL (SQL answers those).
fn pointable(meta: &TableMeta) -> bool {
    !meta.key.is_empty() && meta.merge.is_empty() && !meta.mapped() && !meta.moved() && meta.order.is_none() && meta.ttl.is_none()
}

/// The key `filter` names: `<key> = <value> [AND …]`, each of `meta`'s key columns once and no other column. Its values in
/// key order (None: a parameter, whose number the map gives). None: any other condition.
fn key_of(meta: &TableMeta, filter: &datafusion::sql::sqlparser::ast::Expr) -> Option<(Vec<Option<String>>, HashMap<String, usize>)> {
    use datafusion::sql::sqlparser::ast::{BinaryOperator, DollarQuotedString, Expr, Value};
    let text = |c: &str| meta.columns.iter().any(|(n, t)| n == c && t.contains("Utf8"));
    let (mut eqs, mut params) = (vec![], HashMap::new());
    let mut stack = vec![filter];
    while let Some(e) = stack.pop() {
        match e {
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => stack.extend([left.as_ref(), right.as_ref()]),
            Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(c), Expr::Value(v)) => match &v.value {
                    Value::Placeholder(p) => {
                        params.insert(c.value.clone(), p.trim_start_matches('$').parse::<usize>().unwrap_or(0));
                        eqs.push((c.value.clone(), None));
                    }
                    // (a string's value, not the text it was typed as: `'O''Brien'`, `E'O\'Brien'`, `$$O'Brien$$` and
                    // `N'O''Brien'` are all `O'Brien`)
                    Value::SingleQuotedString(s) | Value::EscapedStringLiteral(s) | Value::NationalStringLiteral(s) | Value::DollarQuotedString(DollarQuotedString { value: s, .. }) => eqs.push((c.value.clone(), Some(s.clone()))),
                    // (a number for a text key is compared as SQL converts the two: SQL answers it)
                    Value::Number(n, false) if !text(&c.value) => eqs.push((c.value.clone(), Some(n.clone()))),
                    Value::Boolean(b) => eqs.push((c.value.clone(), Some(b.to_string()))),
                    _ => return None, // (NULL, X'…', b'…', U&'…' and the like: SQL answers them)
                },
                _ => return None,
            },
            _ => return None,
        }
    }
    let mut cols: Vec<&String> = eqs.iter().map(|(c, _)| c).collect();
    cols.sort();
    let mut key: Vec<&String> = meta.key.iter().collect();
    key.sort();
    if cols != key {
        return None;
    }
    // (a uuid key written another way Postgres takes, `'A0EE…'`, is looked up as the column holds it)
    let key = meta.key.iter().map(|k| eqs.iter().find(|(c, _)| c == k).and_then(|(_, v)| v.clone()).map(|v| if meta.uuid(k) { crate::pg_casts::uuid_spelled(&v).unwrap_or(v) } else { v })).collect();
    Some((key, params))
}

/// The key a write's `WHERE` names on `meta`'s table, as a key lookup's (`point`) would: from the condition alone, with no
/// statement around it to parse (an UPDATE's, `txn::point_key`). None: not one key's, or a parameter in it.
pub fn key_where(meta: &TableMeta, cond: &str) -> Option<Vec<String>> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Token};
    if !pointable(meta) {
        return None;
    }
    let mut p = Parser::new(&GenericDialect {}).try_with_sql(cond).ok()?;
    let filter = p.parse_expr().ok()?;
    if p.peek_token().token != Token::EOF {
        return None;
    }
    key_of(meta, &filter)?.0.into_iter().collect()
}

/// A SQL point query answered like `/lookup` (`Point`): its rows as JSON, or `None` for any other
/// query (it runs as SQL).
pub async fn point_sql(lake: &Lake, sql: &str) -> Result<Option<Vec<u8>>> {
    let Some(p) = point(lake, sql).await? else { return Ok(None) };
    let Some(row) = p.rows(lake).await? else { return Ok(None) };
    if row.num_rows() == 0 {
        return Ok(Some(b"[]".to_vec()));
    }
    let mut w = datafusion::arrow::json::ArrayWriter::new(Vec::new());
    w.write_batches(&[&row])?;
    w.finish()?;
    Ok(Some(w.into_inner()))
}

/// The log tail's rows of one table by the hash of their key, each hash to its newest row
/// (segment, batch, row; invariant 213). Kept per node while the table's `tiered` mark stays, and brought up to
/// date with only the segments committed since, so a lookup is one probe, not a scan of every
/// segment since the last tiering round: a table taking a few hundred small commits a second had
/// thousands to scan (1,700 lookups a second instead of 38,000). The row a hash names is checked;
/// another key of the same hash falls back to the scan.
struct Tail {
    tiered: u64,
    types: Vec<datafusion::arrow::datatypes::DataType>, // (the key's, which the hashes are of)
    upto: u64,                                           // every segment to here is in `rows`
    rows: HashMap<u64, (u64, u32, u32)>,
    segs: HashMap<u64, Arc<Segment>>,
}

const KEYS: datafusion::common::hash_utils::RandomState = datafusion::common::hash_utils::RandomState::with_seed(0);

/// Each row's hash of its key columns, as the key's types.
fn hashes(batch: &RecordBatch, keys: &[(&str, ScalarValue)]) -> Result<Vec<u64>> {
    let cols = keys.iter().map(|(c, v)| {
        let col = batch.column_by_name(c).context("key column missing")?;
        Ok(if col.data_type() == &v.data_type() { col.clone() } else { cast(col, &v.data_type())? })
    }).collect::<Result<Vec<_>>>()?;
    let mut out = vec![0; batch.num_rows()];
    datafusion::common::hash_utils::create_hashes(&cols, &KEYS, &mut out)?;
    Ok(out)
}

/// The log tail's newest row of the key, with the commit that wrote it and when (`Tail`).
async fn newest_in_log(lake: &Lake, table: &str, meta: &TableMeta, keys: &[(&str, ScalarValue)]) -> Result<Option<(RecordBatch, u64, u64)>> {
    crate::per_node!(static TAILS: Mutex<lru::LruCache<(String, String), Arc<Mutex<Tail>>>> = Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(16).expect("non-zero"))); clear = |m| m.lock().unwrap_or_else(|e| e.into_inner()).clear());
    let types: Vec<_> = keys.iter().map(|(_, v)| v.data_type()).collect();
    let tail = {
        let mut all = TAILS.lock().unwrap();
        let id = (lake.url.clone(), table.to_string());
        match all.get(&id).filter(|t| matches!(&*t.lock().unwrap(), t if t.tiered == meta.tiered && t.types == types)) {
            Some(t) => t.clone(),
            None => {
                let t = Arc::new(Mutex::new(Tail { tiered: meta.tiered, types, upto: meta.tiered, rows: HashMap::new(), segs: HashMap::new() }));
                all.put(id, t.clone());
                t
            }
        }
    };
    // The segments committed since, hashed outside the lock (their rows may have to be read).
    let from = tail.lock().unwrap().upto;
    let mut fresh = vec![];
    for (n, seg) in lake.segments_after(from).await? {
        let hashed = match seg.parts.contains_key(table) {
            true => lake.segment_rows(n, &seg, table).await?.iter().map(|b| hashes(b, keys)).collect::<Result<Vec<_>>>()?,
            false => vec![],
        };
        fresh.push((n, seg, hashed));
    }
    let hit = {
        let mut t = tail.lock().unwrap();
        for (n, seg, hashed) in fresh {
            if n <= t.upto {
                continue; // (another lookup brought it this far)
            }
            t.upto = n;
            for (b, hs) in hashed.iter().enumerate() {
                for (i, h) in hs.iter().enumerate() {
                    t.rows.insert(*h, (n, b as u32, i as u32)); // (a later row wins)
                }
            }
            if !hashed.is_empty() {
                t.segs.insert(n, seg);
            }
        }
        if t.rows.is_empty() {
            return Ok(None); // (no rows of the table since its files)
        }
        let mut want = [0];
        datafusion::common::hash_utils::create_hashes(&keys.iter().map(|(_, v)| v.to_array()).collect::<Result<Vec<_>, _>>()?, &KEYS, &mut want)?;
        t.rows.get(&want[0]).map(|&(n, b, i)| (n, t.segs[&n].clone(), b as usize, i as usize))
    };
    let Some((n, seg, b, i)) = hit else { return Ok(None) };
    let row = lake.segment_rows(n, &seg, table).await?[b].slice(i, 1);
    if last_match(&row, keys, false)?.is_some() {
        return Ok(Some((row, n, seg.ts_ms)));
    }
    for (n, seg) in lake.segments_after(meta.tiered).await?.into_iter().rev() {
        if seg.parts.contains_key(table) {
            for batch in lake.segment_rows(n, &seg, table).await?.iter().rev() {
                if let Some(i) = last_match(batch, keys, false)? {
                    return Ok(Some((batch.slice(i, 1), n, seg.ts_ms)));
                }
            }
        }
    }
    Ok(None)
}

/// `lookup`, with the row's `_version` (the commit that wrote it) and `_created_at` (µs): what a
/// transaction's change of one key needs (`txn::point_change`), and the 23505 check of a key (`constraints::held`). None: no
/// such row, a deleted one, one past the TTL (gone, as a statement sees it), or one from before rows had their system columns
/// in files.
pub async fn lookup_versioned(lake: &Lake, table: &str, meta: &TableMeta, key: &[String]) -> Result<Option<(RecordBatch, u64, Option<i64>)>> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit, TimestampMicrosecondType};
    let types: HashMap<&str, &str> = meta.columns.iter().map(|(c, t)| (c.as_str(), t.as_str())).collect();
    let keys: Vec<(&str, ScalarValue)> = meta.key.iter().zip(key).map(|(c, v)| Ok((c.as_str(), ScalarValue::try_from_string(v.to_string(), &types[c.as_str()].parse()?)?))).collect::<Result<_>>()?;
    anyhow::ensure!(keys.len() == meta.key.len(), "the key of {table} has {} parts", meta.key.len());
    let created = |r: &RecordBatch| -> Result<Option<i64>> {
        let Some(c) = r.column_by_name(crate::text::CREATED).filter(|c| c.is_valid(0)) else { return Ok(None) };
        Ok(Some(cast(c, &DataType::Timestamp(TimeUnit::Microsecond, None))?.as_primitive::<TimestampMicrosecondType>().value(0)))
    };
    let deleted = |r: &RecordBatch| r.column_by_name("_deleted").and_then(|c| c.as_any().downcast_ref::<BooleanArray>().map(|b| b.is_valid(0) && b.value(0))) == Some(true);
    let now = crate::log::now_us() as i64;
    if let Some((r, n, ts)) = newest_in_log(lake, table, meta, &keys).await? {
        let at = created(&r)?.unwrap_or(ts as i64 * 1000); // (a new row's: its commit's time)
        return Ok((!deleted(&r) && !meta.past_ttl(&r, now)?).then_some((r, n, Some(at))));
    }
    let Some(r) = newest_in_files(lake, meta, &keys).await? else { return Ok(None) };
    let Some(v) = r.column_by_name(crate::text::VERSION).filter(|c| c.is_valid(0)).map(|c| cast(c, &DataType::Int64)).transpose()? else { return Ok(None) };
    let v = v.as_primitive::<Int64Type>().value(0) as u64;
    let at = created(&r)?;
    Ok((!deleted(&r) && !meta.past_ttl(&r, now)?).then_some((r, v, at)))
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
    if let Some(b) = lake.groups.rows.lock().unwrap().get(&id) {
        return Ok(b.clone());
    }
    let rows = footer.metadata().row_group(g).num_rows() as usize;
    let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(reader(lake, f)?, footer.clone()).with_row_groups(vec![g]).with_batch_size(rows.max(1)).build()?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    let batch = concat_batches(&footer.schema().clone(), &batches)?;
    let col = batch.column_by_name(key).context("key column missing")?;
    let n = col.len();
    let sorted = n < 2 || lt_eq(&col.slice(0, n - 1), &col.slice(1, n - 1))?.true_count() == n - 1;
    let bytes = batch.get_array_memory_size();
    let left = lake.groups.rows.lock().unwrap().put(id, (batch.clone(), sorted), bytes);
    drop(left); // (let go of outside the lock)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_writes_where_names_its_key_as_a_lookup_does() {
        let meta = TableMeta {
            columns: vec![("a".into(), "Int64".into()), ("b".into(), "Utf8".into()), ("v".into(), "Int64".into()), ("u".into(), "Utf8".into())],
            key: vec!["a".into(), "b".into()],
            ..Default::default()
        };
        let key = |cond: &str| key_where(&meta, cond);
        let k = |a: &str, b: &str| Some(vec![a.to_string(), b.to_string()]);
        assert_eq!(key("a = 1 AND b = 'x'"), k("1", "x"));
        assert_eq!(key("b = 'O''Brien' AND a = 2"), k("2", "O'Brien"), "in key order, a string as its value");
        assert_eq!(key("(a = 1) AND b = 'x'"), None, "the shapes a lookup takes, no others");
        for other in ["a = 1", "a = 1 AND b = 'x' AND v = 3", "a = 1 AND a = 2", "a = 1 OR b = 'x'", "a > 1 AND b = 'x'", "a = NULL AND b = 'x'", "a = $1 AND b = 'x'", "a = 1 AND b = 'x' x", "a = -1 AND b = 'x'", ""] {
            assert_eq!(key(other), None, "{other}");
        }
        // (a string written any way Postgres takes is its value; a form not taken here is SQL's to answer)
        for spelled in ["b = E'O\\'Brien' AND a = 2", "b = $$O'Brien$$ AND a = 2", "b = $q$O'Brien$q$ AND a = 2", "b = N'O''Brien' AND a = 2"] {
            assert_eq!(key(spelled), k("2", "O'Brien"), "{spelled}");
        }
        assert_eq!(key("b = E'a\\tb' AND a = 2"), k("2", "a\tb"), "an escape as the character it stands for");
        for other in ["a = 1 AND b = X'61'", "a = 1 AND b = 5", "a = 1.5L AND b = 'x'", "a = 1 AND b = U&'x'"] {
            assert_eq!(key(other), None, "{other}");
        }
        assert_eq!(key("a = true AND b = 'x'"), k("true", "x"), "a boolean as before (its key's type converts it or SQL answers)");
        let uuid = TableMeta { columns: vec![("u".into(), "Utf8".into())], key: vec!["u".into()], sql_types: [("u".to_string(), "uuid".to_string())].into(), ..Default::default() };
        assert_eq!(key_where(&uuid, "u = 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'"), Some(vec!["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".to_string()]), "a uuid as the column holds it");
        let ttl = TableMeta { ttl: Some(("v".into(), 60)), ..meta.clone() };
        assert_eq!(key_where(&ttl, "a = 1 AND b = 'x'"), None, "a TTL's table: SQL answers it");
    }
}
