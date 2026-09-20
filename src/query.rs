//! Hot + cold reads: every table = its Parquet files ∪ the log segments after them.
//! Reading the table meta first and the tail second gives one consistent snapshot:
//! the files cover segments <= `tiered`, the tail covers everything after.
//! Upsert tables (with a key) keep only the latest row per key; merge tables combine the rows
//! of each key (sum, min, max).
use crate::store::*;
use anyhow::{bail, Result};
use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

pub fn schema(columns: &[(String, String)]) -> Result<SchemaRef> {
    let fields = columns.iter().map(|(n, t)| Ok(Field::new(n, t.parse::<DataType>()?, true)));
    Ok(Arc::new(Schema::new(fields.collect::<Result<Vec<_>>>()?)))
}

/// Rows of `table` from committed segments in (after, upto]; `None` = up to the latest.
/// With `ord`, each row gets `_ord` = (segment << 32) + position, so later versions sort last.
pub async fn tail(lake: &Lake, table: &str, after: u64, upto: Option<u64>, ord: bool) -> Result<Vec<RecordBatch>> {
    let end = upto.map_or("s0".to_string(), |u| seg_key(u + 1)); // "s0" sorts right after every "s/…"
    let mut segs = vec![];
    for (key, seg) in lake.cat.scan::<Segment>(&seg_key(after + 1), &end).await? {
        if seg.parts.contains_key(table) {
            segs.push((key[2..].parse::<u64>()?, seg));
        }
    }
    // Fetch many segments at once: on object storage each one may be a round trip.
    let fetches: Vec<_> = segs.iter().map(|(n, seg)| lake.segment_rows(*n, seg, table)).collect();
    let fetched: Vec<Rows> = futures::stream::iter(fetches).buffered(32).try_collect().await?;
    let mut out = vec![];
    for ((n, _), rows) in segs.iter().zip(fetched) {
        let (n, mut pos) = (*n, 0u64);
        for b in rows.iter() {
            if !ord {
                out.push(b.clone());
                continue;
            }
            let ords: ArrayRef = Arc::new(UInt64Array::from_iter_values((pos..pos + b.num_rows() as u64).map(|p| (n << 32) + p)));
            pos += b.num_rows() as u64;
            let mut fields = b.schema().fields().to_vec();
            fields.push(Arc::new(Field::new("_ord", DataType::UInt64, false)));
            let mut cols = b.columns().to_vec();
            cols.push(ords);
            out.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?);
        }
    }
    Ok(out)
}

/// All rows of a table (files ∪ tail up to `upto`); upsert tables carry `_ord` (0 for files).
pub async fn raw(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, upto: Option<u64>) -> Result<DataFrame> {
    let (schema, keyed) = (schema(&meta.columns)?, !meta.key.is_empty());
    let mut parts = vec![];
    let opts = || ParquetReadOptions::default().schema(&schema);
    let path = |f: &DataFile| lake.full(&f.path);
    // Only upsert tables care which file a row came from (the newest version of the key wins).
    // Merge tables combine their rows in any order, so all their files read as one.
    match keyed && meta.merge.is_empty() {
        false if !meta.files.is_empty() => {
            let df = ctx.read_parquet(meta.files.iter().map(path).collect::<Vec<_>>(), opts()).await?;
            parts.push(if keyed { df.with_column("_ord", lit(0u64))? } else { df });
        }
        false => {}
        // One read per generation of files: `_ord` puts a newer file above an older one, and both
        // below the log segments that came after them.
        true => {
            let mut by_ord: std::collections::BTreeMap<u64, Vec<String>> = Default::default();
            for f in &meta.files {
                by_ord.entry(f.ord).or_default().push(path(f));
            }
            for (ord, files) in by_ord {
                parts.push(ctx.read_parquet(files, opts()).await?.with_column("_ord", lit(ord << 32))?);
            }
        }
    }
    let hot = tail(lake, name, meta.tiered, upto, keyed).await?;
    if !hot.is_empty() {
        let mut fields = schema.fields().to_vec(); // the table's own schema, so every batch agrees
        if keyed {
            fields.push(Arc::new(Field::new("_ord", DataType::UInt64, false)));
        }
        let s = Arc::new(Schema::new(fields));
        parts.push(ctx.read_batches(hot.into_iter().map(|b| b.with_schema(s.clone())).collect::<Result<Vec<_>, _>>()?)?);
    }
    Ok(match parts.into_iter().reduce(|a, b| a.union(b).expect("same schema")) {
        Some(df) => df,
        None if keyed => ctx.read_table(Arc::new(MemTable::try_new(schema, vec![vec![]])?))?.with_column("_ord", lit(0u64))?,
        None => ctx.read_table(Arc::new(MemTable::try_new(schema, vec![vec![]])?))?,
    })
}

/// Keyed tables as their users see them. Upsert tables: the latest row per key, without deleted
/// rows (a true `_deleted` column). Merge tables: each key's rows combined by their merge
/// functions. `sorted` orders by key, so a file prunes well on key lookups (used when writing
/// files). `keep_deleted` leaves delete markers in: an intermediate file still has to shadow what
/// older files hold for that key; a full compaction drops them.
pub fn latest_sql(meta: &TableMeta, raw_table: &str, sorted: bool, keep_deleted: bool) -> String {
    debug_assert!(!meta.key.is_empty(), "latest_sql needs a key: an append table has no versions");
    let q = |c: &String| format!("\"{c}\"");
    let key = meta.key.iter().map(q).collect::<Vec<_>>().join(", ");
    let order = if sorted { format!(" ORDER BY {key}") } else { String::new() };
    if !meta.merge.is_empty() {
        let cols = meta.columns.iter().map(|(c, _)| match meta.merge.get(c) {
            Some(f) => format!("{f}({}) AS {}", q(c), q(c)),
            None => q(c),
        });
        return format!("SELECT {} FROM \"{raw_table}\" GROUP BY {key}{order}", cols.collect::<Vec<_>>().join(", "));
    }
    // Newest version per key as a grouped aggregate (a hash table), not a window (a sort).
    let newest = |c: &String| format!("first_value({} ORDER BY \"_ord\" DESC) AS {}", q(c), q(c));
    let cols = meta.columns.iter().map(|(c, _)| if meta.key.contains(c) { q(c) } else { newest(c) });
    let out = meta.columns.iter().map(|(c, _)| q(c)).collect::<Vec<_>>().join(", ");
    let deleted = match keep_deleted {
        false => meta.columns.iter().any(|(c, _)| c == "_deleted").then_some(" WHERE \"_deleted\" IS NOT TRUE").unwrap_or(""),
        true => "",
    };
    format!("SELECT {out} FROM (SELECT {} FROM \"{raw_table}\" GROUP BY {key}){deleted}{order}",
            cols.collect::<Vec<_>>().join(", "))
}

/// The current rows of a keyed table, over `raw_table`. When the source already holds one row
/// per key — a single file, with nothing in the log after it — the "newest wins" window (or the
/// merge GROUP BY) is skipped, so reads of a compacted table cost a plain scan.
pub fn current_sql(lake: &Lake, meta: &TableMeta, raw_table: &str) -> String {
    if meta.files.len() > 1 || lake.visible() != meta.tiered {
        return latest_sql(meta, raw_table, false, false);
    }
    let cols = meta.columns.iter().map(|(c, _)| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
    let deleted = meta.columns.iter().any(|(c, _)| c == "_deleted").then_some(" WHERE \"_deleted\" IS NOT TRUE").unwrap_or("");
    format!("SELECT {cols} FROM \"{raw_table}\"{deleted}")
}

/// A session with every table referenced in `sql` registered (cheap name filter), except
/// `except`, which the caller registers itself.
pub async fn session(lake: &Lake, sql: &str, except: &str) -> Result<SessionContext> {
    let ctx = lake.session();
    for (key, meta) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        let name = &key[2..];
        if !sql.contains(name) || name == except {
            continue;
        }
        let df = raw(lake, &ctx, name, &meta, None).await?;
        if meta.key.is_empty() {
            ctx.register_table(name, df.into_view())?;
        } else {
            let raw_name = format!("__raw_{name}");
            ctx.register_table(raw_name.as_str(), df.into_view())?;
            ctx.register_table(name, ctx.sql(&current_sql(lake, &meta, &raw_name)).await?.into_view())?;
        }
    }
    Ok(ctx)
}

/// Run `sql` with table `source` standing for just `rows` (new rows of a streaming source); every
/// other table it mentions is read from the lake as usual.
pub async fn over(lake: &Lake, source: &str, rows: Vec<RecordBatch>, sql: &str) -> Result<RecordBatch> {
    let meta: TableMeta = lake.cat.get(&table_key(source)).await?.ok_or_else(|| anyhow::anyhow!("no table {source}"))?;
    let ctx = session(lake, sql, source).await?;
    ctx.register_table(source, Arc::new(MemTable::try_new(schema(&meta.columns)?, vec![rows])?))?;
    let df = ctx.sql(sql).await?;
    let out = Arc::new(df.schema().as_arrow().clone());
    Ok(datafusion::arrow::compute::concat_batches(&out, &df.collect().await?)?)
}

/// The first table in the FROM clause (looking inside a FROM subquery): the stream a view follows,
/// or the table a distributed query slices.
pub fn first_table(sql: &str) -> Result<String> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    fn first(q: &Query) -> Option<String> {
        let SetExpr::Select(s) = q.body.as_ref() else { return None };
        match &s.from.first()?.relation {
            TableFactor::Table { name, .. } => Some(name.to_string().trim_matches('"').to_string()),
            TableFactor::Derived { subquery, .. } => first(subquery),
            _ => None,
        }
    }
    match &Parser::parse_sql(&GenericDialect {}, sql)?[..] {
        [Statement::Query(q)] => first(q).ok_or_else(|| anyhow::anyhow!("a view is one SELECT … FROM <table> …")),
        _ => bail!("a view is one SELECT … FROM <table> …"),
    }
}
