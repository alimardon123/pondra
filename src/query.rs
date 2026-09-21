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
    let (tail, files) = sources(lake, ctx, name, meta, upto).await?;
    Ok(match tail.into_iter().chain(files).reduce(|a, b| a.union(b).expect("same schema")) {
        Some(df) => df,
        None => empty(ctx, meta)?,
    })
}

/// A table's rows as separate reads: the log tail up to `upto` (if any rows), and the files.
/// Upsert tables get one read per generation of files, newest first, and `_ord` on every row: a
/// newer file's rows above an older one's, and log rows ((segment << 32) + position) above both.
/// Other tables read all their files as one (merge tables combine rows in any order).
pub async fn sources(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, upto: Option<u64>) -> Result<(Option<DataFrame>, Vec<DataFrame>)> {
    let (schema, keyed, upsert) = (schema(&meta.columns)?, !meta.key.is_empty(), !meta.key.is_empty() && meta.merge.is_empty());
    let opts = || ParquetReadOptions::default().schema(&schema);
    let path = |f: &DataFile| lake.full(&f.path);
    let mut files = vec![];
    if upsert {
        let mut by_ord: std::collections::BTreeMap<u64, Vec<String>> = Default::default();
        for f in &meta.files {
            by_ord.entry(f.ord).or_default().push(path(f));
        }
        for (ord, paths) in by_ord.into_iter().rev() {
            files.push(ctx.read_parquet(paths, opts()).await?.with_column("_ord", lit(ord << 32))?);
        }
    } else if !meta.files.is_empty() {
        let df = ctx.read_parquet(meta.files.iter().map(path).collect::<Vec<_>>(), opts()).await?;
        files.push(if keyed { df.with_column("_ord", lit(0u64))? } else { df });
    }
    let hot = tail(lake, name, meta.tiered, upto, keyed).await?;
    if hot.is_empty() {
        return Ok((None, files));
    }
    let mut fields = schema.fields().to_vec(); // the table's own schema, so every batch agrees
    if keyed {
        fields.push(Arc::new(Field::new("_ord", DataType::UInt64, false)));
    }
    let s = Arc::new(Schema::new(fields));
    Ok((Some(ctx.read_batches(hot.into_iter().map(|b| b.with_schema(s.clone())).collect::<Result<Vec<_>, _>>()?)?), files))
}

fn empty(ctx: &SessionContext, meta: &TableMeta) -> Result<DataFrame> {
    let df = ctx.read_table(Arc::new(MemTable::try_new(schema(&meta.columns)?, vec![vec![]])?))?;
    Ok(if meta.key.is_empty() { df } else { df.with_column("_ord", lit(0u64))? })
}

/// An upsert table as its users see it: the newest row of each key, deleted ones left out.
/// Every file holds one row per key, so rather than group all rows by key, each source keeps the
/// rows whose key no newer source has — an anti-join against the newer keys, which are usually
/// few (the log tail and recent files) — and only the log tail is deduplicated itself. A table
/// that is one compacted file reads as that file.
pub async fn register_upsert(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta) -> Result<()> {
    let (tail, files) = sources(lake, ctx, name, meta, None).await?;
    let mut names = vec![];
    if let Some(t) = tail {
        ctx.register_table(format!("__tail_{name}").as_str(), t.into_view())?;
        let newest = latest_sql(meta, &format!("__tail_{name}"), false, true); // (delete markers still shadow)
        ctx.register_table(format!("__s0_{name}").as_str(), ctx.sql(&newest).await?.into_view())?;
        names.push(format!("__s0_{name}"));
    }
    for (i, f) in files.into_iter().enumerate() {
        ctx.register_table(format!("__f{i}_{name}").as_str(), f.into_view())?;
        names.push(format!("__f{i}_{name}"));
    }
    if names.is_empty() {
        ctx.register_table(name, Arc::new(MemTable::try_new(schema(&meta.columns)?, vec![vec![]])?))?;
        return Ok(());
    }
    let q = |c: &String| format!("\"{c}\"");
    let cols = |p: &str| meta.columns.iter().map(|(c, _)| format!("{p}{}", q(c))).collect::<Vec<_>>().join(", ");
    let keys = meta.key.iter().map(q).collect::<Vec<_>>().join(", ");
    let on = meta.key.iter().map(|k| format!("s.{} = n.{}", q(k), q(k))).collect::<Vec<_>>().join(" AND ");
    let parts: Vec<String> = names.iter().enumerate().map(|(i, src)| match i {
        0 => format!("SELECT {} FROM \"{src}\"", cols("")),
        _ => {
            let newer = names[..i].iter().map(|n| format!("SELECT {keys} FROM \"{n}\"")).collect::<Vec<_>>().join(" UNION ALL ");
            format!("SELECT {} FROM \"{src}\" s LEFT ANTI JOIN ({newer}) n ON {on}", cols("s."))
        }
    }).collect();
    let deleted = meta.columns.iter().any(|(c, _)| c == "_deleted").then_some(" WHERE \"_deleted\" IS NOT TRUE").unwrap_or("");
    let sql = format!("SELECT {} FROM ({}){deleted}", cols(""), parts.join(" UNION ALL "));
    ctx.register_table(name, ctx.sql(&sql).await?.into_view())?;
    Ok(())
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
        if !meta.key.is_empty() && meta.merge.is_empty() {
            register_upsert(lake, &ctx, name, &meta).await?;
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
