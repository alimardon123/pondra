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
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::prelude::*;
use futures::{StreamExt, TryStreamExt};
use std::sync::Arc;

pub fn schema(columns: &[(String, String)]) -> Result<SchemaRef> {
    let fields = columns.iter().map(|(n, t)| Ok(Field::new(n, dtype(t)?, true)));
    Ok(Arc::new(Schema::new(fields.collect::<Result<Vec<_>>>()?)))
}

/// A column's type by name: Arrow's own (`Utf8`, `Int64`, `Timestamp(Microsecond, None)`,
/// `Binary`), `T[]` for a list of T (`Float32[]`: an embedding), and `VARIANT`/`JSON` for
/// semi-structured text, which is stored as a string and read with `json_get(…)`, `->` and `->>`.
pub fn dtype(t: &str) -> Result<DataType> {
    let t = t.trim();
    Ok(match t.strip_suffix("[]") {
        Some(item) => DataType::List(Arc::new(Field::new("item", dtype(item)?, true))),
        None => match t.to_uppercase().as_str() {
            "VARIANT" | "JSON" => DataType::Utf8,
            _ => t.parse::<DataType>().map_err(|e| anyhow::anyhow!("unknown column type {t}: {e}"))?,
        },
    })
}

/// The name a table's columns record a type under (what `dtype` reads back).
pub fn type_name(t: &DataType) -> String {
    match crate::write::stored(t) {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => format!("{}[]", type_name(f.data_type())),
        t => t.to_string(),
    }
}

/// A table's columns as queries read them: strings as views (Utf8View), so scans don't copy them
/// and string filters and joins take their fast paths (TPC-H runs about 8% faster). Tables store
/// plain Utf8 (see `schema`).
pub fn read_schema(columns: &[(String, String)]) -> Result<SchemaRef> {
    let stored = schema(columns)?;
    let fields = stored.fields().iter().map(|f| match f.data_type() {
        DataType::Utf8 => Arc::new(f.as_ref().clone().with_data_type(DataType::Utf8View)),
        _ => f.clone(),
    });
    Ok(Arc::new(Schema::new(fields.collect::<Vec<_>>())))
}

/// `b` with `s`'s columns, by name: older rows (written before an ALTER TABLE … ADD COLUMN) get
/// nulls for the columns added since.
pub fn conform(b: &RecordBatch, s: &SchemaRef) -> Result<RecordBatch> {
    if b.schema().fields() == s.fields() {
        return Ok(b.clone());
    }
    let columns = s.fields().iter().map(|f| match b.column_by_name(f.name()) {
        Some(c) => Ok(datafusion::arrow::compute::cast(c, f.data_type())?),
        None => Ok(datafusion::arrow::array::new_null_array(f.data_type(), b.num_rows())),
    });
    Ok(RecordBatch::try_new(s.clone(), columns.collect::<Result<Vec<_>>>()?)?)
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
    let target = match lake.cat.get::<TableMeta>(&table_key(table)).await? {
        Some(m) => Some(schema(&m.columns)?),
        None => None,
    };
    let mut out = vec![];
    for ((n, _), rows) in segs.iter().zip(fetched) {
        let (n, mut pos) = (*n, 0u64);
        for b in rows.iter() {
            let b = &match &target {
                Some(s) => conform(b, s)?,
                None => b.clone(),
            };
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
    let (schema, keyed, upsert) = (read_schema(&meta.columns)?, !meta.key.is_empty(), !meta.key.is_empty() && meta.merge.is_empty());
    let mut files = vec![];
    if upsert {
        let mut by_ord: std::collections::BTreeMap<u64, Vec<&DataFile>> = Default::default();
        for f in &meta.files {
            by_ord.entry(f.ord).or_default().push(f);
        }
        for (ord, group) in by_ord.into_iter().rev() {
            files.push(read_files(lake, ctx, group, &schema).await?.with_column("_ord", lit(ord << 32))?);
        }
    } else if !meta.files.is_empty() {
        let df = read_files(lake, ctx, meta.files.iter().collect(), &schema).await?;
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
    Ok((Some(ctx.read_batches(hot.iter().map(|b| conform(b, &s)).collect::<Result<Vec<_>>>()?)?), files))
}

/// Parquet files as one read: through the hot columns (`hot.rs`) when they're on.
async fn read_files(lake: &Lake, ctx: &SessionContext, files: Vec<&DataFile>, schema: &SchemaRef) -> Result<DataFrame> {
    if lake.hot.on() {
        let files = files.into_iter().cloned().collect();
        return Ok(ctx.read_table(Arc::new(crate::hot::HotFiles { lake: lake.arc(), files, schema: schema.clone() }))?);
    }
    Ok(ctx.read_parquet(files.iter().map(|f| lake.full(&f.path)).collect::<Vec<_>>(), ParquetReadOptions::default().schema(schema)).await?)
}

fn empty(ctx: &SessionContext, meta: &TableMeta) -> Result<DataFrame> {
    let df = ctx.read_table(Arc::new(MemTable::try_new(read_schema(&meta.columns)?, vec![vec![]])?))?;
    Ok(if meta.key.is_empty() { df } else { df.with_column("_ord", lit(0u64))? })
}

/// An upsert table as its users see it: the newest row of each key, deleted ones left out.
/// Every file holds one row per key, so rather than group all rows by key, each source keeps the
/// rows whose key no newer source has — an anti-join against the newer keys, which are usually
/// few (the log tail and recent files) — and only the log tail is deduplicated itself. A table
/// that is one compacted file reads as that file.
async fn upsert_view(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, upto: Option<u64>) -> Result<Arc<dyn TableProvider>> {
    let (tail, files) = sources(lake, ctx, name, meta, upto).await?;
    let (mut names, aux) = (vec![], lake.session()); // (the parts live in their own context: SHOW TABLES lists only tables)
    if let Some(t) = tail {
        aux.register_table("__tail", t.into_view())?;
        let newest = latest_sql(meta, "__tail", false, true); // (delete markers still shadow)
        aux.register_table("__s0", aux.sql(&newest).await?.into_view())?;
        names.push("__s0".to_string());
    }
    for (i, f) in files.into_iter().enumerate() {
        aux.register_table(format!("__f{i}").as_str(), f.into_view())?;
        names.push(format!("__f{i}"));
    }
    if names.is_empty() {
        return Ok(Arc::new(MemTable::try_new(read_schema(&meta.columns)?, vec![vec![]])?));
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
    let sql = format!("SELECT {} FROM ({}){}", cols(""), parts.join(" UNION ALL "), live(meta));
    Ok(aux.sql(&sql).await?.into_view())
}

/// A table as its users see it: append tables as files + log; keyed tables their current rows.
/// `upto`: the log only that far (a distributed query reads every node's copy at one snapshot).
pub async fn table_view(lake: &Lake, ctx: &SessionContext, name: &str, meta: &TableMeta, upto: Option<u64>) -> Result<Arc<dyn TableProvider>> {
    if !meta.key.is_empty() && meta.merge.is_empty() {
        return upsert_view(lake, ctx, name, meta, upto).await;
    }
    if meta.key.is_empty() {
        let schema = read_schema(&meta.columns)?;
        let ranges = crate::manifest::ranges(name, &crate::manifest::list(lake, meta).await?, &meta.files, &schema);
        return Ok(Arc::new(Pruned { lake: lake.arc(), name: name.into(), meta: meta.clone(), manifests: None, upto, schema, share: None, ranges, range: None }));
    }
    let df = raw(lake, ctx, name, meta, upto).await?;
    let aux = lake.session();
    aux.register_table("__raw", df.into_view())?;
    Ok(aux.sql(&current_sql(meta, "__raw", upto.unwrap_or(lake.visible()))).await?.into_view())
}

/// An append table (or a distributed query's slice of one) that picks its files per query: those
/// whose min/max can match the query's filters, from its inline files and, through the manifest
/// list, its sealed ones (`manifest.rs`). Plus the log tail, which always counts.
pub struct Pruned {
    pub lake: Arc<Lake>,
    pub name: String,
    pub meta: TableMeta,
    pub manifests: Option<Vec<crate::manifest::Manifest>>, // a slice's share; None: the table's list
    pub upto: Option<u64>,                                  // the log up to this segment (None: the latest)
    pub schema: SchemaRef,
    pub share: Option<(u64, u64)>, // a distributed query's slice of it, and the whole table's (rows, bytes)
    pub ranges: Arc<crate::manifest::Stats>, // every column's min and max over the whole table
    pub range: Option<crate::ranges::Range>, // a distributed query's slice by a key's range: only its rows
}

impl std::fmt::Debug for Pruned {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Pruned({})", self.name) }
}

#[async_trait::async_trait]
impl TableProvider for Pruned {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> datafusion::datasource::TableType { datafusion::datasource::TableType::Base }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        Ok(vec![datafusion::logical_expr::TableProviderFilterPushDown::Inexact; filters.len()]) // (they choose files; rows are filtered above)
    }

    /// How big the table is, from what the catalog already knows: its files' row counts and bytes,
    /// plus the sealed manifests' totals (`manifest.rs`), without opening a single footer. A
    /// distributed query's slice reports the whole table (`share`), so every node orders its joins
    /// alike. The log tail isn't counted — it is bounded by a tiering round, and this is for
    /// choosing a join order (`optimize::JoinOrder`), not for counting rows.
    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        use datafusion::common::stats::Precision;
        let (rows, bytes) = self.share.unwrap_or_else(|| {
            let sealed = self.meta.sealed.clone().unwrap_or_default();
            let (rows, bytes) = self.meta.files.iter().fold((0, 0), |(r, b), f| (r + f.rows, b + f.bytes));
            (sealed.rows + rows, sealed.bytes + bytes)
        });
        let mut stats = datafusion::common::Statistics::new_unknown(&self.schema);
        (stats.num_rows, stats.total_byte_size) = (Precision::Inexact(rows as usize), Precision::Inexact(bytes as usize));
        for (i, f) in self.schema.fields().iter().enumerate() {
            let parse = |v: &String| datafusion::common::ScalarValue::try_from_string(v.clone(), f.data_type()).ok();
            let range = self.ranges.get(f.name()).and_then(|(lo, hi)| Some((parse(lo)?, parse(hi)?)));
            // Distinct values: what the table's sketch saw (`sketch.rs`), no more than its range
            // could hold, nor than its rows.
            let sketched = self.meta.sketch.get(f.name()).and_then(|s| crate::sketch::estimate(s));
            let spanned = range.as_ref().and_then(|(lo, hi)| crate::manifest::span(lo, hi));
            if let Some(n) = [sketched, spanned].into_iter().flatten().min() {
                stats.column_statistics[i].distinct_count = Precision::Inexact((n as usize).min(rows as usize).max(1));
            }
            if let Some((lo, hi)) = range {
                (stats.column_statistics[i].min_value, stats.column_statistics[i].max_value) = (Precision::Inexact(lo), Precision::Inexact(hi));
            }
        }
        Some(stats)
    }

    async fn scan(&self, _: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, filters: &[Expr], _: Option<usize>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let e = |e: anyhow::Error| datafusion::error::DataFusionError::External(e.into());
        let range = match &self.range {
            Some(r) => Some(r.expr(self.schema.field_with_name(&r.column)?.data_type()).map_err(e)?),
            None => None,
        };
        let filters: Vec<Expr> = filters.iter().cloned().chain(range.clone()).collect();
        let files = crate::manifest::pruned(&self.lake, &self.meta, self.manifests.as_deref(), &filters, &self.schema).await.map_err(e)?;
        let meta = TableMeta { files, sealed: None, ..self.meta.clone() };
        let ctx = self.lake.session();
        let df = raw(&self.lake, &ctx, &self.name, &meta, self.upto).await.map_err(e)?;
        let df = match range {
            Some(r) => df.filter(r)?, // (reaches the Parquet reader: row groups outside it are skipped)
            None => df,
        };
        let df = match projection {
            Some(p) => df.select_columns(&p.iter().map(|&i| self.schema.field(i).name().as_str()).collect::<Vec<_>>())?,
            None => df,
        };
        let plan = df.create_physical_plan().await?;
        let Some((rows, bytes)) = self.share else { return Ok(plan) };
        Ok(Arc::new(crate::spmd::ShareExec::new(plan, &self.name, rows, bytes, self.range.as_ref().map(|r| r.column.clone()))?))
    }
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
        let live = if keep_deleted { String::new() } else { live(meta) }; // (windows past their TTL drop out)
        return format!("SELECT * FROM (SELECT {} FROM \"{raw_table}\" GROUP BY {key}){live}{order}", cols.collect::<Vec<_>>().join(", "));
    }
    // Newest version per key as a grouped aggregate (a hash table), not a window (a sort).
    let newest = |c: &String| format!("first_value({} ORDER BY \"_ord\" DESC) AS {}", q(c), q(c));
    let cols = meta.columns.iter().map(|(c, _)| if meta.key.contains(c) { q(c) } else { newest(c) });
    let out = meta.columns.iter().map(|(c, _)| q(c)).collect::<Vec<_>>().join(", ");
    let deleted = if keep_deleted { String::new() } else { live(meta) }; // (a partial merge keeps markers and expired rows)
    format!("SELECT {out} FROM (SELECT {} FROM \"{raw_table}\" GROUP BY {key}){deleted}{order}",
            cols.collect::<Vec<_>>().join(", "))
}

/// The current rows of a keyed table, over `raw_table`. When the source already holds one row
/// per key — a single file, with nothing in the log after it — the "newest wins" window (or the
/// merge GROUP BY) is skipped, so reads of a compacted table cost a plain scan.
pub fn current_sql(meta: &TableMeta, raw_table: &str, upto: u64) -> String {
    if meta.files.len() > 1 || upto != meta.tiered {
        return latest_sql(meta, raw_table, false, false);
    }
    let cols = meta.columns.iter().map(|(c, _)| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
    format!("SELECT {cols} FROM \"{raw_table}\"{}", live(meta))
}

/// ` WHERE …` keeping a keyed table's live rows: not deleted, not past their TTL ("" if nothing to drop).
pub fn live(meta: &TableMeta) -> String {
    let deleted = meta.columns.iter().any(|(c, _)| c == "_deleted").then(|| "\"_deleted\" IS NOT TRUE".to_string());
    let conds: Vec<String> = deleted.into_iter().chain(Some(meta.ttl_sql()).filter(|t| !t.is_empty())).collect();
    if conds.is_empty() { String::new() } else { format!(" WHERE {}", conds.join(" AND ")) }
}

/// What SQL from users may do on a node: queries only. No `COPY … TO` files, no `CREATE EXTERNAL
/// TABLE` over the node's disk, no session DDL; writes go through `write.rs`, `SET` through pg.rs.
pub fn read_only() -> datafusion::execution::context::SQLOptions {
    datafusion::execution::context::SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false)
}

/// A session with every table referenced in `sql` registered (cheap name filter), except
/// `except`, which the caller registers itself. Attached lakes' tables are `name.table`.
pub async fn session(lake: &Lake, sql: &str, except: &str) -> Result<SessionContext> {
    let ctx = lake.session();
    crate::udf::register(lake, &ctx).await?; // the lake's own functions (`POST /functions/…`)
    let listing = ["information_schema", "show tables", "show columns"].iter().any(|w| sql.to_lowercase().contains(w)); // (every table)
    let attached = lake.attached.read().unwrap().clone();
    for (ns, other) in [(String::new(), None)].into_iter().chain(attached.into_iter().map(|(n, l)| (n, Some(l)))) {
        let from = other.as_deref().unwrap_or(lake);
        if !ns.is_empty() {
            if !listing && !sql.contains(&format!("{ns}.")) {
                continue;
            }
            let schemas = ctx.catalog("datafusion").expect("the default catalog");
            schemas.register_schema(&ns, Arc::new(datafusion::catalog::MemorySchemaProvider::new()))?;
        }
        for (key, meta) in from.cat.scan::<TableMeta>("t/", "t0").await? {
            let name = &key[2..];
            let full = if ns.is_empty() { name.to_string() } else { format!("{ns}.{name}") };
            if (!listing && !sql.contains(full.as_str())) || (ns.is_empty() && name == except) {
                continue;
            }
            ctx.register_table(full.as_str(), table_view(from, &ctx, name, &meta, None).await?)?;
        }
    }
    Ok(ctx)
}

/// Run `sql` with table `source` standing for just `rows` (new rows of a streaming source); every
/// other table it mentions is read from the lake as usual.
pub async fn over(lake: &Lake, source: &str, rows: Vec<RecordBatch>, sql: &str) -> Result<RecordBatch> {
    let meta: TableMeta = lake.cat.get(&table_key(source)).await?.ok_or_else(|| anyhow::anyhow!("no table {source}"))?;
    let ctx = session(lake, sql, source).await?;
    let s = schema(&meta.columns)?;
    let rows = rows.iter().map(|b| conform(b, &s)).collect::<Result<Vec<_>>>()?;
    ctx.register_table(source, Arc::new(MemTable::try_new(s, vec![rows])?))?;
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
