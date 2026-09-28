//! Other engines' tables, read natively (ADR-026): a Delta or Iceberg table's data files as one
//! scan by DataFusion's Parquet reader, each file with the rows its deletes leave (Delta's
//! deletion vectors, Iceberg's position deletes and deletion vectors) as a row selection, so the
//! reader never decodes a deleted row; filters reach row groups and pages as for any Parquet.
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// What reading one of another engine's files takes besides the file itself.
#[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug)]
pub struct Outside {
    /// Its partition values (Delta's): column (by the name in the files) -> value as text.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<(String, Option<String>)>,
    /// The rows deleted from it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletes: Vec<Delete>,
    /// Its rows, deleted ones too (a row selection covers them all), if its table says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// Its data sequence number (Iceberg's: equality deletes apply to files older than them).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<i64>,
}

/// How another engine's table matches its files' columns to its own (Iceberg: by field id).
#[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug)]
pub struct Table {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub field_ids: std::collections::BTreeMap<String, i64>, // column -> its field id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_mapping: Option<String>, // files written without ids: which names are which ids (JSON)
}

/// Rows deleted from a file, by position or (Iceberg's equality deletes) by value.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum Delete {
    /// A deletion vector in a file: Delta's (at `offset`: its size, the bitmap, a checksum).
    Vector { path: String, offset: u64, size: u64 },
    /// A deletion vector in the log itself (Delta's, Z85).
    Inline { z85: String, size: u64 },
    /// Iceberg's deletion vector: a Puffin file's blob (at `offset`, `size` bytes).
    Blob { path: String, offset: u64, size: u64 },
    /// An Iceberg position-delete file: its rows naming this file (`file`, as they name it) are
    /// the positions.
    Positions { path: String, file: String },
    /// An Iceberg equality-delete file (data sequence number `seq`): a row equal to one of its
    /// rows in these columns is deleted (NULL equal to NULL).
    Equality { path: String, columns: Vec<String>, seq: i64 },
}

/// Another engine's files (`files`, of one table) read as `schema` says: its columns in its
/// order, matched by field id where `table` gives them, partition values from each file's place,
/// deleted rows left out.
pub async fn read(ctx: &datafusion::prelude::SessionContext, files: &[&crate::store::DataFile], schema: &SchemaRef, table: Option<&Table>) -> Result<datafusion::prelude::DataFrame> {
    use datafusion::prelude::{cast, ident};
    let first = files.first().context("no files")?;
    let url = url::Url::parse(&first.path).or_else(|_| url::Url::from_file_path(&first.path).map_err(|_| anyhow::anyhow!("{}: not a path", first.path)))?;
    let store = ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?;
    let partition: Vec<String> = first.outside.as_ref().map(|o| o.values.iter().map(|(k, _)| k.clone()).collect()).unwrap_or_default();
    let field = |n: &str| schema.field_with_name(n).cloned().map(Arc::new).with_context(|| format!("no column {n} ({})", names(schema)));
    let ids = table.map(|t| t.field_ids.clone()).unwrap_or_default();
    let with_id = |f: &FieldRef| match ids.get(f.name()) {
        Some(id) => Arc::new(f.as_ref().clone().with_metadata([("PARQUET:field_id".to_string(), id.to_string())].into())),
        None => f.clone(),
    };
    let file_schema = Arc::new(Schema::new(schema.fields().iter().filter(|f| !partition.contains(f.name())).map(with_id).collect::<Vec<_>>()));
    let mut partition_fields = partition.iter().map(|p| field(p)).collect::<Result<Vec<_>>>()?;
    let equality = files.iter().any(|f| f.outside.iter().flat_map(|o| o.deletes.iter()).any(|d| matches!(d, Delete::Equality { .. })));
    if equality {
        partition_fields.push(crate::scan::field(SEQ, datafusion::arrow::datatypes::DataType::Int64)); // (each file's data sequence number)
    }
    let positions = positions(ctx, files).await?;
    let mut loaded = vec![];
    for f in files {
        loaded.push(deleted(ctx, f, &positions)); // (a loop, not a closure: Send across every lifetime)
    }
    let gone: Vec<Option<Arc<Vec<u64>>>> = futures::future::try_join_all(loaded).await?;
    let mut out = vec![];
    for (f, gone) in files.iter().zip(gone) {
        let key = key_of(&f.path, &url)?;
        let values = f.outside.as_ref().map(|o| o.values.clone()).unwrap_or_default();
        let mut partition = values.iter().zip(&partition_fields).map(|((_, v), p)| Ok(ScalarValue::Utf8(v.clone()).cast_to(p.data_type())?)).collect::<Result<Vec<_>>>()?;
        if equality {
            partition.push(ScalarValue::Int64(Some(f.outside.as_ref().and_then(|o| o.seq).unwrap_or(0))));
        }
        let rows = match (&gone, f.outside.as_ref().and_then(|o| o.rows)) {
            (Some(_), None) => footer_rows(ctx, &store, &key, f.bytes).await?,
            (_, rows) => rows.unwrap_or(f.rows),
        };
        let stats = Arc::new(statistics(f, rows, gone.as_ref().map_or(0, |g| g.len()), table.is_none(), &file_schema));
        out.push(File { key, size: f.bytes, rows, partition, deleted: gone, stats });
    }
    let adapter = (!ids.is_empty()).then(|| Arc::new(ById { ids: ids.clone(), mapping: table.and_then(|t| t.name_mapping.as_deref()).map(name_mapping).unwrap_or_default() }) as _);
    let df = ctx.read_table(Arc::new(Files { store, file_schema, partition: partition_fields, files: out, adapter }))?;
    let df = if equality { without_equal(ctx, df, files).await? } else { df };
    Ok(df.select(schema.fields().iter().map(|f| cast(ident(f.name()), f.data_type().clone()).alias(f.name())).collect::<Vec<_>>())?)
}

const SEQ: &str = "__pondra_seq";

/// What the planner knows of a file before reading it (the join order rests on it): its rows
/// and bytes and, for plain Parquet files, its columns' ranges from their footers, as the
/// statement listed them (bounds for pruning, never read as answers). Another engine's counts
/// come from its log: estimates.
fn statistics(f: &crate::store::DataFile, rows: u64, deleted: usize, plain: bool, schema: &Schema) -> datafusion::common::Statistics {
    use datafusion::common::stats::Precision::{Exact, Inexact};
    let mut s = datafusion::common::Statistics::new_unknown(schema);
    let n = rows.saturating_sub(deleted as u64) as usize;
    (s.num_rows, s.total_byte_size) = (if plain { Exact(n) } else { Inexact(n) }, Inexact(f.bytes as usize));
    for (field, c) in schema.fields().iter().zip(s.column_statistics.iter_mut()).filter(|_| plain) {
        let parse = |v: &String| ScalarValue::try_from_string(v.clone(), field.data_type()).ok();
        if let Some((Some(lo), Some(hi))) = f.stats.get(field.name()).map(|(lo, hi)| (parse(lo), parse(hi))) {
            (c.min_value, c.max_value) = (Inexact(lo), Inexact(hi));
        }
    }
    s
}

/// Rows left by Iceberg's equality deletes: a delete file's row removes the equal rows (NULL
/// equal to NULL) of the files older than it (a smaller data sequence number).
async fn without_equal(ctx: &datafusion::prelude::SessionContext, mut df: datafusion::prelude::DataFrame, files: &[&crate::store::DataFile]) -> Result<datafusion::prelude::DataFrame> {
    use datafusion::prelude::{col, lit};
    let mut by_columns: std::collections::BTreeMap<Vec<String>, Vec<(String, i64)>> = Default::default();
    for d in files.iter().flat_map(|f| f.outside.iter().flat_map(|o| o.deletes.iter())) {
        if let Delete::Equality { path, columns, seq } = d {
            let list = by_columns.entry(columns.clone()).or_default();
            if !list.iter().any(|(p, _)| p == path) {
                list.push((path.clone(), *seq));
            }
        }
    }
    for (i, (columns, deletes)) in by_columns.into_iter().enumerate() {
        let mut gone: Option<datafusion::prelude::DataFrame> = None;
        for (path, seq) in deletes {
            let rows = ctx.read_parquet(path, Default::default()).await?.select(columns.iter().map(|c| col(format!("\"{c}\""))).chain([lit(seq).alias("__pondra_dseq")]).collect::<Vec<_>>())?;
            gone = Some(match gone { Some(g) => g.union(rows)?, None => rows });
        }
        let (data, dels) = (format!("__pondra_d{i}"), format!("__pondra_e{i}"));
        let aux = datafusion::prelude::SessionContext::new_with_state(ctx.state());
        aux.register_table(data.as_str(), df.into_view())?;
        aux.register_table(dels.as_str(), gone.context("no deletes")?.into_view())?;
        let q = |c: &String| format!("\"{}\"", c.replace('"', "\"\""));
        let on = columns.iter().map(|c| format!("(e.{0} IS NOT DISTINCT FROM d.{0})", q(c))).collect::<Vec<_>>().join(" AND ");
        df = aux.sql(&format!("SELECT d.* FROM \"{data}\" d WHERE NOT EXISTS (SELECT 1 FROM \"{dels}\" e WHERE {on} AND d.{SEQ} < e.__pondra_dseq)")).await?;
    }
    Ok(df)
}

/// Columns matched by field id (Iceberg's rule: a renamed column is still itself; a column
/// dropped and added again under its old name is not): each file's columns are named as the table
/// names them before DataFusion's own matching by name. Files written without ids go by the
/// table's name mapping, or by name.
#[derive(Debug)]
struct ById {
    ids: std::collections::BTreeMap<String, i64>, // table column -> id
    mapping: std::collections::HashMap<String, i64>, // a name files may use -> id
}

impl datafusion::physical_expr_adapter::PhysicalExprAdapterFactory for ById {
    fn create(&self, logical: SchemaRef, physical: SchemaRef) -> datafusion::error::Result<Arc<dyn datafusion::physical_expr_adapter::PhysicalExprAdapter>> {
        use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
        let id_of = |f: &Field| f.metadata().get("PARQUET:field_id").and_then(|i| i.parse::<i64>().ok()).or_else(|| self.mapping.get(f.name()).copied());
        let in_file: std::collections::HashMap<i64, &str> = physical.fields().iter().filter_map(|f| Some((id_of(f)?, f.name().as_str()))).collect();
        if in_file.is_empty() {
            return DefaultPhysicalExprAdapterFactory.create(logical, physical); // (no ids at all: by name)
        }
        // The table's columns under the names this file gives their ids; one it lacks, under a
        // name the file can't have (read as NULL).
        let mut renames = std::collections::HashMap::new();
        let fields: Vec<Field> = logical.fields().iter().map(|f| {
            let name = match self.ids.get(f.name()).and_then(|id| in_file.get(id)) {
                Some(n) => n.to_string(),
                None if physical.field_with_name(f.name()).is_ok() => format!("\u{1}missing:{}", f.name()),
                None => f.name().clone(),
            };
            if &name != f.name() {
                renames.insert(f.name().clone(), name.clone());
            }
            f.as_ref().clone().with_name(name)
        }).collect();
        let inner = DefaultPhysicalExprAdapterFactory.create(Arc::new(Schema::new(fields)), physical)?;
        Ok(match renames.is_empty() {
            true => inner,
            false => Arc::new(Renamed { renames, inner }),
        })
    }
}

#[derive(Debug)]
struct Renamed {
    renames: std::collections::HashMap<String, String>,
    inner: Arc<dyn datafusion::physical_expr_adapter::PhysicalExprAdapter>,
}

impl datafusion::physical_expr_adapter::PhysicalExprAdapter for Renamed {
    fn rewrite(&self, expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::physical_expr::expressions::Column;
        let renamed = expr.transform(|e| Ok(match e.downcast_ref::<Column>().and_then(|c| Some((self.renames.get(c.name())?, c.index()))) {
            Some((to, at)) => Transformed::yes(Arc::new(Column::new(to, at)) as _),
            None => Transformed::no(e),
        }))?.data;
        self.inner.rewrite(renamed)
    }
}

/// Iceberg's name mapping (`schema.name-mapping.default`): each name a field went by -> its id.
fn name_mapping(json: &str) -> std::collections::HashMap<String, i64> {
    let v: serde_json::Value = serde_json::from_str(json).unwrap_or_default();
    v.as_array().into_iter().flatten().flat_map(|m| {
        let id = m["field-id"].as_i64();
        m["names"].as_array().into_iter().flatten().filter_map(move |n| Some((n.as_str()?.to_string(), id?)))
    }).collect()
}

/// A file's object path in its store (as the log names it: escapes kept).
fn key_of(path: &str, root: &url::Url) -> Result<String> {
    match path.contains("://") {
        true => Ok(path[root[..url::Position::BeforePath].len()..].trim_start_matches('/').to_string()),
        false => Ok(path.trim_start_matches('/').to_string()), // (this machine's: the whole path)
    }
}

/// A file's row count from its footer (for a file its table doesn't count).
async fn footer_rows(ctx: &datafusion::prelude::SessionContext, store: &ObjectStoreUrl, key: &str, size: u64) -> Result<u64> {
    let s = ctx.runtime_env().object_store(store)?;
    let meta = object_store_df::ObjectMeta { location: object_store_df::path::Path::parse(key)?, last_modified: Default::default(), size, e_tag: None, version: None };
    let footer = datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata::new(s.as_ref(), &meta).fetch_metadata().await?;
    Ok(footer.file_metadata().num_rows() as u64)
}

/// The rows of the position-delete files among `files`' deletes: data file -> positions.
async fn positions(ctx: &datafusion::prelude::SessionContext, files: &[&crate::store::DataFile]) -> Result<std::collections::HashMap<String, Vec<u64>>> {
    use datafusion::arrow::array::{AsArray, Array};
    let mut wanted: Vec<String> = files.iter().flat_map(|f| f.outside.iter().flat_map(|o| o.deletes.iter())).filter_map(|d| match d {
        Delete::Positions { path, .. } => Some(path.clone()),
        _ => None,
    }).collect();
    wanted.sort();
    wanted.dedup();
    let mut out: std::collections::HashMap<String, Vec<u64>> = Default::default();
    if wanted.is_empty() {
        return Ok(out);
    }
    let ours: std::collections::HashSet<&str> = files.iter().flat_map(|f| f.outside.iter().flat_map(|o| o.deletes.iter())).filter_map(|d| match d {
        Delete::Positions { file, .. } => Some(file.as_str()),
        _ => None,
    }).collect();
    let df = ctx.read_parquet(wanted, Default::default()).await?.select_columns(&["file_path", "pos"])?;
    for b in df.collect().await? {
        let paths = datafusion::arrow::compute::cast(b.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
        let (paths, pos) = (paths.as_string::<i32>(), datafusion::arrow::compute::cast(b.column(1), &datafusion::arrow::datatypes::DataType::UInt64)?);
        let pos = pos.as_primitive::<datafusion::arrow::datatypes::UInt64Type>();
        for i in (0..b.num_rows()).filter(|i| !paths.is_null(*i) && ours.contains(paths.value(*i))) {
            out.entry(paths.value(i).to_string()).or_default().push(pos.value(i));
        }
    }
    Ok(out)
}

/// A file's deleted rows (positions, sorted), from its deletes.
async fn deleted(ctx: &datafusion::prelude::SessionContext, f: &crate::store::DataFile, positions: &std::collections::HashMap<String, Vec<u64>>) -> Result<Option<Arc<Vec<u64>>>> {
    let Some(o) = f.outside.as_ref().filter(|o| o.deletes.iter().any(|d| !matches!(d, Delete::Equality { .. }))) else { return Ok(None) };
    let mut out = vec![];
    for d in &o.deletes {
        match d {
            Delete::Inline { z85: text, size } => out.extend(roaring_array(z85(text)?.get(..*size as usize).context("an inline deletion vector cut short")?)?),
            Delete::Vector { path, offset, size } => {
                let b = range(ctx, path, *offset, 4 + *size).await?;
                out.extend(roaring_array(&b[4..])?); // (after its size; its checksum after it)
            }
            Delete::Blob { path, offset, size } => {
                let b = range(ctx, path, *offset, *size).await?;
                ensure!(b.len() >= 12 && b[4..8] == [0xD1, 0xD3, 0x39, 0x64], "{path}: not a deletion-vector-v1 blob");
                out.extend(portable64(&b[8..b.len() - 4])?);
            }
            Delete::Positions { file, .. } => out.extend(positions.get(file).into_iter().flatten().copied()),
            Delete::Equality { .. } => {} // (by value: `without_equal`)
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(Some(Arc::new(out)))
}

/// `size` bytes of an object from `offset`.
async fn range(ctx: &datafusion::prelude::SessionContext, path: &str, offset: u64, size: u64) -> Result<bytes::Bytes> {
    let url = datafusion::datasource::listing::ListingTableUrl::parse(path)?;
    let store = ctx.runtime_env().object_store(&url)?;
    Ok(object_store_df::ObjectStoreExt::get_range(&store, &object_store_df::path::Path::parse(url.prefix().as_ref())?, offset..offset + size).await?)
}

/// A table's data files, read as one.
#[derive(Debug)]
pub struct Files {
    pub store: ObjectStoreUrl,  // where they are (`s3://bucket`)
    pub file_schema: SchemaRef, // the columns in the files, by the names written there
    pub partition: Vec<FieldRef>, // columns a file's place gives (Delta's partition values), after them
    pub files: Vec<File>,
    pub adapter: Option<Arc<dyn datafusion::physical_expr_adapter::PhysicalExprAdapterFactory>>, // (columns matched by field id)
}

#[derive(Debug, Clone)]
pub struct File {
    pub key: String, // its object's path in the store, as listed (percent-encoded where it is)
    pub size: u64,
    pub rows: u64,
    pub partition: Vec<ScalarValue>,
    pub deleted: Option<Arc<Vec<u64>>>, // the positions of its deleted rows, sorted
    pub stats: Arc<datafusion::common::Statistics>, // (its file columns': partition values add theirs)
}

#[async_trait::async_trait]
impl TableProvider for Files {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(self.file_schema.fields().iter().cloned().chain(self.partition.iter().cloned()).collect::<Vec<_>>()))
    }

    fn table_type(&self) -> TableType { TableType::Base }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()]) // (row groups and pages skipped by them; the filter stays above)
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, _: &[Expr], limit: Option<usize>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let format = ParquetFormat::default().with_options(state.table_options().parquet.clone());
        let table = datafusion::datasource::table_schema::TableSchema::builder(self.file_schema.clone()).with_table_partition_cols(self.partition.clone()).build();
        let source = format.file_source(table);
        // Files dealt to the partitions by size, each partition a run of them.
        let parts = state.config().target_partitions().max(1);
        let mut groups: Vec<(u64, Vec<datafusion::datasource::listing::PartitionedFile>)> = vec![(0, vec![]); parts];
        let mut files = self.files.clone();
        files.sort_by(|a, b| b.size.cmp(&a.size).then(a.key.cmp(&b.key)));
        for f in files {
            let mut pf = datafusion::datasource::listing::PartitionedFile::new_from_meta(object_store_df::ObjectMeta {
                location: object_store_df::path::Path::parse(&f.key).map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
                last_modified: Default::default(),
                size: f.size,
                e_tag: None,
                version: None,
            })
            .with_partition_values(f.partition.clone());
            if f.partition.iter().all(|v| !v.is_null()) {
                pf = pf.with_statistics(f.stats.clone()); // (a NULL folder's value would be taken for one without NULLs)
            }
            if let Some(gone) = &f.deleted {
                pf = pf.with_extension(datafusion::datasource::physical_plan::parquet::ParquetRowSelection::new(kept(gone, f.rows)));
            }
            let least = groups.iter_mut().min_by_key(|g| g.0).expect("a partition");
            least.0 += f.size;
            least.1.push(pf);
        }
        let schema = self.schema();
        let known: Vec<_> = groups.iter().flat_map(|g| g.1.iter()).filter_map(|f| f.statistics.clone()).collect();
        let stats = match known.len() == self.files.len() {
            true => datafusion::common::Statistics::try_merge_iter(known.iter().map(|s| s.as_ref()), &schema)?,
            false => datafusion::common::Statistics::new_unknown(&schema),
        };
        let groups = groups.into_iter().filter(|g| !g.1.is_empty()).map(|g| FileGroup::new(g.1)).collect();
        let config = FileScanConfigBuilder::new(self.store.clone(), source).with_file_groups(groups).with_statistics(stats).with_projection_indices(projection.cloned())?.with_limit(limit).with_expr_adapter(self.adapter.clone()).build();
        format.create_physical_plan(state, config).await
    }
}

/// The rows a file keeps, as a row selection: all of its `rows` but the positions `gone`.
fn kept(gone: &[u64], rows: u64) -> datafusion::parquet::arrow::arrow_reader::RowSelection {
    use datafusion::parquet::arrow::arrow_reader::RowSelector;
    let (mut out, mut at) = (vec![], 0u64);
    for &g in gone.iter().filter(|g| **g < rows) {
        if g > at {
            out.push(RowSelector::select((g - at) as usize));
        }
        if g >= at {
            out.push(RowSelector::skip(1));
            at = g + 1;
        }
    }
    if rows > at {
        out.push(RowSelector::select((rows - at) as usize));
    }
    out.into()
}

// ---------------------------------------------------------------- deletion vectors

/// A deletion vector's positions: RoaringBitmapArray in Delta's "portable" form (Iceberg's
/// `deletion-vector-v1` blob is the same bitmap): a magic number, the count of 32-bit bitmaps,
/// then each as its high 32 bits and a standard roaring bitmap.
pub fn roaring_array(b: &[u8]) -> Result<Vec<u64>> {
    ensure!(b.len() >= 12, "a deletion vector of {} bytes", b.len());
    ensure!(u32::from_le_bytes(b[0..4].try_into()?) == 1681511377, "not a deletion vector (its magic number)");
    let (n, mut at, mut out) = (u64::from_le_bytes(b[4..12].try_into()?), 12, vec![]);
    for _ in 0..n {
        let high = u32::from_le_bytes(b.get(at..at + 4).context("a deletion vector cut short")?.try_into()?) as u64;
        let (low, used) = roaring(&b[at + 4..])?;
        out.extend(low.into_iter().map(|l| (high << 32) | l as u64));
        at += 4 + used;
    }
    out.sort_unstable();
    Ok(out)
}

/// A standard 32-bit roaring bitmap (the portable serialization): its values, and its length.
pub fn roaring(b: &[u8]) -> Result<(Vec<u32>, usize)> {
    let u16s = |at: usize| -> Result<u16> { Ok(u16::from_le_bytes(b.get(at..at + 2).context("a bitmap cut short")?.try_into()?)) };
    let u32s = |at: usize| -> Result<u32> { Ok(u32::from_le_bytes(b.get(at..at + 4).context("a bitmap cut short")?.try_into()?)) };
    let cookie = u32s(0)?;
    let (count, runs, mut at) = match cookie & 0xFFFF {
        12347 => {
            let count = (cookie >> 16) as usize + 1;
            let flags = b.get(4..4 + count.div_ceil(8)).context("a bitmap cut short")?.to_vec();
            (count, Some(flags), 4 + count.div_ceil(8))
        }
        _ if cookie == 12346 => (u32s(4)? as usize, None, 8),
        _ => bail!("not a roaring bitmap (cookie {cookie})"),
    };
    let is_run = |i: usize| runs.as_ref().is_some_and(|f| f[i / 8] & (1 << (i % 8)) != 0);
    let headers: Vec<(u32, usize)> = (0..count).map(|i| Ok((u16s(at + 4 * i)? as u32, u16s(at + 4 * i + 2)? as usize + 1))).collect::<Result<_>>()?;
    at += 4 * count;
    if runs.is_none() || count >= 4 {
        at += 4 * count; // (offsets to each container: not needed reading them in order)
    }
    let mut out = vec![];
    for (i, (key, card)) in headers.into_iter().enumerate() {
        let base = key << 16;
        if is_run(i) {
            let n = u16s(at)? as usize;
            for r in 0..n {
                let (start, len) = (u16s(at + 2 + 4 * r)? as u32, u16s(at + 4 + 4 * r)? as u32);
                out.extend((start..=start + len).map(|v| base | v));
            }
            at += 2 + 4 * n;
        } else if card > 4096 {
            for w in 0..1024 {
                let word = u64::from_le_bytes(b.get(at + 8 * w..at + 8 * w + 8).context("a bitmap cut short")?.try_into()?);
                (0..64).filter(|bit| word & (1 << bit) != 0).for_each(|bit| out.push(base | (w as u32 * 64 + bit)));
            }
            at += 8192;
        } else {
            for j in 0..card {
                out.push(base | u16s(at + 2 * j)? as u32);
            }
            at += 2 * card;
        }
    }
    Ok((out, at))
}

/// A 64-bit roaring bitmap in the portable form without Delta's magic number (Iceberg's
/// deletion vectors): the count of 32-bit bitmaps, then each as its high bits and a bitmap.
pub fn portable64(b: &[u8]) -> Result<Vec<u64>> {
    let mut with = 1681511377u32.to_le_bytes().to_vec();
    with.extend_from_slice(b);
    roaring_array(&with)
}

/// Z85, the base-85 of Delta's inline deletion vectors and of the UUIDs naming their files.
pub fn z85(s: &str) -> Result<Vec<u8>> {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    let value = |c: u8| ALPHABET.iter().position(|a| *a == c).with_context(|| format!("{:?} isn't Z85", c as char));
    ensure!(s.len() % 5 == 0, "Z85 comes in fives, not {} characters", s.len());
    let mut out = Vec::with_capacity(s.len() / 5 * 4);
    for chunk in s.as_bytes().chunks(5) {
        let mut n = 0u64;
        for &c in chunk {
            n = n * 85 + value(c)? as u64;
        }
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
    Ok(out)
}

/// The fields of a schema, by name (for errors: what a table has).
pub fn names(s: &Schema) -> String { s.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ") }

/// A field, nullable (a table read from files may hold a NULL anywhere).
pub fn field(name: &str, t: datafusion::arrow::datatypes::DataType) -> FieldRef { Arc::new(Field::new(name, t, true)) }
