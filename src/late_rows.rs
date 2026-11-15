//! A top-N of a wide Parquet scan reads its sort keys first, and its other columns only for the rows
//! it keeps, as DuckDB does.
//!
//! `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` (ClickBench q24, 105
//! columns over 10 M rows) decoded every column of each page a match was in: 0.91-1.0 GB at its
//! peak, against DuckDB's 0.37. Now the top-N runs over a narrow scan of its keys and each row's
//! place (its file, `input_file_name()`, and its row there, `file_row_index()`), and `LateRowsExec`
//! then reads only those rows' columns, by a row selection through each file's page index, and
//! hands them up in the top-N's order.
//!
//! Only a top-N right above a Parquet scan of `WIDE` columns or more, keeping at most `MOST` rows,
//! with its keys at most half of the columns. The narrow scan reads the same files with the same
//! deletes, ranges and filters, so the rows it keeps are the rows the wide one would have kept; they
//! are read again by their places alone (a filter evaluated again could drop one: `random()`).
//! Off with `PONDRA_LATE_ROWS=0`.

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::compute::{cast, interleave};
use datafusion::arrow::datatypes::{DataType, Int64Type, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, Result, Statistics};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, ParquetRowSelection};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::PageIndexPolicy;
use datafusion::physical_expr::expressions::{lit, CastExpr, Column};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ChildStats, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties, StatisticsArgs};
use futures::{FutureExt, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;

use crate::optimize::WIDE; // (the fewest columns a scan has for its top-N to read them late)
/// The most rows a top-N keeps for them to be read again one by one: each costs a page of every column.
const MOST: usize = 100;
/// The fewest files read at once for the rows; as many as the scan's partitions otherwise (`target_partitions`), as the
/// wide scan it takes the place of would read them. Each holds a page of every column while it is read, no more than a
/// partition of that scan holds of a row group, and on a bucket each is a few round trips: 100 files 4 at a time were 25
/// rounds after the narrow scan (11-03 audit M5).
const AT_ONCE: usize = 4;
const ROW: &str = "__late_row";
const FILE: &str = "__late_file";

fn on() -> bool {
    crate::env::get("PONDRA_LATE_ROWS").as_deref() != Some("0")
}

/// Puts a `LateRowsExec` in place of each wide top-N (a merge of the partitions' top-Ns, or one).
/// It runs before DataFusion's sort and dynamic filter pushdown, which then find the narrow scan
/// under the narrow top-N as they would any other.
#[derive(Debug)]
pub struct LateRows;

impl PhysicalOptimizerRule for LateRows {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        if !on() {
            return Ok(plan);
        }
        let config = Arc::new(config.clone());
        plan.transform_down(|p| match late(&p, &config)? {
            Some(late) => Ok(Transformed::new(late, true, TreeNodeRecursion::Jump)),
            None => Ok(Transformed::no(p)),
        })
        .data()
    }

    fn name(&self) -> &str {
        "late_rows"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `p` as a `LateRowsExec`, if it is a top-N whose rows are worth reading late.
fn late(p: &Arc<dyn ExecutionPlan>, config: &Arc<ConfigOptions>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let (merge, sort) = match (p.downcast_ref::<SortPreservingMergeExec>(), p.downcast_ref::<SortExec>()) {
        (Some(m), _) => match (m.fetch(), m.input().downcast_ref::<SortExec>()) {
            (Some(_), Some(s)) => (Some(m), s),
            _ => return Ok(None),
        },
        (None, Some(s)) => (None, s),
        _ => return Ok(None),
    };
    let Some(k) = sort.fetch().filter(|k| (1..=MOST).contains(k)) else { return Ok(None) };
    let scan = sort.input();
    let Some((files, parquet)) = scan.downcast_ref::<DataSourceExec>().and_then(|s| s.downcast_to_file_source::<ParquetSource>()) else { return Ok(None) };
    let schema = scan.schema();
    let width = schema.fields().len();
    let planned = files.file_groups.iter().flat_map(|g| g.files()).any(|f| f.extension::<ParquetAccessPlan>().is_some()); // (a row selection could not join it)
    if width < WIDE || files.limit.is_some() || planned || schema.fields().iter().any(|f| f.name() == ROW || f.name() == FILE) {
        return Ok(None);
    }
    let mut keys: Vec<usize> = vec![];
    for e in sort.expr().iter() {
        e.expr.apply(|n| {
            if let Some(c) = n.downcast_ref::<Column>().filter(|c| !keys.contains(&c.index())) {
                keys.push(c.index());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    if keys.is_empty() || keys.len() * 2 > width {
        return Ok(None);
    }
    let column = |i: usize| -> (Arc<dyn PhysicalExpr>, String) {
        let name = schema.field(i).name().clone();
        (Arc::new(Column::new(&name, i)), name)
    };
    let row: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(datafusion::functions::core::file_row_index(), vec![], &schema, config.clone())?);
    let name: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(datafusion::functions::core::input_file_name(), vec![], &schema, config.clone())?);
    // (a file's name is one value for all its rows: a dictionary of it costs a key a row, not the name)
    let file: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(name, DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), None));

    // The narrow scan: the keys' columns and the rows' places.
    let mut narrow: Vec<_> = keys.iter().map(|i| column(*i)).collect();
    narrow.push((row.clone(), ROW.into()));
    narrow.push((file, FILE.into()));
    let Some(narrow) = scan.try_swapping_with_projection(&ProjectionExec::try_new(narrow, scan.clone())?)? else { return Ok(None) };
    let ordering: Vec<PhysicalSortExpr> = sort
        .expr()
        .iter()
        .map(|e| {
            let expr = e.expr.clone().transform(|n| match n.downcast_ref::<Column>() {
                Some(c) => Ok(Transformed::yes(Arc::new(Column::new(c.name(), keys.iter().position(|k| *k == c.index()).expect("a key"))) as Arc<dyn PhysicalExpr>)),
                None => Ok(Transformed::no(n)),
            });
            Ok(PhysicalSortExpr { expr: expr.data()?, options: e.options })
        })
        .collect::<Result<_>>()?;
    let ordering = LexOrdering::new(ordering).ok_or_else(|| internal_datafusion_err!("a top-N with no keys"))?;
    // (partitioned first: a top-N's dynamic filter counts the partitions that update it)
    let mut top: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering.clone(), narrow).with_preserve_partitioning(sort.preserve_partitioning()).with_fetch(Some(k)));
    if let Some(m) = merge {
        top = Arc::new(SortPreservingMergeExec::new(ordering, top).with_fetch(m.fetch()));
    }

    // The scan the rows are read again by: every column and the row's place, no filter.
    let all: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(FileScanConfigBuilder::from(files.clone()).with_source(Arc::new(parquet.with_predicate(lit(true)))).build());
    let mut wide: Vec<_> = (0..width).map(column).collect();
    wide.push((row, ROW.into()));
    let Some(wide) = all.try_swapping_with_projection(&ProjectionExec::try_new(wide, all.clone())?)? else { return Ok(None) };
    Ok(Some(Arc::new(LateRowsExec { input: top, wide, properties: p.properties().clone(), keys: keys.len(), k })))
}

/// The rows of a narrow top-N (`input`: its keys, then each row's place), read again with all their
/// columns from `wide`'s files and handed up in the top-N's order, as the wide top-N it took the
/// place of (`properties`) would have.
#[derive(Debug, Clone)]
pub struct LateRowsExec {
    input: Arc<dyn ExecutionPlan>,
    /// A scan of every file the top-N reads, with its columns and then each row's place.
    wide: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    /// The top-N's key columns: the rows' places come after them.
    keys: usize,
    k: usize,
}

impl DisplayAs for LateRowsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LateRowsExec: rows={}, columns={}", self.k, self.schema().fields().len())
    }
}

impl ExecutionPlan for LateRowsExec {
    fn name(&self) -> &str {
        "LateRowsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|_| internal_datafusion_err!("LateRowsExec has one input"))?;
        Ok(Arc::new(LateRowsExec { input, ..(*self).clone() }))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    // (the input's rows, with other columns)
    fn statistics_from_inputs(&self, input_stats: &[Arc<Statistics>], _: &StatisticsArgs) -> Result<Arc<Statistics>> {
        let mut stats = Statistics::new_unknown(&self.schema());
        stats.num_rows = input_stats[0].num_rows;
        Ok(Arc::new(stats))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(&self, partition: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let kept = self.input.execute(partition, ctx.clone())?;
        let (wide, schema, keys) = (self.wide.clone(), self.schema(), self.keys);
        let out = futures::stream::once(async move { read_again(kept, wide, schema, keys, ctx).await.transpose() }).filter_map(futures::future::ready); // (no rows: no batch, as the top-N's)
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), out)))
    }
}

/// The rows `kept` names (their places after its `keys` columns), read from `wide`'s files, in
/// `kept`'s order, as `schema`; none if it names none.
async fn read_again(kept: SendableRecordBatchStream, wide: Arc<dyn ExecutionPlan>, schema: SchemaRef, keys: usize, ctx: Arc<TaskContext>) -> Result<Option<RecordBatch>> {
    let kept: Vec<RecordBatch> = kept.try_collect().await?;
    let mut places: Vec<(String, u64)> = vec![];
    for b in &kept {
        let rows = b.column(keys).as_primitive::<Int64Type>();
        let files = cast(b.column(keys + 1), &DataType::Utf8)?;
        let files = files.as_string::<i32>();
        places.extend((0..b.num_rows()).map(|i| (files.value(i).to_string(), rows.value(i) as u64)));
    }
    if places.is_empty() {
        return Ok(None);
    }
    let (config, _) = wide.downcast_ref::<DataSourceExec>().and_then(|s| s.downcast_to_file_source::<ParquetSource>()).ok_or_else(|| internal_datafusion_err!("LateRowsExec reads a Parquet scan"))?;
    let known: HashMap<&str, &PartitionedFile> = config.file_groups.iter().flat_map(|g| g.files()).map(|f| (f.object_meta.location.as_ref(), f)).collect();
    let mut wanted: Vec<(&str, Vec<usize>)> = vec![];
    for (file, row) in &places {
        match wanted.iter_mut().find(|(f, _)| f == file) {
            Some((_, rows)) => rows.push(*row as usize),
            None => wanted.push((file.as_str(), vec![*row as usize])),
        }
    }
    let store = ctx.runtime_env().object_store(&config.object_store_url)?;
    let cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
    let names: Vec<&str> = wanted.iter().map(|(name, _)| *name).collect();
    let mut reads = vec![];
    for (name, mut rows) in wanted {
        let file = (*known.get(name).ok_or_else(|| internal_datafusion_err!("LateRowsExec: {name} is not one of the scan's files"))?).clone();
        let (store, cache, ctx, config) = (store.clone(), cache.clone(), ctx.clone(), config.clone());
        reads.push(
            async move {
                // (with its page index, so only the rows' pages are read: in the cache once read)
                let meta = DFParquetMetadata::new(store.as_ref(), &file.object_meta).with_file_metadata_cache(Some(cache)).with_page_index_policy(Some(PageIndexPolicy::Optional)).fetch_metadata().await?;
                rows.sort_unstable();
                rows.dedup();
                let selection = RowSelection::from_consecutive_ranges(rows.iter().map(|r| *r..*r + 1), meta.file_metadata().num_rows() as usize);
                let mut file = file.with_extension(ParquetRowSelection::new(selection));
                file.range = None;
                let one = DataSourceExec::from_data_source(FileScanConfigBuilder::from(config).with_file_groups(vec![FileGroup::new(vec![file])]).build());
                one.execute(0, ctx)?.map_ok(crate::answer::compact).try_collect::<Vec<RecordBatch>>().await
            }
            .boxed(),
        );
    }
    let at_once = ctx.session_config().target_partitions().max(AT_ONCE);
    let read: Vec<Vec<RecordBatch>> = futures::stream::iter(reads).buffered(at_once).try_collect().await?;
    let width = schema.fields().len();
    let mut batches: Vec<RecordBatch> = vec![];
    let mut at: HashMap<(&str, u64), (usize, usize)> = HashMap::new();
    for (name, read) in names.into_iter().zip(read) {
        for b in read.into_iter().filter(|b| b.num_rows() > 0) {
            let rows = b.column(width).as_primitive::<Int64Type>();
            at.extend((0..b.num_rows()).map(|i| ((name, rows.value(i) as u64), (batches.len(), i))));
            batches.push(b);
        }
    }
    let order = places.iter().map(|(file, row)| at.get(&(file.as_str(), *row)).copied().ok_or_else(|| internal_datafusion_err!("LateRowsExec: row {row} of {file} was not read again"))).collect::<Result<Vec<_>>>()?;
    let columns = (0..width)
        .map(|c| {
            let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
            interleave(&arrays, &order)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(RecordBatch::try_new(schema, columns)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

    /// A file of `rows` rows from `first` on, in two row groups of pages of 50 rows: `id` (unique), `k` (ties, a NULL in
    /// every 11), `s` (text) and 17 more columns, a NULL in some rows of each.
    fn write(path: &std::path::Path, first: i64, rows: i64) -> Arc<Schema> {
        let mut fields = vec![Field::new("id", DataType::Int64, false), Field::new("k", DataType::Int64, true), Field::new("s", DataType::Utf8, false)];
        fields.extend((3..20).map(|c| Field::new(format!("c{c}"), if c % 2 == 0 { DataType::Int64 } else { DataType::Utf8 }, true)));
        let schema = Arc::new(Schema::new(fields));
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(rows as usize / 2)).set_data_page_row_count_limit(50).set_write_batch_size(50).build();
        let mut w = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema.clone(), Some(props)).unwrap();
        let ids: Vec<i64> = (first..first + rows).collect();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from_iter(ids.iter().map(|i| (i % 11 != 3).then_some(i * 7919 % 50)))),
            Arc::new(StringArray::from_iter_values(ids.iter().map(|i| format!("s{}", i * 31 % 1000)))),
        ];
        for c in 3..20i64 {
            columns.push(match c % 2 {
                0 => Arc::new(Int64Array::from_iter(ids.iter().map(|i| (i % 5 != 0).then_some(i * c)))),
                _ => Arc::new(StringArray::from_iter(ids.iter().map(|i| (i % 7 != 0).then(|| format!("{c}-{i}"))))),
            });
        }
        w.write(&RecordBatch::try_new(schema.clone(), columns).unwrap()).unwrap();
        w.close().unwrap();
        schema
    }

    /// Our rules, with or without this one; four partitions, each file in ranges (a row's place is still its file's).
    fn session(late: bool) -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(4).with_batch_size(64);
        config.options_mut().optimizer.repartition_file_min_size = 1;
        let rules = crate::optimize::physical_rules().into_iter().filter(|r| late || r.name() != "late_rows").collect();
        SessionContext::new_with_state(SessionStateBuilder::new().with_config(config).with_default_features().with_physical_optimizer_rules(rules).build())
    }

    async fn shown(ctx: &SessionContext, sql: &str) -> Result<String> {
        Ok(pretty_format_batches(&ctx.sql(sql).await?.collect().await?)?.to_string())
    }

    /// Each query's answer with its rows read late is its answer without: ties, NULLs, a key that is an expression,
    /// an OFFSET, a filter, no row at all, a top-N under another sort. A narrow one, or one of more than `MOST` rows,
    /// is left as it was.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_wide_top_n_read_late_answers_as_before() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-late-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        for (i, first) in [0, 1000, 5000].into_iter().enumerate() {
            write(&dir.join(format!("{i}.parquet")), first, 600);
        }
        let (late, plain) = (session(true), session(false));
        for ctx in [&late, &plain] {
            ctx.register_parquet("t", dir.to_str().unwrap(), ParquetReadOptions::default()).await?;
        }
        for (sql, read_late) in [
            ("SELECT * FROM t ORDER BY k, id LIMIT 10", true),
            ("SELECT * FROM t WHERE s LIKE '%7%' ORDER BY k DESC NULLS FIRST, id LIMIT 7", true),
            ("SELECT * FROM t ORDER BY k + id, id LIMIT 5 OFFSET 3", true),
            ("SELECT * FROM t WHERE c4 IS NULL ORDER BY s DESC, id LIMIT 100", true),
            ("SELECT * FROM t WHERE id < 0 ORDER BY id LIMIT 10", true),
            ("SELECT * FROM (SELECT * FROM t ORDER BY c3 LIMIT 4) ORDER BY id", true),
            ("SELECT id, c5, c18 FROM (SELECT * FROM t ORDER BY c3 LIMIT 4) ORDER BY id", false), // (its scan reads 4 columns)
            ("SELECT id, k FROM t ORDER BY k, id LIMIT 3", false),
            ("SELECT * FROM t ORDER BY k, id LIMIT 101", false),
        ] {
            let plan = shown(&late, &format!("EXPLAIN {sql}")).await?;
            assert_eq!(plan.contains("LateRowsExec"), read_late, "{sql}: {plan}");
            assert_eq!(shown(&late, sql).await?, shown(&plain, sql).await?, "{sql}");
        }
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// A lake file's deleted rows and its range are the narrow scan's: the rows read again by their places are among
    /// those it kept, so none deleted comes back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_file_with_deletes_read_late_answers_as_before() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-late-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("d.parquet");
        let schema = write(&path, 0, 600);
        let (size, key) = (std::fs::metadata(&path)?.len(), path.to_str().unwrap().trim_start_matches('/').to_string());
        let file = |deleted: Vec<u64>, range| crate::scan::File { key: key.clone(), size, rows: 600, partition: vec![], deleted: Some(Arc::new(deleted)), range, only: None, stats: Arc::new(Statistics::new_unknown(&schema)), modified: None };
        let files = || crate::scan::Files { store: ObjectStoreUrl::local_filesystem(), file_schema: schema.clone(), partition: vec![], files: vec![file((0..600).step_by(3).collect(), Some((100, 500)))], adapter: None, row: None };
        let (late, plain) = (session(true), session(false));
        for ctx in [&late, &plain] {
            ctx.register_table("d", Arc::new(files()))?;
        }
        for sql in ["SELECT * FROM d ORDER BY k, id LIMIT 20", "SELECT * FROM d ORDER BY id LIMIT 3", "SELECT * FROM d ORDER BY id DESC LIMIT 3"] {
            assert!(shown(&late, &format!("EXPLAIN {sql}")).await?.contains("LateRowsExec"), "{sql}");
            let answer = shown(&late, sql).await?;
            assert_eq!(answer, shown(&plain, sql).await?, "{sql}");
            assert!(answer.contains("| 101 ") || answer.contains("| 499 ") || sql.contains("k, id"), "{sql}: {answer}");
        }
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
