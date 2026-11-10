//! A scan's files cut into morsels, which its partitions take from one queue as each gets free.
//!
//! DataFusion reads a Parquet scan's ranges from one queue that every partition takes from (`enable_file_stream_work_stealing`),
//! but it cuts the files into only as many ranges as there are partitions, one each. A partition whose range holds more
//! of the work then finishes last while the other cores wait: when a filter keeps a stretch of a file sorted by date,
//! one range holds all of it. Cut into a few morsels a partition, a partition that gets free takes the next one, as
//! DuckDB's and Velox's threads take row groups. At SF10 on 4 cores (11-09, main 43359c6, interleaved runs): TPC-DS
//! 83.0 s to 80.0 s, q72 1.02 to 0.55 s, q22 4.10 to 2.58 s, q99 0.37 to 0.15 s (each half idle before); TPC-H, whose
//! dates are spread through its files, as it was.
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::Result;
use datafusion::datasource::physical_plan::{FileGroup, FileGroupPartitioner, FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Morsels a partition's share is cut into, at most. Finer leaves less for the last core, but DataFusion 55 copies a
/// file's whole footer each time it opens a range of a file without a page index (DuckDB's and pyarrow's): at 16 a
/// partition TPC-DS q88, eight scans of store_sales, ran 11% slower; at 4, within its noise.
const PER_PARTITION: usize = 4;

/// The least a morsel holds, in file bytes: a small file isn't worth the opens.
const LEAST: u64 = 16 << 20;

/// The rule (`optimize::physical_rules`, after the exchanges and DataFusion's own ranges are in place).
#[derive(Debug)]
pub struct Morsels;

impl PhysicalOptimizerRule for Morsels {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        // (a spread query's slices are planned alike on every node, its scans' shares by the spread analysis)
        if !config.execution.enable_file_stream_work_stealing || !config.optimizer.repartition_file_scans || crate::optimize::spread(&plan)? {
            return Ok(plan);
        }
        plan.transform_up(|p| {
            let Some(scan) = p.downcast_ref::<DataSourceExec>() else { return Ok(Transformed::no(p)) };
            let Some(files) = scan.data_source().downcast_ref::<FileScanConfig>() else { return Ok(Transformed::no(p)) };
            Ok(match finer(files) {
                Some(groups) => Transformed::yes(DataSourceExec::from_data_source(FileScanConfigBuilder::from(files.clone()).with_file_groups(groups).build()) as Arc<dyn ExecutionPlan>),
                None => Transformed::no(p),
            })
        })
        .data()
    }

    fn name(&self) -> &str { "Morsels" }

    fn schema_check(&self) -> bool { true }
}

/// The scan's ranges cut finer, as many groups as it had (its partitions), each a run of neighbouring morsels (the
/// queue is the groups one after another, so the files are read in order); or `None` where its partitions may not
/// share: a scan that keeps its files' order, or whose groups are the plan's partitioning, or a file read with a
/// selection of its own (a table's deletes), or one too small to be worth it.
fn finer(files: &FileScanConfig) -> Option<Vec<FileGroup>> {
    let parts = files.file_groups.len();
    if parts < 2 || files.preserve_order || files.output_partitioning.is_some() || !files.output_ordering.is_empty() {
        return None;
    }
    let all: Vec<_> = files.file_groups.iter().flat_map(FileGroup::iter).collect();
    if all.iter().any(|f| !f.extensions.is_empty()) {
        return None;
    }
    let bytes: u64 = all.iter().map(|f| f.effective_size()).sum();
    let morsels = usize::try_from(bytes / LEAST).unwrap_or(usize::MAX).min(parts * PER_PARTITION);
    if morsels <= all.len() {
        return None;
    }
    let cut: Vec<_> = FileGroupPartitioner::new()
        .with_target_partitions(morsels)
        .with_repartition_file_min_size(0)
        .with_preserve_order_within_groups(false)
        .repartition_file_groups(&files.file_groups)?
        .into_iter()
        .flat_map(FileGroup::into_inner)
        .collect();
    (cut.len() >= parts).then(|| (0..parts).map(|i| FileGroup::new(cut[i * cut.len() / parts..(i + 1) * cut.len() / parts].to_vec())).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;

    fn scan(groups: Vec<Vec<PartitionedFile>>) -> FileScanConfig {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(ParquetSource::new(schema))).with_file_groups(groups.into_iter().map(FileGroup::new).collect()).build()
    }

    /// Each file's ranges, in the order the queue holds them.
    fn ranges(groups: &[FileGroup]) -> Vec<(String, i64, i64)> {
        groups.iter().flat_map(FileGroup::iter).map(|f| {
            let r = f.range.clone().expect("a range");
            (f.object_meta.location.to_string(), r.start, r.end)
        }).collect()
    }

    #[test]
    fn a_big_file_in_four_ranges_becomes_four_groups_of_four_neighbouring_morsels() {
        let size = 1u64 << 30;
        let quarter = (size / 4) as i64;
        let file = PartitionedFile::new("a.parquet", size);
        let groups = (0..4).map(|i| vec![file.clone().with_range(i * quarter, (i + 1) * quarter)]).collect();
        let cut = finer(&scan(groups)).expect("cut finer");
        assert_eq!(cut.len(), 4, "as many partitions as before");
        assert!(cut.iter().all(|g| g.len() == 4), "{:?}", cut.iter().map(FileGroup::len).collect::<Vec<_>>());
        let r = ranges(&cut);
        assert_eq!(r.first().map(|r| r.1), Some(0));
        assert_eq!(r.last().map(|r| r.2), Some(size as i64));
        assert!(r.windows(2).all(|w| w[0].2 == w[1].1), "neighbouring and whole: {r:?}");
    }

    #[test]
    fn many_files_are_cut_across_their_ends_and_none_is_lost() {
        let files: Vec<_> = (0..3).map(|i| PartitionedFile::new(format!("f{i}.parquet"), 300 << 20)).collect();
        let cut = finer(&scan(vec![vec![files[0].clone(), files[1].clone()], vec![files[2].clone()]])).expect("cut finer");
        assert_eq!(cut.len(), 2);
        let r = ranges(&cut);
        assert!(r.len() > 3 && r.len() <= 2 * PER_PARTITION + 3, "{r:?}");
        for f in ["f0.parquet", "f1.parquet", "f2.parquet"] {
            let total: i64 = r.iter().filter(|x| x.0.ends_with(f)).map(|x| x.2 - x.1).sum();
            assert_eq!(total, 300 << 20, "{f}: {r:?}");
        }
    }

    #[test]
    fn small_ordered_or_selected_scans_are_left_as_they_are() {
        let small = (0..4).map(|i| vec![PartitionedFile::new(format!("s{i}.parquet"), 4 << 20)]).collect();
        assert!(finer(&scan(small)).is_none(), "16 MB in all: no morsel worth an open");
        let one = scan(vec![vec![PartitionedFile::new("a.parquet", 1 << 30)]]);
        assert!(finer(&one).is_none(), "one partition has no one to share with");
        let mut ordered = scan(vec![vec![PartitionedFile::new("a.parquet", 1 << 30)], vec![PartitionedFile::new("b.parquet", 1 << 30)]]);
        ordered.preserve_order = true;
        assert!(finer(&ordered).is_none(), "a scan that keeps its order");
        let mut selected = PartitionedFile::new("a.parquet", 1 << 30);
        selected.extensions.insert(1u8);
        assert!(finer(&scan(vec![vec![selected], vec![PartitionedFile::new("b.parquet", 1 << 30)]])).is_none(), "a file read with its own selection");
    }
}
