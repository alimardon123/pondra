//! Streaming SQL tasks: run `sql` over the NEW rows of `source` whenever some are committed, and
//! append the result to `target` (created from the query's output if missing). `sql` may join
//! any lake table, including `target` itself: that's how general stateful tasks work. With a
//! `key`, the target is an upsert table holding the task's state, readable by SQL.
//! (Aggregations are simpler and faster as inline views; tasks are for everything else.)
//!
//! Progress is a producer sequence: a run over segments (done, hwm] appends with seq `hwm` and
//! `prev = done`, so output and progress commit together (exactly-once), and a run computed
//! from a stale view is rejected (compare-and-swap) and simply retried.
//!
//! Distributed state: `shards` splits a task by the hash of `shard_by`; shard `s` runs on the
//! `s % n`-th live node and only ever touches its own keys, so shards never conflict.
use crate::cluster::Cluster;
use crate::log::{Log, Src};
use crate::query::{over, schema, session, tail};
use crate::store::*;
use anyhow::{bail, Context, Result};
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::{create_hashes, RandomState};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Task {
    pub source: String,
    pub target: String,
    pub sql: String,
    #[serde(default)]
    pub key: Vec<String>, // target primary key: makes the target an upsert (state) table
    #[serde(default = "one")]
    pub shards: u32,
    #[serde(default)]
    pub shard_by: Option<String>, // required when shards > 1
}

fn one() -> u32 { 1 }

pub fn task_key(name: &str) -> String { format!("k/{name}") }

/// The producer names that carry a task's progress, one per shard.
pub fn producers(name: &str, task: &Task) -> Vec<String> {
    match task.shards {
        1 => vec![format!("task:{name}")],
        n => (0..n).map(|s| format!("task:{name}:{s}")).collect(),
    }
}

/// Run this node's shards of every task over whatever arrived since each shard's last run.
pub async fn run_all(lake: &Lake, cluster: &Cluster, log: &Log) -> Result<()> {
    let hwm = lake.visible(); // not the watch: what this node's reads actually include
    let mut runs = vec![];
    for (key, task) in lake.cat.scan::<Task>("k/", "k0").await? {
        for (s, producer) in producers(&key[2..], &task).into_iter().enumerate() {
            let done: u64 = lake.cat.get(&producer_key(&producer)).await?.unwrap_or(0);
            if hwm <= done || !cluster.runs_shard(s as u32) {
                continue;
            }
            let rows = tail(lake, &task.source, done, Some(hwm), false).await?;
            if rows.iter().all(|b| b.num_rows() == 0) {
                continue; // nothing new for this task: no run, no commit
            }
            let batch = crate::query::cast_as(&run(lake, &task, s as u32, rows).await?, &target_schema(lake, &task).await?)?;
            cluster.shard_runs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let src = Src { producer, seq: hwm, prev: Some(done) }; // output + progress, if progress is still `done`
            runs.push(log.append(task.target.clone(), src, batch));
        }
    }
    for ack in futures::future::join_all(runs).await {
        ack?; // a conflict just means another run got there first: the next one catches up
    }
    Ok(())
}

/// One shard's output for new source rows.
async fn run(lake: &Lake, task: &Task, shard: u32, mut rows: Vec<RecordBatch>) -> Result<RecordBatch> {
    if task.shards > 1 {
        let col = task.shard_by.as_deref().context("shard_by is required with shards > 1")?;
        rows = rows.iter().map(|b| shard_rows(b, col, shard, task.shards)).collect::<Result<_>>()?;
    }
    over(lake, &task.source, rows, &task.sql).await
}

async fn target_schema(lake: &Lake, task: &Task) -> Result<datafusion::arrow::datatypes::SchemaRef> {
    schema(&lake.cat.get::<TableMeta>(&table_key(&task.target)).await?.context("no target table")?.columns)
}

/// Register a task (leader only): its SQL must plan, and the target table is created from the
/// query's output columns if it doesn't exist yet.
pub async fn create(lake: &Lake, name: &str, task: &Task) -> Result<()> {
    if task.shards > 1 && task.shard_by.is_none() {
        bail!("shard_by is required with shards > 1");
    }
    let out = session(lake, &task.sql, "").await?.sql(&crate::asof::rewrite(&task.sql)?).await?.schema().as_arrow().clone();
    let mut puts = vec![(task_key(name), json(task))];
    if lake.cat.get::<TableMeta>(&table_key(&task.target)).await?.is_none() {
        let columns = out.fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
        puts.push((table_key(&task.target), json(&TableMeta { columns, key: task.key.clone(), publish: default_publish(), ..Default::default() })));
    }
    lake.cat.commit(puts, &[]).await
}

/// The rows whose `col` hashes to `shard` (fixed seed: every node agrees).
fn shard_rows(b: &RecordBatch, col: &str, shard: u32, shards: u32) -> Result<RecordBatch> {
    let Some(array) = b.column_by_name(col) else { bail!("no column {col}") };
    let mut hashes = vec![0u64; b.num_rows()];
    create_hashes([array], &RandomState::with_seed(42), &mut hashes)?;
    let keep: BooleanArray = hashes.iter().map(|h| Some(h % shards as u64 == shard as u64)).collect();
    Ok(filter_record_batch(b, &keep)?)
}
