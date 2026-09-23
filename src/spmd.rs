//! Distributed queries, SPMD style. Every live node runs the same SQL over its own slice of the
//! data, reading its files straight from the bucket, and the node that received the query (the
//! coordinator) finishes the plan over everyone's results. No coordinator service and no
//! scheduler: every node plans the same query the same way, so the plans' exchanges line up.
//! Two ways, chosen from DataFusion's own parallel plan:
//!
//! - **Gather.** The query's main table (the first in FROM) is sliced; every other table is read
//!   whole by each node (broadcast, so star joins work). Each node runs the plan up to its first
//!   exchange — for an aggregation, the partial aggregate — and the coordinator merges.
//! - **Shuffle.** When the plan exchanges rows by hash — a GROUP BY with many groups, a join of
//!   two big tables — every table is sliced and each hash exchange becomes a shuffle between
//!   nodes: a node runs a stage over its inputs, splits the output by the exchange's hash into one
//!   bucket per node and keeps it; the next stage on node j fetches bucket j from every node. The
//!   last stage's results go to the coordinator. Only operators that stay correct when split this
//!   way are allowed; anything else runs the gather way, or on one node.
//!
//! Which queries: one SELECT (no subqueries, CTEs or set operations), inner joins only, over
//! append tables. The cuts are taken from DataFusion's own plan, so they are right for any
//! aggregate DataFusion can split (avg, count distinct, …).
use crate::manifest::Manifest;
use crate::query::{session, Pruned};
use crate::spill::Spill;
use crate::store::*;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::execution_plan::replace_children_if_necessary;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties, Partitioning};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

/// Tables smaller than this aren't worth spreading.
const SPREAD_BYTES: u64 = 256 << 20;

/// In a shuffle, tables smaller than this are read whole by every node (joins broadcast them).
const BROADCAST_BYTES: u64 = 64 << 20;

/// One table's share on one node: manifests and files, and maybe the log tail (after..=upto).
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Part {
    pub table: String,
    pub manifests: Vec<Manifest>,
    pub files: Vec<DataFile>,
    pub tail: Option<(u64, u64)>,
}

/// One node's share of a query: gathering, the main table's part; shuffling, every table's.
/// `id` names the folder this node spills its results into, and is what frees them afterwards.
#[derive(Serialize, Deserialize, Clone)]
pub struct Slice {
    pub sql: String,
    pub parts: Vec<Part>,
    pub shuffle: Option<Shuffle>,
    #[serde(default)]
    pub id: String,
}

impl Slice {
    fn job(&self) -> String {
        self.shuffle.as_ref().map_or_else(|| self.id.clone(), |sh| sh.id.clone())
    }
}

/// A shuffle, as one node takes part in it.
#[derive(Serialize, Deserialize, Clone)]
pub struct Shuffle {
    pub id: String,
    pub nodes: Vec<String>,
    pub me: usize,
    pub step: usize, // a hash exchange (bottom-up); the number of them: the last stage
    pub all: bool,   // every table sliced (else small ones are read whole: broadcast)
}

/// Run `sql` across `nodes` (this node is `me`), or None if it should just run here.
pub async fn query(lake: &Lake, nodes: &[String], me: &str, sql: &str, force: bool) -> Result<Option<Vec<RecordBatch>>> {
    if nodes.len() < 2 || !spreadable(sql) {
        return Ok(None);
    }
    let tables = tables(sql)?;
    let main = tables[0].clone();
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&main)).await? else { return Ok(None) };
    let sealed = meta.sealed.clone().unwrap_or_default();
    let bytes = sealed.bytes + meta.files.iter().map(|f| f.bytes).sum::<u64>();
    let files = sealed.files as usize + meta.files.len();
    if !meta.key.is_empty() || files == 0 || (!force && (files < nodes.len() || bytes < SPREAD_BYTES)) {
        return Ok(None); // (`force`: spread anyway, for tests)
    }
    let mine = nodes.iter().position(|n| n == me).context("not a member")?;
    let parts = deal(lake, &meta, &main, nodes.len()).await?;
    let id = uuid::Uuid::new_v4().to_string(); // (the folder every node spills this query's results into)
    let slices: Vec<Slice> = parts.into_iter().map(|p| Slice { sql: sql.into(), parts: vec![p], shuffle: None, id: id.clone() }).collect();
    let (ctx, plan) = plan(lake, &slices[mine]).await?;
    // (a table sliced twice, in a self-join, would only meet its own slice)
    let cut = find_cut(&plan).filter(|_| tables.iter().filter(|t| **t == main).count() == 1);
    if cut.as_ref().is_none_or(hashed) {
        if let Some(rows) = shuffle(lake, nodes, me, sql, &tables).await? {
            return Ok(Some(rows));
        }
    }
    let Some(cut) = cut else { return Ok(None) };
    // Gather: every node computes its partial result at the same time (this one: its files and,
    // as one more slice, the log tail).
    let tail = Slice { sql: sql.into(), parts: vec![Part { table: main, tail: Some((meta.tiered, lake.visible())), ..Default::default() }], shuffle: None, id: id.clone() };
    let runs = nodes.iter().zip(&slices).map(|(node, s)| async move {
        match node == me {
            true => Ok(vec![]), // below
            false => remote(node, s).await.map(|(_, parts)| parts),
        }
    });
    let mine_now = cut.children()[0].clone();
    let ours = drain(&mine_now, ctx.task_ctx(), &id, "here");
    let tail = async {
        let (after, upto) = tail.parts[0].tail.expect("tail");
        match upto > after { true => stage(lake, &tail).await.map(|(_, parts)| parts), false => Ok(vec![]) } // no log tail: nothing to do
    };
    let gathered = async {
        let (mut parts, ours, tail) = futures::future::try_join3(futures::future::try_join_all(runs), ours, tail).await?;
        parts.extend([ours, tail]);
        finish(&ctx, &plan, Some(&cut), parts.into_iter().flatten().collect()).await
    };
    let out = gathered.await;
    crate::spill::clear(&id); // (what this node spilled for it; the others sweep theirs)
    out.map(Some)
}

/// The coordinator's last step: the plan above `cut`, over everyone's results — read back a piece
/// at a time, so what the nodes sent never has to fit in its memory at once. No cut: they are the
/// results.
async fn finish(ctx: &SessionContext, plan: &Arc<dyn ExecutionPlan>, cut: Option<&Arc<dyn ExecutionPlan>>, parts: Vec<Spill>) -> Result<Vec<RecordBatch>> {
    let Some(cut) = cut else { return rows(parts, ctx).await };
    let schema = cut.children()[0].schema();
    ensure!(parts.iter().filter_map(|s| s.schema()).all(|s| s.fields() == schema.fields()), "nodes planned the query differently");
    let pieces: Vec<_> = parts.into_iter().flat_map(|s| s.pieces(schema.clone())).collect();
    let input = Arc::new(StreamingTableExec::try_new(schema, pieces, None, [], false, None)?) as Arc<dyn ExecutionPlan>;
    let plan = plan.clone().transform_down(|p| Ok(if Arc::ptr_eq(&p, cut) { Transformed::yes(replace_children_if_necessary(p, vec![input.clone()])?) } else { Transformed::no(p) }))?.data;
    Ok(datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?)
}

/// Buckets read back as rows (the answer itself, when there is nothing left to do to it).
async fn rows(parts: Vec<Spill>, ctx: &SessionContext) -> Result<Vec<RecordBatch>> {
    let Some(schema) = parts.iter().find_map(|s| s.schema()) else { return Ok(vec![]) };
    ensure!(parts.iter().filter_map(|s| s.schema()).all(|s| s.fields() == schema.fields()), "nodes planned the query differently");
    let pieces = parts.into_iter().flat_map(|s| s.pieces(schema.clone())).collect();
    let plan = Arc::new(StreamingTableExec::try_new(schema, pieces, None, [], false, None)?);
    Ok(datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?)
}

/// A table's manifests and files dealt over `n` nodes, oldest first (so a time range spreads over
/// all of them) and by size: each goes to whichever node has the fewest bytes so far. Files are
/// rarely the same size — a merged one against the last hour's — and round-robin would leave one
/// node reading twice what another does. Too few manifests to go round: their files are dealt
/// instead.
async fn deal(lake: &Lake, meta: &TableMeta, table: &str, n: usize) -> Result<Vec<Part>> {
    let mut manifests = crate::manifest::list(lake, meta).await?;
    let mut files = vec![];
    if manifests.len() < 4 * n {
        for m in manifests.drain(..) {
            files.extend(crate::manifest::files(lake, &m).await?);
        }
    }
    files.extend(meta.files.iter().cloned());
    let (mut parts, mut load) = (vec![Part { table: table.into(), ..Default::default() }; n], vec![0u64; n]);
    let lightest = |load: &[u64]| load.iter().enumerate().min_by_key(|(i, b)| (**b, *i)).expect("a node").0;
    for m in manifests {
        let i = lightest(&load);
        (load[i], _) = (load[i] + m.bytes.max(1), parts[i].manifests.push(m));
    }
    for f in files {
        let i = lightest(&load);
        (load[i], _) = (load[i] + f.bytes.max(1), parts[i].files.push(f));
    }
    Ok(parts)
}

/// This node's part of a query: a gather's partial result, or a shuffle step (with the plan's
/// shape, which every node must agree on). One list of batches per partition, order kept.
pub async fn stage(lake: &Lake, s: &Slice) -> Result<(String, Vec<Spill>)> {
    if let Some(sh) = &s.shuffle {
        return step(lake, s, sh).await;
    }
    let (ctx, plan) = plan(lake, s).await?;
    let cut = find_cut(&plan).context("no single-stage plan")?;
    Ok((String::new(), drain(&cut.children()[0].clone(), ctx.task_ctx(), &s.job(), "out").await?))
}

/// Every partition of `plan` run into a bucket of its own: what this node passes on, held in
/// memory while it is small and on this node's disk beyond, so a stage's result isn't bounded by
/// memory (`spill.rs`).
async fn drain(plan: &Arc<dyn ExecutionPlan>, ctx: Arc<datafusion::execution::TaskContext>, job: &str, name: &str) -> Result<Vec<Spill>> {
    let dir = crate::spill::dir(job);
    let runs = (0..plan.output_partitioning().partition_count()).map(|p| {
        let (plan, ctx, dir) = (plan.clone(), ctx.clone(), dir.clone());
        let name = format!("{name}-{p}");
        async move {
            let (mut spill, mut rows) = (Spill::new(dir, name), plan.execute(p, ctx)?);
            while let Some(b) = futures::StreamExt::next(&mut rows).await {
                spill.push(b?)?;
            }
            Ok::<_, anyhow::Error>(spill)
        }
    });
    futures::future::try_join_all(runs).await
}

/// Another node's share, streamed onto this node's disk piece by piece: the plan's shape first,
/// then how many buckets follow, then each bucket's piece count and its pieces.
async fn remote(node: &str, s: &Slice) -> Result<(String, Vec<Spill>)> {
    let res = crate::cluster::http().post(format!("http://{node}/cluster/stage")).json(s).send().await?;
    ensure!(res.status().is_success(), "{node}: {}", res.text().await?);
    let mut frames = crate::spill::Frames::new(res.bytes_stream());
    let shape = String::from_utf8(frames.next().await?.context("an empty reply")?)?;
    let (dir, mut parts) = (crate::spill::dir(&s.job()), vec![]);
    for p in 0..frames.count().await? {
        let name = format!("from-{}-{p}", node.replace(':', "_"));
        let pieces = frames.count().await?;
        parts.push(Spill::take(dir.clone(), name, &mut frames, pieces).await?);
    }
    Ok((shape, parts))
}

/// A stage's reply as it goes on the wire (`remote` reads it back). `done` frees what the stage
/// spilled once it has been sent — a shuffle passes none, because its buckets are kept for a step
/// that has to be run again.
pub fn reply(shape: &str, parts: Vec<Spill>, done: Option<crate::spill::Gone>) -> impl futures::Stream<Item = Result<Vec<u8>, std::io::Error>> {
    use crate::spill::frame;
    use futures::StreamExt;
    let head = vec![Ok(frame(shape.as_bytes())), Ok(frame(&(parts.len() as u32).to_le_bytes()))];
    let body = parts.into_iter().map(|s| futures::stream::once(std::future::ready(Ok(frame(&(s.count() as u32).to_le_bytes())))).chain(s.framed()));
    futures::stream::iter(head).chain(futures::stream::iter(body).flatten()).map(move |x| { let _keep = &done; x })
}

/// The physical plan of the slice's query, its tables standing for just their parts.
async fn plan(lake: &Lake, s: &Slice) -> Result<(SessionContext, Arc<dyn ExecutionPlan>)> {
    let ctx = session(lake, &s.sql, "").await?;
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        let o = state.config_mut().options_mut();
        // Always aggregate before the exchange: partial results cross the network, so passing
        // raw rows through (DataFusion's shortcut for high-cardinality groups) would ship the table.
        o.execution.skip_partial_aggregation_probe_rows_threshold = usize::MAX;
        if let Some(sh) = &s.shuffle {
            o.optimizer.enable_dynamic_filter_pushdown = false; // (a join's filter would reach a scan of an earlier step)
            if sh.all {
                // Every table sliced: a join must shuffle both sides (a broadcast side would be partial).
                (o.optimizer.hash_join_single_partition_threshold, o.optimizer.hash_join_single_partition_threshold_rows) = (0, 0);
            }
        }
    }
    for p in &s.parts {
        let meta: TableMeta = lake.cat.get(&table_key(&p.table)).await?.context("no table")?;
        let (after, upto) = p.tail.unwrap_or((0, 0)); // (0, 0): no tail
        let schema = crate::query::read_schema(&meta.columns)?;
        let sealed = meta.sealed.clone().unwrap_or_default();
        let share = Some((sealed.rows + meta.files.iter().map(|f| f.rows).sum::<u64>(), sealed.bytes + meta.files.iter().map(|f| f.bytes).sum::<u64>()));
        // The whole table's ranges, not this slice's: every node has to plan the query alike.
        let ranges = crate::manifest::ranges(&p.table, &crate::manifest::list(lake, &meta).await?, &meta.files, &schema);
        let meta = TableMeta { files: p.files.clone(), tiered: after, ..meta };
        let table = Pruned { lake: lake.arc(), name: p.table.clone(), meta, manifests: Some(p.manifests.clone()), upto: Some(upto), schema, share, ranges };
        ctx.deregister_table(p.table.as_str())?;
        ctx.register_table(p.table.as_str(), Arc::new(table))?;
    }
    let plan = ctx.sql_with_options(&s.sql, crate::query::read_only()).await?.create_physical_plan().await?;
    Ok((ctx, plan))
}

/// The exchange whose input is the per-partition part of the plan: the first exchange on the
/// plan's single path down, with no other exchange below it.
fn find_cut(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let exchange = |p: &Arc<dyn ExecutionPlan>| gathers(p) || p.name() == "RepartitionExec" && !matches!(p.output_partitioning(), Partitioning::RoundRobinBatch(_));
    fn any(p: &Arc<dyn ExecutionPlan>, f: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool) -> bool { f(p) || p.children().into_iter().any(|c| any(c, f)) }
    match plan.children()[..] {
        [child] if exchange(plan) && !any(child, &exchange) => Some(plan.clone()),
        [child] => find_cut(child),
        _ => None,
    }
}

fn gathers(p: &Arc<dyn ExecutionPlan>) -> bool { matches!(p.name(), "CoalescePartitionsExec" | "SortPreservingMergeExec") }

fn hashed(p: &Arc<dyn ExecutionPlan>) -> bool { p.name() == "RepartitionExec" && matches!(p.output_partitioning(), Partitioning::Hash(..)) }

// ---------------------------------------------------------------- shuffles

/// Run `sql` as a shuffle, or None if its plan can't be split that way. A node that drops out is
/// left out and the shuffle runs again (its buckets went with it, so there is nothing to resume);
/// with too few nodes left for that, the query runs here instead.
async fn shuffle(lake: &Lake, nodes: &[String], me: &str, sql: &str, tables: &[String]) -> Result<Option<Vec<RecordBatch>>> {
    let mut live: Vec<String> = nodes.to_vec();
    for _ in 0..3 {
        let mine = live.iter().position(|n| n == me).context("not a member")?;
        let out = spread_once(lake, &live, mine, sql, tables).await;
        let Err(e) = out else { return out };
        let Some(dead) = e.downcast_ref::<Dead>().map(|d| d.node).filter(|_| live.len() > 2) else {
            return match e.downcast_ref::<Dead>() {
                Some(d) => {
                    eprintln!("shuffle: {d}; running the query here instead");
                    Ok(None)
                }
                None => Err(e),
            };
        };
        eprintln!("shuffle: {e:#}; running it again without that node");
        abandon(&live, me).await; // (the nodes that kept buckets for it can let them go now)
        live.remove(dead);
    }
    Ok(None)
}

/// Tell the others to forget the shuffle just given up on (and forget it here).
async fn abandon(nodes: &[String], me: &str) {
    let Some(id) = LAST.lock().unwrap().clone() else { return };
    forget(&id);
    let asks = nodes.iter().filter(|n| *n != me).map(|n| crate::cluster::http().get(format!("http://{n}/cluster/shuffle?id={id}&exchange=0&to=0&drop=1")).send());
    futures::future::join_all(asks).await;
}

/// The id of the shuffle this node started last (for `abandon`).
static LAST: LazyLock<Mutex<Option<String>>> = LazyLock::new(Default::default);

/// One attempt: first with small tables read whole by every node (broadcast joins), then with
/// every table sliced.
async fn spread_once(lake: &Lake, nodes: &[String], mine: usize, sql: &str, tables: &[String]) -> Result<Option<Vec<RecordBatch>>> {
    for all in [false, true] {
        let id = uuid::Uuid::new_v4().to_string();
        *LAST.lock().unwrap() = Some(id.clone());
        let mut slices: Vec<Slice> = (0..nodes.len()).map(|me| Slice { sql: sql.into(), parts: vec![], shuffle: Some(Shuffle { id: id.clone(), nodes: nodes.to_vec(), me, step: 0, all }), id: id.clone() }).collect();
        let mut seen = std::collections::HashSet::new();
        for t in tables.iter().filter(|t| seen.insert(*t)) {
            let meta = lake.cat.get::<TableMeta>(&table_key(t)).await?;
            let bytes = meta.as_ref().map_or(0, |m| m.sealed.as_ref().map_or(0, |s| s.bytes) + m.files.iter().map(|f| f.bytes).sum::<u64>());
            if !all && *t != tables[0] && bytes < BROADCAST_BYTES {
                continue; // (read whole)
            }
            let Some(meta) = meta.filter(|m| m.key.is_empty()) else { return Ok(None) }; // (append tables of this lake only)
            for (i, mut p) in deal(lake, &meta, t, nodes.len()).await?.into_iter().enumerate() {
                p.tail = (i == mine).then(|| (meta.tiered, lake.visible())); // (the log tails here)
                slices[i].parts.push(p);
            }
        }
        if let Some(job) = Job::open(lake, &slices[mine]).await? {
            return run(lake, nodes, mine, &slices, job).await.map(Some); // (step 0 here uses this plan)
        }
    }
    Ok(None)
}

/// Every step of a shuffle on every node, then the coordinator's finish.
async fn run(lake: &Lake, nodes: &[String], mine: usize, slices: &[Slice], job: Arc<Job>) -> Result<Vec<RecordBatch>> {
    let id = slices[mine].shuffle.as_ref().expect("a shuffle").id.clone();
    crate::metrics::add(&crate::metrics::SHUFFLED, !job.exchanges.is_empty() as u64);
    let run = async {
        let mut last: Vec<Spill> = vec![];
        for step in 0..=job.exchanges.len() {
            let runs = nodes.iter().enumerate().zip(slices).map(|((i, node), s)| {
                let mut s = s.clone();
                s.shuffle.as_mut().expect("a shuffle").step = step;
                async move {
                    let once = async |s: &Slice| match i == mine {
                        true => stage(lake, s).await,
                        false => remote(node, s).await,
                    };
                    // A step is the same work every time (the buckets it reads are kept until the
                    // job ends), so a node that stumbles — a timeout, a restart mid-step — gets
                    // one more try before the query gives up on it.
                    match once(&s).await {
                        Err(first) => {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            once(&s).await.map_err(|again| Dead { node: i, why: format!("{first:#}; then {again:#}") }.into())
                        }
                        ok => ok,
                    }
                }
            });
            let outs = futures::future::try_join_all(runs).await?;
            ensure!(outs.iter().all(|(shape, _)| *shape == job.shape), "nodes planned the query differently");
            last = outs.into_iter().flat_map(|(_, parts)| parts).collect();
        }
        finish(&job.ctx, &job.plan, job.cut.as_ref(), last).await
    };
    let out = run.await;
    JOBS.lock().unwrap().remove(&id); // (all steps done: nobody fetches from here any more)
    crate::spill::clear(&id);
    out
}

/// A node that failed its step twice: the shuffle runs again without it.
#[derive(Debug)]
struct Dead {
    node: usize,
    why: String,
}

impl std::fmt::Display for Dead {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "node {} dropped out of the shuffle: {}", self.node, self.why) }
}

impl std::error::Error for Dead {}

/// A shuffle on one node: its plan, and the buckets it keeps for the others.
struct Job {
    ctx: SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    exchanges: Vec<Arc<dyn ExecutionPlan>>, // the hash exchanges below the cut, bottom-up
    cut: Option<Arc<dyn ExecutionPlan>>,     // the first gather on the plan's path down (None: nodes run it all)
    shape: String,
    id: String,
    buckets: Mutex<HashMap<(usize, usize), crate::spill::Spill>>, // (exchange, node) -> its rows for that node
    at: std::time::Instant,
    done: std::sync::atomic::AtomicBool, // its last step ran here (others may still fetch)
}

static JOBS: LazyLock<Mutex<HashMap<String, Arc<Job>>>> = LazyLock::new(Default::default);

impl Drop for Job {
    /// What the job spilled goes with it (the coordinator clears its own as soon as the last step
    /// is in; the others' go when their job is forgotten, a minute later).
    fn drop(&mut self) { crate::spill::clear(&self.id) }
}

/// Forget one shuffle here and delete what it spilled (`GET /cluster/shuffle?drop=1`: the
/// coordinator gave up on it, so nobody will fetch from it again).
pub fn forget(id: &str) {
    JOBS.lock().unwrap().remove(id);
    crate::spill::clear(id);
}

/// Forget finished shuffles (and delete what they spilled). Runs every half minute on every node,
/// so a node that took part in a shuffle frees its scratch without being asked.
pub fn gc() {
    let mut jobs = JOBS.lock().unwrap();
    jobs.retain(|_, j| j.at.elapsed().as_secs() < if j.done.load(std::sync::atomic::Ordering::Relaxed) { 30 } else { 600 });
    drop(jobs);
    crate::spill::sweep();
}

impl Job {
    /// Plan the slice's query; None if it can't be split into shuffle stages.
    async fn open(lake: &Lake, s: &Slice) -> Result<Option<Arc<Job>>> {
        let (ctx, plan) = plan(lake, s).await?;
        let mut cut = None;
        let mut p = plan.clone();
        while let [child] = p.children()[..] {
            if gathers(&p) {
                cut = Some(p.clone());
                break;
            }
            if hashed(&p) {
                break;
            }
            p = child.clone();
        }
        let region = cut.as_ref().map_or(plan.clone(), |c| c.children()[0].clone());
        let mut exchanges = vec![];
        // What reaches the coordinator must be split between the nodes (a whole copy from each
        // would count everything once per node).
        if !matches!(spread(&region, &mut exchanges), Some(Spread::Split | Spread::Keyed)) {
            return Ok(None); // (no exchanges is fine: every node runs its share, the coordinator merges)
        }
        let id = s.shuffle.as_ref().expect("a shuffle").id.clone();
        let job = Arc::new(Job { id: id.clone(), ctx, plan, exchanges, cut, shape: shape(&region), buckets: Default::default(), at: std::time::Instant::now(), done: Default::default() });
        let mut jobs = JOBS.lock().unwrap();
        // (finished ones after a minute, abandoned ones after ten)
        jobs.retain(|_, j| j.at.elapsed().as_secs() < if j.done.load(std::sync::atomic::Ordering::Relaxed) { 30 } else { 600 });
        jobs.insert(id, job.clone());
        Ok(Some(job))
    }
}

/// How a plan's rows are spread over the nodes when each node runs it on its share.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Spread {
    Whole, // every node has all of them (a table read whole)
    Split, // each node has its own (a table sliced)
    Keyed, // …and a key's rows are all on one node (after a hash exchange)
}

/// The spread of `p`'s output if every operator in it stays correct run this way, else None.
/// Collects the hash exchanges (shuffles), bottom-up.
fn spread(p: &Arc<dyn ExecutionPlan>, exchanges: &mut Vec<Arc<dyn ExecutionPlan>>) -> Option<Spread> {
    use Spread::*;
    if p.name() == "ShareExec" {
        return Some(Split);
    }
    let kids = p.children().into_iter().map(|c| spread(c, exchanges)).collect::<Option<Vec<_>>>()?;
    let line = displayable(p.as_ref()).one_line().to_string();
    let has = |s: &str| line.contains(s);
    let one = kids.first().copied();
    let single = p.children().first().is_some_and(|c| c.output_partitioning().partition_count() == 1);
    let join = |l: Spread, r: Spread, partitioned: bool| match (l, r, partitioned) {
        (Whole, Whole, _) => Some(Whole),
        (Keyed, Keyed, true) => Some(Keyed), // (both sides shuffled by the join key)
        (Whole, x, false) => Some(x),        // a broadcast build side
        (x, Whole, false) => Some(if x == Keyed { Split } else { x }),
        _ => None,
    };
    if hashed(p) && one? != Whole {
        exchanges.push(p.clone()); // (over a whole copy it stays within the node: shuffled, each row would arrive N times)
        return Some(Keyed);
    }
    if !kids.is_empty() && kids.iter().all(|k| *k == Whole) {
        return Some(Whole); // (every node computes the same)
    }
    match p.name() {
        "DataSourceExec" | "EmptyExec" | "PlaceholderRowExec" => Some(Whole),
        "ProjectionExec" | "FilterExec" | "CoalesceBatchesExec" | "CooperativeExec" | "LocalLimitExec" => one,
        "CoalescePartitionsExec" | "SortPreservingMergeExec" => one, // (within a node)
        "RepartitionExec" => one,
        "AggregateExec" if has("mode=Partial,") => one,
        "AggregateExec" if has("mode=FinalPartitioned,") || has("mode=SinglePartitioned,") => matches!(one?, Keyed | Whole).then_some(one?),
        "AggregateExec" => (one? == Whole).then_some(Whole), // (a final aggregate over all rows)
        "HashJoinExec" if has("join_type=Inner") => join(kids[0], kids[1], has("mode=Partitioned,")),
        "SortMergeJoinExec" | "SortMergeJoin" if has("join_type=Inner") => join(kids[0], kids[1], true),
        "CrossJoinExec" | "NestedLoopJoinExec" if !has("join_type=") || has("join_type=Inner") => join(kids[0], kids[1], false),
        "SortExec" | "SortExec(TopK)" if has("preserve_partitioning=[true]") => one,
        "SortExec" | "SortExec(TopK)" | "GlobalLimitExec" => (one? == Whole).then_some(Whole),
        "BoundedWindowAggExec" | "WindowAggExec" => match single {
            true => (one? == Whole).then_some(Whole), // (a window over all rows)
            false => matches!(one?, Keyed | Whole).then_some(one?),
        },
        _ => None,
    }
}

/// The plan's shape above its scans, which every node must plan alike.
fn shape(p: &Arc<dyn ExecutionPlan>) -> String {
    fn scan(p: &Arc<dyn ExecutionPlan>) -> bool {
        let leafish = matches!(p.name(), "DataSourceExec" | "EmptyExec" | "UnionExec" | "FilterExec" | "ProjectionExec" | "CoalesceBatchesExec" | "CooperativeExec");
        p.name() == "ShareExec" || (leafish || p.name() == "RepartitionExec" && !hashed(p)) && p.children().into_iter().all(scan)
    }
    if scan(p) {
        return format!("scan{:?}", p.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
    }
    let what = match p.name() {
        "RepartitionExec" => p.output_partitioning().to_string(),
        "AggregateExec" | "HashJoinExec" | "SortMergeJoinExec" | "SortExec" | "SortExec(TopK)" | "BoundedWindowAggExec" => displayable(p.as_ref()).one_line().to_string(),
        name => name.to_string(),
    };
    format!("{}[{}]", what.trim(), p.children().iter().map(|c| shape(c)).collect::<Vec<_>>().join(", "))
}

/// One step of a shuffle on this node: the stage below exchange `step` (or, after the last
/// exchange, the last stage), over the buckets for this node of the exchanges right below it.
async fn step(lake: &Lake, s: &Slice, sh: &Shuffle) -> Result<(String, Vec<Spill>)> {
    let open = JOBS.lock().unwrap().get(&sh.id).cloned();
    let job = match (open, sh.step) {
        (Some(job), _) => job,
        (None, 0) => Job::open(lake, s).await?.context("can't shuffle this plan here")?,
        (None, _) => bail!("shuffle expired"),
    };
    let last = sh.step == job.exchanges.len();
    let top = match last {
        true => job.cut.as_ref().map_or(job.plan.clone(), |c| c.children()[0].clone()),
        false => job.exchanges[sh.step].children()[0].clone(),
    };
    let mut inputs = vec![];
    collect_inputs(&top, &job.exchanges, &mut inputs);
    let mut replaced = vec![];
    for x in inputs {
        let k = job.exchanges.iter().position(|e| Arc::ptr_eq(e, &x)).context("an unknown exchange")?;
        let schema = x.children()[0].schema();
        // Each node's bucket for this one, read a piece at a time (they are on disk past 64 MB).
        let fetches = sh.nodes.iter().enumerate().map(|(i, node)| fetch(&job, &sh.id, node, i == sh.me, k, sh.me));
        let pieces = futures::future::try_join_all(fetches).await?.into_iter().flat_map(|s| s.pieces(schema.clone())).collect();
        let input = StreamingTableExec::try_new(schema, pieces, None, [], false, None)?;
        replaced.push((x.clone(), Arc::new(input) as Arc<dyn ExecutionPlan>));
    }
    let top = top.transform_down(|p| match replaced.iter().find(|(x, _)| Arc::ptr_eq(x, &p)) {
        Some((_, input)) => Ok(Transformed::yes(replace_children_if_necessary(p, vec![input.clone()])?)),
        None => Ok(Transformed::no(p)),
    })?.data;
    if last {
        let out = drain(&top, job.ctx.task_ctx(), &sh.id, &format!("out-{}", sh.step)).await?;
        job.done.store(true, std::sync::atomic::Ordering::Relaxed); // (the sweep frees its scratch shortly)
        return Ok((job.shape.clone(), out));
    }
    // Split by the exchange's hash, one bucket per node (the same hash on every node). A bucket
    // past 64 MB goes to this node's disk as it is filled, so a shuffle isn't bounded by memory.
    let Partitioning::Hash(exprs, _) = job.exchanges[sh.step].output_partitioning().clone() else { bail!("not a hash exchange") };
    let made = scatter(&top, job.ctx.task_ctx(), &sh.id, sh.step, Partitioning::Hash(exprs, sh.nodes.len())).await?;
    let mut buckets = job.buckets.lock().unwrap();
    for (to, spill) in made.into_iter().enumerate() {
        buckets.insert((sh.step, to), spill);
    }
    Ok((job.shape.clone(), vec![]))
}

/// Every partition of `plan` run and split by `hash` into one bucket per node — as it runs, so a
/// step's output is bounded by this node's disk, not by its memory. The partitions run at once,
/// each filling buckets of its own; they are joined at the end, in partition order, so every node
/// splits the same rows the same way whatever order they finish in.
async fn scatter(plan: &Arc<dyn ExecutionPlan>, ctx: Arc<datafusion::execution::TaskContext>, job: &str, step: usize, hash: Partitioning) -> Result<Vec<Spill>> {
    use datafusion::physical_plan::repartition::BatchPartitioner;
    let (dir, nodes) = (crate::spill::dir(job), hash.partition_count());
    let runs = (0..plan.output_partitioning().partition_count()).map(|p| {
        let (plan, ctx, dir, hash) = (plan.clone(), ctx.clone(), dir.clone(), hash.clone());
        async move {
            let mut into: Vec<Spill> = (0..nodes).map(|to| Spill::new(dir.clone(), format!("{step}-{to}-p{p}"))).collect();
            let (mut split, mut rows) = (BatchPartitioner::try_new(hash, Default::default(), p, 1)?, plan.execute(p, ctx)?);
            while let Some(b) = futures::StreamExt::next(&mut rows).await {
                let mut wrote = Ok(());
                split.partition(b?, |to, b| {
                    wrote = wrote.as_ref().map_err(|e: &anyhow::Error| anyhow::anyhow!("{e:#}")).and_then(|_| into[to].push(b));
                    Ok(())
                })?;
                wrote?;
            }
            Ok::<_, anyhow::Error>(into)
        }
    });
    let mut out: Vec<Spill> = (0..nodes).map(|to| Spill::new(dir.clone(), format!("{step}-{to}"))).collect();
    for made in futures::future::try_join_all(runs).await? {
        for (to, s) in made.into_iter().enumerate() {
            out[to].absorb(s)?;
        }
    }
    skew(&out);
    Ok(out)
}

/// How uneven the buckets came out: the biggest against the average (1 = even). Rows are dealt by
/// the hash of their key, so a key that holds a lot of the table leaves one node with most of the
/// work — the query still answers (a bucket is bounded by disk, not memory), just not in parallel.
/// `pondra_shuffle_skew` is where that shows, and `GROUP BY` mostly avoids it: every node
/// aggregates its own rows before the exchange, so a hot key crosses as one row per node.
fn skew(buckets: &[Spill]) {
    let sizes: Vec<u64> = buckets.iter().map(|s| s.bytes()).collect();
    let total: u64 = sizes.iter().sum();
    let (Some(&worst), true) = (sizes.iter().max(), total > 0) else { return };
    let ratio = worst as f64 * sizes.len() as f64 / total as f64;
    let was = crate::metrics::SKEW.load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::SKEW.store(was.max((ratio * 100.0) as u64), std::sync::atomic::Ordering::Relaxed);
}

/// The shuffles right below `p` (not below another one).
fn collect_inputs(p: &Arc<dyn ExecutionPlan>, exchanges: &[Arc<dyn ExecutionPlan>], out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    for c in p.children() {
        match exchanges.iter().any(|x| Arc::ptr_eq(x, c)) {
            true => out.push(c.clone()),
            false => collect_inputs(c, exchanges, out),
        }
    }
}

/// Node `from`'s bucket of exchange `k` for node `to`. A remote one is streamed onto this node's
/// disk piece by piece, never held whole in memory. Buckets are kept until the job ends, not
/// taken, so a step that has to be retried can read them again.
async fn fetch(job: &Job, id: &str, from: &str, local: bool, k: usize, to: usize) -> Result<crate::spill::Spill> {
    if local {
        return Ok(job.buckets.lock().unwrap().get(&(k, to)).cloned().unwrap_or_default());
    }
    let res = crate::cluster::http().get(format!("http://{from}/cluster/shuffle?id={id}&exchange={k}&to={to}")).send().await?;
    ensure!(res.status().is_success(), "{from}: {}", res.text().await?);
    let name = format!("in-{k}-{}", from.replace(':', "_"));
    crate::spill::Spill::receive(crate::spill::dir(id), name, res.bytes_stream()).await
}

/// `GET /cluster/shuffle`: a bucket this node keeps for another, sent a piece at a time.
pub fn bucket(id: &str, exchange: usize, to: usize) -> Result<crate::spill::Spill> {
    let job = JOBS.lock().unwrap().get(id).cloned().context("shuffle expired")?;
    let bucket = job.buckets.lock().unwrap().get(&(exchange, to)).cloned();
    Ok(bucket.unwrap_or_default())
}

// ---------------------------------------------------------------- SQL

/// One plain SELECT with inner joins only (then per-slice results combine exactly).
fn spreadable(sql: &str) -> bool {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    let Ok(stmts) = Parser::parse_sql(&GenericDialect {}, sql) else { return false };
    let [Statement::Query(q)] = &stmts[..] else { return false };
    let SetExpr::Select(s) = q.body.as_ref() else { return false };
    let inner = s.from.iter().flat_map(|t| &t.joins).all(|j| matches!(j.join_operator, JoinOperator::Join(_) | JoinOperator::Inner(_)));
    q.with.is_none() && inner && sql.to_lowercase().matches("select").count() == 1
}

/// The tables a (spreadable) query reads, in order, as often as it reads them.
fn tables(sql: &str) -> Result<Vec<String>> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let [Statement::Query(q)] = &stmts[..] else { bail!("not a query") };
    let SetExpr::Select(s) = q.body.as_ref() else { bail!("not a SELECT") };
    let relations = s.from.iter().flat_map(|t| std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)));
    let names: Vec<String> = relations.map(|r| match r {
        TableFactor::Table { name, .. } => Ok(name.to_string().trim_matches('"').to_string()),
        _ => bail!("not a table"),
    }).collect::<Result<_>>()?;
    ensure!(!names.is_empty(), "no table");
    Ok(names)
}

// ---------------------------------------------------------------- a node's share of a table

/// A node's share of a sliced table: its scan, with 2+ partitions (so aggregations plan as
/// partial + final everywhere), reporting the whole table's size, so every node plans its query
/// (join order, build sides) as for the whole table, whatever its share holds.
#[derive(Debug)]
pub struct ShareExec {
    input: Arc<dyn ExecutionPlan>,
    table: String,
    stats: Arc<datafusion::common::Statistics>,
}

impl ShareExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table: &str, rows: u64, bytes: u64) -> datafusion::error::Result<ShareExec> {
        use datafusion::common::stats::Precision;
        let input = match input.output_partitioning().partition_count() < 2 {
            true => datafusion::physical_plan::union::UnionExec::try_new(vec![input.clone(), Arc::new(datafusion::physical_plan::empty::EmptyExec::new(input.schema()))])?,
            false => input,
        };
        let mut stats = datafusion::common::Statistics::new_unknown(&input.schema());
        (stats.num_rows, stats.total_byte_size) = (Precision::Inexact(rows as usize), Precision::Inexact(bytes as usize));
        Ok(ShareExec { input, table: table.into(), stats: Arc::new(stats) })
    }
}

impl datafusion::physical_plan::DisplayAs for ShareExec {
    fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "ShareExec: table={}", self.table) }
}

impl ExecutionPlan for ShareExec {
    fn name(&self) -> &str { "ShareExec" }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> { self.input.properties() }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn maintains_input_order(&self) -> Vec<bool> { vec![true] }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> { vec![false] }
    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion>) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ShareExec { input: children[0].clone(), table: self.table.clone(), stats: self.stats.clone() }))
    }
    fn execute(&self, partition: usize, ctx: Arc<datafusion::execution::TaskContext>) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> { self.input.execute(partition, ctx) }
    fn statistics_from_inputs(&self, _: &[Arc<datafusion::common::Statistics>], args: &datafusion::physical_plan::statistics::StatisticsArgs) -> datafusion::error::Result<Arc<datafusion::common::Statistics>> {
        Ok(match args.partition() {
            None => self.stats.clone(),
            Some(_) => Arc::new(datafusion::common::Statistics::new_unknown(&self.schema())),
        })
    }
    fn gather_filters_for_pushdown(&self, _: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase, parent_filters: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>, _: &datafusion::common::config::ConfigOptions) -> datafusion::error::Result<datafusion::physical_plan::filter_pushdown::FilterDescription> {
        datafusion::physical_plan::filter_pushdown::FilterDescription::from_children(parent_filters, &self.children())
    }
    fn handle_child_pushdown_result(&self, _: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase, result: datafusion::physical_plan::filter_pushdown::ChildPushdownResult, _: &datafusion::common::config::ConfigOptions) -> datafusion::error::Result<datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation::if_all(result))
    }
}
