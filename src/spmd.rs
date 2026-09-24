//! Distributed queries, SPMD style. Every live node runs the same SQL over its own slice of the
//! data, reading its files straight from the bucket, and the node that received the query (the
//! coordinator) finishes the plan over everyone's results. No coordinator service and no
//! scheduler: every node plans the same query the same way, so the plans' exchanges line up.
//! Two ways, chosen from DataFusion's own parallel plan:
//!
//! - **Gather.** The query's biggest append table is sliced; every other table is read whole by
//!   each node (broadcast, so star joins work). Each node runs the plan up to its first exchange —
//!   for an aggregation, the partial aggregate — and the coordinator merges.
//! - **Shuffle.** When the plan exchanges rows — a GROUP BY with many groups, a join of two big
//!   tables — each exchange becomes a step between nodes: every node runs a stage over its inputs
//!   and splits its output by the exchange's hash, a bucket per node and partition; the next stage
//!   on node j reads bucket j from every node, in node order. A final aggregate over rows spread
//!   across the nodes (a scalar subquery's `avg`) sends every node all the partial aggregates
//!   instead, and a table read whole that has to meet a sliced one by key keeps each node's own
//!   keys of it. The last stage's results go to the coordinator.
//!
//! Which queries: any single query — joins of every kind, subqueries, CTEs, unions — over this
//! lake's tables; keyed tables are read whole, at the coordinator's snapshot. Whether a plan may
//! be split is decided operator by operator (`spread`): only what stays correct run this way runs
//! this way, and anything else runs on one node.
use crate::manifest::Manifest;
use crate::query::{session, Pruned};
use crate::spill::Spill;
use crate::store::*;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::physical_planning_context::{ScalarSubqueryResults, SubqueryIndex};
use datafusion::physical_plan::execution_plan::replace_children_if_necessary;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties, Partitioning};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

/// Tables smaller than this aren't worth spreading (`PONDRA_SPREAD_MB`, 256).
fn spread_bytes() -> u64 { mb("PONDRA_SPREAD_MB", 256) }

/// In a shuffle, tables smaller than this are read whole by every node, so joins broadcast them
/// (`PONDRA_BROADCAST_MB`, 64; 0 slices every table).
fn broadcast_bytes() -> u64 { mb("PONDRA_BROADCAST_MB", 64) }

fn mb(var: &str, default: u64) -> u64 { std::env::var(var).ok().and_then(|v| v.parse().ok()).unwrap_or(default) << 20 }

/// One table's share on one node: manifests and files, and maybe the log tail (after..=upto).
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Part {
    pub table: String,
    pub manifests: Vec<Manifest>,
    pub files: Vec<DataFile>,
    pub tail: Option<(u64, u64)>,
    /// Sliced by a key's range (`ranges.rs`): only the rows in it, whatever the files hold.
    #[serde(default)]
    pub range: Option<crate::ranges::Range>,
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
    /// The tables it reads whole, as the coordinator saw them, and how far into the log: every
    /// node reads the very same rows of them, whatever commit its own catalog is at.
    #[serde(default)]
    pub whole: Vec<(String, TableMeta)>,
    #[serde(default)]
    pub upto: u64,
    /// The coordinator's partitions per query: every node plans with as many, whatever its cores,
    /// or their plans' exchanges wouldn't line up.
    #[serde(default)]
    pub partitions: usize,
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
    pub how: How,
    /// This step's partitions too big for one node, shared out (`skew.rs`).
    #[serde(default)]
    pub splits: Vec<crate::skew::Split>,
}

/// The ways a shuffle is tried, cheapest first.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
pub enum How {
    Ranged,      // as Broadcast, the big tables sliced by one key's ranges (`ranges.rs`)
    Broadcast,   // small tables read whole by every node; joins as DataFusion plans them
    Partitioned, // …and every join shuffles both sides by its key (two big tables meet)
    Sliced,      // every append table sliced (a small table on the kept side of an outer join)
}

/// Run `sql` across `nodes` (this node is `me`), or None if it should just run here.
pub async fn query(lake: &Lake, nodes: &[String], me: &str, sql: &str, force: bool) -> Result<Option<Vec<RecordBatch>>> {
    if nodes.len() < 2 {
        return Ok(None);
    }
    let Some(tables) = tables(sql) else { return Ok(refused("not a single query")) };
    // The table to slice is the biggest append table it reads. Keyed tables are read whole: a
    // key's versions are spread over the files, so a share of the files isn't a share of the rows.
    let mut main: Option<(String, TableMeta, u64)> = None;
    for t in &tables {
        let Some(meta) = lake.cat.get::<TableMeta>(&table_key(t)).await?.filter(|m| m.key.is_empty()) else { continue };
        let bytes = size(&meta).1;
        if main.as_ref().is_none_or(|m| bytes > m.2) {
            main = Some((t.clone(), meta, bytes));
        }
    }
    let Some((main, meta, bytes)) = main else { return Ok(refused("no append table to slice")) };
    let files = size(&meta).0;
    if files == 0 || (!force && (files < nodes.len() || bytes < spread_bytes())) {
        return Ok(None); // (`force`: spread anyway, for tests)
    }
    let mine = nodes.iter().position(|n| n == me).context("not a member")?;
    let parts = deal(lake, &meta, &main, nodes.len()).await?;
    let id = uuid::Uuid::new_v4().to_string(); // (the folder every node spills this query's results into)
    let (whole, upto) = (whole(lake, &tables, &[&main]).await?, lake.visible());
    let slice = |parts| Slice { sql: sql.into(), parts, shuffle: None, id: id.clone(), whole: whole.clone(), upto, partitions: partitions() };
    let slices: Vec<Slice> = parts.into_iter().map(|p| slice(vec![p])).collect();
    let (ctx, plan) = plan(lake, &slices[mine]).await?;
    // Gather when everything below the plan's first gather splits over the nodes as it is (each
    // node's share of the main table, the others whole); shuffle when it takes exchanges.
    let cut = find_cut(&plan).filter(|c| {
        let mut exchanges = vec![]; // (a gather has none: they take a shuffle)
        !hashed(c) && spread(&c.children()[0], &mut exchanges) == Some(Spread::Split) && exchanges.is_empty()
    });
    let Some(cut) = cut else {
        return match shuffle(lake, nodes, me, sql, &tables, &main).await? {
            Some(rows) => Ok(Some(rows)),
            None => Ok(refused("no split of its plan is correct")),
        };
    };
    // Gather: every node computes its partial result at the same time (this one: its files and,
    // as one more slice, the log tail).
    let tail = slice(vec![Part { table: main, tail: Some((meta.tiered, upto)), ..Default::default() }]);
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
    let schema = cut.map_or_else(|| plan.schema(), |c| c.children()[0].schema());
    ensure!(parts.iter().filter_map(|s| s.schema()).all(|s| s.fields() == schema.fields()), "nodes planned the query differently: {:?} / {:?}", schema, parts.iter().filter_map(|s| s.schema()).find(|s| s.fields() != schema.fields()));
    // A merge of sorted runs takes each node's partitions as they are; anything else reads them
    // as one partition, in node order, so the rows are combined in the same order every time.
    let parts = match cut.is_some_and(|c| c.name() == "SortPreservingMergeExec") {
        true => parts.into_iter().map(|s| crate::spill::chain(vec![s], schema.clone())).collect(),
        false => vec![crate::spill::chain(parts, schema.clone())],
    };
    let input = Arc::new(StreamingTableExec::try_new(schema, parts, None, [], false, None)?) as Arc<dyn ExecutionPlan>;
    let Some(cut) = cut else { return Ok(datafusion::physical_plan::collect(input, ctx.task_ctx()).await?) };
    let plan = plan.clone().transform_down(|p| Ok(if Arc::ptr_eq(&p, cut) { Transformed::yes(replace_children_if_necessary(p, vec![input.clone()])?) } else { Transformed::no(p) }))?.data;
    Ok(datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?)
}

/// The tables of this lake a query reads that aren't sliced, as this node sees them now.
async fn whole(lake: &Lake, tables: &[String], sliced: &[&str]) -> Result<Vec<(String, TableMeta)>> {
    let mut out = vec![];
    for t in tables.iter().filter(|t| !sliced.contains(&t.as_str())) {
        if let Some(meta) = lake.cat.get::<TableMeta>(&table_key(t)).await? {
            out.push((t.clone(), meta));
        }
    }
    Ok(out)
}

/// A table's (files, bytes): its inline files and its sealed manifests.
fn size(meta: &TableMeta) -> (usize, u64) {
    let sealed = meta.sealed.clone().unwrap_or_default();
    (sealed.files as usize + meta.files.len(), sealed.bytes + meta.files.iter().map(|f| f.bytes).sum::<u64>())
}

/// A table's (rows, bytes) as the catalog has them (the log tail not counted).
fn totals(meta: &TableMeta) -> (u64, u64) {
    let sealed = meta.sealed.clone().unwrap_or_default();
    (sealed.rows + meta.files.iter().map(|f| f.rows).sum::<u64>(), size(meta).1)
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
    read_reply(res, &s.job(), &format!("from-{}", node.replace(':', "_"))).await
}

/// A `reply` read back onto this node's disk, a piece at a time.
async fn read_reply(res: reqwest::Response, job: &str, name: &str) -> Result<(String, Vec<Spill>)> {
    let mut frames = crate::spill::Frames::new(res.bytes_stream());
    let shape = String::from_utf8(frames.next().await?.context("an empty reply")?)?;
    let (dir, mut parts) = (crate::spill::dir(job), vec![]);
    for p in 0..frames.count().await? {
        let pieces = frames.count().await?;
        parts.push(Spill::take(dir.clone(), format!("{name}-{p}"), &mut frames, pieces).await?);
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
    if !s.whole.is_empty() || s.parts.iter().any(|p| p.tail.is_some()) {
        // The whole tables, and log tails, as the coordinator saw them: this node must have seen
        // the log that far.
        let mut hwm = lake.hwm.subscribe();
        let _ = tokio::time::timeout(Duration::from_secs(10), async { while lake.visible() < s.upto && hwm.changed().await.is_ok() {} }).await;
        ensure!(lake.visible() >= s.upto, "this node is behind the lake ({} < {})", lake.visible(), s.upto);
        for (t, meta) in &s.whole {
            let inner = crate::query::table_view(lake, &ctx, t, meta, Some(s.upto)).await?;
            ctx.deregister_table(t.as_str())?;
            ctx.register_table(t.as_str(), Arc::new(WholeTable { inner, name: t.clone(), size: totals(meta) }))?;
        }
    }
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        let o = state.config_mut().options_mut();
        if s.partitions > 0 {
            o.execution.target_partitions = s.partitions;
        }
        // Always aggregate before the exchange: partial results cross the network, so passing
        // raw rows through (DataFusion's shortcut for high-cardinality groups) would ship the table.
        o.execution.skip_partial_aggregation_probe_rows_threshold = usize::MAX;
        if let Some(sh) = &s.shuffle {
            o.optimizer.enable_dynamic_filter_pushdown = false; // (a join's filter would reach a scan of an earlier step)
            if !matches!(sh.how, How::Ranged | How::Broadcast) {
                // Every join shuffles both sides (a broadcast side, if sliced, would be partial).
                (o.optimizer.hash_join_single_partition_threshold, o.optimizer.hash_join_single_partition_threshold_rows) = (0, 0);
            }
        }
    }
    for p in &s.parts {
        let meta: TableMeta = lake.cat.get(&table_key(&p.table)).await?.context("no table")?;
        let (after, upto) = p.tail.unwrap_or((0, 0)); // (0, 0): no tail
        let schema = crate::query::read_schema(&meta.columns)?;
        let share = Some(totals(&meta));
        // The whole table's ranges, not this slice's: every node has to plan the query alike.
        let ranges = crate::manifest::ranges(&p.table, &crate::manifest::list(lake, &meta).await?, &meta.files, &schema);
        let meta = TableMeta { files: p.files.clone(), tiered: after, ..meta };
        let table = Pruned { lake: lake.arc(), name: p.table.clone(), meta, manifests: Some(p.manifests.clone()), upto: Some(upto), schema, share, ranges, range: p.range.clone() };
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
async fn shuffle(lake: &Lake, nodes: &[String], me: &str, sql: &str, tables: &[String], main: &str) -> Result<Option<Vec<RecordBatch>>> {
    let mut live: Vec<String> = nodes.to_vec();
    for _ in 0..3 {
        let mine = live.iter().position(|n| n == me).context("not a member")?;
        let out = spread_once(lake, &live, mine, sql, tables, main).await;
        let Err(e) = out else { return out };
        // (this node failing its own step, or too few nodes left: the query runs here instead)
        let Some(dead) = e.downcast_ref::<Dead>().map(|d| d.node).filter(|&d| live.len() > 2 && d != mine) else {
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
    let asks = nodes.iter().filter(|n| *n != me).map(|n| crate::cluster::http().get(format!("http://{n}/cluster/shuffle?id={id}&exchange=0&to=0&drop=true")).send());
    futures::future::join_all(asks).await;
}

/// The id of the shuffle this node started last (for `abandon`).
static LAST: LazyLock<Mutex<Option<String>>> = LazyLock::new(Default::default);

/// One attempt, planned each way in turn (`How`) until one splits correctly. Keyed tables, and
/// tables of attached lakes, are always read whole.
async fn spread_once(lake: &Lake, nodes: &[String], mine: usize, sql: &str, tables: &[String], main: &str) -> Result<Option<Vec<RecordBatch>>> {
    for how in [How::Ranged, How::Broadcast, How::Partitioned, How::Sliced] {
        let upto = lake.visible();
        // Its append tables (the biggest first), as of now.
        let mut appends: Vec<(String, TableMeta)> = vec![];
        for t in tables {
            if let Some(meta) = lake.cat.get::<TableMeta>(&table_key(t)).await?.filter(|m| m.key.is_empty()) {
                appends.insert(if t == main { 0 } else { appends.len() }, (t.clone(), meta));
            }
        }
        // By ranges of a key, every table that has it, small ones too: a small table cut by the
        // same ranges meets a big one on the key where it is (else it is read whole, as below).
        let scheme = match how {
            How::Ranged => match crate::ranges::scheme(lake, sql, &appends, nodes.len()).await? {
                Some(s) => Some(s),
                None => {
                    refused::<()>("no key its biggest table's files hold narrow ranges of");
                    continue;
                }
            },
            _ => None,
        };
        let cut = |t: &String, meta: &TableMeta| how == How::Sliced || t == main || size(meta).1 >= broadcast_bytes() || scheme.as_ref().is_some_and(|s| s.columns.contains_key(t));
        let big: Vec<(String, TableMeta)> = appends.into_iter().filter(|(t, m)| cut(t, m)).collect();
        let id = uuid::Uuid::new_v4().to_string();
        *LAST.lock().unwrap() = Some(id.clone());
        let shuffle = |me| Some(Shuffle { id: id.clone(), nodes: nodes.to_vec(), me, step: 0, how, splits: vec![] });
        let mut slices: Vec<Slice> = (0..nodes.len()).map(|me| Slice { sql: sql.into(), parts: vec![], shuffle: shuffle(me), id: id.clone(), whole: vec![], upto, partitions: partitions() }).collect();
        for (t, meta) in &big {
            let parts = match scheme.as_ref().filter(|s| s.columns.contains_key(t)) {
                Some(s) => crate::ranges::parts(lake, meta, t, s, nodes.len(), (meta.tiered, upto)).await?, // (every node reads the log tail through its range)
                None => deal(lake, meta, t, nodes.len()).await?.into_iter().enumerate().map(|(i, p)| Part { tail: (i == mine).then_some((meta.tiered, upto)), ..p }).collect(), // (the log tail here)
            };
            for (i, p) in parts.into_iter().enumerate() {
                slices[i].parts.push(p);
            }
        }
        let sliced: Vec<&str> = slices[mine].parts.iter().map(|p| p.table.as_str()).collect();
        let whole = whole(lake, tables, &sliced).await?;
        slices.iter_mut().for_each(|s| s.whole = whole.clone());
        if let Some(job) = Job::open(lake, &slices[mine]).await? {
            if std::env::var_os("PONDRA_DEBUG_SPREAD").is_some() {
                let over = job.exchanges.iter().map(|x| displayable(x.plan.as_ref()).one_line().to_string().trim().to_string()).collect::<Vec<_>>();
                eprintln!("spread: shuffled {how:?}{}: {over:?}", scheme.map(|s| format!(", by ranges of {:?}", s.columns)).unwrap_or_default());
            }
            return run(lake, nodes, mine, &slices, job).await.map(Some); // (step 0 here uses this plan)
        }
    }
    Ok(None)
}

/// Every step of a shuffle on every node, then the coordinator's finish.
async fn run(lake: &Lake, nodes: &[String], mine: usize, slices: &[Slice], job: Arc<Job>) -> Result<Vec<RecordBatch>> {
    let id = slices[mine].shuffle.as_ref().expect("a shuffle").id.clone();
    let run = async {
        let (mut last, mut sizes): (Vec<Spill>, HashMap<usize, Vec<Vec<Vec<u64>>>>) = (vec![], HashMap::new());
        for step in 0..=job.exchanges.len() {
            let splits = crate::skew::splits(&job.joins, step, &sizes); // (hot partitions of what this step joins)
            crate::metrics::add(&crate::metrics::SKEW_SPLITS, splits.len() as u64);
            let runs = nodes.iter().enumerate().zip(slices).map(|((i, node), s)| {
                let mut s = s.clone();
                let sh = s.shuffle.as_mut().expect("a shuffle");
                (sh.step, sh.splits) = (step, splits.clone());
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
            // (an exchange's step also says how much each node sent to each node's partitions)
            let outs: Vec<(&str, &str, Vec<Spill>)> = outs.iter().map(|(reply, parts)| {
                let (shape, sent) = reply.split_once('\n').unwrap_or((reply, ""));
                (shape, sent, parts.clone())
            }).collect();
            if let Some((other, ..)) = outs.iter().find(|(shape, ..)| *shape != job.shape) {
                refused::<()>(format!("plans differ:\n  here:  {}\n  there: {other}", job.shape));
                bail!("nodes planned the query differently");
            }
            if step < job.exchanges.len() {
                sizes.insert(step, outs.iter().map(|(_, sent, _)| serde_json::from_str(sent).unwrap_or_default()).collect());
            }
            last = outs.into_iter().flat_map(|(.., parts)| parts).collect();
        }
        finish(&job.ctx, &job.plan, job.cut.as_ref(), last).await
    };
    let out = run.await;
    crate::metrics::add(&crate::metrics::SHUFFLED, (out.is_ok() && !job.exchanges.is_empty()) as u64);
    let ranged = slices[mine].shuffle.as_ref().is_some_and(|sh| sh.how == How::Ranged);
    crate::metrics::add(&crate::metrics::RANGED, (out.is_ok() && ranged) as u64);
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
    exchanges: Vec<Exchange>, // the exchanges below the cut, bottom-up
    cut: Option<Arc<dyn ExecutionPlan>>,     // the first gather on the plan's path down (None: nodes run it all)
    shape: String,
    id: String,
    buckets: Mutex<HashMap<(usize, usize), Vec<Spill>>>, // (exchange, node) -> its rows for that node, by partition
    subqueries: Vec<Subquery>, // answered as soon as the exchanges they need are done
    joins: Vec<crate::skew::Join>, // shuffled joins that may share out a hot partition
    at: std::time::Instant,
    done: std::sync::atomic::AtomicBool, // its last step ran here (others may still fetch)
}

/// A scalar subquery a shuffle answers on the way (`hoist`): its plan, where its answer goes, and
/// the step from which it can be worked out (its own exchanges done).
struct Subquery {
    plan: Arc<dyn ExecutionPlan>,
    index: SubqueryIndex,
    results: ScalarSubqueryResults,
    from: usize,
}

static JOBS: LazyLock<Mutex<HashMap<String, Arc<Job>>>> = LazyLock::new(Default::default);

impl Drop for Job {
    /// What the job spilled goes with it (the coordinator clears its own as soon as the last step
    /// is in; the others' go when their job is forgotten, a minute later).
    fn drop(&mut self) { crate::spill::clear(&self.id) }
}

/// Forget one shuffle here and delete what it spilled (`GET /cluster/shuffle?drop=true`: the
/// coordinator gave up on it, so nobody will fetch from it again). It is remembered as dropped for
/// a while: a step of it still on its way here must not open it again and leave its buckets behind.
pub fn forget(id: &str) {
    let mut jobs = JOBS.lock().unwrap(); // (held while it is marked: `Job::open` checks under it)
    let mut dropped = DROPPED.lock().unwrap();
    dropped.retain(|_, at| at.elapsed().as_secs() < 600);
    dropped.insert(id.to_string(), std::time::Instant::now());
    jobs.remove(id);
    drop((dropped, jobs));
    crate::spill::clear(id);
}

static DROPPED: LazyLock<Mutex<HashMap<String, std::time::Instant>>> = LazyLock::new(Default::default);

/// Forget finished shuffles (and delete what they spilled). Runs every half minute on every node,
/// so a node that took part in a shuffle frees its scratch without being asked.
pub fn gc() {
    JOBS.lock().unwrap().retain(|_, j| !j.stale());
    crate::spill::sweep();
}

impl Job {
    /// Plan the slice's query; None if it can't be split into shuffle stages.
    async fn open(lake: &Lake, s: &Slice) -> Result<Option<Arc<Job>>> {
        let (ctx, plan) = plan(lake, s).await?;
        let (plan, hoisted) = hoist(plan)?;
        let plan = by_key(plan, ctx.state().config().target_partitions())?;
        // The subqueries first: their exchanges are the job's first steps, and every step after
        // them can use their answers.
        let (mut exchanges, mut subqueries) = (vec![], vec![]);
        for (plan, index, results) in hoisted {
            if spread(&plan, &mut exchanges) != Some(Spread::Whole) {
                return Ok(refused("a subquery that can't be answered alike on every node"));
            }
            subqueries.push(Subquery { plan, index, results, from: exchanges.len() });
        }
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
        // What reaches the coordinator must be split between the nodes (a whole copy from each
        // would count everything once per node).
        if !matches!(spread(&region, &mut exchanges), Some(Spread::Split | Spread::Keyed | Spread::Ranged)) {
            return Ok(None); // (no exchanges is fine: every node runs its share, the coordinator merges)
        }
        let id = s.shuffle.as_ref().expect("a shuffle").id.clone();
        let which = |p: &Arc<dyn ExecutionPlan>| exchanges.iter().position(|x| Arc::ptr_eq(&x.plan, p)).map(|k| (k, hashed(p) && !exchanges[k].own));
        let joins = crate::skew::joins(&region, &which, exchanges.len());
        let job = Arc::new(Job { id: id.clone(), ctx, plan, exchanges, cut, shape: shape(&region), buckets: Default::default(), subqueries, joins, at: std::time::Instant::now(), done: Default::default() });
        let mut jobs = JOBS.lock().unwrap();
        // (a shuffle given up on while this one was being planned must not be left behind)
        ensure!(!DROPPED.lock().unwrap().contains_key(&id), "shuffle abandoned");
        jobs.retain(|_, j| !j.stale());
        jobs.insert(id, job.clone());
        Ok(Some(job))
    }

    /// Done here for half a minute (the others have fetched what they need), or given up on ten
    /// minutes ago.
    fn stale(&self) -> bool { self.at.elapsed().as_secs() >= if self.done.load(std::sync::atomic::Ordering::Relaxed) { 30 } else { 600 } }
}

/// Why a query runs on one node after all (`PONDRA_DEBUG_SPREAD=1` prints it on stderr).
fn refused<T>(why: impl std::fmt::Display) -> Option<T> {
    if std::env::var_os("PONDRA_DEBUG_SPREAD").is_some() {
        eprintln!("spread: not spread: {why}");
    }
    None
}

/// Where rows go between two steps of a shuffle: a hash exchange sends each row to the node its
/// key belongs to; a gather sends every node all of them (`everywhere`, `collected`).
/// `own`: every node already has all the rows (a table read whole) and keeps just its own keys' —
/// nothing moves, and a small table can meet a sliced one in an outer join by key. `whole`: an
/// all-gather of `plan`'s own output, partition by partition (the other kinds exchange what is
/// below them, standing in for an operator that moves rows).
#[derive(Clone)]
struct Exchange {
    plan: Arc<dyn ExecutionPlan>,
    own: bool,
    whole: bool,
}

/// The plan with its scalar subqueries taken out, and the subqueries (innermost first). An
/// operator that answers them (`ScalarSubqueryExec`) does it when its step runs, and whatever uses
/// the answer may sit in an earlier step, below a shuffle — TPC-H q22 filters customers by an
/// `avg` before shuffling them. So a shuffle answers them itself, on every node, as soon as the
/// exchanges they need are done, and the expressions that use them (which hold the same answer
/// slots) find the answers whichever step they run in.
type Hoisted = (Arc<dyn ExecutionPlan>, SubqueryIndex, ScalarSubqueryResults);

fn hoist(plan: Arc<dyn ExecutionPlan>) -> Result<(Arc<dyn ExecutionPlan>, Vec<Hoisted>)> {
    use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
    let mut found = vec![];
    let plan = plan.transform_up(|p| {
        let Some(s) = p.downcast_ref::<ScalarSubqueryExec>() else { return Ok(Transformed::no(p)) };
        found.extend(s.subqueries().iter().map(|q| (q.plan.clone(), q.index, s.results().clone())));
        Ok(Transformed::yes(s.input().clone()))
    })?;
    Ok((plan.data, found))
}

/// A join that keeps its left side by looking at all of its right one (left, semi, anti), planned
/// to collect the left side, where that doesn't split correctly: shuffled by its key instead, both
/// sides (a side read whole keeps just its own keys' rows: `own`). Without it the whole query goes
/// to the plan that shuffles every join — TPC-H q17's and q21's `EXISTS` moved all of `lineitem`
/// two or three times over for one such join. Only a join that fails as it is changes, so a plan
/// that split before splits the same way now.
fn by_key(plan: Arc<dyn ExecutionPlan>, parts: usize) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion::common::JoinType::*;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::repartition::RepartitionExec;
    Ok(plan.transform_up(|p| {
        let Some(j) = p.downcast_ref::<HashJoinExec>() else { return Ok(Transformed::no(p)) };
        let fails = || spread(j.left(), &mut vec![]).is_some() && spread(j.right(), &mut vec![]).is_some() && spread(&p, &mut vec![]).is_none();
        if *j.partition_mode() != PartitionMode::CollectLeft || !matches!(j.join_type(), Left | LeftSemi | LeftAnti | LeftMark) || j.null_aware || !fails() {
            return Ok(Transformed::no(p));
        }
        let left = if j.left().name() == "CoalescePartitionsExec" { j.left().children()[0] } else { j.left() };
        let hash = |side: &Arc<dyn ExecutionPlan>, keys| RepartitionExec::try_new(side.clone(), Partitioning::Hash(keys, parts)).map(|r| Arc::new(r) as Arc<dyn ExecutionPlan>);
        let sides = vec![hash(left, j.on().iter().map(|k| k.0.clone()).collect())?, hash(j.right(), j.on().iter().map(|k| k.1.clone()).collect())?];
        Ok(Transformed::yes(j.builder().with_partition_mode(PartitionMode::Partitioned).with_new_children(sides)?.recompute_properties().reset_state().build_exec()?))
    })?.data)
}

/// One scalar subquery's answer, worked out from its plan: NULL for no row, an error for two.
async fn scalar(plan: Arc<dyn ExecutionPlan>, ctx: Arc<datafusion::execution::TaskContext>) -> Result<datafusion::common::ScalarValue> {
    let schema = plan.schema();
    let rows = datafusion::physical_plan::collect(plan, ctx).await?;
    let rows: Vec<&RecordBatch> = rows.iter().filter(|b| b.num_rows() > 0).collect();
    Ok(match rows[..] {
        [] => datafusion::common::ScalarValue::try_from(schema.field(0).data_type())?,
        [b] if b.num_rows() == 1 => datafusion::common::ScalarValue::try_from_array(b.column(0), 0)?,
        _ => bail!("a scalar subquery returned more than one row"),
    })
}

/// How a plan's rows are spread over the nodes when each node runs it on its share.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Spread {
    Whole,  // every node has all of them (a table read whole)
    Split,  // each node has its own (a table sliced)
    Keyed,  // …and a key's rows are all on one node (after a hash exchange)
    Ranged, // each node has its own, by ranges of a key the tables share (`ranges.rs`): every row
            // with a given value of it is on one node, whichever table it came from
}

/// The spread of `p`'s output if every operator in it stays correct run this way, else None.
/// Collects the exchanges, bottom-up.
fn spread(p: &Arc<dyn ExecutionPlan>, exchanges: &mut Vec<Exchange>) -> Option<Spread> {
    use Spread::*;
    match p.name() {
        "ShareExec" => return Some(if p.downcast_ref::<ShareExec>()?.range.is_some() { Ranged } else { Split }),
        "WholeExec" => return Some(Whole),
        _ => {}
    }
    let (mut kids, mut below_input) = (vec![], 0..0);
    for (i, c) in p.children().into_iter().enumerate() {
        let before = exchanges.len();
        kids.push(spread(c, exchanges)?);
        if i == 0 {
            below_input = before..exchanges.len();
        }
    }
    let line = displayable(p.as_ref()).one_line().to_string();
    let has = |s: &str| line.contains(s);
    let one = kids.first().copied();
    let single = p.children().first().is_some_and(|c| c.output_partitioning().partition_count() == 1);
    if hashed(p) && one? == Ranged && by_range(p.children()[0], &hash_keys(p)) {
        return Some(Ranged); // (a key's rows are on one node already: this stays within it)
    }
    if hashed(p) && one? != Whole {
        if has("preserve_order=true") {
            return refused(format!("{} (a shuffle keeps no order)", line.trim()));
        }
        exchanges.push(Exchange { plan: p.clone(), own: false, whole: false }); // (over a whole copy it stays within the node: shuffled, each row would arrive N times)
        return Some(Keyed);
    }
    if p.name() == "ScalarSubqueryExec" {
        // Every node works out the same answer to each subquery, and whatever uses the answer must
        // run in the same step as this — not below a shuffle, where it would run before it exists.
        let used_below = exchanges[below_input].iter().any(|x| x.plan.exists(|n| Ok(displayable(n.as_ref()).one_line().to_string().contains("scalar_subquery("))).unwrap_or(true));
        return (kids[1..].iter().all(|k| *k == Whole) && !used_below).then_some(one?).or_else(|| refused(format!("{} over {kids:?}", line.trim())));
    }
    if !kids.is_empty() && kids.iter().all(|k| *k == Whole) {
        return Some(Whole); // (every node computes the same)
    }
    match p.name() {
        "DataSourceExec" | "EmptyExec" | "PlaceholderRowExec" => Some(Whole),
        "ProjectionExec" | "FilterExec" | "CoalesceBatchesExec" | "CooperativeExec" | "LocalLimitExec" => one,
        // (within a node — and a limit within one node, over rows spread across them all, is wrong)
        "CoalescePartitionsExec" | "SortPreservingMergeExec" => (!has("fetch=")).then_some(one?),
        "RepartitionExec" => one,
        "AggregateExec" if has("mode=Partial,") => one,
        "AggregateExec" if has("mode=FinalPartitioned,") || has("mode=SinglePartitioned,") => match one? {
            Keyed | Whole => one,
            Ranged => by_range(p.children()[0], &group_keys(p)).then_some(Ranged), // (grouped by the key, among others)
            Split => None,
        },
        "AggregateExec" => everywhere(p, exchanges), // (a final aggregate over all rows)
        "HashJoinExec" if kids[..] == [Ranged, Ranged] && meets(p) => Some(Ranged), // (collected or partitioned alike)
        "HashJoinExec" if has("mode=Partitioned,") => join(&line, kids[0], kids[1], true).or_else(|| {
            // A side read whole on every node where each node must hold only its own keys (the
            // kept side of an outer, semi or anti join): every node keeps its own share of it.
            let whole = match (kids[0], kids[1]) {
                (Whole, Keyed) => p.children()[0],
                (Keyed, Whole) => p.children()[1],
                _ => return None,
            };
            hashed(whole).then(|| exchanges.push(Exchange { plan: whole.clone(), own: true, whole: false }))?;
            join(&line, Keyed, Keyed, true)
        }),
        "HashJoinExec" | "CrossJoinExec" | "NestedLoopJoinExec" => join(&line, kids[0], kids[1], false).or_else(|| collected(p, &line, &kids, exchanges)),
        "SortMergeJoinExec" | "SortMergeJoin" => join(&line, kids[0], kids[1], true),
        // (every node's own rows of each input: a copy read whole on every node would repeat)
        "UnionExec" => (!kids.contains(&Whole)).then_some(Split),
        "InterleaveExec" => kids.iter().all(|k| *k == Keyed).then_some(Keyed),
        "SortExec" | "SortExec(TopK)" if has("preserve_partitioning=[true]") => one,
        "SortExec" | "SortExec(TopK)" | "GlobalLimitExec" => (one? == Whole).then_some(Whole),
        "BoundedWindowAggExec" | "WindowAggExec" => match single {
            true => (one? == Whole).then_some(Whole), // (a window over all rows)
            false => matches!(one?, Keyed | Whole).then_some(one?),
        },
        _ => None,
    }
    .or_else(|| refused(format!("{} over {kids:?}", line.trim())))
}

/// A join over rows spread across the nodes (the left side builds, the right probes). Whatever a
/// join emits for a row by looking at *all* of the other side — an outer join's unmatched rows, a
/// semi or anti join's answer, a mark — needs that other side whole on every node, or both sides
/// shuffled by the key. Rows it emits only on a match are right either way.
fn join(line: &str, l: Spread, r: Spread, partitioned: bool) -> Option<Spread> {
    use Spread::*;
    let kind = line.split("join_type=").nth(1).map_or("Inner", |t| t.split(|c: char| !c.is_alphanumeric()).next().unwrap_or(""));
    let (left_needs_all_right, right_needs_all_left) = match kind {
        "Inner" => (false, false),
        "Left" | "LeftSemi" | "LeftAnti" | "LeftMark" => (true, false),
        "Right" | "RightSemi" | "RightAnti" | "RightMark" => (false, true),
        "Full" => (true, true),
        _ => return None,
    };
    match (l, r) {
        (Whole, Whole) => Some(Whole),
        // (a key's rows meet on one node — but NOT IN must see a NULL wherever it is)
        (Keyed, Keyed) if partitioned && !line.contains("null_aware") => Some(Keyed),
        (Whole, x) if !left_needs_all_right => Some(x),
        (x, Whole) if !right_needs_all_left => Some(if x == Keyed && !partitioned { Split } else { x }),
        _ => None,
    }
}

/// Whether any of `keys` (expressions over `p`'s output) is a key the tables are sliced by the
/// ranges of, passed through unchanged from the slice (`ShareExec`) — through projections,
/// filters, aggregations' groups and joins. Rows equal in it are then on one node.
fn by_range(p: &Arc<dyn ExecutionPlan>, keys: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>]) -> bool {
    use datafusion::physical_expr::expressions::Column;
    keys.iter().any(|k| k.downcast_ref::<Column>().is_some_and(|c| ranged(p, c.index())))
}

/// Whether column `i` of `p`'s output is the key a slice below it is cut by the ranges of.
fn ranged(p: &Arc<dyn ExecutionPlan>, i: usize) -> bool {
    use datafusion::physical_plan::{aggregates::AggregateExec, filter::FilterExec, joins::HashJoinExec, projection::ProjectionExec};
    let one = |e: &Arc<dyn datafusion::physical_plan::PhysicalExpr>, below: &Arc<dyn ExecutionPlan>| by_range(below, std::slice::from_ref(e));
    if let Some(s) = p.downcast_ref::<ShareExec>() {
        return s.range.as_ref().is_some_and(|r| p.schema().fields().get(i).is_some_and(|f| f.name() == r));
    }
    if let Some(x) = p.downcast_ref::<ProjectionExec>() {
        return x.expr().get(i).is_some_and(|e| one(&e.expr, x.input()));
    }
    if let Some(f) = p.downcast_ref::<FilterExec>() {
        return ranged(f.input(), f.projection().as_ref().map_or(Some(i), |cols| cols.get(i).copied()).unwrap_or(usize::MAX));
    }
    if let Some(a) = p.downcast_ref::<AggregateExec>() {
        return a.group_expr().expr().get(i).is_some_and(|(e, _)| one(e, a.input()));
    }
    if let Some(j) = p.downcast_ref::<HashJoinExec>() {
        use datafusion::common::{JoinSide, JoinType::*};
        let i = j.projection.as_ref().map_or(Some(i), |cols| cols.get(i).copied());
        let (_, columns) = datafusion::physical_plan::joins::utils::build_join_schema(&j.left().schema(), &j.right().schema(), j.join_type());
        // (not a side an outer join pads with NULLs: those NULLs sit wherever the other side's
        // unmatched rows do, not with the first range's)
        return i.and_then(|i| columns.get(i)).is_some_and(|c| match (c.side, j.join_type()) {
            (JoinSide::Left, Right | Full) | (JoinSide::Right, Left | Full) | (JoinSide::None, _) => false,
            (JoinSide::Left, _) => ranged(j.left(), c.index),
            (JoinSide::Right, _) => ranged(j.right(), c.index),
        });
    }
    matches!(p.name(), "CoalesceBatchesExec" | "CooperativeExec" | "RepartitionExec" | "CoalescePartitionsExec" | "SortExec" | "SortPreservingMergeExec") && ranged(p.children()[0], i)
}

/// A join of two sides cut by the same ranges that runs where the rows are: a pair of its keys is
/// the key on both sides, so every row that meets another is on the same node (NOT IN aside: a
/// NULL anywhere empties it, and only the first range holds NULLs).
fn meets(p: &Arc<dyn ExecutionPlan>) -> bool {
    let Some(j) = p.downcast_ref::<datafusion::physical_plan::joins::HashJoinExec>() else { return false };
    !j.null_aware && j.on().iter().any(|(l, r)| by_range(j.left(), std::slice::from_ref(l)) && by_range(j.right(), std::slice::from_ref(r)))
}

/// A hash repartition's expressions; an aggregation's groups.
fn hash_keys(p: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
    match p.output_partitioning() {
        Partitioning::Hash(exprs, _) => exprs.clone(),
        _ => vec![],
    }
}

fn group_keys(p: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
    let Some(a) = p.downcast_ref::<datafusion::physical_plan::aggregates::AggregateExec>() else { return vec![] };
    a.group_expr().expr().iter().map(|(e, _)| e.clone()).collect()
}

/// A join that collects its left side, where that side is spread across the nodes (a small table
/// sliced, or a big one filtered down): every node is sent all of it — DataFusion collects it
/// because it expects it to be small — and joins its own share of the other side against it. A
/// broadcast of a result rather than of a table; without it both sides would be shuffled.
fn collected(p: &Arc<dyn ExecutionPlan>, line: &str, kids: &[Spread], exchanges: &mut Vec<Exchange>) -> Option<Spread> {
    let c = p.children()[0];
    if line.contains("null_aware") && kids[1] != Spread::Whole && !exchanges.iter().any(|x| Arc::ptr_eq(&x.plan, p.children()[1])) {
        // NOT IN: a NULL anywhere in the subquery empties the answer, so every node needs all of
        // it — it is usually small — while each keeps its own share of the other side.
        let out = join(line, kids[0], Spread::Whole, false)?;
        exchanges.push(Exchange { plan: p.children()[1].clone(), own: false, whole: true });
        return Some(out);
    }
    if c.name() != "CoalescePartitionsExec" || kids[0] == Spread::Whole || displayable(c.as_ref()).one_line().to_string().contains("fetch=") {
        return None;
    }
    let out = join(line, Spread::Whole, kids[1], false)?;
    exchanges.push(Exchange { plan: c.clone(), own: false, whole: false });
    Some(out)
}

/// A final aggregate over rows spread across the nodes — a scalar subquery's `avg`, a `max` over
/// groups. What reaches it is partial aggregates, a few rows per node, so every node is sent all
/// of them (an all-gather: the gather below becomes an exchange) and computes the same answer.
fn everywhere(p: &Arc<dyn ExecutionPlan>, exchanges: &mut Vec<Exchange>) -> Option<Spread> {
    let g = p.children()[0];
    let partial = |c: &Arc<dyn ExecutionPlan>| c.name() == "AggregateExec" && displayable(c.as_ref()).one_line().to_string().contains("mode=Partial,");
    if g.name() != "CoalescePartitionsExec" || !partial(g.children()[0]) {
        return None;
    }
    exchanges.push(Exchange { plan: g.clone(), own: false, whole: false });
    Some(Spread::Whole)
}

/// The plan's shape above its scans, which every node must plan alike.
fn shape(p: &Arc<dyn ExecutionPlan>) -> String {
    fn scan(p: &Arc<dyn ExecutionPlan>) -> bool {
        // (how a node reads a table — its partitions, gathered into one or not — depends on what it
        // holds in memory (`hot.rs`); only the exchanges have to line up)
        let leafish = matches!(p.name(), "DataSourceExec" | "EmptyExec" | "UnionExec" | "FilterExec" | "ProjectionExec" | "CoalesceBatchesExec" | "CooperativeExec" | "CoalescePartitionsExec");
        matches!(p.name(), "ShareExec" | "WholeExec") || (leafish || p.name() == "RepartitionExec" && !hashed(p)) && p.children().into_iter().all(scan)
    }
    if scan(p) {
        return format!("scan{:?}", p.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
    }
    let what = match p.name() {
        "RepartitionExec" => p.output_partitioning().to_string(),
        "AggregateExec" | "HashJoinExec" | "SortMergeJoinExec" | "SortExec" | "SortExec(TopK)" | "BoundedWindowAggExec" => displayable(p.as_ref()).one_line().to_string(),
        name => name.to_string(),
    };
    // (a subquery's answer, shown in the line, may differ in its last digit from node to node)
    let what = what.split("scalar_subquery(").enumerate().map(|(i, part)| if i == 0 { part } else { part.split_once(')').map_or(part, |(_, rest)| rest) }).collect::<Vec<_>>().join("scalar_subquery()");
    format!("{}[{}]", what.trim(), p.children().iter().map(|c| shape(c)).collect::<Vec<_>>().join(", "))
}

/// One step of a shuffle on this node: the stage below exchange `step` (or, after the last
/// exchange, the last stage), over the buckets for this node of the exchanges right below it.
async fn step(lake: &Lake, s: &Slice, sh: &Shuffle) -> Result<(String, Vec<Spill>)> {
    ensure!(!DROPPED.lock().unwrap().contains_key(&sh.id), "shuffle abandoned");
    let open = JOBS.lock().unwrap().get(&sh.id).cloned();
    let job = match (open, sh.step) {
        (Some(job), _) => job,
        (None, 0) => Job::open(lake, s).await?.context("can't shuffle this plan here")?,
        (None, _) => bail!("shuffle expired"),
    };
    // The subqueries whose exchanges are done are answered first, here as on every node.
    for q in job.subqueries.iter().filter(|q| sh.step >= q.from && q.results.get(q.index).is_none()) {
        let plan = received(&job, sh, &q.plan).await?;
        q.results.set(q.index, scalar(plan, job.ctx.task_ctx()).await?)?;
    }
    let last = sh.step == job.exchanges.len();
    let top = match last {
        true => job.cut.as_ref().map_or(job.plan.clone(), |c| c.children()[0].clone()),
        false => match job.exchanges[sh.step].whole {
            true => job.exchanges[sh.step].plan.clone(),
            false => job.exchanges[sh.step].plan.children()[0].clone(),
        },
    };
    let top = received(&job, sh, &top).await?;
    if last {
        let out = drain(&top, job.ctx.task_ctx(), &sh.id, &format!("out-{}", sh.step)).await?;
        job.done.store(true, std::sync::atomic::Ordering::Relaxed); // (the sweep frees its scratch shortly)
        return Ok((job.shape.clone(), out));
    }
    // Split by the exchange's hash (the same on every node): a bucket per node and partition. A
    // bucket past 64 MB goes to this node's disk as it is filled, so a shuffle isn't bounded by memory.
    let x = &job.exchanges[sh.step];
    let made = match x.plan.output_partitioning().clone() {
        _ if x.whole => vec![drain(&top, job.ctx.task_ctx(), &sh.id, &format!("{}-all", sh.step)).await?; sh.nodes.len()], // (every node, every partition)
        Partitioning::Hash(exprs, parts) => scatter(&top, job.ctx.task_ctx(), &sh.id, sh.step, exprs, sh.nodes.len(), parts, x.own.then_some(sh.me)).await?,
        _ => {
            // An all-gather (`everywhere`): every node is sent all of what this one has.
            let mut all = Spill::new(crate::spill::dir(&sh.id), format!("{}-gathered", sh.step));
            for part in drain(&top, job.ctx.task_ctx(), &sh.id, &format!("{}-all", sh.step)).await? {
                all.absorb(part)?;
            }
            vec![vec![all]; sh.nodes.len()]
        }
    };
    let sent: Vec<Vec<u64>> = made.iter().map(|to| to.iter().map(|s| s.bytes()).collect()).collect();
    let mut buckets = job.buckets.lock().unwrap();
    for (to, spill) in made.into_iter().enumerate() {
        buckets.insert((sh.step, to), spill);
    }
    Ok((format!("{}\n{}", job.shape, serde_json::to_string(&sent)?), vec![]))
}

/// `top` with the exchanges right below it replaced by what they brought this node: every node's
/// buckets for it, read a piece at a time (they are on disk past 64 MB) — an `own` exchange's
/// are this node's alone.
async fn received(job: &Job, sh: &Shuffle, top: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let mut inputs = vec![];
    collect_inputs(top, &job.exchanges, &mut inputs);
    let mut replaced = vec![];
    for x in inputs {
        let k = job.exchanges.iter().position(|e| Arc::ptr_eq(&e.plan, &x)).context("an unknown exchange")?;
        let own = job.exchanges[k].own;
        let fetches = sh.nodes.iter().enumerate().filter(|(i, _)| !own || *i == sh.me).map(|(i, node)| fetch(job, &sh.id, node, i == sh.me, k, sh.me, None));
        let from = futures::future::try_join_all(fetches).await?;
        let mut parts: Vec<Vec<Spill>> = (0..x.output_partitioning().partition_count()).map(|q| from.iter().map(|f| f.get(q).cloned().unwrap_or_default()).collect()).collect();
        // A hot partition shared out (`skew.rs`): on its split side, each node keeps the share it
        // hashed there itself; on the other, every node gets all of it — in its own partition of
        // the same number, beside its own keys (a join only ever pairs equal keys).
        for sp in sh.splits.iter().filter(|sp| sp.exchange == k || sp.with == k) {
            let q = sp.part;
            let more: Vec<Spill> = match (sp.exchange == k, sp.node == sh.me) {
                (true, true) => {
                    parts[q] = from.get(sh.me).and_then(|f| f.get(q)).cloned().into_iter().collect(); // (only my share)
                    continue;
                }
                (true, false) => fetch(job, &sh.id, "", true, k, sp.node, Some(q)).await?,
                (false, true) => continue,
                (false, false) => {
                    let all = sh.nodes.iter().enumerate().map(|(i, node)| fetch(job, &sh.id, node, i == sh.me, k, sp.node, Some(q)));
                    futures::future::try_join_all(all).await?.concat()
                }
            };
            parts[q].extend(more);
        }
        replaced.push((x.clone(), Received::new(&x, parts)?));
    }
    Ok(top.clone().transform_down(|p| match replaced.iter().find(|(x, _)| Arc::ptr_eq(x, &p)) {
        Some((_, input)) => Ok(Transformed::yes(input.clone())),
        None => Ok(Transformed::no(p)),
    })?.data)
}

/// Every partition of `plan` run and split by the exchange's hash into a bucket per node and
/// partition — as it runs, so a step's output is bounded by this node's disk, not by its memory.
/// Hashing into `nodes × parts` buckets does both splits at once: bucket `i` goes to node
/// `i / parts` and lands in partition `i % parts`, which is the partition DataFusion itself would
/// put the row in (`hash % parts`), so a receiving node needs no second pass and the operators
/// above it find each key where they expect it. The partitions run at once, each filling buckets
/// of its own; they are joined at the end in partition order, so a bucket's rows are always in
/// the same order.
async fn scatter(plan: &Arc<dyn ExecutionPlan>, ctx: Arc<datafusion::execution::TaskContext>, job: &str, step: usize, exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>, nodes: usize, parts: usize, only: Option<usize>) -> Result<Vec<Vec<Spill>>> {
    use datafusion::physical_plan::repartition::BatchPartitioner;
    let (dir, n) = (crate::spill::dir(job), nodes * parts);
    let runs = (0..plan.output_partitioning().partition_count()).map(|p| {
        let (plan, ctx, dir, exprs) = (plan.clone(), ctx.clone(), dir.clone(), exprs.clone());
        async move {
            let mut into: Vec<Spill> = (0..n).map(|i| Spill::new(dir.clone(), format!("{step}-{i}-p{p}"))).collect();
            let (mut split, mut rows) = (BatchPartitioner::try_new(Partitioning::Hash(exprs, n), Default::default(), p, 1)?, plan.execute(p, ctx)?);
            while let Some(b) = futures::StreamExt::next(&mut rows).await {
                let mut wrote = Ok(());
                split.partition(b?, |i, b| {
                    if only.is_none_or(|me| i / parts == me) {
                        wrote = wrote.as_ref().map_err(|e: &anyhow::Error| anyhow::anyhow!("{e:#}")).and_then(|_| into[i].push(b));
                    }
                    Ok(())
                })?;
                wrote?;
            }
            Ok::<_, anyhow::Error>(into)
        }
    });
    let mut out: Vec<Spill> = (0..n).map(|i| Spill::new(dir.clone(), format!("{step}-{i}"))).collect();
    for made in futures::future::try_join_all(runs).await? {
        for (i, s) in made.into_iter().enumerate() {
            out[i].absorb(s)?;
        }
    }
    let mut by_node: Vec<Vec<Spill>> = vec![vec![]; nodes];
    for (i, s) in out.into_iter().enumerate() {
        by_node[i / parts].push(s);
    }
    if only.is_none() {
        skew(&by_node);
    }
    Ok(by_node)
}

/// How uneven the buckets came out: the biggest against the average (1 = even). Rows are dealt by
/// the hash of their key, so a key that holds a lot of the table leaves one node with most of the
/// work — the query still answers (a bucket is bounded by disk, not memory), just not in parallel.
/// `pondra_shuffle_skew` is where that shows, and `GROUP BY` mostly avoids it: every node
/// aggregates its own rows before the exchange, so a hot key crosses as one row per node.
fn skew(buckets: &[Vec<Spill>]) {
    let sizes: Vec<u64> = buckets.iter().map(|b| b.iter().map(|s| s.bytes()).sum()).collect();
    let total: u64 = sizes.iter().sum();
    let (Some(&worst), true) = (sizes.iter().max(), total > 0) else { return };
    let ratio = worst as f64 * sizes.len() as f64 / total as f64;
    let was = crate::metrics::SKEW.load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::SKEW.store(was.max((ratio * 100.0) as u64), std::sync::atomic::Ordering::Relaxed);
}

/// The shuffles right below `p` (not below another one).
fn collect_inputs(p: &Arc<dyn ExecutionPlan>, exchanges: &[Exchange], out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    for c in p.children() {
        match exchanges.iter().any(|x| Arc::ptr_eq(&x.plan, c)) {
            true => out.push(c.clone()),
            false => collect_inputs(c, exchanges, out),
        }
    }
}

/// Node `from`'s bucket of exchange `k` for node `to`. A remote one is streamed onto this node's
/// disk piece by piece, never held whole in memory. Buckets are kept until the job ends, not
/// taken, so a step that has to be retried can read them again.
async fn fetch(job: &Job, id: &str, from: &str, local: bool, k: usize, to: usize, part: Option<usize>) -> Result<Vec<Spill>> {
    if local {
        return Ok(one(job.buckets.lock().unwrap().get(&(k, to)).cloned().unwrap_or_default(), part));
    }
    let only = part.map(|q| format!("&part={q}")).unwrap_or_default();
    let res = crate::cluster::http().get(format!("http://{from}/cluster/shuffle?id={id}&exchange={k}&to={to}{only}")).send().await?;
    ensure!(res.status().is_success(), "{from}: {}", res.text().await?);
    let name = format!("in-{k}-{to}-{}{}", from.replace(':', "_"), part.map(|q| format!("-p{q}")).unwrap_or_default());
    Ok(read_reply(res, id, &name).await?.1)
}

/// A bucket's partitions, or just one of them.
fn one(bucket: Vec<Spill>, part: Option<usize>) -> Vec<Spill> {
    match part {
        Some(q) => bucket.get(q).cloned().into_iter().collect(),
        None => bucket,
    }
}

/// `GET /cluster/shuffle`: the buckets this node keeps for another, one per partition, sent a
/// piece at a time (as `reply` sends a stage's).
pub fn bucket(id: &str, exchange: usize, to: usize, part: Option<usize>) -> Result<Vec<Spill>> {
    let job = JOBS.lock().unwrap().get(id).cloned().context("shuffle expired")?;
    let bucket = job.buckets.lock().unwrap().get(&(exchange, to)).cloned();
    Ok(one(bucket.unwrap_or_default(), part))
}

/// What an exchange brought this node, standing where the exchange was: one partition for each
/// of the exchange's own, each the nodes' rows for it in node order. It reports the exchange's
/// partitioning, so what is above it runs as planned.
#[derive(Debug)]
struct Received {
    props: Arc<datafusion::physical_plan::PlanProperties>,
    parts: Vec<Arc<dyn datafusion::physical_plan::streaming::PartitionStream>>,
}

impl Received {
    /// `parts[partition]`: what this node reads for each of the exchange's partitions, in order
    /// (every node's bucket for it, in node order).
    fn new(x: &Arc<dyn ExecutionPlan>, parts: Vec<Vec<Spill>>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = x.schema();
        ensure!(parts.iter().flatten().filter_map(|s| s.schema()).all(|s| s.fields() == schema.fields()), "nodes planned the query differently: {schema:?} / {:?}", parts.iter().flatten().filter_map(|s| s.schema()).find(|s| s.fields() != schema.fields()));
        crate::metrics::add(&crate::metrics::RECEIVED, parts.iter().flatten().map(|s| s.bytes()).sum());
        let parts = parts.into_iter().map(|p| crate::spill::chain(p, schema.clone())).collect();
        Ok(Arc::new(Received { props: x.properties().clone(), parts }))
    }
}

impl datafusion::physical_plan::DisplayAs for Received {
    fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Received: partitions={}", self.parts.len()) }
}

impl ExecutionPlan for Received {
    fn name(&self) -> &str { "Received" }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> { &self.props }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion>) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> { Ok(self) }
    fn execute(&self, partition: usize, ctx: Arc<datafusion::execution::TaskContext>) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        Ok(self.parts[partition].execute(ctx))
    }
}

// ---------------------------------------------------------------- SQL

/// Every table a single query reads, anywhere in it — joins, subqueries, CTEs, unions — once
/// each, CTE names left out. None for anything that isn't one query. Whether the query can be
/// split is not decided here but from its plan (`spread`), operator by operator.
fn tables(sql: &str) -> Option<Vec<String>> {
    use datafusion::sql::sqlparser::{ast::*, dialect::GenericDialect, parser::Parser};
    use std::ops::ControlFlow;
    #[derive(Default)]
    struct Names {
        tables: Vec<String>,
        ctes: std::collections::HashSet<String>,
    }
    fn name(i: &Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } } // (as SQL resolves it)
    impl Visitor for Names {
        type Break = ();
        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
            self.ctes.extend(q.with.iter().flat_map(|w| &w.cte_tables).map(|c| name(&c.alias.name)));
            ControlFlow::Continue(())
        }
        fn pre_visit_relation(&mut self, r: &ObjectName) -> ControlFlow<()> {
            let parts: Option<Vec<String>> = r.0.iter().map(|p| p.as_ident().map(name)).collect();
            self.tables.extend(parts.map(|p| p.join(".")));
            ControlFlow::Continue(())
        }
    }
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [stmt @ Statement::Query(_)] = &stmts[..] else { return None };
    let mut names = Names::default();
    let _ = stmt.visit(&mut names);
    let mut seen = std::collections::HashSet::new();
    let tables: Vec<String> = names.tables.into_iter().filter(|t| !names.ctes.contains(t) && seen.insert(t.clone())).collect();
    (!tables.is_empty()).then_some(tables)
}

// ---------------------------------------------------------------- a node's share of a table

/// A node's share of a sliced table: its scan, with 2+ partitions (so aggregations plan as
/// partial + final everywhere), reporting the whole table's size, so every node plans its query
/// (join order, build sides) as for the whole table, whatever its share holds. `whole`
/// (`WholeExec`): a table every node reads in full, reporting its size from the catalog.
#[derive(Debug)]
pub struct ShareExec {
    input: Arc<dyn ExecutionPlan>,
    table: String,
    stats: Arc<datafusion::common::Statistics>,
    whole: bool,
    range: Option<String>, // sliced by this column's ranges (`ranges.rs`)
    props: Arc<datafusion::physical_plan::PlanProperties>,
}

impl ShareExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, table: &str, rows: u64, bytes: u64, range: Option<String>) -> datafusion::error::Result<ShareExec> {
        let input = match input.output_partitioning().partition_count() < 2 {
            true => datafusion::physical_plan::union::UnionExec::try_new(vec![input.clone(), Arc::new(datafusion::physical_plan::empty::EmptyExec::new(input.schema()))])?,
            false => input,
        };
        Ok(ShareExec { range, ..ShareExec::of(input, table, rows, bytes, false) })
    }

    fn of(input: Arc<dyn ExecutionPlan>, table: &str, rows: u64, bytes: u64, whole: bool) -> ShareExec {
        use datafusion::common::stats::Precision;
        let mut stats = datafusion::common::Statistics::new_unknown(&input.schema());
        (stats.num_rows, stats.total_byte_size) = (Precision::Inexact(rows as usize), Precision::Inexact(bytes as usize));
        ShareExec { props: ShareExec::props(&input), input, table: table.into(), stats: Arc::new(stats), whole, range: None }
    }

    /// The input's properties, but no orders or constant columns: what a node's files happen to
    /// hold (one partition's value, rows in order) must not make its plan differ from the others'.
    fn props(input: &Arc<dyn ExecutionPlan>) -> Arc<datafusion::physical_plan::PlanProperties> {
        Arc::new(input.properties().as_ref().clone().with_eq_properties(datafusion::physical_expr::EquivalenceProperties::new(input.schema())))
    }
}

/// A table every node of a distributed query reads in full (a small one, a keyed one), as the
/// coordinator saw it. Its size comes from the catalog, not from how this node happens to read it:
/// decoded in memory (`hot.rs`) or from Parquet, more of it in the log or less, the sizes differ,
/// DataFusion picks joins by size, and every node must plan alike. What is inside a whole read —
/// the joins of a keyed table's versions — is the node's own business (`shape`).
#[derive(Debug)]
struct WholeTable {
    inner: Arc<dyn datafusion::catalog::TableProvider>,
    name: String,
    size: (u64, u64),
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for WholeTable {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef { self.inner.schema() }
    fn table_type(&self) -> datafusion::datasource::TableType { self.inner.table_type() }
    fn statistics(&self) -> Option<datafusion::common::Statistics> { self.inner.statistics() }
    fn supports_filters_pushdown(&self, f: &[&datafusion::logical_expr::Expr]) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(f)
    }
    async fn scan(&self, state: &dyn datafusion::catalog::Session, projection: Option<&Vec<usize>>, filters: &[datafusion::logical_expr::Expr], limit: Option<usize>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let input = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(ShareExec::of(input, &self.name, self.size.0, self.size.1, true)))
    }
}

impl datafusion::physical_plan::DisplayAs for ShareExec {
    fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: table={}{}", self.name(), self.table, self.range.as_ref().map(|r| format!(", range={r}")).unwrap_or_default())
    }
}

impl ExecutionPlan for ShareExec {
    fn name(&self) -> &str { if self.whole { "WholeExec" } else { "ShareExec" } }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> { &self.props }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn maintains_input_order(&self) -> Vec<bool> { vec![false] } // (it reports none: `props`)
    fn benefits_from_input_partitioning(&self) -> Vec<bool> { vec![false] }
    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion>) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ShareExec { props: ShareExec::props(&children[0]), input: children[0].clone(), table: self.table.clone(), stats: self.stats.clone(), whole: self.whole, range: self.range.clone() }))
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
