//! Materialized views kept current by running their query again (ADR-056, ADR-057): what the
//! rows alone can't keep.
//!
//! **By key**: a `GROUP BY` whose answers can't be added up as rows arrive (`median`, percentiles,
//! `count(DISTINCT …)`, `string_agg`, `array_agg`, …), or one asked to be (`refresh = 'by key'`).
//! After commits the leader finds the groups that its source's new, changed and deleted rows
//! touch, works out those groups again from the source as of then, and puts their rows in place of
//! the view's old ones, which go to `{view}$deleted` as a change's do (ADR-020).
//!
//! **Full**: any other query (`ORDER BY … LIMIT`, a window function, a subquery over its own
//! table, `now()`, …), or one asked to be (`refresh = 'full'`): run whole again once a table it
//! reads has changed (or, calling `now()` or reading outside the lake, every `lag`), at most so
//! often that its runs take a tenth of the time.
//!
//! Either way, rows that came out the same stay; a group that came out different keeps its row's
//! id, so the view's change feed shows an update. That is one commit with the view's progress
//! (producer `rerun:{view}`, seq the commit it read up to, `prev` the one before): a run is
//! applied once, whichever leader runs it. Nothing is held between runs.
use crate::query::{session_at, table_ref};
use crate::store::*;
use crate::sys::{CREATED, ROW_ID, VERSION};
use crate::views::View;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{Aggregate, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Volatility};
use datafusion::prelude::{cast, ident, lit, DataFrame};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How a view that runs its query again is kept.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Rerun {
    /// This view's own (one made again under its name is another, and never gets this one's rows).
    pub id: String,
    /// Its columns that are its `GROUP BY`, in that order (a run by key works these groups out; a
    /// full run keeps the id of the row of each it had). None: rows are matched whole.
    pub keys: Vec<String>,
    /// Run whole, not by key (`refresh = 'full'`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full: Option<Full>,
    /// Why it isn't kept from each write's rows (`pondra.flows`).
    pub reason: String,
    /// Asked for (`refresh = 'by key' | 'full'` or a `lag`), not chosen.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub asked: bool,
    /// At most one run a lag (`lag = '1 minute'`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_secs: Option<u64>,
}

/// What a full run reads.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Full {
    /// This lake's tables it reads: a commit that changes one starts a run.
    pub reads: Vec<String>,
    /// Its answer changes with nothing of this lake changing (`now()`, another lake's table, a
    /// file): it runs every lag too (a minute unless given).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub timed: bool,
}

/// How often a timed view runs when nothing it reads here changed, unless given a lag.
const TIMED: Duration = Duration::from_secs(60);

/// The producer a view's runs commit under (`{…}:deleted`: its old rows').
pub fn producer(view: &str) -> String { format!("rerun:{view}") }

/// The table the touched groups' keys are read from, as `__k0`, `__k1`, ….
const TOUCHED: &str = "__touched";

/// What a view needs to be kept by key: a `GROUP BY` of one table of this lake read alone (no other
/// table, no subquery), every `GROUP BY` expression one of its columns, only `HAVING` above it, and
/// nothing that answers otherwise as time passes. Its key columns, or why not (a clause that reads
/// after "can't be kept by key: ").
pub fn keys(plan: &LogicalPlan) -> Result<Vec<String>> {
    let across = "ORDER BY, LIMIT, DISTINCT or a window over its groups looks across groups";
    ensure!(matches!(plan, LogicalPlan::Projection(_)), across);
    let agg = aggregate(plan).context(across)?;
    ensure!(!agg.group_expr.is_empty() && !agg.group_expr.iter().any(|e| matches!(e, Expr::GroupingSet(_))), "it has no GROUP BY, or GROUPING SETS, ROLLUP or CUBE");
    ensure!(crate::views::alone(&agg.input, false), "it reads more than one table, or a subquery");
    let keys = selected(plan, agg).context("a GROUP BY expression isn't selected, so a row doesn't say which group it is")?;
    if let Some(f) = changing(plan) {
        bail!("{f}() answers otherwise as time passes");
    }
    Ok(keys)
}

/// The columns of a projection over `agg` that are its `GROUP BY` expressions, in their order, if
/// every one is.
fn selected(plan: &LogicalPlan, agg: &Aggregate) -> Option<Vec<String>> {
    let LogicalPlan::Projection(p) = plan else { return None };
    let mut keys = vec![None; agg.group_expr.len()];
    for (e, f) in p.expr.iter().zip(plan.schema().fields()) {
        if let Expr::Column(c) = e.clone().unalias_nested().data {
            match agg.schema.index_of_column(&c) {
                Ok(i) if i < keys.len() && keys[i].is_none() => keys[i] = Some(f.name().clone()),
                _ => {}
            }
        }
    }
    keys.into_iter().collect()
}

/// A full run's keys: the query's own `GROUP BY` columns under its `ORDER BY` and `LIMIT`, when it
/// selects them all (so a group whose answer changed keeps its row's id), else none.
fn group_keys(plan: &LogicalPlan) -> Vec<String> {
    match plan {
        LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => group_keys(plan.inputs()[0]),
        _ => aggregate(plan).filter(|a| !a.group_expr.iter().any(|e| matches!(e, Expr::GroupingSet(_)))).and_then(|a| selected(plan, a)).unwrap_or_default(),
    }
}

/// What the rows alone can't keep: the first aggregate that needs every row of its group
/// (`median`, a DISTINCT, an ordered `string_agg`, …), as "median() needs every row of a group".
pub fn needs(plan: &LogicalPlan) -> Option<String> {
    const KEPT: [&str; 13] = ["count", "sum", "min", "max", "avg", "stddev", "stddev_pop", "var", "var_pop", "variance", "bool_and", "bool_or", crate::finish::MOMENTS];
    let mut found = None;
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::Aggregate(a) = node {
            found = a.aggr_expr.iter().find_map(|e| match e.clone().unalias_nested().data {
                Expr::AggregateFunction(a) if a.params.distinct => Some(format!("{}(DISTINCT …) needs every row of a group", a.func.name())),
                Expr::AggregateFunction(a) if !KEPT.contains(&a.func.name()) || !a.params.order_by.is_empty() => Some(format!("{}() needs every row of a group", a.func.name())),
                _ => None,
            });
        }
        Ok(if found.is_some() { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
    });
    found
}

/// What makes a view look across rows of `source` that each write's rows alone can't answer, below
/// its own `GROUP BY` if it has one: an `ORDER BY` or `LIMIT`, a window, a `DISTINCT`, a `GROUP BY`
/// or a subquery over it, or reading it twice. None: it is row by row (filters, projections and
/// joins with other tables).
pub fn across(plan: &LogicalPlan, source: &str) -> Option<String> {
    let source = table_ref(source);
    let reads = |p: &LogicalPlan| {
        let mut found = false;
        let _ = p.apply_with_subqueries(|n| {
            found = matches!(n, LogicalPlan::TableScan(t) if t.table_name.resolved_eq(&source));
            Ok(if found { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
        });
        found
    };
    let mut scans = 0;
    let _ = plan.apply_with_subqueries(|n| {
        scans += matches!(n, LogicalPlan::TableScan(t) if t.table_name.resolved_eq(&source)) as usize;
        Ok(TreeNodeRecursion::Continue)
    });
    if scans > 1 {
        return Some(format!("it reads {source} twice"));
    }
    // (a GROUP BY at the top is the view's own, kept from the rows or by key: what is below it)
    fn below(p: &LogicalPlan) -> Option<&LogicalPlan> {
        match p {
            LogicalPlan::Aggregate(a) => Some(&a.input),
            LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => below(p.inputs()[0]),
            _ => None,
        }
    }
    let mut found = None;
    let _ = below(plan).unwrap_or(plan).apply_with_subqueries(|n| {
        let what = match n {
            LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => "ORDER BY or LIMIT",
            LogicalPlan::Window(_) => "a window function",
            LogicalPlan::Distinct(_) => "DISTINCT",
            LogicalPlan::Aggregate(_) => "a GROUP BY under it",
            LogicalPlan::Subquery(_) => "a subquery",
            _ => return Ok(TreeNodeRecursion::Continue),
        };
        if reads(n) {
            found = Some(format!("{what} over {source} looks across its rows"));
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// The query's own `GROUP BY`, under its projection and `HAVING`.
fn aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
    match plan {
        LogicalPlan::Aggregate(a) => Some(a),
        LogicalPlan::Projection(p) => aggregate(&p.input),
        LogicalPlan::Filter(f) => aggregate(&f.input),
        _ => None,
    }
}

/// The first function in `plan` whose answer may change with nothing else changing (`now()`,
/// `random()`, a Python function not declared IMMUTABLE).
fn changing(plan: &LogicalPlan) -> Option<String> {
    let mut found = None;
    let _ = plan.apply_with_subqueries(|node| {
        for e in node.expressions() {
            let _ = e.apply(|x| {
                if let Expr::ScalarFunction(f) = x {
                    if f.func.signature().volatility != Volatility::Immutable {
                        found = Some(f.func.name().to_string());
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(if found.is_some() { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
    });
    found
}

/// How a view is run again.
pub enum How {
    ByKey,
    Full,
}

/// Make view `name` run again by key or whole (leader only): its table gets the query's columns,
/// and its first run works every group out (`ddl::settle` waits for it). `why`: None when asked.
#[allow(clippy::too_many_arguments)]
pub async fn create(lake: &Lake, name: &str, sql: &str, source: &str, src: &TableMeta, upstream: Option<&View>, plan: &LogicalPlan, how: How, why: Option<String>, lag_secs: Option<u64>) -> Result<()> {
    let asked = why.is_none();
    let (keys, full, reason) = match how {
        How::ByKey => {
            let keys = keys(plan).map_err(|e| anyhow::anyhow!("{name} can't be kept by key: {e:#}. refresh = 'full' runs its query whole again"))?;
            ensure!(src.key.is_empty(), "{source} is keyed: a new row replaces its key's row, which says nothing of the group that row was in, so a view of it can't be kept by key. refresh = 'full' runs its query whole again");
            ensure!(src.merge.is_empty() && src.finish.is_none(), "{source} is a GROUP BY view, whose table keeps partial rows: make {name} from {source}'s own source, or refresh = 'full'");
            ensure!(src.history.is_none(), "{source} is a history view, whose __start_at and __end_at are worked out as it is read: make {name} with refresh = 'full'");
            ensure!(upstream.is_none_or(|u| u.sessions.is_none() || u.rerun.is_some()), "{source} cuts sessions as they close: make {name} from {source}'s own source");
            (keys, None, why.unwrap_or_else(|| "asked for (refresh = 'by key')".into()))
        }
        How::Full => {
            let (reads, timed) = reads(lake, sql, source).await?;
            let timed = timed || changing(plan).is_some();
            (group_keys(plan), Some(Full { reads, timed }), why.unwrap_or_else(|| "asked for (refresh = 'full')".into()))
        }
    };
    crate::format::require(lake, 2, "a materialized view that runs its query again (median, count(DISTINCT …), ORDER BY … LIMIT, …)").await?;
    let columns: Vec<(String, String)> = plan.schema().fields().iter().map(|f| (f.name().clone(), crate::query::type_name(f.data_type()))).collect();
    if let Some((c, _)) = columns.iter().find(|(c, _)| crate::sys::NAMES.contains(&c.as_str())) {
        bail!("{c} is a system column of the view's own table: name it something else ({c} AS source{c})");
    }
    let meta = TableMeta { columns, publish: default_publish(), ids: true, tiered: lake.visible(), ..Default::default() };
    let r = Rerun { id: uuid::Uuid::new_v4().to_string(), keys, full, reason, asked, lag_secs };
    let view = View { source: source.into(), sql: sql.into(), emit: None, sessions: None, ids: false, join: None, fill: None, expect: vec![], once: None, written: None, rerun: Some(r) };
    lake.cat.commit(vec![(table_key(name), json(&meta)), (crate::views::view_key(name), json(&view))], &[]).await
}

/// The tables of this lake a view's query reads (through stored views too), and whether it reads
/// anything else (another lake's table, a file, a system table). From the statement's names, not
/// its plan, where a keyed table is a query of its own.
async fn reads(lake: &Lake, sql: &str, source: &str) -> Result<(Vec<String>, bool)> {
    let Some(names) = crate::spmd::tables(sql) else { return Ok((vec![source.to_string()], true)) };
    let (mut here, mut elsewhere, mut seen) = (vec![], false, std::collections::BTreeSet::new());
    let mut names = names;
    while let Some(n) = names.pop() {
        let Ok((None, local)) = crate::ddl::resolve(lake, &n).await else {
            elsewhere = true;
            continue;
        };
        if !seen.insert(local.clone()) {
            continue;
        }
        if lake.cat.get::<TableMeta>(&table_key(&local)).await?.is_some() {
            here.push(local);
        } else if let Some(v) = lake.cat.get::<crate::ddl::StoredView>(&crate::ddl::query_key(&local)).await? {
            match crate::spmd::tables(&v.sql) {
                Some(t) => names.extend(t), // (a stored view: the tables it reads)
                None => elsewhere = true,
            }
        } else {
            elsewhere = true;
        }
    }
    Ok((here, elsewhere))
}

// ---------------------------------------------------------------- the runs (the leader's)

/// When each view last ran, how long it took, and how many runs in a row failed: a view runs again
/// only once at least that long (ten times that for a full run, and its lag) has passed, so a slow
/// one never takes the leader over, and one that keeps failing waits longer each time (up to a
/// minute).
#[derive(Default)]
struct Pace {
    running: bool,
    started: Option<Instant>,
    took: Duration,
    failures: u32,
    error: Option<String>,
    /// When a timed view last ran whatever changed.
    timed: Option<Instant>,
}

static PACE: Mutex<BTreeMap<(String, String), Pace>> = Mutex::new(BTreeMap::new());

/// A view's turn to run, given back when the run ends (or panics). `due`: a timed view's time to
/// run though nothing it reads here changed.
struct Turn {
    key: (String, String),
    due: bool,
}

impl Drop for Turn {
    fn drop(&mut self) {
        if let Some(p) = PACE.lock().unwrap().get_mut(&self.key) {
            (p.running, p.took) = (false, p.started.map_or(Duration::ZERO, |s| s.elapsed()));
        }
    }
}

impl Turn {
    /// How the run went: an error is logged when it is new, not at every try.
    fn ended(&self, r: &Result<()>) {
        let mut pace = PACE.lock().unwrap();
        let Some(p) = pace.get_mut(&self.key) else { return };
        match r {
            Ok(()) => (p.failures, p.error) = (0, None),
            Err(e) => {
                let e = format!("{e:#}");
                if p.error.as_ref() != Some(&e) {
                    eprintln!("materialized view {}: {e}", self.key.1);
                }
                (p.failures, p.error) = (p.failures + 1, Some(e));
            }
        }
    }
}

fn turn(lake: &Lake, view: &str, r: &Rerun) -> Option<Turn> {
    let mut pace = PACE.lock().unwrap();
    let key = (lake.url.clone(), view.to_string());
    let p = pace.entry(key.clone()).or_default();
    let backoff = Duration::from_secs(if p.failures == 0 { 0 } else { (1u64 << p.failures.min(6)).min(60) / 2 });
    let spaced = if r.full.is_some() { p.took * 10 } else { p.took }; // (a full run reads everything)
    let wait = spaced.max(Duration::from_secs(r.lag_secs.unwrap_or(0))).max(backoff);
    if p.running || p.started.is_some_and(|s| s.elapsed() < wait) {
        return None;
    }
    let every = r.lag_secs.map_or(TIMED, Duration::from_secs);
    let due = r.full.as_ref().is_some_and(|f| f.timed) && p.timed.is_none_or(|t| t.elapsed() >= every);
    (p.running, p.started) = (true, Some(Instant::now()));
    if due {
        p.timed = p.started;
    }
    Some(Turn { key, due })
}

/// Why a view's runs keep failing on this node (two in a row), if they do: making it says so
/// instead of waiting for a first run that won't come.
pub fn failing(lake: &Lake, view: &str) -> Option<String> {
    let pace = PACE.lock().unwrap();
    pace.get(&(lake.url.clone(), view.to_string())).filter(|p| p.failures >= 2).and_then(|p| p.error.clone())
}

/// Leader: start a run of every view that runs its query again whose turn it is, each in a task of
/// its own (a run that reads many rows never holds up the others, or the leader's other work). A
/// run is no loop: one that fails or panics is tried again at a later turn.
pub async fn run_all(lake: &Arc<Lake>, seq: &Arc<crate::log::Sequencer>, lock: &Arc<tokio::sync::Mutex<()>>) -> Result<()> {
    for (key, v) in lake.cat.scan::<View>("v/", "v0").await? {
        let Some(r) = &v.rerun else { continue };
        let name = key[2..].to_string();
        let Some(turn) = turn(lake, &name, r) else { continue };
        let (lake, seq, lock) = (lake.clone(), seq.clone(), lock.clone());
        tokio::spawn(async move {
            let r = run(&lake, &seq, &lock, &name, &v, turn.due).await;
            turn.ended(&r);
        });
    }
    Ok(())
}

/// Has a log segment changed `source`'s rows: rows in (the log's, or a file commit's), old versions
/// out (`{t}$deleted`), files taken out or rows deleted from them (another engine's change)?
pub fn moved(s: &Segment, source: &str) -> bool {
    s.rows_of(source) > 0 || s.rows_of(&crate::sys::deleted(source)) > 0 || s.files.get(source).is_some_and(|f| !f.removed.is_empty() || !f.deleted.is_empty())
}

/// The tables whose changes a view's runs follow: its source, or every table of this lake a full
/// run reads.
pub fn follows(v: &View) -> Vec<String> {
    match v.rerun.as_ref().and_then(|r| r.full.as_ref()) {
        Some(f) => f.reads.clone(),
        None => vec![v.source.clone()],
    }
}

/// Did commits (after, upto] change any of `tables`?
async fn moved_since(lake: &Lake, after: u64, upto: u64, tables: &[String]) -> Result<bool> {
    Ok(lake.cat.scan::<Segment>(&seg_key(after + 1), &seg_key(upto + 1)).await?.iter().any(|(_, s)| tables.iter().any(|t| moved(s, t))))
}

/// One run: the groups commits (done, now] touched (or every row, run whole), worked out again as
/// of `now`, in place of theirs. The first run works every group out, and so does one whose
/// changes its source no longer holds whole (`past::kept_since`). `due`: a timed view runs though
/// nothing changed here, and commits only if its answer did.
async fn run(lake: &Lake, seq: &crate::log::Sequencer, lock: &tokio::sync::Mutex<()>, view: &str, v: &View, due: bool) -> Result<()> {
    let r = v.rerun.as_ref().context("not run again")?;
    let producer = producer(view);
    let done: Option<u64> = lake.cat.get(&producer_key(&producer)).await?;
    let mut now = lake.visible();
    if now == 0 {
        seq.number().await?; // (a lake with no commit yet: its first, so the run has one to be as of)
        return Ok(());
    }
    let mut fresh = done.is_some_and(|d| d >= now); // (nothing committed since its last run)
    if fresh && !due {
        return Ok(());
    }
    let touched = match (done, &r.full) {
        (Some(d), Some(f)) if !fresh => {
            if !due && !moved_since(lake, d, now, &f.reads).await? {
                return Ok(()); // (nothing it reads changed: no run, no commit)
            }
            None
        }
        (Some(d), None) => {
            let src: TableMeta = lake.cat.get(&table_key(&v.source)).await?.with_context(|| format!("no table {}", v.source))?;
            match crate::past::kept_since(&src).is_none_or(|(kept, _)| kept <= d) {
                true if !moved_since(lake, d, now, &[v.source.clone()]).await? => return Ok(()),
                true => Some(touched(lake, v, d, now).await?),
                false => None, // (its changes no longer whole: every group)
            }
        }
        _ => None, // (every group)
    };
    let scanned = match &touched {
        Some(t) if t.iter().all(|b| b.num_rows() == 0) => None,
        Some(t) => Some(scan(t.clone())?),
        None => None,
    };
    let none = touched.is_some() && scanned.is_none();
    loop {
        let new = match none {
            true => vec![],
            false => worked_out(lake, v, now, scanned.as_ref()).await?,
        };
        // Under the lake's lock, as a change is (invariant 56): the view's rows as they are, and
        // this view still the one this run is for (dropped and made again meanwhile, it's another).
        let guard = lock.lock().await;
        if lake.cat.get::<View>(&crate::views::view_key(view)).await?.and_then(|v| v.rerun).is_none_or(|x| x.id != r.id) {
            return Ok(());
        }
        let meta: TableMeta = lake.cat.get(&table_key(view)).await?.context("view without table")?;
        crate::change::companion(lake, view, &meta).await?; // (its `$deleted`, which every run names)
        let meta: TableMeta = lake.cat.get(&table_key(view)).await?.context("view without table")?;
        let old = match none {
            true => vec![],
            false => old_rows(lake, view, &r.keys, scanned.as_ref()).await?,
        };
        let keys = match r.keys.is_empty() {
            true => meta.columns.iter().map(|(c, _)| c.clone()).collect(), // (rows matched whole)
            false => r.keys.clone(),
        };
        let (old, new) = diff(&meta, &keys, &old, &new)?;
        if fresh {
            if old.num_rows() == 0 && new.num_rows() == 0 {
                return Ok(()); // (a timed run, nothing new here, and the same answer: nothing to say)
            }
            // (its answer moved with time alone: a commit of its own to be as of, and again)
            drop(guard);
            (now, fresh) = (seq.number().await?.version, false);
            continue;
        }
        let (mut pending, _) = crate::change::appends(lake, seq, view, &meta, vec![old], vec![new], &producer).await?;
        if pending.is_empty() {
            // (nothing came out different: only its progress moves, both parts' together)
            let empty = |table: String, s: SchemaRef| crate::log::Append { table, src: crate::log::Src { producer: String::new(), seq: 0, prev: None }, batch: RecordBatch::new_empty(s), ack: tokio::sync::oneshot::channel().0 };
            let s = crate::query::schema(&meta.columns)?;
            pending = vec![empty(view.to_string(), s.clone()), empty(crate::sys::deleted(view), s)];
        }
        // Both parts under the same seq and `prev`: they always move together, so a run that lost
        // the race (another leader's, or a retry) is refused whole.
        for a in &mut pending {
            let name = if a.table == view { producer.clone() } else { format!("{producer}:deleted") };
            a.src = crate::log::Src { producer: name, seq: now, prev: Some(done.unwrap_or(0)) };
        }
        if let crate::log::Outcome::Acks(acks) = crate::change::submit(lake, seq, &pending).await? {
            if acks.iter().any(|a| a.conflict) {
                eprintln!("materialized view {view}: another run committed first; the next one reads on from it");
            }
        }
        return Ok(());
    }
}

/// The keys of the groups commits (done, now] touched: the view's `GROUP BY` over the rows they
/// changed (new ones, the old versions they replaced or deleted, the rows other engines took out),
/// under its `WHERE`, each key once.
async fn touched(lake: &Lake, v: &View, done: u64, now: u64) -> Result<Vec<RecordBatch>> {
    let rows = changed(lake, &v.source, done, now).await?;
    let ctx = session_at(lake, &v.sql, "", Some(now)).await?;
    ctx.deregister_table(table_ref(&v.source))?;
    ctx.register_table(table_ref(&v.source), rows.into_view())?;
    let plan = crate::query::sql(&ctx, &crate::asof::rewrite(&v.sql)?).await?.into_unoptimized_plan();
    let agg = aggregate(&plan).context("a view kept by key is a GROUP BY")?;
    let keys: Vec<Expr> = agg.group_expr.iter().enumerate().map(|(i, g)| g.clone().unalias().alias(format!("__k{i}"))).collect();
    let p = LogicalPlanBuilder::from(agg.input.as_ref().clone()).project(keys)?.distinct()?.build()?;
    Ok(ctx.execute_logical_plan(p).await?.collect().await?)
}

/// `source`'s rows commits (after, upto] changed, its columns as SQL names them: what they wrote
/// (`_version` says which commit wrote a row, `sys.rs`), the old versions they put in
/// `{source}$deleted`, and the rows file commits took out.
async fn changed(lake: &Lake, source: &str, after: u64, upto: u64) -> Result<DataFrame> {
    let meta: TableMeta = lake.cat.get(&table_key(source)).await?.with_context(|| format!("no table {source}"))?;
    let ctx = session_at(lake, &format!("SELECT {VERSION} FROM {}", crate::write::sql_name(source)), "", Some(upto)).await?;
    let within = |df: DataFrame| df.filter(ident(VERSION).gt(lit(after as i64)).and(ident(VERSION).lt_eq(lit(upto as i64))));
    let new = within(ctx.table(table_ref(source)).await?)?;
    let columns: Vec<(String, DataType)> = meta.logical().columns.iter().map(|(c, _)| Ok((c.clone(), new.schema().field_with_unqualified_name(c)?.data_type().clone()))).collect::<Result<_>>()?;
    let pick = |df: DataFrame| -> Result<DataFrame> {
        let exprs = columns.iter().map(|(c, t)| match df.schema().has_column_with_unqualified_name(c) {
            true => Ok(cast(ident(c), t.clone()).alias(c)),
            false => Ok(lit(ScalarValue::try_from(t)?).alias(c)), // (a column added since)
        }).collect::<Result<Vec<_>>>()?;
        Ok(df.select(exprs)?)
    };
    let mut all = pick(new)?;
    let gone = crate::sys::deleted(source);
    if let (true, Some(dmeta)) = (meta.changed, lake.cat.get::<TableMeta>(&table_key(&gone)).await?) {
        let t = crate::query::table_view(lake, &ctx, &gone, &crate::sys::with_sys(&dmeta), Some(upto)).await?;
        all = all.union(pick(within(ctx.read_table(crate::query::named(&ctx, t, &dmeta, false)?)?)?)?)?;
    }
    let out = crate::change::taken_out(lake, source, after, Some(upto)).await?;
    if out.iter().any(|b| b.num_rows() > 0) {
        let stored = crate::query::schema(&crate::sys::with_sys(&meta).columns)?;
        let out = out.iter().map(|b| meta.to_logical(&crate::query::conform(b, &stored)?)).collect::<Result<Vec<_>>>()?;
        all = all.union(pick(ctx.read_batches(out)?)?)?;
    }
    Ok(all)
}

/// The touched keys as a table to join with.
fn scan(rows: Vec<RecordBatch>) -> Result<LogicalPlan> {
    let t = datafusion::datasource::MemTable::try_new(rows[0].schema(), vec![rows])?;
    Ok(LogicalPlanBuilder::scan(TOUCHED, datafusion::datasource::provider_as_source(Arc::new(t)), None)?.build()?)
}

fn key(i: usize) -> Expr { Expr::Column(Column::new(Some(TOUCHED), format!("__k{i}"))) }

/// The view's rows as of `now`: of the touched groups only (their rows, and nothing else, go into
/// the `GROUP BY`), or of every group.
async fn worked_out(lake: &Lake, v: &View, now: u64, touched: Option<&LogicalPlan>) -> Result<Vec<RecordBatch>> {
    let ctx = session_at(lake, &v.sql, "", Some(now)).await?;
    let plan = crate::query::sql(&ctx, &crate::asof::rewrite(&v.sql)?).await?.into_unoptimized_plan();
    let plan = match touched {
        Some(t) => restrict(&plan, t)?,
        None => plan,
    };
    Ok(ctx.execute_logical_plan(plan).await?.collect().await?)
}

/// `plan` with its `GROUP BY` reading only the touched groups' rows: a semi join on its keys
/// (`IS NOT DISTINCT FROM`: a NULL key is a group too).
fn restrict(plan: &LogicalPlan, touched: &LogicalPlan) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Aggregate(a) => {
            let on: Vec<Expr> = a.group_expr.iter().enumerate().map(|(i, g)| same(g.clone().unalias(), key(i))).collect();
            let input = LogicalPlanBuilder::from(a.input.as_ref().clone()).join_on(touched.clone(), JoinType::LeftSemi, on)?.build()?;
            LogicalPlan::Aggregate(Aggregate::try_new(Arc::new(input), a.group_expr.clone(), a.aggr_expr.clone())?)
        }
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) => plan.with_new_exprs(plan.expressions(), vec![restrict(plan.inputs()[0], touched)?])?,
        _ => bail!("a view kept by key is a GROUP BY"),
    })
}

/// The view's rows of the touched groups (or all of them), with their `_row_id`, `_created_at`
/// and `_version`.
async fn old_rows(lake: &Lake, view: &str, keys: &[String], touched: Option<&LogicalPlan>) -> Result<Vec<RecordBatch>> {
    let sql = format!("SELECT *, {ROW_ID}, {CREATED}, {VERSION} FROM {}", crate::write::sql_name(view));
    let ctx = session_at(lake, &sql, "", Some(lake.visible())).await?;
    let mut plan = ctx.sql(&crate::sys::hide(&sql)).await?.into_unoptimized_plan(); // (`*` without the system columns)
    if let Some(t) = touched {
        let on: Vec<Expr> = keys.iter().enumerate().map(|(i, k)| same(ident(k), key(i))).collect();
        plan = LogicalPlanBuilder::from(plan).join_on(t.clone(), JoinType::LeftSemi, on)?.build()?;
    }
    Ok(ctx.execute_logical_plan(plan).await?.collect().await?)
}

/// What a run changes: the old rows (with their system columns) that aren't among the new ones,
/// and the new rows that aren't among the old, each with the id and time of the old row of its
/// key it replaces (an update in the change feed), or none (a new group). Rows compared whole, by
/// Arrow's row format: NULLs equal, as a key's are.
fn diff(meta: &TableMeta, keys: &[String], old: &[RecordBatch], new: &[RecordBatch]) -> Result<(RecordBatch, RecordBatch)> {
    use datafusion::arrow::compute::{concat_batches, filter_record_batch, take};
    use datafusion::arrow::row::{RowConverter, SortField};
    let s = crate::query::schema(&meta.columns)?;
    let mut fields = s.fields().to_vec();
    let ts = crate::sys::time();
    fields.extend([Field::new(ROW_ID, DataType::Int64, true), Field::new(CREATED, ts, true), Field::new(VERSION, DataType::Int64, true)].map(Arc::new));
    let olds = Arc::new(Schema::new(fields));
    let old = concat_batches(&olds, &old.iter().map(|b| crate::query::conform(b, &olds)).collect::<Result<Vec<_>>>()?)?;
    let new = concat_batches(&s, &new.iter().map(|b| crate::query::cast_as(b, &s)).collect::<Result<Vec<_>>>()?)?;
    let at: Vec<usize> = keys.iter().map(|k| s.index_of(k)).collect::<Result<_, _>>()?;
    let all: Vec<usize> = (0..s.fields().len()).collect();
    // (one converter per pair, so the rows of old and new compare)
    let pair = |cols: &[usize]| -> Result<(datafusion::arrow::row::Rows, datafusion::arrow::row::Rows)> {
        let c = RowConverter::new(cols.iter().map(|&i| SortField::new(s.field(i).data_type().clone())).collect())?;
        let get = |b: &RecordBatch| cols.iter().map(|&i| b.column(i).clone()).collect::<Vec<ArrayRef>>();
        Ok((c.convert_columns(&get(&old))?, c.convert_columns(&get(&new))?))
    };
    let ((old_whole, new_whole), (old_key, new_key)) = (pair(&all)?, pair(&at)?);
    let mut by_key: HashMap<&[u8], Vec<usize>> = HashMap::new();
    for i in 0..old.num_rows() {
        by_key.entry(old_key.row(i).data()).or_default().push(i);
    }
    let mut gone = vec![true; old.num_rows()];
    let mut put = vec![];
    for j in 0..new.num_rows() {
        let same = by_key.get(new_key.row(j).data()).and_then(|is| is.iter().copied().find(|&i| gone[i] && old_whole.row(i) == new_whole.row(j)));
        match same {
            Some(i) => gone[i] = false, // (came out the same: stays as it is)
            None => put.push(j),
        }
    }
    let mut replaced = vec![false; old.num_rows()];
    let mut prior: Vec<Option<u32>> = vec![];
    for &j in &put {
        let i = by_key.get(new_key.row(j).data()).and_then(|is| is.iter().copied().find(|&i| gone[i] && !replaced[i]));
        if let Some(i) = i {
            replaced[i] = true;
        }
        prior.push(i.map(|i| i as u32));
    }
    let picked = UInt32Array::from(put.iter().map(|&j| j as u32).collect::<Vec<_>>());
    let from_old = UInt32Array::from(prior);
    let mut columns: Vec<ArrayRef> = new.columns().iter().map(|c| take(c, &picked, None)).collect::<Result<_, _>>()?;
    let mut out_fields = s.fields().to_vec();
    for c in [ROW_ID, CREATED] {
        let i = olds.index_of(c)?;
        columns.push(take(old.column(i), &from_old, None)?);
        out_fields.push(olds.fields()[i].clone());
    }
    let new = RecordBatch::try_new(Arc::new(Schema::new(out_fields)), columns)?;
    let old = filter_record_batch(&old, &datafusion::arrow::array::BooleanArray::from(gone))?;
    Ok((old, new))
}

/// `l IS NOT DISTINCT FROM r`: a NULL key is a group of its own.
fn same(l: Expr, r: Expr) -> Expr {
    Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(Box::new(l), datafusion::logical_expr::Operator::IsNotDistinctFrom, Box::new(r)))
}
