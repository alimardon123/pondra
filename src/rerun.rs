//! Materialized views kept current by running their query again (ADR-056, ADR-059): what the
//! rows alone can't keep.
//!
//! **By key**: a `GROUP BY` whose answers can't be added up as rows arrive (`median`, percentiles,
//! `count(DISTINCT …)`, `string_agg`, `array_agg`, …), windows that all partition by columns the
//! view selects (each key's rows come from that key's source rows alone: the latest bid of each
//! bidder, the top ten of each auction), over one table or tables joined on those columns (each
//! auction's winning bid), or one asked to be (`refresh = 'by key'`). After commits the leader
//! finds the keys that the new, changed and deleted rows of what it reads touch, works out those
//! keys again as of then, and puts their rows in place of the view's old ones, which go to
//! `{view}$deleted` as a change's do (ADR-020).
//!
//! **Full**: any other query (`ORDER BY … LIMIT`, a window over every row, a subquery over its own
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
use crate::text::{CREATED, ROW_ID, VERSION};
use crate::views::View;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::logical_expr::{Aggregate, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Volatility};
use datafusion::prelude::{cast, ident, lit, DataFrame, SessionContext};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How a view that runs its query again is kept.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Rerun {
    /// This view's own (one made again under its name is another, and never gets this one's rows).
    pub id: String,
    /// Its columns that are its `GROUP BY`, or that its windows all partition by, in that order (a
    /// run by key works these keys out; a full run keeps the id of the row of each it had). None:
    /// rows are matched whole.
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
    /// The tables of this lake a view kept by key joins under its windows, its source among them
    /// (each auction's winning bid: auction and bid), when it does. A change to one starts a run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub joins: Vec<String>,
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
    if let Some(w) = window(plan) {
        return window_keys(plan, w);
    }
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

/// What a view of windows needs to be kept by key: one table of this lake read alone under them,
/// or tables joined on them (`joined`), columns every window partitions by (each window's
/// partition then lies within one key's rows), each selected as it is (so a row says which key it
/// is), and nothing that answers otherwise as time passes. Those columns, by the view's names, in
/// the first window's order.
fn window_keys(plan: &LogicalPlan, w: &LogicalPlan) -> Result<Vec<String>> {
    let (by, input) = partitioned(w).context("a window over all its rows, or windows that share no PARTITION BY column, look across every row")?;
    let mut scans = 0;
    plan.apply_with_subqueries(|n| {
        scans += matches!(n, LogicalPlan::TableScan(_)) as usize;
        Ok(TreeNodeRecursion::Continue)
    })?;
    let tables = joined(input, &by).map_or(1, |t| t.len());
    ensure!(scans == tables && (tables > 1 || crate::views::alone(input, false)), "its windows read a GROUP BY, a subquery, a keyed table or an outer join, or tables not joined on every column they partition by");
    let names = by.iter().map(|c| {
        let i = input.schema().index_of_column(c).ok()?; // (a window keeps its input's columns first, at their places)
        (0..plan.schema().fields().len()).find(|&j| traced(plan, j) == Some(i)).map(|j| plan.schema().field(j).name().clone())
    });
    let names = names.collect::<Option<Vec<_>>>().context("a column its windows partition by isn't selected as it is, so a row doesn't say which window it is in")?;
    if let Some(f) = changing(plan) {
        bail!("{f}() answers otherwise as time passes");
    }
    Ok(names)
}

/// The view's windows, under its projections, filters and aliases.
fn window(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Window(_) => Some(plan),
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => window(plan.inputs()[0]),
        _ => None,
    }
}

/// The columns every window of a stack partitions by, in the first's order, and the rows under
/// them. None if they share none.
fn partitioned(plan: &LogicalPlan) -> Option<(Vec<Column>, &LogicalPlan)> {
    let (mut by, mut p) = (None::<Vec<Column>>, plan);
    while let LogicalPlan::Window(w) = p {
        for e in &w.window_expr {
            let Expr::WindowFunction(f) = e.clone().unalias_nested().data else { return None };
            let cols: Vec<Column> = f.params.partition_by.iter().filter_map(|e| match e { Expr::Column(c) => Some(c.clone()), _ => None }).collect();
            by = Some(match by {
                None => cols,
                Some(b) => b.into_iter().filter(|c| cols.contains(c)).collect(),
            });
        }
        p = &w.input;
    }
    by.filter(|b| !b.is_empty()).map(|b| (b, p))
}

/// Rows under windows that come from inner joins of tables joined on the windows' keys: each table
/// read (under its alias) and its column equal to each key, through the joins' conditions and the
/// filters over them (`a.id = b.auction`), of the key's type (strings of any kind alike: a table
/// read from files has string views, one from the log plain strings). A key's rows then come from
/// that key's rows of each table alone. None if a table has no such column, or the rows come any
/// other way (one table, an outer join, a projection, a subquery, a keyed table's query).
fn joined(input: &LogicalPlan, by: &[Column]) -> Option<Vec<(LogicalPlan, Vec<Column>)>> {
    fn equal(e: &Expr, pairs: &mut Vec<(Column, Column)>) -> Option<()> {
        if e.exists(|e| Ok(matches!(e, Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)))).unwrap_or(true) {
            return None;
        }
        for c in datafusion::logical_expr::utils::split_conjunction(e) {
            if let Expr::BinaryExpr(b) = c {
                if let (Operator::Eq, Expr::Column(l), Expr::Column(r)) = (b.op, &*b.left, &*b.right) {
                    pairs.push((l.clone(), r.clone()));
                }
            }
        }
        Some(())
    }
    fn walk(p: &LogicalPlan, tables: &mut Vec<LogicalPlan>, pairs: &mut Vec<(Column, Column)>) -> Option<()> {
        match p {
            LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
                for (l, r) in &j.on {
                    if let (Expr::Column(l), Expr::Column(r)) = (l, r) {
                        pairs.push((l.clone(), r.clone()));
                    }
                }
                if let Some(f) = &j.filter {
                    equal(f, pairs)?;
                }
                walk(&j.left, tables, pairs)?;
                walk(&j.right, tables, pairs)
            }
            LogicalPlan::Filter(f) => {
                equal(&f.predicate, pairs)?;
                walk(&f.input, tables, pairs)
            }
            _ => {
                scanned(p)?;
                tables.push(p.clone());
                Some(())
            }
        }
    }
    let (mut tables, mut pairs) = (vec![], vec![]);
    walk(input, &mut tables, &mut pairs)?;
    if tables.len() < 2 {
        return None;
    }
    // (each key's class: the columns equal to it)
    let classes: Vec<Vec<Column>> = by.iter().map(|c| {
        let mut class = vec![c.clone()];
        while let Some(more) = pairs.iter().find_map(|(l, r)| match (class.contains(l), class.contains(r)) {
            (true, false) => Some(r.clone()),
            (false, true) => Some(l.clone()),
            _ => None,
        }) {
            class.push(more);
        }
        class
    }).collect();
    let typed = |c: &Column, s: &datafusion::common::DFSchema| {
        s.qualified_field_from_column(c).ok().map(|(_, f)| match f.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
            DataType::BinaryView | DataType::LargeBinary => DataType::Binary,
            t => t.clone(),
        })
    };
    tables.into_iter().map(|t| {
        let cols = by.iter().zip(&classes).map(|(k, class)| class.iter().find(|c| t.schema().has_column(c) && typed(c, t.schema()) == typed(k, input.schema())).cloned());
        Some((t.clone(), cols.collect::<Option<Vec<_>>>()?))
    }).collect()
}

/// The table a plan reads, if it is one table under its aliases.
fn scanned(p: &LogicalPlan) -> Option<&TableReference> {
    match p {
        LogicalPlan::TableScan(t) => Some(&t.table_name),
        LogicalPlan::SubqueryAlias(s) => scanned(&s.input),
        _ => None,
    }
}

/// Which of `tables` (this lake's names) `t` is: the same name, else the same once resolved
/// (`public.t` is `t`).
fn named<'a>(t: &TableReference, tables: &'a [String]) -> Option<&'a String> {
    tables.iter().find(|n| table_ref(n) == *t).or_else(|| tables.iter().find(|n| t.resolved_eq(&table_ref(n))))
}

/// Where output column `i` comes from, unchanged, in the windows' columns (through projections,
/// filters and aliases).
fn traced(plan: &LogicalPlan, i: usize) -> Option<usize> {
    match plan {
        LogicalPlan::Projection(p) => match p.expr.get(i)?.clone().unalias_nested().data {
            Expr::Column(c) => traced(&p.input, p.input.schema().index_of_column(&c).ok()?),
            _ => None,
        },
        LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => traced(plan.inputs()[0], i),
        LogicalPlan::Window(_) => Some(i),
        _ => None,
    }
}

/// What a view kept by key works out a key at a time: its `GROUP BY` over the rows it groups, or
/// the columns its windows partition by over the rows under them.
fn grouping(plan: &LogicalPlan) -> Option<(Vec<Expr>, &LogicalPlan)> {
    match plan {
        LogicalPlan::Aggregate(a) => Some((a.group_expr.clone(), &a.input)),
        LogicalPlan::Window(_) => partitioned(plan).map(|(by, input)| (by.into_iter().map(Expr::Column).collect(), input)),
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => grouping(plan.inputs()[0]),
        _ => None,
    }
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
    let (keys, joins, full, reason) = match how {
        How::ByKey => {
            let keys = keys(plan).map_err(|e| anyhow::anyhow!("{name} can't be kept by key: {e:#}. refresh = 'full' runs its query whole again"))?;
            ensure!(src.key.is_empty(), "{source} is keyed: a new row replaces its key's row, which says nothing of the group that row was in, so a view of it can't be kept by key. refresh = 'full' runs its query whole again");
            ensure!(src.merge.is_empty() && src.finish.is_none(), "{source} is a GROUP BY view, whose table keeps partial rows: make {name} from {source}'s own source, or refresh = 'full'");
            ensure!(src.history.is_none(), "{source} is a history view, whose __start_at and __end_at are worked out as it is read: make {name} with refresh = 'full'");
            ensure!(upstream.is_none_or(|u| u.sessions.is_none() || u.rerun.is_some()), "{source} cuts sessions as they close: make {name} from {source}'s own source");
            let joins = joins(lake, plan, sql, source).await.map_err(|e| anyhow::anyhow!("{name} can't be kept by key: {e:#}. refresh = 'full' runs its query whole again"))?;
            (keys, joins, None, why.unwrap_or_else(|| "asked for (refresh = 'by key')".into()))
        }
        How::Full => {
            let (reads, timed) = reads(lake, sql, source).await?;
            let timed = timed || changing(plan).is_some();
            (group_keys(plan), vec![], Some(Full { reads, timed }), why.unwrap_or_else(|| "asked for (refresh = 'full')".into()))
        }
    };
    crate::format::require(lake, 2, "a materialized view that runs its query again (median, count(DISTINCT …), ORDER BY … LIMIT, …)").await?;
    let ctx = crate::query::session(lake, sql, "").await?; // (the table's columns are the types the query runs with)
    let columns: Vec<(String, String)> = crate::query::run_schema(&ctx, plan)?.fields().iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect();
    if let Some((c, _)) = columns.iter().find(|(c, _)| crate::text::NAMES.contains(&c.as_str())) {
        bail!("{c} is a system column of the view's own table: name it something else ({c} AS source{c})");
    }
    let names: Vec<String> = columns.iter().map(|(c, _)| c.clone()).collect();
    let sql_types = crate::write::query_types(lake, sql, &names).await;
    let meta = TableMeta { columns, publish: default_publish(), ids: true, tiered: lake.visible(), sql_types, ..Default::default() };
    let r = Rerun { id: uuid::Uuid::new_v4().to_string(), keys, full, reason, asked, lag_secs, joins };
    let view = View { source: source.into(), sql: sql.into(), emit: None, sessions: None, ids: false, join: None, fill: None, expect: vec![], once: None, written: None, rerun: Some(r) };
    lake.cat.commit(vec![(table_key(name), json(&meta)), (crate::views::view_key(name), json(&view))], &[]).await
}

/// The tables of this lake a view kept by key joins under its windows (on their keys), its source
/// among them, each one whose changes it can follow by key; or why not. None when it joins none.
pub async fn joins(lake: &Lake, plan: &LogicalPlan, sql: &str, source: &str) -> Result<Vec<String>> {
    let Some(tables) = window(plan).and_then(partitioned).and_then(|(by, input)| joined(input, &by)) else { return Ok(vec![]) };
    let (here, elsewhere) = reads(lake, sql, source).await?;
    let mut joins: Vec<String> = vec![];
    for (t, _) in &tables {
        let name = scanned(t).and_then(|t| named(t, &here)).filter(|_| !elsewhere).context("its windows join a view or another lake's table")?;
        if joins.contains(name) {
            continue;
        }
        if name == source {
            joins.push(name.clone()); // (checked as the source)
            continue;
        }
        let meta: TableMeta = lake.cat.get(&table_key(name)).await?.with_context(|| format!("no table {name}"))?;
        let view = lake.cat.get::<View>(&crate::views::view_key(name)).await?;
        ensure!(meta.key.is_empty(), "{name} is keyed: a new row replaces its key's row, which says nothing of the key that row joined");
        ensure!(meta.merge.is_empty() && meta.finish.is_none() && meta.history.is_none(), "{name} keeps partial rows, or is a history view");
        ensure!(view.is_none_or(|u| u.sessions.is_none() || u.rerun.is_some()), "{name} cuts sessions as they close");
        joins.push(name.clone());
    }
    Ok(joins)
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

/// When each view last ran, how long it worked, and how many runs in a row failed: a view runs again
/// only once that long has passed since its run ended (nine times that for a full run, and its lag),
/// so its runs take at most half the time (a tenth for a full run), and one that keeps failing waits
/// longer each time (up to a minute). Its waits for the lake's lock and for its commit aren't work:
/// counted, a run behind a tiering round waited ten times that again.
///
/// A run slowed by the node's other work still counts as long, and its view then waited ten times
/// that after the other work was done: Nexmark's q5, 40 ms of work once the stream had ended, took
/// 0.3 s while it ran, and so waited three seconds more. So once what a view reads has been still for
/// `STILL` since a change its last run didn't see, it runs then, if that keeps its runs to half the
/// time (as a by-key view's): a burst's last rows are answered a moment after it. Such a run comes
/// once a wait (a stream sent in batches is still between them, and running at each slowed its
/// writes), and again only if a change overtook it while it ran: q5's came as the stream's last
/// windows were still being emitted, and its answer then came four seconds after it.
#[derive(Default)]
struct Pace {
    running: bool,
    started: Option<Instant>,
    took: Duration,
    failures: u32,
    error: Option<String>,
    /// When a timed view last ran whatever changed.
    timed: Option<Instant>,
    /// The commit its last run that worked was as of, whether that run came early (its tables still
    /// before its wait was over), and when it ended (ms).
    upto: u64,
    early: bool,
    ended: u64,
}

const STILL: Duration = Duration::from_secs(1);

crate::per_node!(static PACE: Mutex<BTreeMap<(String, String), Pace>> = Mutex::new(BTreeMap::new()));

/// A view's turn to run, given back when the run ends (or panics). `due`: a timed view's time to
/// run though nothing it reads here changed.
struct Turn {
    key: (String, String),
    due: bool,
    waited: std::sync::atomic::AtomicU64, // µs: the run's waits (`waiting`), left out of what it took
    upto: std::sync::atomic::AtomicU64, // the commit the run is as of
}

impl Drop for Turn {
    fn drop(&mut self) {
        if let Some(p) = PACE.lock().unwrap().get_mut(&self.key) {
            let waited = Duration::from_micros(self.waited.load(std::sync::atomic::Ordering::Relaxed));
            (p.running, p.took) = (false, p.started.map_or(Duration::ZERO, |s| s.elapsed().saturating_sub(waited)));
            p.ended = crate::log::now_ms();
        }
    }
}

/// `f`, its time added to what a run waited.
async fn waiting<T>(waited: &std::sync::atomic::AtomicU64, f: impl std::future::Future<Output = T>) -> T {
    let start = Instant::now();
    let out = f.await;
    waited.fetch_add(start.elapsed().as_micros() as u64, std::sync::atomic::Ordering::Relaxed);
    out
}

impl Turn {
    /// How the run went: an error is logged when it is new, not at every try.
    fn ended(&self, r: &Result<()>) {
        let mut pace = PACE.lock().unwrap();
        let Some(p) = pace.get_mut(&self.key) else { return };
        match r {
            Ok(()) => (p.failures, p.error, p.upto) = (0, None, p.upto.max(self.upto.load(std::sync::atomic::Ordering::Relaxed))),
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

/// `changed`: the newest commit that changed a table the view follows, and when it was made.
fn turn(lake: &Lake, view: &str, r: &Rerun, changed: Option<(u64, u64)>) -> Option<Turn> {
    let mut pace = PACE.lock().unwrap();
    let key = (lake.url.clone(), view.to_string());
    let p = pace.entry(key.clone()).or_default();
    let backoff = Duration::from_secs(if p.failures == 0 { 0 } else { (1u64 << p.failures.min(6)).min(60) / 2 });
    let least = Duration::from_secs(r.lag_secs.unwrap_or(0)).max(backoff);
    let spaced = if r.full.is_some() { p.took * 10 } else { p.took * 2 }; // (from its start; a full run reads everything)
    let since = p.started.map(|s| s.elapsed());
    let paced = since.is_none_or(|s| s >= spaced.max(least));
    let overtaken = |ms: u64| !p.early || ms <= p.ended; // (after an early run, a change made while it ran)
    let still = changed.is_some_and(|(n, ms)| n > p.upto && crate::log::now_ms().saturating_sub(ms) >= STILL.as_millis() as u64 && overtaken(ms));
    if p.running || !(paced || still && since.is_none_or(|s| s >= (p.took * 2).max(least))) {
        return None;
    }
    p.early = !paced;
    let every = r.lag_secs.map_or(TIMED, Duration::from_secs);
    let due = r.full.as_ref().is_some_and(|f| f.timed) && p.timed.is_none_or(|t| t.elapsed() >= every);
    (p.running, p.started) = (true, Some(Instant::now()));
    if due {
        p.timed = p.started;
    }
    Some(Turn { key, due, waited: Default::default(), upto: Default::default() })
}

/// Why a view's runs keep failing on this node (two in a row), if they do: making it says so
/// instead of waiting for a first run that won't come.
pub fn failing(lake: &Lake, view: &str) -> Option<String> {
    let pace = PACE.lock().unwrap();
    pace.get(&(lake.url.clone(), view.to_string())).filter(|p| p.failures >= 2).and_then(|p| p.error.clone())
}

/// Leader: start a run of every view that runs its query again whose turn it is, each in a task of
/// its own on the queries' runtime (a run that reads many rows never holds up the others, the
/// leader's other work, or appends and commits). A run is no loop: one that fails or panics is tried
/// again at a later turn.
pub async fn run_all(lake: &Arc<Lake>, seq: &Arc<crate::log::Sequencer>, lock: &Arc<tokio::sync::Mutex<()>>) -> Result<()> {
    let views: Vec<(String, View)> = lake.cat.scan::<View>("v/", "v0").await?.into_iter().filter(|(_, v)| v.rerun.is_some()).map(|(k, v)| (k[2..].to_string(), v)).collect();
    if views.is_empty() {
        CHANGED.lock().unwrap().remove(&lake.url);
        return Ok(());
    }
    look(lake).await?;
    for (name, v) in views {
        let Some(turn) = turn(lake, &name, v.rerun.as_ref().expect("filtered"), last_change(lake, &follows(&v))) else { continue };
        let (lake, seq, lock) = (lake.clone(), seq.clone(), lock.clone());
        crate::panics::aside(async move {
            let r = run(&lake, &seq, &lock, &name, &v, &turn).await;
            turn.ended(&r);
        });
    }
    Ok(())
}

crate::per_node!(
    /// Each lake's tables, with the newest commit that changed each and when it was made, as far as the
    /// log has been looked at: brought up to date from the commits since the last look (the first look
    /// starts at the log's end, and a lake no view runs again for is let go).
    static CHANGED: Mutex<BTreeMap<String, (u64, HashMap<String, (u64, u64)>)>> = Mutex::new(BTreeMap::new()));

async fn look(lake: &Lake) -> Result<()> {
    let now = lake.visible();
    let from = CHANGED.lock().unwrap().entry(lake.url.clone()).or_insert_with(|| (now, HashMap::new())).0;
    if now <= from {
        return Ok(());
    }
    let segments = lake.cat.scan::<Segment>(&seg_key(from + 1), &seg_key(now + 1)).await?;
    let mut seen = CHANGED.lock().unwrap();
    let (upto, at) = seen.get_mut(&lake.url).context("let go meanwhile")?;
    for (key, s) in segments {
        let n: u64 = key[2..].parse()?;
        for t in s.parts.keys().chain(s.files.keys()).filter(|t| moved(&s, t)) {
            at.insert(t.clone(), (n, s.ts_ms));
        }
    }
    *upto = (*upto).max(now);
    Ok(())
}

/// The newest commit that changed any of `tables` (`moved`), and when it was made.
fn last_change(lake: &Lake, tables: &[String]) -> Option<(u64, u64)> {
    let seen = CHANGED.lock().unwrap();
    let at = &seen.get(&lake.url)?.1;
    tables.iter().flat_map(|t| [at.get(t), at.get(&crate::sys::deleted(t))]).flatten().max().copied()
}

/// Has a log segment changed `source`'s rows: rows in (the log's, or a file commit's), old versions
/// out (`{t}$deleted`), files taken out or rows deleted from them (another engine's change)?
pub fn moved(s: &Segment, source: &str) -> bool {
    s.rows_of(source) > 0 || s.rows_of(&crate::sys::deleted(source)) > 0 || s.files.get(source).is_some_and(|f| !f.removed.is_empty() || !f.deleted.is_empty())
}

/// The tables whose changes a view's runs follow: its source and the tables its windows join, or
/// every table of this lake a full run reads.
pub fn follows(v: &View) -> Vec<String> {
    match v.rerun.as_ref() {
        Some(Rerun { full: Some(f), .. }) => f.reads.clone(),
        r => {
            let mut tables = vec![v.source.clone()];
            tables.extend(r.iter().flat_map(|r| &r.joins).filter(|t| **t != v.source).cloned());
            tables
        }
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
async fn run(lake: &Lake, seq: &crate::log::Sequencer, lock: &tokio::sync::Mutex<()>, view: &str, v: &View, turn: &Turn) -> Result<()> {
    let (due, waited) = (turn.due, &turn.waited);
    let r = v.rerun.as_ref().context("not run again")?;
    let producer = producer(view);
    // (each step's time since the run began, to stderr: where a slow view's runs go)
    let t0 = Instant::now();
    let tr = |w: &str| if crate::env::is_set("PONDRA_TRACE_RERUN") { eprintln!("rerun {view}: {w} {:.3} s", t0.elapsed().as_secs_f64()) };
    let done: Option<u64> = lake.cat.get(&producer_key(&producer)).await?;
    let mut now = lake.visible();
    if now == 0 {
        seq.number().await?; // (a lake with no commit yet: its first, so the run has one to be as of)
        return Ok(());
    }
    turn.upto.store(now, std::sync::atomic::Ordering::Relaxed);
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
            let tables = follows(v);
            let mut whole = true;
            for t in &tables {
                let meta: TableMeta = lake.cat.get(&table_key(t)).await?.with_context(|| format!("no table {t}"))?;
                whole &= crate::past::kept_since(&meta).is_none_or(|(kept, _)| kept <= d);
            }
            match whole {
                true if !moved_since(lake, d, now, &tables).await? => return Ok(()),
                true => Some(touched(lake, v, d, now).await?),
                false => None, // (its changes no longer whole: every group)
            }
        }
        _ => None, // (every group)
    };
    tr(&format!("touched {} keys", touched.as_ref().map_or(0, |t| t.iter().map(|b| b.num_rows()).sum::<usize>())));
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
        tr(&format!("worked out {} rows", new.iter().map(|b| b.num_rows()).sum::<usize>()));
        // The view's rows as they are, and what came out different, worked out before the lake's
        // lock: a by-key run's take a second or two, and every other view's run waited for them.
        // Under the lock they stand when no commit has changed the view's table since (only its own
        // runs write it), else they are worked out again there.
        let seen = lake.visible();
        let before: TableMeta = lake.cat.get(&table_key(view)).await?.context("view without table")?;
        let read = match none {
            true => vec![],
            false => old_rows(lake, view, &r.keys, scanned.as_ref(), seen).await?,
        };
        tr(&format!("old rows {}", read.iter().map(|b| b.num_rows()).sum::<usize>()));
        let changed = diff(&before, &r.keys, &read, &new)?;
        tr("diff");
        // Under the lake's lock, as a change is (invariant 56): this view still the one this run is
        // for (dropped and made again meanwhile, it's another), and its rows as they are.
        let guard = waiting(waited, lock.lock()).await;
        tr("locked");
        if lake.cat.get::<View>(&crate::views::view_key(view)).await?.and_then(|v| v.rerun).is_none_or(|x| x.id != r.id) {
            return Ok(());
        }
        let meta: TableMeta = lake.cat.get(&table_key(view)).await?.context("view without table")?;
        crate::change::companion(lake, view, &meta).await?; // (its `$deleted`, which every run names)
        let meta: TableMeta = lake.cat.get(&table_key(view)).await?.context("view without table")?;
        let at = lake.visible();
        let stands = none || meta.columns == before.columns && !moved_since(lake, seen, at, &[view.to_string()]).await?;
        let (old, new) = match stands {
            true => changed,
            false => diff(&meta, &r.keys, &old_rows(lake, view, &r.keys, scanned.as_ref(), at).await?, &new)?,
        };
        if fresh {
            if old.num_rows() == 0 && new.num_rows() == 0 {
                return Ok(()); // (a timed run, nothing new here, and the same answer: nothing to say)
            }
            // (its answer moved with time alone: a commit of its own to be as of, and again)
            drop(guard);
            (now, fresh) = (waiting(waited, seq.number()).await?.version, false);
            turn.upto.store(now, std::sync::atomic::Ordering::Relaxed);
            continue;
        }
        let (mut pending, _) = crate::change::appends(lake, seq, view, &meta, vec![old], vec![new], &producer, false).await?; // (a view's rows: upserts)
        if pending.is_empty() {
            // (nothing came out different: only its progress moves, both parts' together)
            let empty = |table: String, s: SchemaRef| crate::log::Append { table, src: crate::log::Src { producer: String::new(), seq: 0, prev: None }, batch: RecordBatch::new_empty(s), ack: tokio::sync::oneshot::channel().0 };
            let s = crate::answer::schema(&meta.columns)?;
            pending = vec![empty(view.to_string(), s.clone()), empty(crate::sys::deleted(view), s)];
        }
        // Both parts under the same seq and `prev`: they always move together, so a run that lost
        // the race (another leader's, or a retry) is refused whole.
        for a in &mut pending {
            let name = if a.table == view { producer.clone() } else { format!("{producer}:deleted") };
            a.src = crate::log::Src { producer: name, seq: now, prev: Some(done.unwrap_or(0)) };
        }
        tr("appends");
        if let crate::log::Outcome::Acks(acks) = waiting(waited, crate::change::submit(lake, seq, &pending)).await? {
            if acks.iter().any(|a| a.conflict) {
                eprintln!("materialized view {view}: another run committed first; the next one reads on from it");
            }
        }
        tr("committed");
        return Ok(());
    }
}

/// The keys commits (done, now] touched: the view's `GROUP BY` (or its windows' key) over the rows
/// they changed (new ones, the old versions they replaced or deleted, the rows other engines took
/// out), under its `WHERE`, each key once.
async fn touched(lake: &Lake, v: &View, done: u64, now: u64) -> Result<Vec<RecordBatch>> {
    let joins = v.rerun.as_ref().map_or(&[][..], |r| &r.joins);
    let ctx = session_at(lake, &v.sql, "", Some(now)).await?;
    if !joins.is_empty() {
        // (windows over a join: each table's changed rows' columns equal to the keys, as the keys' types)
        let plan = crate::engine::sql(&ctx, &crate::asof::rewrite(&v.sql)?).await?.into_unoptimized_plan();
        let (by, input) = window(&plan).and_then(partitioned).context("a view kept by key over a join is windows within keys")?;
        let tables = joined(input, &by).context("its windows' join is no longer on the columns they partition by: make it again")?;
        let mut keys: Option<DataFrame> = None;
        for (t, cols) in tables {
            let name = scanned(&t).and_then(|t| named(t, joins)).context("its windows join a table the view doesn't follow")?;
            let k = cols.iter().zip(&by).enumerate().map(|(i, (c, k))| Ok(cast(ident(&c.name), input.schema().qualified_field_from_column(k)?.1.data_type().clone()).alias(format!("__k{i}"))));
            let k = changed(lake, name, done, now).await?.select(k.collect::<Result<Vec<_>>>()?)?;
            keys = Some(match keys {
                Some(all) => all.union(k)?,
                None => k,
            });
        }
        return Ok(keys.context("no table")?.distinct()?.collect().await?);
    }
    ctx.deregister_table(table_ref(&v.source))?;
    ctx.register_table(table_ref(&v.source), changed(lake, &v.source, done, now).await?.into_view())?;
    let plan = crate::engine::sql(&ctx, &crate::asof::rewrite(&v.sql)?).await?.into_unoptimized_plan();
    let (by, input) = grouping(&plan).context("a view kept by key is a GROUP BY or windows within keys")?;
    let keys: Vec<Expr> = by.iter().enumerate().map(|(i, g)| g.clone().unalias().alias(format!("__k{i}"))).collect();
    let p = LogicalPlanBuilder::from(input.clone()).project(keys)?.distinct()?.build()?;
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
    let columns: Vec<(String, DataType)> = meta.logical_columns().iter().map(|(c, _)| Ok((c.clone(), new.schema().field_with_unqualified_name(c)?.data_type().clone()))).collect::<Result<_>>()?;
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
        let stored = crate::answer::schema(&crate::sys::with_sys(&meta).columns)?;
        let out = out.iter().map(|b| meta.to_logical(&crate::query::conform(b, &stored)?)).collect::<Result<Vec<_>>>()?;
        all = all.union(pick(ctx.read_batches(out)?)?)?;
    }
    Ok(all)
}

/// The keys a run works out again: a table of them to join with, and each key's least and
/// greatest value, when it holds no NULL and is of a type files keep ranges of (not a float).
struct Touched {
    plan: LogicalPlan,
    ranges: Vec<Option<(ScalarValue, ScalarValue)>>,
}

/// The touched keys as a table to join with: one batch of their own (a `DISTINCT`'s batches are
/// slices that each count the whole of what it held).
fn scan(rows: Vec<RecordBatch>) -> Result<Touched> {
    let rows = datafusion::arrow::compute::concat_batches(&rows[0].schema(), &rows)?;
    let exact = |c: &ArrayRef| c.null_count() == 0 && crate::manifest::orderable(c.data_type()) && !c.data_type().is_floating();
    let ranges = rows.columns().iter().map(|c| exact(c).then(|| crate::sketch::min_max(std::slice::from_ref(c))).flatten()).collect();
    let t = datafusion::datasource::MemTable::try_new(rows.schema(), vec![vec![rows]])?;
    Ok(Touched { plan: LogicalPlanBuilder::scan(TOUCHED, datafusion::datasource::provider_as_source(Arc::new(t)), None)?.build()?, ranges })
}

fn key(i: usize) -> Expr { Expr::Column(Column::new(Some(TOUCHED), format!("__k{i}"))) }

/// The view's rows as of `now`: of the touched keys only (their rows, and nothing else, go into
/// the `GROUP BY` or the windows), or of every key.
async fn worked_out(lake: &Lake, v: &View, now: u64, touched: Option<&Touched>) -> Result<Vec<RecordBatch>> {
    let ctx = session_at(lake, &v.sql, "", Some(now)).await?;
    let plan = crate::engine::sql(&ctx, &crate::asof::rewrite(&v.sql)?).await?.into_unoptimized_plan();
    let plan = match touched {
        Some(t) => {
            kept_as_written(&ctx);
            restrict(&plan, t)?
        }
        None => plan,
    };
    Ok(ctx.execute_logical_plan(plan).await?.collect().await?)
}

/// `plan` with its `GROUP BY` or its windows reading only the touched keys' rows: a semi join on
/// its keys (`IS NOT DISTINCT FROM`: a NULL key is a group too). A window sees every row of each
/// partition it keeps, so each comes out as it would over every row.
fn restrict(plan: &LogicalPlan, touched: &Touched) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Aggregate(a) => LogicalPlan::Aggregate(Aggregate::try_new(Arc::new(semi(&a.input, &a.group_expr, touched)?), a.group_expr.clone(), a.aggr_expr.clone())?),
        LogicalPlan::Window(_) => {
            let (by, _) = grouping(plan).context("windows kept by key share a PARTITION BY column")?;
            within(plan, &by, touched)?
        }
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => plan.with_new_exprs(plan.expressions(), vec![restrict(plan.inputs()[0], touched)?])?,
        _ => bail!("a view kept by key is a GROUP BY or windows within keys"),
    })
}

/// A stack of windows over only the touched keys' rows: each table a join under them reads cut to
/// them before it is joined, or the rows under them as they come.
fn within(plan: &LogicalPlan, by: &[Expr], touched: &Touched) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Window(w) => LogicalPlan::Window(datafusion::logical_expr::Window::try_new(w.window_expr.clone(), Arc::new(within(&w.input, by, touched)?))?),
        _ => match by.iter().map(|e| e.try_as_col().cloned()).collect::<Option<Vec<_>>>().and_then(|by| joined(plan, &by)) {
            Some(tables) => cut(plan, &tables, touched)?,
            None => semi(plan, by, touched)?,
        },
    })
}

/// `plan` with each of `tables` cut to its rows of the touched keys.
fn cut(plan: &LogicalPlan, tables: &[(LogicalPlan, Vec<Column>)], touched: &Touched) -> Result<LogicalPlan> {
    if let Some((_, by)) = tables.iter().find(|(t, _)| t == plan) {
        return semi(plan, &by.iter().cloned().map(Expr::Column).collect::<Vec<_>>(), touched);
    }
    let inputs = plan.inputs().into_iter().map(|i| cut(i, tables, touched)).collect::<Result<Vec<_>>>()?;
    Ok(plan.with_new_exprs(plan.expressions(), inputs)?)
}

/// `input`'s rows whose `by` is a touched key. The touched keys are the side the join holds, and
/// `input`'s rows stream past them as they are read. A key column is first kept to the touched
/// keys' range: the join can't skip anything, but the scans below skip the files and batches a
/// filter rules out (the newest auctions' bids, not every bid), and every row the join keeps is
/// in that range.
fn semi(input: &LogicalPlan, by: &[Expr], touched: &Touched) -> Result<LogicalPlan> {
    let on: Vec<Expr> = by.iter().enumerate().map(|(i, g)| same(g.clone().unalias(), key(i))).collect();
    let ranged = by.iter().zip(&touched.ranges).filter_map(|(g, r)| Some(g.clone().unalias().try_as_col()?.clone()).zip(r.clone()));
    let mut input = LogicalPlanBuilder::from(input.clone());
    if let Some(f) = ranged.map(|(c, (lo, hi))| Expr::Column(c).between(lit(lo), lit(hi))).reduce(Expr::and) {
        input = input.filter(f)?;
    }
    Ok(LogicalPlanBuilder::from(touched.plan.clone()).join_on(input.build()?, JoinType::RightSemi, on)?.build()?)
}

/// A run by key's session joins as `semi` says. DataFusion would swap the sides by its estimates,
/// which count a table by its files' compressed bytes, and so held the whole table instead: every
/// batch out of the join then pointed into all of it, and a window's sort above it counted
/// gigabytes and failed (Nexmark's q18 at 2M events). The view's own joins (windows over tables
/// joined on their key) are then run as written too.
fn kept_as_written(ctx: &SessionContext) {
    ctx.state_ref().write().config_mut().options_mut().optimizer.join_reordering = false;
}

/// The view's rows of the touched groups (or all of them) as of commit `at`, with their `_row_id`,
/// `_created_at` and `_version`.
async fn old_rows(lake: &Lake, view: &str, keys: &[String], touched: Option<&Touched>, at: u64) -> Result<Vec<RecordBatch>> {
    let sql = format!("SELECT *, {ROW_ID}, {CREATED}, {VERSION} FROM {}", crate::write::sql_name(view));
    let ctx = session_at(lake, &sql, "", Some(at)).await?;
    let mut plan = ctx.sql(&crate::sys::hide(&sql)).await?.into_unoptimized_plan(); // (`*` without the system columns)
    if let Some(t) = touched {
        kept_as_written(&ctx);
        plan = semi(&plan, &keys.iter().map(|k| ident(k)).collect::<Vec<_>>(), t)?;
    }
    Ok(ctx.execute_logical_plan(plan).await?.collect().await?)
}

/// What a run changes: the old rows (with their system columns) that aren't among the new ones,
/// and the new rows that aren't among the old, each with the id and time of the old row of its
/// key it replaces (an update in the change feed), or none (a new group). Rows compared whole, by
/// Arrow's row format: NULLs equal, as a key's are. No `keys`: a row's key is all of it.
fn diff(meta: &TableMeta, keys: &[String], old: &[RecordBatch], new: &[RecordBatch]) -> Result<(RecordBatch, RecordBatch)> {
    use datafusion::arrow::compute::{concat_batches, filter_record_batch, take, take_record_batch};
    use datafusion::arrow::row::{RowConverter, SortField};
    let s = crate::answer::schema(&meta.columns)?;
    let mut fields = s.fields().to_vec();
    let ts = crate::sys::time();
    fields.extend([Field::new(ROW_ID, DataType::Int64, true), Field::new(CREATED, ts, true), Field::new(VERSION, DataType::Int64, true)].map(Arc::new));
    let olds = Arc::new(Schema::new(fields));
    let old = concat_batches(&olds, &old.iter().map(|b| crate::query::conform(b, &olds)).collect::<Result<Vec<_>>>()?)?;
    let new = concat_batches(&s, &new.iter().map(|b| crate::query::cast_as(b, &s)).collect::<Result<Vec<_>>>()?)?;
    let all: Vec<usize> = (0..s.fields().len()).collect();
    let at: Vec<usize> = match keys.is_empty() {
        true => all.clone(), // (no keys: rows matched whole)
        false => keys.iter().map(|k| s.index_of(k)).collect::<Result<_, _>>()?,
    };
    // (one converter per pair, so the rows of old and new compare)
    let pair = |cols: &[usize], new: &RecordBatch| -> Result<(datafusion::arrow::row::Rows, datafusion::arrow::row::Rows)> {
        let c = RowConverter::new(cols.iter().map(|&i| SortField::new(s.field(i).data_type().clone())).collect())?;
        let get = |b: &RecordBatch| cols.iter().map(|&i| b.column(i).clone()).collect::<Vec<ArrayRef>>();
        Ok((c.convert_columns(&get(&old))?, c.convert_columns(&get(new))?))
    };
    let (old_key, new_key) = pair(&at, &new)?;
    let mut by_key: HashMap<&[u8], Vec<usize>> = HashMap::new();
    for i in 0..old.num_rows() {
        by_key.entry(old_key.row(i).data()).or_default().push(i);
    }
    // (a new row whose key has no old row is new, so its whole row is never converted)
    let matched: Vec<usize> = (0..new.num_rows()).filter(|&j| by_key.contains_key(new_key.row(j).data())).collect();
    // (no keys: the key is the whole row, so a row found by it is the same)
    let whole = match keys.is_empty() {
        true => None,
        false => Some(pair(&all, &take_record_batch(&new, &UInt32Array::from(matched.iter().map(|&j| j as u32).collect::<Vec<_>>()))?)?),
    };
    let mut gone = vec![true; old.num_rows()];
    let mut kept = vec![false; new.num_rows()];
    for (m, &j) in matched.iter().enumerate() {
        let same = by_key[new_key.row(j).data()].iter().copied().find(|&i| gone[i] && whole.as_ref().is_none_or(|(o, n)| o.row(i) == n.row(m)));
        if let Some(i) = same {
            gone[i] = false; // (came out the same: stays as it is)
            kept[j] = true;
        }
    }
    let put: Vec<usize> = (0..new.num_rows()).filter(|&j| !kept[j]).collect();
    let mut replaced = vec![false; old.num_rows()];
    let mut prior: Vec<Option<u32>> = vec![];
    for &j in &put {
        let i = by_key.get(new_key.row(j).data()).and_then(|is| is.iter().copied().find(|&i| gone[i] && !replaced[i]));
        if let Some(i) = i {
            replaced[i] = true;
        }
        prior.push(i.map(|i| i as u32));
    }
    let from_old = UInt32Array::from(prior);
    let mut columns: Vec<ArrayRef> = match put.len() == new.num_rows() {
        true => new.columns().to_vec(), // (every new row, in order: nothing to take)
        false => {
            let picked = UInt32Array::from(put.iter().map(|&j| j as u32).collect::<Vec<_>>());
            new.columns().iter().map(|c| take(c, &picked, None)).collect::<Result<_, _>>()?
        }
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray, Int64Array, StringArray, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{Int64Type, TimestampMicrosecondType};

    /// The batch's column names.
    fn names(b: &RecordBatch) -> Vec<String> {
        b.schema().fields().iter().map(|f| f.name().clone()).collect()
    }

    /// The batch's rows as text, one per row: values as they are, NULL as `NULL`, timestamps as micros.
    fn text(b: &RecordBatch) -> Vec<String> {
        (0..b.num_rows())
            .map(|r| {
                b.columns()
                    .iter()
                    .map(|c| match c.is_null(r) {
                        true => "NULL".to_string(),
                        false => match c.data_type() {
                            DataType::Utf8 => c.as_string::<i32>().value(r).to_string(),
                            DataType::Int64 => c.as_primitive::<Int64Type>().value(r).to_string(),
                            DataType::Timestamp(_, _) => c.as_primitive::<TimestampMicrosecondType>().value(r).to_string(),
                            t => panic!("no text for {t}"),
                        },
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect()
    }

    /// A view's rows as the table holds them: (k, v) and the system columns.
    fn old_batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            ("k", Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(5), Some(5), None])) as ArrayRef),
            ("v", Arc::new(StringArray::from(vec!["a", "b", "c", "p", "q", "n"])) as ArrayRef),
            (ROW_ID, Arc::new(Int64Array::from(vec![10, 20, 30, 50, 51, 60])) as ArrayRef),
            (CREATED, Arc::new(TimestampMicrosecondArray::from(vec![100, 200, 300, 500, 510, 600]).with_timezone("UTC")) as ArrayRef),
            (VERSION, Arc::new(Int64Array::from(vec![1_i64; 6])) as ArrayRef),
        ])
        .unwrap()
    }

    /// A run's rows: (k, v) only. (5, q) is the old (5, q) again; the two (5, r) rows are equal to each other, not to any old row.
    fn new_batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            ("k", Arc::new(Int64Array::from(vec![Some(4), Some(1), Some(2), Some(5), Some(5), Some(5), None])) as ArrayRef),
            ("v", Arc::new(StringArray::from(vec!["d", "a", "x", "q", "r", "r", "m"])) as ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn diff_compares_whole_rows_only_for_old_keys() {
        let meta = TableMeta { columns: vec![("k".into(), "Int64".into()), ("v".into(), "Utf8".into())], ..Default::default() };
        let (old, new) = (vec![old_batch()], vec![new_batch()]);

        let (gone, put) = diff(&meta, &["k".to_string()], &old, &new).unwrap();
        assert_eq!(names(&gone), ["k", "v", ROW_ID, CREATED, VERSION]);
        assert_eq!(text(&gone), ["2,b,20,200,1", "3,c,30,300,1", "5,p,50,500,1", "NULL,n,60,600,1"]);
        assert_eq!(names(&put), ["k", "v", ROW_ID, CREATED]);
        assert_eq!(text(&put), ["4,d,NULL,NULL", "2,x,20,200", "5,r,50,500", "5,r,NULL,NULL", "NULL,m,60,600"]);

        let (gone, put) = diff(&meta, &[], &old, &new).unwrap();
        assert_eq!(names(&gone), ["k", "v", ROW_ID, CREATED, VERSION]);
        assert_eq!(text(&gone), ["2,b,20,200,1", "3,c,30,300,1", "5,p,50,500,1", "NULL,n,60,600,1"]);
        assert_eq!(names(&put), ["k", "v", ROW_ID, CREATED]);
        assert_eq!(text(&put), ["4,d,NULL,NULL", "2,x,NULL,NULL", "5,r,NULL,NULL", "5,r,NULL,NULL", "NULL,m,NULL,NULL"]);
    }
}
