//! Materialized views kept current by running their query again (ADR-056, ADR-059): what the
//! rows alone can't keep.
//!
//! **By key**: a `GROUP BY` whose answers can't be added up as rows arrive (`median`, percentiles,
//! `count(DISTINCT …)`, `string_agg`, `array_agg`, …), windows that all partition by columns the
//! view selects (each key's rows come from that key's source rows alone: the latest bid of each
//! bidder, the top ten of each auction), over one table or tables joined on those columns (each
//! auction's winning bid), or one asked to be (`refresh = 'by key'`). Windows and `GROUP BY`s may be
//! stacked over one table (a `MATCH_RECOGNIZE`'s stack, with projections, filters and unnests between)
//! when every node keeps a key's rows apart. After commits the leader
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
use crate::codes::coded;
use crate::query::{session_at, table_ref};
use crate::store::*;
use crate::text::{CREATED, ROW_ID, VERSION};
use crate::views::View;
use anyhow::{bail, ensure, Context, Result};
use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, Transformed};
use datafusion::common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion::logical_expr::{Aggregate, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Volatility};
use datafusion::prelude::{cast, ident, lit, DataFrame, SessionContext};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use datafusion::common::instant::Instant;
use std::time::Duration;

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

/// What a view needs to be kept by key: a stack of windows and `GROUP BY`s over one table of this lake
/// read alone (the module doc's rule: each node keeps a key's rows apart, and nothing in it answers
/// otherwise as time passes). Its key columns by the view's names, or why not (a clause that reads
/// after "can't be kept by key: ").
pub fn keys(plan: &LogicalPlan) -> Result<Vec<String>> {
    if joined_windows(plan).is_some() {
        return window_keys(plan);
    }
    let top = peeled(plan);
    let k = keyed(plan)?;
    let names = k.by.iter().map(|key| {
        (0..top.schema().fields().len()).find(|&j| to_base(top, &column_at(top.schema(), j)).as_ref() == Some(key)).map(|j| top.schema().field(j).name().clone())
    });
    let why = if k.grouped { "a GROUP BY expression isn't selected, so a row doesn't say which group it is" } else { "a column its windows partition by isn't selected as it is, so a row doesn't say which window it is in" };
    let names = names.collect::<Option<Vec<_>>>().context(why)?;
    if let Some(f) = changing(plan) {
        bail!("{f}() answers otherwise as time passes");
    }
    Ok(names)
}

/// What a view of windows over tables joined on their keys needs to be kept by key (`joined`): each
/// table read alone under them, and each column the windows partition by selected as it is (so a row
/// says which key it is). Those columns, by the view's names, in the first window's order.
fn window_keys(plan: &LogicalPlan) -> Result<Vec<String>> {
    let w = window(plan).context("a window over all its rows, or windows that share no PARTITION BY column, look across every row")?;
    let (by, input) = partitioned(w).context("a window over all its rows, or windows that share no PARTITION BY column, look across every row")?;
    let mut scans = 0;
    plan.apply_with_subqueries(|n| {
        scans += matches!(n, LogicalPlan::TableScan(_)) as usize;
        Ok(TreeNodeRecursion::Continue)
    })?;
    let tables = joined(input, &by).map_or(1, |t| t.len());
    ensure!(scans == tables, "its windows read a GROUP BY, a subquery, a keyed table or an outer join, or tables not joined on every column they partition by");
    let top = peeled(plan);
    let names = by.iter().map(|c| {
        let i = input.schema().index_of_column(c).ok()?; // (a window keeps its input's columns first, at their places)
        (0..top.schema().fields().len()).find(|&j| traced(top, j) == Some(i)).map(|j| top.schema().field(j).name().clone())
    });
    let names = names.collect::<Option<Vec<_>>>().context("a column its windows partition by isn't selected as it is, so a row doesn't say which window it is in")?;
    if let Some(f) = changing(plan) {
        bail!("{f}() answers otherwise as time passes");
    }
    Ok(names)
}

/// The tables of a view of windows over tables joined on their keys (`joined`), when it is one: its
/// own path (`window_keys`, `restrict_joined`, the joins `touched` reads).
fn joined_windows(plan: &LogicalPlan) -> Option<Vec<(LogicalPlan, Vec<Column>)>> {
    window(plan).and_then(partitioned).and_then(|(by, input)| joined(input, &by))
}

/// The plan under a top ORDER BY with no LIMIT: rows are a set, so the order doesn't change what a
/// key's rows are.
fn peeled(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Sort(s) if s.fetch.is_none() => peeled(&s.input),
        _ => plan,
    }
}

/// Whether an expression reads rows other than its own node's: a subquery, or an outer reference
/// into one. A node with such an expression isn't key-local.
fn reads_other_rows(e: &Expr) -> bool {
    e.exists(|e| Ok(matches!(e, Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) | Expr::OuterReferenceColumn(_, _)))).unwrap_or(true)
}

/// The view's windows, under its projections, filters and aliases.
fn window(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    let plan = peeled(plan);
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
        if reads_other_rows(e) {
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

/// The stack a view kept by key works out a key at a time over: its key nodes (windows and `GROUP BY`s,
/// top down), and the keys `by`: each key node's keys over the table that every key node has too, in
/// the lowest node's order. `low`: the lowest key node's input, and `low_by`: `by` as that node writes
/// them over `low` (what `touched` reads: only the rows under the lowest node). `grouped`: a `GROUP BY`
/// is a node.
struct Keyed {
    low: LogicalPlan,
    by: Vec<Expr>,
    low_by: Vec<Expr>,
    grouped: bool,
}

/// The keys of `plan`'s stack (its top past an ORDER BY with no LIMIT), over one table: `Keyed`, or why
/// it isn't key-local.
fn keyed(plan: &LogicalPlan) -> Result<Keyed> {
    let top = peeled(plan);
    let mut nodes = vec![];
    stacked(top, &mut nodes)?;
    ensure!(!nodes.is_empty(), "ORDER BY, LIMIT, DISTINCT or a window over its groups looks across groups");
    let grouped = nodes.iter().any(|n| matches!(n, LogicalPlan::Aggregate(_)));
    let low = nodes.last().context("a key node")?.inputs()[0].clone();
    let lists: Vec<Vec<(Expr, Expr)>> = nodes.iter().map(|n| node_keys(n)).collect();
    let (lowest, above) = lists.split_last().context("a key node")?;
    let (mut by, mut low_by): (Vec<Expr>, Vec<Expr>) = (vec![], vec![]);
    for (raw, base) in lowest {
        if !by.contains(base) && above.iter().all(|l| l.iter().any(|(_, b)| b == base)) {
            by.push(base.clone());
            low_by.push(raw.clone());
        }
    }
    let why = if grouped { "its GROUP BYs and windows share no column of the table, so a key's rows don't come from its own rows alone" } else { "a window over all its rows, or windows that share no PARTITION BY column, look across every row" };
    ensure!(!by.is_empty(), "{why}");
    Ok(Keyed { low, by, low_by, grouped })
}

/// A stack from its top down to its table, which it returns: each node key-local (a projection, filter,
/// alias, unnest, window or GROUP BY, with no expression that reads other rows), with the key nodes
/// pushed to `nodes`, top down. A LIMIT, a DISTINCT or a join looks across keys.
fn stacked<'a>(plan: &'a LogicalPlan, nodes: &mut Vec<&'a LogicalPlan>) -> Result<&'a LogicalPlan> {
    ensure!(!plan.expressions().iter().any(reads_other_rows), "a subquery or an outer reference reads other rows than a key's");
    match plan {
        LogicalPlan::TableScan(_) => Ok(plan),
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) | LogicalPlan::Unnest(_) => stacked(plan.inputs()[0], nodes),
        LogicalPlan::Window(w) => {
            nodes.push(plan);
            stacked(&w.input, nodes)
        }
        LogicalPlan::Aggregate(a) => {
            ensure!(!a.group_expr.is_empty() && !a.group_expr.iter().any(|e| matches!(e, Expr::GroupingSet(_))), "it has no GROUP BY, or GROUPING SETS, ROLLUP or CUBE");
            nodes.push(plan);
            stacked(&a.input, nodes)
        }
        LogicalPlan::Limit(_) => bail!("a LIMIT looks across every key (ORDER BY … LIMIT keeps the first rows of all of them)"),
        LogicalPlan::Sort(s) if s.fetch.is_some() => bail!("a LIMIT looks across every key (ORDER BY … LIMIT keeps the first rows of all of them)"),
        LogicalPlan::Sort(_) => bail!("an ORDER BY under the view's top looks across its rows"),
        LogicalPlan::Distinct(_) => bail!("DISTINCT looks across its rows"),
        LogicalPlan::Join(_) if nodes.iter().any(|n| matches!(n, LogicalPlan::Window(_))) => {
            bail!("it reads more than one table: windows over a join are kept by key only over tables joined on every column they partition by, with no keyed table or outer join")
        }
        LogicalPlan::Join(_) => bail!("it reads more than one table"),
        _ => bail!("it reads its rows another way than a key's own"),
    }
}

/// The keys one key node has, each as the node writes it over its input (`raw`) and over the table
/// (`base`): a window's partition columns that every one of its functions partitions by, or a `GROUP
/// BY`'s expressions. Those that trace to the table only.
fn node_keys(n: &LogicalPlan) -> Vec<(Expr, Expr)> {
    match n {
        LogicalPlan::Window(w) => {
            let mut shared: Option<Vec<Column>> = None;
            for e in &w.window_expr {
                let cols: Vec<Column> = match e.clone().unalias_nested().data {
                    Expr::WindowFunction(f) => f.params.partition_by.iter().filter_map(|p| match p {
                        Expr::Column(c) => Some(c.clone()),
                        _ => None,
                    }).collect(),
                    _ => vec![],
                };
                shared = Some(match shared {
                    None => cols,
                    Some(s) => s.into_iter().filter(|c| cols.contains(c)).collect(),
                });
            }
            shared.unwrap_or_default().into_iter().map(Expr::Column).filter_map(|raw| to_base(&w.input, &raw).map(|base| (raw, base))).collect()
        }
        LogicalPlan::Aggregate(a) => a.group_expr.iter().map(|g| g.clone().unalias_nested().data).filter_map(|raw| to_base(&a.input, &raw).map(|base| (raw, base))).collect(),
        _ => vec![],
    }
}

/// `e`, an expression over `p`'s output, as one over the table's columns: through the nodes under `p`.
/// None where a column isn't carried unchanged from the table (a window's or GROUP BY's result, an
/// unnested column, a join's column).
fn to_base(p: &LogicalPlan, e: &Expr) -> Option<Expr> {
    match p {
        LogicalPlan::TableScan(_) => Some(e.clone()),
        LogicalPlan::Filter(f) => to_base(&f.input, e),
        LogicalPlan::Projection(pr) => subst(e, p.schema(), &|j: usize| to_base(&pr.input, &pr.expr.get(j)?.clone().unalias_nested().data)),
        LogicalPlan::SubqueryAlias(s) => subst(e, p.schema(), &|j: usize| to_base(&s.input, &column_at(s.input.schema(), j))),
        LogicalPlan::Window(w) => subst(e, p.schema(), &|j: usize| if j < w.input.schema().fields().len() { to_base(&w.input, &column_at(w.input.schema(), j)) } else { None }),
        LogicalPlan::Aggregate(a) => subst(e, p.schema(), &|j: usize| to_base(&a.input, &a.group_expr.get(j)?.clone().unalias_nested().data)),
        LogicalPlan::Unnest(u) => subst(e, p.schema(), &|j: usize| {
            let d = *u.dependency_indices.get(j)?;
            let unnested = u.list_type_columns.iter().any(|(i, _)| *i == d) || u.struct_type_columns.contains(&d) || u.exec_columns.iter().any(|c| u.input.schema().index_of_column(c).ok() == Some(d));
            if unnested { None } else { to_base(&u.input, &column_at(u.input.schema(), d)) }
        }),
        _ => None,
    }
}

/// `e` with each column of `schema` replaced by what `f` gives its place; None if one has nothing.
fn subst(e: &Expr, schema: &DFSchema, f: &dyn Fn(usize) -> Option<Expr>) -> Option<Expr> {
    let mut whole = true;
    let out = e.clone().transform_up(|x| {
        if let Expr::Column(c) = &x {
            match schema.index_of_column(c).ok().and_then(|j| f(j)) {
                Some(r) => return Ok(Transformed::yes(r)),
                None => whole = false,
            }
        }
        Ok(Transformed::no(x))
    });
    let out = out.ok()?;
    whole.then_some(out.data)
}

/// Column `j` of `schema`, as an expression.
fn column_at(schema: &DFSchema, j: usize) -> Expr {
    let (q, f) = schema.qualified_field(j);
    Expr::Column(Column::new(q.cloned(), f.name()))
}

/// `plan` with its table's scan replaced by `f` of it, the nodes over it as they are.
fn cut_scan(plan: &LogicalPlan, f: &dyn Fn(&LogicalPlan) -> Result<LogicalPlan>) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::TableScan(_) => f(plan),
        _ => Ok(plan.with_new_exprs(plan.expressions(), vec![cut_scan(plan.inputs()[0], f)?])?),
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

/// Why a view can't follow its source's rows as they come, or by key: the table is a GROUP BY view's table, keyed, or a
/// history view's (`create`'s refusals, and `matches::create`'s).
#[derive(Clone, Copy)]
pub enum Unfollow {
    Keyed,
    Partial,
    History,
}

/// The reason a view can't follow `src`, in the order the refusals check them; None when it can.
pub fn unfollow(src: &TableMeta) -> Option<Unfollow> {
    if !src.merge.is_empty() || src.finish.is_some() {
        Some(Unfollow::Partial) // (a GROUP BY view's table is keyed by its groups: named for what it is)
    } else if !src.key.is_empty() {
        Some(Unfollow::Keyed)
    } else if src.history.is_some() {
        Some(Unfollow::History)
    } else {
        None
    }
}

impl Unfollow {
    /// What the table is, as the refusal names it; the view's own words follow it.
    pub fn said(self, source: &str) -> String {
        match self {
            Unfollow::Keyed => format!("{source} is keyed: a new row replaces its key's row, which says nothing of the group that row was in"),
            Unfollow::Partial => format!("{source} is a GROUP BY view, whose table keeps partial rows"),
            Unfollow::History => format!("{source} is a history view, whose __start_at and __end_at are worked out as it is read"),
        }
    }
}

/// A view's columns as its plan runs them, and the SQL types its query keeps (`query_types`). None of the columns may be a
/// system column's name: the view's own table has those.
pub(crate) async fn table_columns(lake: &Lake, sql: &str, ctx: &datafusion::prelude::SessionContext, plan: &LogicalPlan) -> Result<(Vec<(String, String)>, std::collections::BTreeMap<String, String>)> {
    let columns: Vec<(String, String)> = crate::query::run_schema(ctx, plan)?.fields().iter().map(|f| (f.name().clone(), crate::answer::type_name(f.data_type()))).collect();
    if let Some((c, _)) = columns.iter().find(|(c, _)| crate::text::NAMES.contains(&c.as_str())) {
        bail!("{c} is a system column of the view's own table: name it something else ({c} AS source{c})");
    }
    let names: Vec<String> = columns.iter().map(|(c, _)| c.clone()).collect();
    let sql_types = crate::write::query_types(lake, sql, &names).await;
    Ok((columns, sql_types))
}

/// Make view `name` run again by key or whole (leader only): its table gets the query's columns,
/// and its first run works every group out (`ddl::settle` waits for it). `why`: None when asked.
#[allow(clippy::too_many_arguments)]
pub async fn create(lake: &Lake, name: &str, sql: &str, source: &str, src: &TableMeta, upstream: Option<&View>, plan: &LogicalPlan, how: How, why: Option<String>, lag_secs: Option<u64>) -> Result<()> {
    let asked = why.is_none();
    let (keys, joins, full, reason) = match how {
        How::ByKey => {
            let keys = keys(plan).map_err(|e| anyhow::anyhow!("{name} can't be kept by key: {e:#}. refresh = 'full' runs its query whole again"))?;
            if let Some(u) = unfollow(src) {
                return Err(coded("0A000", match u {
                    Unfollow::Keyed => format!("{}, so a view of it can't be kept by key. refresh = 'full' runs its query whole again", u.said(source)),
                    Unfollow::Partial => format!("{}: make {name} from {source}'s own source, or refresh = 'full'", u.said(source)),
                    Unfollow::History => format!("{}: make {name} with refresh = 'full'", u.said(source)),
                }));
            }
            if upstream.is_some_and(|u| u.sessions.is_some() && u.rerun.is_none()) {
                return Err(coded("0A000", format!("{source} cuts sessions as they close: make {name} from {source}'s own source")));
            }
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
    let (columns, sql_types) = table_columns(lake, sql, &ctx, plan).await?;
    let meta = TableMeta { columns, publish: default_publish(), ids: true, tiered: lake.visible(), sql_types, ..Default::default() };
    let r = Rerun { id: uuid::Uuid::new_v4().to_string(), keys, full, reason, asked, lag_secs, joins };
    let view = View { source: source.into(), sql: sql.into(), emit: None, sessions: None, ids: false, join: None, fill: None, expect: vec![], once: None, written: None, rerun: Some(r), matches: None };
    lake.cat.commit(vec![(table_key(name), json(&meta)), (crate::views::view_key(name), json(&view))], &[]).await
}

/// The tables of this lake a view kept by key joins under its windows (on their keys), its source
/// among them, each one whose changes it can follow by key; or why not. None when it joins none.
pub async fn joins(lake: &Lake, plan: &LogicalPlan, sql: &str, source: &str) -> Result<Vec<String>> {
    let Some(tables) = joined_windows(plan) else { return Ok(vec![]) };
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
    of: String, // (the run's view: its rerun's id, so a view made again starts afresh)
    running: bool,
    started: Option<Instant>,
    took: Duration,
    failed: crate::views::Failures,
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
        p.failed.note(&format!("materialized view {}", self.key.1), r.as_ref().err().map(|e| format!("{e:#}")));
        if r.is_ok() {
            p.upto = p.upto.max(self.upto.load(std::sync::atomic::Ordering::Relaxed));
        }
    }
}

/// `changed`: the newest commit that changed a table the view follows, and when it was made.
fn turn(lake: &Lake, view: &str, r: &Rerun, changed: Option<(u64, u64)>) -> Option<Turn> {
    let mut pace = PACE.lock().unwrap();
    let key = (lake.url.clone(), view.to_string());
    let p = pace.entry(key.clone()).or_default();
    if p.of != r.id {
        if p.running {
            return None; // (the old view's run is still going: the new one waits for it to end)
        }
        *p = Pace { of: r.id.clone(), ..Default::default() };
    }
    let least = Duration::from_secs(r.lag_secs.unwrap_or(0)).max(p.failed.backoff());
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
/// instead of waiting for a first run that won't come. `of`: the view's `identity`.
pub fn failing(lake: &Lake, view: &str, of: &str) -> Option<String> {
    let pace = PACE.lock().unwrap();
    pace.get(&(lake.url.clone(), view.to_string())).filter(|p| p.of == of).and_then(|p| p.failed.failing())
}

/// Leader: start a run of every view that runs its query again whose turn it is, each in a task of
/// its own on the queries' runtime (a run that reads many rows never holds up the others, the
/// leader's other work, or appends and commits). A run is no loop: one that fails or panics is tried
/// again at a later turn.
pub async fn run_all(lake: &Arc<Lake>, seq: &Arc<crate::log::Sequencer>, lock: &Arc<tokio::sync::Mutex<()>>) -> Result<()> {
    let views: Vec<(String, View)> = lake.cat.scan::<View>("v/", "v0").await?.into_iter().filter(|(_, v)| v.rerun.is_some()).map(|(k, v)| (k[2..].to_string(), v)).collect();
    {
        // (a dropped view's pace goes too, before the early return below)
        let names: std::collections::HashSet<&str> = views.iter().map(|(name, _)| name.as_str()).collect();
        PACE.lock().unwrap_or_else(|e| e.into_inner()).retain(|(url, view), _| url != &lake.url || names.contains(view.as_str()));
    }
    if views.is_empty() {
        CHANGED.lock().unwrap().remove(&lake.url);
        return Ok(());
    }
    look(lake).await?;
    for (name, v) in views {
        let Some(turn) = turn(lake, &name, v.rerun.as_ref().expect("filtered"), last_change(lake, &follows(&v))) else { continue };
        let (lake, seq, lock) = (lake.clone(), seq.clone(), lock.clone());
        crate::panics::aside(async move {
            let _slot = waiting(&turn.waited, crate::views::slot()).await; // (waiting for a slot is not the run's time)
            let r = crate::views::bounded(run(&lake, &seq, &lock, &name, &v, &turn)).await;
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
        let meta = lake.cat.meta(&table_key(view)).await?.context("view without table")?;
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
        let (mut pending, _) = crate::change::appends(lake, seq, view, &meta, vec![old], vec![new], &producer, false, false).await?; // (a view's rows: upserts)
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
        let plan = crate::engine::sql(&ctx, &crate::sys::planned(&v.sql)?).await?.into_unoptimized_plan();
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
    let plan = crate::engine::sql(&ctx, &crate::sys::planned(&v.sql)?).await?.into_unoptimized_plan();
    // (the lowest key node's keys over the changed rows, through the nodes under it alone: a filter there,
    // the view's WHERE under a GROUP BY, cuts the keys as it cuts the view's rows)
    let k = keyed(&plan).context("a view kept by key is a GROUP BY or windows within keys")?;
    let rows = changed(lake, &v.source, done, now).await?.into_unoptimized_plan();
    let low = cut_scan(&k.low, &|scan: &LogicalPlan| in_place_of(scan, &rows))?;
    let keys: Vec<Expr> = k.low_by.iter().enumerate().map(|(i, g)| g.clone().alias(format!("__k{i}"))).collect();
    let p = LogicalPlanBuilder::from(low).project(keys)?.distinct()?.build()?;
    Ok(ctx.execute_logical_plan(p).await?.collect().await?)
}

/// `rows` in the place of the table's `scan`: the scan's columns by name and type (NULL where `rows` has
/// none, as a system column), under the scan's name, so the nodes over it read them as they read the table.
/// (A view registered under the table's name would be planned inline, as a union under the stack.)
fn in_place_of(scan: &LogicalPlan, rows: &LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::TableScan(t) = scan else { bail!("the view's table is read another way") };
    let columns = scan.schema().fields().iter().map(|f| Ok(match rows.schema().has_column_with_unqualified_name(f.name()) {
        true => cast(ident(f.name()), f.data_type().clone()).alias(f.name()),
        false => lit(ScalarValue::try_from(f.data_type())?).alias(f.name()),
    }));
    Ok(LogicalPlanBuilder::from(rows.clone()).project(columns.collect::<Result<Vec<_>>>()?)?.alias(t.table_name.clone())?.build()?)
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
        let t = crate::query::table_view_once(lake, &ctx, &gone, Arc::new(crate::sys::with_sys(&dmeta)), Some(upto)).await?;
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
    let plan = crate::engine::sql(&ctx, &crate::sys::planned(&v.sql)?).await?.into_unoptimized_plan();
    let plan = match touched {
        Some(t) => {
            kept_as_written(&ctx);
            restrict(&plan, t)?
        }
        None => plan,
    };
    Ok(ctx.execute_logical_plan(plan).await?.collect().await?)
}

/// `plan` with its table's scan read at the touched keys' rows only: a semi join on the keys
/// (`IS NOT DISTINCT FROM`: a NULL key is a group too), directly over the scan under every node. Each
/// node then sees a touched key's rows whole, so each comes out as it would over every row.
fn restrict(plan: &LogicalPlan, touched: &Touched) -> Result<LogicalPlan> {
    let plan = peeled(plan); // (rows are a set: the order is left out)
    if joined_windows(plan).is_some() {
        return restrict_joined(plan, touched);
    }
    let k = keyed(plan).context("a view kept by key is a GROUP BY or windows within keys")?;
    cut_scan(plan, &|scan: &LogicalPlan| semi(scan, &k.by, touched))
}

/// `restrict` for windows over tables joined on their keys: each table under the join cut to the
/// touched keys (`within`).
fn restrict_joined(plan: &LogicalPlan, touched: &Touched) -> Result<LogicalPlan> {
    Ok(match plan {
        LogicalPlan::Window(_) => {
            let (by, _) = partitioned(plan).context("windows kept by key share a PARTITION BY column")?;
            within(plan, &by.into_iter().map(Expr::Column).collect::<Vec<_>>(), touched)?
        }
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => plan.with_new_exprs(plan.expressions(), vec![restrict_joined(plan.inputs()[0], touched)?])?,
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
    use datafusion::arrow::datatypes::{Int64Type, TimeUnit, TimestampMicrosecondType};

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

    /// `sql`'s plan as a view's query is planned (a MATCH_RECOGNIZE rewritten first), over `ticker` (symbol,
    /// day, price), `other` (symbol) and `events` (ts), all empty, with the pattern functions registered.
    async fn planned(sql: &str) -> Result<LogicalPlan> {
        let ctx = SessionContext::new();
        crate::matcher::register(&ctx);
        let ticker = Arc::new(Schema::new(vec![Field::new("symbol", DataType::Utf8, true), Field::new("day", DataType::Int64, true), Field::new("price", DataType::Int64, true)]));
        let other = Arc::new(Schema::new(vec![Field::new("symbol", DataType::Utf8, true)]));
        let events = Arc::new(Schema::new(vec![Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true)]));
        for (name, schema) in [("ticker", ticker), ("other", other), ("events", events)] {
            let table = datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![RecordBatch::new_empty(schema)]])?;
            ctx.register_table(name, Arc::new(table))?;
        }
        Ok(ctx.sql(&crate::pattern::rewritten(sql)?).await?.into_unoptimized_plan())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_view_made_again_under_its_name_starts_afresh() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-rerun-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        let made = |id: &str| Rerun { id: id.into(), keys: vec![], full: None, reason: String::new(), asked: false, lag_secs: None, joins: vec![] };
        let (a, b) = (made("a"), made("b"));
        let key = (lake.url.clone(), "v".to_string());
        let t = turn(&lake, "v", &a, None).expect("a's first turn");
        assert!(turn(&lake, "v", &b, None).is_none(), "while a's run is going, b's waits for it to end");
        t.ended(&Err(anyhow::anyhow!("boom")));
        drop(t);
        PACE.lock().unwrap().get_mut(&key).expect("a's pace").started = None; // (past a's pace)
        let t = turn(&lake, "v", &a, None).expect("a's next turn");
        t.ended(&Err(anyhow::anyhow!("boom")));
        drop(t);
        assert_eq!(failing(&lake, "v", "a").as_deref(), Some("boom"));
        let t = turn(&lake, "v", &b, None).expect("b's first turn, with none of a's backoff");
        assert_eq!(failing(&lake, "v", "a"), None, "a is gone: its failures went with it");
        drop(t);
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn keys_of_a_match_recognize_are_its_partition() {
        for rows in ["ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW", "ONE ROW PER MATCH AFTER MATCH SKIP TO NEXT ROW", "ALL ROWS PER MATCH AFTER MATCH SKIP PAST LAST ROW", "ALL ROWS PER MATCH AFTER MATCH SKIP TO NEXT ROW"] {
            let sql = format!("SELECT * FROM ticker MATCH_RECOGNIZE (PARTITION BY symbol ORDER BY day MEASURES strt.day AS start_day {rows} PATTERN (strt down+ up+) DEFINE down AS down.price < PREV(down.price), up AS up.price > PREV(up.price)) AS m");
            assert_eq!(keys(&planned(&sql).await.unwrap()).unwrap(), ["symbol"], "{rows}");
        }
    }

    #[tokio::test]
    async fn keys_of_nested_windows_are_their_partition() {
        let plan = planned("SELECT symbol, day, rank() OVER (PARTITION BY symbol ORDER BY day) AS r FROM (SELECT *, sum(price) OVER (PARTITION BY symbol) AS total FROM ticker) AS s").await.unwrap();
        assert_eq!(keys(&plan).unwrap(), ["symbol"]);
    }

    #[tokio::test]
    async fn keys_of_windows_with_a_projection_between_are_their_partition() {
        let plan = planned("SELECT symbol, day, r, sum(price) OVER (PARTITION BY symbol) AS total FROM (SELECT symbol, day, price, rank() OVER (PARTITION BY symbol ORDER BY day) AS r FROM ticker) AS w").await.unwrap();
        assert_eq!(keys(&plan).unwrap(), ["symbol"]);
    }

    #[tokio::test]
    async fn keys_of_a_group_by_are_its_groups_as_before() {
        let plan = planned("SELECT symbol, count(*) AS n FROM ticker GROUP BY symbol").await.unwrap();
        assert_eq!(keys(&plan).unwrap(), ["symbol"]);
        let plan = planned("SELECT day % 2 AS parity, symbol, count(*) AS n FROM ticker GROUP BY symbol, day % 2").await.unwrap();
        assert_eq!(keys(&plan).unwrap(), ["symbol", "parity"]);
        let plan = planned("SELECT symbol, count(*) AS n FROM ticker GROUP BY symbol, day").await.unwrap();
        assert!(keys(&plan).unwrap_err().to_string().contains("isn't selected"));
    }

    #[tokio::test]
    async fn a_window_over_a_group_by_that_doesnt_hold_its_key_is_refused() {
        let plan = planned("SELECT symbol, n, rank() OVER (PARTITION BY day ORDER BY n) AS r FROM (SELECT symbol, max(day) AS day, count(*) AS n FROM ticker GROUP BY symbol) AS g").await.unwrap();
        let err = keys(&plan).expect_err("a refusal");
        assert!(err.to_string().contains("share no column of the table"), "{err}");
    }

    #[tokio::test]
    async fn a_subquery_in_a_filter_is_refused() {
        for sql in [
            "SELECT symbol, count(*) AS n FROM ticker WHERE symbol IN (SELECT symbol FROM other) GROUP BY symbol",
            "SELECT symbol, day, rank() OVER (PARTITION BY symbol ORDER BY day) AS r FROM ticker WHERE symbol IN (SELECT symbol FROM other)",
        ] {
            let err = keys(&planned(sql).await.unwrap()).expect_err(sql);
            assert!(err.to_string().contains("subquery"), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn a_limit_is_refused() {
        for sql in [
            "SELECT symbol, count(*) AS n FROM ticker GROUP BY symbol LIMIT 5",
            "SELECT symbol, day, rank() OVER (PARTITION BY symbol ORDER BY day) AS r FROM (SELECT * FROM ticker LIMIT 10) AS l",
        ] {
            let err = keys(&planned(sql).await.unwrap()).expect_err(sql);
            assert!(err.to_string().contains("a LIMIT looks across every key"), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn a_top_order_by_is_not_a_limit() {
        let plan = planned("SELECT symbol, count(*) AS n FROM ticker GROUP BY symbol ORDER BY symbol").await.unwrap();
        assert_eq!(keys(&plan).unwrap(), ["symbol"]);
    }

    /// A GROUP BY under a WHERE: the lowest key node's input holds the filter, and the keys touched are read through it.
    #[tokio::test]
    async fn touched_keys_are_read_under_the_lowest_key_node() {
        let plan = planned("SELECT symbol, count(*) AS n FROM ticker WHERE price > 5 GROUP BY symbol").await.unwrap();
        let k = keyed(&plan).unwrap();
        assert!(matches!(k.low, LogicalPlan::Filter(_)), "{}", k.low);
        assert_eq!(k.low_by, [Expr::Column(Column::new(Some("ticker"), "symbol"))]);
        assert_eq!(k.low_by, k.by);
    }

    /// A GROUP BY expression is a key as the node writes it, and as the table holds it: `by` and `low_by` agree.
    #[tokio::test]
    async fn a_group_by_expression_is_a_key_in_both_halves() {
        let plan = planned("SELECT date_trunc('day', ts) AS d, count(*) AS n FROM events GROUP BY 1").await.unwrap();
        let k = keyed(&plan).unwrap();
        assert_eq!(k.by.len(), 1);
        assert!(matches!(k.by[0], Expr::ScalarFunction(_)), "{}", k.by[0]);
        assert_eq!(k.low_by, k.by);
        assert_eq!(keys(&plan).unwrap(), ["d"]);
    }
}
