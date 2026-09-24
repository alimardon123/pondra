//! Planning rules Pondra adds to DataFusion's, and the engine settings it starts from.
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, Result};
use datafusion::logical_expr::utils::{can_hash, conjunction, disjunction, find_valid_equijoin_key_pair, split_binary, split_conjunction};
use datafusion::logical_expr::{build_join_schema, Aggregate, Expr, Filter, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Projection, SubqueryAlias};
use datafusion::optimizer::{optimizer::ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule};
use datafusion::common::config::ConfigOptions;
use datafusion::physical_optimizer::{optimizer::PhysicalOptimizer, PhysicalOptimizerRule};
use datafusion::physical_plan::{aggregates::AggregateExec, filter::FilterExec, joins::HashJoinExec, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use std::collections::HashSet;
use std::sync::Arc;

/// Engine settings, before the user's own (`PONDRA_SQL_OPTIONS`, DataFusion's names):
/// - `0.06 + 0.01` is the exact decimal 0.07, as in the SQL standard (and DuckDB, Postgres), not a
///   float a hair below it;
/// - a join whose smaller side is under 32 MB builds one hash table that every thread probes,
///   instead of shuffling both sides by key.
pub fn config(mut config: SessionConfig) -> SessionConfig {
    let user = std::env::var("PONDRA_SQL_OPTIONS").unwrap_or_default();
    let defaults = "datafusion.sql_parser.parse_float_as_decimal=true,\
        datafusion.optimizer.hash_join_single_partition_threshold=33554432,\
        datafusion.optimizer.hash_join_single_partition_threshold_rows=1048576";
    for (k, v) in defaults.split(',').chain(user.split(',')).filter_map(|kv| kv.trim().split_once('=')) {
        config = config.set_str(k, v);
    }
    config
}

/// DataFusion's rules, with Pondra's placed where they work best.
pub fn rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = Optimizer::new().rules;
    for r in rules.iter_mut().filter(|r| r.name() == "eliminate_outer_join") {
        *r = Arc::new(crate::asof::KeepOuter(r.clone()));
    }
    let at = rules.iter().position(|r| r.name() == "push_down_filter").map_or(rules.len(), |i| i + 1);
    rules.insert(at, Arc::new(SemiJoinDown));
    rules.insert(at, Arc::new(GroupOnlyJoined));
    if std::env::var("PONDRA_JOIN_ORDER").as_deref() != Ok("0") {
        rules.insert(at, Arc::new(JoinOrder)); // (after the filters are down: they say how big each input is)
    }
    rules.push(Arc::new(CheapFirst));
    rules
}

/// A join with a grouped subquery (`l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE
/// l_partkey = p_partkey)`) keeps only the groups whose key the other side has. When that key comes
/// from a filtered table (the 200 parts of one brand and container), the subquery groups only
/// those keys' rows instead of all of them (TPC-H q17: 6 thousand lineitems, not 6 million).
#[derive(Debug)]
struct GroupOnlyJoined;

const KEYS: &str = "__pondra_keys";

impl OptimizerRule for GroupOnlyJoined {
    fn name(&self) -> &str {
        "group_only_joined"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(join) = &plan else { return Ok(Transformed::no(plan)) };
        if join.join_type != JoinType::Inner {
            return Ok(Transformed::no(plan));
        }
        for (l, r) in &join.on {
            let (Expr::Column(l), Expr::Column(r)) = (l, r) else { continue };
            for (grouped, other, gk, ok) in [(&join.right, &join.left, r, l), (&join.left, &join.right, l, r)] {
                let Some(leaf) = filtered_leaf(other, ok) else { continue };
                let keys = LogicalPlanBuilder::from(leaf).project([Expr::Column(ok.clone())])?.alias(KEYS)?.build()?;
                let key = Column::new(Some(KEYS), &ok.name);
                if let Some(side) = only_keys(grouped, gk, &keys, &key)? {
                    let (left, right) = if Arc::ptr_eq(grouped, &join.right) { (join.left.clone(), side) } else { (side, join.right.clone()) };
                    let j = Join::try_new(left, right, join.on.clone(), join.filter.clone(), join.join_type, join.join_constraint, join.null_equality, join.null_aware)?;
                    return Ok(Transformed::yes(LogicalPlan::Join(j)));
                }
            }
        }
        Ok(Transformed::no(plan))
    }
}

/// The filtered table (a Filter on a scan) that `col` comes from, through inner joins.
fn filtered_leaf(p: &Arc<LogicalPlan>, col: &Column) -> Option<LogicalPlan> {
    match p.as_ref() {
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => [&j.left, &j.right].into_iter().find(|s| s.schema().has_column(col)).and_then(|s| filtered_leaf(s, col)),
        LogicalPlan::Filter(f) if matches!(f.input.as_ref(), LogicalPlan::TableScan(_)) => Some(p.as_ref().clone()),
        _ => None,
    }
}

/// `p` (through aliases, projections and HAVING filters, down to a grouping by `col`) with the
/// grouping's input cut to the rows whose `col` is in `keys`.
fn only_keys(p: &Arc<LogicalPlan>, col: &Column, keys: &LogicalPlan, key: &Column) -> Result<Option<Arc<LogicalPlan>>> {
    let at = |schema: &DFSchema| schema.index_of_column(col).ok();
    let new = match p.as_ref() {
        LogicalPlan::SubqueryAlias(a) => {
            let Some(i) = at(&a.schema) else { return Ok(None) };
            let inner = Column::from(a.input.schema().qualified_field(i));
            let Some(input) = only_keys(&a.input, &inner, keys, key)? else { return Ok(None) };
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(input, a.alias.clone())?)
        }
        LogicalPlan::Projection(pr) => {
            let Some(Expr::Column(inner)) = at(&pr.schema).map(|i| pr.expr[i].clone().unalias()) else { return Ok(None) };
            let Some(input) = only_keys(&pr.input, &inner, keys, key)? else { return Ok(None) };
            LogicalPlan::Projection(Projection::try_new(pr.expr.clone(), input)?)
        }
        LogicalPlan::Filter(f) => {
            let Some(input) = only_keys(&f.input, col, keys, key)? else { return Ok(None) };
            LogicalPlan::Filter(Filter::try_new(f.predicate.clone(), input)?)
        }
        LogicalPlan::Aggregate(a) => {
            let Some(Expr::Column(inner)) = at(&a.schema).and_then(|i| a.group_expr.get(i)).map(|g| g.clone().unalias()) else { return Ok(None) };
            if a.input.exists(|n| Ok(matches!(n, LogicalPlan::SubqueryAlias(s) if s.alias.table() == KEYS)))? {
                return Ok(None); // done already
            }
            let on = vec![(Expr::Column(key.clone()), Expr::Column(inner))];
            let input = Join::try_new(Arc::new(keys.clone()), a.input.clone(), on, None, JoinType::RightSemi, JoinConstraint::On, NullEquality::NullEqualsNothing, false)?;
            LogicalPlan::Aggregate(Aggregate::try_new(Arc::new(LogicalPlan::Join(input)), a.group_expr.clone(), a.aggr_expr.clone())?)
        }
        _ => return Ok(None),
    };
    Ok(Some(Arc::new(new)))
}

/// A filter's conditions run cheapest first: `AND` looks at its right side only for the rows its
/// left side kept, so the date and number comparisons go before string matching and functions (as
/// DuckDB orders them). Otherwise, the order written.
#[derive(Debug)]
struct CheapFirst;

impl OptimizerRule for CheapFirst {
    fn name(&self) -> &str {
        "cheap_first"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(f) = &plan else { return Ok(Transformed::no(plan)) };
        let predicate = cheap_first(&f.predicate, f.input.schema());
        if predicate == f.predicate {
            return Ok(Transformed::no(plan));
        }
        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(predicate, f.input.clone())?)))
    }
}

fn cheap_first(e: &Expr, schema: &DFSchema) -> Expr {
    match e {
        Expr::BinaryExpr(b) if b.op == Operator::And => {
            let mut parts: Vec<_> = split_conjunction(e).into_iter().map(|p| cheap_first(p, schema)).collect();
            parts.sort_by_key(|p| cost(p, schema)); // stable: equal costs keep their order
            conjunction(parts).expect("at least two conditions")
        }
        Expr::BinaryExpr(b) if b.op == Operator::Or => disjunction(split_binary(e, Operator::Or).into_iter().map(|p| cheap_first(p, schema))).expect("at least two"),
        _ => e.clone(),
    }
}

/// Roughly what evaluating `e` costs per row: strings and functions cost more than numbers.
fn cost(e: &Expr, schema: &DFSchema) -> usize {
    let mut total = 0;
    let _ = e.apply(|n| {
        total += match n {
            Expr::Column(c) => match schema.qualified_field_from_column(c).map(|(_, f)| f.data_type().clone()) {
                Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Binary | DataType::LargeBinary | DataType::BinaryView) => 8,
                Ok(t) if t.is_nested() => 8,
                _ => 1,
            },
            Expr::Literal(..) | Expr::Alias(_) => 0,
            Expr::InList(l) => l.list.len(),
            Expr::Like(_) | Expr::SimilarTo(_) | Expr::ScalarFunction(_) => 20,
            Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) => 1000,
            _ => 1,
        };
        Ok(TreeNodeRecursion::Continue)
    });
    total
}

/// DataFusion's physical rules, with Pondra's placed where they work best.
pub fn physical_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = PhysicalOptimizer::new().rules;
    let at = rules.iter().position(|r| r.name() == "join_selection").map_or(0, |i| i + 1);
    rules.insert(at, Arc::new(HavingBuilds));
    rules.insert(at + 1, Arc::new(crate::asof::Rule)); // (before the rules that add exchanges: it asks for its own)
    rules
}

/// `x IN (SELECT k … GROUP BY k HAVING …)`: the few groups a HAVING keeps are the hash table, the
/// table probes it. (Statistics can't tell how many groups a HAVING keeps, and DataFusion's guess,
/// a fifth of the input, has it build on the table instead: 1.5 million orders, for 57 keys.)
#[derive(Debug)]
struct HavingBuilds;

impl PhysicalOptimizerRule for HavingBuilds {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            if let Some(j) = p.downcast_ref::<HashJoinExec>() {
                if matches!(j.join_type(), JoinType::LeftSemi | JoinType::LeftAnti) && !j.null_aware && having(j.right()) && !having(j.left()) {
                    return Ok(Transformed::yes(j.swap_inputs(*j.partition_mode())?));
                }
            }
            Ok(Transformed::no(p))
        })
        .data()
    }

    fn name(&self) -> &str {
        "having_builds"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Is `p` the groups a HAVING filter kept (under projections and repartitioning)?
fn having(p: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(f) = p.downcast_ref::<FilterExec>() {
        if f.input().downcast_ref::<AggregateExec>().is_some() {
            return true;
        }
    }
    match p.children()[..] {
        [c] if p.downcast_ref::<AggregateExec>().is_none() => having(c),
        _ => false,
    }
}

/// `x IN (SELECT k … GROUP BY k HAVING …)` filters the one table `x` comes from, so it runs on
/// that table, before the joins: like a WHERE filter would, rather than after the joins have
/// multiplied its rows (TPC-H q18 joins 57 orders instead of 6 million lineitems). Only for
/// subqueries that reduce their input (an aggregate or a limit): a semi join against a big table
/// is better left after the joins that shrink its other side.
#[derive(Debug)]
struct SemiJoinDown;

impl OptimizerRule for SemiJoinDown {
    fn name(&self) -> &str {
        "semi_join_down"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(semi) = &plan else { return Ok(Transformed::no(plan)) };
        // Written either way round: the subquery's rows (`set`) and the table they filter (`outer`).
        let (set, outer, on, anti) = match semi.join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => (&semi.right, &semi.left, semi.on.iter().map(|(l, r)| (r.clone(), l.clone())).collect::<Vec<_>>(), semi.join_type == JoinType::LeftAnti),
            JoinType::RightSemi | JoinType::RightAnti => (&semi.left, &semi.right, semi.on.clone(), semi.join_type == JoinType::RightAnti),
            _ => return Ok(Transformed::no(plan)),
        };
        let LogicalPlan::Join(inner) = outer.as_ref() else { return Ok(Transformed::no(plan)) };
        let reduced = set.exists(|p| Ok(matches!(p, LogicalPlan::Aggregate(_) | LogicalPlan::Limit(_))))?;
        if semi.null_aware || on.is_empty() || inner.join_type != JoinType::Inner || !reduced {
            return Ok(Transformed::no(plan));
        }
        // The outer columns it reads, and the side of the inner join that has them all.
        let mut used = HashSet::new();
        on.iter().for_each(|(_, o)| used.extend(o.column_refs()));
        if let Some(f) = &semi.filter {
            used.extend(f.column_refs().into_iter().filter(|c| !set.schema().has_column(c)));
        }
        let owns = |p: &LogicalPlan| used.iter().all(|c| p.schema().has_column(c));
        // Below it, the subquery's rows are the hash table (the left input, built first) that the table probes.
        let below = |side: &Arc<LogicalPlan>| -> Result<Arc<LogicalPlan>> {
            let t = if anti { JoinType::RightAnti } else { JoinType::RightSemi };
            Ok(Arc::new(LogicalPlan::Join(Join::try_new(set.clone(), side.clone(), on.clone(), semi.filter.clone(), t, semi.join_constraint, semi.null_equality, false)?)))
        };
        let (left, right) = match (owns(&inner.left), owns(&inner.right)) {
            (true, _) => (below(&inner.left)?, inner.right.clone()),
            (_, true) => (inner.left.clone(), below(&inner.right)?),
            _ => return Ok(Transformed::no(plan)),
        };
        let j = Join::try_new(left, right, inner.on.clone(), inner.filter.clone(), inner.join_type, inner.join_constraint, inner.null_equality, inner.null_aware)?;
        Ok(Transformed::yes(LogicalPlan::Join(j)))
    }
}

/// Inner joins run in the order the query names them, so a query that starts from its biggest
/// table carries those rows through every join after it. This picks the order by what the catalog
/// already knows — each table's row count and each column's range (`query::Pruned::statistics`,
/// from the file and manifest entries, which cost nothing to read) — building the tree one input
/// at a time, each time the one that leaves the fewest rows in flight.
///
/// A join's rows are estimated the textbook way — `rows(a) × rows(b) / distinct(key)`, a side
/// whose distinct count nothing knows counting as one row per value, which is what a key usually
/// is. That is what catches the joins that *expand*: TPC-H q5 relates customers to suppliers by
/// nation, 25 values, so every customer meets four hundred suppliers.
///
/// Two things keep it honest, and they matter more than the search. The order the query wrote is
/// costed the same way, as the tree it is, and kept unless the new one is cheaper — a query that
/// already says it well is left alone. And nothing is reordered unless every input's size is
/// known and every step joins on a key: a tree with a cross join in it is one these estimates say
/// nothing useful about. And because the estimates are bounds rather than counts, the new order
/// has to look a good deal cheaper, not a little (`PONDRA_JOIN_ORDER`: the margin, 2 by default;
/// `0` turns the rule off).
#[derive(Debug)]
struct JoinOrder;

/// How much cheaper the new order has to look before it is taken (`PONDRA_JOIN_ORDER`, 2 by
/// default). The estimates are bounds, not counts, so a small difference between two orders is
/// not a reason to overrule the one the query asked for.
fn margin() -> u64 {
    std::env::var("PONDRA_JOIN_ORDER").ok().and_then(|m| m.parse().ok()).unwrap_or(2).max(1)
}

impl OptimizerRule for JoinOrder {
    fn name(&self) -> &str {
        "join_order"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(top) = &plan else { return Ok(Transformed::no(plan)) };
        let (equality, constraint) = (top.null_equality, top.join_constraint);
        if !flat(top, equality) {
            return Ok(Transformed::no(plan));
        }
        let (mut leaves, mut keys, mut filters) = (vec![], vec![], vec![]);
        flatten(&plan, equality, &mut leaves, &mut keys, &mut filters);
        if leaves.len() < 3 {
            return Ok(Transformed::no(plan)); // (two inputs: the build side is chosen by size when it runs)
        }
        let Some(sizes) = leaves.iter().map(size).collect::<Option<Vec<Size>>>() else { return Ok(Transformed::no(plan)) };
        let asked: Vec<usize> = (0..leaves.len()).collect();
        let order = cheapest(&leaves, &sizes, &keys)?;
        // Both costed the same way, and only when every step joins on a key: a tree with a cross
        // join in it is one these estimates can say nothing useful about.
        let (Some((_, was)), Some(now)) = (as_written(&plan, equality)?, rows_moved(&leaves, &sizes, &keys, &order)?) else { return Ok(Transformed::no(plan)) };
        if order == asked || now.saturating_mul(margin()) >= was {
            return Ok(Transformed::no(plan));
        }
        // Rebuilt left-deep in that order: each join takes the keys that connect its input to what
        // is built, and every condition that can be evaluated by then.
        let mut used = vec![false; keys.len()];
        let mut left = leaves[order[0]].clone();
        for &i in &order[1..] {
            let right = Arc::new(leaves[i].clone());
            let mut on = vec![];
            for (k, pair) in connect(&keys, left.schema(), right.schema())? {
                if !std::mem::replace(&mut used[k], true) {
                    on.push(pair);
                }
            }
            let schema = build_join_schema(left.schema(), right.schema(), &JoinType::Inner)?;
            let (mine, rest) = std::mem::take(&mut filters).into_iter().partition::<Vec<Expr>, _>(|f| f.column_refs().iter().all(|c| schema.has_column(c)));
            filters = rest;
            left = LogicalPlan::Join(Join::try_new(Arc::new(left), right, on, conjunction(mine), JoinType::Inner, constraint, equality, false)?);
        }
        // A key no join could take (one side spanning two inputs joined apart) stays a condition,
        // so nothing is ever dropped; the next pass pushes it back down.
        filters.extend(keys.iter().zip(&used).filter(|(_, &u)| !u).map(|((l, r), _)| l.clone().eq(r.clone())));
        let schema = Arc::clone(plan.schema());
        if left.schema() != &schema {
            left = LogicalPlan::Projection(Projection::new_from_schema(Arc::new(left), schema)); // (the columns as the query had them)
        }
        if let Some(rest) = conjunction(filters) {
            left = LogicalPlan::Filter(Filter::try_new(rest, Arc::new(left))?);
        }
        Ok(Transformed::yes(left))
    }
}

/// A join tree this rule may take apart: inner joins on equalities, nothing null-aware, all
/// treating nulls alike.
fn flat(j: &Join, equality: NullEquality) -> bool {
    j.join_type == JoinType::Inner && j.join_constraint == JoinConstraint::On && !j.null_aware && j.null_equality == equality
}

/// The tree's inputs, in the order it joins them, with every equi-key and condition it holds.
fn flatten(plan: &LogicalPlan, equality: NullEquality, leaves: &mut Vec<LogicalPlan>, keys: &mut Vec<(Expr, Expr)>, filters: &mut Vec<Expr>) {
    match plan {
        LogicalPlan::Join(j) if flat(j, equality) => {
            keys.extend(j.on.iter().cloned());
            filters.extend(j.filter.iter().flat_map(split_conjunction).cloned());
            flatten(&j.left, equality, leaves, keys, filters);
            flatten(&j.right, equality, leaves, keys, filters);
        }
        _ => leaves.push(plan.clone()),
    }
}

/// How big a join input is: its rows, and an upper bound on each column's distinct values where
/// the catalog knows one (by column name: a name is unique within one table, and an input holding
/// more than one table is left without bounds).
#[derive(Clone, Default)]
struct Size {
    rows: u64,
    distinct: std::collections::HashMap<String, u64>,
}

impl Size {
    /// The same input cut to `rows` (a filter, or a join that kept some of them).
    fn cut(&self, rows: u64) -> Size {
        Size { rows, distinct: self.distinct.iter().map(|(c, &n)| (c.clone(), n.min(rows))).collect() }
    }

    fn of(&self, e: &Expr) -> Option<u64> {
        match e {
            Expr::Column(c) => self.distinct.get(&c.name).copied(),
            Expr::Alias(a) => self.of(&a.expr),
            Expr::Cast(c) => self.of(&c.expr),
            _ => None,
        }
    }
}

/// The two put together, as a join leaves them.
fn joined(a: &Size, b: &Size, rows: u64) -> Size {
    let mut distinct = a.cut(rows).distinct;
    for (c, n) in b.cut(rows).distinct {
        let n = distinct.get(&c).map_or(n, |had| n.min(*had)); // (the same name twice: take the smaller — the join looks no cheaper than it is)
        distinct.insert(c, n);
    }
    Size { rows, distinct }
}

/// How many rows a join of `a` and `b` on `on` leaves: every row of one side meets the rows of the
/// other that share its key, which is `rows(a) × rows(b) / distinct(key)` — the textbook estimate,
/// and the one that catches a join on a column with few values (TPC-H q5 joins customers to
/// suppliers by nation: 25 values, so every customer meets 400 suppliers). A side whose distinct
/// count nothing knows counts as one row per value, which is what a key usually is.
fn join_rows(a: &Size, b: &Size, on: &[(Expr, Expr)]) -> u64 {
    let (ra, rb) = (a.rows.max(1) as f64, b.rows.max(1) as f64);
    if on.is_empty() {
        return (ra * rb).min(u64::MAX as f64) as u64; // a cross join
    }
    let spread: f64 = on.iter().map(|(l, r)| {
        let d = |s: &Size, e: &Expr, rows: f64| s.of(e).map_or(rows, |n| n as f64);
        d(a, l, ra).max(d(b, r, rb)).max(1.0)
    }).product();
    (ra * rb / spread).max(1.0).min(u64::MAX as f64) as u64
}

/// The order to join the inputs in: the cheapest first pair, then each time the input that leaves
/// the fewest rows. Inputs that share no key with what is built go last — a cross join the query
/// already asked for, never one this makes up.
fn cheapest(leaves: &[LogicalPlan], sizes: &[Size], keys: &[(Expr, Expr)]) -> Result<Vec<usize>> {
    let mut todo: Vec<usize> = (0..leaves.len()).collect();
    todo.sort_by_key(|&i| (sizes[i].rows, i));
    let first = todo.remove(0); // (the smallest input: nothing else is joined yet, so nothing else can be costed)
    let (mut order, mut built, mut schema) = (vec![first], sizes[first].clone(), leaves[first].schema().as_ref().clone());
    while !todo.is_empty() {
        let mut best: Option<(u64, usize, usize)> = None; // (rows, whether it is joined at all, place in todo)
        for (at, &i) in todo.iter().enumerate() {
            let on: Vec<(Expr, Expr)> = connect(keys, &schema, leaves[i].schema())?.into_iter().map(|(_, p)| p).collect();
            let rank = (join_rows(&built, &sizes[i], &on), usize::from(on.is_empty()), at);
            if best.is_none_or(|b| (rank.1, rank.0) < (b.1, b.0)) {
                best = Some(rank);
            }
        }
        let at = best.expect("something is left to join").2;
        let i = todo.remove(at);
        let on: Vec<(Expr, Expr)> = connect(keys, &schema, leaves[i].schema())?.into_iter().map(|(_, p)| p).collect();
        (built, schema) = (joined(&built, &sizes[i], join_rows(&built, &sizes[i], &on)), build_join_schema(&schema, leaves[i].schema(), &JoinType::Inner)?);
        order.push(i);
    }
    Ok(order)
}

/// What the tree as the query wrote it costs, and how big its result is — the shape it has, which
/// may be deeper than left-deep. The order this rule picks has to beat this to be worth it.
fn as_written(plan: &LogicalPlan, equality: NullEquality) -> Result<Option<(Size, u64)>> {
    let LogicalPlan::Join(j) = plan else { return Ok(size(plan).map(|s| (s, 0))) };
    if !flat(j, equality) {
        return Ok(size(plan).map(|s| (s, 0)));
    }
    let (Some((l, cl)), Some((r, cr))) = (as_written(&j.left, equality)?, as_written(&j.right, equality)?) else { return Ok(None) };
    if j.on.is_empty() {
        return Ok(None);
    }
    let rows = join_rows(&l, &r, &j.on);
    Ok(Some((joined(&l, &r, rows), cl.saturating_add(cr).saturating_add(rows))))
}

/// What joining them left-deep in this order costs: the rows every step leaves, added up.
fn rows_moved(leaves: &[LogicalPlan], sizes: &[Size], keys: &[(Expr, Expr)], order: &[usize]) -> Result<Option<u64>> {
    let (mut built, mut schema, mut total) = (sizes[order[0]].clone(), leaves[order[0]].schema().as_ref().clone(), 0u64);
    for &i in &order[1..] {
        let on: Vec<(Expr, Expr)> = connect(keys, &schema, leaves[i].schema())?.into_iter().map(|(_, p)| p).collect();
        if on.is_empty() {
            return Ok(None); // a step with nothing to join on: not an order worth trusting
        }
        let rows = join_rows(&built, &sizes[i], &on);
        (built, schema, total) = (joined(&built, &sizes[i], rows), build_join_schema(&schema, leaves[i].schema(), &JoinType::Inner)?, total.saturating_add(rows));
    }
    Ok(Some(total))
}

/// The keys that join `right` to what is built, each written (built side, right side) and with
/// its place in `keys`, so the caller can take each one exactly once.
fn connect(keys: &[(Expr, Expr)], left: &DFSchema, right: &DFSchema) -> Result<Vec<(usize, (Expr, Expr))>> {
    use datafusion::logical_expr::ExprSchemable;
    let mut on: Vec<(usize, (Expr, Expr))> = vec![];
    for (k, (l, r)) in keys.iter().enumerate() {
        let Some(pair) = find_valid_equijoin_key_pair(l, r, left, right)? else { continue };
        if can_hash(&pair.0.get_type(left)?) && !on.iter().any(|(_, p)| *p == pair) {
            on.push((k, pair));
        }
    }
    Ok(on)
}

/// How big a join input is, as well as anything here can say: a table's own count from the
/// catalog, scaled by the filters above it (a condition keeps about a third of the rows, which is
/// what DataFusion assumes too), and the column bounds that came with it. `None` where nothing
/// knows — then the joins are left in the order the query wrote them.
fn size(plan: &LogicalPlan) -> Option<Size> {
    let kept = |s: &Size, conds: usize| s.cut((s.rows as f64 * 0.3f64.powi(conds as i32)).ceil() as u64);
    let groups = |s: &Size| s.cut((s.rows as f64).sqrt().ceil() as u64); // (a grouping's rows: unknowable, but far fewer)
    let rows = |n: u64| Size { rows: n, distinct: Default::default() };
    Some(match plan {
        LogicalPlan::TableScan(s) => {
            let stats = datafusion::datasource::source_as_provider(&s.source).ok()?.statistics()?;
            // `column_statistics` covers the table's own schema, not the columns this scan reads.
            let distinct = s.source.schema().fields().iter().zip(&stats.column_statistics)
                .filter_map(|(f, c)| Some((f.name().clone(), *c.distinct_count.get_value()? as u64)))
                .collect();
            let whole = Size { rows: *stats.num_rows.get_value()? as u64, distinct };
            kept(&whole, s.filters.len()).cut(whole.rows.min(s.fetch.unwrap_or(usize::MAX) as u64))
        }
        LogicalPlan::Filter(f) => kept(&size(&f.input)?, split_conjunction(&f.predicate).len()),
        LogicalPlan::Projection(p) => size(&p.input)?,
        LogicalPlan::SubqueryAlias(a) => size(&a.input)?,
        LogicalPlan::Sort(s) => size(&s.input)?,
        LogicalPlan::Limit(l) => size(&l.input)?,
        LogicalPlan::Aggregate(a) if a.group_expr.is_empty() => rows(1),
        LogicalPlan::Aggregate(a) => groups(&size(&a.input)?),
        LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) => groups(&size(input)?),
        LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(d)) => groups(&size(&d.input)?),
        LogicalPlan::Union(u) => rows(u.inputs.iter().map(|i| Some(size(i)?.rows)).sum::<Option<u64>>()?),
        LogicalPlan::Join(j) => rows(size(&j.left)?.rows.max(size(&j.right)?.rows)),
        LogicalPlan::Values(v) => rows(v.values.len() as u64),
        LogicalPlan::EmptyRelation(_) => rows(1),
        _ => return None,
    })
}
