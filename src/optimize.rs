//! Planning rules Pondra adds to DataFusion's, and the engine settings it starts from.
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, Result};
use datafusion::logical_expr::utils::{conjunction, disjunction, split_binary, split_conjunction};
use datafusion::logical_expr::{Aggregate, Expr, Filter, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Projection, SubqueryAlias};
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
    let at = rules.iter().position(|r| r.name() == "push_down_filter").map_or(rules.len(), |i| i + 1);
    rules.insert(at, Arc::new(SemiJoinDown));
    rules.insert(at, Arc::new(GroupOnlyJoined));
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
