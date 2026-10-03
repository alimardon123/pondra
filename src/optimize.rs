//! Planning rules Pondra adds to DataFusion's, and the engine settings it starts from.
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, Result};
use datafusion::logical_expr::utils::{can_hash, conjunction, disjunction, find_valid_equijoin_key_pair, split_binary, split_conjunction};
use datafusion::logical_expr::{build_join_schema, Aggregate, Expr, Filter, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Projection, SubqueryAlias};
use datafusion::optimizer::{optimizer::ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule};
use datafusion::common::config::ConfigOptions;
use datafusion::physical_optimizer::{optimizer::PhysicalOptimizer, PhysicalOptimizerRule};
use datafusion::physical_expr::expressions::{lit, Column as PhysicalColumn, DynamicFilterPhysicalExpr};
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::execution_plan::replace_children_if_necessary;
use datafusion::physical_plan::{aggregates::AggregateExec, filter::FilterExec, joins::HashJoinExec, ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionConfig;
use std::collections::HashSet;
use std::sync::Arc;

/// Engine settings, before the user's own (`PONDRA_SQL_OPTIONS`, DataFusion's names):
/// - `0.06 + 0.01` is the exact decimal 0.07, as in the SQL standard (and DuckDB, Postgres), not a
///   float a hair below it;
/// - `TIMESTAMPTZ` (`TIMESTAMP WITH TIME ZONE`) is an instant, kept and shown in UTC, as
///   Postgres keeps it; `TIMESTAMP` stays a wall-clock time without a zone;
/// - a join whose smaller side is under 32 MB builds one hash table that every thread probes,
///   instead of shuffling both sides by key.
pub fn config(mut config: SessionConfig) -> SessionConfig {
    let user = std::env::var("PONDRA_SQL_OPTIONS").unwrap_or_default();
    let defaults = "datafusion.sql_parser.parse_float_as_decimal=true,\
        datafusion.execution.time_zone=+00:00,\
        datafusion.optimizer.hash_join_single_partition_threshold=33554432,\
        datafusion.optimizer.hash_join_single_partition_threshold_rows=1048576,\
        datafusion.execution.skip_physical_aggregate_schema_check=true";
    // (the last: DataFusion 55 works out a CASE's nullability two ways, and the planner's one is
    // the more careful, so `SELECT DISTINCT CASE WHEN a < 1 AND b = 5 THEN b ELSE 5 END` failed its
    // check that they agree; the rows are the same either way)
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
    let ordered = std::env::var("PONDRA_JOIN_ORDER").as_deref() != Ok("0");
    rules.insert(at, Arc::new(GroupOnlyJoined));
    if ordered {
        rules.insert(at, Arc::new(JoinOrder)); // (after the filters are down: they say how big each input is)
    }
    rules.insert(at, Arc::new(SemiJoinDown)); // (before it: a table a subquery cuts is smaller)
    if ordered {
        rules.insert(at, Arc::new(OuterLast)); // (the inner joins it gathers are the order's to choose)
    }
    rules.insert(0, Arc::new(InListOfRows)); // (before DataFusion's simplifier meets two lists of one column)
    rules.insert(0, Arc::new(Seconds)); // (before a literal is folded into a timestamp)
    rules.push(Arc::new(CheapFirst));
    rules.push(Arc::new(AsyncBelow)); // (last: after COUNT(DISTINCT) became a GROUP BY)
    rules
}

/// `to_timestamp(…)` and its `_seconds`, `_millis`, `_micros` and `_nanos` answer a `TIMESTAMP`
/// without a zone (UTC's time of day for text that names a zone), as DataFusion, Spark and
/// DuckDB's `strptime` do (ADR-032), though the session's zone is UTC so that `TIMESTAMPTZ`
/// columns are UTC's (`config`). DataFusion reads that zone into these functions when they are
/// made; here they are made without it, every time the session's settings change. (With it, they
/// also answered a column of text without the zone their type said, and the batch was refused:
/// the answer is cast to the type they say.)
pub fn register_zoned(ctx: &datafusion::prelude::SessionContext) {
    use datafusion::execution::FunctionRegistry;
    let naive = naive(ctx.copied_config().options());
    for name in ["to_timestamp", "to_timestamp_seconds", "to_timestamp_millis", "to_timestamp_micros", "to_timestamp_nanos"] {
        if let Ok(inner) = ctx.udf(name) {
            let inner = inner.inner().with_updated_config(&naive).map(Arc::new).unwrap_or(inner);
            ctx.register_udf(datafusion::logical_expr::ScalarUDF::new_from_impl(Zoned(inner)));
        }
    }
}

/// The session's settings without its zone.
fn naive(config: &ConfigOptions) -> ConfigOptions {
    let mut c = config.clone();
    c.execution.time_zone = None;
    c
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Zoned(Arc<datafusion::logical_expr::ScalarUDF>);

impl datafusion::logical_expr::ScalarUDFImpl for Zoned {
    fn name(&self) -> &str { self.0.name() }
    fn aliases(&self) -> &[String] { self.0.aliases() }
    fn signature(&self) -> &datafusion::logical_expr::Signature { self.0.signature() }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> { self.0.coerce_types(types) }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> { self.0.return_type(types) }
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<datafusion::logical_expr::ScalarUDF> {
        let inner = self.0.inner().with_updated_config(&naive(config))?;
        Some(datafusion::logical_expr::ScalarUDF::new_from_impl(Zoned(Arc::new(inner))))
    }
    fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> Result<datafusion::logical_expr::ColumnarValue> {
        let want = args.return_field.data_type().clone();
        let out = self.0.invoke_with_args(args)?;
        if out.data_type() == want { Ok(out) } else { out.cast_to(&want, None) }
    }
}

/// `CAST('2024-05-01 10:30' AS TIMESTAMP)`, `TIMESTAMP '…'`, `ts > '2024-05-01 10:30'`: text
/// that DataFusion will parse as a timestamp, given its seconds if it has none (`query::seconds`),
/// before the literal is folded.
#[derive(Debug)]
struct Seconds;

impl OptimizerRule for Seconds {
    fn name(&self) -> &str { "pondra_seconds" }
    fn apply_order(&self) -> Option<ApplyOrder> { Some(ApplyOrder::TopDown) }
    fn supports_rewrite(&self) -> bool { true }
    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        use datafusion::common::ScalarValue;
        let fixed = |e: &Expr| match e {
            Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => crate::query::seconds(s).map(|s| Box::new(Expr::Literal(ScalarValue::Utf8(Some(s)), None))),
            _ => None,
        };
        plan.map_expressions(|e| e.transform_up(|e| Ok(match e {
            Expr::Cast(mut c) if matches!(c.field.data_type(), DataType::Timestamp(..)) => match fixed(&c.expr) {
                Some(l) => { c.expr = l; Transformed::yes(Expr::Cast(c)) }
                None => Transformed::no(Expr::Cast(c)),
            },
            Expr::TryCast(mut c) if matches!(c.field.data_type(), DataType::Timestamp(..)) => match fixed(&c.expr) {
                Some(l) => { c.expr = l; Transformed::yes(Expr::TryCast(c)) }
                None => Transformed::no(Expr::TryCast(c)),
            },
            e => Transformed::no(e),
        })))
    }
}

/// DataFusion runs async functions — Python functions (`pyfn.rs`), Flight ones (`udf.rs`) — in
/// projections, filters and aggregates' arguments. One in a GROUP BY, an ORDER BY or a window
/// function (or a COUNT(DISTINCT …), which becomes a GROUP BY) is computed here by a projection
/// below it, once a row, and the node above reads its column.
#[derive(Debug)]
struct AsyncBelow;

impl OptimizerRule for AsyncBelow {
    fn name(&self) -> &str {
        "async_below"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Aggregate(a) if a.group_expr.iter().any(is_async) && !a.group_expr.iter().any(|g| matches!(g, Expr::GroupingSet(_))) => {
                let names: Vec<String> = a.group_expr.iter().map(|e| e.schema_name().to_string()).collect();
                let (input, groups) = lift(a.input, a.group_expr)?;
                let groups = groups.into_iter().zip(names).map(|(g, n)| if g.schema_name().to_string() == n { g } else { g.alias(n) }).collect();
                Ok(Transformed::yes(LogicalPlan::Aggregate(Aggregate::try_new(Arc::new(input), groups, a.aggr_expr)?)))
            }
            LogicalPlan::Sort(s) if s.expr.iter().any(|e| is_async(&e.expr)) => {
                let columns: Vec<Expr> = s.input.schema().columns().into_iter().map(Expr::Column).collect();
                let (input, exprs) = lift(s.input, s.expr.iter().map(|e| e.expr.clone()).collect())?;
                let expr = s.expr.into_iter().zip(exprs).map(|(e, x)| e.with_expr(x)).collect();
                let sorted = LogicalPlan::Sort(datafusion::logical_expr::Sort { expr, input: Arc::new(input), fetch: s.fetch });
                Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(columns, Arc::new(sorted))?))) // (without the columns it sorted by)
            }
            LogicalPlan::Window(w) if w.window_expr.iter().any(is_async) => {
                let columns: Vec<Expr> = w.input.schema().columns().into_iter().map(Expr::Column).collect();
                let names: Vec<String> = w.window_expr.iter().map(|e| e.schema_name().to_string()).collect();
                let (input, exprs) = lift(w.input, w.window_expr)?;
                let exprs = exprs.into_iter().zip(&names).map(|(e, n)| if e.schema_name().to_string() == *n { e } else { e.alias(n) }).collect();
                let window = LogicalPlan::Window(datafusion::logical_expr::Window::try_new(exprs, Arc::new(input))?);
                let out = columns.into_iter().chain(names.iter().map(|n| Expr::Column(Column::from_name(n)))).collect::<Vec<_>>();
                Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(out, Arc::new(window))?)))
            }
            // An async call in another's arguments (`caption(file_read(path))`): the inner one first,
            // below, as a column (DataFusion computes an async call's arguments as they are).
            LogicalPlan::Projection(p) if p.expr.iter().any(nested) => {
                let names: Vec<String> = p.expr.iter().map(|e| e.schema_name().to_string()).collect();
                let (input, exprs) = unnest(p.input, p.expr)?;
                let exprs = exprs.into_iter().zip(names).map(|(e, n)| if e.schema_name().to_string() == n { e } else { e.alias(n) }).collect();
                Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(input))?)))
            }
            LogicalPlan::Filter(f) if nested(&f.predicate) => {
                let columns: Vec<Expr> = f.input.schema().columns().into_iter().map(Expr::Column).collect();
                let (input, mut predicate) = unnest(f.input, vec![f.predicate])?;
                let filtered = LogicalPlan::Filter(Filter::try_new(predicate.remove(0), Arc::new(input))?);
                Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(columns, Arc::new(filtered))?))) // (without the computed columns)
            }
            // `VALUES ('x', file_read('a.png'))` (an INSERT's): each row a projection of one row,
            // which runs async calls, and the rows put together.
            LogicalPlan::Values(v) if v.values.iter().flatten().any(is_async) => {
                let fields = v.schema.fields().clone();
                let mut rows = v.values.into_iter().map(|row| {
                    let exprs = row.into_iter().zip(fields.iter()).map(|(e, f)| datafusion::prelude::cast(e, f.data_type().clone()).alias(f.name()));
                    LogicalPlanBuilder::empty(true).project(exprs)?.build()
                });
                let mut all = LogicalPlanBuilder::from(rows.next().expect("a row")?);
                for r in rows {
                    all = all.union(r?)?;
                }
                Ok(Transformed::yes(all.build()?))
            }
            plan => Ok(Transformed::no(plan)),
        }
    }
}

/// Is there an async call with another async call in its arguments?
fn nested(e: &Expr) -> bool {
    e.exists(|e| Ok(matches!(e, Expr::ScalarFunction(f) if f.func.as_async().is_some() && f.args.iter().any(is_async)))).unwrap_or(false)
}

/// `input` with the async calls inside other async calls' arguments computed by a projection
/// over it (`__async_n{i}`, numbered across the plan), and `exprs` reading them instead; calls
/// nested deeper are unnested in that projection in turn.
fn unnest(input: Arc<LogicalPlan>, exprs: Vec<Expr>) -> Result<(LogicalPlan, Vec<Expr>)> {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let mut lifted: Vec<Expr> = vec![];
    let exprs = exprs.into_iter().map(|e| e.transform_down(|e| {
        let Expr::ScalarFunction(f) = &e else { return Ok(Transformed::no(e)) };
        if f.func.as_async().is_none() || !f.args.iter().any(is_async) {
            return Ok(Transformed::no(e));
        }
        let args = f.args.iter().cloned().map(|a| a.transform_down(|a| match &a {
            Expr::ScalarFunction(g) if g.func.as_async().is_some() => {
                let name = format!("__async_n{}", NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
                lifted.push(a.clone().alias(&name));
                Ok(Transformed::new(Expr::Column(Column::from_name(name)), true, TreeNodeRecursion::Jump))
            }
            _ => Ok(Transformed::no(a)),
        }).data()).collect::<Result<Vec<_>>>()?;
        let call = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(f.func.clone(), args));
        Ok(Transformed::new(call, true, TreeNodeRecursion::Jump))
    }).data()).collect::<Result<Vec<_>>>()?;
    let mut columns: Vec<Expr> = input.schema().columns().into_iter().map(Expr::Column).collect();
    columns.extend(lifted);
    let below = Projection::try_new(columns, input)?;
    let below = match below.expr.iter().any(nested) {
        true => {
            let (deeper, inner) = unnest(below.input, below.expr)?; // (the lifted calls keep their names: aliases)
            Projection::try_new(inner, Arc::new(deeper))?
        }
        false => below,
    };
    Ok((LogicalPlan::Projection(below), exprs))
}

fn is_async(e: &Expr) -> bool {
    e.exists(|e| Ok(matches!(e, Expr::ScalarFunction(f) if f.func.as_async().is_some()))).unwrap_or(false)
}

/// `input` with each async call in `exprs` as a column of a projection over it (`__async_{i}`),
/// and `exprs` reading those columns instead.
fn lift(input: Arc<LogicalPlan>, exprs: Vec<Expr>) -> Result<(LogicalPlan, Vec<Expr>)> {
    let mut lifted: Vec<Expr> = vec![];
    let exprs = exprs.into_iter().map(|e| e.transform_down(|e| {
        if !matches!(&e, Expr::ScalarFunction(f) if f.func.as_async().is_some()) {
            return Ok(Transformed::no(e));
        }
        let at = lifted.iter().position(|l| *l == e).unwrap_or_else(|| {
            lifted.push(e.clone());
            lifted.len() - 1
        });
        Ok(Transformed::new(Expr::Column(Column::from_name(format!("__async_{at}"))), true, TreeNodeRecursion::Jump))
    }).data()).collect::<Result<Vec<_>>>()?;
    let mut columns: Vec<Expr> = input.schema().columns().into_iter().map(Expr::Column).collect();
    columns.extend(lifted.into_iter().enumerate().map(|(i, e)| e.alias(format!("__async_{i}"))));
    Ok((LogicalPlan::Projection(Projection::try_new(columns, input)?), exprs))
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

/// `x IN (a, b)` whose list isn't all values (`c1`, `CASE WHEN c1 < 0 THEN … END`, `NULL`, `NULL +
/// 1`) as `x = a OR x = b` (`NOT IN`: `x <> a AND x <> b`), the same answer to NULLs too, before
/// DataFusion simplifies anything. DataFusion 55.1 tries such a list on an empty batch to see if
/// it is constant; a `CASE` passes, and every row is then compared with what it gave there. And
/// its simplifier meets `x IN (y, 'a') AND x IN ('a', 'b')` as sets of values: their intersection
/// was `x IN ()`, false, and `x IN (1, 2) AND x NOT IN (NULL)` was `x IN (1, 2)` (all three found
/// by `tools/random_sql.py`). Lists of values alone stay lists.
#[derive(Debug)]
struct InListOfRows;

impl OptimizerRule for InListOfRows {
    fn name(&self) -> &str {
        "in_list_of_rows"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let names = datafusion::logical_expr::expr_rewriter::NamePreserver::new(&plan); // (a column keeps the name its expression gave it)
        plan.map_expressions(|e| {
            let name = names.save(&e);
            e.transform_up(|e| match e {
                Expr::InList(i) if !i.list.is_empty() && !i.list.iter().all(value) => {
                    let (op, join) = if i.negated { (Operator::NotEq, Operator::And) } else { (Operator::Eq, Operator::Or) };
                    let each = i.list.into_iter().map(|x| datafusion::logical_expr::binary_expr((*i.expr).clone(), op, x));
                    Ok(Transformed::yes(each.reduce(|a, b| datafusion::logical_expr::binary_expr(a, join, b)).expect("a list")))
                }
                e => Ok(Transformed::no(e)),
            })
            .map(|t| t.update_data(|e| name.restore(e)))
        })
    }
}

/// A value that isn't NULL, perhaps cast.
fn value(e: &Expr) -> bool {
    match e {
        Expr::Literal(v, _) => !v.is_null(),
        Expr::Cast(c) => value(&c.expr),
        Expr::TryCast(c) => value(&c.expr),
        Expr::Negative(e) => value(e),
        _ => false,
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
    for r in rules.iter_mut().filter(|r| r.name() == "ProjectionPushdown") {
        *r = Arc::new(GuardedPushdown(r.clone()));
    }
    let at = rules.iter().position(|r| r.name() == "join_selection").map_or(0, |i| i + 1);
    rules.insert(at, Arc::new(HavingBuilds));
    rules.insert(at + 1, Arc::new(crate::asof::Rule)); // (before the rules that add exchanges: it asks for its own)
    let end = rules.iter().position(|r| r.name() == "SanityCheckPlan").unwrap_or(rules.len());
    rules.insert(end, Arc::new(crate::hot::TopFirst)); // (after DataFusion's own sort pushdown)
    rules.insert(end, Arc::new(MinMaxBounds)); // (after the filters are pushed down)
    rules.insert(end, Arc::new(WideTopN)); // (likewise)
    rules
}

/// DataFusion's `ProjectionPushdown`, but not over a projection right above a filter that already
/// has one of its own. DataFusion 55 (and 54) pushes such a projection through the filter as if the
/// filter's input were its output, so its columns point at others: `SELECT a.c || b.c, a.d FROM a
/// LEFT JOIN b ON a.k = b.k WHERE b.e IS DISTINCT FROM 3` failed when it ran a second time (found
/// by `tools/random_sql.py`), and columns of one name and type would have been taken for each
/// other unnoticed. A projection left where it was costs next to nothing.
#[derive(Debug)]
struct GuardedPushdown(Arc<dyn PhysicalOptimizerRule + Send + Sync>);

impl PhysicalOptimizerRule for GuardedPushdown {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let over = |p: &Arc<dyn ExecutionPlan>| p.downcast_ref::<ProjectionExec>().and_then(|p| p.input().downcast_ref::<FilterExec>()).is_some_and(|f| f.projection().is_some());
        match plan.exists(|p| Ok(over(p)))? {
            true => Ok(plan),
            false => self.0.optimize(plan, config),
        }
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn schema_check(&self) -> bool {
        self.0.schema_check()
    }
}

/// A global `min` / `max` hands the scans a filter of the rows that could still change its answer
/// (DataFusion's `a < least so far OR b > greatest so far`), and the scans skip row groups and hot
/// batches by it. DataFusion leaves two things out of it: a min or max of anything but a column,
/// and one with no value yet (every row so far NULL in its column). The filter then skips rows
/// those still need: `min(a), max(b + 1)` came back too low, and `min(a), max(b)` NULL when the
/// first files' `b` were. Where that can happen the aggregate keeps a filter of its own and the
/// scans keep theirs, which never moves from `true`. It can't happen with one aggregate, or with
/// several of one column, or of columns that are never NULL, and none with a FILTER.
#[derive(Debug)]
struct MinMaxBounds;

impl PhysicalOptimizerRule for MinMaxBounds {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(a) = p.downcast_ref::<AggregateExec>() else { return Ok(Transformed::no(p)) };
            let produced = a.dynamic_expressions_produced();
            let Some(filter) = produced.first().and_then(|f| f.downcast_ref::<DynamicFilterPhysicalExpr>()) else { return Ok(Transformed::no(p)) };
            if bounds_whole(a) {
                return Ok(Transformed::no(p));
            }
            let own = DynamicFilterPhysicalExpr::new(filter.children().into_iter().cloned().collect(), lit(true));
            Ok(Transformed::yes(Arc::new(a.clone().with_dynamic_filter_expr(Arc::new(own))?) as Arc<dyn ExecutionPlan>))
        })
        .data()
    }

    fn name(&self) -> &str {
        "min_max_bounds"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A top-N of many columns (`SELECT * … WHERE … ORDER BY t LIMIT 10`) reads its Parquet files
/// filtering as it decodes: the filters' and the sort key's columns first, every other column only
/// for the rows they keep, as DuckDB fetches them (ClickBench q24 from files 2.34 → 0.57 s).
/// Anywhere else decoding all of a scan's columns at once is faster (with it on for every scan,
/// TPC-H from files took a third longer), so only under a top-N, through what keeps its rows as they
/// are, and only for a scan of at least `WIDE` columns. The filters still run above it.
#[derive(Debug)]
struct WideTopN;

const WIDE: usize = 16;

impl PhysicalOptimizerRule for WideTopN {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|p| {
            if !p.downcast_ref::<SortExec>().is_some_and(|s| s.fetch().is_some()) {
                return Ok(Transformed::no(p));
            }
            let child = late(p.children()[0].clone())?;
            if Arc::ptr_eq(&child, p.children()[0]) {
                return Ok(Transformed::no(p));
            }
            Ok(Transformed::new(replace_children_if_necessary(p.clone(), vec![child])?, true, TreeNodeRecursion::Jump))
        })
        .data()
    }

    fn name(&self) -> &str {
        "wide_top_n"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `p` with its wide Parquet scans filtering as they decode, through nodes that keep rows as they are.
fn late(p: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(scan) = p.downcast_ref::<DataSourceExec>() {
        return Ok(match scan.downcast_to_file_source::<ParquetSource>() {
            Some((config, parquet)) if p.schema().fields().len() >= WIDE && !parquet.table_parquet_options().global.pushdown_filters => {
                let parquet = parquet.clone().with_pushdown_filters(true).with_reorder_filters(true);
                DataSourceExec::from_data_source(FileScanConfigBuilder::from(config.clone()).with_source(Arc::new(parquet)).build())
            }
            _ => p,
        });
    }
    if !(p.is::<ProjectionExec>() || p.is::<FilterExec>() || p.is::<RepartitionExec>() || p.is::<CoalescePartitionsExec>() || p.is::<UnionExec>()) {
        return Ok(p);
    }
    let children = p.children().into_iter().map(|c| late(c.clone())).collect::<Result<Vec<_>>>()?;
    replace_children_if_necessary(p, children)
}

/// Does every bound of `a`'s filter have a value as soon as any has (so none is left out while
/// NULL), with each aggregate's argument a column (so none is left out at all)?
fn bounds_whole(a: &AggregateExec) -> bool {
    let schema = a.input().schema();
    let columns: Option<Vec<usize>> = a
        .aggr_expr()
        .iter()
        .map(|e| match &e.expressions()[..] {
            [c] => c.downcast_ref::<PhysicalColumn>().map(|c| c.index()),
            _ => None,
        })
        .collect();
    match columns.as_deref() {
        None => false,
        Some([_]) => true,
        Some(columns) => {
            a.filter_expr().iter().all(Option::is_none)
                && (columns.iter().all(|c| *c == columns[0]) || columns.iter().all(|c| !schema.field(*c).is_nullable()))
        }
    }
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
/// multiplied its rows (TPC-H q18 joins 57 orders instead of 6 million lineitems). Always for
/// subqueries that reduce their input (an aggregate or a limit); any other only onto a table no
/// bigger than what it is joined to: a semi join against a big table is better left after the
/// joins that shrink its other side. Before the join order is chosen, which then knows the table
/// is cut (`size`).
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
        if semi.null_aware || on.is_empty() || inner.join_type != JoinType::Inner {
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
        let (mine, other) = match (owns(&inner.left), owns(&inner.right)) {
            (true, _) => (&inner.left, &inner.right),
            (_, true) => (&inner.right, &inner.left),
            _ => return Ok(Transformed::no(plan)),
        };
        // Any other set goes down only onto the smaller side: a table it filters before the joins
        // read it (TPC-DS q58's one week of dates: 0.8 s, DuckDB 0.05 s), not the big one whose rows
        // the joins above cut first.
        let reduced = set.exists(|p| Ok(matches!(p, LogicalPlan::Aggregate(_) | LogicalPlan::Limit(_))))?;
        if !reduced && !matches!((size(mine), size(other)), (Some(a), Some(b)) if a.rows <= b.rows) {
            return Ok(Transformed::no(plan));
        }
        let (left, right) = match Arc::ptr_eq(mine, &inner.left) {
            true => (below(&inner.left)?, inner.right.clone()),
            false => (inner.left.clone(), below(&inner.right)?),
        };
        let j = Join::try_new(left, right, inner.on.clone(), inner.filter.clone(), inner.join_type, inner.join_constraint, inner.null_equality, inner.null_aware)?;
        Ok(Transformed::yes(LogicalPlan::Join(j)))
    }
}

/// An inner join that reads nothing of a LEFT JOIN's padded side runs before it: `(a LEFT JOIN b)
/// JOIN c ON a.x = c.y` is `(a JOIN c) LEFT JOIN b`, and the inner joins are the ones that cut rows.
/// As written, TPC-DS q80 matched every store sale to its returns before one month of dates kept a
/// twelfth of them (3.4 s; DuckDB 0.08 s), and `JoinOrder` couldn't see past the outer join to
/// order the inner ones.
#[derive(Debug)]
struct OuterLast;

impl OptimizerRule for OuterLast {
    fn name(&self) -> &str {
        "outer_last"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(j) = &plan else { return Ok(Transformed::no(plan)) };
        if j.join_type != JoinType::Inner {
            return Ok(Transformed::no(plan));
        }
        let was = Arc::new(plan);
        let now = lifted(was.clone())?;
        if Arc::ptr_eq(&now, &was) {
            drop(now);
            return Ok(Transformed::no(Arc::unwrap_or_clone(was)));
        }
        let schema = Arc::clone(was.schema());
        Ok(Transformed::yes(LogicalPlan::Projection(Projection::new_from_schema(now, schema)))) // (the columns as the query had them)
    }
}

/// `plan`'s inner joins with every LEFT JOIN among their inputs lifted above the ones that read
/// none of its padded side, its columns in another order; `plan` itself when there is none. (An
/// as-of join stays where it is: its plan has a shape of its own.)
fn lifted(plan: Arc<LogicalPlan>) -> Result<Arc<LogicalPlan>> {
    let LogicalPlan::Join(j) = plan.as_ref() else { return Ok(plan) };
    if j.join_type != JoinType::Inner {
        return Ok(plan);
    }
    let (left, right) = (lifted(j.left.clone())?, lifted(j.right.clone())?);
    let used: Vec<&Column> = j.on.iter().flat_map(|(l, r)| [l, r]).chain(&j.filter).flat_map(|e| e.column_refs()).collect();
    let padded = |p: &LogicalPlan| match p {
        LogicalPlan::Join(o) if o.join_type == JoinType::Left && !crate::asof::marked(o) && !used.iter().any(|c| o.right.schema().has_column(c)) => Some(o.clone()),
        _ => None,
    };
    let inner = |l, r| Ok::<_, datafusion::error::DataFusionError>(Arc::new(LogicalPlan::Join(Join::try_new(l, r, j.on.clone(), j.filter.clone(), JoinType::Inner, j.join_constraint, j.null_equality, j.null_aware)?)));
    let (kept, outer) = match (padded(&left), padded(&right)) {
        (Some(o), _) => (inner(o.left.clone(), right)?, o),
        (_, Some(o)) => (inner(left, o.left.clone())?, o),
        _ if Arc::ptr_eq(&left, &j.left) && Arc::ptr_eq(&right, &j.right) => return Ok(plan),
        _ => return inner(left, right),
    };
    let kept = lifted(kept)?; // (a LEFT JOIN under that one goes up too)
    Ok(Arc::new(LogicalPlan::Join(Join::try_new(kept, outer.right, outer.on, outer.filter, JoinType::Left, outer.join_constraint, outer.null_equality, outer.null_aware)?)))
}

/// Inner joins run in the order the query names them, so a query that starts from its biggest
/// table carries those rows through every join after it. This picks the order by what the catalog
/// already knows — each table's row count and each column's range (`query::Pruned::statistics`,
/// from the file and manifest entries, which cost nothing to read) — building the tree one input
/// at a time, each time the one that leaves the fewest rows in flight, from each input in turn,
/// and taking the cheapest of those trees.
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
/// `0` turns the rule off). `PONDRA_DEBUG_JOIN_ORDER=1` prints both costs of every tree it weighs.
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
        let Some(mut sizes) = leaves.iter().map(size).collect::<Option<Vec<Size>>>() else { return Ok(Transformed::no(plan)) };
        let named: std::collections::HashSet<&str> = keys.iter().flat_map(|(l, r)| [l, r]).flat_map(|e| e.column_refs()).map(|c| c.name.as_str()).collect();
        sizes.iter_mut().for_each(|s| s.distinct.retain(|c, _| named.contains(c.as_str()))); // (only keys' bounds are ever asked for)
        let asked: Vec<usize> = (0..leaves.len()).collect();
        // The greedy order from every input in turn, the cheapest taken: starting from the smallest
        // alone began TPC-DS q72 at its 5 warehouses, then all of the inventory (81 s; DuckDB's
        // order, the sales cut by their dates and demographics first, takes 1 s). Only orders whose
        // every step joins on a key count: a tree with a cross join in it is one these estimates
        // can say nothing useful about.
        // Costed over which inputs each key reads (`reads`), not over schemas built a step at a time:
        // every start's every candidate, in an 18-table query, made q64's planning 0.5 s longer.
        let Some(sides) = reads(&leaves, &keys, plan.schema()) else { return Ok(Transformed::no(plan)) };
        let mut starts = asked.clone();
        starts.sort_by_key(|&i| (sizes[i].rows, i)); // (ties: the smallest first, as before)
        let mut best: Option<(u64, Vec<usize>)> = None;
        for first in starts {
            let order = cheapest(&sizes, &keys, &sides, first);
            if let Some(cost) = rows_moved(&sizes, &keys, &sides, &order) {
                if best.as_ref().is_none_or(|(b, _)| cost < *b) {
                    best = Some((cost, order));
                }
            }
        }
        // Both costed the same way.
        let (Some((_, was)), Some((now, order))) = (as_written(&plan, equality)?, best) else { return Ok(Transformed::no(plan)) };
        if std::env::var_os("PONDRA_DEBUG_JOIN_ORDER").is_some() {
            let named: Vec<String> = leaves.iter().zip(&sizes).map(|(l, s)| format!("{} ({} rows)", l.display(), s.rows)).collect();
            eprintln!("join order: as written {was} rows moved, {now} in the order {order:?} of {named:?}");
        }
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
            let (on, rest) = equalities(j);
            keys.extend(on);
            filters.extend(rest);
            flatten(&j.left, equality, leaves, keys, filters);
            flatten(&j.right, equality, leaves, keys, filters);
        }
        _ => leaves.push(plan.clone()),
    }
}

/// A join's equi-keys — its `on`, and each equality in its condition that pairs a column of
/// either side — and the rest of its condition. Conditions pushed into a join are not keys until
/// the next pass (`extract_equijoin_predicate`), and by then projections sit between the joins and
/// the tree can't be taken apart: TPC-H q21's comma joins under its EXISTS were never reordered.
fn equalities(j: &Join) -> (Vec<(Expr, Expr)>, Vec<Expr>) {
    let (mut keys, mut rest) = (j.on.clone(), vec![]);
    for f in j.filter.iter().flat_map(split_conjunction) {
        let pair = match f {
            Expr::BinaryExpr(b) if b.op == Operator::Eq && j.null_equality == NullEquality::NullEqualsNothing => find_valid_equijoin_key_pair(&b.left, &b.right, j.left.schema(), j.right.schema()).ok().flatten(),
            _ => None,
        };
        match pair {
            Some(pair) => keys.push(pair),
            None => rest.push(f.clone()),
        }
    }
    (keys, rest)
}

/// How big a join input is: its rows, and an upper bound on each column's distinct values where
/// the catalog knows one (by column name: a name is unique within one table, and an input holding
/// more than one table is left without bounds).
#[derive(Clone, Default)]
struct Size {
    rows: u64,
    distinct: std::collections::HashMap<String, u64>,
    spans: std::collections::HashMap<String, (f64, f64)>, // (a table's columns' least and greatest values, as numbers: for its filters)
}

impl Size {
    /// The same input cut to `rows` (a filter, or a join that kept some of them).
    fn cut(&self, rows: u64) -> Size {
        Size { rows, distinct: self.distinct.iter().map(|(c, &n)| (c.clone(), n.min(rows))).collect(), spans: self.spans.clone() }
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

/// The two put together, as a join on `on` leaves them. Of each side only the rows whose key the
/// other side has go on (the side with fewer key values has its values among the other's), and
/// none of that side's columns keeps more values than those rows: inventory joined to all of
/// `date_dim` by its 261 dates keeps 261 of the dates' weeks, not all 10,436, and a later join on
/// the week counted as if 2% of inventory met each day of a year (TPC-DS q72).
fn joined(a: &Size, b: &Size, on: &[(&Expr, &Expr)], rows: u64) -> Size {
    let d = |s: &Size, e: &Expr| s.of(e).unwrap_or(s.rows).max(1) as f64;
    let (mut met_a, mut met_b) = (1f64, 1f64); // (the share of each side's rows that meets the other)
    for &(l, r) in on {
        (met_a, met_b) = (met_a.min(d(b, r) / d(a, l)), met_b.min(d(a, l) / d(b, r)));
    }
    let left = |s: &Size, met: f64| rows.min((s.rows as f64 * met).ceil() as u64);
    let mut distinct = a.cut(left(a, met_a)).distinct;
    for (c, n) in b.cut(left(b, met_b)).distinct {
        let n = distinct.get(&c).map_or(n, |had| n.min(*had)); // (the same name twice: take the smaller — the join looks no cheaper than it is)
        distinct.insert(c, n);
    }
    Size { rows, distinct, spans: Default::default() }
}

/// How many rows a join of `a` and `b` on `on` leaves: every row of one side meets the rows of the
/// other that share its key, which is `rows(a) × rows(b) / distinct(key)` — the textbook estimate,
/// and the one that catches a join on a column with few values (TPC-H q5 joins customers to
/// suppliers by nation: 25 values, so every customer meets 400 suppliers). A side whose distinct
/// count nothing knows counts as one row per value, which is what a key usually is. A join on
/// several keys counts the one that spreads the rows most, not their product: the keys of a row
/// go together (lineitem's part and supplier are partsupp's key), and multiplied they made TPC-H q9
/// start by joining partsupp to all of lineitem, as if 2,400 rows came out (6 million do).
fn join_rows(a: &Size, b: &Size, on: &[(&Expr, &Expr)]) -> u64 {
    let (ra, rb) = (a.rows.max(1) as f64, b.rows.max(1) as f64);
    if on.is_empty() {
        return (ra * rb).min(u64::MAX as f64) as u64; // a cross join
    }
    let spread: f64 = on.iter().map(|(l, r)| {
        let d = |s: &Size, e: &Expr, rows: f64| s.of(e).map_or(rows, |n| n as f64);
        d(a, l, ra).max(d(b, r, rb)).max(1.0)
    }).fold(1.0, f64::max);
    (ra * rb / spread).max(1.0).min(u64::MAX as f64) as u64
}

/// Which inputs each key's two sides read (a bit an input), for a key every column of which is in
/// exactly one input and that can be hashed; `None` for more than 64 inputs.
fn reads(leaves: &[LogicalPlan], keys: &[(Expr, Expr)], all: &DFSchema) -> Option<Vec<Option<(u64, u64)>>> {
    use datafusion::logical_expr::ExprSchemable;
    if leaves.len() > 64 {
        return None;
    }
    let side = |e: &Expr| -> Option<u64> {
        let mut bits = 0u64;
        for c in e.column_refs() {
            let mut whose = leaves.iter().enumerate().filter(|(_, l)| l.schema().has_column(c)).map(|(n, _)| n);
            let (Some(n), None) = (whose.next(), whose.next()) else { return None };
            bits |= 1 << n;
        }
        (bits != 0).then_some(bits)
    };
    Some(keys.iter().map(|(l, r)| Some((side(l)?, side(r)?)).filter(|_| l.get_type(all).is_ok_and(|t| can_hash(&t)))).collect())
}

/// The keys that join input `i` to the inputs in `built`, each written (built side, `i`'s side), as
/// `connect` finds them in the schemas.
fn joining<'a>(keys: &'a [(Expr, Expr)], sides: &[Option<(u64, u64)>], built: u64, i: usize) -> Vec<(&'a Expr, &'a Expr)> {
    let mut on: Vec<(&Expr, &Expr)> = vec![];
    for ((l, r), side) in keys.iter().zip(sides) {
        let pair = match *side {
            Some((a, b)) if a & !built == 0 && b == 1 << i => (l, r),
            Some((a, b)) if b & !built == 0 && a == 1 << i => (r, l),
            _ => continue,
        };
        if !on.contains(&pair) {
            on.push(pair);
        }
    }
    on
}

/// The order to join the inputs in, starting from `first`: each time the input that leaves the
/// fewest rows. Inputs that share no key with what is built go last — a cross join the query
/// already asked for, never one this makes up.
fn cheapest(sizes: &[Size], keys: &[(Expr, Expr)], sides: &[Option<(u64, u64)>], first: usize) -> Vec<usize> {
    let mut todo: Vec<usize> = (0..sizes.len()).filter(|&i| i != first).collect();
    todo.sort_by_key(|&i| (sizes[i].rows, i));
    let (mut order, mut built, mut mask) = (vec![first], sizes[first].clone(), 1u64 << first);
    while !todo.is_empty() {
        let mut best: Option<(u64, usize, usize)> = None; // (rows, whether it is joined at all, place in todo)
        for (at, &i) in todo.iter().enumerate() {
            let on = joining(keys, sides, mask, i);
            let rank = (join_rows(&built, &sizes[i], &on), usize::from(on.is_empty()), at);
            if best.is_none_or(|b| (rank.1, rank.0) < (b.1, b.0)) {
                best = Some(rank);
            }
        }
        let (rows, _, at) = best.expect("something is left to join");
        let i = todo.remove(at);
        let on = joining(keys, sides, mask, i);
        (built, mask) = (joined(&built, &sizes[i], &on, rows), mask | 1 << i);
        order.push(i);
    }
    order
}

/// What the tree as the query wrote it costs, and how big its result is — the shape it has, which
/// may be deeper than left-deep. The order this rule picks has to beat this to be worth it.
fn as_written(plan: &LogicalPlan, equality: NullEquality) -> Result<Option<(Size, u64)>> {
    let LogicalPlan::Join(j) = plan else { return Ok(size(plan).map(|s| (s, 0))) };
    if !flat(j, equality) {
        return Ok(size(plan).map(|s| (s, 0)));
    }
    let (Some((l, cl)), Some((r, cr))) = (as_written(&j.left, equality)?, as_written(&j.right, equality)?) else { return Ok(None) };
    let (on, _) = equalities(j);
    if on.is_empty() {
        return Ok(None);
    }
    let on: Vec<(&Expr, &Expr)> = on.iter().map(|(a, b)| (a, b)).collect();
    let rows = join_rows(&l, &r, &on);
    Ok(Some((joined(&l, &r, &on, rows), cl.saturating_add(cr).saturating_add(rows))))
}

/// What joining them left-deep in this order costs: the rows every step leaves, added up.
fn rows_moved(sizes: &[Size], keys: &[(Expr, Expr)], sides: &[Option<(u64, u64)>], order: &[usize]) -> Option<u64> {
    let (mut built, mut mask, mut total) = (sizes[order[0]].clone(), 1u64 << order[0], 0u64);
    for &i in &order[1..] {
        let on = joining(keys, sides, mask, i);
        if on.is_empty() {
            return None; // a step with nothing to join on: not an order worth trusting
        }
        let rows = join_rows(&built, &sizes[i], &on);
        (built, mask, total) = (joined(&built, &sizes[i], &on, rows), mask | 1 << i, total.saturating_add(rows));
    }
    Some(total)
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

/// The share of its rows a filter's conditions keep. `column = value` keeps one of the column's
/// distinct values and `column IN (…)` as many as it lists; ranges on a column whose least and
/// greatest values the catalog knows keep the part of that span they leave, both ends of one column
/// together (a month of dates is a month of the table's years, not a quarter of them). Anything else
/// keeps about a third. A third for every condition made a month of TPC-DS's dates look like a
/// tenth of them, so its joins started from the sales instead.
fn kept(s: &Size, conds: &[&Expr]) -> f64 {
    const THIRD: f64 = 0.3;
    fn literal(e: &Expr) -> Option<f64> {
        match e {
            Expr::Literal(v, _) => number(v),
            _ => None,
        }
    }
    fn column(e: &Expr) -> Option<&str> {
        match e {
            Expr::Column(c) => Some(c.name.as_str()),
            Expr::Cast(c) => column(&c.expr),
            _ => None,
        }
    }
    // A condition that bounds a column from below or above: the column, and the bounds it leaves.
    fn bound(e: &Expr) -> Option<(&str, f64, f64)> {
        let (inf, sup) = (f64::NEG_INFINITY, f64::INFINITY);
        match e {
            Expr::Between(b) if !b.negated => Some((column(&b.expr)?, literal(&b.low)?, literal(&b.high)?)),
            Expr::BinaryExpr(b) => {
                let (c, v, op) = match (column(&b.left), literal(&b.right)) {
                    (Some(c), Some(v)) => (c, v, b.op),
                    _ => (column(&b.right)?, literal(&b.left)?, b.op.swap()?),
                };
                match op {
                    Operator::Gt | Operator::GtEq => Some((c, v, sup)),
                    Operator::Lt | Operator::LtEq => Some((c, inf, v)),
                    _ => None,
                }
            }
            _ => None,
        }
    }
    let (mut share, mut left) = (1.0, std::collections::HashMap::<&str, (f64, f64)>::new());
    for e in conds {
        if let Some((c, lo, hi)) = bound(e).filter(|(c, ..)| s.spans.contains_key(*c)) {
            let b = left.entry(c).or_insert(s.spans[c]);
            *b = (b.0.max(lo), b.1.min(hi));
            continue;
        }
        share *= match e {
            Expr::BinaryExpr(b) if b.op == Operator::Eq && (literal(&b.left).is_some() || literal(&b.right).is_some()) => {
                let other = if literal(&b.right).is_some() { &b.left } else { &b.right };
                s.of(other).map_or(THIRD, |d| 1.0 / d.max(1) as f64)
            }
            Expr::InList(l) if !l.negated && l.list.iter().all(|v| literal(v).is_some()) => s.of(&l.expr).map_or(THIRD, |d| (l.list.len() as f64 / d.max(1) as f64).min(1.0)),
            _ => THIRD,
        };
    }
    for (c, (lo, hi)) in left {
        let (min, max) = s.spans[c];
        let least = 1.0 / s.distinct.get(c).copied().unwrap_or(s.rows).max(1) as f64; // (one value, at the least)
        share *= if max > min { ((hi - lo) / (max - min)).clamp(least, 1.0) } else { 1.0 };
    }
    share
}

/// A value as a number on its column's own scale (dates in days, times in seconds), for spans.
fn number(v: &datafusion::common::ScalarValue) -> Option<f64> {
    use datafusion::common::ScalarValue as V;
    Some(match v {
        V::Int8(Some(x)) => *x as f64,
        V::Int16(Some(x)) => *x as f64,
        V::Int32(Some(x)) => *x as f64,
        V::Int64(Some(x)) => *x as f64,
        V::UInt8(Some(x)) => *x as f64,
        V::UInt16(Some(x)) => *x as f64,
        V::UInt32(Some(x)) => *x as f64,
        V::UInt64(Some(x)) => *x as f64,
        V::Float32(Some(x)) => *x as f64,
        V::Float64(Some(x)) => *x,
        V::Decimal128(Some(x), _, scale) => *x as f64 / 10f64.powi(*scale as i32),
        V::Date32(Some(x)) => *x as f64,
        V::Date64(Some(x)) => *x as f64 / 86_400_000.0,
        V::TimestampSecond(Some(x), _) => *x as f64,
        V::TimestampMillisecond(Some(x), _) => *x as f64 / 1e3,
        V::TimestampMicrosecond(Some(x), _) => *x as f64 / 1e6,
        V::TimestampNanosecond(Some(x), _) => *x as f64 / 1e9,
        _ => return None,
    }.into()).filter(|n: &f64| n.is_finite())
}

/// How big a join input is, as well as anything here can say: a table's own count from the
/// catalog, scaled by the filters above it (`kept`), and the column bounds that came with it.
/// `None` where nothing knows — then the joins are left in the order the query wrote them.
fn size(plan: &LogicalPlan) -> Option<Size> {
    let groups = |s: &Size| s.cut((s.rows as f64).sqrt().ceil() as u64); // (a grouping's rows: unknowable, but far fewer)
    let rows = |n: u64| Size { rows: n, ..Default::default() };
    Some(match plan {
        LogicalPlan::TableScan(s) => {
            let stats = datafusion::datasource::source_as_provider(&s.source).ok()?.statistics()?;
            // `column_statistics` covers the table's own schema, not the columns this scan reads.
            let columns = || s.source.schema().fields().iter().zip(&stats.column_statistics).map(|(f, c)| (f.name().clone(), c)).collect::<Vec<_>>();
            let distinct = columns().into_iter().filter_map(|(f, c)| Some((f, *c.distinct_count.get_value()? as u64))).collect();
            let spans = columns().into_iter().filter_map(|(f, c)| Some((f, (number(c.min_value.get_value()?)?, number(c.max_value.get_value()?)?)))).collect();
            let whole = Size { rows: *stats.num_rows.get_value()? as u64, distinct, spans };
            whole.cut(whole.rows.min(s.fetch.unwrap_or(usize::MAX) as u64)) // (its filters are counted by the Filter above it: a lake's tables take them inexactly)
        }
        LogicalPlan::Filter(f) => {
            let input = size(&f.input)?;
            input.cut((input.rows as f64 * kept(&input, &split_conjunction(&f.predicate))).ceil() as u64)
        }
        LogicalPlan::Projection(p) => size(&p.input)?,
        LogicalPlan::SubqueryAlias(a) => size(&a.input)?,
        LogicalPlan::Sort(s) => size(&s.input)?,
        LogicalPlan::Limit(l) => size(&l.input)?,
        LogicalPlan::Aggregate(a) if a.group_expr.is_empty() => rows(1),
        LogicalPlan::Aggregate(a) => groups(&size(&a.input)?),
        LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(input)) => groups(&size(input)?),
        LogicalPlan::Distinct(datafusion::logical_expr::Distinct::On(d)) => groups(&size(&d.input)?),
        LogicalPlan::Union(u) => rows(u.inputs.iter().map(|i| Some(size(i)?.rows)).sum::<Option<u64>>()?),
        LogicalPlan::Join(j) if matches!(j.join_type, JoinType::LeftSemi | JoinType::RightSemi) => {
            let (kept, set) = if j.join_type == JoinType::LeftSemi { (size(&j.left)?, size(&j.right)?) } else { (size(&j.right)?, size(&j.left)?) };
            kept.cut(kept.rows.min(set.rows)) // (a set of keys keeps at most a row for each, of a table with one row a key)
        }
        LogicalPlan::Join(j) if j.join_type == JoinType::LeftAnti => size(&j.left)?,
        LogicalPlan::Join(j) if j.join_type == JoinType::RightAnti => size(&j.right)?,
        LogicalPlan::Join(j) => rows(size(&j.left)?.rows.max(size(&j.right)?.rows)),
        LogicalPlan::Values(v) => rows(v.values.len() as u64),
        LogicalPlan::EmptyRelation(_) => rows(1),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    /// A table's size: its rows, and each column's (name, distinct values, least, greatest).
    fn table(rows: u64, columns: &[(&str, u64, f64, f64)]) -> Size {
        let distinct = columns.iter().map(|c| (c.0.to_string(), c.1)).collect();
        Size { rows, distinct, spans: columns.iter().map(|c| (c.0.to_string(), (c.2, c.3))).collect() }
    }

    /// A month of 200 years of dates is a month of them (both ends of the range together), a year
    /// one of its 201 values, and a condition nothing knows about about a third.
    #[test]
    fn filters_keep_their_share() {
        let dates = table(73_049, &[("d_date_sk", 73_049, 2_415_022.0, 2_488_070.0), ("d_year", 201, 1900.0, 2100.0)]);
        let month = kept(&dates, &[&col("d_date_sk").gt_eq(lit(2_451_000)), &col("d_date_sk").lt(lit(2_451_030))]);
        assert!((month * 73_049.0 - 30.0).abs() < 1.0, "{month}");
        assert!((kept(&dates, &[&col("d_year").eq(lit(1999))]) - 1.0 / 201.0).abs() < 1e-12);
        assert_eq!(kept(&dates, &[&col("d_year").is_not_null()]), 0.3);
    }

    /// Inventory joined to every date by its 261 dates keeps 261 of the dates' weeks, so a later
    /// join on the week isn't taken for a cut (TPC-DS q72 went 10× slower when it was).
    #[test]
    fn a_join_keeps_only_the_values_it_meets() {
        let inventory = table(11_745_000, &[("inv_date_sk", 261, 2_450_815.0, 2_452_635.0)]);
        let dates = table(73_049, &[("d_date_sk", 73_049, 2_415_022.0, 2_488_070.0), ("d_week_seq", 10_436, 1.0, 10_436.0)]);
        let (l, r) = (col("inv_date_sk"), col("d_date_sk"));
        let both = joined(&inventory, &dates, &[(&l, &r)], join_rows(&inventory, &dates, &[(&l, &r)]));
        assert_eq!(both.rows, 11_745_000);
        assert_eq!((both.distinct["d_week_seq"], both.distinct["d_date_sk"], both.distinct["inv_date_sk"]), (261, 261, 261));
    }
}
