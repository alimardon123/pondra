//! Planning rules Pondra adds to DataFusion's, and the engine settings it starts from.
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, NullEquality, Result};
use datafusion::logical_expr::utils::{can_hash, conjunction, disjunction, find_valid_equijoin_key_pair, split_binary, split_conjunction};
use datafusion::logical_expr::{ExprSchemable, build_join_schema, Aggregate, Expr, Filter, Join, JoinConstraint, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Projection, SubqueryAlias};
use datafusion::optimizer::{analyzer::{Analyzer, AnalyzerRule}, optimizer::ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule};
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
    let user = crate::env::get("PONDRA_SQL_OPTIONS").unwrap_or_default();
    let defaults = "datafusion.sql_parser.parse_float_as_decimal=true,\
        datafusion.execution.time_zone=+00:00,\
        datafusion.optimizer.hash_join_single_partition_threshold=33554432,\
        datafusion.optimizer.hash_join_single_partition_threshold_rows=1048576,\
        datafusion.execution.perfect_hash_join_small_build_threshold=262144,\
        datafusion.execution.skip_physical_aggregate_schema_check=true";
    // (the perfect hash join's: a join on one integer key whose build side's keys span under 262,144
    // values looks them up in an array of that span, at most 1 MB, however few they are; DataFusion's
    // 1,024 kept TPC-H q17's 204 parts, spread over 200,000 keys, in a hash table, and its 6 M probes
    // took 2.4 times as long)
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
    let ordered = crate::env::get("PONDRA_JOIN_ORDER").as_deref() != Some("0");
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

/// DataFusion's analyzer rules, with Pondra's before its type coercion.
pub fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let mut rules = Analyzer::new().rules;
    let at = rules.iter().position(|r| r.name() == "type_coercion").unwrap_or(0);
    rules.insert(at, Arc::new(FloatsWin));
    rules
}

/// A DOUBLE (or REAL) that meets a DECIMAL makes both DOUBLEs, as Postgres and DuckDB do: `price >
/// 9.99` compares doubles. DataFusion made both a DECIMAL(30, 15) instead, which cast every row of
/// the column (TPC-H q6 compares a DOUBLE with three decimals) and failed on any value of 10^15 or
/// more ("too large to store in a Decimal128 of precision 30"). Done before DataFusion's type
/// coercion, which then finds both sides alike: comparisons and arithmetic, BETWEEN, IN lists,
/// CASE's branches, COALESCE and its kind, a join's keys and a UNION's columns. (An INSERT's VALUES
/// take their columns' types: `write::rows`.)
#[derive(Debug)]
struct FloatsWin;

impl AnalyzerRule for FloatsWin {
    fn name(&self) -> &str {
        "pondra_floats_win"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            if !may_float(&plan) {
                return Ok(Transformed::no(plan)); // (most steps of most queries: no float can meet anything there)
            }
            let plan = match plan {
                LogicalPlan::Union(u) => union_floats(u)?,
                LogicalPlan::Join(j) => join_floats(j)?,
                plan => Transformed::no(plan),
            };
            plan.transform_data(|plan| {
                let mut schema = datafusion::logical_expr::utils::merge_schema(&plan.inputs());
                if let LogicalPlan::TableScan(ts) = &plan {
                    schema.merge(&DFSchema::try_from_qualified_schema(ts.table_name.clone(), &ts.source.schema())?);
                }
                let names = datafusion::logical_expr::expr_rewriter::NamePreserver::new(&plan); // (a column keeps the name its expression gave it)
                let t = plan.map_expressions(|e| {
                    let name = names.save(&e);
                    Ok(e.transform_up(|e| expr_floats(e, &schema))?.update_data(|e| name.restore(e)))
                })?;
                if t.transformed { t.map_data(|p| p.recompute_schema()) } else { Ok(t) } // (a node above reads the types its columns have now)
            })
        })
        .data()
    }
}

/// Whether a float may be among a step's expressions or its UNION's columns: a column of its inputs
/// (or of the table a scan's filters read) is a REAL or DOUBLE, or an expression is a float literal,
/// a cast to one, or something whose type comes from elsewhere (a function's or an aggregate's
/// answer, a subquery's, a parameter, an outer query's column, an unnested list). Nothing else
/// makes one, so a step with none is left as it is without working out its types: working them
/// out for every step cost join-heavy queries 3–7% more planning (TPC-H q8 2.9 ms).
fn may_float(plan: &LogicalPlan) -> bool {
    let floats = |s: &DFSchema| s.fields().iter().any(|f| f.data_type().is_floating());
    let scanned = match plan {
        LogicalPlan::TableScan(ts) => !ts.filters.is_empty() && ts.source.schema().fields().iter().any(|f| f.data_type().is_floating()),
        _ => false,
    };
    if scanned || plan.inputs().into_iter().any(|p| floats(p.schema())) {
        return true;
    }
    let mut may = false;
    let _ = plan.apply_expressions(|e| {
        e.apply(|e| {
            may = match e {
                Expr::Literal(v, _) => v.data_type().is_floating(),
                Expr::Cast(c) => c.field.data_type().is_floating(),
                Expr::TryCast(c) => c.field.data_type().is_floating(),
                Expr::ScalarVariable(f, _) => f.data_type().is_floating(),
                Expr::ScalarFunction(_) | Expr::AggregateFunction(_) | Expr::WindowFunction(_) | Expr::ScalarSubquery(_) | Expr::Placeholder(_) | Expr::OuterReferenceColumn(..) | Expr::Unnest(_) => true,
                _ => false,
            };
            Ok(if may { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
        })
    });
    may
}

/// `e` with its operands as doubles where a float meets a decimal among them.
fn expr_floats(mut e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    use Operator::*;
    let changed = match &mut e {
        Expr::BinaryExpr(b) if b.op.is_numerical_operators() || matches!(b.op, Eq | NotEq | Lt | LtEq | Gt | GtEq | IsDistinctFrom | IsNotDistinctFrom) => {
            as_doubles(vec![&mut *b.left, &mut *b.right], schema)
        }
        Expr::Between(b) => as_doubles(vec![&mut *b.expr, &mut *b.low, &mut *b.high], schema),
        Expr::InList(i) => as_doubles(std::iter::once(&mut *i.expr).chain(i.list.iter_mut()).collect(), schema),
        Expr::Case(c) => {
            let results = c.when_then_expr.iter_mut().map(|(_, t)| &mut **t).chain(c.else_expr.as_deref_mut());
            let mut changed = as_doubles(results.collect(), schema);
            if let Some(base) = c.expr.as_deref_mut() {
                changed |= as_doubles(std::iter::once(base).chain(c.when_then_expr.iter_mut().map(|(w, _)| &mut **w)).collect(), schema);
            }
            changed
        }
        Expr::ScalarFunction(f) if matches!(f.name(), "coalesce" | "nullif" | "greatest" | "least" | "nvl" | "ifnull") => as_doubles(f.args.iter_mut().collect(), schema),
        _ => false,
    };
    Ok(if changed { Transformed::yes(e) } else { Transformed::no(e) })
}

/// The decimals among `es` cast to DOUBLE when a float is among them too (whether any changed).
fn as_doubles(es: Vec<&mut Expr>, schema: &DFSchema) -> bool {
    let types: Vec<Option<DataType>> = es.iter().map(|e| (**e).get_type(schema).ok()).collect();
    if !types.iter().flatten().any(DataType::is_floating) || !types.iter().flatten().any(DataType::is_decimal) {
        return false;
    }
    for (e, t) in es.into_iter().zip(&types) {
        if t.as_ref().is_some_and(DataType::is_decimal) {
            *e = std::mem::take(e).cast_to(&DataType::Float64, schema).expect("a decimal casts to a double");
        }
    }
    true
}

/// A join's keys as doubles where a float meets a decimal.
fn join_floats(mut j: Join) -> Result<Transformed<LogicalPlan>> {
    let (left, right) = (j.left.schema().clone(), j.right.schema().clone());
    let mut changed = false;
    for (l, r) in j.on.iter_mut() {
        let (lt, rt) = (l.get_type(&left)?, r.get_type(&right)?);
        if lt.is_floating() && rt.is_decimal() {
            *r = std::mem::take(r).cast_to(&DataType::Float64, &right)?;
            changed = true;
        } else if lt.is_decimal() && rt.is_floating() {
            *l = std::mem::take(l).cast_to(&DataType::Float64, &left)?;
            changed = true;
        }
    }
    Ok(Transformed::new_transformed(LogicalPlan::Join(j), changed))
}

/// A UNION's columns as doubles where one input has a float and another a decimal there: the
/// inputs with the decimal read it as a double.
fn union_floats(u: datafusion::logical_expr::Union) -> Result<Transformed<LogicalPlan>> {
    let width = u.schema.fields().len();
    let mixed: Vec<bool> = (0..width).map(|i| {
        let types = || u.inputs.iter().map(|p| p.schema().field(i).data_type());
        types().any(DataType::is_floating) && types().any(DataType::is_decimal)
    }).collect();
    if !mixed.contains(&true) {
        return Ok(Transformed::no(LogicalPlan::Union(u)));
    }
    let inputs = u.inputs.into_iter().map(|p| {
        let wanted = |i: usize| mixed[i] && p.schema().field(i).data_type().is_decimal();
        if !(0..width).any(wanted) {
            return Ok(p);
        }
        let columns = p.schema().iter().enumerate().map(|(i, (q, f))| {
            let c = Expr::Column(Column::new(q.cloned(), f.name()));
            if wanted(i) { Ok(c.cast_to(&DataType::Float64, p.schema())?.alias_qualified(q.cloned(), f.name())) } else { Ok(c) }
        }).collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(columns, p)?)))
    }).collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(LogicalPlan::Union(datafusion::logical_expr::Union::try_new_with_loose_types(inputs)?)))
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

/// A hash join reads its other side while its table is built, a megabyte a thread ahead
/// (DataFusion's `BufferExec`), where that side groups, sorts or windows before its first row:
/// otherwise it starts only once the table is built, so two halves that each group millions of rows
/// ran one after the other (TPC-DS q59's, each grouping 2.9 million sales: 198 ms, 138 ms at once).
/// Never where that side streams its rows (scans, joins over scans): read early, it only takes cores
/// from the build and runs before the join's filter is ready (TPC-DS q22 1.4×, q90 1.7× slower with
/// every join's other side read early). In DataFusion's place for that (`HashJoinBuffering`), whose
/// setting, when given, still reads every join's other side early. Never in a spread query's
/// slices, whose exchanges are the spread analysis's: `BufferExec` is no operator it knows.
#[derive(Debug)]
struct EarlyProbes;

impl PhysicalOptimizerRule for EarlyProbes {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::buffer::BufferExec;
        if plan.exists(|p| Ok(matches!(p.name(), "ShareExec" | "WholeExec")))? {
            return Ok(plan);
        }
        let every = config.execution.hash_join_buffering_capacity; // (the user's: every join, this far ahead)
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<HashJoinExec>() else { return Ok(Transformed::no(p)) };
            let (build, probe) = (j.left().clone(), j.right().clone()); // (DataFusion's probe side is the right)
            if probe.is::<BufferExec>() || every == 0 && !waits(&probe)? {
                return Ok(Transformed::no(p));
            }
            let early: Arc<dyn ExecutionPlan> = Arc::new(BufferExec::new(probe, if every > 0 { every } else { 1 << 20 }));
            Ok(Transformed::yes(replace_children_if_necessary(p, vec![build, early])?))
        })
        .data()
    }

    fn name(&self) -> &str {
        "EarlyProbes"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Whether a plan gives its first row only once it has read all its input: it groups, sorts or
/// windows somewhere.
fn waits(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    plan.exists(|p| Ok(p.is::<AggregateExec>() || p.is::<SortExec>() || p.name() == "WindowAggExec"))
}

/// DataFusion's physical rules, with Pondra's placed where they work best.
pub fn physical_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules = PhysicalOptimizer::new().rules;
    for r in rules.iter_mut() {
        match r.name() {
            "ProjectionPushdown" => *r = Arc::new(GuardedPushdown(r.clone())),
            "HashJoinBuffering" => *r = Arc::new(EarlyProbes),
            _ => {}
        }
    }
    let sides = rules.iter().position(|r| r.name() == "join_selection").unwrap_or(0);
    rules.insert(sides, Arc::new(FilterShares { apart: true })); // (the joins' sides are picked by their inputs' sizes)
    let at = sides + 2;
    rules.insert(at, Arc::new(HavingBuilds));
    rules.insert(at + 1, Arc::new(crate::asof::Rule)); // (before the rules that add exchanges: it asks for its own)
    rules.insert(at + 2, Arc::new(SharedBuilds)); // (after both: a HAVING's few groups and an as-of join are theirs)
    let end = rules.iter().position(|r| r.name() == "SanityCheckPlan").unwrap_or(rules.len());
    rules.insert(end, Arc::new(crate::hot::TopFirst)); // (after DataFusion's own sort pushdown)
    rules.insert(end, Arc::new(MinMaxBounds)); // (after the filters are pushed down)
    let filters = rules.iter().position(|r| r.name() == "FilterPushdown").unwrap_or(0);
    rules.insert(filters + 1, Arc::new(FilterShares { apart: false })); // (again: a filter pushed into a union's parts is a new one, at DataFusion's fifth)
    rules.insert(filters, Arc::new(WideTopN)); // (before the filters are pushed down: then they go into the scan)
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

/// A filter whose condition DataFusion can't size (an OR, an IN list, a string equal to a value,
/// LIKE, a function: its interval analysis takes none of them) keeps the share Pondra counts for it
/// (`share`), as the join order does, in place of DataFusion's fifth. The joins then build on the
/// side that is smaller: TPC-H q12 built its hash table on 1.5M orders, its other side the 31K
/// lineitems an OR of ship modes leaves. A filter none of whose columns' counts or ranges are known
/// keeps DataFusion's fifth. It runs just before DataFusion picks the joins' sides, and a share
/// under 1% counts as 1%, the least a filter's estimate can say. So there (`apart`), a filter under
/// 1% with conditions DataFusion sizes beside those it can't is two filters while the sides are
/// picked: the ones it can't, at Pondra's share, under the ones it sizes by their columns' ranges,
/// which can say less. TPC-DS q68's two days of three years keep 72 of 73,049 dates, counted 731 at
/// 1%, and the joins above them built on 100,000 customers instead of the 2,000 rows they meet.
/// DataFusion's `FilterPushdown`, right after, makes them one filter again (the second run sizes it).
#[derive(Debug)]
struct FilterShares {
    apart: bool,
}

impl PhysicalOptimizerRule for FilterShares {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_expr::intervals::utils::check_support;
        use datafusion::physical_plan::filter::FilterExecBuilder;
        plan.transform_up(|p| {
            let Some(f) = p.downcast_ref::<FilterExec>() else { return Ok(Transformed::no(p)) };
            if check_support(f.predicate(), &f.input().schema()) {
                return Ok(Transformed::no(p)); // (DataFusion sizes it from its columns' ranges)
            }
            let Some(kept) = input_share(f.input(), &conds(f.predicate()))? else {
                return Ok(Transformed::no(p)); // (nothing known of its columns: DataFusion's guess stands, as before)
            };
            if self.apart && kept < 0.01 {
                if let Some(two) = apart(f)? {
                    return Ok(Transformed::yes(two));
                }
            }
            if percent(kept) == f.default_selectivity() {
                return Ok(Transformed::no(p));
            }
            Ok(Transformed::yes(Arc::new(FilterExecBuilder::from(f).with_default_selectivity(percent(kept)).build()?) as Arc<dyn ExecutionPlan>))
        })
        .data()
    }

    fn name(&self) -> &str {
        "FilterShares"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A share as a filter's estimate says it: a whole percent, 1% at least.
fn percent(kept: f64) -> u8 {
    (kept * 100.0).round().clamp(1.0, 100.0) as u8
}

/// `f` as two filters (`FilterShares`): its conditions DataFusion can't size, at the share Pondra
/// counts for them, under those it sizes from their columns' ranges. None unless it has both.
fn apart(f: &FilterExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    use datafusion::physical_expr::intervals::utils::check_support;
    use datafusion::physical_expr::{split_conjunction, utils::conjunction};
    use datafusion::physical_plan::filter::FilterExecBuilder;
    let schema = f.input().schema();
    let (sized, rest): (Vec<_>, Vec<_>) = split_conjunction(f.predicate()).into_iter().cloned().partition(|c| check_support(c, &schema));
    if sized.is_empty() || rest.is_empty() {
        return Ok(None);
    }
    let rest = conjunction(rest);
    let Some(kept) = input_share(f.input(), &conds(&rest))? else { return Ok(None) };
    let below = FilterExecBuilder::from(f).with_predicate(rest).apply_projection(None)?.with_fetch(None).with_default_selectivity(percent(kept)).build()?;
    let above = FilterExecBuilder::from(f).with_predicate(conjunction(sized)).with_input(Arc::new(below)).build()?;
    Ok(Some(Arc::new(above)))
}

/// The share of `input`'s rows a filter's conditions keep, or None when nothing is known of their
/// columns. A union's parts count apart, each with its own facts, weighed by its rows: a table with
/// rows still in its log is its log tail and its files side by side (`query::plain_scan`), and the
/// tail knows no column's count, so merged together they knew none (TPC-H q12's ship modes over a
/// lineitem with a tail: a fifth of 6 million rows, not the 31K they are).
fn input_share(input: &Arc<dyn ExecutionPlan>, conds: &[Cond<usize>]) -> Result<Option<f64>> {
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    let parts = match input.downcast_ref::<UnionExec>() {
        Some(u) => u.inputs().clone(),
        None => vec![input.clone()],
    };
    let blind = share(&Blind, conds);
    let mut counted = vec![];
    for part in &parts {
        let stats = StatisticsContext::new().compute(part.as_ref(), &StatisticsArgs::new())?;
        counted.push((stats.num_rows.get_value().map_or(0.0, |&n| n as f64), share(stats.as_ref(), conds)));
    }
    if counted.iter().all(|&(_, share)| share == blind) {
        return Ok(None);
    }
    let rows: f64 = counted.iter().map(|&(n, _)| n).sum();
    Ok(Some(match rows > 0.0 {
        true => counted.iter().map(|&(n, share)| n * share).sum::<f64>() / rows,
        false => counted.iter().map(|&(_, share)| share).sum::<f64>() / counted.len() as f64, // (no part says its rows)
    }))
}

/// Facts of nothing: what `share` keeps of a filter none of whose columns it knows.
struct Blind;

impl Facts<usize> for Blind {
    fn rows(&self) -> u64 { 0 }
    fn distinct(&self, _: &usize) -> Option<u64> { None }
    fn span(&self, _: &usize) -> Option<(f64, f64)> { None }
}

impl Facts<usize> for datafusion::common::Statistics {
    fn rows(&self) -> u64 { self.num_rows.get_value().map_or(0, |&n| n as u64) }
    fn distinct(&self, k: &usize) -> Option<u64> { self.column_statistics.get(*k)?.distinct_count.get_value().map(|&n| n as u64) }
    fn span(&self, k: &usize) -> Option<(f64, f64)> {
        let c = self.column_statistics.get(*k)?;
        Some((number(c.min_value.get_value()?)?, number(c.max_value.get_value()?)?))
    }
}

/// A physical filter's conditions as the estimates count them, as `cond` counts a logical one's.
fn conds(e: &Arc<dyn PhysicalExpr>) -> Vec<Cond<usize>> {
    datafusion::physical_expr::split_conjunction(e).into_iter().map(physical_cond).collect()
}

fn physical_cond(e: &Arc<dyn PhysicalExpr>) -> Cond<usize> {
    use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, InListExpr, Literal, NotExpr};
    fn column(e: &Arc<dyn PhysicalExpr>) -> Option<usize> {
        match e.downcast_ref::<PhysicalColumn>() {
            Some(c) => Some(c.index()),
            None => column(e.downcast_ref::<CastExpr>()?.expr()),
        }
    }
    fn value(e: &Arc<dyn PhysicalExpr>) -> Option<&datafusion::common::ScalarValue> {
        e.downcast_ref::<Literal>().map(|l| l.value())
    }
    if let Some(b) = e.downcast_ref::<BinaryExpr>() {
        if *b.op() == Operator::Or {
            return Cond::Any(datafusion::physical_expr::utils::split_disjunction(e).into_iter().map(conds).collect());
        }
        let (c, v, op) = match (column(b.left()), value(b.right()), column(b.right()), value(b.left())) {
            (Some(c), Some(v), ..) => (c, v, *b.op()),
            (.., Some(c), Some(v)) => match b.op().swap() {
                Some(op) => (c, v, op),
                None => return Cond::Other,
            },
            _ => return Cond::Other,
        };
        return match op {
            Operator::Eq => Cond::Equal(c),
            Operator::NotEq => Cond::Not(vec![Cond::Equal(c)]),
            op => number(v).and_then(|v| bounds(op, v)).map_or(Cond::Other, |(lo, hi)| Cond::Bound(c, lo, hi)),
        };
    }
    if let Some(l) = e.downcast_ref::<InListExpr>() {
        let (Some(c), true) = (column(l.expr()), l.list().iter().all(|v| value(v).is_some())) else { return Cond::Other };
        let within = Cond::Within(c, l.list().len());
        return if l.negated() { Cond::Not(vec![within]) } else { within };
    }
    match e.downcast_ref::<NotExpr>() {
        Some(n) => Cond::Not(conds(n.arg())),
        None => Cond::Other,
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
/// are, and only for a scan of at least `WIDE` columns. It runs before the filters are pushed down,
/// so the scan takes them whole and no filter is left above it: a filter holds its rows back until
/// it has a batch's worth, and a rare match then reached the top-N only once every file was read,
/// so its moving bound skipped nothing (ClickBench q24 over 100 files 1.28 s, DuckDB 0.50 s). With
/// none, the bound skips files and rows as soon as the top-N has its first rows, and DataFusion
/// reads first the files whose statistics hold the smallest keys.
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

/// Which joins build one hash table that every thread probes (`CollectLeft`) and which hash both
/// sides by their keys, a table to a thread (`Partitioned`), past DataFusion's sizes:
/// - a side to build of a million rows or more is hashed (DataFusion's own rows threshold, which it
///   reads only when a side's bytes are unknown): one thread builds a shared table, and a million rows
///   take it longer than the shuffle (TPC-H q9's 1.5 million orders);
/// - a join whose rows are then grouped by its keys and nothing else is hashed by them when its side to
///   build has a sixteenth of its other side's rows or more: the grouping then needs no shuffle of its
///   own, and each thread groups its own keys (TPC-H q13 counts 150,000 customers' orders). With fewer
///   keys, grouping first leaves little to move.
/// Never a HAVING's few groups (`HavingBuilds`: their estimate is the whole table's), a NOT IN
/// (null-aware: one table sees every NULL), or a spread query's slices, whose exchanges are the spread
/// analysis's (a broadcast side hashed would cross the network).
#[derive(Debug)]
struct SharedBuilds;

impl PhysicalOptimizerRule for SharedBuilds {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::aggregates::AggregateMode;
        let most = config.optimizer.hash_join_single_partition_threshold_rows;
        if most == 0 || plan.exists(|p| Ok(matches!(p.name(), "ShareExec" | "WholeExec")))? {
            return Ok(plan); // (with no threshold every join already hashes both sides)
        }
        plan.transform_down(|p| {
            if let Some(a) = p.downcast_ref::<AggregateExec>() {
                let single = matches!(a.mode(), AggregateMode::Partial) && a.group_expr().is_single(); // (a `Single` one gathers its rows first)
                let groups: Option<Vec<usize>> = a.group_expr().expr().iter().map(|(e, _)| e.downcast_ref::<PhysicalColumn>().map(|c| c.index())).collect();
                let input = match groups {
                    Some(groups) if single && !groups.is_empty() => grouped(a.input(), &groups)?,
                    _ => None,
                };
                return match input {
                    Some(input) => Ok(Transformed::yes(replace_children_if_necessary(p.clone(), vec![input])?)),
                    None => Ok(Transformed::no(p)),
                };
            }
            match p.downcast_ref::<HashJoinExec>() {
                Some(j) if hashable(j) && rows(j.left()).is_some_and(|n| n >= most) => Ok(Transformed::yes(partitioned(j)?)),
                _ => Ok(Transformed::no(p)),
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "shared_builds"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `p` with the join it reads (through projections and filters) hashed by its keys, when `columns` of
/// `p`'s rows are those keys and nothing else, and its sides aren't too far apart (`SharedBuilds`).
fn grouped(p: &Arc<dyn ExecutionPlan>, columns: &[usize]) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let through = |projection: &Option<datafusion::physical_expr::projection::ProjectionRef>| match projection {
        Some(kept) => columns.iter().map(|&c| kept[c]).collect(),
        None => columns.to_vec(),
    };
    if let Some(j) = p.downcast_ref::<HashJoinExec>() {
        let near = matches!((rows(j.left()), rows(j.right())), (Some(build), Some(probe)) if build >= probe / 16);
        return Ok(match hashable(j) && near && keyed(j, &through(&j.projection)) {
            true => Some(partitioned(j)?),
            false => None,
        });
    }
    let below: Option<Vec<usize>> = if let Some(projection) = p.downcast_ref::<ProjectionExec>() {
        columns.iter().map(|&c| projection.expr()[c].expr.downcast_ref::<PhysicalColumn>().map(|c| c.index())).collect()
    } else {
        p.downcast_ref::<FilterExec>().map(|f| through(f.projection()))
    };
    let Some(below) = below else { return Ok(None) };
    match grouped(p.children()[0], &below)? {
        Some(input) => Ok(Some(replace_children_if_necessary(p.clone(), vec![input])?)),
        None => Ok(None),
    }
}

/// Are `columns` of a hash join's rows (before its projection) its keys, one for each, on a side its
/// rows would be hashed by? (An inner join's keys are either side's: they are equal.)
fn keyed(j: &HashJoinExec, columns: &[usize]) -> bool {
    let left = j.left().schema().fields().len();
    let index = |e: &Arc<dyn PhysicalExpr>| e.downcast_ref::<PhysicalColumn>().map(|c| c.index());
    let keys: Vec<Vec<usize>> = j
        .on()
        .iter()
        .map(|(l, r)| match j.join_type() {
            JoinType::Inner => [index(l), index(r).map(|r| left + r)].into_iter().flatten().collect(),
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => index(l).into_iter().collect(),
            JoinType::Right => index(r).map(|r| left + r).into_iter().collect(),
            JoinType::RightSemi | JoinType::RightAnti => index(r).into_iter().collect(),
            _ => vec![],
        })
        .collect();
    columns.len() == keys.len() && keys.iter().all(|k| k.iter().any(|c| columns.contains(c))) && columns.iter().all(|c| keys.iter().any(|k| k.contains(c)))
}

/// Does a join build one shared table that it may build a table a thread instead (`SharedBuilds`)?
fn hashable(j: &HashJoinExec) -> bool {
    use datafusion::physical_plan::joins::PartitionMode;
    *j.partition_mode() == PartitionMode::CollectLeft && !j.null_aware && !few(j.left()) && !having(j.left())
}

fn partitioned(j: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    j.builder().with_partition_mode(datafusion::physical_plan::joins::PartitionMode::Partitioned).build_exec()
}

/// The rows DataFusion expects of `p`.
fn rows(p: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    StatisticsContext::new().compute(p.as_ref(), &StatisticsArgs::new()).ok().and_then(|s| s.num_rows.get_value().copied())
}

/// `x IN (SELECT k … GROUP BY k HAVING …)`: the few groups a HAVING keeps are the hash table, the
/// table probes it, and the rows that pass are few too, so a join above them builds on them whole
/// rather than shuffling its other side. (Statistics can't tell how many groups a HAVING keeps:
/// DataFusion takes all of them, so it built on the table instead, 1.5 million orders for 57 keys,
/// and once a table's ranges said each order has its customer, shuffled 6 million lineitems to
/// join the 57 orders' customers.)
#[derive(Debug)]
struct HavingBuilds;

impl PhysicalOptimizerRule for HavingBuilds {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::joins::PartitionMode;
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<HashJoinExec>() else { return Ok(Transformed::no(p)) };
            if matches!(j.join_type(), JoinType::LeftSemi | JoinType::LeftAnti) && !j.null_aware && having(j.right()) && !having(j.left()) {
                return Ok(Transformed::yes(j.swap_inputs(*j.partition_mode())?));
            }
            if *j.join_type() != JoinType::Inner || *j.partition_mode() != PartitionMode::Partitioned {
                return Ok(Transformed::no(p));
            }
            if few(j.left()) {
                return Ok(Transformed::yes(j.builder().with_partition_mode(PartitionMode::CollectLeft).build_exec()?));
            }
            if few(j.right()) {
                return Ok(Transformed::yes(j.swap_inputs(PartitionMode::CollectLeft)?));
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

/// Does `p` give only the rows a HAVING's semi join let through (`having`), joined to no more than
/// one row each (DataFusion's estimate of a join no bigger than that side's)?
fn few(p: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(j) = p.downcast_ref::<HashJoinExec>() {
        return match j.join_type() {
            JoinType::LeftSemi => having(j.right()) || few(j.left()),
            JoinType::RightSemi => having(j.left()) || few(j.right()),
            JoinType::Inner => [j.left(), j.right()].into_iter().any(|side| few(side) && matches!((rows(p), rows(side)), (Some(out), Some(rows)) if out <= rows)),
            _ => false,
        };
    }
    let passes = p.downcast_ref::<FilterExec>().is_some() || p.downcast_ref::<ProjectionExec>().is_some() || p.downcast_ref::<CoalescePartitionsExec>().is_some() || p.downcast_ref::<RepartitionExec>().is_some();
    match p.children()[..] {
        [c] if passes => few(c),
        _ => false,
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
/// nation, 25 values, so every customer meets four hundred suppliers. Keys the query's imply count
/// too (`implied`): with `c_nationkey = s_nationkey` and `s_nationkey = n_nationkey`, customers can
/// meet their nation first, and q5 starts from Asia's five nations instead of a year of orders.
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
    crate::env::or::<u64>("PONDRA_JOIN_ORDER", 2).max(1)
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
        let written = keys.len();
        keys.extend(implied(&keys, plan.schema()));
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
        if crate::env::is_set("PONDRA_DEBUG_JOIN_ORDER") {
            let named: Vec<String> = leaves.iter().zip(&sizes).map(|(l, s)| format!("{} ({} rows{})", l.display(), s.rows, if s.learned { ", learned" } else { "" })).collect();
            eprintln!("join order: as written {was} rows moved, {now} in the order {order:?} of {named:?}");
        }
        // Counts are trusted more than bounds: where every input's rows are a share a run measured,
        // 1.2 times cheaper is enough (ADR-050 §3).
        let margin = if sizes.iter().all(|s| s.learned) { (margin() as f64).min(1.2) } else { margin() as f64 };
        if order == asked || now as f64 * margin >= was as f64 {
            return Ok(Transformed::no(plan));
        }
        // Rebuilt left-deep in that order: each join takes the keys that connect its input to what
        // is built, and every condition that can be evaluated by then.
        // A key the ones already taken say is left out: TPC-H q17's last join, given the key its
        // part and its subquery imply as well, hashed two columns where one says it all (no array
        // lookup, twice as slow).
        let (mut used, mut equal) = (vec![false; keys.len()], Equal::default());
        let mut left = leaves[order[0]].clone();
        for &i in &order[1..] {
            let right = Arc::new(leaves[i].clone());
            let schema = build_join_schema(left.schema(), right.schema(), &JoinType::Inner)?;
            let mut on = vec![];
            for (k, pair) in connect(&keys, left.schema(), right.schema())? {
                if !std::mem::replace(&mut used[k], true) && !equal.said(&pair.0, &pair.1, &schema) {
                    on.push(pair);
                }
            }
            let (mine, rest) = std::mem::take(&mut filters).into_iter().partition::<Vec<Expr>, _>(|f| f.column_refs().iter().all(|c| schema.has_column(c)));
            filters = rest;
            left = LogicalPlan::Join(Join::try_new(Arc::new(left), right, on, conjunction(mine), JoinType::Inner, constraint, equality, false)?);
        }
        // A key no join could take (one side spanning two inputs joined apart) stays a condition,
        // so nothing is ever dropped; the next pass pushes it back down. An implied one no join
        // took is already said by the written ones.
        filters.extend(keys.iter().zip(&used).take(written).filter(|(_, &u)| !u).map(|((l, r), _)| l.clone().eq(r.clone())));
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

/// The keys `keys` imply and don't say: two columns of one type that each equal a third are equal
/// (with NULLs too: a NULL meets nothing, or, where NULLs meet, only NULLs). Only plain columns, a
/// class of at most eight, each pair once, in the order the keys name them, so every node finds
/// the same. A join may then take one of them; the rest are left out, as the written keys hold them.
fn implied(keys: &[(Expr, Expr)], schema: &DFSchema) -> Vec<(Expr, Expr)> {
    let mut equal = Equal::default();
    for (l, r) in keys {
        equal.said(l, r, schema);
    }
    let said = |a: &Column, b: &Column| keys.iter().any(|(l, r)| matches!((l, r), (Expr::Column(x), Expr::Column(y)) if (x == a && y == b) || (x == b && y == a)));
    let mut out = vec![];
    for class in equal.0.iter().filter(|k| k.len() <= 8) {
        for (i, a) in class.iter().enumerate() {
            for b in &class[i + 1..] {
                if !said(a, b) {
                    out.push((Expr::Column(a.clone()), Expr::Column(b.clone())));
                }
            }
        }
    }
    out
}

/// Columns known equal, in classes: two plain columns of one type that a key pairs, and through
/// them every column either is equal to.
#[derive(Default)]
struct Equal(Vec<Vec<Column>>);

impl Equal {
    /// Whether the key `l = r` is said by the ones before it; if not, it is now. A key of anything
    /// but two plain columns of one type is never said.
    fn said(&mut self, l: &Expr, r: &Expr, schema: &DFSchema) -> bool {
        let (Expr::Column(a), Expr::Column(b)) = (l, r) else { return false };
        let (Ok((_, ta)), Ok((_, tb))) = (schema.qualified_field_from_column(a), schema.qualified_field_from_column(b)) else { return false };
        if ta.data_type() != tb.data_type() {
            return false;
        }
        let at = |c: &Column| self.0.iter().position(|k| k.contains(c));
        match (at(a), at(b)) {
            (Some(i), Some(j)) if i == j => return true,
            (Some(i), Some(j)) => {
                let moved = self.0.remove(i.max(j));
                self.0[i.min(j)].extend(moved);
            }
            (Some(i), None) => self.0[i].push(b.clone()),
            (None, Some(j)) => self.0[j].push(a.clone()),
            (None, None) => self.0.push(vec![a.clone(), b.clone()]),
        }
        false
    }
}

/// How big a join input is: its rows, and an upper bound on each column's distinct values where
/// the catalog knows one (by column name: a name is unique within one table, and an input holding
/// more than one table is left without bounds).
#[derive(Clone, Default)]
struct Size {
    rows: u64,
    distinct: std::collections::HashMap<String, u64>,
    spans: std::collections::HashMap<String, (f64, f64)>, // (a table's columns' least and greatest values, as numbers: for its filters)
    learned: bool, // (its rows are a share of its table a run measured: learned.rs)
}

impl Size {
    /// The same input cut to `rows` (a filter, or a join that kept some of them).
    fn cut(&self, rows: u64) -> Size {
        Size { rows, distinct: self.distinct.iter().map(|(c, &n)| (c.clone(), n.min(rows))).collect(), spans: self.spans.clone(), learned: false }
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
    Size { rows, distinct, spans: Default::default(), learned: false }
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

/// A filter's condition as the estimates count it, whether it comes from a logical plan's
/// expressions (`kept`, for the join order) or a physical plan's (`FilterShares`, for the joins'
/// sides), so both count a filter alike. `K` names a column: by name in a logical plan, by its
/// place in a physical one.
#[derive(Debug, Clone, PartialEq)]
enum Cond<K> {
    Bound(K, f64, f64),     // (the column between these values, either end open)
    Equal(K),               // (the column is one value)
    Within(K, usize),       // (the column is one of so many values)
    Any(Vec<Vec<Cond<K>>>), // (one of these conjunctions holds: an OR)
    Not(Vec<Cond<K>>),      // (this conjunction doesn't hold)
    Other,
}

/// What an estimate knows of a filter's input: its rows, and each column's distinct values and
/// least and greatest value (as numbers: `number`).
trait Facts<K> {
    fn rows(&self) -> u64;
    fn distinct(&self, k: &K) -> Option<u64>;
    fn span(&self, k: &K) -> Option<(f64, f64)>;
}

/// The share of its rows a filter's conditions keep. `column = value` keeps one of the column's
/// distinct values and `column IN (…)` as many as it lists; ranges on a column whose least and
/// greatest values are known keep the part of that span they leave, both ends of one column
/// together (a month of dates is a month of the table's years, not a quarter of them). An OR keeps
/// what any of its sides keeps, a NOT what its side leaves. Anything else keeps about a third. A
/// third for every condition made a month of TPC-DS's dates look like a tenth of them, so its joins
/// started from the sales instead.
fn share<K: Clone + Eq + std::hash::Hash>(facts: &impl Facts<K>, conds: &[Cond<K>]) -> f64 {
    const THIRD: f64 = 0.3;
    let (mut share, mut left) = (1.0, std::collections::HashMap::<K, (f64, f64)>::new());
    for c in conds {
        share *= match c {
            Cond::Bound(k, lo, hi) => match facts.span(k) {
                Some(span) => {
                    let b = left.entry(k.clone()).or_insert(span);
                    *b = (b.0.max(*lo), b.1.min(*hi));
                    continue;
                }
                None => THIRD,
            },
            Cond::Equal(k) => facts.distinct(k).map_or(THIRD, |d| 1.0 / d.max(1) as f64),
            Cond::Within(k, n) => facts.distinct(k).map_or(THIRD, |d| (*n as f64 / d.max(1) as f64).min(1.0)),
            Cond::Any(sides) => 1.0 - sides.iter().map(|s| 1.0 - self::share(facts, s)).product::<f64>(),
            Cond::Not(side) => 1.0 - self::share(facts, side),
            Cond::Other => THIRD,
        };
    }
    for (k, (lo, hi)) in left {
        let Some((min, max)) = facts.span(&k) else { continue };
        let least = 1.0 / facts.distinct(&k).unwrap_or(facts.rows()).max(1) as f64; // (one value, at the least)
        share *= if max > min { ((hi - lo) / (max - min)).clamp(least, 1.0) } else { 1.0 };
    }
    share
}

/// The bounds `column op value` leaves the column: `op` as written with the column on the left.
fn bounds(op: Operator, v: f64) -> Option<(f64, f64)> {
    let (inf, sup) = (f64::NEG_INFINITY, f64::INFINITY);
    match op {
        Operator::Gt | Operator::GtEq => Some((v, sup)),
        Operator::Lt | Operator::LtEq => Some((inf, v)),
        _ => None,
    }
}

impl Facts<String> for Size {
    fn rows(&self) -> u64 { self.rows }
    fn distinct(&self, k: &String) -> Option<u64> { self.distinct.get(k).copied() }
    fn span(&self, k: &String) -> Option<(f64, f64)> { self.spans.get(k).copied() }
}

/// The share of its rows a logical filter's conditions keep (`share`).
fn kept(s: &Size, conds: &[&Expr]) -> f64 {
    share(s, &conds.iter().map(|e| cond(e)).collect::<Vec<_>>())
}

/// A logical plan's condition as the estimates count it.
fn cond(e: &Expr) -> Cond<String> {
    fn literal(e: &Expr) -> Option<f64> {
        match e {
            Expr::Literal(v, _) => number(v),
            _ => None,
        }
    }
    fn column(e: &Expr) -> Option<String> {
        match e {
            Expr::Column(c) => Some(c.name.clone()),
            Expr::Alias(a) => column(&a.expr),
            Expr::Cast(c) => column(&c.expr),
            _ => None,
        }
    }
    let all = |e: &Expr| split_conjunction(e).into_iter().map(cond).collect::<Vec<_>>();
    match e {
        Expr::Between(b) => match (column(&b.expr), literal(&b.low), literal(&b.high)) {
            (Some(c), Some(lo), Some(hi)) if !b.negated => Cond::Bound(c, lo, hi),
            (Some(c), Some(lo), Some(hi)) => Cond::Not(vec![Cond::Bound(c, lo, hi)]),
            _ => Cond::Other,
        },
        Expr::BinaryExpr(b) if b.op == Operator::Or => Cond::Any(split_binary(e, Operator::Or).into_iter().map(all).collect()),
        Expr::BinaryExpr(b) => {
            let is_value = |e: &Expr| matches!(e, Expr::Literal(..));
            let (c, v, op) = match (column(&b.left), &*b.right, b.op.swap()) {
                (Some(c), r, _) if is_value(r) => (c, &*b.right, b.op),
                (_, _, Some(op)) if is_value(&b.left) => match column(&b.right) {
                    Some(c) => (c, &*b.left, op),
                    None => return Cond::Other,
                },
                _ => return Cond::Other,
            };
            match op {
                Operator::Eq => Cond::Equal(c),
                Operator::NotEq => Cond::Not(vec![Cond::Equal(c)]),
                op => match literal(v).and_then(|v| bounds(op, v)) {
                    Some((lo, hi)) => Cond::Bound(c, lo, hi),
                    None => Cond::Other,
                },
            }
        }
        Expr::InList(l) if l.list.iter().all(|v| matches!(v, Expr::Literal(..))) => match column(&l.expr) {
            Some(c) if l.negated => Cond::Not(vec![Cond::Within(c, l.list.len())]),
            Some(c) => Cond::Within(c, l.list.len()),
            None => Cond::Other,
        },
        Expr::Not(inner) => Cond::Not(all(inner)),
        _ => Cond::Other,
    }
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
            let whole = Size { rows: *stats.num_rows.get_value()? as u64, distinct, spans, learned: false };
            whole.cut(whole.rows.min(s.fetch.unwrap_or(usize::MAX) as u64)) // (its filters are counted by the Filter above it: a lake's tables take them inexactly)
        }
        LogicalPlan::Filter(f) => {
            let input = size(&f.input)?;
            if let LogicalPlan::TableScan(s) = f.input.as_ref() {
                if let Some(share) = crate::learned::share(&s.table_name, &f.predicate) {
                    return Some(Size { learned: true, ..input.cut((input.rows as f64 * share).ceil() as u64) }); // (a share a run measured: `learned.rs`)
                }
            }
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
        Size { rows, distinct, spans: columns.iter().map(|c| (c.0.to_string(), (c.2, c.3))).collect(), learned: false }
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

    /// An OR keeps what either side keeps, a NOT what its side leaves, and a name is a value too.
    #[test]
    fn ors_and_nots_keep_their_share() {
        let modes = table(6_000_000, &[("mode", 7, 0.0, 0.0), ("day", 2_526, 0.0, 2_525.0)]);
        let either = kept(&modes, &[&col("mode").eq(lit("MAIL")).or(col("mode").eq(lit("SHIP")))]);
        assert!((either - (1.0 - (6.0f64 / 7.0).powi(2))).abs() < 1e-12, "{either}");
        assert!((kept(&modes, &[&col("mode").not_eq(lit("AIR"))]) - 6.0 / 7.0).abs() < 1e-12);
        assert!((kept(&modes, &[&col("mode").in_list(vec![lit("A"), lit("B")], true)]) - 5.0 / 7.0).abs() < 1e-12);
        let year = kept(&modes, &[&Expr::Not(Box::new(col("day").lt(lit(365))))]);
        assert!((year - (1.0 - 365.0 / 2_525.0)).abs() < 1e-12, "{year}");
    }

    /// A physical filter is counted as its logical one is: TPC-H q12's lineitems, a year of receipt
    /// dates, two comparisons of dates and an OR of two of seven ship modes, keep under 1%.
    #[test]
    fn physical_filters_count_alike() {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::{stats::Precision, ColumnStatistics, ScalarValue, Statistics};
        use datafusion::physical_expr::expressions::{binary, col as pcol, lit as plit};
        let schema = Schema::new(vec![Field::new("receipt", DataType::Date32, false), Field::new("commit", DataType::Date32, false), Field::new("mode", DataType::Utf8, false)]);
        let mut stats = Statistics::new_unknown(&schema);
        stats.num_rows = Precision::Exact(6_000_000);
        stats.column_statistics[0] = ColumnStatistics { min_value: Precision::Inexact(ScalarValue::Date32(Some(8_037))), max_value: Precision::Inexact(ScalarValue::Date32(Some(10_562))), ..ColumnStatistics::new_unknown() };
        stats.column_statistics[2].distinct_count = Precision::Inexact(7);
        let c = |n: &str| pcol(n, &schema).unwrap();
        let date = |d: i32| plit(ScalarValue::Date32(Some(d)));
        let and = |a, b| binary(a, Operator::And, b, &schema).unwrap();
        let mode = |m: &str| binary(c("mode"), Operator::Eq, plit(m), &schema).unwrap();
        let predicate = and(and(and(binary(c("receipt"), Operator::GtEq, date(8_766), &schema).unwrap(), binary(date(9_131), Operator::Gt, c("receipt"), &schema).unwrap()),
            binary(c("receipt"), Operator::Gt, c("commit"), &schema).unwrap()), binary(mode("MAIL"), Operator::Or, mode("SHIP"), &schema).unwrap());
        let got = share(&stats, &conds(&predicate));
        let want = 365.0 / 2_525.0 * 0.3 * (1.0 - (6.0f64 / 7.0).powi(2));
        assert!((got - want).abs() < 1e-12, "{got} {want}");
        let logical = kept(&table(6_000_000, &[("receipt", 2_526, 8_037.0, 10_562.0), ("mode", 7, 0.0, 0.0)]),
            &[&col("receipt").gt_eq(lit(8_766)), &lit(9_131).gt(col("receipt")), &col("receipt").gt(col("commit")), &col("mode").eq(lit("MAIL")).or(col("mode").eq(lit("SHIP")))]);
        assert!((logical - want).abs() < 1e-12, "{logical} {want}");
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

    /// Customers meet suppliers by nation and suppliers their nation, so customers meet their
    /// nation too (TPC-H q5); a key of another type, or one already said, adds nothing.
    #[test]
    fn keys_imply_keys_through_a_third() {
        use datafusion::arrow::datatypes::Field;
        let field = |t: &str, c: &str, ty: DataType| (Some(datafusion::common::TableReference::bare(t)), Arc::new(Field::new(c, ty, true)));
        let schema = DFSchema::new_with_metadata(
            vec![field("c", "c_nationkey", DataType::Int32), field("s", "s_nationkey", DataType::Int32), field("n", "n_nationkey", DataType::Int32), field("x", "x_key", DataType::Int64)],
            Default::default(),
        )
        .unwrap();
        let c = |t: &str, n: &str| Expr::Column(Column::new(Some(t), n));
        let keys = vec![(c("c", "c_nationkey"), c("s", "s_nationkey")), (c("s", "s_nationkey"), c("n", "n_nationkey")), (c("n", "n_nationkey"), c("x", "x_key"))];
        assert_eq!(implied(&keys, &schema), vec![(c("c", "c_nationkey"), c("n", "n_nationkey"))]);
        assert!(implied(&keys[..1], &schema).is_empty());
        // Taken one at a time, a key the ones before it say is said; one of two types never is.
        let mut equal = Equal::default();
        assert!(!equal.said(&keys[0].0, &keys[0].1, &schema) && !equal.said(&keys[1].0, &keys[1].1, &schema));
        assert!(equal.said(&c("n", "n_nationkey"), &c("c", "c_nationkey"), &schema));
        assert!(!equal.said(&keys[2].0, &keys[2].1, &schema) && !equal.said(&keys[2].0, &keys[2].1, &schema));
    }
}
