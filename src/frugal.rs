//! How a query that ran out of memory is planned again. `query::frugally` runs it once more in a frugal session
//! (`query::frugal`), and the rules here change nothing unless the session is frugal (`is_on`), so a query that fits
//! plans exactly as before. A window over whole partitions (`count(*) OVER ()`, `avg(x) OVER (PARTITION BY k)`) holds
//! every row it has seen until the end (`held.rs`), which is what ran out. `WholePartition` answers such a window
//! instead as each partition's aggregates in a GROUP BY, joined back to the rows on their keys, so no row is held.
//! The answers are the same; only how much is kept at once changes.
//! A median holds every value of its group, which a spill can't help. `MiddleRows` answers a GROUP BY over medians
//! from a sort instead: each group's values numbered in order, the one or two in the middle kept.
use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::{ConfigExtension, ConfigOptions};
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{extensions_options, Column, NullEquality, Result};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::expr::{AggregateFunction, Sort, WindowFunction, WindowFunctionDefinition};
use datafusion::logical_expr::{cast, lit, Aggregate, Distinct, Expr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder, Window, WindowFrameBound};
use datafusion::optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule};

extensions_options! {
    /// The frugal session's switch, set by `query::frugal`.
    pub struct Frugal {
        /// Whether the session is frugal.
        pub on: bool, default = false
    }
}

impl ConfigExtension for Frugal {
    const PREFIX: &'static str = "pondra_frugal";
}

/// Marks a session's options as frugal.
pub fn mark(o: &mut ConfigOptions) {
    let mut frugal = Frugal::default();
    frugal.on = true;
    o.extensions.insert(frugal);
}

/// Whether a session's options are frugal's.
pub fn is_on(o: &ConfigOptions) -> bool {
    o.extensions.get::<Frugal>().is_some_and(|f| f.on)
}

/// The aggregates a window can be answered with as a GROUP BY. Each gives every row of a partition the same answer,
/// whatever order the rows come in. `first_value`, `rank` and `array_agg` depend on the order or the frame, so their
/// windows are left to DataFusion.
const WHOLE: [&str; 7] = ["count", "sum", "avg", "min", "max", "bool_and", "bool_or"];

/// A window over whole partitions, answered in a frugal session as a GROUP BY joined back to its rows. It runs first,
/// before the other rules, so the filters and projections after it see the join it makes.
#[derive(Debug)]
pub struct WholePartition;

impl OptimizerRule for WholePartition {
    fn name(&self) -> &str {
        "pondra_whole_partition"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp) // (with none, the rule sees only the root)
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        if !is_on(&config.options()) {
            return Ok(Transformed::no(plan));
        }
        Ok(match rewritten(&plan, config)? {
            Some(new) => Transformed::yes(new),
            None => Transformed::no(plan),
        })
    }
}

/// The rewrite of `plan`, when it is a window over whole partitions that can be read twice (`None` otherwise).
fn rewritten(plan: &LogicalPlan, config: &dyn OptimizerConfig) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Window(window) = plan else { return Ok(None) };
    let Some((keys, calls)) = whole(window) else { return Ok(None) };
    // The input is read twice (`build`). Known limit, not fixed here: each read is planned apart and takes the log's
    // end when it is planned, so under ingest the two can see different commits, as a self-join can today.
    if any_node(plan, differs_by_reading)? {
        return Ok(None);
    }
    let Ok(new) = build(window, &keys, calls, config) else { return Ok(None) }; // (a rewrite that can't be made leaves the query as it was: refused as before, never another error)
    let same = same_fields(&new, plan);
    Ok(same.then_some(new))
}

/// The partition keys of a window whose every expression is a whole-partition aggregate over one partition, and each
/// aggregate called over its arguments (`None` otherwise).
fn whole(window: &Window) -> Option<(Vec<Expr>, Vec<Expr>)> {
    let mut keys: Option<&Vec<Expr>> = None;
    let mut calls = vec![];
    for e in &window.window_expr {
        let w = window_function(e)?;
        if keys.is_some_and(|k| k != &w.params.partition_by) {
            return None;
        }
        keys = Some(&w.params.partition_by);
        calls.push(whole_call(w)?);
    }
    Some((keys?.clone(), calls))
}

/// The window function an expression is, under at most one alias.
fn window_function(e: &Expr) -> Option<&WindowFunction> {
    let e = if let Expr::Alias(a) = e { a.expr.as_ref() } else { e };
    match e {
        Expr::WindowFunction(w) => Some(&**w),
        _ => None,
    }
}

/// The aggregate of a window function over its whole partition, called over its arguments: `None` unless it is one of
/// `WHOLE`'s, with no order, FILTER, DISTINCT or null handling, and a frame from the partition's first row to its last.
fn whole_call(w: &WindowFunction) -> Option<Expr> {
    let WindowFunctionDefinition::AggregateUDF(udaf) = &w.fun else { return None };
    let p = &w.params;
    let whole_frame = matches!(&p.window_frame.start_bound, WindowFrameBound::Preceding(v) if v.is_null())
        && matches!(&p.window_frame.end_bound, WindowFrameBound::Following(v) if v.is_null());
    let ok = WHOLE.contains(&udaf.name()) && p.order_by.is_empty() && whole_frame && p.filter.is_none() && !p.distinct && p.null_treatment.is_none();
    ok.then(|| udaf.call(p.args.clone()))
}

/// Whether some node of the plan meets `bad`: the plan's inputs and the subqueries in its expressions included.
fn any_node(plan: &LogicalPlan, bad: impl Fn(&LogicalPlan) -> bool) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|n| {
        found |= bad(n);
        Ok(if found { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
    })?;
    Ok(found)
}

/// Whether a node gives other rows, or other values, when it is read again. A row limit or a DISTINCT ON picks its
/// rows by the order they come in; a volatile expression gives a new value each time, and a correlated one (an outer
/// reference) belongs to a query around it.
fn differs_by_reading(n: &LogicalPlan) -> bool {
    let picks = match n {
        LogicalPlan::Limit(_) | LogicalPlan::Distinct(Distinct::On(_)) => true,
        LogicalPlan::Sort(s) => s.fetch.is_some(),
        LogicalPlan::TableScan(t) => t.fetch.is_some(),
        _ => false,
    };
    picks || n.contains_outer_reference() || n.expressions().iter().any(Expr::is_volatile)
}

/// The rewrite of a window over `keys` with `calls`. The input's rows come first, each key as a column of its own
/// (`read`). Each partition's aggregates are a GROUP BY over `read`, one row per key, joined back to `read` on the
/// keys. A NULL key is a partition too. The frugal session's sort-merge join streams its left side and holds only the
/// right side's rows of one key, so `read` is on the left and the one row per key on the right. With no keys, that
/// one row is on the left of a cross join, the side a cross join holds.
fn build(window: &Window, keys: &[Expr], calls: Vec<Expr>, config: &dyn OptimizerConfig) -> Result<LogicalPlan> {
    let ids = config.alias_generator(); // (each name is new, so two rewrites in one plan never clash)
    let input = window.input.as_ref();
    let columns = input.schema().columns();
    let pk: Vec<String> = keys.iter().map(|_| ids.next("__pk")).collect();
    let gk: Vec<String> = keys.iter().map(|_| ids.next("__gk")).collect();
    let pw: Vec<String> = calls.iter().map(|_| ids.next("__pw")).collect();
    let read = if keys.is_empty() {
        input.clone()
    } else {
        let mut own: Vec<Expr> = columns.iter().cloned().map(Expr::Column).collect();
        own.extend(keys.iter().zip(&pk).map(|(k, name)| k.clone().alias(name)));
        LogicalPlanBuilder::from(input.clone()).project(own)?.build()?
    };
    // (the key is grouped under its own name: the join's columns then have no two of one name)
    let group: Vec<Expr> = pk.iter().zip(&gk).map(|(p, g)| Expr::Column(Column::from_name(p)).alias(g)).collect();
    let aggregates: Vec<Expr> = calls.into_iter().zip(&pw).map(|(c, name)| c.alias(name)).collect();
    let each = LogicalPlanBuilder::from(read.clone()).aggregate(group, aggregates)?.build()?;
    let joined = if keys.is_empty() {
        LogicalPlanBuilder::from(each).cross_join(read)?.build()?
    } else {
        let left: Vec<Column> = pk.iter().map(|p| Column::from_name(p)).collect();
        let right: Vec<Column> = gk.iter().map(|g| Column::from_name(g)).collect();
        LogicalPlanBuilder::from(read).join_detailed(each, JoinType::Inner, (left, right), None, NullEquality::NullEqualsNull)?.build()?
    };
    // The window's columns as the window had them: the input's, then each aggregate under the name the window gave it.
    let mut out: Vec<Expr> = columns.iter().cloned().map(Expr::Column).collect();
    out.extend(pw.iter().enumerate().map(|(j, name)| Expr::Column(Column::from_name(name)).alias(window.schema.field(columns.len() + j).name())));
    LogicalPlanBuilder::from(joined).project(out)?.build()
}

/// Whether two plans have the same fields, one for one, in qualifier, name and type: what a rewrite must keep for the
/// plan above it (DataFusion checks the same after each rule).
fn same_fields(a: &LogicalPlan, b: &LogicalPlan) -> bool {
    let (a, b) = (a.schema(), b.schema());
    a.fields().len() == b.fields().len()
        && a.iter().zip(b.iter()).all(|((qa, fa), (qb, fb))| qa == qb && fa.name() == fb.name() && fa.data_type() == fb.data_type())
}

/// A GROUP BY over medians, answered in a frugal session from a sort's middle rows. A median holds every value of its
/// group, and DataFusion's can't spill; a sort can. So each median's values are numbered in order (a window), the one or
/// two in the middle are kept, and their median is the group's (`median_rows`). It runs before `WholePartition`, so a
/// window a rewrite leaves for it is answered in the same pass.
#[derive(Debug)]
pub struct MiddleRows;

impl OptimizerRule for MiddleRows {
    fn name(&self) -> &str {
        "pondra_middle_rows"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp) // (with none, the rule sees only the root)
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        if !is_on(&config.options()) {
            return Ok(Transformed::no(plan));
        }
        Ok(match rewritten_medians(&plan, config)? {
            Some(new) => Transformed::yes(new),
            None => Transformed::no(plan),
        })
    }
}

/// The rewrite of `plan`, when it is a GROUP BY that takes a median and can be read again (`None` otherwise). ROLLUP,
/// CUBE and GROUPING SETS are left alone.
fn rewritten_medians(plan: &LogicalPlan, config: &dyn OptimizerConfig) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Aggregate(agg) = plan else { return Ok(None) };
    let sets = agg.group_expr.iter().any(|g| matches!(g, Expr::GroupingSet(_)));
    if sets || !agg.aggr_expr.iter().any(|e| median_of(e).is_some()) {
        return Ok(None);
    }
    // The keys and the input are read again for each median (`build_medians`), so they must give the same rows each time.
    if any_node(plan, differs_by_reading)? {
        return Ok(None);
    }
    let Ok(new) = build_medians(agg, config) else { return Ok(None) }; // (a rewrite that can't be made leaves the query as it was)
    Ok(same_fields(&new, plan).then_some(new))
}

/// The median a rewrite takes: `median(x)`, under an alias if any, over one argument, with no DISTINCT, FILTER, ORDER BY
/// or null handling. A median over the rule's own output (its `__pondra_middle` column) is not taken again.
fn median_of(e: &Expr) -> Option<&AggregateFunction> {
    let e = if let Expr::Alias(a) = e { a.expr.as_ref() } else { e };
    let Expr::AggregateFunction(f) = e else { return None };
    let p = &f.params;
    let own = matches!(p.args.first(), Some(Expr::Column(c)) if c.relation.is_none() && c.name.starts_with("__pondra_middle"));
    let ok = f.func.name() == "median" && p.args.len() == 1 && !p.distinct && p.filter.is_none() && p.order_by.is_empty() && p.null_treatment.is_none() && !own;
    ok.then_some(f)
}

/// A column of the plan by its generated name.
fn named(name: &str) -> Expr {
    Expr::Column(Column::from_name(name))
}

/// The rewrite of `agg`: a GROUP BY over the keys with every aggregate but the medians (`base`), joined with each
/// median's rows on the keys (`median_rows`), and projected back to the aggregate's fields. A NULL key is a group, and
/// a group whose values are all NULL has no median row, so its median is NULL as before. With no keys each median has
/// one row, and the rows are joined by a cross join.
fn build_medians(agg: &Aggregate, config: &dyn OptimizerConfig) -> Result<LogicalPlan> {
    let n = agg.group_expr.len();
    let picked: Vec<Option<&AggregateFunction>> = agg.aggr_expr.iter().map(median_of).collect();
    let mut rest: Vec<Expr> = agg.aggr_expr.iter().zip(&picked).filter(|(_, m)| m.is_none()).map(|(e, _)| e.clone()).collect();
    if rest.is_empty() && n == 0 {
        rest.push(count(lit(1_i64)).alias(config.alias_generator().next("__pondra_middle_count"))); // (no keys and no aggregate is no plan: a count stands in, not projected)
    }
    let base = LogicalPlanBuilder::from(agg.input.as_ref().clone()).aggregate(agg.group_expr.clone(), rest)?.build()?;
    let base_cols = base.schema().columns();
    let mut joined = base;
    let mut medians = vec![];
    for m in picked.iter().flatten() {
        let rows = median_rows(agg.input.as_ref(), &agg.group_expr, m, config)?;
        let cols = rows.schema().columns();
        let (keys, value) = (cols[..n].to_vec(), cols[n].clone());
        joined = if n == 0 {
            LogicalPlanBuilder::from(joined).cross_join(rows)?.build()?
        } else {
            let left = joined.schema().columns()[..n].to_vec(); // (the base's keys: the join keeps its left side's columns)
            LogicalPlanBuilder::from(joined).join_detailed(rows, JoinType::Left, (left, keys), None, NullEquality::NullEqualsNull)?.build()?
        };
        medians.push(value);
    }
    // The aggregate's fields in order: the keys as the base has them, each aggregate as its median or the base's column.
    let (mut rest_cols, mut median_cols) = (base_cols[n..].iter(), medians.iter());
    let mut out: Vec<Expr> = base_cols[..n].iter().cloned().map(Expr::Column).collect();
    for (i, m) in picked.iter().enumerate() {
        let c = if m.is_some() { median_cols.next() } else { rest_cols.next() };
        out.push(Expr::Column(c.expect("a column for each aggregate").clone()).alias(agg.schema.field(n + i).name()));
    }
    LogicalPlanBuilder::from(joined).project(out)?.build()
}

/// A median's rows over `input`, one for each group of `keys`: the keys (`__jk`) and the median of the values in the
/// group's middle. The values are numbered once over every row (`row_number`, in key order and then value order), not
/// once per group: a window per group holds the group's rows until it lets them go, which is what ran out. A group's
/// first number is one more than the rows of the groups before it (a running sum of the counts, `__cum`), so a value's
/// place in its group is its number less that. The places in the middle (the one for an odd count, the two for an even
/// one) are kept by `FILTER` in the median itself, not by a filter above the join: DataFusion pushes such a filter into
/// the join, which then holds a group's rows.
fn median_rows(input: &LogicalPlan, keys: &[Expr], m: &AggregateFunction, config: &dyn OptimizerConfig) -> Result<LogicalPlan> {
    let ids = config.alias_generator(); // (each name is new, so two rewrites in one plan never clash)
    let mk: Vec<String> = keys.iter().map(|_| ids.next("__mk")).collect();
    let ck: Vec<String> = keys.iter().map(|_| ids.next("__ck")).collect();
    let jk: Vec<String> = keys.iter().map(|_| ids.next("__jk")).collect();
    let mv = ids.next("__pondra_middle"); // (the mark: `median_of` leaves a median of this column alone, its own output)
    let (rn, cnt, cum) = (ids.next("__rn"), ids.next("__n"), ids.next("__cum"));
    let mut exprs: Vec<Expr> = keys.iter().zip(&mk).map(|(k, name)| k.clone().alias(name)).collect();
    exprs.push(m.params.args[0].clone().alias(&mv));
    let values = LogicalPlanBuilder::from(input.clone()).project(exprs)?.filter(named(&mv).is_not_null())?.build()?;
    // Ascending, NULLs first: the order the numbering, the running sum and the join all keep. The join's NULL keys are
    // equal (it asks for NULLs first); with NULLs last, DataFusion sorts the numbered rows again, all 12 M of them.
    let mut order: Vec<Sort> = mk.iter().map(|k| named(k).sort(true, true)).collect();
    order.push(named(&mv).sort(true, true));
    let numbered = LogicalPlanBuilder::from(values.clone()).window(vec![row_number().order_by(order).build()?.alias(&rn)])?.build()?;
    let counts = mk.iter().zip(&ck).map(|(k, c)| named(k).alias(c)).collect::<Vec<Expr>>();
    let counted = LogicalPlanBuilder::from(values).aggregate(counts, vec![count(lit(1_i64)).alias(&cnt)])?.build()?;
    let by_ck: Vec<Sort> = ck.iter().map(|c| named(c).sort(true, true)).collect();
    let running = Expr::from(WindowFunction::new(sum_udaf(), vec![named(&cnt)])).order_by(by_ck).build()?.alias(&cum);
    let running = LogicalPlanBuilder::from(counted).window(vec![running])?.build()?;
    let joined = if keys.is_empty() {
        LogicalPlanBuilder::from(numbered).cross_join(running)?.build()?
    } else {
        let left: Vec<Column> = mk.iter().map(|k| Column::from_name(k)).collect();
        let right: Vec<Column> = ck.iter().map(|c| Column::from_name(c)).collect();
        LogicalPlanBuilder::from(numbered).join_detailed(running, JoinType::Inner, (left, right), None, NullEquality::NullEqualsNull)?.build()?
    };
    // The value's place in its group (from 1), and the middle places of its group's count.
    let place = cast(named(&rn), DataType::Int64) - (named(&cum) - named(&cnt));
    let middle = place.clone().eq((named(&cnt) + lit(1_i64)) / lit(2_i64)).or(place.eq(named(&cnt) / lit(2_i64) + lit(1_i64)));
    let median = Expr::AggregateFunction(AggregateFunction::new_udf(m.func.clone(), vec![named(&mv)], false, Some(Box::new(middle)), vec![], None)).alias(ids.next("__m"));
    let group: Vec<Expr> = mk.iter().zip(&jk).map(|(k, j)| named(k).alias(j)).collect();
    LogicalPlanBuilder::from(joined).aggregate(group, vec![median])?.build()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    /// `t (k, g, x, s)`: 300 rows in six batches over three partitions. `k` is unique; `g` is NULL in every seventh
    /// row, `x` in every fifth and `s` in every eleventh.
    fn table() -> MemTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("g", DataType::Int64, true),
            Field::new("x", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = |from: i64| {
            let ks: Vec<i64> = (from..from + 50).collect();
            let g = Int64Array::from(ks.iter().map(|&k| (k % 7 != 0).then_some(k % 4)).collect::<Vec<_>>());
            let x = Float64Array::from(ks.iter().map(|&k| (k % 5 != 0).then(|| ((k * 7919) % 101) as f64 / 3.0 - 9.5)).collect::<Vec<_>>());
            let s = StringArray::from(ks.iter().map(|&k| (k % 11 != 0).then(|| format!("s{}", k % 13))).collect::<Vec<_>>());
            let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(ks)), Arc::new(g), Arc::new(x), Arc::new(s)];
            RecordBatch::try_new(schema.clone(), columns).unwrap()
        };
        let partitions: Vec<Vec<RecordBatch>> = (0..3).map(|p| (0..2).map(|b| batch(p * 100 + b * 50)).collect()).collect();
        MemTable::try_new(schema, partitions).unwrap()
    }

    /// `m (k, g, x, i, d)`: 300 rows in six batches over three partitions, for the medians. `g` is NULL in every
    /// eleventh row and otherwise `k % 7` (groups of odd and even sizes); `x` is NULL in every ninth row and in all of
    /// group 6, with ties (23 values); `i` is NULL in every seventeenth row; `d` (DECIMAL(10,2)) in every nineteenth.
    fn medians() -> MemTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("g", DataType::Int64, true),
            Field::new("x", DataType::Float64, true),
            Field::new("i", DataType::Int64, true),
            Field::new("d", DataType::Decimal128(10, 2), true),
        ]));
        let batch = |from: i64| {
            let ks: Vec<i64> = (from..from + 50).collect();
            let g = Int64Array::from(ks.iter().map(|&k| (k % 11 != 0).then_some(k % 7)).collect::<Vec<_>>());
            let x = Float64Array::from(ks.iter().map(|&k| (k % 9 != 0 && k % 7 != 6).then(|| ((k * 37) % 23) as f64 / 2.0)).collect::<Vec<_>>());
            let i = Int64Array::from(ks.iter().map(|&k| (k % 17 != 0).then_some((k * 7) % 41 - 20)).collect::<Vec<_>>());
            let d = Decimal128Array::from(ks.iter().map(|&k| (k % 19 != 0).then(|| ((k * 13) % 101 - 50) as i128 * 100 + (k % 7) as i128)).collect::<Vec<_>>())
                .with_precision_and_scale(10, 2)
                .unwrap();
            let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(ks)), Arc::new(g), Arc::new(x), Arc::new(i), Arc::new(d)];
            RecordBatch::try_new(schema.clone(), columns).unwrap()
        };
        let partitions: Vec<Vec<RecordBatch>> = (0..3).map(|p| (0..2).map(|b| batch(p * 100 + b * 50)).collect()).collect();
        MemTable::try_new(schema, partitions).unwrap()
    }

    /// Pondra's rules, and a plain session or a frugal one (`query::frugal`) over `t` and `m`.
    fn session(frugal: bool) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(crate::optimize::config(SessionConfig::new().with_target_partitions(2)))
            .with_default_features()
            .with_analyzer_rules(crate::optimize::analyzer_rules())
            .with_optimizer_rules(crate::optimize::rules())
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("t", Arc::new(table())).unwrap();
        ctx.register_table("m", Arc::new(medians())).unwrap();
        if frugal {
            crate::query::frugal(&ctx);
        }
        ctx
    }

    async fn answer(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    /// A query's physical plan, as text (EXPLAIN's every cell).
    async fn plan(ctx: &SessionContext, sql: &str) -> String {
        let mut text = String::new();
        for b in answer(ctx, &format!("EXPLAIN {sql}")).await {
            for c in b.columns() {
                for r in 0..b.num_rows() {
                    text += &array_value_to_string(c, r).unwrap();
                    text.push('\n');
                }
            }
        }
        text
    }

    /// A query's rows: each row's cells as text (a float cell left out of it) and its floats as numbers, sorted by the
    /// text, so two answers in any order line up.
    fn rows(batches: &[RecordBatch]) -> Vec<(Vec<String>, Vec<Option<f64>>)> {
        let mut out = vec![];
        for b in batches {
            for r in 0..b.num_rows() {
                let (mut text, mut nums) = (vec![], vec![]);
                for col in b.columns() {
                    if let Some(f) = col.as_any().downcast_ref::<Float64Array>() {
                        text.push(String::new());
                        nums.push((!f.is_null(r)).then(|| f.value(r)));
                    } else {
                        text.push(array_value_to_string(col, r).unwrap());
                        nums.push(None);
                    }
                }
                out.push((text, nums));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The same rows, with floats equal to 1e-9 (relative to the larger, and absolute below 1).
    fn assert_same(plain: &[RecordBatch], frugal: &[RecordBatch], sql: &str) {
        let (a, b) = (rows(plain), rows(frugal));
        assert_eq!(a.len(), b.len(), "{sql}");
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.0, y.0, "{sql}");
            for (p, q) in x.1.iter().zip(&y.1) {
                match (p, q) {
                    (None, None) => {}
                    (Some(p), Some(q)) => assert!((p - q).abs() <= 1e-9 * p.abs().max(q.abs()).max(1.0), "{sql}: {p} vs {q}"),
                    _ => panic!("{sql}: a NULL in one answer only"),
                }
            }
        }
    }

    #[tokio::test]
    async fn whole_partitions_answer_as_the_plain_session_does() {
        let (plain, frugal) = (session(false), session(true));
        for sql in [
            "SELECT k, x, count(*) OVER () AS n FROM t",
            "SELECT k, x, avg(x) OVER (PARTITION BY g) AS a, sum(x) OVER (PARTITION BY g) AS s FROM t",
            "SELECT k, x - avg(x) OVER (PARTITION BY k % 3) AS d FROM t",
            "SELECT k, count(x) OVER (PARTITION BY g, k % 2) AS n, max(s) OVER (PARTITION BY g, k % 2) AS m FROM t WHERE k > 10",
            "SELECT * FROM (SELECT g, avg(x) OVER (PARTITION BY g) AS a FROM t) WHERE g = 2",
            "SELECT k, count(*) OVER (PARTITION BY g) AS a, count(*) OVER () AS b FROM t",
        ] {
            let (want, got) = (answer(&plain, sql).await, answer(&frugal, sql).await);
            assert!(!rows(&want).is_empty(), "{sql}");
            assert_same(&want, &got, sql);
        }
    }

    #[tokio::test]
    async fn an_empty_input_answers_no_rows_either_way() {
        let sql = "SELECT k, count(*) OVER () AS n FROM t WHERE k < 0";
        for ctx in [session(false), session(true)] {
            assert!(answer(&ctx, sql).await.iter().all(|b| b.num_rows() == 0), "{sql}");
        }
    }

    /// The keyed join's input (`__pk`) is on the left and the aggregates (`Aggregate:`) on the right, in the plan the
    /// frugal session runs: a sort-merge join holds only the right side's rows of one key, so the input must not be there.
    #[tokio::test]
    async fn the_input_is_the_joins_left_side_and_the_aggregates_its_right() {
        let text = plan(&session(true), "SELECT k, avg(x) OVER (PARTITION BY g) AS a FROM t").await;
        let joined = &text[text.find("Inner Join").expect("a join in the plan")..];
        assert!(joined.find("__pk").is_some_and(|p| p < joined.find("Aggregate:").expect("the aggregates")), "{text}");
    }

    #[tokio::test]
    async fn the_rewrite_takes_whole_partition_windows_out_and_leaves_the_rest() {
        let (plain, frugal) = (session(false), session(true));
        for sql in ["SELECT k, x, count(*) OVER () AS n FROM t", "SELECT k, count(*) OVER () AS n FROM t WHERE k < 0"] {
            assert!(plan(&plain, sql).await.contains("WindowAggExec"), "{sql}");
            assert!(!plan(&frugal, sql).await.contains("WindowAggExec"), "{sql}");
        }
        for sql in [
            "SELECT k, rank() OVER (ORDER BY x) AS r FROM t",
            "SELECT k, sum(x) OVER (PARTITION BY g ORDER BY k) AS s FROM t",
            "SELECT k, sum(x) OVER (PARTITION BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM t",
            "SELECT k, first_value(x) OVER (PARTITION BY g) AS f FROM t",
            "SELECT r, sum(r) OVER () AS s FROM (SELECT random() AS r FROM t)",
            "SELECT count(*) OVER () FROM (SELECT * FROM t LIMIT 10)",
        ] {
            assert!(plan(&frugal, sql).await.contains("WindowAggExec"), "{sql}");
        }
    }

    /// Each median query answers as the plain session does: groups of odd and even sizes, ties, NULLs in the values
    /// and in the keys, a group all NULL, three types, HAVING, DISTINCT and FILTER.
    #[tokio::test]
    async fn medians_answer_as_the_plain_session_does() {
        let (plain, frugal) = (session(false), session(true));
        for sql in [
            "SELECT g, median(x) AS m FROM m GROUP BY g",
            "SELECT median(x) AS a, median(i) AS b, median(d) AS c FROM m",
            "SELECT g, k % 3 AS r, median(i) AS mi, sum(x) AS s, count(*) AS n FROM m GROUP BY g, k % 3",
            "SELECT g FROM m GROUP BY g HAVING median(x) > 5",
            "SELECT g, median(DISTINCT x) AS m FROM m GROUP BY g",
            "SELECT g, median(x) AS a, median(x) FILTER (WHERE k > 1) AS b FROM m GROUP BY g",
        ] {
            let (want, got) = (answer(&plain, sql).await, answer(&frugal, sql).await);
            assert!(!rows(&want).is_empty(), "{sql}");
            assert_same(&want, &got, sql);
        }
    }

    /// No rows: one row of NULL for the whole table either way, and no rows for the groups.
    #[tokio::test]
    async fn medians_of_no_rows_answer_as_before() {
        for sql in ["SELECT median(x) AS a FROM m WHERE k < 0", "SELECT g, median(x) AS a FROM m WHERE k < 0 GROUP BY g"] {
            let (plain, frugal) = (session(false), session(true));
            let (want, got) = (answer(&plain, sql).await, answer(&frugal, sql).await);
            assert_eq!(rows(&want).len(), rows(&got).len(), "{sql}");
            assert_same(&want, &got, sql);
        }
    }

    /// The rewrite happens in a frugal session only, and a median's rows are numbered once (`row_number`). What is
    /// read again differently, or is not a median it takes (a FILTER, ROLLUP), is left as it was.
    #[tokio::test]
    async fn medians_are_rewritten_only_when_frugal_and_only_when_they_can_be_read_again() {
        let (plain, frugal) = (session(false), session(true));
        let sql = "SELECT g, median(x) AS m FROM m GROUP BY g";
        assert!(!plan(&plain, sql).await.contains("row_number"), "{sql}");
        let text = plan(&frugal, sql).await;
        assert!(text.contains("row_number"), "{text}");
        for sql in [
            "SELECT median(x) FILTER (WHERE k > 1) AS m FROM m",
            "SELECT g, median(x) AS m FROM m GROUP BY ROLLUP (g)",
            "SELECT g, median(r) AS m FROM (SELECT g, random() AS r FROM m) GROUP BY g",
            "SELECT floor(random() * 3) AS r, median(x) AS m FROM m GROUP BY floor(random() * 3)",
        ] {
            let text = plan(&frugal, sql).await;
            assert!(!text.contains("row_number") && text.contains("median"), "{sql}\n{text}");
        }
    }
}
