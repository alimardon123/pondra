//! A grouping over a join groups the big table by its join keys first, then joins the dimension's text columns to the
//! groups (eager aggregation, Yan and Larson). TPC-DS q22 averages 26.5 M inventory rows by item, joined to the item's
//! four names under a ROLLUP: DataFusion hashed the four strings of every row, five times (6.9 of its 7.6 s of
//! grouping), where DuckDB took 0.51 s in all. Grouped by `inv_item_sk` first, the 26.5 M rows are 102,000 groups, and
//! the names join those (by hand at SF10: 3.70 → 0.42 s; the rule: 3.46 → 0.42 s). TPC-DS q11's `year_total` groups
//! store sales by a customer's seven text columns and the year: by `ss_customer_sk` and the year first, by hand, 3.32 →
//! 1.32 s; q04 4.48 → 1.99 s. The rule doesn't reach those yet: what they sum is an expression a projection works out
//! between the grouping and its joins, and only a projection of columns as they are is looked through.
//!
//! It groups first where the grouping it saves hashes several strings a row: two text columns or more, or grouping sets
//! (ROLLUP, CUBE). By one short text column the extra grouping costs what it saves (TPC-DS q05 was 8% slower).
//!
//! The rule takes an aggregate over a tree of inner joins and splits its inputs in two:
//! - the dimensions: whole tables (no filter), joined to the rest by equalities only, that give the grouping at least
//!   one text column and no aggregate an argument. A filtered table would cut the rows the join keeps, and grouping
//!   them all first would be work the join saves.
//! - the rest, which give every argument.
//!
//! The rest is grouped first by the columns the dimensions join on and the columns of its own the grouping names, all
//! of a fixed width (grouping by text there would cost what it saves); then the dimensions join those groups, and the
//! grouping is done again over them, combining what the first one computed: a sum of sums, a sum of counts (0 when
//! there are none, as a count is), a least of least values, a greatest of greatest, and an average as its sum over its
//! count (of an integer only: a DECIMAL's average rounds as DataFusion's does, which this doesn't follow). A float is
//! neither summed nor averaged in two steps: its one sum is rounded once (`fsum.rs`), a sum of sums twice.
//! This holds whatever the join keys are: a dimension's row that meets a group meets each of its rows, so each sum is
//! counted as many times as before. A grouping with no aggregate (a DISTINCT), DISTINCT, FILTER and ORDER BY inside an
//! aggregate, any other aggregate, and a condition between a dimension and the rest other than an equality, leave the
//! plan as it was.
//!
//! Its answers are the ones the query had, column for column (`Projection` with the aggregate's own schema).
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchema, ExprSchema, NullEquality, Result, ScalarValue};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{cast, lit, Aggregate, Expr, ExprSchemable, Filter, Join, JoinConstraint, JoinType, LogicalPlan, Projection};
use datafusion::optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule};
use std::sync::Arc;

/// The names of the first grouping's aggregates, and of the second's.
const FIRST: &str = "__eager_";
const SECOND: &str = "__eager_by_";

/// Groups the big table by its join keys before a dimension's text columns join it (the module's doc).
#[derive(Debug)]
pub struct Eager;

impl OptimizerRule for Eager {
    fn name(&self) -> &str {
        "eager_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(a) = &plan else { return Ok(Transformed::no(plan)) };
        match eager(a)? {
            Some(eager) => Ok(Transformed::yes(eager)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

/// An aggregate the second grouping can make of the first's: its argument.
enum Part {
    Sum(Expr),
    Count(Expr),
    Min(Expr),
    Max(Expr),
    Avg(Expr), // (of an integer)
}

impl Part {
    fn arg(&self) -> &Expr {
        match self {
            Part::Sum(e) | Part::Count(e) | Part::Min(e) | Part::Max(e) | Part::Avg(e) => e,
        }
    }
}

/// `e` as a `Part`, or None when the second grouping can't make it.
fn part(e: &Expr, schema: &DFSchema) -> Result<Option<Part>> {
    let e = match e {
        Expr::Alias(a) => a.expr.as_ref(),
        e => e,
    };
    let Expr::AggregateFunction(AggregateFunction { func, params }) = e else { return Ok(None) };
    if params.distinct || params.filter.is_some() || !params.order_by.is_empty() || params.null_treatment.is_some() {
        return Ok(None);
    }
    let [arg] = &params.args[..] else { return Ok(None) };
    let arg = arg.clone();
    // (a float's sum is rounded once, exactly (`fsum.rs`): a sum of each group's rounded sums can end a bit away from it,
    // so a float is summed, and an average taken, only as the query wrote it; an integer's sums are exact either way)
    let exact = arg.get_type(schema).is_ok_and(|t| !t.is_floating());
    let integer = arg.get_type(schema).is_ok_and(|t| t.is_integer());
    Ok(match func.name() {
        "sum" if exact => Some(Part::Sum(arg)),
        "count" => Some(Part::Count(arg)),
        "min" => Some(Part::Min(arg)),
        "max" => Some(Part::Max(arg)),
        "avg" if integer => Some(Part::Avg(arg)),
        _ => None,
    })
}

/// `a` grouped first below its dimensions' joins, or None when it can't be (the module's doc).
fn eager(a: &Aggregate) -> Result<Option<LogicalPlan>> {
    // (through a projection of its columns as they are: the join order puts one over the joins it reorders, to keep
    // their columns' order, and the joins made here give every column the aggregate reads)
    let input = match a.input.as_ref() {
        LogicalPlan::Projection(p) if p.expr.iter().all(|e| matches!(e, Expr::Column(_))) => p.input.as_ref(),
        input => input,
    };
    let LogicalPlan::Join(top) = input else { return Ok(None) };
    let equality = top.null_equality;
    if !crate::optimize::flat(top, equality) {
        return Ok(None);
    }
    let (mut leaves, mut keys, mut filters) = (vec![], vec![], vec![]);
    crate::optimize::flatten(input, equality, &mut leaves, &mut keys, &mut filters);
    let schema = input.schema();
    let Some(parts) = a.aggr_expr.iter().map(|e| part(e, schema)).collect::<Result<Option<Vec<Part>>>>()? else { return Ok(None) };
    let args: Vec<&Column> = parts.iter().flat_map(|p| p.arg().column_refs()).collect();
    // (grouped first already: the second grouping's arguments are the first's. A grouping with no aggregate, a DISTINCT,
    // is left as it is: nothing would tell its second grouping from the first, and the rule would group it again and
    // again, TPC-DS q38's planning never ending)
    if parts.is_empty() || args.iter().any(|c| c.name.starts_with(FIRST)) {
        return Ok(None);
    }
    let owner = |c: &Column| leaves.iter().position(|l| l.schema().has_column(c));
    let grouped: Vec<&Column> = a.group_expr.iter().flat_map(|e| e.column_refs()).collect();
    let wide = |c: &Column| schema.field_from_column(c).is_ok_and(|f| text(f.data_type()));
    // Grouping first adds a grouping: it pays where the one it saves hashes several strings a row, two text columns or
    // more, or a ROLLUP's or CUBE's sets, each row grouped once a set (q22: 3.46 → 0.42 s at SF10, q86 0.079 → 0.070).
    // By one short text column the two cost about the same, and grouping first was slower (q05 by a store's id, +8%).
    let sets = a.group_expr.iter().any(|e| matches!(e, Expr::GroupingSet(_)));
    if !sets && grouped.iter().filter(|c| wide(c)).count() < 2 {
        return Ok(None);
    }

    // The dimensions: whole tables giving the grouping a text column, no argument and no other condition.
    let dims: Vec<bool> = (0..leaves.len())
        .map(|i| {
            whole(&leaves[i])
                && grouped.iter().any(|c| owner(c) == Some(i) && wide(c))
                && !args.iter().any(|c| owner(c) == Some(i))
                && !filters.iter().any(|f| f.column_refs().iter().any(|c| owner(c) == Some(i)))
        })
        .collect();
    if !dims.contains(&true) || !dims.contains(&false) || args.iter().any(|c| owner(c).is_none_or(|i| dims[i])) {
        return Ok(None);
    }
    let dim = |e: &Expr| -> Option<bool> {
        let sides: Vec<bool> = e.column_refs().iter().map(|c| owner(c).map(|i| dims[i])).collect::<Option<_>>()?;
        match (sides.contains(&true), sides.contains(&false)) {
            (true, true) => None,
            (d, _) => Some(d),
        }
    };

    // What the rest is grouped by: the columns the dimensions join on, and those of its own the grouping names.
    let mut by: Vec<Column> = vec![];
    let (mut rest_keys, mut dim_keys) = (vec![], vec![]);
    for (l, r) in &keys {
        match (dim(l), dim(r)) {
            (Some(false), Some(false)) => rest_keys.push((l.clone(), r.clone())),
            (Some(true), Some(true)) => dim_keys.push((l.clone(), r.clone())),
            (Some(dl), Some(_)) => {
                by.extend(if dl { r } else { l }.column_refs().into_iter().cloned());
                dim_keys.push((l.clone(), r.clone()));
            }
            _ => return Ok(None),
        }
    }
    by.extend(grouped.iter().filter(|c| owner(c).is_some_and(|i| !dims[i])).map(|c| (*c).clone()));
    let mut seen = std::collections::HashSet::new();
    by.retain(|c| seen.insert(c.clone()));
    if by.is_empty() || by.iter().any(|c| wide(c)) {
        return Ok(None);
    }

    // The rest joined and grouped first.
    let (rest, dimensions): (Vec<_>, Vec<_>) = leaves.into_iter().zip(&dims).partition(|(_, d)| !**d);
    let Some(mut first) = joined(rest.into_iter().map(|(l, _)| l).collect(), &rest_keys, equality)? else { return Ok(None) };
    if let Some(f) = conjunction(filters) {
        first = LogicalPlan::Filter(Filter::try_new(f, Arc::new(first))?);
    }
    let (mut firsts, mut seconds, mut answers) = (vec![], vec![], vec![]);
    let total = a.schema.fields().len();
    for (i, p) in parts.iter().enumerate() {
        let want = a.schema.field(total - parts.len() + i).data_type().clone();
        let mut take = |first: Expr, second: fn(Expr) -> Expr| {
            let n = firsts.len();
            firsts.push(first.alias(format!("{FIRST}{n}")));
            seconds.push(second(Expr::Column(Column::from_name(format!("{FIRST}{n}")))).alias(format!("{SECOND}{n}")));
            Expr::Column(Column::from_name(format!("{SECOND}{n}")))
        };
        answers.push(match p {
            Part::Sum(x) => cast(take(sum(x.clone()), sum), want),
            Part::Count(x) => cast(coalesce(vec![take(count(x.clone()), sum), lit(ScalarValue::Int64(Some(0)))]), want),
            Part::Min(x) => cast(take(min(x.clone()), min), want),
            Part::Max(x) => cast(take(max(x.clone()), max), want),
            Part::Avg(x) => {
                let total = take(sum(cast(x.clone(), DataType::Float64)), sum);
                let n = take(count(x.clone()), sum);
                cast(total / cast(n, DataType::Float64), want)
            }
        });
    }
    let first = LogicalPlan::Aggregate(Aggregate::try_new(Arc::new(first), by.into_iter().map(Expr::Column).collect(), firsts)?);

    // The dimensions joined to its groups, and the grouping done again over them.
    let inputs = std::iter::once(first).chain(dimensions.into_iter().map(|(l, _)| l)).collect();
    let Some(second) = joined(inputs, &dim_keys, equality)? else { return Ok(None) };
    let second = Aggregate::try_new(Arc::new(second), a.group_expr.clone(), seconds)?;
    let groups = second.schema.fields().len() - second.aggr_expr.len();
    let mut out: Vec<Expr> = second.schema.iter().take(groups).map(|(q, f)| Expr::Column(Column::new(q.cloned(), f.name()))).collect();
    // (each answer named as the aggregate named it: a schema worked out again from the expressions, as type coercion's
    // is, keeps the names the plan above reads)
    out.extend(answers.into_iter().zip(a.schema.iter().skip(groups)).map(|(e, (q, f))| e.alias_qualified(q.cloned(), f.name())));
    let second = LogicalPlan::Aggregate(second);
    Ok(Some(LogicalPlan::Projection(Projection::try_new_with_schema(out, Arc::new(second), a.schema.clone())?)))
}

/// `inputs` joined left-deep, each by the keys that connect it to what is built (the first input it can join next),
/// and a key left over (two inputs already joined) as a condition; None when an input connects to none.
fn joined(mut inputs: Vec<LogicalPlan>, keys: &[(Expr, Expr)], equality: NullEquality) -> Result<Option<LogicalPlan>> {
    let mut used = vec![false; keys.len()];
    let mut left = inputs.remove(0);
    while !inputs.is_empty() {
        let next = inputs.iter().position(|r| keys.iter().zip(&used).any(|(k, u)| !u && connects(k, left.schema(), r.schema()).is_some()));
        let Some(next) = next else { return Ok(None) };
        let right = inputs.remove(next);
        let mut on = vec![];
        for (k, u) in keys.iter().zip(used.iter_mut()) {
            if let Some(pair) = connects(k, left.schema(), right.schema()).filter(|_| !*u) {
                on.push(pair);
                *u = true;
            }
        }
        left = LogicalPlan::Join(Join::try_new(Arc::new(left), Arc::new(right), on, None, JoinType::Inner, JoinConstraint::On, equality, false)?);
    }
    let rest = keys.iter().zip(&used).filter(|(_, u)| !**u).map(|((l, r), _)| l.clone().eq(r.clone()));
    if let Some(rest) = conjunction(rest) {
        left = LogicalPlan::Filter(Filter::try_new(rest, Arc::new(left))?);
    }
    Ok(Some(left))
}

/// `key` ordered as (a column of `left`, one of `right`), when it joins them.
fn connects((l, r): &(Expr, Expr), left: &DFSchema, right: &DFSchema) -> Option<(Expr, Expr)> {
    let all = |e: &Expr, s: &DFSchema| e.column_refs().iter().all(|c| s.has_column(c));
    if all(l, left) && all(r, right) {
        Some((l.clone(), r.clone()))
    } else if all(r, left) && all(l, right) {
        Some((r.clone(), l.clone()))
    } else {
        None
    }
}

/// Is `p` a whole table: a scan with no filter and no limit?
fn whole(p: &LogicalPlan) -> bool {
    match p {
        LogicalPlan::TableScan(s) => s.filters.is_empty() && s.fetch.is_none(),
        LogicalPlan::SubqueryAlias(s) => whole(&s.input),
        _ => false,
    }
}

/// Is a value of `t` text (or bytes): of no fixed width, costly to group by?
fn text(t: &DataType) -> bool {
    match t {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Binary | DataType::LargeBinary | DataType::BinaryView => true,
        DataType::Dictionary(_, v) => text(v),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    async fn session(eager: bool) -> Result<SessionContext> {
        let ctx = crate::engine::session(SessionConfig::new().with_target_partitions(4), Arc::new(RuntimeEnv::default()));
        let rules = crate::optimize::rules().into_iter().filter(|r| eager || r.name() != "eager_aggregate").collect();
        let ctx = SessionContext::new_with_state(SessionStateBuilder::new_from_existing(ctx.state()).with_optimizer_rules(rules).build());
        for sql in [
            "CREATE TABLE sales AS SELECT v % 1000 AS item, v % 7 AS day, CAST(v % 13 AS INT) AS qty, CAST(v AS DECIMAL(12, 2)) / 100 AS price, \
             CASE WHEN v % 11 = 0 THEN NULL ELSE v % 5 END AS maybe FROM generate_series(1, 50000) AS t(v)",
            "CREATE TABLE item AS SELECT v AS id, 'brand ' || (v % 17) AS brand, 'class ' || (v % 5) AS class FROM generate_series(0, 999) AS t(v)",
            "CREATE TABLE day AS SELECT v AS id, 2000 + v % 3 AS year FROM generate_series(0, 6) AS t(v)",
        ] {
            ctx.sql(sql).await?.collect().await?;
        }
        Ok(ctx)
    }

    /// The answer as text, and the logical plan.
    async fn answer(ctx: &SessionContext, sql: &str) -> Result<(String, String)> {
        let df = ctx.sql(sql).await?;
        let plan = df.clone().into_optimized_plan()?.display_indent().to_string();
        Ok((pretty_format_batches(&df.collect().await?)?.to_string(), plan))
    }

    /// Grouped by a dimension's text columns, the sales are grouped by their item and year first, and every aggregate
    /// answers as it did: sums of decimals and integers, counts of rows and of values, least, greatest and averages,
    /// under a ROLLUP as under a plain GROUP BY.
    #[tokio::test]
    async fn sales_are_grouped_by_their_keys_first() -> Result<()> {
        let (with, without) = (session(true).await?, session(false).await?);
        for grouping in ["brand, class, year", "ROLLUP (brand, class, year)"] {
            let sql = format!(
                "SELECT brand, class, year, sum(price) AS p, sum(qty) AS q, count(*) AS n, count(maybe) AS m, min(price) AS lo, \
                 max(qty) AS hi, avg(qty) AS a FROM sales, item, day WHERE sales.item = item.id AND sales.day = day.id \
                 GROUP BY {grouping} ORDER BY brand, class, year"
            );
            let (got, plan) = answer(&with, &sql).await?;
            assert!(plan.contains("__eager_0"), "{plan}");
            let (want, plan) = answer(&without, &sql).await?;
            assert!(!plan.contains("__eager"), "{plan}");
            assert_eq!(got, want, "{grouping}");
        }
        Ok(())
    }

    /// Over the projection of plain columns that sits between an aggregate and its joins once the join order or the
    /// projections' pruning has run (TPC-DS q22's), the rule groups first too, and the answers are as they were.
    #[tokio::test]
    async fn through_a_projection_of_columns() -> Result<()> {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        let ctx = session(false).await?;
        let sql = "SELECT brand, sum(qty) AS q, avg(qty) AS a FROM sales, item WHERE sales.item = item.id GROUP BY ROLLUP (brand) ORDER BY brand";
        let plan = ctx.sql(sql).await?.into_optimized_plan()?;
        let mut aggregate = None;
        plan.apply(|p| {
            if let LogicalPlan::Aggregate(a) = p {
                aggregate = Some(a.clone());
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let aggregate = aggregate.expect("an aggregate");
        assert!(matches!(aggregate.input.as_ref(), LogicalPlan::Projection(_)), "{}", plan.display_indent());
        let grouped = eager(&aggregate)?.expect("grouped first");
        let rewritten = plan.clone().transform_up(|p| match p {
            LogicalPlan::Aggregate(_) => Ok(Transformed::yes(grouped.clone())),
            p => Ok(Transformed::no(p)),
        })?.data;
        let got = pretty_format_batches(&ctx.execute_logical_plan(rewritten).await?.collect().await?)?.to_string();
        let want = pretty_format_batches(&ctx.execute_logical_plan(plan).await?.collect().await?)?.to_string();
        assert_eq!(got, want);
        Ok(())
    }

    /// A filtered dimension cuts the rows its join keeps: grouping them all first would be work the join saves.
    /// DISTINCT inside an aggregate can't be combined, a DISTINCT of rows has no aggregate to tell its two groupings
    /// apart, one text column costs about what grouping first does, and nothing is grouped first where nothing is text.
    #[tokio::test]
    async fn left_as_it_was() -> Result<()> {
        let ctx = session(true).await?;
        for sql in [
            "SELECT brand, sum(price) FROM sales, item WHERE sales.item = item.id AND item.class = 'class 1' GROUP BY brand",
            "SELECT brand, count(DISTINCT qty) FROM sales, item WHERE sales.item = item.id GROUP BY brand",
            "SELECT DISTINCT brand, sales.day FROM sales, item WHERE sales.item = item.id",
            "SELECT brand, sales.day, sum(qty) FROM sales, item WHERE sales.item = item.id GROUP BY brand, sales.day",
            "SELECT year, sum(price) FROM sales, day WHERE sales.day = day.id GROUP BY year",
        ] {
            let (_, plan) = answer(&ctx, sql).await?;
            assert!(!plan.contains("__eager"), "{sql}: {plan}");
        }
        Ok(())
    }

    /// Over no rows, a count is 0 and the rest NULL, as before: the grand total of a ROLLUP of nothing.
    #[tokio::test]
    async fn no_rows_count_none() -> Result<()> {
        let (with, without) = (session(true).await?, session(false).await?);
        let sql = "SELECT brand, count(*) AS n, sum(price) AS p, avg(qty) AS a FROM sales, item \
                   WHERE sales.item = item.id AND sales.qty > 100 GROUP BY ROLLUP (brand)";
        let (got, plan) = answer(&with, sql).await?;
        assert!(plan.contains("__eager_0"), "{plan}");
        assert_eq!(got, answer(&without, sql).await?.0);
        Ok(())
    }
}
