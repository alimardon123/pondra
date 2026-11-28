//! `pondra.diff_steps(a, b [, data => 'db'])` (Trellis T2): which step of two definitions moved the rows. One row for
//! each `WITH` step of either side, matched by name, and one for the query itself, `(final)`:
//! `step VARCHAR, rows_a BIGINT, rows_b BIGINT, mark VARCHAR`, b's steps in b's order, then those a has alone, then the
//! query itself.
//!
//! - `mark` says what the step's code did: `same`; `changed` (its text differs, each side read by the one parser and
//!   printed); `new` (b's alone); `removed` (a's alone); or `reads changed` (its text is the same, and it reads a step
//!   that isn't `same`).
//! - `rows_a` and `rows_b` count the step's rows on each side, NULL on a side that hasn't the step.
//! - A side is a view's name or a query. A view's definition reads its own database's tables, so each side's names are
//!   read where its database has them, as `data =>` reads another's (`redirect.rs`); `data => 'db'` reads `db`'s on
//!   both, so the counts move with the code alone. A table has no steps: `0A000`.
//!
//! The call is answered as `pondra.diff` is, on the parsed tree: it becomes a query in its place (`diff::rewrite`),
//! here of the counts, worked out as the front reads the call. Each side's steps run once, in order, from one plan of
//! the side (`counted`): a step that a later one, or the query, reads is held as it runs, in the query's memory and
//! `HOLD` bytes at most a side, and those read its rows there, until the last of them has run. A step nothing reads is
//! counted alone, as is one past the line, whose readers then run it again. Counting each step with the steps before it ran a chain of N steps about
//! N²/2 times (11-14 audit L7).
use crate::diff::{Call, Side};
use crate::front::{Host, Relation};
use anyhow::Result;
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast;
use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

/// The bytes of steps' rows a side holds for the steps that read them, at most.
const HOLD: usize = 256 << 20;

/// The step that is the query itself, after its `WITH`.
const FINAL: &str = "(final)";

/// What a step's code did between the two sides.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Mark {
    Same,
    Changed,
    New,
    Removed,
    ReadsChanged,
}

impl Mark {
    fn word(self) -> &'static str {
        match self {
            Mark::Same => "same",
            Mark::Changed => "changed",
            Mark::New => "new",
            Mark::Removed => "removed",
            Mark::ReadsChanged => "reads changed",
        }
    }
}

/// A step of either side: its name as SQL reads it, whether each side has it, and its mark.
#[derive(Clone, Debug, PartialEq)]
struct Step {
    name: String,
    in_a: bool,
    in_b: bool,
    mark: Mark,
}

/// The query that answers `pondra.diff_steps(a, b)`.
pub(crate) async fn answer_of<H: Host + ?Sized>(host: &H, call: &Call) -> Result<String> {
    let (a, ran_a) = definition(host, &call.a, call.data.as_deref()).await?;
    let (b, ran_b) = definition(host, &call.b, call.data.as_deref()).await?;
    let parse = crate::expander::parse_query;
    let steps = steps(&parse(&a)?, &parse(&b)?);
    let side = |ran: ast::Query| async move {
        let frame = host.ask(&counting(&ran)).await?;
        counted(frame, &ran, HOLD, |f| host.collect(f)).await
    };
    let (rows_a, rows_b) = (side(parse(&ran_a)?).await?, side(parse(&ran_b)?).await?);
    Ok(answer(&steps, &rows_a, &rows_b))
}

/// A side's definition as written (its steps' marks compare these), and as it runs: its names read where its database
/// has them, or from `data`'s tables.
async fn definition<H: Host + ?Sized>(host: &H, side: &Side, data: Option<&str>) -> Result<(String, String)> {
    let over = |own: String| data.map_or(own, str::to_string);
    Ok(match (side, crate::diff::relation_of(host, side).await) {
        (Side::Name(n), Some(Relation::Columns(_))) => {
            return Err(crate::codes::coded("0A000", format!("pondra.diff_steps compares two definitions' steps, and {n} is a table, which has none: name a view or a query")));
        }
        (Side::Name(n), None) => return Err(crate::codes::coded("42P01", format!("relation \"{n}\" does not exist"))),
        (Side::Name(n), Some(Relation::View { sql, lake: Some(other) })) => {
            let ran = crate::redirect::redirect(&*other, sql.clone(), over(crate::diff::database_of(n)), 0).await?;
            (sql, ran)
        }
        (Side::Name(_), Some(Relation::View { sql, lake: None })) => {
            let ran = crate::redirect::redirect(host, sql.clone(), over(host.name()), 0).await?;
            (sql, ran)
        }
        (Side::Query(q), _) => (q.clone(), crate::redirect::redirect(host, q.clone(), over(host.name()), 0).await?),
    })
}

/// The steps of two definitions, b's in b's order, then those a has alone, then the query itself, each marked. Pure.
fn steps(a: &ast::Query, b: &ast::Query) -> Vec<Step> {
    let (of_a, of_b) = (ctes(a), ctes(b));
    let find = |of: &[(String, &ast::Cte)], n: &str| of.iter().find(|(m, _)| m == n).map(|(_, c)| c.to_string());
    let mut out = vec![];
    for (n, c) in &of_b {
        let mark = match find(&of_a, n) {
            Some(text) if text == c.to_string() => Mark::Same,
            Some(_) => Mark::Changed,
            None => Mark::New,
        };
        out.push((Step { name: n.clone(), in_a: mark != Mark::New, in_b: true, mark }, crate::redirect::read_names(&c.query)));
    }
    for (n, c) in of_a.iter().filter(|(n, _)| find(&of_b, n).is_none()) {
        out.push((Step { name: n.clone(), in_a: true, in_b: false, mark: Mark::Removed }, crate::redirect::read_names(&c.query)));
    }
    let (body_a, body_b) = (body(a), body(b));
    let mark = if body_a.to_string() == body_b.to_string() { Mark::Same } else { Mark::Changed };
    out.push((Step { name: FINAL.to_string(), in_a: true, in_b: true, mark }, crate::redirect::read_names(&body_b)));
    // (a step whose text is the same reads changed when a step it reads isn't the same, until no more are)
    loop {
        let moved: Vec<String> = out.iter().filter(|(s, _)| s.mark != Mark::Same).map(|(s, _)| s.name.clone()).collect();
        let mut more = false;
        for (s, reads) in out.iter_mut().filter(|(s, _)| s.mark == Mark::Same) {
            if reads.iter().any(|r| *r != s.name && moved.contains(r)) {
                s.mark = Mark::ReadsChanged;
                more = true;
            }
        }
        if !more {
            return out.into_iter().map(|(s, _)| s).collect();
        }
    }
}

/// A query's `WITH` steps, each by its name as SQL reads it.
fn ctes(q: &ast::Query) -> Vec<(String, &ast::Cte)> {
    q.with.iter().flat_map(|w| w.cte_tables.iter()).map(|c| (crate::text::ident(&c.alias.name), c)).collect()
}

/// A query without its `WITH`: the step `(final)`.
fn body(q: &ast::Query) -> ast::Query {
    let mut q = q.clone();
    q.with = None;
    q
}

/// The query that answers the call: one row for each step, in order, with its rows on each side.
fn answer(steps: &[Step], rows_a: &HashMap<String, i64>, rows_b: &HashMap<String, i64>) -> String {
    let rows = |of: &HashMap<String, i64>, has: bool, name: &str| match of.get(name).filter(|_| has) {
        Some(n) => format!("CAST({n} AS BIGINT)"),
        None => "CAST(NULL AS BIGINT)".to_string(),
    };
    let values: Vec<String> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| format!("({i}, '{}', {}, {}, '{}')", s.name.replace('\'', "''"), rows(rows_a, s.in_a, &s.name), rows(rows_b, s.in_b, &s.name), s.mark.word()))
        .collect();
    // (the LIMIT keeps the order where the call stands, a FROM's subquery, whose ORDER BY alone may be dropped)
    format!("SELECT step, rows_a, rows_b, mark FROM (VALUES {}) AS steps(ord, step, rows_a, rows_b, mark) ORDER BY ord LIMIT {}", values.join(", "), steps.len())
}

/// The query one plan of a side comes from: its steps, then a count of each step's rows and of the query's, in order
/// (`counted` runs its parts, not it).
fn counting(q: &ast::Query) -> String {
    let with = q.with.as_ref().map(|w| format!("{w} ")).unwrap_or_default();
    let mut counts: Vec<String> = ctes(q).iter().enumerate().map(|(i, (_, c))| format!("(SELECT count(*) FROM {}) AS n{i}", c.alias.name)).collect();
    counts.push(format!("(SELECT count(*) FROM ({}) AS f) AS n{}", body(q), counts.len()));
    format!("{with}SELECT {}", counts.join(", "))
}

/// Each step's rows on a side, and the query's (`FINAL`), from `frame`, the plan of `counting(q)`: each count's step,
/// as the plan reads it, runs once, in order, held for the later ones that read it (the module's doc). `collect` runs
/// a count, as the host collects a query's rows; `hold` is the line (`HOLD`).
async fn counted<F, Fut>(frame: DataFrame, q: &ast::Query, hold: usize, collect: F) -> Result<HashMap<String, i64>>
where
    F: Fn(DataFrame) -> Fut,
    Fut: Future<Output = Result<Vec<RecordBatch>>>,
{
    let (state, plan) = frame.into_parts();
    let LogicalPlan::Projection(top) = &plan else { anyhow::bail!("pondra.diff_steps: a side's counts planned as {}", plan.display()) };
    let mut names: Vec<String> = ctes(q).into_iter().map(|(n, _)| n).collect();
    names.push(FINAL.to_string());
    let later: Vec<_> = ctes(q).iter().map(|(_, c)| crate::redirect::read_names(&c.query)).chain([crate::redirect::read_names(&body(q))]).collect();
    let memory = MemoryConsumer::new("pondra.diff_steps").register(&state.runtime_env().memory_pool);
    let readers = |i: usize| -> Vec<usize> { (i + 1..names.len()).filter(|&r| later[r].contains(&names[i])).collect() };
    let (mut held, mut was_held): (Vec<Held>, Vec<bool>) = (vec![], vec![]);
    let mut out = HashMap::new();
    anyhow::ensure!(top.expr.len() == names.len(), "pondra.diff_steps: {} counts planned for {} steps", top.expr.len(), names.len());
    for (i, (name, e)) in names.iter().zip(&top.expr).enumerate() {
        let step = match e.clone().unalias() {
            Expr::ScalarSubquery(s) => first_alias(&s.subquery).ok_or_else(|| anyhow::anyhow!("pondra.diff_steps: no step under {e}"))?,
            other => anyhow::bail!("pondra.diff_steps: a side's count planned as {other}"),
        };
        let LogicalPlan::SubqueryAlias(alias) = &step else { unreachable!("first_alias finds an alias") };
        let alias = alias.alias.clone();
        let run = swapped(step.clone(), &held)?;
        let read_by = readers(i);
        let mut rows = None;
        if !read_by.is_empty() {
            let mut stream = DataFrame::new(state.clone(), run.clone()).execute_stream().await?;
            let (schema, mut batches, mut bytes, mut whole) = (stream.schema(), vec![], 0, true);
            while let Some(b) = stream.next().await {
                let b = b?;
                let size = b.get_array_memory_size();
                if memory.size() + size > hold || memory.try_grow(size).is_err() {
                    whole = false; // (past the line: counted alone below, and run again where it is read)
                    break;
                }
                bytes += size;
                batches.push(b);
            }
            drop(stream);
            match whole {
                true => {
                    rows = Some(batches.iter().map(|b| b.num_rows() as i64).sum());
                    let scan = LogicalPlanBuilder::scan(alias.clone(), provider_as_source(Arc::new(MemTable::try_new(schema, vec![batches])?)), None)?;
                    held.push(Held { step: step.clone(), rows: scan.alias(alias)?.build()?, bytes, read_by });
                }
                false => memory.shrink(bytes),
            }
        }
        was_held.push(rows.is_some());
        let n = match rows {
            Some(n) => n,
            None => {
                let count = DataFrame::new(state.clone(), run).aggregate(vec![], vec![datafusion::functions_aggregate::count::count_all()])?;
                collect(count).await?.first().filter(|b| b.num_rows() == 1).map(|b| b.column(0).as_primitive::<Int64Type>().value(0)).unwrap_or(0)
            }
        };
        out.insert(name.clone(), n);
        // (a held step whose readers have all run, each held itself, is read no more: its memory goes back)
        held.retain(|h| match h.read_by.iter().all(|&r| r <= i && was_held[r]) {
            true => {
                memory.shrink(h.bytes);
                false
            }
            false => true,
        });
    }
    Ok(out)
}

/// A step's rows, held for the steps that read it: the step as the plan reads it, the plan of its rows, their bytes, and
/// the counts that read it.
struct Held {
    step: LogicalPlan,
    rows: LogicalPlan,
    bytes: usize,
    read_by: Vec<usize>,
}

/// The first `SubqueryAlias` from the top of a count's plan: the step it counts, as the plan reads it (DataFusion plans a
/// step once, as its name's alias, and copies it where it is read).
fn first_alias(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let mut found = None;
    let _ = plan.apply(|p| {
        if matches!(p, LogicalPlan::SubqueryAlias(_)) {
            found = Some(p.clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// `plan` reading each held step's rows where it read the step itself.
fn swapped(plan: LogicalPlan, held: &[Held]) -> Result<LogicalPlan> {
    if held.is_empty() {
        return Ok(plan);
    }
    let LogicalPlan::SubqueryAlias(top) = plan else { return Ok(plan) };
    let input = Arc::unwrap_or_clone(top.input.clone()).transform_down_with_subqueries(|p| match held.iter().find(|h| matches!(p, LogicalPlan::SubqueryAlias(_)) && h.step == p) {
        Some(h) => Ok(Transformed::new(h.rows.clone(), true, TreeNodeRecursion::Jump)),
        None => Ok(Transformed::no(p)),
    })?;
    // (the nodes above a held step's rows take their columns as the rows have them)
    let input = match input.transformed {
        true => input.data.transform_up_with_subqueries(|p| Ok(Transformed::yes(p.recompute_schema()?))).data()?,
        false => input.data,
    };
    Ok(LogicalPlanBuilder::from(input).alias(top.alias.clone())?.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;

    fn q(sql: &str) -> ast::Query { crate::expander::parse_query(sql).unwrap() }

    fn marked(a: &str, b: &str) -> Vec<(String, bool, bool, &'static str)> {
        steps(&q(a), &q(b)).into_iter().map(|s| (s.name, s.in_a, s.in_b, s.mark.word())).collect()
    }

    fn row(name: &str, in_a: bool, in_b: bool, mark: &'static str) -> (String, bool, bool, &'static str) { (name.to_string(), in_a, in_b, mark) }

    /// Steps are matched by name: b's in order, then a's alone, then the query itself; one that reads a moved step reads
    /// changed, and so does one that reads it.
    #[test]
    fn steps_are_matched_by_name_and_marked() {
        let a = "WITH big AS (SELECT * FROM t WHERE n >= 20), named AS (SELECT n, s FROM big WHERE s IS NOT NULL), gone AS (SELECT 1 AS one), kept AS (SELECT n FROM u) SELECT s, count(*) AS c FROM named GROUP BY s";
        let b = "with big as (select * from t where n >= 30), named as (SELECT n, s FROM big WHERE s IS NOT NULL), extra AS (SELECT 1 AS one), kept AS (SELECT n FROM u) SELECT s, count(*) AS c FROM named GROUP BY s";
        assert_eq!(
            marked(a, b),
            [row("big", true, true, "changed"), row("named", true, true, "reads changed"), row("extra", false, true, "new"), row("kept", true, true, "same"), row("gone", true, false, "removed"), row(FINAL, true, true, "reads changed")]
        );
        // (the one parser's print: case and space aside, the same text is the same; a step's columns are its text)
        let same = "WITH s AS (SELECT 1 AS x) SELECT x FROM s";
        assert_eq!(marked(same, "with s as (select   1 as x)\n select x from s"), [row("s", true, true, "same"), row(FINAL, true, true, "same")]);
        assert_eq!(marked("WITH s(y) AS (SELECT 1) SELECT y FROM s", "WITH s(z) AS (SELECT 1) SELECT z FROM s"), [row("s", true, true, "changed"), row(FINAL, true, true, "changed")]);
        // (a step reading one that was removed reads changed; a recursive step reading itself is the same)
        assert_eq!(
            marked("WITH g AS (SELECT 1 AS v), h AS (SELECT v FROM g) SELECT 1", "WITH h AS (SELECT v FROM g) SELECT 1"),
            [row("h", true, true, "reads changed"), row("g", true, false, "removed"), row(FINAL, true, true, "same")]
        );
        let walk = "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r";
        assert_eq!(marked(walk, walk), [row("r", true, true, "same"), row(FINAL, true, true, "same")]);
        // (no WITH on a side: the query itself alone; a quoted name is its own)
        assert_eq!(marked("SELECT 1", r#"WITH "Big" AS (SELECT 1) SELECT 2"#), [row("Big", false, true, "new"), row(FINAL, true, true, "changed")]);
    }

    /// The `tick(n)` calls a session has made so far: a step that calls it shows how often it ran.
    fn ticking() -> (SessionContext, Arc<std::sync::atomic::AtomicUsize>) {
        use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
        use datafusion::arrow::datatypes::DataType;
        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = ticks.clone();
        let tick = create_udf("tick", vec![DataType::Int64], DataType::Int64, Volatility::Volatile, Arc::new(move |args: &[ColumnarValue]| {
            let n = match &args[0] {
                ColumnarValue::Array(a) => a.len(),
                ColumnarValue::Scalar(_) => 1,
            };
            seen.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            Ok(args[0].clone())
        }));
        let ctx = SessionContext::new();
        ctx.register_udf(tick);
        (ctx, ticks)
    }

    async fn side(ctx: &SessionContext, q: &ast::Query, hold: usize) -> HashMap<String, i64> {
        let frame = ctx.sql(&counting(q)).await.unwrap_or_else(|e| panic!("{}: {e}", counting(q)));
        counted(frame, q, hold, |f| async move { Ok(f.collect().await?) }).await.unwrap()
    }

    /// The answer counts each step's rows on each side, in order, as DataFusion runs them.
    #[tokio::test]
    async fn the_answer_counts_each_steps_rows() {
        let a = q("WITH src AS (SELECT * FROM (VALUES (1), (2), (3)) AS v(n)), big AS (SELECT n FROM src WHERE n >= 2), gone AS (SELECT 1 AS one) SELECT n FROM big");
        let b = q(r#"WITH src AS (SELECT * FROM (VALUES (1), (2), (3)) AS v(n)), big AS (SELECT n FROM src WHERE n >= 3), "It's" AS (SELECT 1 AS one) SELECT n FROM big"#);
        let ctx = SessionContext::new();
        let answer = answer(&steps(&a, &b), &side(&ctx, &a, HOLD).await, &side(&ctx, &b, HOLD).await);
        // (as it is, and where the call stands: a FROM's subquery, with a filter over it, in step order either way)
        for sql in [answer.clone(), format!("SELECT * FROM ({answer}) AS d"), format!("SELECT step, mark FROM ({answer}) AS d WHERE mark <> 'same'")] {
            let batches = ctx.sql(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")).collect().await.unwrap();
            let got: Vec<String> = batches
                .iter()
                .flat_map(|b| (0..b.num_rows()).map(move |i| b.columns().iter().map(|c| if c.is_null(i) { "NULL".into() } else { array_value_to_string(c, i).unwrap() }).collect::<Vec<_>>().join(" | ")))
                .collect();
            let want: Vec<&str> = match sql.starts_with("SELECT step, mark") {
                true => vec!["big | changed", "It's | new", "gone | removed", "(final) | reads changed"],
                false => vec!["src | 3 | 3 | same", "big | 2 | 1 | changed", "It's | NULL | 1 | new", "gone | 1 | NULL | removed", "(final) | 2 | 1 | reads changed"],
            };
            assert_eq!(got, want, "{sql}");
        }
    }

    /// Each step runs once, and the later steps read its rows: a chain of four steps whose first ticks once a row ticks
    /// 5 times, where counting each step with those before it ticked 5 times a step that reads it. Past the line, every
    /// step is counted alone, with the same counts; a step read in a subquery, or under another name, reads them too.
    #[tokio::test]
    async fn each_step_runs_once() {
        let chain = q("WITH s1 AS (SELECT tick(value) AS n FROM generate_series(1, 5)), s2 AS (SELECT n FROM s1 WHERE n > 1), s3 AS (SELECT n FROM s2 AS x WHERE n > 2), s4 AS (SELECT count(*) AS c FROM s1 WHERE n IN (SELECT n FROM s3)) SELECT c FROM s4");
        let want: HashMap<String, i64> = [("s1", 5), ("s2", 4), ("s3", 3), ("s4", 1), (FINAL, 1)].into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        let (ctx, ticks) = ticking();
        assert_eq!(side(&ctx, &chain, HOLD).await, want);
        assert_eq!(ticks.load(std::sync::atomic::Ordering::Relaxed), 5, "s1 ran once");
        let (ctx, ticks) = ticking();
        assert_eq!(side(&ctx, &chain, 0).await, want, "nothing held: each counted alone");
        assert!(ticks.load(std::sync::atomic::Ordering::Relaxed) > 5 * 4, "s1 ran for each count that reads it");
        // (a recursive step, and a side with no steps)
        let walk = q("WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3), d AS (SELECT n FROM r) SELECT d.n FROM d, r AS r2");
        let (ctx, _) = ticking();
        assert_eq!(side(&ctx, &walk, HOLD).await, [("r", 3), ("d", 3), (FINAL, 9)].into_iter().map(|(k, v)| (k.to_string(), v)).collect());
        assert_eq!(side(&ctx, &q("SELECT 1 AS one"), HOLD).await, [(FINAL.to_string(), 1)].into_iter().collect());
    }
}
