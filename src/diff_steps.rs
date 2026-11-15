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
//! The call is answered as `pondra.diff` is, on the parsed tree: it becomes a query of counts in its place
//! (`diff::rewrite`).
use crate::diff::{Call, Side};
use crate::front::{Host, Relation};
use anyhow::Result;
use datafusion::sql::sqlparser::ast;

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
    Ok(answer(&parse(&ran_a)?, &parse(&ran_b)?, &steps(&parse(&a)?, &parse(&b)?)))
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

/// The query of counts that answers the call: one row for each step, in order.
fn answer(ran_a: &ast::Query, ran_b: &ast::Query, steps: &[Step]) -> String {
    let rows: Vec<String> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let side = |q: &ast::Query, has: bool, alias: &str| has.then(|| format!("({}) AS {alias}", count(q, &s.name)));
            let from: Vec<String> = [side(ran_a, s.in_a, "a"), side(ran_b, s.in_b, "b")].into_iter().flatten().collect();
            let rows = |has: bool, alias: &str| if has { format!("{alias}.n") } else { "CAST(NULL AS BIGINT)".to_string() };
            format!(
                "SELECT {i} AS ord, '{}' AS step, {} AS rows_a, {} AS rows_b, '{}' AS mark FROM {}",
                s.name.replace('\'', "''"),
                rows(s.in_a, "a"),
                rows(s.in_b, "b"),
                s.mark.word(),
                from.join(", ")
            )
        })
        .collect();
    // (the LIMIT keeps the order where the call stands, a FROM's subquery, whose ORDER BY alone may be dropped)
    format!("SELECT step, rows_a, rows_b, mark FROM ({}) AS steps ORDER BY ord LIMIT {}", rows.join(" UNION ALL "), steps.len())
}

/// The query that counts one step's rows on a side: the side's steps, then a count of the one asked (of the whole
/// query, for `(final)`).
fn count(q: &ast::Query, step: &str) -> String {
    match (step, &q.with) {
        (FINAL, _) | (_, None) => format!("SELECT count(*) AS n FROM ({q}) AS f"),
        (_, Some(with)) => {
            let named = ctes(q).into_iter().find(|(n, _)| n == step).map(|(_, c)| c.alias.name.to_string()).unwrap_or_else(|| step.to_string());
            format!("{with} SELECT count(*) AS n FROM {named}")
        }
    }
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

    /// The answer counts each step's rows on each side, in order, as DataFusion runs it.
    #[tokio::test]
    async fn the_answer_counts_each_steps_rows() {
        let a = q("WITH src AS (SELECT * FROM (VALUES (1), (2), (3)) AS v(n)), big AS (SELECT n FROM src WHERE n >= 2), gone AS (SELECT 1 AS one) SELECT n FROM big");
        let b = q(r#"WITH src AS (SELECT * FROM (VALUES (1), (2), (3)) AS v(n)), big AS (SELECT n FROM src WHERE n >= 3), "It's" AS (SELECT 1 AS one) SELECT n FROM big"#);
        let answer = answer(&a, &b, &steps(&a, &b));
        let ctx = SessionContext::new();
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
}
