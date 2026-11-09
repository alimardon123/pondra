//! `EXCEPT ALL` and `INTERSECT ALL` as SQL means them: a row that is m times on the left and n times on the right is in
//! `EXCEPT ALL`'s answer m - n times (none when n ≥ m), and in `INTERSECT ALL`'s min(m, n) times, as Postgres and DuckDB
//! answer. DataFusion plans each as a join on every column that keeps the left's rows whole, so EXCEPT ALL dropped every
//! copy of a row the right holds at all, and INTERSECT ALL kept all m (apache/datafusion#12956): a silent wrong answer.
//!
//! `SetsAll`, an analyzer rule, numbers each side's copies of a row (`row_number() OVER (PARTITION BY` every column`)`) and
//! joins on the number too. The left's k-th copy then has a match only when the right has a k-th, so EXCEPT ALL keeps the
//! copies past the right's count and INTERSECT ALL the copies up to it. Which copy gets which number doesn't matter: the
//! copies are alike in every column.
//!
//! The join it numbers is the set operation's alone. That is a semi or anti join on each column of the two sides in order,
//! with NULLs equal and nothing else in its condition. DataFusion's planner makes one for INTERSECT and EXCEPT, and nothing
//! else does before the optimizer: a `[NOT] EXISTS` or `IN` is still a subquery here, and a join written in SQL takes NULLs
//! as unequal. INTERSECT and EXCEPT without ALL make the left distinct first, and over a distinct left the join is right
//! as it is, so a distinct left is left alone.
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult};
use datafusion::common::{NullEquality, Result};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{Distinct, Expr, ExprFunctionExt, Join, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::analyzer::AnalyzerRule;

/// The numbers' columns, the left's and the right's.
const LEFT: &str = "__set_all_l";
const RIGHT: &str = "__set_all_r";

#[derive(Debug)]
pub struct SetsAll;

impl AnalyzerRule for SetsAll {
    fn name(&self) -> &str { "pondra_sets_all" }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| match plan {
            LogicalPlan::Join(j) if counted(&j) => numbered(j).map(Transformed::yes),
            plan => Ok(Transformed::no(plan)),
        })
        .data()
    }
}

/// Whether `j` is an INTERSECT ALL's or an EXCEPT ALL's join, which must count copies: see the module's words.
fn counted(j: &Join) -> bool {
    let (left, right) = (j.left.schema(), j.right.schema());
    let named = |e: &Expr, name: &str| matches!(e, Expr::Column(c) if c.name == name);
    matches!(j.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        && j.null_equality == NullEquality::NullEqualsNull
        && j.filter.is_none()
        && j.on.len() == left.fields().len()
        && j.on.len() == right.fields().len()
        && j.on.iter().zip(left.fields().iter().zip(right.fields().iter())).all(|((l, r), (lf, rf))| named(l, lf.name()) && named(r, rf.name()))
        && !matches!(j.left.as_ref(), LogicalPlan::Distinct(Distinct::All(_)))
        && !j.on.last().is_some_and(|(l, _)| named(l, LEFT)) // (numbered already: a plan analyzed again)
}

/// `j` on each side's copies numbered, and the left's columns of it, as `j` gave them.
fn numbered(j: Join) -> Result<LogicalPlan> {
    let number = |side: &LogicalPlan, name: &str| -> Result<LogicalPlan> {
        let every = side.schema().columns().into_iter().map(Expr::Column).collect();
        LogicalPlanBuilder::from(side.clone()).window(vec![row_number().partition_by(every).build()?.alias(name)])?.build()
    };
    let (left, right) = (number(&j.left, LEFT)?, number(&j.right, RIGHT)?);
    let (keys_l, keys_r) = (left.schema().columns(), right.schema().columns());
    LogicalPlanBuilder::from(left)
        .join_detailed(right, j.join_type, (keys_l, keys_r), None, NullEquality::NullEqualsNull)?
        .project(j.left.schema().columns().into_iter().map(Expr::Column))?
        .build()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;

    /// Each query's rows as text, sorted (NULL as `NULL`), as the session with Pondra's analyzer answers it.
    async fn rows(sql: &str) -> Vec<String> {
        let state = datafusion::execution::SessionStateBuilder::new().with_default_features().with_analyzer_rules(crate::optimize::analyzer_rules()).build();
        let ctx = SessionContext::new_with_state(state);
        let batches = ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")).collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut out: Vec<String> = batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).map(move |i| b.columns().iter().map(|c| if c.is_null(i) { "NULL".into() } else { array_value_to_string(c, i).unwrap() }).collect::<Vec<_>>().join(",")))
            .collect();
        out.sort();
        out
    }

    const L: &str = "(VALUES (1), (1), (1), (2), (2), (3), (NULL), (NULL)) AS l(a)";
    const R: &str = "(VALUES (1), (2), (2), (2), (4), (NULL)) AS r(a)";

    #[tokio::test]
    async fn except_all_and_intersect_all_count_the_copies() {
        // (each answer is DuckDB's and Postgres's: 1 three times on the left and once on the right, 2 twice and three
        // times, NULL twice and once)
        assert_eq!(rows(&format!("SELECT a FROM {L} EXCEPT ALL SELECT a FROM {R}")).await, ["1", "1", "3", "NULL"]);
        assert_eq!(rows(&format!("SELECT a FROM {L} INTERSECT ALL SELECT a FROM {R}")).await, ["1", "2", "2", "NULL"]);
        assert_eq!(rows(&format!("SELECT a FROM {R} EXCEPT ALL SELECT a FROM {L}")).await, ["2", "4"]);
        // (without ALL, as before: each row once)
        assert_eq!(rows(&format!("SELECT a FROM {L} EXCEPT SELECT a FROM {R}")).await, ["3"]);
        assert_eq!(rows(&format!("SELECT a FROM {L} INTERSECT SELECT a FROM {R}")).await, ["1", "2", "NULL"]);
        // (a distinct left, where the plain join was right already)
        assert_eq!(rows(&format!("SELECT DISTINCT a FROM {L} EXCEPT ALL SELECT a FROM {R}")).await, ["3"]);
        assert_eq!(rows(&format!("SELECT DISTINCT a FROM {L} INTERSECT ALL SELECT a FROM {R}")).await, ["1", "2", "NULL"]);
    }

    #[tokio::test]
    async fn every_column_is_counted_in_every_place() {
        let l = "(VALUES (1, 'x'), (1, 'x'), (1, 'y'), (2, NULL), (2, NULL)) AS l(a, b)";
        let r = "(VALUES (1, 'x'), (1, 'y'), (1, 'y'), (2, NULL)) AS r(a, b)";
        assert_eq!(rows(&format!("SELECT a, b FROM {l} EXCEPT ALL SELECT a, b FROM {r}")).await, ["1,x", "2,NULL"]);
        assert_eq!(rows(&format!("SELECT a, b FROM {l} INTERSECT ALL SELECT a, b FROM {r}")).await, ["1,x", "1,y", "2,NULL"]);
        // (sides of two types, one table on both sides, a chain, and inside a subquery, an IN and a CTE)
        assert_eq!(rows(&format!("SELECT a FROM {L} EXCEPT ALL SELECT CAST(a AS INT) FROM {R}")).await, ["1", "1", "3", "NULL"]);
        assert_eq!(rows(&format!("SELECT a FROM {L} EXCEPT ALL SELECT a FROM {L} WHERE a = 1")).await, ["2", "2", "3", "NULL", "NULL"]);
        assert_eq!(rows(&format!("(SELECT a FROM {L} EXCEPT ALL SELECT a FROM {R}) EXCEPT ALL SELECT 1")).await, ["1", "3", "NULL"]);
        assert_eq!(rows(&format!("SELECT count(*) FROM (SELECT a FROM {L} INTERSECT ALL SELECT a FROM {R})")).await, ["4"]);
        assert_eq!(rows(&format!("SELECT x FROM (VALUES (1), (3), (5)) AS v(x) WHERE x IN (SELECT a FROM {L} EXCEPT ALL SELECT a FROM {R})")).await, ["1", "3"]);
        assert_eq!(rows(&format!("WITH s AS (SELECT a FROM {L} EXCEPT ALL SELECT a FROM {R}) SELECT a, count(*) FROM s GROUP BY a")).await, ["1,2", "3,1", "NULL,1"]);
    }

    #[tokio::test]
    async fn a_join_written_in_sql_is_not_a_set_operation() {
        // (NOT EXISTS keeps every copy of a left row the right lacks, and a semi join every copy it has: neither counts)
        assert_eq!(rows(&format!("SELECT a FROM {L} WHERE NOT EXISTS (SELECT 1 FROM {R} WHERE r.a = l.a)")).await, ["3", "NULL", "NULL"]);
        assert_eq!(rows(&format!("SELECT a FROM {L} WHERE a IN (SELECT a FROM {R})")).await, ["1", "1", "1", "2", "2"]);
        assert_eq!(rows(&format!("SELECT l.a FROM {L} LEFT SEMI JOIN {R} ON l.a = r.a")).await, ["1", "1", "1", "2", "2"]);
    }
}
