//! EXPLAIN's answer, the same on a node and a page: each operator's expected rows in place of DataFusion's statistics,
//! and each table of files named as SQL named it (`ext_names::readable`).
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Is `sql` an EXPLAIN (its first word, past any comments)?
pub fn asked(sql: &str) -> bool {
    crate::text::first_word(sql).get(..7).is_some_and(|w| w.eq_ignore_ascii_case("explain"))
}

/// When `sql` is an EXPLAIN, asks DataFusion for each operator's statistics, which `expected` makes readable. Postgres's
/// JSON has no place for them, so it is left as DataFusion gives it.
pub fn prepare(ctx: &SessionContext, sql: &str) {
    if asked(sql) && !sql.to_lowercase().contains("pgjson") {
        ctx.state_ref().write().config_mut().options_mut().explain.show_statistics = true;
    }
}

/// A plan's text with each operator's estimate as `expected_rows=n` before its metrics, in place of
/// DataFusion's `statistics=[…]` (its every column's range and bytes, too long to read): `EXPLAIN`'s
/// physical plans, `EXPLAIN ANALYZE`'s and the plans kept here. An operator nothing is known about
/// shows none.
pub fn expected(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let Some(at) = line.rfind(", statistics=[Rows=") else {
            out.push_str(line);
            continue;
        };
        let (head, stats) = (&line[..at], &line[at + ", statistics=[Rows=".len()..]);
        let rows = stats.split(',').next().unwrap_or("");
        let rows = rows.strip_prefix("Exact(").or_else(|| rows.strip_prefix("Inexact(")).and_then(|r| r.strip_suffix(')'));
        let (op, metrics) = head.find(", metrics=[").map_or((head, ""), |m| head.split_at(m));
        out.push_str(op);
        if let Some(rows) = rows {
            out.push_str(if op.contains(':') { ", " } else { ": " });
            out.push_str("expected_rows=");
            out.push_str(rows);
        }
        out.push_str(metrics);
        out.push_str(if line.ends_with('\n') { "\n" } else { "" });
    }
    out
}

/// An EXPLAIN's rows as they are read: the `plan` column with each operator's estimate (`expected`), and every text
/// column with each table of files named as SQL named it (`readable`).
pub fn answered(batches: Vec<RecordBatch>) -> anyhow::Result<Vec<RecordBatch>> {
    batches.into_iter().map(|b| {
        let schema = b.schema();
        let columns: Vec<ArrayRef> = b.columns().iter().zip(schema.fields()).map(|(c, f)| match c.data_type() {
            DataType::Utf8 => {
                let plan = f.name() == "plan";
                Arc::new(c.as_string::<i32>().iter().map(|t| t.map(|t| shown(t, plan))).collect::<StringArray>()) as ArrayRef
            }
            _ => c.clone(),
        }).collect();
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }).collect()
}

/// One text of an EXPLAIN as it is shown: a plan's estimates made readable first, then the files' names.
fn shown(text: &str, plan: bool) -> String {
    let text = if plan { expected(text) } else { text.to_string() };
    crate::ext_names::readable(&text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::sql::sqlparser::{ast::{SetExpr, Statement}, dialect::GenericDialect, parser::Parser};

    /// The `ext:` name SQL gives a file, as the front writes it (`ext_names::table` of its `FROM`).
    fn name_of(url: &str) -> String {
        let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, &format!("SELECT * FROM '{url}'")) else { panic!("{url}") };
        let Statement::Query(q) = stmts.remove(0) else { panic!("a query") };
        let SetExpr::Select(s) = *q.body else { panic!("a select") };
        crate::ext_names::table(&s.from[0].relation).unwrap().unwrap()
    }

    #[test]
    fn an_explain_is_asked_by_its_first_word() {
        assert!(asked("EXPLAIN SELECT 1"));
        assert!(asked("  -- a note\n explain analyze SELECT 1"));
        assert!(!asked("SELECT 'explain'"));
        assert!(!asked("EXPLAI"));
    }

    #[test]
    fn expected_rows_replace_statistics_and_keep_metrics_last() {
        let text = "DataSourceExec: partitions=1, statistics=[Rows=Exact(3), Bytes=Absent]\n\
                    AggregateExec: mode=Final, metrics=[output_rows=1], statistics=[Rows=Inexact(1), Bytes=Absent]\n\
                    CoalescePartitionsExec, statistics=[Rows=Absent, Bytes=Absent]\n\
                    ProjectionExec: expr=[a]\n";
        assert_eq!(
            expected(text),
            "DataSourceExec: partitions=1, expected_rows=3\n\
             AggregateExec: mode=Final, expected_rows=1, metrics=[output_rows=1]\n\
             CoalescePartitionsExec\n\
             ProjectionExec: expr=[a]\n"
        );
    }

    #[test]
    fn answered_makes_the_plan_and_the_file_names_readable_and_nothing_else() {
        let file = name_of("files/x.csv");
        let plan = format!("TableScan: {file} projection=[a]\nDataSourceExec: partitions=1, statistics=[Rows=Exact(2), Bytes=Absent]\n");
        let note = format!("read \"{file}\" with statistics=[Rows=Exact(2)]");
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true), Field::new("plan_type", DataType::Utf8, true), Field::new("plan", DataType::Utf8, true), Field::new("note", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["logical_plan"])),
            Arc::new(StringArray::from(vec![plan.as_str()])),
            Arc::new(StringArray::from(vec![note.as_str()])),
        ]).unwrap();
        let out = answered(vec![batch]).unwrap();
        let text = |col: usize| out[0].column(col).as_string::<i32>().value(0).to_string();
        assert_eq!(out[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 1);
        assert_eq!(text(1), "logical_plan");
        assert_eq!(text(2), "TableScan: 'files/x.csv' projection=[a]\nDataSourceExec: partitions=1, expected_rows=2\n");
        assert_eq!(text(3), "read 'files/x.csv' with statistics=[Rows=Exact(2)]", "a column other than the plan keeps its statistics");
    }
}
