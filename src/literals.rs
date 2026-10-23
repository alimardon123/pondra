//! An INSERT's `VALUES` of literals, as rows, with no session and no plan.
//!
//! An INSERT's rows are a `VALUES` (`write::rows_sql`). Planned in a session they cost a session (which
//! scans every table's catalog entry), a plan and a run through DataFusion's optimizer, for what are
//! constants. So this module reads each value the way DataFusion's planner reads it (`sql_to_expr`,
//! knowing no table, function or variable), folds it to the constant it is, and builds the columns the
//! way DataFusion's `VALUES` does: a column's type is the one type its values share, and a value for a
//! DOUBLE, REAL or DECIMAL column is cast to that column's type first, as `write::values_typed` casts it.
//! The values and types are DataFusion's own, so the rows are the ones the planned path writes (the
//! unit tests at the end compare the two, case by case).
//!
//! The functions of the statement's time (`now()`, `CURRENT_TIMESTAMP`, `current_date`, `current_time`) are
//! folded as the planner folds them: to the time the statement started, which is taken once, so every
//! row of it shares one time, as a planned query's rows do.
//!
//! What is not a literal (any other function, arithmetic, a column, a placeholder, a cast that fails, two
//! types in one column, a session with DataFusion settings of its own, a query that is more than a
//! `VALUES`) is planned as before: `literal_rows` says None.

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::{ConfigOptions, Dialect};
use datafusion::common::{plan_err, DFSchema, ScalarValue, TableReference};
use datafusion::functions::datetime::{current_date, current_time, now};
use datafusion::logical_expr::planner::ContextProvider;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{AggregateUDF, Expr, HigherOrderUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion::prelude::SessionConfig;
use datafusion::sql::planner::{ParserOptions, PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast::{self, Statement};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use std::sync::Arc;

/// The rows of an INSERT's query, when it is a `VALUES` of literals only, as DataFusion would plan them
/// into `target`'s columns (`write::fit` fits them to the table). Functions of the statement's time are
/// folded as the planner folds them; any other function is planned. None: not that, so the query is planned.
pub fn literal_rows(sql: &str, target: &Schema) -> Option<RecordBatch> {
    let rows = value_rows(sql)?;
    // (a session's own DataFusion settings may read the literals differently: it plans them)
    if crate::settings::any() {
        return None;
    }
    let config = crate::optimize::config(SessionConfig::new());
    let options = config.options();
    if options.sql_parser.dialect != Dialect::Generic {
        return None;
    }
    let provider = Literals { options, functions: statement_functions(options) };
    let planner = SqlToRel::new_with_options(&provider, ParserOptions::from(&options.sql_parser));
    // (the statement's time is taken once, here: every row's now() is this one, as in a planned query)
    let ctx = SimplifyContext::builder()
        .with_config_options(Arc::clone(options))
        .with_query_execution_start_time(Some(chrono::Utc::now()))
        .build();
    let values = rows.into_iter().map(|row| row.into_iter().map(|v| plan(&planner, v, &ctx)).collect::<Option<Vec<_>>>()).collect::<Option<Vec<_>>>()?;
    columns(typed(values, target)?)
}

/// The functions of the statement's time, made from the session's config as the session makes them. The
/// session then passes its config to each function again (`SessionState`), so each one gets that step too:
/// a function that takes no config keeps itself.
fn statement_functions(options: &ConfigOptions) -> Vec<Arc<ScalarUDF>> {
    [now(options), current_date(), current_time()]
        .into_iter()
        .map(|udf| {
            let updated = udf.inner().with_updated_config(options).map(Arc::new);
            updated.unwrap_or(udf)
        })
        .collect()
}

/// The expressions of a query that is only `VALUES`: no WITH, ORDER BY, LIMIT, FETCH or the like, one
/// statement, the same number of values in every row. None for anything else.
fn value_rows(sql: &str) -> Option<Vec<Vec<ast::Expr>>> {
    let mut parsed = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if parsed.len() != 1 {
        return None;
    }
    let Statement::Query(q) = parsed.remove(0) else { return None };
    let plain = q.with.is_none()
        && q.order_by.is_none()
        && q.limit_clause.is_none()
        && q.fetch.is_none()
        && q.locks.is_empty()
        && q.for_clause.is_none()
        && q.settings.is_none()
        && q.format_clause.is_none()
        && q.pipe_operators.is_empty();
    let ast::SetExpr::Values(values) = *q.body else { return None };
    if !plain || values.value_keyword {
        return None; // (`VALUE` is DataFusion's error to give, planned)
    }
    let rows: Vec<Vec<ast::Expr>> = values.rows.into_iter().map(|r| r.content).collect();
    let width = rows.first()?.len();
    (width > 0 && rows.iter().all(|r| r.len() == width)).then_some(rows)
}

/// One value as DataFusion's planner reads it, folded to a constant. None: not a constant (or it fails to plan).
fn plan(planner: &SqlToRel<'_, Literals<'_>>, value: ast::Expr, ctx: &SimplifyContext) -> Option<ScalarValue> {
    let expr = planner.sql_to_expr(value, &DFSchema::empty(), &mut PlannerContext::new()).ok()?;
    fold(&expr, ctx)
}

/// A planned value as the constant it is: a literal, a cast of one (cast as DataFusion casts constants), or
/// a function of the statement's time with no arguments, simplified as the planner simplifies it. Anything
/// else (another function, a column, a CASE, TRY_CAST, arithmetic) is None.
fn fold(e: &Expr, ctx: &SimplifyContext) -> Option<ScalarValue> {
    match e {
        // (a number with an exponent is the whole number it stands for, as `WholeLiterals` makes it in a planned query:
        // the planner's decimal of a negative scale casts to a float a little short of 1e15 (invariant 415))
        Expr::Literal(v, _) => Some(crate::optimize::whole(v).unwrap_or_else(|| v.clone())),
        // (a function called by an alias, `CURRENT_TIMESTAMP` for `now`, is planned under the alias's name; a
        // VALUES row can't name its own, so the wrapper is only that)
        Expr::Alias(a) => fold(&a.expr, ctx),
        // (`Literals` offers the statement's three functions only, and each takes no arguments; `now(1)` is planned)
        Expr::ScalarFunction(f) if f.args.is_empty() => match f.func.simplify(vec![], ctx) {
            Ok(ExprSimplifyResult::Simplified(Expr::Literal(v, _))) => Some(v),
            _ => None,
        },
        Expr::Cast(c) => {
            let v = fold(&c.expr, ctx)?;
            // (what the optimizer's `Seconds` rule does before a text is cast to a time: `'… 10:30'` has seconds first)
            let v = match (c.field.data_type(), text(&v).and_then(crate::optimize::seconds)) {
                (DataType::Timestamp(..), Some(s)) => ScalarValue::Utf8(Some(s)),
                _ => v,
            };
            // (`casts::into` rounds a fraction going into an integer, as the planned path's `casts::Rounds` does; a cast
            // that fails is the planned path's error to report)
            let cast = crate::casts::into(&v.to_array().ok()?, c.field.data_type()).ok()?;
            ScalarValue::try_from_array(&cast, 0).ok()
        }
        _ => None,
    }
}

/// The text of a string constant, of any of DataFusion's string types.
fn text(v: &ScalarValue) -> Option<&str> {
    match v {
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => s.as_deref(),
        _ => None,
    }
}

/// Each value cast to its column's type where the column is a DOUBLE, REAL or DECIMAL: DataFusion would
/// otherwise give one column's values the one type it finds for them all, and `1e20` with `1e-20` made a
/// DECIMAL(38, 20) that couldn't hold the first (`write::values_typed`). The values fill the table's columns as
/// `write::fit` fills them: a row gives `_deleted` only by name (`write::by_position`).
fn typed(mut rows: Vec<Vec<ScalarValue>>, target: &Schema) -> Option<Vec<Vec<ScalarValue>>> {
    for row in &mut rows {
        for (v, f) in row.iter_mut().zip(crate::write::by_position(target)) {
            let t = f.data_type();
            if t.is_floating() || t.is_decimal() {
                *v = v.cast_to(t).ok()?;
            }
        }
    }
    Some(rows)
}

/// The rows as columns named `column1`, `column2`, … (as DataFusion names a `VALUES`' columns). A column's
/// type is the one type its values share, and a NULL is that type's NULL; a column of NULLs alone is NULL-typed.
/// Two types in one column: None, since DataFusion finds the type they widen to.
fn columns(rows: Vec<Vec<ScalarValue>>) -> Option<RecordBatch> {
    let width = rows.first()?.len();
    let mut fields = vec![];
    let mut arrays: Vec<ArrayRef> = vec![];
    for j in 0..width {
        let mut types = rows.iter().map(|r| r[j].data_type()).filter(|t| *t != DataType::Null);
        let t = types.next().unwrap_or(DataType::Null);
        if types.any(|u| u != t) {
            return None;
        }
        let values = rows.iter().map(|r| match &r[j] {
            ScalarValue::Null => ScalarValue::try_from(&t),
            v => Ok(v.clone()),
        }).collect::<datafusion::common::Result<Vec<_>>>().ok()?;
        fields.push(Field::new(format!("column{}", j + 1), t, true));
        arrays.push(ScalarValue::iter_to_array(values).ok()?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).ok()
}

/// What the planner may ask of the session: the statement's three functions of time, and nothing else. A
/// literal names no table, aggregate, window, variable or type, so each lookup misses, and a value that
/// needs one fails to plan (None).
struct Literals<'a> {
    options: &'a ConfigOptions,
    functions: Vec<Arc<ScalarUDF>>,
}

impl ContextProvider for Literals<'_> {
    fn get_table_source(&self, _: TableReference) -> datafusion::common::Result<Arc<dyn TableSource>> {
        plan_err!("a literal names no table")
    }
    /// A function by its name or one of its aliases (`CURRENT_TIMESTAMP` is `now`'s alias).
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.functions.iter().find(|f| f.name() == name || f.aliases().iter().any(|a| a == name)).cloned()
    }
    fn get_higher_order_meta(&self, _: &str) -> Option<Arc<HigherOrderUDF>> { None }
    fn get_aggregate_meta(&self, _: &str) -> Option<Arc<AggregateUDF>> { None }
    fn get_window_meta(&self, _: &str) -> Option<Arc<WindowUDF>> { None }
    fn get_variable_type(&self, _: &[String]) -> Option<DataType> { None }
    fn options(&self) -> &ConfigOptions { self.options }
    fn udf_names(&self) -> Vec<String> {
        self.functions.iter().flat_map(|f| std::iter::once(f.name().to_string()).chain(f.aliases().iter().cloned())).collect()
    }
    fn higher_order_function_names(&self) -> Vec<String> { vec![] }
    fn udaf_names(&self) -> Vec<String> { vec![] }
    fn udwf_names(&self) -> Vec<String> { vec![] }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::TableMeta;
    use chrono::{DateTime, Timelike, Utc};
    use crate::write::{fit, planned};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    /// A session the way Pondra makes one for a query (`Lake::made`), without the lake's own tables and functions.
    fn session() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(crate::optimize::config(SessionConfig::new()))
            .with_default_features()
            .with_analyzer_rules(crate::optimize::analyzer_rules())
            .with_optimizer_rules(crate::optimize::rules())
            .build();
        SessionContext::new_with_state(state)
    }

    /// `write::rows` for these tests' tables, which have no DEFAULTs: their rows are planned and fit, with no lake to
    /// take a default's values from.
    async fn rows(ctx: &SessionContext, meta: &TableMeta, sql: &str) -> anyhow::Result<RecordBatch> {
        let target = crate::answer::schema(&meta.columns)?;
        match planned(ctx, &target, sql).await? {
            Some(all) => fit(&target, &all),
            None => Ok(RecordBatch::new_empty(target)),
        }
    }

    fn meta(columns: &[(&str, &str)]) -> TableMeta {
        TableMeta { columns: columns.iter().map(|(n, t)| (n.to_string(), t.to_string())).collect(), ..Default::default() }
    }

    /// The INSERT's rows both ways, for a table of `columns`: planned, and literal (`literal_rows`, then `fit`).
    /// None when the literal path is not taken.
    async fn both(columns: &[(&str, &str)], sql: &str) -> Option<(anyhow::Result<RecordBatch>, anyhow::Result<RecordBatch>)> {
        let meta = meta(columns);
        let target = crate::answer::schema(&meta.columns).unwrap();
        let literal = literal_rows(sql, &target)?;
        Some((rows(&session(), &meta, sql).await, fit(&target, &literal)))
    }

    /// A literal case: the literal path takes it, and it gives what the planned path gives (both fail, or
    /// both give the same rows).
    async fn agree(columns: &[(&str, &str)], sql: &str) {
        let (planned, literal) = both(columns, sql).await.unwrap_or_else(|| panic!("not taken as a literal: {sql}"));
        match (planned, literal) {
            (Ok(p), Ok(l)) => same(&p, &l, sql),
            (Err(_), Err(_)) => {}
            (p, l) => panic!("{sql}: planned {:?}, literal {:?}", p.err(), l.err()),
        }
    }

    /// The same rows, column by column and value by value. Floats compare by their bits (NaN is NaN, -0.0 is
    /// -0.0), which an array's own `==` would not.
    fn same(a: &RecordBatch, b: &RecordBatch, sql: &str) {
        assert_eq!(a.schema().fields(), b.schema().fields(), "{sql}");
        assert_eq!(a.num_rows(), b.num_rows(), "{sql}");
        for (x, y) in a.columns().iter().zip(b.columns()) {
            assert_eq!(x.data_type(), y.data_type(), "{sql}");
            for i in 0..x.len() {
                let (p, l) = (ScalarValue::try_from_array(x.as_ref(), i).unwrap(), ScalarValue::try_from_array(y.as_ref(), i).unwrap());
                assert_eq!(p, l, "{sql}, row {i}");
            }
        }
    }

    #[tokio::test]
    async fn integers_are_as_planned() {
        let int = [("v", "Int64")];
        for sql in ["VALUES (1)", "VALUES (-1)", "VALUES ((-5))", "VALUES (9223372036854775807)", "VALUES (1.5)", "VALUES ('12')", "VALUES ('x')"] {
            agree(&int, sql).await;
        }
        // (a fraction cast to an integer rounds on both paths: a decimal half away from zero, a float half to even)
        for sql in ["VALUES (2.5)", "VALUES (-2.5)", "VALUES (CAST(2.5 AS INT))", "VALUES (CAST(-2.5 AS BIGINT))", "VALUES ((-2.5)::bigint)", "VALUES (2.5::double::int)", "VALUES (3.5::double::int)"] {
            agree(&int, sql).await;
        }
        let (_, rows) = both(&int, "VALUES (CAST(2.5 AS INT)), (CAST(-2.5 AS INT)), (2.5::double::int), (3.5::double::int)").await.expect("literal");
        let rows = rows.expect("rows");
        let got: Vec<ScalarValue> = (0..4).map(|i| ScalarValue::try_from_array(rows.column(0).as_ref(), i).unwrap()).collect();
        assert_eq!(got, [3, -3, 2, 4].map(|n| ScalarValue::Int64(Some(n))));
        agree(&[("v", "Int16")], "VALUES (70000)").await;
    }

    #[tokio::test]
    async fn floats_are_as_planned() {
        let float = [("v", "Float64")];
        for sql in ["VALUES (1.5)", "VALUES (1e20)", "VALUES (1e-20)", "VALUES (-0.0)", "VALUES ('NaN')", "VALUES (CAST('1.5' AS DOUBLE))", "VALUES (1e20), (1e-20)"] {
            agree(&float, sql).await;
        }
        agree(&[("v", "Float32")], "VALUES (3.4e39)").await;
    }

    #[tokio::test]
    async fn whole_numbers_with_an_exponent_are_as_planned() {
        // (`1e15` is the whole number on both paths: the planner's decimal of a negative scale is no value to cast)
        agree(&[("v", "Float64")], "VALUES (1e15)").await;
        agree(&[("v", "Float64")], "VALUES (CAST(1e15 AS DOUBLE))").await;
        agree(&[("v", "Decimal128(20, 0)")], "VALUES (1e15)").await;
        agree(&[("v", "Int64")], "VALUES (1e15)").await;
    }

    #[tokio::test]
    async fn decimals_are_as_planned() {
        let decimal = [("v", "Decimal128(10, 2)")];
        for sql in ["VALUES (1.5)", "VALUES (123.456)", "VALUES (1)", "VALUES ('1.25')", "VALUES (-1.25)", "VALUES (NULL), (2.5)"] {
            agree(&decimal, sql).await;
        }
    }

    #[tokio::test]
    async fn bytes_and_escapes_are_as_planned() {
        agree(&[("v", "Binary")], "VALUES (X'00ff')").await;
        agree(&[("v", "Utf8")], "VALUES (E'a\\nb')").await;
    }

    #[tokio::test]
    async fn strings_are_as_planned() {
        let text = [("v", "Utf8")];
        for sql in ["VALUES ('a')", "VALUES ('it''s')", "VALUES ('')", "VALUES ('é ü 日本')", "VALUES (NULL)", "VALUES ($$x$$)"] {
            agree(&text, sql).await;
        }
    }

    #[tokio::test]
    async fn booleans_are_as_planned() {
        let boolean = [("v", "Boolean")];
        for sql in ["VALUES (true)", "VALUES (false)", "VALUES ('t')", "VALUES (1)"] {
            agree(&boolean, sql).await;
        }
    }

    #[tokio::test]
    async fn dates_are_as_planned() {
        let date = [("v", "Date32")];
        for sql in ["VALUES ('2026-10-13')", "VALUES (DATE '2026-10-13')", "VALUES ('2026-02-30')"] {
            agree(&date, sql).await;
        }
    }

    #[tokio::test]
    async fn timestamps_are_as_planned() {
        let zoned = "Timestamp(Microsecond, Some(\"+00:00\"))";
        for column in ["Timestamp(Microsecond, None)", zoned] {
            let ts = [("v", column)];
            for sql in [
                "VALUES ('2026-10-13 10:30')",
                "VALUES ('2026-10-13 10:30:15.123456')",
                "VALUES ('2026-10-13T10:30:00Z')",
                "VALUES (TIMESTAMP '2026-10-13 10:30')",
                "VALUES (TIMESTAMPTZ '2026-10-13 10:30:00Z')",
            ] {
                agree(&ts, sql).await;
            }
        }
        agree(&[("v", zoned)], "VALUES ('2026-10-13 10:30:00+02')").await;
    }

    #[tokio::test]
    async fn intervals_are_as_planned() {
        // (DataFusion plans an interval text as a literal of its own: `INTERVAL '1 day'` is one value)
        agree(&[("v", "Interval(MonthDayNano)")], "VALUES (INTERVAL '1 day')").await;
        agree(&[("v", "Utf8")], "VALUES (INTERVAL '1 day')").await;
    }

    #[tokio::test]
    async fn times_are_as_planned() {
        let time = [("v", "Time64(Microsecond)")];
        for sql in ["VALUES ('10:30')", "VALUES ('10:30:15.123456')"] {
            agree(&time, sql).await;
        }
    }

    /// The UTC day a time falls on, as Date32 counts days (from 1970-01-01).
    fn day(t: DateTime<Utc>) -> i64 {
        t.timestamp().div_euclid(86_400)
    }

    /// A time's nanoseconds since its day's midnight (UTC), as Time64(Nanosecond) counts them.
    fn nanos_of_day(t: DateTime<Utc>) -> i64 {
        t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64
    }

    /// A statement that reads the time, both ways. The literal path takes it, and both batches have the same
    /// types. Each path takes its own time, so `agree` can't compare them: a literal time lies between one
    /// taken just before `literal_rows` and one taken just after, the rows of one statement share one time,
    /// a date is the planned one unless the statement straddled midnight, and a time is checked unless it
    /// wraps past midnight between the two.
    async fn statement_time_case(columns: &[(&str, &str)], sql: &str) {
        let meta = meta(columns);
        let target = crate::answer::schema(&meta.columns).unwrap();
        let before = Utc::now();
        let literal = literal_rows(sql, &target).unwrap_or_else(|| panic!("not taken as a literal: {sql}"));
        let after = Utc::now();
        let planned = rows(&session(), &meta, sql).await.unwrap_or_else(|e| panic!("{sql}: planned: {e}"));
        let end = Utc::now();
        let literal = fit(&target, &literal).unwrap_or_else(|e| panic!("{sql}: literal: {e}"));
        assert_eq!(planned.schema().fields(), literal.schema().fields(), "{sql}");

        let value = |b: &RecordBatch, i: usize| ScalarValue::try_from_array(b.column(0).as_ref(), i).unwrap();
        let values: Vec<ScalarValue> = (0..literal.num_rows()).map(|i| value(&literal, i)).collect();
        assert!(values.windows(2).all(|w| w[0] == w[1]), "{sql}: rows differ: {values:?}");
        match &values[0] {
            ScalarValue::TimestampMicrosecond(Some(us), None) => {
                assert!(before.timestamp_micros() <= *us && *us <= after.timestamp_micros(), "{sql}: {us}");
            }
            ScalarValue::Date32(Some(d)) => {
                let d = *d as i64;
                assert!(day(before) <= d && d <= day(end), "{sql}: {d}");
                if day(before) == day(end) {
                    assert_eq!(value(&planned, 0), values[0], "{sql}");
                }
            }
            ScalarValue::Time64Nanosecond(Some(ns)) => {
                if nanos_of_day(before) <= nanos_of_day(after) {
                    assert!(nanos_of_day(before) <= *ns && *ns <= nanos_of_day(after), "{sql}: {ns}");
                }
            }
            other => panic!("{sql}: {other:?}"),
        }
    }

    #[tokio::test]
    async fn statement_times_are_as_planned() {
        let timestamp: &[(&str, &str)] = &[("v", "Timestamp(Microsecond, None)")];
        let date: &[(&str, &str)] = &[("v", "Date32")];
        let time: &[(&str, &str)] = &[("v", "Time64(Nanosecond)")];
        for (sql, columns) in [
            ("VALUES (now())", timestamp),
            ("VALUES (CURRENT_TIMESTAMP)", timestamp),
            ("VALUES (current_timestamp())", timestamp),
            ("VALUES (current_date)", date),
            ("VALUES (current_time)", time),
            ("VALUES (now()), (now())", timestamp),
            ("VALUES (CAST(now() AS DATE))", date),
        ] {
            statement_time_case(columns, sql).await;
        }
    }

    #[tokio::test]
    async fn several_columns_and_rows_are_as_planned() {
        let table = [("n", "Int64"), ("s", "Utf8"), ("x", "Float64"), ("ts", "Timestamp(Microsecond, None)")];
        agree(&table, "VALUES (1, 'x', 1.5, '2026-10-13 10:00:00'), (2, NULL, NULL, NULL), (NULL, 'z', -2.5e3, '2026-10-13 10:00')").await;
        agree(&[("v", "Int64")], "VALUES (CAST(NULL AS BIGINT)), (7)").await;
    }

    #[tokio::test]
    async fn fewer_or_more_values_than_columns_are_as_planned() {
        agree(&[("a", "Int64"), ("b", "Utf8"), ("c", "Float64")], "VALUES (1)").await;
        agree(&[("a", "Int64")], "VALUES (1, 2)").await; // (too many: an error both ways)
    }

    #[tokio::test]
    async fn a_row_gives_deleted_only_by_name() {
        let table = [("k", "Int64"), ("v", "Utf8"), ("_deleted", "Boolean")];
        agree(&table, "VALUES (1, 'x')").await;
        agree(&table, "VALUES (1, 'x', true)").await; // (one value too many: 42601 both ways)
    }

    #[tokio::test]
    async fn what_is_not_a_literal_is_planned() {
        let int = [("v", "Int64")];
        let not_literal = [
            "VALUES (now() + INTERVAL '1 day')",
            "VALUES (now(1))",
            "VALUES (random())",
            "VALUES (1 + 1)",
            "VALUES (nextval('s'))",
            "SELECT 1",
            "VALUES (1) ORDER BY 1",
            "VALUES ([1, 2])",
            "VALUES (TRY_CAST('x' AS INT))",
            "VALUES ($1)",
            "VALUES (1), (2.5)",
            "VALUES (TIMESTAMP '2026-10-13 10:30'), ('2026-10-13 10:30')",
            "VALUES (1); VALUES (2)",
            "VALUES (INTERVAL '1 day' + INTERVAL '1 hour')",
        ];
        for sql in not_literal {
            assert!(literal_rows(sql, &crate::answer::schema(&meta(&int).columns).unwrap()).is_none(), "{sql}");
        }
    }

    #[tokio::test]
    async fn a_cast_that_fails_is_planned_and_fails_there() {
        // (99,999,999,999 has more digits than DECIMAL(10, 2) holds: the planned path reports it)
        let decimal = [("v", "Decimal128(10, 2)")];
        assert!(literal_rows("VALUES (99999999999)", &crate::answer::schema(&meta(&decimal).columns).unwrap()).is_none());
        assert!(rows(&session(), &meta(&decimal), "VALUES (99999999999)").await.is_err());
    }
}
