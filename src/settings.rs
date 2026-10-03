//! A session's settings and prepared statements (round 31): `SET name = value`, `SET TIME ZONE`,
//! `RESET name | ALL`, `PREPARE name AS …`, `EXECUTE name(…)` and `DEALLOCATE name | ALL`, as in
//! Postgres. A session is a Postgres connection, or a client's `x-pondra-session` (`temp.rs`).
//!
//! - A `datafusion.*` name is one of DataFusion's options (`datafusion.execution.time_zone`,
//!   `datafusion.execution.batch_size`, …), checked as it is set. The session's queries use it,
//!   so they run on its node and aren't answered from the result cache. `datafusion.runtime.*` is
//!   the node's (its memory), not a session's.
//! - A name without a dot is Postgres's (`extra_float_digits`, `search_path`, `application_name`,
//!   …): kept and shown by `SHOW`, whatever it is, as clients set many on connect. `TIME ZONE` and
//!   `timezone` are DataFusion's `datafusion.execution.time_zone`.
//! - A prepared statement is kept as its text. `EXECUTE` puts its arguments where `$1`, `$2`, …
//!   were (cast to the types `PREPARE` gave) and runs it as if it were sent: a query or a write.
use crate::store::Lake;
use anyhow::{bail, ensure, Context, Result};
use datafusion::logical_expr::{LogicalPlan, SetVariable, Statement as Plan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, Expr, Reset, Set, Statement, Value, VisitMut, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use std::ops::ControlFlow;

const NO_SESSION: &str = "a setting or a prepared statement is a session's: a Postgres connection's, the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>), or a script's (send it with the statements that use it)";

/// Is this one of the statements kept here?
pub fn is(sql: &str) -> bool {
    let word = crate::write::first_word(sql).split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("").to_uppercase();
    matches!(word.as_str(), "SET" | "RESET" | "PREPARE" | "EXECUTE" | "EXEC" | "DEALLOCATE") && crate::runs::execute_of(sql).is_none() // (EXECUTE TASK: the leader's)
}

/// What a statement kept here comes to.
pub enum Done {
    Said(&'static str), // (its command tag)
    Run(String),        // an `EXECUTE`: this statement, run as if sent
}

/// Carry out a `SET`, `RESET`, `PREPARE`, `EXECUTE` or `DEALLOCATE` for the current session.
pub async fn statement(lake: &Lake, sql: &str) -> Result<Done> {
    let mut parsed = Parser::parse_sql(&GenericDialect {}, sql)?;
    ensure!(parsed.len() == 1, "one statement at a time");
    let session = crate::temp::current();
    let session = || session.clone().context(NO_SESSION);
    Ok(match parsed.remove(0) {
        Statement::Set(Set::SingleAssignment { variable, values, .. }) => {
            let value = values.iter().map(text).collect::<Result<Vec<_>>>()?.join(", ");
            set(&session()?, &variable.to_string(), value)?;
            Done::Said("SET")
        }
        Statement::Set(Set::SetTimeZone { value, .. }) => {
            set(&session()?, "timezone", text(&value)?)?;
            Done::Said("SET")
        }
        Statement::Set(_) => Done::Said("SET"), // (SET TRANSACTION, SET ROLE, SET NAMES: Postgres's, kept by no one; transactions are snapshots)
        Statement::Reset(r) => {
            let name = match r.reset {
                Reset::ALL => None,
                Reset::ConfigurationParameter(n) => Some(name(&n.to_string())),
            };
            if let Some(n) = name.as_deref().filter(|n| n.starts_with("datafusion.") && !n.starts_with("datafusion.runtime.")) {
                datafusion::config::ConfigField::reset(&mut datafusion::config::ConfigOptions::new(), n)?; // (a name DataFusion has)
            }
            crate::temp::with(&session()?, false, |s| Ok(match &name {
                Some(n) => drop(s.settings.remove(n)),
                None => s.settings.clear(),
            }))?;
            Done::Said("RESET")
        }
        Statement::Prepare { name, statement, .. } => {
            let name = name.value.to_lowercase();
            if !matches!(*statement, Statement::Insert(_) | Statement::Update(..) | Statement::Delete(_) | Statement::Merge(..)) {
                let ctx = crate::query::session(lake, &statement.to_string(), "").await?;
                ctx.execute_logical_plan(ctx.state().create_logical_plan(sql).await?).await?; // (its parameters and their types agree; its tables are there)
            }
            crate::temp::with(&session()?, false, |s| Ok(s.prepared.insert(name, sql.to_string())))?;
            Done::Said("PREPARE")
        }
        Statement::Execute { name: Some(name), parameters, .. } if !name.to_string().starts_with('\'') => {
            let name = name.to_string().to_lowercase();
            let kept = crate::temp::with(&session()?, false, |s| Ok(s.prepared.get(&name).cloned()))?;
            let kept = kept.with_context(|| format!("Prepared statement '{name}' does not exist"))?;
            let Some(Statement::Prepare { data_types, statement, .. }) = Parser::parse_sql(&GenericDialect {}, &kept)?.pop() else { bail!("{name}: not a prepared statement") };
            let mut statement = *statement;
            let mut args = Arguments { values: parameters, types: data_types, wrong: None };
            let _ = VisitMut::visit(&mut statement, &mut args);
            if let Some(e) = args.wrong {
                bail!(e);
            }
            Done::Run(statement.to_string())
        }
        Statement::Execute { .. } => bail!("EXECUTE statement requires a name"),
        Statement::Deallocate { name, .. } => {
            let name = name.value.to_lowercase();
            crate::temp::with(&session()?, false, |s| match name.as_str() {
                "all" => Ok(s.prepared.clear()),
                _ => s.prepared.remove(&name).map(drop).with_context(|| format!("Prepared statement '{name}' does not exist")),
            })?;
            Done::Said("DEALLOCATE")
        }
        _ => bail!("not a setting or a prepared statement"),
    })
}

/// A setting's name as kept: lower case, Postgres's `timezone` as DataFusion's.
fn name(n: &str) -> String {
    match n.to_lowercase().as_str() {
        "timezone" | "time.zone" | "time zone" => "datafusion.execution.time_zone".into(),
        n => n.into(),
    }
}

/// A value as `SET` takes it: a string's text, a number, a word.
fn text(e: &Expr) -> Result<String> {
    Ok(match e {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            Value::Number(n, _) => n.to_string(),
            Value::Boolean(b) => b.to_string(),
            v => bail!("SET takes a string, a number or a word, not {v}"),
        },
        Expr::Identifier(i) => i.value.clone(),
        Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => format!("-{}", text(expr)?),
        Expr::UnaryOp { op: ast::UnaryOperator::Plus, expr } => format!("+{}", text(expr)?),
        e => bail!("SET takes a string, a number or a word, not {e}"),
    })
}

fn set(session: &str, variable: &str, value: String) -> Result<()> {
    let n = name(variable);
    ensure!(!n.starts_with("datafusion.runtime."), "{n} is the node's (its memory and spill files), not a session's: PONDRA_MEMORY_MB and PONDRA_SPILL_DIR set it");
    if n.contains('.') {
        datafusion::config::ConfigOptions::new().set(&n, &value)?; // (a name DataFusion has, a value it takes)
    }
    crate::temp::with(session, false, |s| Ok(s.settings.insert(n, value)))?;
    Ok(())
}

/// Does the current session have settings of DataFusion's? (Its queries then run on its node,
/// planned with them, and aren't answered from what another query found.)
pub fn any() -> bool {
    crate::temp::current().is_some_and(|s| crate::temp::with(&s, false, |s| Ok(s.settings.keys().any(|k| k.starts_with("datafusion.")))).unwrap_or(false))
}

/// A query's session with the current session's settings of DataFusion's. In Spark's dialect
/// (`datafusion.sql_parser.dialect = 'spark'` or `'databricks'`), Spark's functions over
/// DataFusion's of the same names too (`floor` of a DOUBLE is a BIGINT, `substring` counts from
/// the end, …), and Spark's `EXTRACT` and `SUBSTRING`.
pub async fn apply(ctx: SessionContext) -> Result<SessionContext> {
    let kept = crate::temp::current().map(|s| crate::temp::with(&s, false, |s| Ok(s.settings.iter().filter(|(k, _)| k.starts_with("datafusion.")).map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>())));
    for (variable, value) in kept.transpose()?.unwrap_or_default() {
        ctx.execute_logical_plan(LogicalPlan::Statement(Plan::SetVariable(SetVariable { variable, value }))).await?; // (as DataFusion sets one: functions that read it told)
    }
    use datafusion::common::config::Dialect;
    if !matches!(ctx.state().config().options().sql_parser.dialect, Dialect::Spark | Dialect::Databricks) {
        return Ok(ctx);
    }
    // DataFusion's of a name Spark has go, aliases and all: a session rebuilt later (files read by
    // URL) registers what is left in no set order, and must find Spark's alone.
    let (scalar, aggregate) = (datafusion_spark::all_default_scalar_functions(), datafusion_spark::all_default_aggregate_functions());
    let names: std::collections::HashSet<String> = scalar.iter().map(|f| (f.name(), f.aliases())).chain(aggregate.iter().map(|f| (f.name(), f.aliases())))
        .flat_map(|(n, a)| std::iter::once(n.to_string()).chain(a.iter().cloned())).collect();
    let mut spark = datafusion::execution::SessionStateBuilder::new_from_existing(ctx.state());
    spark.expr_planners().get_or_insert_with(Vec::new).insert(0, std::sync::Arc::new(datafusion_spark::planner::SparkFunctionPlanner));
    let fns = spark.scalar_functions().get_or_insert_with(Vec::new);
    fns.retain(|f| !names.contains(f.name()));
    fns.extend(scalar);
    let fns = spark.aggregate_functions().get_or_insert_with(Vec::new);
    fns.retain(|f| !names.contains(f.name()));
    fns.extend(aggregate);
    Ok(SessionContext::new_with_state(spark.build()))
}

/// The SQL dialect the session set (or the node: `PONDRA_SQL_OPTIONS`), for a statement the
/// generic parser doesn't take (DuckDB's `STRUCT(a INT)`, …).
pub fn dialect() -> Option<Box<dyn datafusion::sql::sqlparser::dialect::Dialect>> {
    const KEY: &str = "datafusion.sql_parser.dialect";
    let session = crate::temp::current().and_then(|s| crate::temp::with(&s, false, |s| Ok(s.settings.get(KEY).cloned())).ok().flatten());
    let node = || std::env::var("PONDRA_SQL_OPTIONS").ok()?.split(',').find_map(|kv| Some(kv.trim().split_once('=').filter(|(k, _)| *k == KEY)?.1.to_string()));
    datafusion::sql::sqlparser::dialect::dialect_from_str(session.or_else(node)?)
}

/// A Postgres setting the current session set (`SHOW name`).
pub fn shown(n: &str) -> Option<String> {
    let s = crate::temp::current()?;
    crate::temp::with(&s, false, |s| Ok(s.settings.get(&name(n)).cloned())).ok().flatten()
}

/// `EXECUTE`'s arguments put where `$1`, `$2`, … are.
struct Arguments {
    values: Vec<Expr>,
    types: Vec<ast::DataType>,
    wrong: Option<String>,
}

impl VisitorMut for Arguments {
    type Break = ();
    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        let Expr::Value(v) = e else { return ControlFlow::Continue(()) };
        let Value::Placeholder(p) = &v.value else { return ControlFlow::Continue(()) };
        let Some(i) = p.strip_prefix('$').and_then(|n| n.parse::<usize>().ok()).filter(|i| *i >= 1) else {
            self.wrong = Some(format!("Unknown placeholder: {p}"));
            return ControlFlow::Break(());
        };
        let Some(value) = self.values.get(i - 1).cloned() else {
            self.wrong = Some(format!("No value found for placeholder with name {p}"));
            return ControlFlow::Break(());
        };
        *e = match self.types.get(i - 1) {
            Some(t) => Expr::Cast { kind: ast::CastKind::Cast, expr: Box::new(value), data_type: t.clone(), array: false, format: None },
            None => Expr::Nested(Box::new(value)),
        };
        ControlFlow::Continue(())
    }
}
