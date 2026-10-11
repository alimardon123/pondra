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

pub const NO_SESSION: &str = "a setting or a prepared statement is a session's: a Postgres connection's, the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>, 8 to 128 letters, digits, - or _), or a script's (send it with the statements that use it)";

/// Is this one of the statements kept here? (`USE`, too: the session's search path, `path.rs`.)
pub fn is(sql: &str) -> bool {
    let word = crate::write::first_word(sql).split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("").to_uppercase();
    matches!(word.as_str(), "SET" | "RESET" | "PREPARE" | "EXECUTE" | "EXEC" | "DEALLOCATE" | "ANALYZE" | "ANALYSE" | "REFRESH" | "USE")
        && crate::runs::execute_of(sql).is_none() // (EXECUTE TASK: the leader's)
}

/// What Postgres's tools send that has nothing to do here (dbt and BI tools send both): `ANALYZE
/// [t]`, since statistics are kept with every write, and `REFRESH MATERIALIZED VIEW v`, since a
/// view is current as rows arrive. Refused for a name that isn't one.
async fn current(lake: &Lake, sql: &str) -> Result<Option<Done>> {
    let name = {
        let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql)?;
        let word = |p: &mut Parser, w: &str| crate::routines::word(p, w);
        if word(&mut p, "analyze") || word(&mut p, "analyse") {
            return Ok(Some(Done::Said("ANALYZE")));
        }
        if !word(&mut p, "refresh") {
            return Ok(None);
        }
        ensure!(word(&mut p, "materialized") && word(&mut p, "view"), "REFRESH MATERIALIZED VIEW name");
        let _ = word(&mut p, "concurrently");
        crate::write::object(&p.parse_object_name(false)?)
    };
    let (other, local) = crate::ddl::resolve(lake, &name).await?;
    let l = other.as_deref().unwrap_or(lake);
    let (schema, view) = crate::ddl::split(&local);
    let here = crate::ddl::lake_name(l);
    let found = crate::ddl::listed(l).await?.into_iter().any(|t| t.lake == here && t.schema == schema && t.name == view && t.kind == "materialized view");
    ensure!(found, "REFRESH MATERIALIZED VIEW {name}: there is no materialized view of that name");
    Ok(Some(Done::Said("REFRESH MATERIALIZED VIEW")))
}

/// What a statement kept here comes to.
pub enum Done {
    Said(&'static str), // (its command tag)
    Run(String),        // an `EXECUTE`: this statement, run as if sent
}

/// Carry out a `SET`, `RESET`, `PREPARE`, `EXECUTE` or `DEALLOCATE` for the current session.
pub async fn statement(lake: &Lake, sql: &str) -> Result<Done> {
    if let Some(done) = current(lake, sql).await? {
        return Ok(done);
    }
    let session = crate::temp::current();
    let session = || session.clone().context(NO_SESSION);
    // `USE` and `SET SCHEMA` set the session's search path (`path.rs`). sqlparser reads neither, so
    // they are taken here. The session is looked for first: a USE with none says so, by name.
    if crate::path::is_use(sql) {
        let session = session()?;
        set(&session, "search_path", crate::path::use_of(lake, sql).await?)?;
        return Ok(Done::Said("USE"));
    }
    if let Some(schema) = crate::path::set_schema(sql) {
        set(&session()?, "search_path", schema)?;
        return Ok(Done::Said("SET"));
    }
    let mut parsed = Parser::parse_sql(&GenericDialect {}, sql)?;
    ensure!(parsed.len() == 1, "one statement at a time");
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
        Statement::Set(Set::MultipleAssignments { assignments }) => {
            let session = session()?;
            for a in assignments {
                set(&session, &a.name.to_string(), text(&a.value)?)?;
            }
            Done::Said("SET")
        }
        // What other statements called SET ask for is done as asked, or refused by name: a SET
        // answered and not done would be a quiet lie (principle 10).
        Statement::Set(Set::SetTransaction { modes, snapshot, .. }) => {
            if snapshot.is_some() {
                return Err(crate::codes::coded("0A000", "SET TRANSACTION SNAPSHOT: a transaction reads the commit its BEGIN saw, never another's"));
            }
            for m in &modes {
                match m {
                    ast::TransactionMode::IsolationLevel(ast::TransactionIsolationLevel::Serializable) => return Err(crate::codes::coded("0A000", SERIALIZABLE)),
                    ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadOnly) => return Err(crate::codes::coded("0A000", READ_ONLY)),
                    _ => {} // (READ COMMITTED, READ UNCOMMITTED, REPEATABLE READ, SNAPSHOT, READ WRITE: a snapshot is each of them or stronger, as the standard allows)
                }
            }
            Done::Said("SET")
        }
        Statement::Set(Set::SetRole { role_name: None, .. }) => Done::Said("SET"), // (SET ROLE NONE: the sign-in's own rights, which a session always has)
        Statement::Set(Set::SetRole { role_name: Some(role), .. }) => {
            return Err(crate::codes::coded("0A000", format!("SET ROLE {role}: a session has its sign-in's rights, and no other: sign in as a user who has them, or GRANT {role} TO the user you sign in as")))
        }
        Statement::Set(Set::SetSessionAuthorization(_)) => {
            return Err(crate::codes::coded("0A000", "SET SESSION AUTHORIZATION: a session is its sign-in's: sign in as that user (with its token or password)"))
        }
        Statement::Set(Set::SetNames { charset_name, .. }) => {
            set(&session()?, "client_encoding", charset_name.value)?; // (Postgres's SET NAMES is client_encoding)
            Done::Said("SET")
        }
        Statement::Set(Set::SetNamesDefault {}) => Done::Said("SET"),
        Statement::Set(Set::ParenthesizedAssignments { variables, values }) => {
            let each = variables.iter().zip(values).map(|(n, v)| format!("${n} = {v}")).collect::<Vec<_>>().join("; ");
            let first = variables.first().map(|n| format!("${n}")).unwrap_or_default();
            return Err(crate::codes::coded("0A000", format!("SET (…) = (…) is Snowflake's for variables: here each is set on its own, {each} (or DECLARE {first} INT = …), and {first} reads it")));
        }
        Statement::Set(other) => return Err(crate::codes::coded("0A000", format!("{other}: not a setting here (SET name = value, or SHOW name)"))),
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
            let Some(Statement::Prepare { data_types, statement, .. }) = Parser::parse_sql(&GenericDialect {}, &kept)?.pop() else { bail!("{name}: not a prepared statement (PREPARE {name} AS … makes one)") };
            let mut statement = *statement;
            let mut args = Arguments { values: parameters, types: data_types, wrong: None };
            let _ = VisitMut::visit(&mut statement, &mut args);
            if let Some(e) = args.wrong {
                bail!(e);
            }
            Done::Run(statement.to_string())
        }
        Statement::Execute { .. } => bail!("EXECUTE statement requires a name (EXECUTE name, or EXECUTE name(1, 'a') with its values)"),
        Statement::Deallocate { name, .. } => {
            let name = name.value.to_lowercase();
            crate::temp::with(&session()?, false, |s| match name.as_str() {
                "all" => Ok(s.prepared.clear()),
                _ => s.prepared.remove(&name).map(drop).with_context(|| format!("Prepared statement '{name}' does not exist")),
            })?;
            Done::Said("DEALLOCATE")
        }
        _ => bail!("not a setting or a prepared statement (SET name = value or SHOW name for a setting, PREPARE name AS … for a statement)"),
    })
}

/// A setting's name as kept: lower case, Postgres's `timezone` as DataFusion's.
fn name(n: &str) -> String {
    match n.to_lowercase().as_str() {
        "timezone" | "time.zone" | "time zone" => "datafusion.execution.time_zone".into(),
        "threads" | "worker_threads" => "datafusion.execution.target_partitions".into(), // (DuckDB's: how many parts a query runs in at once)
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
        Expr::CompoundIdentifier(parts) => parts.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join("."), // (SET search_path TO dev.sales)
        Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => format!("-{}", text(expr)?),
        Expr::UnaryOp { op: ast::UnaryOperator::Plus, expr } => format!("+{}", text(expr)?),
        e => bail!("SET takes a string, a number or a word, not {e}"),
    })
}

/// Postgres's settings that ask for something done one way here: the values that say what is done
/// are kept (and `DEFAULT`), any other refused by name, with its SQLSTATE.
const ONE_WAY: &[(&str, &[&str], &str, &str)] = &[
    ("client_encoding", &["utf8", "utf-8", "unicode", "utf8mb4", "utf8mb3"], "22023", "text is UTF-8 here: SET client_encoding = 'UTF8'"),
    ("standard_conforming_strings", &["on", "true", "1"], "22023", "a backslash in '…' is itself here, as Postgres has it by default; E'a\\nb' writes an escape"),
    ("autocommit", &["on", "true", "1"], "22023", "each statement commits on its own here: BEGIN … COMMIT makes several one"),
    ("transaction_read_only", &["off", "false", "0"], "0A000", READ_ONLY),
    ("default_transaction_read_only", &["off", "false", "0"], "0A000", READ_ONLY),
    ("transaction_isolation", &["read committed", "read uncommitted", "repeatable read"], "0A000", SERIALIZABLE),
    ("default_transaction_isolation", &["read committed", "read uncommitted", "repeatable read"], "0A000", SERIALIZABLE),
];

/// Other engines' settings for what is the node's here, with the way to set it.
const THE_NODES: &[(&str, &str)] = &[
    ("memory_limit", "start the node with --memory-gb (PONDRA_MEMORY_GB)"),
    ("max_memory", "start the node with --memory-gb (PONDRA_MEMORY_GB)"),
    ("temp_directory", "spill files go to the system's temporary folder (TMPDIR)"),
];

pub const SERIALIZABLE: &str = "SERIALIZABLE isn't kept here: a transaction is one snapshot, and a row another commit changed meanwhile refuses its COMMIT (40001), as Postgres's REPEATABLE READ does: SET TRANSACTION ISOLATION LEVEL REPEATABLE READ";
pub const READ_ONLY: &str = "a read-only transaction isn't kept here: a session writes as far as its sign-in may, so sign in as a user granted SELECT only for reads that can't write";

/// Is this value of a setting one `ONE_WAY` refuses? (Its SQLSTATE and why.) `DateStyle` takes any
/// order of days and months, but only ISO's output.
fn one_way(n: &str, value: &str) -> Option<(&'static str, String)> {
    let v = value.trim().to_lowercase();
    if v == "default" {
        return None;
    }
    if n == "datestyle" {
        let other = v.split(|c: char| c == ',' || c.is_whitespace()).find(|w| matches!(*w, "postgres" | "sql" | "german"))?;
        return Some(("22023", format!("DateStyle {other}: dates are shown as ISO 8601 here (2026-10-11): SET DateStyle = 'ISO'")));
    }
    let (_, kept, code, why) = ONE_WAY.iter().find(|(name, ..)| *name == n)?;
    (!kept.contains(&v.as_str())).then(|| (*code, format!("{n} = '{value}': {why}")))
}

fn set(session: &str, variable: &str, value: String) -> Result<()> {
    let n = name(variable);
    if let Some(v) = n.strip_prefix('@') {
        return Err(crate::codes::coded("0A000", format!("SET @{v} is MySQL's for a variable: here a variable is ${v} = …, and ${v} reads it")));
    }
    if let Some((code, why)) = one_way(&n, &value) {
        return Err(crate::codes::coded(code, why));
    }
    if let Some((_, how)) = THE_NODES.iter().find(|(name, _)| *name == n) {
        return Err(crate::codes::coded("55P02", format!("{n} is the node's, not a session's: {how}"))); // (cant_change_runtime_param)
    }
    if n.starts_with("datafusion.runtime.") {
        return Err(crate::codes::coded("55P02", format!("{n} is the node's, not a session's: start the node with --memory-gb (PONDRA_MEMORY_GB) for its queries' memory; spill files go to the system's temporary folder (TMPDIR)")));
    }
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
    // URL, a query's copy) registers what is left in no set order, and must find Spark's alone. One
    // whose alias is Spark's name goes too (`array_has`, also `array_contains`): kept, it answered
    // for Spark's `array_contains` in some sessions and not others.
    let (scalar, aggregate) = (datafusion_spark::all_default_scalar_functions(), datafusion_spark::all_default_aggregate_functions());
    let names: std::collections::HashSet<String> = scalar.iter().map(|f| (f.name(), f.aliases())).chain(aggregate.iter().map(|f| (f.name(), f.aliases())))
        .flat_map(|(n, a)| std::iter::once(n.to_string()).chain(a.iter().cloned())).collect();
    let mut spark = datafusion::execution::SessionStateBuilder::new_from_existing(ctx.state());
    spark.expr_planners().get_or_insert_with(Vec::new).insert(0, std::sync::Arc::new(datafusion_spark::planner::SparkFunctionPlanner));
    let fns = spark.scalar_functions().get_or_insert_with(Vec::new);
    fns.retain(|f| !names.contains(f.name()) && !f.aliases().iter().any(|a| names.contains(a)));
    fns.extend(scalar);
    let fns = spark.aggregate_functions().get_or_insert_with(Vec::new);
    fns.retain(|f| !names.contains(f.name()) && !f.aliases().iter().any(|a| names.contains(a)));
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

/// `SHOW name` of a Postgres setting the session keeps: one row, its column named as the setting,
/// as Postgres answers it (`search_path` is `public` until set). DataFusion's own (`datafusion.*`,
/// `TIME ZONE`) and a name never set are left to DataFusion's `SHOW`.
pub fn show(sql: &str) -> Option<String> {
    static SHOW: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)^\s*show\s+([a-z_][a-z0-9_]*)\s*;?\s*$").expect("a regex"));
    let n = SHOW.captures(sql)?[1].to_lowercase();
    if name(&n).contains('.') {
        return None;
    }
    let v = shown(&n).or_else(|| (n == "search_path").then(|| "public".to_string()))?;
    Some(format!("SELECT '{}' AS {n}", v.replace('\'', "''")))
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
            self.wrong = Some(format!("Unknown placeholder: {p} (the placeholders are $1, $2, … in order)"));
            return ControlFlow::Break(());
        };
        let Some(value) = self.values.get(i - 1).cloned() else {
            self.wrong = Some(format!("No value found for placeholder with name {p} (EXECUTE name(1, 'a') gives one value per placeholder, in order)"));
            return ControlFlow::Break(());
        };
        *e = match self.types.get(i - 1) {
            Some(t) => Expr::Cast { kind: ast::CastKind::Cast, expr: Box::new(value), data_type: t.clone(), array: false, format: None },
            None => Expr::Nested(Box::new(value)),
        };
        ControlFlow::Continue(())
    }
}
