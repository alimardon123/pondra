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
use crate::answer::Kind;
use crate::store::Lake;
use anyhow::{bail, ensure, Context, Result};
use datafusion::logical_expr::{LogicalPlan, SetVariable, Statement as Plan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, Expr, Reset, Set, Statement, Value, VisitorMut};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use std::ops::ControlFlow;

pub const NO_SESSION: &str = "a setting or a prepared statement is a session's: a Postgres connection's, the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>, 8 to 128 letters, digits, - or _), or a script's (send it with the statements that use it)";

/// Is this one of the statements kept here? (`USE`, too: the session's search path, `path.rs`.)
pub fn is(sql: &str) -> bool {
    let word = crate::text::first_word(sql).split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("").to_uppercase();
    (matches!(word.as_str(), "SET" | "RESET" | "PREPARE" | "EXECUTE" | "EXEC" | "DEALLOCATE" | "ANALYZE" | "ANALYSE" | "REFRESH" | "USE")
        && crate::runs::execute_of(sql).is_none()) // (EXECUTE TASK: the leader's)
        || crate::extension::is(sql) // (DuckDB's INSTALL, FORCE INSTALL and LOAD, and Pondra's: `extension.rs`)
}

/// What Postgres's tools send that has nothing to do here (dbt and BI tools send both): `ANALYZE
/// [t]`, since statistics are kept with every write, and `REFRESH MATERIALIZED VIEW v`, since a
/// view is current as rows arrive. Refused for a name that isn't one.
async fn current(lake: &Lake, sql: &str, scope: &crate::rights::Scope) -> Result<Option<Done>> {
    let name = {
        let mut p = Parser::new(&GenericDialect {}).try_with_sql(sql)?;
        let word = |p: &mut Parser, w: &str| crate::routines::word(p, w);
        if word(&mut p, "analyze") || word(&mut p, "analyse") {
            return Ok(Some(Done::Said(Kind::new("ANALYZE"))));
        }
        if !word(&mut p, "refresh") {
            return Ok(None);
        }
        ensure!(word(&mut p, "materialized") && word(&mut p, "view"), "REFRESH MATERIALIZED VIEW name");
        let _ = word(&mut p, "concurrently");
        crate::text::object(&p.parse_object_name(false)?)
    };
    let (other, local) = crate::ddl::resolve(lake, &name).await?;
    let l = other.as_deref().unwrap_or(lake);
    let (schema, view) = crate::text::split(&local);
    let here = crate::ddl::lake_name(l);
    let found = crate::ddl::listed_for(l, scope).await?.into_iter().any(|t| t.lake == here && t.schema == schema && t.name == view && t.kind == "materialized view");
    ensure!(found, "REFRESH MATERIALIZED VIEW {name}: there is no materialized view of that name");
    Ok(Some(Done::Said(Kind::new("REFRESH MATERIALIZED VIEW"))))
}

/// What a statement kept here comes to.
pub enum Done {
    Said(Kind), // (its command tag)
    Run(String),        // an `EXECUTE`: this statement, run as if sent
}

/// Carry out a `SET`, `RESET`, `PREPARE`, `EXECUTE` or `DEALLOCATE` for the current session. `scope`: the asker's, whom a
/// PREPARE's query plans for (the rest plan nothing).
pub async fn statement(lake: &Lake, sql: &str, scope: &crate::rights::Scope) -> Result<Done> {
    if let Some(said) = crate::extension::statement(lake, sql).await {
        let said = said?; // (DuckDB's INSTALL and LOAD: no session needed; the notice goes to the client as the others do)
        crate::routines::heard(&said.notice);
        return Ok(Done::Said(Kind::new(said.tag)));
    }
    if let Some(done) = current(lake, sql, scope).await? {
        return Ok(done);
    }
    let session = crate::temp::current();
    let session = || session.clone().context(NO_SESSION);
    // `USE` and `SET SCHEMA` set the session's search path (`path.rs`). sqlparser reads neither, so
    // they are taken here. The session is looked for first: a USE with none says so, by name.
    if crate::path::is_use(sql) {
        let session = session()?;
        set(&session, "search_path", crate::path::use_of(lake, sql).await?)?;
        return Ok(Done::Said(Kind::new("USE")));
    }
    if let Some(schema) = crate::path::set_schema(sql) {
        set(&session()?, "search_path", schema)?;
        return Ok(Done::Said(Kind::new("SET")));
    }
    let mut parsed = Parser::parse_sql(&GenericDialect {}, sql)?;
    ensure!(parsed.len() == 1, "one statement at a time");
    Ok(match parsed.remove(0) {
        Statement::Set(Set::SingleAssignment { variable, values, .. }) => {
            let value = values.iter().map(text).collect::<Result<Vec<_>>>()?.join(", ");
            set(&session()?, &variable.to_string(), value)?;
            Done::Said(Kind::new("SET"))
        }
        Statement::Set(Set::SetTimeZone { value, .. }) => {
            set(&session()?, "timezone", text(&value)?)?;
            Done::Said(Kind::new("SET"))
        }
        Statement::Set(Set::MultipleAssignments { assignments }) => {
            let session = session()?;
            for a in assignments {
                set(&session, &a.name.to_string(), text(&a.value)?)?;
            }
            Done::Said(Kind::new("SET"))
        }
        // What other statements called SET ask for is done as asked, or refused by name: a SET
        // answered and not done would be a quiet lie (principle 10).
        Statement::Set(Set::SetTransaction { modes, snapshot, session: for_session }) => {
            // SET TRANSACTION (the open transaction) and SET SESSION CHARACTERISTICS (the session's levels):
            // ADR-067 §1. An imported snapshot isn't built, so it is refused by name.
            if snapshot.is_some() {
                return Err(crate::codes::coded("0A000", "SET TRANSACTION SNAPSHOT isn't built here"));
            }
            if let Some(w) = crate::txn::modes(&modes, for_session)? {
                crate::routines::heard(&format!("WARNING: {w}"));
            }
            Done::Said(Kind::new("SET"))
        }
        Statement::Set(Set::SetRole { role_name: None, .. }) => Done::Said(Kind::new("SET")), // (SET ROLE NONE: the sign-in's own rights, which a session always has)
        Statement::Set(Set::SetRole { role_name: Some(role), .. }) => {
            return Err(crate::codes::coded("0A000", format!("SET ROLE {role}: a session has its sign-in's rights, and no other: sign in as a user who has them, or GRANT {role} TO the user you sign in as")))
        }
        Statement::Set(Set::SetSessionAuthorization(_)) => {
            return Err(crate::codes::coded("0A000", "SET SESSION AUTHORIZATION: a session is its sign-in's: sign in as that user (with its token or password)"))
        }
        Statement::Set(Set::SetNames { charset_name, .. }) => {
            set(&session()?, "client_encoding", charset_name.value)?; // (Postgres's SET NAMES is client_encoding)
            Done::Said(Kind::new("SET"))
        }
        Statement::Set(Set::SetNamesDefault {}) => Done::Said(Kind::new("SET")),
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
            Done::Said(Kind::new("RESET"))
        }
        Statement::Prepare { name, statement, .. } => {
            let name = name.value.to_lowercase();
            if !matches!(*statement, Statement::Insert(_) | Statement::Update(..) | Statement::Delete(_) | Statement::Merge(..)) {
                let ctx = crate::query::session_for(lake, &statement.to_string(), "", None, scope).await?;
                ctx.execute_logical_plan(ctx.state().create_logical_plan(sql).await?).await?; // (its parameters and their types agree; its tables are there)
            }
            crate::temp::with(&session()?, false, |s| Ok(s.prepared.insert(name, sql.to_string())))?;
            Done::Said(Kind::new("PREPARE"))
        }
        Statement::Execute { name: Some(name), parameters, .. } if !name.to_string().starts_with('\'') => {
            let name = name.to_string().to_lowercase();
            let kept = crate::temp::with(&session()?, false, |s| Ok(s.prepared.get(&name).cloned()))?;
            let kept = kept.ok_or_else(|| crate::codes::coded("26000", format!("Prepared statement '{name}' does not exist")))?;
            let Some(Statement::Prepare { data_types, statement, .. }) = Parser::parse_sql(&GenericDialect {}, &kept)?.pop() else { bail!("{name}: not a prepared statement (PREPARE {name} AS … makes one)") };
            let mut statement = *statement;
            let mut args = Arguments { values: parameters, types: data_types, wrong: None };
            let _ = crate::walk::visit_mut(&mut statement, &mut args);
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
                _ => s.prepared.remove(&name).map(drop).ok_or_else(|| crate::codes::coded("26000", format!("Prepared statement '{name}' does not exist"))),
            })?;
            Done::Said(Kind::new("DEALLOCATE"))
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
    ("transaction_isolation", &["read committed", "read uncommitted", "repeatable read"], "0A000", SERIALIZABLE),
    ("default_transaction_isolation", &["read committed", "read uncommitted", "repeatable read"], "0A000", SERIALIZABLE),
];

/// Other engines' settings for what is the node's here, with the way to set it.
const THE_NODES: &[(&str, &str)] = &[
    ("memory_limit", "start the node with --memory-gb (PONDRA_MEMORY_GB)"),
    ("max_memory", "start the node with --memory-gb (PONDRA_MEMORY_GB)"),
    ("temp_directory", "spill files go to the system's temporary folder (TMPDIR)"),
];

pub use crate::rules::txn::SERIALIZABLE; // (one definition: rules::txn)

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

/// The table formats a new table can be made in, and the file formats its files can be written in: what
/// `default_table_format` and `default_file_format` take (I11). `iceberg` joins with this lake's own Iceberg tables (I9),
/// and `delta` in 1.1; until then a table is published in them (`WITH (publish = …)`).
pub const TABLE_FORMATS: &[&str] = &["pondra"];
pub const FILE_FORMATS: &[&str] = &["parquet"];

/// Where a new table's default format came from, nearest first (I11): its session's `SET`, its node's setting
/// (`--default-table-format`), else Pondra's own. A schema's and a database's go between the session and the node, with
/// their records (step 5).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Source {
    Session,
    Node,
    Pondra,
}

/// One of the defaults a CREATE TABLE that names no format takes: its session name, its node setting, what it takes.
struct FormatDefault {
    name: &'static str,
    var: &'static str,
    takes: &'static [&'static str],
    what: &'static str,
}

const TABLE: FormatDefault = FormatDefault { name: "default_table_format", var: "PONDRA_DEFAULT_TABLE_FORMAT", takes: TABLE_FORMATS, what: "table format" };
const FILE: FormatDefault = FormatDefault { name: "default_file_format", var: "PONDRA_DEFAULT_FILE_FORMAT", takes: FILE_FORMATS, what: "file format" };

/// A new table's format when its CREATE TABLE names none, and where it came from (I11): what CREATE's planning reads
/// (step 8, L1 and L6), as `SHOW default_table_format` does.
#[allow(dead_code)] // (until step 8's CREATE reads it)
pub fn table_format(lake: &Lake) -> Result<(String, Source)> { resolved(&TABLE, shown(TABLE.name), lake.node.settings.get(TABLE.var)) }

/// A new table's file format when its CREATE TABLE names none (`WITH (file_format = …)`), and where it came from (I11).
#[allow(dead_code)] // (until step 8's CREATE reads it)
pub fn file_format(lake: &Lake) -> Result<(String, Source)> { resolved(&FILE, shown(FILE.name), lake.node.settings.get(FILE.var)) }

/// The session's value (checked as it was set), else the node's (checked here: a setting is read as given), else Pondra's own.
fn resolved(d: &FormatDefault, session: Option<String>, node: Option<&str>) -> Result<(String, Source)> {
    if let Some(v) = session.filter(|v| !v.eq_ignore_ascii_case("default")) {
        return Ok((v, Source::Session));
    }
    match node {
        Some(v) => Ok((taken(d, v).with_context(|| format!("the node's {}", d.var))?, Source::Node)),
        None => Ok((d.takes[0].to_string(), Source::Pondra)),
    }
}

/// `value` as one of `d`'s formats, lower case, or why not: by name for a format that comes later.
fn taken(d: &FormatDefault, value: &str) -> Result<String> {
    let v = value.trim().to_ascii_lowercase();
    if d.takes.contains(&v.as_str()) {
        return Ok(v);
    }
    let later = |when: &str, publish: &str| crate::codes::coded("0A000", format!("{} = '{value}': this lake's own {publish} tables come {when}; until then a table is published as {publish} with WITH (publish = '{v}')", d.name));
    Err(match (d.name, v.as_str()) {
        ("default_table_format", "iceberg") => later("in 1.0", "Iceberg"),
        ("default_table_format", "delta") => later("in 1.1", "Delta"),
        _ => crate::codes::coded("22023", format!("invalid value for parameter \"{}\": \"{value}\": a {} here is {}", d.name, d.what, d.takes.join(" or "))),
    })
}

/// The defaults' values as `SET` keeps them (I11): a format this build has (or `DEFAULT`, which leaves it to the node).
/// Postgres's `default_table_access_method` is taken for its one value here, `heap`, which is Pondra's own format
/// (`pg_dump` sets it before each table): it sets `default_table_format` to `pondra`.
fn defaults_kept(n: String, value: String) -> Result<(String, String)> {
    let default = value.trim().eq_ignore_ascii_case("default");
    match n.as_str() {
        "default_table_format" | "default_file_format" if default => Ok((n, "default".into())),
        "default_table_format" => Ok((n, taken(&TABLE, &value)?)),
        "default_file_format" => Ok((n, taken(&FILE, &value)?)),
        "default_table_access_method" if default || value.trim().eq_ignore_ascii_case("heap") => Ok((TABLE.name.into(), if default { "default".into() } else { "pondra".into() })),
        "default_table_access_method" => Err(crate::codes::coded("22023", format!("default_table_access_method = '{value}': a table here is kept in Pondra's own format (heap); SET default_table_format names another"))),
        // (a bare name is a table's own, live; `default_` is what new tables copy: the names point at each other)
        "table_format" | "file_format" => {
            let (d, own) = if n == "table_format" { (&TABLE, "named by its CREATE TABLE's USING") } else { (&FILE, "set with the table") };
            Err(crate::codes::coded("42704", format!("unrecognized configuration parameter \"{n}\": a table's {} is its own, {own}; SET {} = '{}' sets the one new tables take", d.what, d.name, d.takes[0])))
        }
        _ => Ok((n, value)),
    }
}

/// `SHOW` of a default the session didn't set: the node's, else Pondra's own. (No lake here: the node's as the process
/// was given it, `env::get`.)
fn default_shown(n: &str) -> Option<String> {
    let d = [&TABLE, &FILE].into_iter().find(|d| d.name == n)?;
    resolved(d, None, crate::env::get(d.var).as_deref()).ok().map(|(v, _)| v)
}

/// The settings a client sent as its session started, set for the current session as `SET name = 'value'` sets each, in
/// order: a Postgres client's startup `options` (`-c name=value`) and `application_name`, as Postgres sets them. A value
/// `SET` refuses is refused here with its code, and the connection with it.
pub fn at_start(settings: &[(String, String)]) -> Result<()> {
    let session = crate::temp::current().context(NO_SESSION)?;
    settings.iter().try_for_each(|(n, v)| set(&session, n, v.clone()))
}

fn set(session: &str, variable: &str, value: String) -> Result<()> {
    let n = name(variable);
    if let Some(v) = n.strip_prefix('@') {
        return Err(crate::codes::coded("0A000", crate::dialect_stmts::user_variable(v)));
    }
    if let Some((code, why)) = one_way(&n, &value) {
        return Err(crate::codes::coded(code, why));
    }
    let (n, value) = defaults_kept(n, value)?;
    if let Some((_, how)) = THE_NODES.iter().find(|(name, _)| *name == n) {
        return Err(crate::codes::coded("55P02", format!("{n} is the node's, not a session's: {how}"))); // (cant_change_runtime_param)
    }
    if n.starts_with("datafusion.runtime.") {
        return Err(crate::codes::coded("55P02", format!("{n} is the node's, not a session's: start the node with --memory-gb (PONDRA_MEMORY_GB) for its queries' memory; spill files go to the system's temporary folder (TMPDIR)")));
    }
    if n.contains('.') {
        // (a name DataFusion has is datafusion.…: any other dotted name is no setting of Pondra's, refused as Postgres refuses
        // one it doesn't have; a Spark one says where its settings are)
        if !n.starts_with("datafusion.") {
            let spark = match n.starts_with("spark.") {
                true => ": Spark's settings aren't Pondra's (SHOW ALL lists Pondra's)",
                false => "",
            };
            return Err(crate::codes::coded("42704", format!("unrecognized configuration parameter \"{n}\"{spark}")));
        }
        datafusion::config::ConfigOptions::new().set(&n, &value)?; // (a value DataFusion takes for a name it has)
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
    let node = || crate::env::get("PONDRA_SQL_OPTIONS")?.split(',').find_map(|kv| Some(kv.trim().split_once('=').filter(|(k, _)| *k == KEY)?.1.to_string()));
    datafusion::sql::sqlparser::dialect::dialect_from_str(session.or_else(node)?)
}

/// `SHOW name` of a Postgres setting the session keeps: one row, its column named as the setting,
/// as Postgres answers it (`search_path` is `public` until set). DataFusion's own (`datafusion.*`,
/// `TIME ZONE`) and a name never set are left to DataFusion's `SHOW`.
pub fn show(sql: &str) -> Option<String> {
    if crate::text::head(sql) != "show" {
        return None;
    }
    static SHOW: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)^\s*show\s+([a-z_][a-z0-9_]*)\s*;?\s*$").expect("a regex"));
    let n = SHOW.captures(sql)?[1].to_lowercase();
    if name(&n).contains('.') {
        return None;
    }
    let v = shown(&n).or_else(|| (n == "search_path").then(|| "public".to_string())).or_else(|| default_shown(&n))?;
    Some(format!("SELECT '{}' AS {n}", v.replace('\'', "''")))
}

/// A Postgres setting the current session set (`SHOW name`).
pub fn shown(n: &str) -> Option<String> {
    let s = crate::temp::current()?;
    crate::temp::peek(&s, |x| x.settings.get(&name(n)).cloned()) // (a peek: reading a setting makes no session)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A table's default formats (I11): SET keeps a format this build has, refuses one that comes later by name and any
    /// other with the ones it has; pg_dump's `heap` is Pondra's own; the session's comes before the node's, which comes
    /// before Pondra's own, and a node's bad value is refused naming its setting.
    #[test]
    fn a_new_tables_default_formats_are_kept_checked_and_resolved_nearest_first() {
        let kept = |n: &str, v: &str| defaults_kept(n.into(), v.into());
        assert_eq!(kept("default_table_format", "Pondra").unwrap(), ("default_table_format".into(), "pondra".into()));
        assert_eq!(kept("default_file_format", " PARQUET ").unwrap(), ("default_file_format".into(), "parquet".into()));
        assert_eq!(kept("default_table_format", "DEFAULT").unwrap().1, "default");
        let e = kept("default_table_format", "iceberg").unwrap_err();
        assert_eq!(crate::codes::of(&e), "0A000");
        assert!(e.to_string().contains("Iceberg tables come in 1.0") && e.to_string().contains("WITH (publish = 'iceberg')"), "{e}");
        assert!(kept("default_table_format", "delta").unwrap_err().to_string().contains("Delta tables come in 1.1"));
        let e = kept("default_file_format", "orc").unwrap_err();
        assert_eq!((crate::codes::of(&e), e.to_string()), ("22023", "invalid value for parameter \"default_file_format\": \"orc\": a file format here is parquet".into()));
        // (pg_dump's `SET default_table_access_method = heap`: Pondra's own format; another access method names the setting)
        assert_eq!(kept("default_table_access_method", "heap").unwrap(), ("default_table_format".into(), "pondra".into()));
        let e = kept("default_table_access_method", "columnar").unwrap_err();
        assert!(crate::codes::of(&e) == "22023" && e.to_string().contains("SET default_table_format"), "{e}");
        let e = kept("file_format", "parquet").unwrap_err();
        assert!(crate::codes::of(&e) == "42704" && e.to_string().contains("SET default_file_format = 'parquet' sets the one new tables take"), "{e}");
        assert!(kept("table_format", "pondra").unwrap_err().to_string().contains("named by its CREATE TABLE's USING; SET default_table_format"));
        assert_eq!(kept("search_path", "crm").unwrap(), ("search_path".into(), "crm".into()), "any other setting is kept as given");

        assert_eq!(resolved(&TABLE, Some("pondra".into()), Some("nope")).unwrap(), ("pondra".into(), Source::Session), "the session's first");
        assert_eq!(resolved(&TABLE, Some("default".into()), Some("POndra")).unwrap(), ("pondra".into(), Source::Node), "DEFAULT: the node's");
        assert_eq!(resolved(&FILE, None, None).unwrap(), ("parquet".into(), Source::Pondra), "else Pondra's own");
        let e = resolved(&TABLE, None, Some("hudi")).unwrap_err();
        assert!(format!("{e:#}").starts_with("the node's PONDRA_DEFAULT_TABLE_FORMAT: invalid value"), "{e:#}");
        assert_eq!(default_shown("default_table_format").as_deref(), Some("pondra"), "SHOW with nothing set: Pondra's own");
        assert_eq!(default_shown("search_path"), None);
        let flag = crate::settings::find(TABLE.var).and_then(|s| s.flag.map(|_| s.default()));
        assert_eq!(flag, Some(Some("pondra".into())), "pondra serve --default-table-format");
    }

    /// The settings a session may set over its node's are the registry's rows that say so (`pondra.settings`' `session`): the
    /// defaults kept here, each by its name.
    #[test]
    fn the_settings_a_session_sets_over_its_nodes_are_the_registrys() {
        let mut rows: Vec<(String, &str)> = crate::settings::all().iter().filter(|s| s.session).map(|s| (s.name(), s.var)).collect();
        rows.sort();
        let mut kept: Vec<(String, &str)> = [&TABLE, &FILE].iter().map(|d| (d.name.to_string(), d.var)).collect();
        kept.sort();
        assert_eq!(rows, kept);
    }
}
