//! A session's settings and prepared statements (round 31): `SET name = value`, `SET TIME ZONE`,
//! `RESET name | ALL`, `PREPARE name AS …`, `EXECUTE name(…)`, `DEALLOCATE name | ALL` and `DISCARD`, as in
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
    (matches!(word.as_str(), "SET" | "RESET" | "PREPARE" | "EXECUTE" | "EXEC" | "DEALLOCATE" | "DISCARD" | "ANALYZE" | "ANALYSE" | "REFRESH" | "USE")
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

/// Carry out a `SET`, `RESET`, `PREPARE`, `EXECUTE`, `DEALLOCATE` or `DISCARD` for the current session. `scope`: the asker's, whom a
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
            crate::temp::with(&session()?, false, |s| Ok(match name.as_deref() {
                Some(DIALECT) => drop([DIALECT, "language"].map(|n| s.settings.remove(n))), // (the dialect's setting names the language too)
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
            Done::Said(Kind::new(if name == "all" { "DEALLOCATE ALL" } else { "DEALLOCATE" })) // (Postgres's tags: psycopg forgets its own prepared statements at DEALLOCATE ALL's)
        }
        Statement::Discard { object_type } => {
            discard(&session()?, object_type)?;
            Done::Said(Kind::new(format!("DISCARD {object_type}")))
        }
        _ => bail!("not a setting or a prepared statement (SET name = value or SHOW name for a setting, PREPARE name AS … for a statement)"),
    })
}

/// `DISCARD`, as Postgres's. `ALL` is what a pooler sends before it hands a connection to another client (PgBouncer's
/// reset), and is Postgres's CLOSE ALL, SET SESSION AUTHORIZATION DEFAULT, RESET ALL, DEALLOCATE ALL, UNLISTEN *,
/// pg_advisory_unlock_all(), DISCARD PLANS, TEMP and SEQUENCES. A session here has of those its cursors, settings (the
/// language among them), prepared statements, temporary objects and sequences' currval, and its `$variables` beside:
/// all go, as `Sessions::end` ends a session. It has no role but its sign-in's (SET ROLE and SET SESSION AUTHORIZATION are
/// refused), no LISTEN and no advisory lock yet: each, once there is one, goes here too. The statements the protocol
/// prepared (a Parse's) are pgwire's, and the Postgres port drops them after DISCARD ALL and DEALLOCATE (`pg::forgets`).
fn discard(session: &str, what: ast::DiscardObject) -> Result<()> {
    let Some(at) = crate::temp::here().map(|n| n.at) else { bail!("no session here") };
    match what {
        ast::DiscardObject::ALL if crate::txn::held(&at, session) => {
            bail!(crate::codes::coded("25001", "DISCARD ALL cannot run inside a transaction block (COMMIT or ROLLBACK first)"))
        }
        ast::DiscardObject::ALL => drop(at.end(session)),
        ast::DiscardObject::TEMP => crate::temp::discard_temporary(session)?,
        ast::DiscardObject::SEQUENCES => crate::temp::with(session, false, |s| Ok(s.currval.clear()))?,
        ast::DiscardObject::PLANS => {} // (a session keeps no plan: each statement is planned as it runs)
    }
    Ok(())
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
        _ => Ok((n, value)),
    }
}

/// `SHOW` of a default the session didn't set: the node's, else Pondra's own. (No lake here: the node's as the process
/// was given it, `env::get`.)
fn default_shown(n: &str) -> Option<String> {
    let d = [&TABLE, &FILE].into_iter().find(|d| d.name == n)?;
    resolved(d, None, crate::env::get(d.var).as_deref()).ok().map(|(v, _)| v)
}

fn set(session: &str, variable: &str, value: String) -> Result<()> {
    let mut n = name(variable);
    // (the session's language, `languages.rs`: by its name, or by DataFusion's dialect where that names one (`spark`). A
    // language picks the parse, so DataFusion's dialect is then generic, the one its print is read in; another dialect
    // (`mysql`, until step 8's PR 9) is Pondra's SQL read so)
    let value = if n == "language" {
        language_named(&value)?
    } else if let Some(l) = (n == DIALECT).then(|| crate::languages::get().named(&value)).flatten() {
        n = "language".into();
        l.name.to_string()
    } else {
        value
    };
    if n == "language" || n == DIALECT {
        let other = if n == "language" { DIALECT } else { "language" };
        crate::temp::with(session, false, |s| Ok(drop(s.settings.remove(other))))?;
    }
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

/// A query's session with the current session's settings of DataFusion's. (Spark SQL's meaning is in the statement
/// itself, its functions and casts turned as it arrives: `sparksql.rs`, `statement::in_language`.)
pub async fn apply(ctx: SessionContext) -> Result<SessionContext> {
    let kept = crate::temp::current().map(|s| crate::temp::with(&s, false, |s| Ok(s.settings.iter().filter(|(k, _)| k.starts_with("datafusion.")).map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>())));
    for (variable, value) in kept.transpose()?.unwrap_or_default() {
        ctx.execute_logical_plan(LogicalPlan::Statement(Plan::SetVariable(SetVariable { variable, value }))).await?; // (as DataFusion sets one: functions that read it told)
    }
    Ok(ctx)
}

const DIALECT: &str = "datafusion.sql_parser.dialect";

/// The node's DataFusion dialect (`PONDRA_SQL_OPTIONS`).
fn node_dialect() -> Option<String> {
    crate::env::get("PONDRA_SQL_OPTIONS")?.split(',').find_map(|kv| Some(kv.trim().split_once('=').filter(|(k, _)| *k == DIALECT)?.1.trim().to_string()))
}

/// The SQL dialect the session set (or the node: `PONDRA_SQL_OPTIONS`), for a statement the generic parser doesn't take
/// (DuckDB's `STRUCT(a INT)`, …). A dialect that names a language (`spark`) is the language's, read as the statement
/// arrives (`language`), and none here.
pub fn dialect() -> Option<Box<dyn datafusion::sql::sqlparser::dialect::Dialect>> {
    let session = crate::temp::current().and_then(|s| crate::temp::peek(&s, |x| x.settings.get(DIALECT).cloned()));
    let d = session.or_else(node_dialect).filter(|d| crate::languages::get().named(d).is_none())?;
    datafusion::sql::sqlparser::dialect::dialect_from_str(d)
}

/// The language the session's statements are written in (`languages.rs`): its own `SET language` (or the dialect's
/// setting that names one), else the node's (`PONDRA_LANGUAGE`, or `PONDRA_SQL_OPTIONS`'s dialect where that names one),
/// else Pondra's SQL.
pub fn language() -> &'static str {
    own_language().unwrap_or_else(node_language)
}

/// The node's language, a session's when it sets none: `PONDRA_LANGUAGE`, or `PONDRA_SQL_OPTIONS`'s dialect where that
/// names one, else Pondra's SQL.
pub fn node_language() -> &'static str {
    let named = |n: String| crate::languages::get().named(&n).map(|l| l.name);
    crate::env::get("PONDRA_LANGUAGE").and_then(named).or_else(|| node_dialect().and_then(named)).unwrap_or(crate::parsed::LANGUAGE.name)
}

/// What a statement does to the session's language, as `statement` does it: `Some(Some(name))` when it sets one (by
/// `language`, or by DataFusion's dialect where that names one), `Some(None)` when it lets the session's own go (RESET
/// language or ALL, DISCARD ALL, a dialect that names none), `None` when it doesn't touch it. A request's statements
/// after it are read in the language it leaves (`statement::in_language`), as each would be sent on its own.
pub(crate) fn language_change(one: &str) -> Option<Option<String>> {
    let head = crate::text::head(one);
    if !["set", "reset", "discard"].iter().any(|h| head.eq_ignore_ascii_case(h)) {
        return None;
    }
    let mut parsed = Parser::parse_sql(&GenericDialect {}, one).ok()?;
    let assigned = |n: &str, value: String| match name(n).as_str() {
        "language" => Some(Some(value)),
        DIALECT => Some(crate::languages::get().named(&value).map(|l| l.name.to_string())),
        _ => None,
    };
    match (parsed.len(), parsed.pop()?) {
        (1, Statement::Set(Set::SingleAssignment { variable, values, .. })) => assigned(&variable.to_string(), values.iter().map(text).collect::<Result<Vec<_>>>().ok()?.join(", ")),
        (1, Statement::Set(Set::MultipleAssignments { assignments })) => assignments.iter().filter_map(|a| assigned(&a.name.to_string(), text(&a.value).ok()?)).last(),
        (1, Statement::Reset(r)) => match r.reset {
            Reset::ALL => Some(None),
            Reset::ConfigurationParameter(n) => matches!(name(&n.to_string()).as_str(), "language" | DIALECT).then_some(None),
        },
        (1, Statement::Discard { object_type: ast::DiscardObject::ALL }) => Some(None),
        _ => None,
    }
}

/// The language the session itself set (`SET language`, or the dialect's setting that names one), not the node's.
pub fn own_language() -> Option<&'static str> {
    let session = crate::temp::current().and_then(|s| crate::temp::peek(&s, |x| x.settings.get("language").cloned()))?;
    crate::languages::get().named(&session).map(|l| l.name)
}

/// The language `SET language` names, by its name: one no language has is refused, naming those there are.
fn language_named(value: &str) -> Result<String> {
    let languages = crate::languages::get();
    match languages.named(value) {
        Some(l) => Ok(l.name.to_string()),
        None => Err(crate::codes::coded("22023", format!("invalid value for parameter \"language\": \"{value}\" (the languages are {})", languages.listed()))),
    }
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
    if n == "language" {
        return Some(format!("SELECT '{}' AS language", language()));
    }
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

    /// A session's language is its own (`SET language`, or the dialect's setting that names one), else the node's, else
    /// Pondra's SQL; a dialect that names none is DataFusion's, read in Pondra's SQL, and the two never stand together.
    #[tokio::test]
    async fn a_session_s_language_is_its_own_or_pondra_s_sql() {
        let (s, at) = (crate::temp::of_script(""), std::sync::Arc::new(crate::temp::Sessions::default()));
        crate::temp::SESSION.sync_scope(Some(crate::temp::Named { name: s.clone(), at: at.clone() }), || {
            assert_eq!((language(), own_language()), ("pondra", None));
            set(&s, "language", " Spark".into()).unwrap();
            assert_eq!((language(), own_language()), ("spark", Some("spark")));
            set(&s, DIALECT, "mysql".into()).unwrap();
            assert_eq!((language(), dialect().is_some()), ("pondra", true), "another dialect: Pondra's SQL read so");
            set(&s, DIALECT, "databricks".into()).unwrap();
            assert_eq!((language(), dialect().is_none()), ("spark", true), "a dialect that names a language is the language's alone");
            set(&s, "LANGUAGE", "pondra".into()).unwrap();
            assert_eq!((language(), own_language()), ("pondra", Some("pondra")));
            let refused = set(&s, "language", "cobol".into()).unwrap_err();
            assert_eq!(crate::codes::of(&refused), "22023");
            assert!(refused.to_string().contains("(the languages are pondra (Pondra's SQL) or spark (Spark SQL))"), "{refused}");
        });
        at.end(&s);
    }

    #[tokio::test]
    async fn discard_is_a_sessions() {
        let (s, at) = (crate::temp::of_script(""), std::sync::Arc::new(crate::temp::Sessions::default()));
        crate::temp::SESSION.sync_scope(Some(crate::temp::Named { name: s.clone(), at: at.clone() }), || {
            let kept = |f: fn(&crate::temp::Session) -> bool| crate::temp::peek(&s, |x| Some(f(x))).unwrap_or(false);
            let fill = || {
                set(&s, "language", "spark".into()).unwrap();
                crate::temp::with(&s, false, |x| Ok(x.currval.insert("s".into(), 1))).unwrap();
            };
            fill();
            discard(&s, ast::DiscardObject::SEQUENCES).unwrap();
            assert!(kept(|x| x.currval.is_empty() && !x.settings.is_empty()), "SEQUENCES: currval alone");
            discard(&s, ast::DiscardObject::PLANS).unwrap();
            discard(&s, ast::DiscardObject::TEMP).unwrap();
            assert_eq!(own_language(), Some("spark"), "PLANS and TEMP keep the settings");
            fill();
            discard(&s, ast::DiscardObject::ALL).unwrap();
            assert_eq!((own_language(), kept(|x| !x.currval.is_empty())), (None, false), "ALL: the session starts again");
        });
        at.end(&s);
        for form in ["DISCARD ALL", "discard temp;", "/* a pooler's */ DISCARD  ALL"] {
            assert!(is(form), "{form}");
        }
        assert!(!is("-- DISCARD ALL\nSELECT 1"));
    }
}
