//! Schemas, names, and the statements that shape a lake: `CREATE`/`DROP SCHEMA`, `DROP TABLE`,
//! `CREATE VIEW` (a stored query), `CREATE MATERIALIZED VIEW` (a live view, `views.rs`) and
//! `DROP VIEW`.
//!
//! A lake is a database, and its tables live in schemas: `public` unless one is named. Inside the
//! lake a table of schema `s` is called `s.t`, and one of `public` just `t`, as before schemas
//! existed. So everything keyed by a table's name works unchanged: the catalog, the log, the
//! files, Kafka topics. In SQL:
//! - `t` is `public.t`;
//! - `s.t` is schema `s` here, or, if no schema here has that name, an attached lake's `public.t`
//!   (what two-part names meant before);
//! - `l.s.t` is lake `l`: this one (by its folder's name) or an attached one.
//!
//! Other lakes are attached with `--attach name=dir` when a node starts, or in SQL with
//! `ATTACH 'dir' AS name` (kept in the catalog, so every node attaches it) and `DETACH name`.
use crate::store::{json, table_key, Lake, TableMeta};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::sync::Arc;
use crate::text::{join, local_in, mentions, split, PUBLIC};

pub fn schema_key(s: &str) -> String { format!("ns/{s}") }
/// The system's schema: `pondra.runs`, `pondra.tasks` and the rest (`query.rs` registers them). No schema of a user's may take
/// its name (`CREATE SCHEMA`, here, the one step that makes a schema's key), so none of them is ever hidden by one.
pub const SYSTEM_SCHEMA: &str = "pondra";
/// What a schema that is not there says (`resolve`, `new_name`): `CREATE SCHEMA` makes one, but pondra is the system's and no one
/// makes it, so its words name its tables (a write into them is read-only). `path::schema_in` says the same for pondra, and
/// names the database for any other schema.
/// Refuses `name` (42939) for a schema, a database or another engine's catalog when it is the system's schema's: any of
/// them called pondra would hide `pondra.runs` and the rest. `then` says what to do instead.
pub(crate) fn not_system(name: &str, then: &str) -> Result<()> {
    match name.eq_ignore_ascii_case(SYSTEM_SCHEMA) {
        true => Err(crate::codes::coded("42939", format!("pondra is the system's schema (pondra.runs, pondra.tasks, …): {then}"))),
        false => Ok(()),
    }
}

pub fn no_schema(s: &str) -> String {
    match s.eq_ignore_ascii_case(SYSTEM_SCHEMA) {
        true => "pondra is the system's schema: its tables (pondra.runs, pondra.tasks, …) are read-only".into(),
        false => format!("no schema {s} (CREATE SCHEMA {s})"),
    }
}
/// A stored view (`CREATE VIEW`): a query with a name, run where it is used.
pub fn query_key(v: &str) -> String { format!("q/{v}") }

#[derive(Serialize, Deserialize, Default)]
pub struct Schema {
    pub created_ms: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StoredView {
    pub sql: String,
    /// Made by CREATE EXTERNAL TABLE (a view of files): DROP TABLE drops it too.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub external: bool,
    /// A view's key facts, NOT ENFORCED (ADR-065 §2): its own, so a rename, a drop and a clone take them along.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub facts: Vec<crate::constraints::Constraint>,
}

/// Another lake, attached in SQL (`ATTACH 'dir' AS name`): every node attaches it.
pub fn attachment_key(n: &str) -> String { format!("a/{n}") }

#[derive(Serialize, Deserialize, Clone)]
pub struct Attachment {
    pub dir: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool, // (a lake on another server, read with a read-only key: ADR-058)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>, // (its server's URL, where its leader is asked for a pin)
}

/// What a schema, table or view may be called: letters, digits, `_` and `-`. A dot separates the
/// parts of a name; quotes, slashes and spaces would get in the way of paths and SQL.
pub fn check(part: &str) -> Result<()> {
    let ok = |c: char| c.is_alphanumeric() || c == '_' || c == '-';
    if !part.is_empty() && part.chars().all(ok) {
        return Ok(());
    }
    // (a table's name is its folder's name, and Windows refuses a quote or a slash in a file name: the
    // name to write is the same one with each such character as `_`)
    let fixed: String = part.chars().map(|c| if ok(c) { c } else { '_' }).collect();
    let hint = if fixed.is_empty() { String::new() } else { format!(": a table's name is its folder's, and Windows refuses \" and / in file names; name it {fixed}") };
    Err(crate::codes::coded("42602", format!("'{part}': names are letters, digits, _ and - (a dot separates schema and table){hint}")))
}

/// What a database may be called: `check`'s, and not a name starting with `_`. Those are the server's own: its `_pondra`
/// folder (and the workspace there, ADR-054) sits beside the databases, so a database's folder must not take one.
pub fn database_name(name: &str) -> Result<()> {
    check(name)?;
    match name.starts_with('_') {
        true => Err(crate::codes::coded("42602", format!("'{name}': names starting with _ are the server's own (its _pondra folder), so name the database without the _"))),
        false => Ok(()),
    }
}

/// This lake's own name in three-part names: its folder's (or prefix's) last part (`mylake` of
/// `D:\data\mylake` too), and `memory` for a lake held in this process's memory, as DuckDB names its own.
pub fn lake_name(lake: &Lake) -> String {
    name_of(&lake.url)
}

/// `lake_name` of a lake at `url`. A lake in memory is `memory://<id>`, whose id would show in every name and error.
fn name_of(url: &str) -> String {
    if crate::memory::in_memory(url) {
        return "memory".into();
    }
    url.trim_end_matches(['/', '\\']).rsplit(['/', '\\']).next().unwrap_or("lake").to_lowercase()
}

/// A table or view of a lake or one attached to it, as `pondra.tables`, the shell's `.tables` and
/// the console list it: read from the catalog alone, no query run.
pub struct Listed {
    pub lake: String,
    pub schema: String,
    pub name: String,
    /// `table`, `view`, `materialized view` (its `_final` table too) or `external table`.
    pub kind: &'static str,
    pub meta: Option<TableMeta>, // (a table's, as its users see it)
    pub sql: Option<String>,     // (a view's definition; a materialized view's query: for a caller who may see code, `views::code_for_caller`)
}

/// Every table and view of this lake and the lakes attached to it, lake by lake.
pub async fn listed(lake: &Lake) -> Result<Vec<Listed>> {
    let mut all = vec![];
    let attached: Vec<(String, Arc<Lake>)> = lake.attached.read().unwrap().clone();
    for (catalog, l) in std::iter::once((lake_name(lake), lake.arc())).chain(attached) {
        if catalog != lake_name(lake) && crate::users::across(&l, &catalog).await.is_err() {
            continue; // (another database that signs in on its own: not even its tables' names)
        }
        // (a materialized view of any kind: windows, sessions and joins keep more than its SQL)
        let materialized: std::collections::HashMap<String, String> = l.cat.scan::<Value>("v/", "v0").await?.into_iter().map(|(k, v)| (k[2..].to_string(), crate::views::shown(v["written"].as_str().or(v["sql"].as_str()).unwrap_or_default()))).collect();
        for (k, m) in l.cat.scan::<TableMeta>("t/", "t0").await? {
            let name = &k[2..];
            if crate::sys::hidden(name) {
                continue;
            }
            let (schema, table) = split(name);
            let found = materialized.get(name).or_else(|| materialized.get(name.trim_end_matches("_final"))).or_else(|| materialized.get(&crate::once::open(name)));
            let kind = if found.is_some() { "materialized view" } else { "table" };
            let sql = crate::views::code_for_caller(|| found.filter(|s| !s.is_empty()).cloned()).flatten();
            all.push(Listed { lake: catalog.clone(), schema: schema.into(), name: table.into(), kind, meta: Some(m.described()), sql });
        }
        for (k, v) in l.cat.scan::<StoredView>("q/", "q0").await? {
            let (schema, view) = split(&k[2..]);
            all.push(Listed { lake: catalog.clone(), schema: schema.into(), name: view.into(), kind: if v.external { "external table" } else { "view" }, meta: None, sql: crate::views::code_for_caller(|| crate::views::shown(&v.sql)) });
        }
    }
    if let Some(a) = crate::auth::limited() {
        let here = lake_name(lake);
        all.retain(|o| a.may("select", &if o.lake == here { join(&o.schema, &o.name) } else { format!("{}.{}", o.lake, join(&o.schema, &o.name)) })); // (a user's: what it may read, at every listing: S27)
    }
    Ok(all)
}

pub async fn schemas(lake: &Lake) -> Result<Vec<String>> {
    let mut out = vec![PUBLIC.to_string()];
    out.extend(lake.cat.scan::<Schema>("ns/", "ns0").await?.into_iter().map(|(k, _)| k[3..].to_string()));
    Ok(out)
}

pub async fn has_schema(lake: &Lake, s: &str) -> Result<bool> {
    Ok(s == PUBLIC || lake.cat.get::<Schema>(&schema_key(s)).await?.is_some())
}

/// Where a name points: this lake (None) or an attached one, and the name inside it. Names are as
/// SQL resolves them (`text::object`): unquoted parts already lower-case.
pub async fn resolve(lake: &Lake, name: &str) -> Result<(Option<Arc<Lake>>, String)> {
    let parts: Vec<&str> = name.split('.').collect();
    Ok(match parts[..] {
        [t] => (None, t.to_string()),
        [s, t] if has_schema(lake, s).await? => (None, join(s, t)),
        [l, t] => match attached(lake, l).await? {
            Some(other) => (Some(other), t.to_string()),
            None => bail!("{}", no_schema(l)),
        },
        [l, s, t] if l == lake_name(lake) => {
            ensure!(has_schema(lake, s).await?, "{}", no_schema(s));
            (None, join(s, t))
        }
        [l, s, t] => (Some(attached(lake, l).await?.with_context(|| format!("no lake {l}: this one is {}, and none is attached as {l}", lake_name(lake)))?), join(s, t)),
        _ => bail!("{name}: a name is table, schema.table or lake.schema.table"),
    })
}

/// The lake attached as `l`. One the catalog lists that this node hasn't attached yet (made a
/// moment ago on another node: `CREATE DATABASE`, `ATTACH`) is waited for, as the node attaches it
/// within a second (`sync`).
async fn attached(lake: &Lake, l: &str) -> Result<Option<Arc<Lake>>> {
    let here = || lake.attached.read().unwrap().iter().find(|(n, _)| n == l).map(|(_, o)| o.clone());
    if here().is_none() && lake.cat.get_raw(&attachment_key(l)).await?.is_some() {
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if here().is_some() {
                break;
            }
        }
    }
    Ok(here())
}

/// The database called `name`: this lake (its own name) or an attached one; None when there is
/// none. Attachments are read here, so a name the catalog lists but this node hasn't attached yet
/// is waited for, as `attached` does.
pub async fn database(lake: &Lake, name: &str) -> Result<Option<Arc<Lake>>> {
    if name == lake_name(lake) {
        return Ok(Some(lake.arc()));
    }
    attached(lake, name).await
}

/// A table of this lake by its name inside it, from a name as SQL wrote it (`text::object`):
/// None for another lake's. (A two-part name may yet be an attached lake's: no table has it here.)
pub fn local(lake: &Lake, name: &str) -> Option<String> { local_in(&lake_name(lake), name) }

/// A name for something new in this lake: its schema exists and its parts are fine. `public.t`
/// is `t`.
pub async fn new_name(lake: &Lake, name: &str) -> Result<String> {
    let (s, t) = split(name);
    check(s)?;
    check(t)?;
    ensure!(!t.contains('.'), "{name}: a name is table, schema.table or lake.schema.table");
    ensure!(has_schema(lake, s).await?, "{}", no_schema(s));
    Ok(join(s, t))
}

/// Sequences and indexes share names with tables and views (as in Postgres's `pg_class`): a table
/// or view can't take a name one of them has.
pub async fn unclaimed(lake: &Lake, name: &str) -> Result<()> {
    for (key, what) in [(crate::seq::key(name), "a sequence"), (crate::index::key(name), "an index")] {
        ensure!(lake.cat.get::<Value>(&key).await?.is_none(), "relation \"{name}\" already exists: {what} has the name");
    }
    Ok(())
}

// ---------------------------------------------------------------- statements

/// A statement that changes what the lake holds, not its rows: the leader carries it out.
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Ddl {
    CreateSchema { name: String, if_not_exists: bool },
    DropSchema { name: String, if_exists: bool, cascade: bool },
    // (made_by: only the table that job made, a failed CREATE TABLE … AS undoing itself; any other table under the name is left)
    DropTable { name: String, if_exists: bool, #[serde(default)] purge: bool, #[serde(default, skip_serializing_if = "Option::is_none")] made_by: Option<String> }, // (PURGE: not kept to be undropped)
    Undrop { name: String },                                                   // UNDROP TABLE (ADR-043)
    Clone { name: String, from: String },                                      // CREATE TABLE c CLONE t: t's files, none copied (ADR-043)
    CreateView { name: String, sql: String, replace: bool, #[serde(default)] facts: Option<Vec<crate::constraints::Constraint>> }, // (None: as before, or kept through OR REPLACE: `facts.rs`)
    CreateExternal { name: String, sql: String, replace: bool, if_not_exists: bool }, // DataFusion's CREATE EXTERNAL TABLE: a view of files (`ext::external`)
    CreateMaterialized { name: String, sql: String, options: std::collections::BTreeMap<String, String> }, // (`views::options`)
    DropView { name: String, if_exists: bool },
    Attach { name: String, dir: String, #[serde(default, skip_serializing_if = "std::ops::Not::not")] read_only: bool, #[serde(default, skip_serializing_if = "Option::is_none")] endpoint: Option<String> }, // (READ_ONLY and ENDPOINT: a lake on another server, ADR-058)
    Detach { name: String, if_exists: bool },
    CreateDatabase { name: String, if_not_exists: bool, dir: Option<String>, #[serde(default, skip_serializing_if = "Option::is_none")] clone: Option<crate::branch::CloneOf> }, // a new lake (beside this one unless `dir`), attached; a branch of another (ADR-047)
    Branch(crate::branch::Make),            // the new lake's leader: make it its base as that is now (ADR-047)
    Pin { lake: String, ms: Option<u64>, #[serde(default, skip_serializing_if = "std::ops::Not::not")] lease: bool }, // a base's leader: keep the files a branch reads (lease: renewed by a branch on another server, let go when it isn't: `branch::lapse`)
    Unpin { lake: String },
    Refresh { database: Option<String>, tables: Vec<String> }, // ALTER DATABASE b REFRESH t, …: b's leader (ADR-047)
    SetLocation { database: String, to: String }, // ALTER DATABASE m SET LOCATION 'dir': a lake in memory moved there (memory.rs)
    BackupDatabase { database: Option<String>, to: String, #[serde(default, skip_serializing_if = "Option::is_none")] retention_days: Option<u64> }, // BACKUP DATABASE [b] TO 'url': run on the node the statement reaches, as its owner (ADR-070, `backup.rs`)
    RestoreDatabase { name: String, from: String, #[serde(default, skip_serializing_if = "Option::is_none")] dir: Option<String>, #[serde(default, skip_serializing_if = "Option::is_none")] at: Option<u64> }, // RESTORE DATABASE b FROM 'url': a new lake, attached
    Backup(crate::backup::Take), // the backup's (or the restored lake's) leader: a run
    Protect { database: String, on: bool, #[serde(default)] by: String }, // ALTER DATABASE b SET (protected = …): b's leader (ADR-058, `protect.rs`)
    // (the wire op keeps the old word: a node of the last release sends it to the leader)
    #[serde(rename = "deploy")]
    Apply { claim: bool, after: u64, record: Vec<u8> }, // an apply's entry: its claim, its record, a migration done (`apply::keep`, ADR-047 §4)
    DropDatabase { name: String, if_exists: bool }, // a folder of databases' (`dbserver.rs`): its node stopped, its folder deleted (ADR-030)
    AlterColumn { table: String, column: String, change: Change }, // ALTER TABLE … RENAME/DROP/ALTER COLUMN (ADR-022)
    RenameTable { name: String, to: String }, // ALTER TABLE | VIEW … RENAME TO (ADR-030)
    Swap { name: String, with: String }, // ALTER TABLE … SWAP WITH … (write-audit-publish)
    ReplaceTable { name: String, staged: String }, // CREATE OR REPLACE TABLE: the staged table takes name's place in one commit (`replace::replace`, audit M3)
    CreateRoutine { name: String, routine: crate::expander::Routine, replace: bool }, // CREATE MACRO, CREATE PROCEDURE (ADR-023)
    DropRoutine { name: String, if_exists: bool },
    CreateSecret { name: String, params: std::collections::BTreeMap<String, String>, replace: bool, if_not_exists: bool }, // (ADR-026: `ext.rs`)
    AttachOutside { name: String, url: String, kind: String, options: std::collections::BTreeMap<String, String> }, // another engine's tables (`ext.rs`)
    DropSecret { name: String, if_exists: bool },
    FetchExtension { name: String, from: String, version: Option<String> }, // INSTALL name FROM 'file or URL': the node that got it reads the file, then sends the record (`extension.rs`)
    InstallExtension { name: String, record: crate::extension::Installed }, // the record of an extension, kept by the leader (ADR-075 §6: `extension.rs`)
    UninstallExtension { name: String, if_exists: bool }, // UNINSTALL name: the leader drops its record (`extension.rs`)
    CreateTask { name: String, task: crate::runs::Task, replace: bool }, // CREATE TASK … SCHEDULE … AS … (ADR-027: `runs.rs`)
    DropTask { name: String, if_exists: bool },
    ExecuteTask { name: String, params: std::collections::HashMap<String, Value> }, // EXECUTE TASK name (…): a tick claimed now (ADR-045)
    AlterTask { name: String, suspended: bool },                  // ALTER TASK name SUSPEND | RESUME
    RunLog, // the run log's table (`pondra.runs`), made when a node first has a line for it
    AuditLog, // the audit log's (`pondra.audit`), the same way (`audit.rs`)
    HistoryLog, // the query history's (`pondra.history`, `history.rs`)
    Users(crate::users::Change), // CREATE USER and ROLE, GRANT, REVOKE, CREATE TOKEN (ADR-035: `users.rs`)
    Shares(crate::shares::Change), // CREATE SHARE and RECIPIENT, GRANT SELECT ON SHARE (ADR-046: `shares.rs`)
    Unless { name: String, kind: String, then: Box<Ddl> }, // CREATE … IF NOT EXISTS: nothing if a `kind` ("relation", "routine", "task") of that name is there
    Replacing { name: String, then: Box<Ddl> },              // CREATE OR REPLACE MATERIALIZED VIEW: the old one dropped first (refused while another follows it)
    DetachView { name: String },                             // ALTER MATERIALIZED VIEW v DETACH: its rows stop following, and stay a table
    ViewFacts { view: String, #[serde(default)] if_exists: bool, change: crate::constraints::Change }, // ALTER [MATERIALIZED] VIEW [IF EXISTS] v ADD | DROP CONSTRAINT: a view's key facts (ADR-065 §2, `facts.rs`)
    Object(crate::objects::Op),                              // the registry's: COMMENT ON, CREATE OR ALTER TABLE (`objects.rs`)
    Sequence(crate::seq::Change),                            // CREATE, ALTER, DROP SEQUENCE (`seq.rs`)
    Index(crate::index::Change),                             // CREATE, ALTER, DROP INDEX (`index.rs`)
    Type(crate::types::Change),                              // CREATE TYPE … AS ENUM, ALTER TYPE, DROP TYPE (`types.rs`)
    Constraint { table: String, change: crate::constraints::Change }, // ALTER TABLE … ADD | DROP CONSTRAINT (`constraints.rs`)
}

/// What `ALTER TABLE` does to a column: rename it, drop it, or widen its type (a SQL type).
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Rename(String),
    Drop { if_exists: bool },
    Type(String),
    Default(Option<String>), // SET DEFAULT expr (Some), DROP DEFAULT (None): `defaults::alter`
    NotNull(bool),           // SET NOT NULL (true), DROP NOT NULL (false): `defaults::alter`
}

/// Leader: carry one out (under the lake's lock). Comments follow what it renames, and go with
/// what it drops (`objects::follow`).
pub async fn apply(lake: &Lake, d: Ddl) -> Result<Value> {
    let moves = crate::objects::moves(&d);
    let out = carry_out(lake, d).await?;
    if moves {
        crate::objects::follow(lake, &out).await?;
        crate::index::follow(lake, &out).await?;
    }
    Ok(out)
}

async fn carry_out(lake: &Lake, d: Ddl) -> Result<Value> {
    // (this lake's own three-part names, as dbt writes them: `"lake"."schema"."t"` is `schema.t`)
    let here = |n: String| match n.split('.').collect::<Vec<_>>()[..] {
        [l, s, t] if l == lake_name(lake) => join(s, t),
        [s, t] => join(s, t), // (`public.t` is `t`, invariant 49: `DROP TABLE public.t` found no table)
        _ => n,
    };
    let d = match d {
        Ddl::DropTable { name, if_exists, purge, made_by } => Ddl::DropTable { name: here(name), if_exists, purge, made_by },
        Ddl::Undrop { name } => Ddl::Undrop { name: here(name) },
        Ddl::Clone { name, from } => Ddl::Clone { name: here(name), from: here(from) },
        Ddl::CreateView { name, sql, replace, facts } => Ddl::CreateView { name: here(name), sql, replace, facts },
        Ddl::CreateExternal { name, sql, replace, if_not_exists } => Ddl::CreateExternal { name: here(name), sql, replace, if_not_exists },
        Ddl::CreateMaterialized { name, sql, options } => Ddl::CreateMaterialized { name: here(name), sql, options },
        Ddl::DropView { name, if_exists } => Ddl::DropView { name: here(name), if_exists },
        Ddl::RenameTable { name, to } => Ddl::RenameTable { name: here(name), to },
        Ddl::Swap { name, with } => Ddl::Swap { name: here(name), with: here(with) },
        Ddl::ReplaceTable { name, staged } => Ddl::ReplaceTable { name: here(name), staged: here(staged) },
        Ddl::CreateRoutine { name, routine, replace } => Ddl::CreateRoutine { name: here(name), routine, replace },
        Ddl::DropRoutine { name, if_exists } => Ddl::DropRoutine { name: here(name), if_exists },
        Ddl::Unless { name, kind, then } => Ddl::Unless { name: here(name), kind, then },
        Ddl::Replacing { name, then } => Ddl::Replacing { name: here(name), then },
        Ddl::DetachView { name } => Ddl::DetachView { name: here(name) },
        Ddl::ViewFacts { view, if_exists, change } => Ddl::ViewFacts { view: here(view), if_exists, change },
        d => d,
    };
    match d {
        Ddl::Unless { name, kind, then } => {
            let name = new_name(lake, &name).await?;
            let there = match kind.as_str() {
                "routine" => lake.cat.get::<Value>(&crate::routines::key(&name)).await?.is_some(),
                "task" => lake.cat.get::<Value>(&crate::runs::task_key(&name)).await?.is_some(),
                _ => lake.cat.get::<StoredView>(&query_key(&name)).await?.is_some() || lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_some(), // (a view, a materialized view or a table)
            };
            if there {
                return Ok(j!({"name": name, "exists": true}));
            }
            Box::pin(apply(lake, *then)).await
        }
        Ddl::Replacing { name, then } => {
            let here = new_name(lake, &name).await?;
            let once = crate::views::view_key(&crate::once::open(&here)); // (EMIT FINAL's)
            if lake.cat.get::<Value>(&crate::views::view_key(&here)).await?.is_some() || lake.cat.get::<Value>(&once).await?.is_some() || lake.cat.get::<Value>(&crate::feeds::feed_key(&here)).await?.is_some() {
                drop_view(lake, &name, true).await?; // (refused, saying so, while another view follows it)
            }
            Box::pin(apply(lake, *then)).await
        }
        Ddl::DetachView { name } => detach_view(lake, &new_name(lake, &name).await?).await,
        Ddl::Object(op) => crate::objects::apply(lake, op).await,
        Ddl::Sequence(c) => crate::seq::apply(lake, c).await,
        Ddl::Index(c) => crate::index::apply(lake, c).await,
        Ddl::Type(c) => crate::types::apply(lake, c).await,
        Ddl::Constraint { table, change } => {
            let (other, table) = resolve(lake, &table).await?;
            ensure!(other.is_none(), "{table} is an attached lake's: change its constraints from a node of that lake");
            crate::facts::not_a_view(lake, &table).await?; // (a view's name here: ALTER VIEW says how, 42809)
            crate::constraints::apply(lake, &table, change).await
        }
        Ddl::ViewFacts { view, if_exists, change } => {
            let (other, view) = resolve(lake, &view).await?;
            ensure!(other.is_none(), "{view} is an attached lake's: change its facts from a node of that lake");
            crate::facts::apply(lake, &view, if_exists, change).await
        }
        Ddl::CreateSchema { name, if_not_exists } => {
            check(&name)?;
            not_system(&name, "name the schema another way")?;
            if has_schema(lake, &name).await? {
                ensure!(if_not_exists, "schema {name} already exists");
                return Ok(j!({"schema": name, "unchanged": true}));
            }
            lake.cat.commit(vec![(schema_key(&name), json(&Schema { created_ms: crate::log::now_ms() }))], &[]).await?;
            Ok(j!({"schema": name}))
        }
        Ddl::DropSchema { name, if_exists, cascade } => {
            ensure!(name != PUBLIC, "the public schema stays");
            if !has_schema(lake, &name).await? {
                ensure!(if_exists, "no schema {name}");
                return Ok(j!({"schema": name, "dropped": false}));
            }
            let inside = |k: &str, p: usize| split(&k[p..]).0 == name && k[p..].contains('.');
            let views: Vec<String> = lake.cat.scan::<Value>("v/", "v0").await?.into_iter().chain(lake.cat.scan::<Value>("q/", "q0").await?)
                .filter(|(k, _)| inside(k, 2)).map(|(k, _)| k[2..].to_string()).collect();
            let routines: Vec<String> = lake.cat.scan::<Value>("r/", "r0").await?.into_iter().filter(|(k, _)| inside(k, 2)).map(|(k, _)| k[2..].to_string()).collect();
            let tables: Vec<String> = lake.cat.scan::<TableMeta>("t/", "t0").await?.into_iter().filter(|(k, _)| inside(k, 2) && !crate::sys::hidden(k)).map(|(k, _)| k[2..].to_string()).collect();
            ensure!(cascade || (views.is_empty() && tables.is_empty() && routines.is_empty()), "schema {name} isn't empty ({}): drop them first, or DROP SCHEMA {name} CASCADE", [&views[..], &tables[..], &routines[..]].concat().join(", "));
            for r in &routines {
                crate::routines::drop(lake, r, true).await?;
            }
            for v in &views {
                drop_view(lake, v, true).await?;
            }
            for t in tables.iter().filter(|t| !views.iter().any(|v| *t == v || **t == format!("{v}_final"))) {
                drop_table(lake, t, true, false, None).await?;
            }
            lake.cat.commit(vec![], &[schema_key(&name)]).await?;
            Ok(j!({"schema": name, "dropped": true}))
        }
        Ddl::DropTable { name, if_exists, purge, made_by } => drop_table(lake, &name, if_exists, purge, made_by.as_deref()).await,
        Ddl::Undrop { name } => undrop(lake, &new_name(lake, &name).await?).await,
        Ddl::Clone { name, from } => clone(lake, &new_name(lake, &name).await?, &from).await,
        Ddl::CreateView { name, sql, replace, facts } => create_view(lake, &name, sql, replace, false, facts).await,
        Ddl::CreateExternal { name, sql, replace, if_not_exists } => {
            let name = new_name(lake, &name).await?;
            let taken = lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_some() || lake.cat.get::<StoredView>(&query_key(&name)).await?.is_some()
                || lake.cat.get::<Value>(&crate::views::view_key(&name)).await?.is_some();
            if taken && if_not_exists {
                return Ok(j!({"table": name, "unchanged": true}));
            }
            ensure!(replace || !taken, "{name} already exists (CREATE OR REPLACE EXTERNAL TABLE, or IF NOT EXISTS)");
            create_view(lake, &name, sql, replace, true, None).await
        }
        Ddl::CreateMaterialized { name, sql, mut options } => {
            let name = new_name(lake, &name).await?;
            let facts = crate::facts::taken(&mut options)?; // (ALTER VIEW's facts in its list: `facts.rs`)
            ensure!(lake.cat.get::<StoredView>(&query_key(&name)).await?.is_none(), "{name} is a (stored) view");
            let outside = crate::ext_names::names(&sql);
            if let [topic] = &outside[..] {
                if crate::ext_names::spec(topic).is_some_and(|s| s.format == "kafka") {
                    crate::feeds::create(lake, &name, &sql, topic, &options).await?; // (a topic's records, as they arrive)
                    crate::views::forget(lake);
                    if let Err(e) = crate::facts::materialized(lake, &name, facts).await {
                        drop_view(lake, &name, true).await?; // (refused: nothing made)
                        return Err(e);
                    }
                    return Ok(j!({"view": name, "feed": true}));
                }
            }
            ensure!(!crate::seq::calls(&sql), "a materialized view can't call nextval or setval: its rows are worked out again as its tables change, and would be numbered again (number the rows where they are written: an identity column, or DEFAULT nextval('s'))");
            ensure!(outside.is_empty(), "a materialized view follows the rows its tables take in, and files outside the lake take none: read them into a table (CREATE TABLE … AS, INSERT … SELECT) and follow that, or make a stored view (CREATE VIEW)");
            crate::views::create(lake, &name, &sql, crate::views::options(&options)?).await?;
            crate::views::forget(lake); // (the sequencer holds flushes to it from its next commit)
            if let Err(e) = crate::facts::materialized(lake, &name, facts).await {
                drop_view(lake, &name, true).await?; // (refused: nothing made)
                return Err(e);
            }
            let mut out = j!({"view": name, "materialized": true});
            if let Some(n) = lake.cat.get::<TableMeta>(&table_key(&name)).await?.and_then(|m| crate::delta::left_out(&name, &m)) {
                out["notice"] = n.into();
            }
            Ok(out)
        }
        Ddl::DropView { name, if_exists } => drop_view(lake, &name, if_exists).await,
        Ddl::Attach { name, dir, read_only, endpoint } => {
            attachable(lake, &name, false).await?;
            ensure!(endpoint.is_none() || read_only, "ENDPOINT goes with READ_ONLY: a database on another server is read with its bucket's read-only key and asked through its leader's URL");
            ensure!(endpoint.as_deref().is_none_or(|e| e.starts_with("http://") || e.starts_with("https://")), "ENDPOINT is its server's URL: https://…");
            if let Some(a) = lake.cat.get::<Attachment>(&attachment_key(&name)).await? {
                ensure!(a.dir == full(&dir)? && a.read_only == read_only && a.endpoint == endpoint, "{name} is attached already, to {}{}", a.dir, if a.read_only { " (READ_ONLY)" } else { "" });
                return Ok(j!({"attached": name, "dir": a.dir, "unchanged": true}));
            }
            ensure!(!lake.attached.read().unwrap().iter().any(|(n, _)| *n == name), "{name} is attached already (--attach, or found beside this lake by the shell)");
            ensure!(full(&dir)? != lake.url, "{dir} is this lake");
            let (dir, created) = match read_only {
                true => {
                    let dir = full(&dir)?;
                    crate::ext::reach(lake, &dir, true, endpoint.as_deref()).await?; // (its key and its server, before it is opened)
                    ensure!(has_catalog(&dir).await?, "{dir} holds no lake: a READ_ONLY attachment reads one that is there");
                    (dir, false)
                }
                false => lake_dir(&dir).await?,
            };
            lake.cat.commit(vec![(attachment_key(&name), json(&Attachment { dir: dir.clone(), read_only, endpoint }))], &[]).await?;
            Ok(match (created, read_only) {
                (true, _) => j!({"attached": name, "dir": dir, "created": true}),
                (false, true) => j!({"attached": name, "dir": dir, "read_only": true}),
                (false, false) => j!({"attached": name, "dir": dir}),
            })
        }
        Ddl::CreateDatabase { name, if_not_exists, dir, clone: Some(of) } => crate::branch::create(lake, &name, if_not_exists, dir, of).await,
        Ddl::Branch(m) => crate::branch::make(lake, m).await,
        Ddl::Pin { lake: branch, ms, lease } => crate::branch::pin(lake, &branch, ms, lease).await,
        Ddl::Unpin { lake: branch } => crate::branch::unpin(lake, &branch).await,
        Ddl::Refresh { .. } => bail!("ALTER DATABASE … REFRESH is done by its database's leader (write::handle)"),
        Ddl::SetLocation { .. } => bail!("ALTER DATABASE … SET LOCATION is done by its database's leader (memory::set_location)"),
        Ddl::BackupDatabase { .. } | Ddl::RestoreDatabase { .. } => bail!("BACKUP and RESTORE DATABASE are run on the node the statement reaches (backup::statement), not recorded as DDL"),
        Ddl::Backup(t) => crate::backup::take(lake, t).await,
        Ddl::Protect { database, on, by } => crate::protect::set(lake, &database, on, &by).await,
        Ddl::Apply { claim, after, record } => crate::apply::keep(lake, claim, after, &record).await,
        Ddl::CreateDatabase { name, if_not_exists, dir, clone: None } => {
            database_name(&name)?;
            let dir = dir.unwrap_or_else(|| beside(&lake.url, &name));
            if has_catalog(&full(&dir)?).await? {
                ensure!(if_not_exists, "a lake is at {dir} already: ATTACH '{dir}' AS {name} (or CREATE DATABASE IF NOT EXISTS {name})");
            }
            Box::pin(apply(lake, Ddl::Attach { name, dir, read_only: false, endpoint: None })).await
        }
        Ddl::DropDatabase { name, if_exists } => {
            ensure!(name != lake_name(lake), "this is database {name}: drop it from another one");
            // (a branch's pins in its bases go with it; a base goes only after its branches: ADR-047)
            let target = lake.attached.read().unwrap().iter().find(|(n, _)| *n == name).map(|(_, l)| l.clone());
            let (mut bases, mut branches) = (None, vec![]);
            if let Some(t) = &target {
                // (as the lake is now, not as this node last saw it: a branch made or dropped just now counts)
                let now = Lake::open(&t.url, false, false).await?;
                bases = now.cat.get::<crate::branch::Bases>(crate::branch::BASES).await?;
                branches = now.cat.scan::<crate::branch::Pin>("pn/", "pn0").await?.into_iter().map(|(_, p)| p.lake).collect();
            }
            ensure!(branches.is_empty(), "{name} has branches ({}): drop them first", branches.join(", "));
            if target.is_none() && if_exists && !crate::env::is_set("PONDRA_SERVER_URL") {
                return Ok(j!({"database": name, "dropped": false, "notice": format!("database \"{name}\" does not exist, skipping")}));
            }
            let dropped = match (crate::env::get("PONDRA_SERVER_URL"), &bases, &target) {
                (Some(server), ..) => {
                    let r = crate::dial::call(&server, crate::dial::Call::delete(format!("/databases/{name}?if_exists={if_exists}"))).await?;
                    let (ok, text) = (r.ok(), r.text().await?);
                    ensure!(ok, "{text}");
                    serde_json::from_str(&text)?
                }
                (None, Some(_), Some(t)) => {
                    // A branch: its own objects alone are its (its bases' files are theirs).
                    delete_lake(&t.url).await?;
                    if lake.cat.get::<Attachment>(&attachment_key(&name)).await?.is_some() {
                        lake.cat.commit(vec![], &[attachment_key(&name)]).await?;
                    }
                    j!({"database": name, "dropped": true})
                }
                _ => bail!("DROP DATABASE drops a branch (CREATE DATABASE … CLONE) or one of the databases `pondra serve <folder>` serves, folder and all; on a node serving one lake, DETACH {name} (its folder stays)"),
            };
            lake.attached.write().unwrap().retain(|(n, _)| *n != name); // (its tables are gone from here too)
            if let Some(b) = bases {
                crate::branch::release(lake, &b).await;
            }
            Ok(dropped)
        }
        Ddl::AlterColumn { table, column, change } => alter_column(lake, &table, &column, change).await,
        Ddl::RenameTable { name, to } => rename(lake, &name, &to).await,
        Ddl::Swap { name, with } => swap(lake, &name, &with).await,
        Ddl::ReplaceTable { name, staged } => crate::replace::replace(lake, &name, &staged).await,
        Ddl::CreateRoutine { name, routine, replace } => crate::routines::create(lake, &name, routine, replace).await,
        Ddl::DropRoutine { name, if_exists } => crate::routines::drop(lake, &name, if_exists).await,
        Ddl::CreateSecret { name, params, replace, if_not_exists } => crate::ext::create(lake, &name, params, replace, if_not_exists).await,
        Ddl::DropSecret { name, if_exists } => crate::ext::drop(lake, &name, if_exists).await,
        Ddl::InstallExtension { name, record } => crate::extension::install(lake, &name, record).await,
        Ddl::UninstallExtension { name, if_exists } => crate::extension::uninstall(lake, &name, if_exists).await,
        Ddl::FetchExtension { name, .. } => anyhow::bail!("INSTALL {name} FROM is read by the node the statement reached, not the leader"),
        Ddl::CreateTask { name, task, replace } => crate::runs::create_task(lake, &name, task, replace).await,
        Ddl::DropTask { name, if_exists } => crate::runs::drop_task(lake, &name, if_exists).await,
        Ddl::ExecuteTask { name, params } => crate::runs::execute_task(lake, &name, params).await,
        Ddl::AlterTask { name, suspended } => crate::runs::alter_task(lake, &name, suspended).await,
        Ddl::RunLog => crate::runs::create_log(lake).await,
        Ddl::AuditLog => crate::audit::create_log(lake).await,
        Ddl::HistoryLog => crate::history::create_log(lake).await,
        Ddl::Users(c) => crate::users::apply(lake, c).await,
        Ddl::Shares(c) => crate::shares::apply(lake, c).await,
        Ddl::AttachOutside { name, url, kind, options } => {
            not_system(&name, "attach it under another name")?;
            ensure!(!has_schema(lake, &name).await?, "a schema here is called {name}: attach under another name");
            ensure!(lake.cat.get::<Attachment>(&attachment_key(&name)).await?.is_none(), "{name} is an attached lake: attach under another name");
            crate::ext::attach(lake, &name, &url, &kind, options).await
        }
        Ddl::Detach { name, if_exists } => {
            if crate::ext::detach(lake, &name).await? {
                return Ok(j!({"detached": name})); // (another engine's tables)
            }
            if lake.cat.get::<Attachment>(&attachment_key(&name)).await?.is_none() {
                let flag = lake.attached.read().unwrap().iter().any(|(n, _)| *n == name);
                ensure!(if_exists && !flag, "{name} {}", if flag { "was attached when the node started (--attach, or found beside this lake by the shell): it stays" } else { "isn't attached" });
                return Ok(j!({"detached": name, "unchanged": true}));
            }
            lake.cat.commit(vec![], &[attachment_key(&name)]).await?;
            Ok(j!({"detached": name}))
        }
    }
}

/// Whether a database can be attached here as `name`: a name of its own (not this lake's, a
/// schema's or another engine's tables'), and (`fresh`: one about to be made) no database's.
pub async fn attachable(lake: &Lake, name: &str, fresh: bool) -> Result<()> {
    database_name(name)?;
    not_system(name, "give the database another name")?;
    ensure!(name != lake_name(lake), "this lake is called {name}: attach the other under another name");
    ensure!(!has_schema(lake, name).await?, "a schema here is called {name}: attach the other under another name");
    ensure!(!crate::ext::is_attached(lake, name).await?, "{name} is attached already, as another engine's tables");
    if fresh {
        let taken = lake.cat.get::<Attachment>(&attachment_key(name)).await?.is_some() || lake.attached.read().unwrap().iter().any(|(n, _)| n == name);
        ensure!(!taken, "a database here is called {name} already: give the new one another name");
    }
    Ok(())
}

/// Where a lake to attach is, as its nodes open it: a bucket's URL, or a folder's full path (a
/// relative one would mean something else on every machine); and whether it was made just now.
/// Nothing there yet (no catalog): a new, empty lake is made there, as DuckDB's ATTACH makes a
/// new database file.
async fn lake_dir(dir: &str) -> Result<(String, bool)> {
    let dir = &crate::memory::place(dir); // (`:memory:`: a new lake in memory, which only opening makes: `node::lead_once`)
    if !dir.contains("://") {
        std::fs::create_dir_all(dir).with_context(|| format!("no folder {dir}"))?;
    }
    let dir = full(dir)?;
    if has_catalog(&dir).await? {
        return Ok((dir, false));
    }
    crate::node::lead_once(&dir).await?; // (whoever leads a lake first makes it)
    ensure!(has_catalog(&dir).await?, "couldn't make a lake at {dir}");
    Ok((dir, true))
}

/// A lake's place as its nodes name it: a bucket's URL as it is, a folder's full path.
pub fn full(dir: &str) -> Result<String> {
    Ok(match dir.contains("://") {
        true => dir.trim_end_matches('/').to_string(),
        false => match std::fs::canonicalize(dir) {
            Ok(p) => p.to_string_lossy().trim_start_matches(r"\\?\").to_string(), // (as `store::open_store` has it)
            Err(_) => {
                // Not there yet: absolute, with `.` and `..` taken out (object stores refuse them).
                let mut p = std::path::PathBuf::new();
                for c in std::path::absolute(dir)?.components() {
                    match c {
                        std::path::Component::ParentDir => drop(p.pop()),
                        std::path::Component::CurDir => {}
                        c => p.push(c),
                    }
                }
                p.to_string_lossy().to_string()
            }
        },
    })
}

/// Delete a lake no process leads (`DROP DATABASE`): its folder, or every object under its prefix.
pub async fn delete_lake(dir: &str) -> Result<()> {
    let store = crate::store::open_store(dir)?.1;
    if let Some(t) = crate::terms::latest(&store).await? {
        ensure!(!crate::terms::alive(&store, &t).await, "another process leads the lake at {dir} ({}): stop it first", if t.addr.is_empty() { "a pondra sql" } else { &t.addr });
    }
    match dir.contains("://") {
        false => std::fs::remove_dir_all(dir).with_context(|| format!("deleting {dir}"))?,
        true => {
            use futures::{StreamExt, TryStreamExt};
            let listed = store.list(None).map_ok(|o| o.location).boxed();
            store.delete_stream(listed).try_collect::<Vec<_>>().await.with_context(|| format!("deleting {dir}"))?;
        }
    }
    Ok(())
}

pub async fn has_catalog(dir: &str) -> Result<bool> {
    if (!dir.contains("://") && !std::path::Path::new(dir).exists()) || (crate::memory::in_memory(dir) && !crate::memory::memory_held(dir)) {
        return Ok(false); // (asking a lake in memory that nothing here opened would make an empty one)
    }
    let store = crate::store::open_store(dir)?.1;
    Ok(futures::StreamExt::next(&mut store.list(Some(&object_store::path::Path::from("catalog")))).await.transpose()?.is_some())
}

/// The lakes in a folder, or under a bucket's prefix: (name, where), each a subfolder that holds
/// one, by name (ADR-030, ADR-032: a folder of databases, the shell's lakes side by side).
pub async fn lakes_in(folder: &str) -> Result<Vec<(String, String)>> {
    let folder = folder.trim_end_matches('/');
    let names: Vec<String> = match folder.contains("://") {
        false => std::fs::read_dir(folder).into_iter().flatten().flatten().filter(|e| e.path().join("catalog").is_dir())
            .filter_map(|e| e.file_name().to_str().map(str::to_string)).collect(),
        true => {
            let store = crate::store::open_store(folder)?.1;
            let listed = store.list_with_delimiter(None).await?;
            let mut out = vec![];
            for p in listed.common_prefixes {
                let Some(name) = p.filename().map(str::to_string) else { continue };
                if futures::StreamExt::next(&mut store.list(Some(&p.clone().join("catalog")))).await.transpose()?.is_some() {
                    out.push(name);
                }
            }
            out
        }
    };
    // (a name starting `_` is refused for a new database only: one made before that rule is still listed, and attached
    // at start, with a line saying so: `attach_found`)
    let mut out: Vec<(String, String)> = names.into_iter().filter(|n| check(n).is_ok()).map(|n| (n.clone(), format!("{folder}/{n}"))).collect();
    out.sort();
    Ok(out)
}

/// Lake `name` in the folder (or prefix) this lake's is in.
pub fn beside(url: &str, name: &str) -> String {
    match url.contains("://") {
        true => format!("{}/{name}", url.trim_end_matches('/').rsplit_once('/').map_or(url, |(p, _)| p)),
        false => std::path::Path::new(url).parent().unwrap_or(std::path::Path::new(".")).join(name).to_string_lossy().to_string(),
    }
}

/// Attach lake `dir` as `name` here, and return it. A node (`follow`) keeps it fresh from its catalog, until it
/// is detached; `live` opens it to follow its leader's commit stream too (`--attach`: for good, `node::attach`).
pub async fn attach(home: &Lake, name: &str, dir: &str, follow: bool, live: bool) -> Result<Arc<Lake>> {
    let other = Lake::open(dir, false, live).await?;
    if follow {
        let (home, weak) = (Arc::downgrade(&home.arc()), Arc::downgrade(&other));
        crate::panics::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                let (Some(home), Some(other)) = (home.upgrade(), weak.upgrade()) else { return }; // (detached)
                if !home.attached.read().unwrap().iter().any(|(_, o)| Arc::ptr_eq(o, &other)) {
                    return;
                }
                if let Err(e) = other.refresh().await {
                    eprintln!("background job failed: {e:#}");
                }
            }
        });
    }
    home.attach(name, other.clone())?;
    Ok(other)
}

/// The lakes in `folder` (its subfolders that hold one), attached under their folder names for as
/// long as this node runs: the shell's own folder, so the lakes side by side are its databases, as
/// a database server's are (ADR-024). Nothing is written to the catalog. Left out: this lake, a
/// name the catalog attaches (that ATTACH says where), a schema's name here, and a lake that won't
/// open (said on the log). The names attached.
pub async fn attach_found(home: &Lake, folder: &str) -> Vec<String> {
    let lakes = lakes_in(folder).await.unwrap_or_else(|e| {
        eprintln!("listing the lakes in {folder}: {e:#}");
        vec![]
    });
    let mut found = vec![];
    for (name, dir) in lakes {
        let (name, Ok(dir)) = (name.to_lowercase(), full(&dir)) else { continue };
        let taken = dir == home.url || check(&name).is_err() || name == lake_name(home)
            || home.attached.read().unwrap().iter().any(|(n, _)| *n == name)
            || !matches!(home.cat.get::<Attachment>(&attachment_key(&name)).await, Ok(None))
            || has_schema(home, &name).await.unwrap_or(true);
        if taken || !has_catalog(&dir).await.unwrap_or(false) {
            continue;
        }
        match attach(home, &name, &dir, true, false).await {
            Ok(_) => {
                if name.starts_with('_') {
                    eprintln!("database {name} in {folder} starts with _, a name a new database may not take (database_name): it was made before that rule, and stays attached");
                }
                found.push(name)
            }
            Err(e) => eprintln!("attaching {name} ({dir}), found in {folder}: {e:#}"),
        }
    }
    found
}

crate::per_node!(
    /// The lakes this process attached because the catalog said so, per home lake.
    static FROM_SQL: std::sync::Mutex<Vec<(String, String)>> = std::sync::Mutex::new(Vec::new()));

/// Attach here the lakes the catalog lists (`ATTACH`), and let go of those it no longer does
/// (`DETACH`). Lakes attached with `--attach` stay. One that can't be opened is skipped (and
/// tried again next time).
pub async fn sync(lake: &Lake, follow: bool) -> Result<()> {
    let listed = lake.cat.scan::<Attachment>("a/", "a0").await?;
    for (key, a) in &listed {
        let name = &key[2..];
        if lake.attached.read().unwrap().iter().any(|(n, _)| n == name) {
            continue;
        }
        if a.read_only {
            if let Err(e) = crate::ext::reach(lake, &a.dir, true, a.endpoint.as_deref()).await {
                eprintln!("attaching {name} ({}): {e:#}", a.dir);
                continue;
            }
        }
        match attach(lake, name, &a.dir, follow, false).await {
            Ok(_) => FROM_SQL.lock().unwrap().push((lake.url.clone(), name.to_string())),
            Err(e) => eprintln!("attaching {name} ({}): {e:#}", a.dir),
        }
    }
    let gone: Vec<String> = FROM_SQL.lock().unwrap().iter().filter(|(u, n)| *u == lake.url && !listed.iter().any(|(k, _)| &k[2..] == n)).map(|(_, n)| n.clone()).collect();
    if !gone.is_empty() {
        lake.attached.write().unwrap().retain(|(n, _)| !gone.contains(n));
        FROM_SQL.lock().unwrap().retain(|(u, n)| !(*u == lake.url && gone.contains(n)));
    }
    Ok(())
}

/// After this node sent an `ATTACH` or `DETACH`: once its catalog shows it, as it does here. After
/// `CREATE MATERIALIZED VIEW`: once it is filled from the rows already there (`views::fill_all`).
pub async fn settle(lake: &Lake, d: &Ddl) -> Result<()> {
    match d {
        // (CREATE OR REPLACE makes what it says, so it is waited for as when made plainly; IF NOT
        // EXISTS a materialized view only: one there already was filled when it was made, and a
        // routine there already may differ from the one asked for)
        Ddl::Replacing { then, .. } => return Box::pin(settle(lake, then)).await,
        Ddl::Unless { then, .. } if matches!(**then, Ddl::CreateMaterialized { .. }) => return Box::pin(settle(lake, then)).await,
        _ => {}
    }
    if let Ddl::CreateMaterialized { name, .. } = d {
        let name = local(lake, name).unwrap_or_default();
        let open = crate::once::open(&name);
        let name = if lake.cat.get::<crate::views::View>(&crate::views::view_key(&open)).await?.is_some() { open } else { name }; // (EMIT FINAL's)
        let (filled, worked) = (crate::store::producer_key(&format!("fill:{name}")), crate::store::producer_key(&crate::rerun::producer(&name)));
        for _ in 0..12_000 {
            let view = lake.cat.get::<crate::views::View>(&crate::views::view_key(&name)).await?;
            let Some(view) = view else { break };
            let ready = match (&view.fill, &view.rerun) {
                (Some(_), _) => &filled,
                (None, Some(_)) => &worked, // (kept by key: its first run works every group out)
                (None, None) => break,
            };
            if lake.cat.get::<u64>(ready).await?.is_some() {
                break;
            }
            if let Some(e) = view.rerun.as_ref().and_then(|_| crate::rerun::failing(lake, &name, crate::views::identity(&view))) {
                bail!("{name} is made, but its first run fails: {e}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        return Ok(());
    }
    if let Ddl::AlterColumn { table, column, change } = d {
        // (a write to this node right after uses the new names: it must know them)
        let key = table_key(&local(lake, table).unwrap_or_default());
        for _ in 0..200 {
            let Some(m) = lake.cat.get::<TableMeta>(&key).await? else { break };
            let done = match change {
                Change::Rename(to) => m.stored(to).is_some(),
                Change::Drop { .. } => m.stored(column).is_none(),
                Change::Type(_) => true, // (the same names either way)
                Change::Default(d) => m.stored(column).is_none_or(|s| m.defaults.get(s) == d.as_ref()),
                Change::NotNull(on) => m.stored(column).is_none_or(|s| m.not_null.iter().any(|c| c == s) == *on),
            };
            if done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        return Ok(());
    }
    if let Ddl::CreateRoutine { name, .. } | Ddl::DropRoutine { name, .. } = d {
        // (a statement sent here right after may use it, or expect it gone)
        let (key, want) = (crate::routines::key(&local(lake, name).unwrap_or_default()), match d {
            Ddl::CreateRoutine { routine, .. } => Some(routine.clone()),
            _ => None,
        });
        for _ in 0..200 {
            if lake.cat.get::<crate::expander::Routine>(&key).await? == want {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        return Ok(());
    }
    let (Ddl::Attach { name, .. } | Ddl::Detach { name, .. } | Ddl::CreateDatabase { name, .. } | Ddl::RestoreDatabase { name, .. }) = d else { return Ok(()) };
    let want = !matches!(d, Ddl::Detach { .. });
    for _ in 0..100 {
        if lake.cat.get::<Attachment>(&attachment_key(name)).await?.is_some() == want {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    sync(lake, true).await
}

/// `CREATE VIEW` (and CREATE EXTERNAL TABLE's view of files): a query by name, planned once here
/// to be sure it can be, and kept as written.
async fn create_view(lake: &Lake, name: &str, sql: String, replace: bool, external: bool, facts: Option<Vec<crate::constraints::Constraint>>) -> Result<Value> {
    let name = new_name(lake, name).await?;
    unclaimed(lake, &name).await?;
    ensure!(lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_none(), "{name} is a table");
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(&name)).await?.is_none(), "{name} is a materialized view");
    ensure!(replace || lake.cat.get::<StoredView>(&query_key(&name)).await?.is_none(), "view {name} already exists (CREATE OR REPLACE VIEW)");
    crate::facts::measures_checked(lake, &sql).await?; // (a view with measures: its shape, and each join onto a key: ADR-065 §3)
    let expanded = crate::routines::expand(lake, &sql).await?; // (kept as written: macros are read when it is used)
    let planned = crate::sys::planned(&expanded)?;
    let df = crate::engine::sql(&crate::query::session(lake, &planned, "").await?, &planned).await.context(if external { "reading its files" } else { "the view's query" })?; // (it plans)
    let facts = crate::facts::settled(lake, &name, &sql, df.schema(), facts, replace).await?; // (its key facts: `facts.rs`)
    lake.cat.commit(vec![(query_key(&name), json(&StoredView { sql, external, facts }))], &[]).await?;
    Ok(match external {
        true => j!({"table": name, "files": true}),
        false => j!({"view": name}),
    })
}

/// `DROP TABLE`: the table leaves the catalog at once. Refused while a view or task reads it or it
/// is a view's own. It is kept to be undropped for its `retention` (a day unless set: ADR-043),
/// its rows in the log sent to files first, so nothing of it is left in a log that moves on; with
/// PURGE, or `retention = '0 seconds'`, it isn't, and its files go once a day old
/// (`tier::collect_orphans`). It leaves the shares that hand it out (`shares::tables_moved`).
/// `made_by` (a failed `CREATE TABLE … AS SELECT`'s undo): only a table that job made is dropped. The leader decides it
/// here, under the lake's lock: a follower may not see the table yet, and another statement's table under the name stays.
pub(crate) async fn drop_table(lake: &Lake, name: &str, if_exists: bool, purge: bool, made_by: Option<&str>) -> Result<Value> {
    if let Some(job) = made_by {
        if lake.cat.get::<TableMeta>(&table_key(name)).await?.and_then(|m| m.made_by).as_deref() != Some(job) {
            return Ok(j!({"table": name, "dropped": false, "kept": true}));
        }
    }
    if lake.cat.get::<StoredView>(&query_key(name)).await?.is_some_and(|v| v.external) {
        lake.cat.commit(vec![], &[query_key(name)]).await?; // (CREATE EXTERNAL TABLE's: its files stay)
        return Ok(j!({"table": name, "dropped": true}));
    }
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(name)).await? else {
        ensure!(if_exists, "no table {name}");
        return Ok(j!({"table": name, "dropped": false}));
    };
    table_only(lake, name, "dropped").await?;
    let readers = readers(lake, name).await?;
    if !readers.is_empty() {
        // (CASCADE drops views only: with a task among the readers, it would refuse too)
        let views_only = readers.iter().all(|r| r.starts_with("view ") || r.starts_with("materialized view "));
        let how = if views_only { format!("DROP TABLE {name} CASCADE drops them too, or drop them first") } else { "drop them first".to_string() };
        bail!("{name} is read by {}: {how}", readers.join(", "));
    }
    let kept = keeps(name, &meta, purge);
    let deleted = crate::sys::deleted(name);
    let _held = match kept {
        true => Some(still(lake, &[name.to_string(), deleted.clone()]).await?), // (until it's gone: rows sent since are refused)
        false => None,
    };
    let meta = lake.cat.get::<TableMeta>(&table_key(name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?; // (its rows in files now: the entry as it is)
    let (puts, from) = set_aside(lake, name, &meta, kept).await?;
    let copies = crate::delta::Copies::of(lake, name, meta.folder(name), &meta.publish).await?; // (their states go in this commit)
    let gone: Vec<String> = [table_key(name), table_key(&deleted)].into_iter().chain(copies.keys()).collect(); // (and its replaced rows)
    lake.cat.commit(puts, &gone).await?;
    copies.delete(lake).await;
    if !kept {
        crate::seq::drop_owned(lake, &meta).await?; // (a table kept to be undropped keeps its identity's sequences)
    }
    Ok(left_shares(j!({"table": name, "dropped": true}), from))
}

/// What a statement on a table leaves alone, for `doing` (the verb its refusal names: "dropped", "renamed", "swapped",
/// "replaced"): a materialized view's own table, its `_final` part, its open table, and a table a topic feeds
/// (`feeds::feed_key`: a feed has no view of its own, so a drop missed it before). Drop, rename, swap and replace all call it.
pub(crate) async fn table_only(lake: &Lake, name: &str, doing: &str) -> Result<()> {
    let owner = name.strip_suffix("_final").unwrap_or(name);
    let open = crate::once::open(name);
    for (key, view) in [(name, name), (owner, owner), (open.as_str(), name)] {
        if lake.cat.get::<Value>(&crate::views::view_key(key)).await?.is_some() {
            bail!(kept_by(name, view, doing));
        }
    }
    ensure!(lake.cat.get::<Value>(&crate::feeds::feed_key(name)).await?.is_none(), "{name} is fed by a topic, so it can't be {doing}: DROP MATERIALIZED VIEW {name}");
    Ok(())
}

/// The refusal for a table that materialized view `view` keeps: `name` is the view's own table, or its `_final` part. A rename
/// is said to be done by dropping the view and creating it under the new name; the other statements say `DROP MATERIALIZED VIEW`.
fn kept_by(name: &str, view: &str, doing: &str) -> String {
    let hint = match doing {
        "renamed" => "DROP it, and CREATE it under the new name".to_string(),
        _ => format!("DROP MATERIALIZED VIEW {view}"),
    };
    let what = match name == view {
        true => "a materialized view".to_string(),
        false => format!("part of materialized view {view}"),
    };
    format!("{name} is {what}, so it can't be {doing}: {hint}")
}

/// Refused while a table has followers: a materialized view or a task that reads it (`readers`, but not a stored view, which
/// reads a table by name). `why` says what the followers keep, as the refusal words it.
pub(crate) async fn no_followers(lake: &Lake, name: &str, why: &str) -> Result<()> {
    let followers: Vec<String> = readers(lake, name).await?.into_iter().filter(|r| !r.starts_with("view ")).collect();
    ensure!(followers.is_empty(), "{name} is followed by {}: {why}, so drop them first", followers.join(", "));
    Ok(())
}

/// Is a dropped table kept to be undropped (ADR-043)? Not with PURGE, not a hidden table, and not with a retention of 0.
pub(crate) fn keeps(name: &str, meta: &TableMeta, purge: bool) -> bool { meta.retention_secs.map_or(KEEP_MS, |s| s * 1000) > 0 && !purge && !crate::sys::hidden(name) }

/// A table's entries set aside as a drop sets them (leader, under the lock): kept to be undropped when `kept`, as `dt/` with
/// its changed rows' old versions; taken out of every share (`shares::tables_moved`). The caller's commit deletes the table's own
/// entries and its published states (`delta::Copies`), and deletes their objects after it: until then other engines still
/// find the table's copies, so nothing is unpublished here. Returns the puts, and the shares it left.
pub(crate) async fn set_aside(lake: &Lake, name: &str, meta: &TableMeta, kept: bool) -> Result<(Vec<(String, Vec<u8>)>, Vec<String>)> {
    let mut puts = vec![];
    if kept {
        let at_ms = crate::log::now_ms();
        let keep_ms = meta.retention_secs.map_or(KEEP_MS, |s| s * 1000);
        let deleted = lake.cat.get::<TableMeta>(&table_key(&crate::sys::deleted(name))).await?;
        puts.push((format!("dt/{name}/{at_ms:020}"), json(&Dropped { at_ms, keep_ms, meta: meta.clone(), deleted })));
    }
    let (unshared, from) = crate::shares::tables_moved(lake, &[(name, None)]).await?; // (UNDROP doesn't share it again)
    puts.extend(unshared);
    Ok((puts, from))
}

/// A drop's answer, with the shares it took the table out of.
pub(crate) fn left_shares(mut out: Value, shares: Vec<String>) -> Value {
    if !shares.is_empty() {
        out["unshared"] = j!(shares);
    }
    out
}

/// How long a table's past is kept unless its `retention` says: read as it was, or undropped.
pub const KEEP_MS: u64 = 24 * 3_600_000;

/// A dropped table, kept while it can be undropped (`dt/{name}/{at_ms}`, ADR-043): its entries as
/// they were when it was dropped, every row in files. `tier::expire` lets it go after `keep_ms`;
/// until then its files are in use (`tier::collect_orphans`) and its folder taken (`free_folder`).
#[derive(Serialize, Deserialize)]
pub struct Dropped {
    pub at_ms: u64,
    pub keep_ms: u64,
    pub meta: TableMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted: Option<TableMeta>, // (its changed rows' old versions: `{t}$deleted`)
}

/// Every dropped table still kept: (its name, what was kept), oldest first for each name.
pub async fn dropped(lake: &Lake) -> Result<Vec<(String, Dropped)>> {
    Ok(lake.cat.scan::<Dropped>("dt/", "dt0").await?.into_iter().map(|(k, d)| (k[3..].rsplit_once('/').map_or(&k[3..], |(n, _)| n).to_string(), d)).collect())
}

/// `retention = '7 days'`: in seconds ('0 seconds' keeps nothing).
pub fn retention(text: &str) -> Result<u64> {
    match crate::runs::every(text) {
        Ok(crate::runs::Every::Seconds(s)) => Ok(s),
        _ if text.split_whitespace().next().is_some_and(|n| n.parse() == Ok(0u64)) => Ok(0),
        _ => bail!("retention: a time such as '7 days', '12 hours' or '0 seconds', not {text:?}"),
    }
}

/// `UNDROP TABLE t`: the table dropped last under that name, back as it was. It reads the log from
/// now on (its rows went to files when it was dropped; the log since may hold another table's of
/// its name). Refused while something else has the name: rename that first.
async fn undrop(lake: &Lake, name: &str) -> Result<Value> {
    let taken = lake.cat.get::<TableMeta>(&table_key(name)).await?.is_some() || lake.cat.get::<StoredView>(&query_key(name)).await?.is_some()
        || lake.cat.get::<Value>(&crate::views::view_key(name)).await?.is_some();
    if taken {
        return Err(crate::codes::coded("42P07", format!("{name} exists: rename it (ALTER TABLE {name} RENAME TO …), then UNDROP TABLE {name}")));
    }
    let mut kept: Vec<(String, Dropped)> = lake.cat.scan::<Dropped>(&format!("dt/{name}/"), &format!("dt/{name}0")).await?;
    let Some((key, d)) = kept.pop() else {
        return Err(crate::codes::coded("42P01", format!("no dropped table {name} is kept (pondra.dropped lists those that are)")));
    };
    let at = lake.visible();
    let mut puts = vec![];
    for (t, meta) in [(name.to_string(), Some(d.meta)), (crate::sys::deleted(name), d.deleted)] {
        if let Some(mut meta) = meta {
            meta.tiered = at;
            puts.push((table_key(&t), json(&meta)));
        }
    }
    lake.cat.commit(puts, &[key]).await?;
    crate::delta::publish_all(lake).await?; // (other engines see it again, if it published)
    Ok(j!({"table": name, "undropped": true}))
}

/// `CREATE TABLE c CLONE t`: a table whose files are t's as they are now, none copied. Its rows in
/// the log go to files first; then c lists t's files (and its changed rows' old versions') where
/// they are, and writes its own to a folder of its own. Files in another table's folder are never
/// deleted by expiry, only by the orphan sweep once no table lists them (`TableMeta::shares`), so
/// t's merges, a drop of t, or c's own merges free nothing the other still reads. It isn't
/// published (Delta and Iceberg name files under a table's folder), and its past starts now.
async fn clone(lake: &Lake, name: &str, from: &str) -> Result<Value> {
    let from = local(lake, from).ok_or_else(|| anyhow::anyhow!("{from} is an attached lake's: a clone is of a table in this lake"))?;
    let taken = lake.cat.get::<TableMeta>(&table_key(name)).await?.is_some() || lake.cat.get::<StoredView>(&query_key(name)).await?.is_some()
        || lake.cat.get::<Value>(&crate::views::view_key(name)).await?.is_some();
    ensure!(!taken, "{name} exists already");
    ensure!(lake.cat.get::<TableMeta>(&table_key(&from)).await?.is_some(), "no table {from}");
    ensure!(!crate::sys::hidden(&from), "{from} is a hidden table");
    let deleted = crate::sys::deleted(&from);
    let _held = still(lake, &[from.clone(), deleted.clone()]).await?; // (every row before the clone is in it)
    let (at, now) = (lake.visible(), crate::log::now_ms());
    let mut puts = vec![];
    for (source, target) in [(from.clone(), name.to_string()), (deleted.clone(), crate::sys::deleted(name))] {
        let Some(mut m) = lake.cat.get::<TableMeta>(&table_key(&source)).await? else { continue };
        let folder = m.folder(&source).to_string();
        m.shares.push(folder);
        m.shares.sort();
        m.shares.dedup();
        m.folder = free_folder(lake, &target).await?;
        m.tiered = at;
        m.publish.clear();
        m.garbage.clear(); // (the source's to delete, not ours)
        m.garbage_deletes.clear();
        m.replaced.clear();
        if source == from {
            m.past_from = Some((at, now));
        }
        puts.push((table_key(&target), json(&m)));
    }
    lake.cat.commit(puts, &[]).await?;
    Ok(j!({"table": name, "cloned": from}))
}

/// `tables`' rows all in files, and no more coming until the hold is dropped, for a statement that
/// moves their entries (rename, swap, drop, clone). Their rows go to files while appends still go
/// in; then their appends are held (`Sequencer::hold`: a node sends them again, to whichever table
/// has the name then) and the rows that came meanwhile go to files too. Looking at the log again
/// instead left a row sequenced between the last look and the statement's commit under a name no
/// table read, and refused the statement while writers kept up.
pub(crate) async fn still(lake: &Lake, tables: &[String]) -> Result<crate::log::Hold> {
    to_files(lake, tables).await?;
    let Some(crate::log::To::Local(seq)) = lake.to.get() else { bail!("a table's entry is moved by the leader") };
    let hold = seq.hold(tables).await?;
    to_files(lake, tables).await?;
    Ok(hold)
}

/// Send `tables`' rows in the log to files, up to its end now.
async fn to_files(lake: &Lake, tables: &[String]) -> Result<()> {
    for t in tables {
        while !crate::tier::tier_table(lake, t, lake.visible(), &["here".to_string()], "here").await?.1 {}
    }
    Ok(())
}

/// `ALTER TABLE | VIEW name RENAME TO to` (ADR-030). A stored view's entry moves. A table's
/// entries move in one commit — its own, its replaced rows' table, its Delta and Iceberg states —
/// and its files stay where they are: the table keeps its folder (`TableMeta::folder`). Its rows
/// still in the log go to files first, so none is left under the old name. Refused while a
/// materialized view or a task follows the table; stored views read by name, so a view of the
/// old name reads whatever takes that name next (dbt's rename-and-replace); a share hands out the
/// renamed table, under the name it had there.
async fn rename(lake: &Lake, name: &str, to: &str) -> Result<Value> {
    let name = local(lake, name).ok_or_else(|| anyhow::anyhow!("{name} is an attached lake's: rename it from a node of that lake"))?;
    let to = match to.split('.').collect::<Vec<_>>()[..] {
        [t] => join(split(&name).0, t), // (Postgres: the same schema)
        _ => local(lake, to).ok_or_else(|| anyhow::anyhow!("{to}: a table is renamed within its lake"))?,
    };
    let to = new_name(lake, &to).await?;
    let taken = lake.cat.get::<TableMeta>(&table_key(&to)).await?.is_some() || lake.cat.get::<StoredView>(&query_key(&to)).await?.is_some()
        || lake.cat.get::<Value>(&crate::views::view_key(&to)).await?.is_some();
    ensure!(!taken, "{to} exists already");
    unclaimed(lake, &to).await?;
    if let Some(v) = lake.cat.get::<StoredView>(&query_key(&name)).await? {
        lake.cat.commit(vec![(query_key(&to), json(&v))], &[query_key(&name)]).await?;
        return Ok(j!({"view": name, "renamed": to}));
    }
    table_only(lake, &name, "renamed").await?;
    ensure!(!crate::sys::hidden(&name), "{name} is written by Pondra alone");
    ensure!(lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_some(), "no table or view {name}");
    no_followers(lake, &name, "they follow it by name").await?;
    // Its rows in the log go to files, so none is left under the old name; its changed rows' old
    // versions (`{name}$deleted`) too, or they would show again. Appends wait meanwhile, then go to
    // whatever has the name, or are refused if nothing does.
    let _held = still(lake, &[name.clone(), crate::sys::deleted(&name)]).await?;
    let (mut puts, gone) = move_entry(lake, &name, &to, |_| {}).await?;
    puts.extend(crate::shares::tables_moved(lake, &[(name.as_str(), Some(to.as_str()))]).await?.0); // (shared under the name it had)
    lake.cat.commit(puts, &gone).await?;
    Ok(j!({"table": name, "renamed": to}))
}

/// A table's entries under another name (`rename`, `replace`): its entry and its `$deleted` entry move, each with the folder it
/// has (`TableMeta::folder`), and so do its published states (`x/`, `i/`). `change` sets what the new name's entries take. The
/// names left behind are returned as deletes; a caller that puts a key over one of them keeps only the put.
pub(crate) async fn move_entry(lake: &Lake, from: &str, to: &str, change: impl Fn(&mut TableMeta)) -> Result<(Vec<(String, Vec<u8>)>, Vec<String>)> {
    let moved = |mut m: TableMeta, n: &str| {
        m.folder = Some(m.folder(n).to_string());
        change(&mut m);
        m
    };
    let meta = lake.cat.get::<TableMeta>(&table_key(from)).await?.ok_or_else(|| anyhow::anyhow!("no table {from}"))?;
    let mut puts = vec![(table_key(to), json(&moved(meta, from)))];
    let mut gone = vec![table_key(from)];
    let from_deleted = crate::sys::deleted(from);
    if let Some(d) = lake.cat.get::<TableMeta>(&table_key(&from_deleted)).await? {
        puts.push((table_key(&crate::sys::deleted(to)), json(&moved(d, &from_deleted))));
        gone.push(table_key(&from_deleted));
    }
    for state in ["x", "i"] {
        if let Some(v) = lake.cat.get::<Value>(&format!("{state}/{from}")).await? {
            puts.push((format!("{state}/{to}"), json(&v)));
            gone.push(format!("{state}/{from}"));
        }
    }
    Ok((puts, gone))
}

/// `ALTER TABLE a SWAP WITH b` (write-audit-publish): two tables alike but for their rows exchange
/// names in one commit. Each keeps its files, row ids, past, changed rows and folder, under the
/// other's name, so a table written beside the one readers use is published by one step. Comments,
/// indexes and grants stay with the names, and a stored view reads whichever table has its name now.
/// Refused for what would change a table's definition or leave its readers reading the wrong rows
/// (`swappable`, `alike`).
async fn swap(lake: &Lake, name: &str, with: &str) -> Result<Value> {
    let a = local(lake, name).ok_or_else(|| anyhow::anyhow!("{name} is an attached lake's: swap it from a node of that lake"))?;
    let b = local(lake, with).ok_or_else(|| anyhow::anyhow!("{with} is an attached lake's: swap it from a node of that lake"))?;
    ensure!(a != b, "{a} is swapped with itself: name two tables");
    swappable(lake, &a).await?;
    swappable(lake, &b).await?;
    alike(lake, &a, &b).await?;
    // Their rows in the log go to files first, as rename does: a row left there would show under the
    // other table's name after the swap.
    let (da, db) = (crate::sys::deleted(&a), crate::sys::deleted(&b));
    let _held = still(lake, &[a.clone(), da, b.clone(), db]).await?;
    exchange(lake, &a, &b).await?;
    Ok(j!({"table": a, "swapped": b}))
}

/// What a swap needs of one of its tables: a table, not a view or part of a materialized view,
/// that nothing keeps rows derived from, and that no other engine reads or a share hands out.
async fn swappable(lake: &Lake, n: &str) -> Result<()> {
    ensure!(lake.cat.get::<StoredView>(&query_key(n)).await?.is_none(), "{n} is a view: a swap exchanges two tables (CREATE OR REPLACE VIEW changes a view)");
    table_only(lake, n, "swapped").await?;
    ensure!(!crate::sys::hidden(n), "{n} is written by Pondra alone");
    let meta = lake.cat.get::<TableMeta>(&table_key(n)).await?.ok_or_else(|| anyhow::anyhow!("no table {n}"))?;
    // (a stored view reads by name, so it keeps working after a swap; what keeps rows derived from the table does not)
    no_followers(lake, n, "they keep rows derived from what it holds").await?;
    // (a share publishes its table as Delta, so its share is named before its publishing)
    if let Some((s, _)) = crate::shares::shares(lake).await?.into_iter().find(|(_, s)| s.tables.iter().any(|x| x.table == n)) {
        bail!("{n} is in share {s}: its recipients read its files by name");
    }
    ensure!(meta.publish.is_empty(), "{n} is published ({}): other engines read its folder, which a swap doesn't move", meta.publish.join(", "));
    Ok(())
}

/// Two tables swap only when they differ in nothing but their rows: the same definition (what a
/// deploy and a protected database see) and the same stored columns, which the files are read by.
async fn alike(lake: &Lake, a: &str, b: &str) -> Result<()> {
    let ma = lake.cat.get::<TableMeta>(&table_key(a)).await?.ok_or_else(|| anyhow::anyhow!("no table {a}"))?;
    let mb = lake.cat.get::<TableMeta>(&table_key(b)).await?.ok_or_else(|| anyhow::anyhow!("no table {b}"))?;
    let same = crate::objects::table_sql("t", &ma) == crate::objects::table_sql("t", &mb) && ma.columns == mb.columns && ma.names == mb.names && ma.dropped == mb.dropped;
    ensure!(same, "{a} and {b} differ in more than their rows: a swap exchanges two tables alike (CREATE TABLE {b} CLONE {a} makes one)");
    Ok(())
}

/// The largest `tiered` mark of the entries given (0 if none): the entries that move together take it (see `exchange`).
pub(crate) fn mark<'a>(metas: impl IntoIterator<Item = Option<&'a TableMeta>>) -> u64 {
    metas.into_iter().flatten().map(|m| m.tiered).max().unwrap_or(0)
}

/// The swap itself, in one commit (leader), the four tables' rows all in files and their appends held
/// (`still`). Each name takes the other's entry, and its `$deleted` entry (or none, when the other had
/// none), keeping the folder it had. Every moved entry takes the largest of the four `tiered` marks: a
/// table reads its log rows after its mark, by name, and the log between the two marks holds rows the
/// other table wrote under this name, already in its files.
async fn exchange(lake: &Lake, a: &str, b: &str) -> Result<()> {
    let (da, db) = (crate::sys::deleted(a), crate::sys::deleted(b));
    let (ea, eb) = (lake.cat.get::<TableMeta>(&table_key(a)).await?, lake.cat.get::<TableMeta>(&table_key(b)).await?);
    let (eda, edb) = (lake.cat.get::<TableMeta>(&table_key(&da)).await?, lake.cat.get::<TableMeta>(&table_key(&db)).await?);
    let mark = mark([ea.as_ref(), eb.as_ref(), eda.as_ref(), edb.as_ref()]);
    let (mut puts, mut gone) = (vec![], vec![]);
    for (to, from, from_name) in [(a, eb, b), (b, ea, a), (&da[..], edb, &db[..]), (&db[..], eda, &da[..])] {
        match from {
            Some(mut m) => {
                m.folder = Some(m.folder(from_name).to_string());
                m.tiered = mark;
                puts.push((table_key(to), json(&m)));
            }
            None => gone.push(table_key(to)), // (the other had no changed rows: none is left under this name)
        }
    }
    lake.cat.commit(puts, &gone).await
}

/// A folder under `data/` for a new table: its name's, unless another table has that one (a
/// renamed table keeps its folder, a dropped one while it can be undropped): then `name__2`, `name__3`… (characters a bucket's paths
/// take as they are).
pub async fn free_folder(lake: &Lake, name: &str) -> Result<Option<String>> {
    let users = folder_users(lake).await?;
    Ok(match users.contains_key(name) {
        false => None,
        true => (2..).map(|i| format!("{name}__{i}")).find(|f| !users.contains_key(f)),
    })
}

/// How a table uses a folder under `data/` (`folder_users`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Use {
    /// A live table's own folder: where its files and published copies are written.
    Own,
    /// A live clone's source's folder (`TableMeta::shares`, even once the source is gone: ADR-043).
    Shares,
    /// A dropped table's, kept to be undropped (its own, its sources', its changed rows' old versions').
    Kept,
}

/// The tables that use each folder under `data/`, and how: the live tables, and the dropped ones kept to be undropped.
pub async fn folder_users(lake: &Lake) -> Result<std::collections::BTreeMap<String, Vec<(String, Use)>>> {
    let mut users: std::collections::BTreeMap<String, Vec<(String, Use)>> = Default::default();
    for (k, m) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        let name = &k[2..];
        users.entry(m.folder(name).to_string()).or_default().push((name.to_string(), Use::Own));
        for folder in m.shares {
            users.entry(folder).or_default().push((name.to_string(), Use::Shares));
        }
    }
    for (n, d) in dropped(lake).await? {
        let deleted = d.deleted.as_ref().map(|m| m.folder(&crate::sys::deleted(&n)).to_string()); // (its changed rows' old versions)
        for folder in std::iter::once(d.meta.folder(&n).to_string()).chain(d.meta.shares).chain(deleted) {
            users.entry(folder).or_default().push((n.clone(), Use::Kept));
        }
    }
    Ok(users)
}

/// Refuses publishing `name` in `folder` when another live table's files are written there too (`except`: the tables this
/// statement is about): each table's Delta log and Iceberg metadata would be written in one place. `ALTER TABLE … RENAME`
/// keeps a folder, so `t` renamed and `t` undropped share one.
pub async fn publishes_alone(lake: &Lake, name: &str, folder: &str, except: &[&str]) -> Result<()> {
    let users = folder_users(lake).await?;
    if let Some((other, _)) = users.get(folder).into_iter().flatten().find(|(o, u)| *u == Use::Own && !except.contains(&o.as_str())) {
        let why = format!("{name} can't be published: its folder ({folder}) is {other}'s too, and two tables can't publish in one folder (CREATE TABLE … AS SELECT * FROM {name} copies it into a folder of its own)");
        return Err(crate::codes::coded("55000", why)); // object_not_in_prerequisite_state
    }
    Ok(())
}

/// What reads or writes table `name` by its columns: views, stored views, tasks.
pub(crate) async fn readers(lake: &Lake, name: &str) -> Result<Vec<String>> {
    let mut out: Vec<String> = vec![];
    for (k, v) in lake.cat.scan::<crate::views::View>("v/", "v0").await? {
        if v.source == name || reads(lake, &v.sql, name) {
            out.push(format!("materialized view {}", &k[2..]));
        }
    }
    for (k, v) in lake.cat.scan::<StoredView>("q/", "q0").await? {
        if reads(lake, &v.sql, name) {
            out.push(format!("view {}", &k[2..]));
        }
    }
    for (k, t) in lake.cat.scan::<crate::tasks::Task>("k/", "k0").await? {
        if t.source == name || t.target == name {
            out.push(format!("task {}", &k[2..]));
        }
    }
    Ok(out)
}

/// Why a column that isn't held as `t` (uuid, json or jsonb) can't become one: its rows were never
/// checked as `t`. A uuid's words are as they were; JSON's name the type.
fn not_held(column: &str, t: &str) -> String {
    if t == "uuid" {
        return format!("{column} can't become a uuid: a column is a uuid only from when it is made, since the rows there were never checked as uuids (a new UUID column can take them: UPDATE … SET new = {column}::uuid)");
    }
    let shown = t.to_uppercase();
    format!("{column} can't become {shown}: a column is {shown} only from when it is made, since the rows there were never checked as {shown} (a new {shown} column can take them: UPDATE … SET new = {column}::{t})")
}

/// `ALTER TABLE t RENAME COLUMN a TO b`, `DROP COLUMN a`, `ALTER COLUMN a TYPE BIGINT`: the
/// catalog changes, the files never do (ADR-022). A column keeps the name it was written under
/// (`TableMeta::columns`); `names` says what SQL calls it, `dropped` that nothing reads it again.
/// A type only widens (what every file holds still reads as it). Refused while a view or task
/// names the table's columns: it would break.
async fn alter_column(lake: &Lake, table: &str, column: &str, change: Change) -> Result<Value> {
    let name = local(lake, table).ok_or_else(|| anyhow::anyhow!("{table} is an attached lake's: ALTER it from a node of that lake"))?;
    let Some(mut m) = lake.cat.get::<TableMeta>(&table_key(&name)).await? else { anyhow::bail!("no table {name}") };
    let owner = name.strip_suffix("_final").unwrap_or(&name);
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(owner)).await?.is_none() && lake.cat.get::<Value>(&crate::views::view_key(&crate::once::open(&name))).await?.is_none(),
        "{name} is materialized view {owner}'s: its columns are its query's");
    let system = |c: &str| crate::text::NAMES.contains(&c) || c == "_old_version" || c == "_deleted" || c.contains('~');
    ensure!(!system(column), "{column} is a system column: it stays as it is");
    if let c @ (Change::Default(_) | Change::NotNull(_)) = change {
        return crate::defaults::alter(lake, &name, column, c).await; // (no name or type changes: a view that reads the table doesn't stop it)
    }
    // (Postgres's codes for a column a view uses: `rules::alter::used_by`)
    let readers = readers(lake, &name).await?;
    if !readers.is_empty() {
        return Err(crate::rules::alter::used_by(&name, &readers, matches!(change, Change::Drop { .. })));
    }
    let Some(stored) = m.stored(column).map(str::to_string) else {
        ensure!(matches!(change, Change::Drop { if_exists: true }), crate::rules::alter::no_column(&name, column));
        return Ok(j!({"table": name, "unchanged": true}));
    };
    let partition = m.partition.as_deref().map(|p| p.split_once('(').map_or(p, |(_, c)| c.trim_end_matches(')')));
    let what = match change {
        Change::Rename(to) => {
            ensure!(!to.is_empty(), "a column needs a name");
            ensure!(!system(&to), "{to} is a system column's name");
            ensure!(m.stored(&to).is_none(), crate::rules::alter::has_column(&name, &to));
            match to == stored {
                true => m.names.remove(&stored),
                false => m.names.insert(stored.clone(), to.clone()),
            };
            for (_, condition) in &mut m.checks {
                *condition = crate::rules::alter::check_renamed(condition, column, &to); // (its checks are in SQL's names: they follow it)
            }
            j!({"renamed": column, "to": to})
        }
        Change::Drop { .. } => {
            let role = match () {
                _ if m.key.contains(&stored) => "key",
                _ if partition == Some(stored.as_str()) => "partition_by",
                _ if m.ttl.as_ref().is_some_and(|(c, _)| *c == stored) => "ttl",
                _ if m.order.as_ref() == Some(&stored) => "order_by",
                _ => "",
            };
            ensure!(role.is_empty(), crate::rules::alter::stays(&name, column, role));
            ensure!(m.live().count() > 1 + usize::from(m.stored("_deleted").is_some()), crate::rules::alter::last_column(&name, column));
            m.cluster.retain(|c| *c != stored);
            m.merge.remove(&stored);
            m.names.remove(&stored);
            crate::constraints::without_column(&mut m, &stored); // (its UNIQUE and the facts naming it go with it, as in Postgres)
            m.checks.retain(|(_, condition)| !crate::rules::alter::check_reads(condition, column)); // (and its checks)
            m.dropped.push(stored.clone());
            j!({"dropped": column})
        }
        Change::Type(sql_type) => {
            let (held, declared) = (m.sql_types.get(&stored).cloned(), crate::types::declared(&sql_type)?);
            if let Some(t) = declared.as_deref().filter(|t| matches!(*t, "uuid" | "json" | "jsonb")) {
                if held.as_deref() != Some(t) {
                    return Err(crate::codes::coded("42804", not_held(column, t)));
                }
                return Ok(j!({"table": name, "unchanged": true}));
            }
            let ctx = datafusion::prelude::SessionContext::new();
            ctx.sql(&format!("CREATE TABLE t (c {sql_type})")).await?;
            let new = crate::answer::stored(ctx.table("t").await?.schema().field(0).data_type());
            let at = m.columns.iter().position(|(c, _)| *c == stored).expect("a live column");
            let old = crate::answer::dtype(&m.columns[at].1)?;
            if let Some(wanted) = declared.filter(|_| old == new) {
                // A length on text only grows: the rows there were checked against the one it had.
                match (held.as_deref().and_then(crate::rules::typed::length), crate::rules::typed::length(&wanted)) {
                    (Some(k), Some(n)) if n == k => return Ok(j!({"table": name, "unchanged": true})),
                    (Some(k), Some(n)) if n > k => {}
                    (Some(k), _) => return Err(crate::codes::coded("42804", format!("{column} is VARCHAR({k}): its length only grows, since its rows may be {k} characters long (VARCHAR({k}) or more, or TEXT)"))),
                    (None, _) => return Err(crate::codes::coded("42804", format!("{column} can't take a length: a column's length holds only from when it is made, since the rows there were never checked against one (a new {} column can take them: UPDATE … SET new = {column})", wanted.to_uppercase()))),
                }
                m.sql_types.insert(stored.clone(), wanted.clone());
                j!({"column": column, "type": wanted.to_uppercase()})
            } else {
                if old == new && (held.is_some() || m.enums.contains_key(&stored)) {
                    // (a uuid, a length or an enum is text held a way: as text, every row reads as it did, as Postgres casts them)
                    m.sql_types.remove(&stored);
                    m.enums.remove(&stored);
                } else if old == new {
                    return Ok(j!({"table": name, "unchanged": true}));
                }
                ensure!(old == new || widens(&old, &new), "{column} is {old}: a column's type can only widen (a smaller integer to a bigger one, FLOAT to DOUBLE, a DECIMAL to more digits), which every file already written still reads as");
                ensure!(old == new || partition != Some(stored.as_str()), "{column} is {name}'s partition_by column: its type stays");
                m.columns[at].1 = crate::answer::type_name(&new);
                j!({"column": column, "type": m.columns[at].1})
            }
        }
        Change::Default(_) | Change::NotNull(_) => unreachable!("defaults::alter takes them, above"),
    };
    let mut puts = vec![(table_key(&name), json(&m))];
    let del = crate::sys::deleted(&name);
    if let Some(mut d) = lake.cat.get::<TableMeta>(&table_key(&del)).await? {
        // (its replaced rows' table follows: the same columns, before its `_old_version`)
        d.columns = m.columns.iter().cloned().chain([("_old_version".to_string(), "Int64".to_string())]).collect();
        (d.names, d.dropped) = (m.names.clone(), m.dropped.clone());
        puts.push((table_key(&del), json(&d)));
    }
    lake.cat.commit(puts, &[]).await?;
    if !m.publish.is_empty() {
        crate::delta::publish_all(lake).await?; // (other engines see the new names at once: they write by them)
    }
    Ok(j!({"table": name, "altered": what}))
}

/// Does every value of type `old` read as `new`, exactly? The widenings Iceberg and Delta allow.
pub(crate) fn widens(old: &datafusion::arrow::datatypes::DataType, new: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType::*;
    let int = |t: &datafusion::arrow::datatypes::DataType| match t {
        Int8 => Some(8),
        Int16 => Some(16),
        Int32 => Some(32),
        Int64 => Some(64),
        _ => None,
    };
    match (old, new) {
        (Float16 | Float32, Float64) | (Float16, Float32) => true,
        (Decimal128(p, s), Decimal128(q, t)) => q > p && s == t,
        _ => matches!((int(old), int(new)), (Some(a), Some(b)) if b > a),
    }
}

/// `DROP VIEW` or `DROP MATERIALIZED VIEW`: a stored view, or a live one with its tables and state
/// (and out of the shares that hand them out).
async fn drop_view(lake: &Lake, name: &str, if_exists: bool) -> Result<Value> {
    if lake.cat.get::<crate::feeds::Feed>(&crate::feeds::feed_key(name)).await?.is_some() {
        let offsets: Vec<String> = lake.cat.scan::<u64>(&crate::store::producer_key(&format!("feed:{name}:")), &crate::store::producer_key(&format!("feed:{name};"))).await?.into_iter().map(|(k, _)| k).collect();
        let gone: Vec<String> = [crate::feeds::feed_key(name), table_key(name)].into_iter().chain(offsets.iter().cloned()).collect();
        let (unshared, from) = crate::shares::tables_moved(lake, &[(name, None)]).await?;
        lake.cat.commit(unshared, &gone).await?;
        crate::log::forget_producers(offsets.into_iter().map(|k| k[2..].to_string())); // (its offsets: made again, it starts over)
        return Ok(left_shares(j!({"view": name, "dropped": true}), from));
    }
    if lake.cat.get::<StoredView>(&query_key(name)).await?.is_some() {
        lake.cat.commit(vec![], &[query_key(name)]).await?;
        return Ok(j!({"view": name, "dropped": true}));
    }
    let (shown, open) = (name, crate::once::open(name));
    let once = lake.cat.get::<crate::views::View>(&crate::views::view_key(name)).await?.is_none() && lake.cat.get::<crate::views::View>(&crate::views::view_key(&open)).await?.is_some();
    let name: &str = if once { &open } else { name }; // (EMIT FINAL's: its entry and partial rows)
    let Some(view) = lake.cat.get::<crate::views::View>(&crate::views::view_key(name)).await? else {
        ensure!(if_exists, "no view {name}");
        return Ok(j!({"view": name, "dropped": false}));
    };
    let kept = view.once.as_ref().map_or(format!("{name}_final"), |o| o.into.clone()); // (what it keeps once)
    // (what follows it by name would be left without rows: a flow is dropped from its end)
    let mut followers = readers(lake, name).await?;
    followers.extend(readers(lake, &kept).await?);
    followers.retain(|r| !r.starts_with("view ") && r != &format!("materialized view {name}") && r != &format!("materialized view {shown}"));
    ensure!(followers.is_empty(), "{shown} is followed by {}: drop them first", followers.join(", "));
    let producers = crate::views::producers(name);
    let mut gone: Vec<String> = [crate::views::view_key(name), format!("w/{name}")].into_iter().chain(producers.iter().map(|p| crate::store::producer_key(p))).collect();
    let mut tables: Vec<String> = vec![name.to_string(), kept, crate::sys::deleted(name)];
    tables.extend(view.matches.iter().flat_map(|_| crate::matches::tables(name))); // (a MATCH_RECOGNIZE's held and resume rows)
    let mut copies = vec![];
    for table in &tables {
        if let Some(meta) = lake.cat.get::<TableMeta>(&table_key(table)).await? {
            let published = crate::delta::Copies::of(lake, table, meta.folder(table), &meta.publish).await?; // (as `drop_table`'s)
            gone.extend(published.keys().chain([table_key(table)]));
            copies.push(published);
        }
    }
    let (unshared, from) = crate::shares::tables_moved(lake, &[(tables[0].as_str(), None), (tables[1].as_str(), None)]).await?;
    lake.cat.commit(unshared, &gone).await?;
    for published in &copies {
        published.delete(lake).await;
    }
    crate::log::forget_producers(producers); // (a view made again under this name starts over)
    crate::views::forget(lake);
    Ok(left_shares(j!({"view": shown, "dropped": true}), from))
}

/// `ALTER MATERIALIZED VIEW v DETACH`: the view is gone and its table stays, a table like any
/// other from now on (a GROUP BY view's: a merge table, its rows combining as they are read). What
/// follows it keeps following its table. A view that keeps windows (its `_final` table) is refused:
/// the windows still open would be left half counted. A view fed by a topic stops reading it.
async fn detach_view(lake: &Lake, name: &str) -> Result<Value> {
    if lake.cat.get::<crate::feeds::Feed>(&crate::feeds::feed_key(name)).await?.is_some() {
        // (its offsets go with the feed: a shard's append in flight, made against them, is refused)
        let offsets = lake.cat.scan::<u64>(&crate::store::producer_key(&format!("feed:{name}:")), &crate::store::producer_key(&format!("feed:{name};"))).await?;
        let gone: Vec<String> = std::iter::once(crate::feeds::feed_key(name)).chain(offsets.iter().map(|(k, _)| k.clone())).collect();
        lake.cat.commit(vec![], &gone).await?;
        crate::log::forget_producers(offsets.into_iter().map(|(k, _)| k[2..].to_string())); // (`p/…`: a feed made again starts over)
        return Ok(j!({"view": name, "detached": true, "table": name}));
    }
    ensure!(lake.cat.get::<crate::views::View>(&crate::views::view_key(&crate::once::open(name))).await?.is_none(),
        "{name} keeps each group once it's over, and those still open would be left half counted: make a table of what it has (CREATE TABLE t AS SELECT * FROM {name}), then drop it");
    ensure!(lake.cat.get::<crate::views::View>(&crate::views::view_key(name)).await?.is_some(), "{name} is not a materialized view");
    ensure!(lake.cat.get::<TableMeta>(&table_key(name)).await?.is_none_or(|m| m.finish.is_none()),
        "{name} works its answers out as it is read (avg, HAVING, …), from partial rows only the view keeps: make a table of what it has (CREATE TABLE t AS SELECT * FROM {name}), then drop it");
    ensure!(lake.cat.get::<TableMeta>(&table_key(&format!("{name}_final"))).await?.is_none(),
        "{name} keeps windows, and those still open would be left half counted: make a table of what it has (CREATE TABLE t AS SELECT * FROM {name}_final), then drop it");
    let producers = crate::views::producers(name);
    let mut gone: Vec<String> = [crate::views::view_key(name), format!("w/{name}")].into_iter().chain(producers.iter().map(|p| crate::store::producer_key(p))).collect();
    if lake.cat.get::<crate::views::View>(&crate::views::view_key(name)).await?.is_some_and(|v| v.matches.is_some()) {
        gone.extend(crate::matches::tables(name).iter().map(|t| table_key(t))); // (a MATCH_RECOGNIZE's held and resume rows: its table stays)
    }
    lake.cat.commit(vec![], &gone).await?;
    crate::log::forget_producers(producers);
    crate::views::forget(lake);
    Ok(j!({"view": name, "detached": true, "table": name}))
}

/// Does query `sql` read this lake's table `t`? (If it can't be parsed, a word match: cautious, so
/// a drop is refused rather than a view broken.)
fn reads(lake: &Lake, sql: &str, t: &str) -> bool {
    match crate::spmd::tables(sql) {
        Some(names) => names.iter().any(|n| local(lake, n).as_deref() == Some(t)),
        None => mentions(sql, t),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lake's name is its folder's or prefix's last part, in lower case; a lake in memory is `memory`, not its id.
    #[test]
    fn a_lakes_name_is_its_folders_and_memory_for_one_in_memory() {
        assert_eq!(name_of("/data/MyLake/"), "mylake");
        assert_eq!(name_of("D:\\data\\mylake"), "mylake");
        assert_eq!(name_of("s3://bucket/shop"), "shop");
        assert_eq!(name_of("memory://2965062e0e8442ce87cf27d8a2c4f13e"), "memory");
    }

    /// A failed `CREATE TABLE … AS SELECT` drops only the table its own job made: the leader checks `made_by` under the lake's
    /// lock, so another statement's table under the name stays, and the job's own table goes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_ctas_drops_only_the_table_its_job_made() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("pondra-made-{}", uuid::Uuid::new_v4()));
        let lake = Lake::open(dir.to_str().unwrap(), true, false).await?;
        crate::write::create_table(&lake, "t", r#"{"columns": [["a", "Int64"]], "made_by": "job-a"}"#).await?;
        let drop = |made_by: &str| Ddl::DropTable { name: "t".into(), if_exists: true, purge: true, made_by: Some(made_by.into()) };
        let out = apply(&lake, drop("job-b")).await?;
        assert_eq!(out["dropped"], false, "{out}");
        assert!(lake.cat.get::<TableMeta>(&table_key("t")).await?.is_some(), "another job's table was dropped");
        apply(&lake, drop("job-a")).await?;
        assert!(lake.cat.get::<TableMeta>(&table_key("t")).await?.is_none(), "the job's own table was kept");
        lake.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    /// A database made before names starting `_` were refused is still listed in its folder, under its name, so it is
    /// attached at start and opened by name as before; a new one with that name is refused (`database_name`). The
    /// server's own folder (`_pondra`) has no catalog, so it is not listed.
    #[tokio::test]
    async fn an_old_underscore_database_is_listed_and_a_new_one_refused() -> Result<()> {
        let folder = std::env::temp_dir().join(format!("pondra-lakes-{}", uuid::Uuid::new_v4()));
        for name in ["_x", "ok", "_pondra"] {
            std::fs::create_dir_all(folder.join(name))?;
        }
        std::fs::create_dir_all(folder.join("_x").join("catalog"))?;
        std::fs::create_dir_all(folder.join("ok").join("catalog"))?;
        let found = lakes_in(folder.to_str().unwrap()).await?;
        let names: Vec<&str> = found.iter().map(|(n, _)| n.as_str()).collect();
        std::fs::remove_dir_all(&folder)?;
        assert_eq!(names, vec!["_x", "ok"], "listed as before #183; the server's own folder is not a database");
        assert_eq!(crate::codes::of(&database_name("_x").unwrap_err()), "42602", "a new one is refused");
        Ok(())
    }

    #[test]
    fn a_database_name_is_not_the_servers_own() {
        for name in ["_x", "_pondra", "_"] {
            assert_eq!(crate::codes::of(&database_name(name).unwrap_err()), "42602", "{name}");
        }
        for name in ["x_", "a-b", "lake"] {
            database_name(name).unwrap();
        }
    }

    /// A missing schema says how to make it, except pondra, which is the system's: no hint names a statement it refuses.
    #[test]
    fn a_missing_schema_says_how_to_make_it_but_pondras() {
        assert_eq!(no_schema("sales"), "no schema sales (CREATE SCHEMA sales)");
        assert_eq!(no_schema("Pondra"), "pondra is the system's schema: its tables (pondra.runs, pondra.tasks, …) are read-only");
        assert_eq!(no_schema("PONDRA"), no_schema("pondra"));
        // (a schema, a database or another engine's catalog called pondra would hide the system's tables)
        for name in ["pondra", "Pondra", "PONDRA"] {
            assert_eq!(crate::codes::of(&not_system(name, "x").unwrap_err()), "42939", "{name}");
        }
        not_system("pondras", "x").unwrap();
    }
}
