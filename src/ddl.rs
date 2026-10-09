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

pub const PUBLIC: &str = "public";

pub fn schema_key(s: &str) -> String { format!("ns/{s}") }
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
}

/// Another lake, attached in SQL (`ATTACH 'dir' AS name`): every node attaches it.
pub fn attachment_key(n: &str) -> String { format!("a/{n}") }

#[derive(Serialize, Deserialize, Clone)]
pub struct Attachment {
    pub dir: String,
}

/// A table's schema, and its name within it.
pub fn split(name: &str) -> (&str, &str) { name.split_once('.').unwrap_or((PUBLIC, name)) }

/// The name a table of `schema` has inside the lake.
pub fn join(schema: &str, table: &str) -> String {
    if schema == PUBLIC { table.to_string() } else { format!("{schema}.{table}") }
}

/// What a schema, table or view may be called: letters, digits, `_` and `-`. A dot separates the
/// parts of a name; quotes, slashes and spaces would get in the way of paths and SQL.
pub fn check(part: &str) -> Result<()> {
    ensure!(!part.is_empty() && part.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-'),
            "{part:?}: names are letters, digits, _ and - (a dot separates schema and table)");
    Ok(())
}

/// This lake's own name in three-part names: its folder's (or prefix's) last part (`mylake` of
/// `D:\data\mylake` too).
pub fn lake_name(lake: &Lake) -> String {
    lake.url.trim_end_matches(['/', '\\']).rsplit(['/', '\\']).next().unwrap_or("lake").to_lowercase()
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
    pub sql: Option<String>,     // (a view's definition; a materialized view's query)
}

/// Every table and view of this lake and the lakes attached to it, lake by lake.
pub async fn listed(lake: &Lake) -> Result<Vec<Listed>> {
    let mut all = vec![];
    let attached: Vec<(String, Arc<Lake>)> = lake.attached.read().unwrap().clone();
    for (catalog, l) in std::iter::once((lake_name(lake), lake.arc())).chain(attached) {
        // (a materialized view of any kind: windows, sessions and joins keep more than its SQL)
        let materialized: std::collections::HashMap<String, String> = l.cat.scan::<Value>("v/", "v0").await?.into_iter().map(|(k, v)| (k[2..].to_string(), v["written"].as_str().or(v["sql"].as_str()).unwrap_or_default().to_string())).collect();
        for (k, m) in l.cat.scan::<TableMeta>("t/", "t0").await? {
            let name = &k[2..];
            if crate::sys::hidden(name) {
                continue;
            }
            let (schema, table) = split(name);
            let found = materialized.get(name).or_else(|| materialized.get(name.trim_end_matches("_final"))).or_else(|| materialized.get(&crate::once::open(name)));
            let kind = if found.is_some() { "materialized view" } else { "table" };
            let sql = found.filter(|s| !s.is_empty()).cloned();
            all.push(Listed { lake: catalog.clone(), schema: schema.into(), name: table.into(), kind, meta: Some(m.described()), sql });
        }
        for (k, v) in l.cat.scan::<StoredView>("q/", "q0").await? {
            let (schema, view) = split(&k[2..]);
            all.push(Listed { lake: catalog.clone(), schema: schema.into(), name: view.into(), kind: if v.external { "external table" } else { "view" }, meta: None, sql: Some(v.sql) });
        }
    }
    Ok(all)
}

pub async fn schemas(lake: &Lake) -> Result<Vec<String>> {
    let mut out = vec![PUBLIC.to_string()];
    out.extend(lake.cat.scan::<Schema>("ns/", "ns0").await?.into_iter().map(|(k, _)| k[3..].to_string()));
    Ok(out)
}

async fn has_schema(lake: &Lake, s: &str) -> Result<bool> {
    Ok(s == PUBLIC || lake.cat.get::<Schema>(&schema_key(s)).await?.is_some())
}

/// Where a name points: this lake (None) or an attached one, and the name inside it. Names are as
/// SQL resolves them (`write::object`): unquoted parts already lower-case.
pub async fn resolve(lake: &Lake, name: &str) -> Result<(Option<Arc<Lake>>, String)> {
    let parts: Vec<&str> = name.split('.').collect();
    Ok(match parts[..] {
        [t] => (None, t.to_string()),
        [s, t] if has_schema(lake, s).await? => (None, join(s, t)),
        [l, t] => match attached(lake, l).await? {
            Some(other) => (Some(other), t.to_string()),
            None => bail!("no schema {l} (CREATE SCHEMA {l})"),
        },
        [l, s, t] if l == lake_name(lake) => {
            ensure!(has_schema(lake, s).await?, "no schema {s} (CREATE SCHEMA {s})");
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

/// A table of this lake by its name inside it, from a name as SQL wrote it (`write::object`):
/// None for another lake's. (A two-part name may yet be an attached lake's: no table has it here.)
pub fn local(lake: &Lake, name: &str) -> Option<String> {
    match name.split('.').collect::<Vec<_>>()[..] {
        [t] => Some(t.to_string()),
        [s, t] => Some(join(s, t)),
        [l, s, t] if l == lake_name(lake) => Some(join(s, t)),
        _ => None,
    }
}

/// A name for something new in this lake: its schema exists and its parts are fine. `public.t`
/// is `t`.
pub async fn new_name(lake: &Lake, name: &str) -> Result<String> {
    let (s, t) = split(name);
    check(s)?;
    check(t)?;
    ensure!(!t.contains('.'), "{name}: a name is table, schema.table or lake.schema.table");
    ensure!(has_schema(lake, s).await?, "no schema {s} (CREATE SCHEMA {s})");
    Ok(join(s, t))
}

// ---------------------------------------------------------------- statements

/// A statement that changes what the lake holds, not its rows: the leader carries it out.
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Ddl {
    CreateSchema { name: String, if_not_exists: bool },
    DropSchema { name: String, if_exists: bool, cascade: bool },
    DropTable { name: String, if_exists: bool, #[serde(default)] purge: bool }, // (PURGE: not kept to be undropped)
    Undrop { name: String },                                                   // UNDROP TABLE (ADR-043)
    Clone { name: String, from: String },                                      // CREATE TABLE c CLONE t: t's files, none copied (ADR-043)
    CreateView { name: String, sql: String, replace: bool },
    CreateExternal { name: String, sql: String, replace: bool, if_not_exists: bool }, // DataFusion's CREATE EXTERNAL TABLE: a view of files (`ext::external`)
    CreateMaterialized { name: String, sql: String, options: std::collections::BTreeMap<String, String> }, // (`views::options`)
    DropView { name: String, if_exists: bool },
    Attach { name: String, dir: String },
    Detach { name: String, if_exists: bool },
    CreateDatabase { name: String, if_not_exists: bool, dir: Option<String>, #[serde(default, skip_serializing_if = "Option::is_none")] clone: Option<crate::branch::CloneOf> }, // a new lake (beside this one unless `dir`), attached; a branch of another (ADR-047)
    Branch(crate::branch::Make),            // the new lake's leader: make it its base as that is now (ADR-047)
    Pin { lake: String, ms: Option<u64> },  // a base's leader: keep the files a branch reads
    Unpin { lake: String },
    DropDatabase { name: String, if_exists: bool }, // a folder of databases' (`dbserver.rs`): its node stopped, its folder deleted (ADR-030)
    AlterColumn { table: String, column: String, change: Change }, // ALTER TABLE … RENAME/DROP/ALTER COLUMN (ADR-022)
    RenameTable { name: String, to: String }, // ALTER TABLE | VIEW … RENAME TO (ADR-030)
    CreateRoutine { name: String, routine: crate::routines::Routine, replace: bool }, // CREATE MACRO, CREATE PROCEDURE (ADR-023)
    DropRoutine { name: String, if_exists: bool },
    CreateSecret { name: String, params: std::collections::BTreeMap<String, String>, replace: bool, if_not_exists: bool }, // (ADR-026: `ext.rs`)
    AttachOutside { name: String, url: String, kind: String, options: std::collections::BTreeMap<String, String> }, // another engine's tables (`ext.rs`)
    DropSecret { name: String, if_exists: bool },
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
    Object(crate::objects::Op),                              // the registry's: COMMENT ON, CREATE OR ALTER TABLE (`objects.rs`)
}

/// What `ALTER TABLE` does to a column: rename it, drop it, or widen its type (a SQL type).
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Rename(String),
    Drop { if_exists: bool },
    Type(String),
}

/// Leader: carry one out (under the lake's lock). Comments follow what it renames, and go with
/// what it drops (`objects::follow`).
pub async fn apply(lake: &Lake, d: Ddl) -> Result<Value> {
    let moves = crate::objects::moves(&d);
    let out = carry_out(lake, d).await?;
    if moves {
        crate::objects::follow(lake, &out).await?;
    }
    Ok(out)
}

async fn carry_out(lake: &Lake, d: Ddl) -> Result<Value> {
    // (this lake's own three-part names, as dbt writes them: `"lake"."schema"."t"` is `schema.t`)
    let here = |n: String| match n.split('.').collect::<Vec<_>>()[..] {
        [l, s, t] if l == lake_name(lake) => join(s, t),
        _ => n,
    };
    let d = match d {
        Ddl::DropTable { name, if_exists, purge } => Ddl::DropTable { name: here(name), if_exists, purge },
        Ddl::Undrop { name } => Ddl::Undrop { name: here(name) },
        Ddl::Clone { name, from } => Ddl::Clone { name: here(name), from: here(from) },
        Ddl::CreateView { name, sql, replace } => Ddl::CreateView { name: here(name), sql, replace },
        Ddl::CreateExternal { name, sql, replace, if_not_exists } => Ddl::CreateExternal { name: here(name), sql, replace, if_not_exists },
        Ddl::CreateMaterialized { name, sql, options } => Ddl::CreateMaterialized { name: here(name), sql, options },
        Ddl::DropView { name, if_exists } => Ddl::DropView { name: here(name), if_exists },
        Ddl::RenameTable { name, to } => Ddl::RenameTable { name: here(name), to },
        Ddl::CreateRoutine { name, routine, replace } => Ddl::CreateRoutine { name: here(name), routine, replace },
        Ddl::DropRoutine { name, if_exists } => Ddl::DropRoutine { name: here(name), if_exists },
        Ddl::Unless { name, kind, then } => Ddl::Unless { name: here(name), kind, then },
        Ddl::Replacing { name, then } => Ddl::Replacing { name: here(name), then },
        Ddl::DetachView { name } => Ddl::DetachView { name: here(name) },
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
        Ddl::CreateSchema { name, if_not_exists } => {
            check(&name)?;
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
                drop_table(lake, t, true, false).await?;
            }
            lake.cat.commit(vec![], &[schema_key(&name)]).await?;
            Ok(j!({"schema": name, "dropped": true}))
        }
        Ddl::DropTable { name, if_exists, purge } => drop_table(lake, &name, if_exists, purge).await,
        Ddl::Undrop { name } => undrop(lake, &new_name(lake, &name).await?).await,
        Ddl::Clone { name, from } => clone(lake, &new_name(lake, &name).await?, &from).await,
        Ddl::CreateView { name, sql, replace } => create_view(lake, &name, sql, replace, false).await,
        Ddl::CreateExternal { name, sql, replace, if_not_exists } => {
            let name = new_name(lake, &name).await?;
            let taken = lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_some() || lake.cat.get::<StoredView>(&query_key(&name)).await?.is_some()
                || lake.cat.get::<Value>(&crate::views::view_key(&name)).await?.is_some();
            if taken && if_not_exists {
                return Ok(j!({"table": name, "unchanged": true}));
            }
            ensure!(replace || !taken, "{name} already exists (CREATE OR REPLACE EXTERNAL TABLE, or IF NOT EXISTS)");
            create_view(lake, &name, sql, replace, true).await
        }
        Ddl::CreateMaterialized { name, sql, options } => {
            let name = new_name(lake, &name).await?;
            ensure!(lake.cat.get::<StoredView>(&query_key(&name)).await?.is_none(), "{name} is a (stored) view");
            let outside = crate::ext::names(&sql);
            if let [topic] = &outside[..] {
                if crate::ext::spec(topic).is_some_and(|s| s.format == "kafka") {
                    crate::feeds::create(lake, &name, &sql, topic, &options).await?; // (a topic's records, as they arrive)
                    crate::views::forget(lake);
                    return Ok(j!({"view": name, "feed": true}));
                }
            }
            ensure!(outside.is_empty(), "a materialized view follows the rows its tables take in, and files outside the lake take none: read them into a table (CREATE TABLE … AS, INSERT … SELECT) and follow that, or make a stored view (CREATE VIEW)");
            crate::views::create(lake, &name, &sql, crate::views::options(&options)?).await?;
            crate::views::forget(lake); // (the sequencer holds flushes to it from its next commit)
            Ok(j!({"view": name, "materialized": true}))
        }
        Ddl::DropView { name, if_exists } => drop_view(lake, &name, if_exists).await,
        Ddl::Attach { name, dir } => {
            check(&name)?;
            ensure!(name != lake_name(lake), "this lake is called {name}: attach the other under another name");
            ensure!(!has_schema(lake, &name).await?, "a schema here is called {name}: attach the other under another name");
            ensure!(!crate::ext::is_attached(lake, &name).await?, "{name} is attached already, as another engine's tables");
            if let Some(a) = lake.cat.get::<Attachment>(&attachment_key(&name)).await? {
                ensure!(a.dir == full(&dir)?, "{name} is attached already, to {}", a.dir);
                return Ok(j!({"attached": name, "dir": a.dir, "unchanged": true}));
            }
            ensure!(!lake.attached.read().unwrap().iter().any(|(n, _)| *n == name), "{name} is attached already (--attach, or found beside this lake by the shell)");
            ensure!(full(&dir)? != lake.url, "{dir} is this lake");
            let (dir, created) = lake_dir(&dir).await?;
            lake.cat.commit(vec![(attachment_key(&name), json(&Attachment { dir: dir.clone() }))], &[]).await?;
            Ok(match created {
                true => j!({"attached": name, "dir": dir, "created": true}),
                false => j!({"attached": name, "dir": dir}),
            })
        }
        Ddl::CreateDatabase { name, if_not_exists, dir, clone: Some(of) } => crate::branch::create(lake, &name, if_not_exists, dir, of).await,
        Ddl::Branch(m) => crate::branch::make(lake, m).await,
        Ddl::Pin { lake: branch, ms } => crate::branch::pin(lake, &branch, ms).await,
        Ddl::Unpin { lake: branch } => crate::branch::unpin(lake, &branch).await,
        Ddl::CreateDatabase { name, if_not_exists, dir, clone: None } => {
            check(&name)?;
            let dir = dir.unwrap_or_else(|| beside(&lake.url, &name));
            if has_catalog(&full(&dir)?).await? {
                ensure!(if_not_exists, "a lake is at {dir} already: ATTACH '{dir}' AS {name} (or CREATE DATABASE IF NOT EXISTS {name})");
            }
            Box::pin(apply(lake, Ddl::Attach { name, dir })).await
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
            let dropped = match (std::env::var("PONDRA_SERVER_URL"), &bases, &target) {
                (Ok(server), ..) => {
                    let r = crate::cluster::http().delete(format!("{server}/databases/{name}?if_exists={if_exists}")).send().await?;
                    let (ok, text) = (r.status().is_success(), r.text().await?);
                    ensure!(ok, "{text}");
                    serde_json::from_str(&text)?
                }
                (Err(_), Some(_), Some(t)) => {
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
        Ddl::CreateRoutine { name, routine, replace } => crate::routines::create(lake, &name, routine, replace).await,
        Ddl::DropRoutine { name, if_exists } => crate::routines::drop(lake, &name, if_exists).await,
        Ddl::CreateSecret { name, params, replace, if_not_exists } => crate::ext::create(lake, &name, params, replace, if_not_exists).await,
        Ddl::DropSecret { name, if_exists } => crate::ext::drop(lake, &name, if_exists).await,
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

/// Where a lake to attach is, as its nodes open it: a bucket's URL, or a folder's full path (a
/// relative one would mean something else on every machine); and whether it was made just now.
/// Nothing there yet (no catalog): a new, empty lake is made there, as DuckDB's ATTACH makes a
/// new database file.
async fn lake_dir(dir: &str) -> Result<(String, bool)> {
    if !dir.contains("://") {
        std::fs::create_dir_all(dir).with_context(|| format!("no folder {dir}"))?;
    }
    let dir = full(dir)?;
    if has_catalog(&dir).await? {
        return Ok((dir, false));
    }
    crate::inbox::lead_once(&dir).await?; // (whoever leads a lake first makes it)
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
    if let Some(t) = crate::cluster::latest(&store).await? {
        ensure!(!crate::cluster::alive(&store, &t).await, "another process leads the lake at {dir} ({}): stop it first", if t.addr.is_empty() { "a pondra sql" } else { &t.addr });
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
    if !dir.contains("://") && !std::path::Path::new(dir).exists() {
        return Ok(false);
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

/// Attach lake `dir` as `name` here. A node (`follow`) keeps it fresh from its catalog, until it
/// is detached; `stream` also follows its leader's commit stream (`--attach`: for good).
pub async fn attach(home: &Lake, name: &str, dir: &str, me: &str, follow: bool, stream: bool) -> Result<()> {
    let leader = crate::cluster::latest(&crate::store::open_store(dir)?.1).await?.map(|t| t.addr).filter(|a| !a.is_empty());
    let live = match (&leader, stream) {
        (Some(a), true) => crate::cluster::http().get(crate::tls::url(&format!("{a}/cluster/leader"))).timeout(std::time::Duration::from_secs(2)).send().await.is_ok(),
        _ => false,
    };
    let other = Lake::open(dir, false, live).await?;
    if let (true, Some(a)) = (live, leader) {
        crate::cluster::mirror(other.clone(), a, me.to_string(), None);
    }
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
    home.attach(name, other)
}

/// The lakes in `folder` (its subfolders that hold one), attached under their folder names for as
/// long as this node runs: the shell's own folder, so the lakes side by side are its databases, as
/// a database server's are (ADR-024). Nothing is written to the catalog. Left out: this lake, a
/// name the catalog attaches (that ATTACH says where), a schema's name here, and a lake that won't
/// open (said on the log). The names attached.
pub async fn attach_found(home: &Lake, folder: &str, me: &str) -> Vec<String> {
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
        match attach(home, &name, &dir, me, true, false).await {
            Ok(()) => found.push(name),
            Err(e) => eprintln!("attaching {name} ({dir}), found in {folder}: {e:#}"),
        }
    }
    found
}

/// The lakes this process attached because the catalog said so, per home lake.
static FROM_SQL: std::sync::Mutex<Vec<(String, String)>> = std::sync::Mutex::new(Vec::new());

/// Attach here the lakes the catalog lists (`ATTACH`), and let go of those it no longer does
/// (`DETACH`). Lakes attached with `--attach` stay. One that can't be opened is skipped (and
/// tried again next time).
pub async fn sync(lake: &Lake, me: &str, follow: bool) -> Result<()> {
    let listed = lake.cat.scan::<Attachment>("a/", "a0").await?;
    for (key, a) in &listed {
        let name = &key[2..];
        if lake.attached.read().unwrap().iter().any(|(n, _)| n == name) {
            continue;
        }
        match attach(lake, name, &a.dir, me, follow, false).await {
            Ok(()) => FROM_SQL.lock().unwrap().push((lake.url.clone(), name.to_string())),
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
pub async fn settle(lake: &Lake, d: &Ddl, me: &str) -> Result<()> {
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
            if let Some(e) = view.rerun.as_ref().and_then(|_| crate::rerun::failing(lake, &name)) {
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
            if lake.cat.get::<crate::routines::Routine>(&key).await? == want {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        return Ok(());
    }
    let (Ddl::Attach { name, .. } | Ddl::Detach { name, .. } | Ddl::CreateDatabase { name, .. }) = d else { return Ok(()) };
    let want = !matches!(d, Ddl::Detach { .. });
    for _ in 0..100 {
        if lake.cat.get::<Attachment>(&attachment_key(name)).await?.is_some() == want {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    sync(lake, me, true).await
}

/// `CREATE VIEW` (and CREATE EXTERNAL TABLE's view of files): a query by name, planned once here
/// to be sure it can be, and kept as written.
async fn create_view(lake: &Lake, name: &str, sql: String, replace: bool, external: bool) -> Result<Value> {
    let name = new_name(lake, name).await?;
    ensure!(lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_none(), "{name} is a table");
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(&name)).await?.is_none(), "{name} is a materialized view");
    ensure!(replace || lake.cat.get::<StoredView>(&query_key(&name)).await?.is_none(), "view {name} already exists (CREATE OR REPLACE VIEW)");
    let expanded = crate::routines::expand(lake, &sql).await?; // (kept as written: macros are read when it is used)
    let planned = crate::asof::rewrite(&expanded)?;
    crate::query::sql(&crate::query::session(lake, &planned, "").await?, &planned).await.context(if external { "reading its files" } else { "the view's query" })?; // (it plans)
    lake.cat.commit(vec![(query_key(&name), json(&StoredView { sql, external }))], &[]).await?;
    Ok(match external {
        true => j!({"table": name, "files": true}),
        false => j!({"view": name}),
    })
}

/// `DROP TABLE`: the table leaves the catalog at once. Refused while a view or task reads it or it
/// is a view's own. It is kept to be undropped for its `retention` (a day unless set: ADR-043),
/// its rows in the log sent to files first, so nothing of it is left in a log that moves on; with
/// PURGE, or `retention = '0 seconds'`, it isn't, and its files go once a day old
/// (`tier::collect_orphans`).
async fn drop_table(lake: &Lake, name: &str, if_exists: bool, purge: bool) -> Result<Value> {
    if lake.cat.get::<StoredView>(&query_key(name)).await?.is_some_and(|v| v.external) {
        lake.cat.commit(vec![], &[query_key(name)]).await?; // (CREATE EXTERNAL TABLE's: its files stay)
        return Ok(j!({"table": name, "dropped": true}));
    }
    let Some(meta) = lake.cat.get::<TableMeta>(&table_key(name)).await? else {
        ensure!(if_exists, "no table {name}");
        return Ok(j!({"table": name, "dropped": false}));
    };
    let owner = name.strip_suffix("_final").unwrap_or(name);
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(owner)).await?.is_none(), "{name} is materialized view {owner}'s: DROP MATERIALIZED VIEW {owner}");
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(&crate::once::open(name))).await?.is_none(), "{name} is a materialized view: DROP MATERIALIZED VIEW {name}");
    let readers = readers(lake, name).await?;
    ensure!(readers.is_empty(), "{name} is used by {}: drop them first", readers.join(", "));
    let keep_ms = meta.retention_secs.map_or(KEEP_MS, |s| s * 1000);
    let deleted = crate::sys::deleted(name);
    let mut puts = vec![];
    if keep_ms > 0 && !purge && !crate::sys::hidden(name) {
        let mut sent = false;
        for _ in 0..10 {
            if to_files(lake, &[name.to_string(), deleted.clone()]).await? {
                sent = true;
                break;
            }
        }
        ensure!(sent, "{name} kept taking rows while it was dropped: try again when its writers pause (or DROP TABLE {name} PURGE)");
        let at_ms = crate::log::now_ms();
        let meta = lake.cat.get::<TableMeta>(&table_key(name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?;
        let deleted = lake.cat.get::<TableMeta>(&table_key(&deleted)).await?;
        puts.push((format!("dt/{name}/{at_ms:020}"), json(&Dropped { at_ms, keep_ms, meta, deleted })));
    }
    for format in &meta.publish {
        crate::delta::unpublish(lake, name, format).await?; // (no copy left for other engines)
    }
    lake.cat.commit(puts, &[table_key(name), table_key(&deleted)]).await?; // (and its replaced rows)
    Ok(j!({"table": name, "dropped": true}))
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
    ensure!(!taken, "{name} exists: rename it (ALTER TABLE {name} RENAME TO …), then UNDROP TABLE {name}");
    let mut kept: Vec<(String, Dropped)> = lake.cat.scan::<Dropped>(&format!("dt/{name}/"), &format!("dt/{name}0")).await?;
    let Some((key, d)) = kept.pop() else { bail!("no dropped table {name} is kept (pondra.dropped lists those that are)") };
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
    for _ in 0..10 {
        if !to_files(lake, &[from.clone(), deleted.clone()]).await? {
            continue;
        }
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
        return Ok(j!({"table": name, "cloned": from}));
    }
    bail!("{from} kept taking rows while it was cloned: try again when its writers pause")
}

/// Send `tables`' rows in the log to files, up to its end. False if more came meanwhile.
async fn to_files(lake: &Lake, tables: &[String]) -> Result<bool> {
    let mut late = false;
    for t in tables {
        loop {
            let (_, done) = crate::tier::tier_table(lake, t, lake.visible(), &["here".to_string()], "here").await?;
            if done {
                break;
            }
        }
        let Some(meta) = lake.cat.get::<TableMeta>(&table_key(t)).await? else { continue }; // (no rows ever changed: no `$deleted`)
        let upto = lake.visible();
        late |= lake.cat.scan::<crate::store::Segment>(&crate::store::seg_key(meta.tiered + 1), &crate::store::seg_key(upto + 1)).await?.iter().any(|(_, s)| s.parts.contains_key(t));
    }
    Ok(!late)
}

/// `ALTER TABLE | VIEW name RENAME TO to` (ADR-030). A stored view's entry moves. A table's
/// entries move in one commit — its own, its replaced rows' table, its Delta and Iceberg states —
/// and its files stay where they are: the table keeps its folder (`TableMeta::folder`). Its rows
/// still in the log go to files first, so none is left under the old name. Refused while a
/// materialized view or a task follows the table; stored views read by name, so a view of the
/// old name reads whatever takes that name next (dbt's rename-and-replace).
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
    if let Some(v) = lake.cat.get::<StoredView>(&query_key(&name)).await? {
        lake.cat.commit(vec![(query_key(&to), json(&v))], &[query_key(&name)]).await?;
        return Ok(j!({"view": name, "renamed": to}));
    }
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(&name)).await?.is_none(), "{name} is a materialized view: they aren't renamed yet (DROP it, and CREATE it under the new name)");
    let owner = name.strip_suffix("_final").unwrap_or(&name);
    ensure!(!crate::sys::hidden(&name) && lake.cat.get::<Value>(&crate::views::view_key(owner)).await?.is_none(), "{name} is part of materialized view {owner}");
    ensure!(lake.cat.get::<Value>(&crate::views::view_key(&crate::once::open(&name))).await?.is_none(), "{name} is a materialized view: they aren't renamed yet (DROP it, and CREATE it under the new name)");
    ensure!(lake.cat.get::<TableMeta>(&table_key(&name)).await?.is_some(), "no table or view {name}");
    let followers: Vec<String> = readers(lake, &name).await?.into_iter().filter(|r| !r.starts_with("view ")).collect();
    ensure!(followers.is_empty(), "{name} is followed by {}: they follow it by name, so drop them first", followers.join(", "));
    // Its rows in the log go to files, so none is left under the old name (new ones may come:
    // then again); its changed rows' old versions (`{name}$deleted`) too, or they would show again.
    let (old_deleted, new_deleted) = (crate::sys::deleted(&name), crate::sys::deleted(&to));
    for _ in 0..10 {
        if !to_files(lake, &[name.clone(), old_deleted.clone()]).await? {
            continue;
        }
        let meta: TableMeta = lake.cat.get(&table_key(&name)).await?.ok_or_else(|| anyhow::anyhow!("no table {name}"))?;
        let mut puts = vec![];
        let mut gone = vec![table_key(&name)];
        let moved = |mut m: TableMeta, n: &str| {
            m.folder = Some(m.folder(n).to_string());
            m
        };
        puts.push((table_key(&to), json(&moved(meta, &name))));
        if let Some(d) = lake.cat.get::<TableMeta>(&table_key(&old_deleted)).await? {
            puts.push((table_key(&new_deleted), json(&moved(d, &old_deleted))));
            gone.push(table_key(&old_deleted));
        }
        for state in ["x", "i"] {
            if let Some(v) = lake.cat.get::<Value>(&format!("{state}/{name}")).await? {
                puts.push((format!("{state}/{to}"), json(&v)));
                gone.push(format!("{state}/{name}"));
            }
        }
        lake.cat.commit(puts, &gone).await?;
        return Ok(j!({"table": name, "renamed": to}));
    }
    bail!("{name} kept taking rows while it was renamed: try again when its writers pause")
}

/// A folder under `data/` for a new table: its name's, unless another table has that one (a
/// renamed table keeps its folder, a dropped one while it can be undropped): then `name__2`, `name__3`… (characters a bucket's paths
/// take as they are).
pub async fn free_folder(lake: &Lake, name: &str) -> Result<Option<String>> {
    let mut used: std::collections::HashSet<String> = Default::default();
    for (k, m) in lake.cat.scan::<TableMeta>("t/", "t0").await? {
        used.insert(m.folder(&k[2..]).to_string());
        used.extend(m.shares); // (a clone's source's, even once the source is gone: ADR-043)
    }
    for (n, d) in dropped(lake).await? {
        used.insert(d.meta.folder(&n).to_string()); // (kept to be undropped: ADR-043)
        used.extend(d.meta.shares);
        used.extend(d.deleted.map(|m| m.folder(&crate::sys::deleted(&n)).to_string()));
    }
    Ok(match used.contains(name) {
        false => None,
        true => (2..).map(|i| format!("{name}__{i}")).find(|f| !used.contains(f)),
    })
}

/// What reads or writes table `name` by its columns: views, stored views, tasks.
async fn readers(lake: &Lake, name: &str) -> Result<Vec<String>> {
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
    let readers = readers(lake, &name).await?;
    ensure!(readers.is_empty(), "{name} is used by {}, which name its columns: drop them first", readers.join(", "));
    let system = |c: &str| crate::sys::NAMES.contains(&c) || c == "_old_version" || c == "_deleted" || c.contains('~');
    ensure!(!system(column), "{column} is a system column: it stays as it is");
    let Some(stored) = m.stored(column).map(str::to_string) else {
        ensure!(matches!(change, Change::Drop { if_exists: true }), "{name} has no column {column}");
        return Ok(j!({"table": name, "unchanged": true}));
    };
    let partition = m.partition.as_deref().map(|p| p.split_once('(').map_or(p, |(_, c)| c.trim_end_matches(')')));
    let what = match change {
        Change::Rename(to) => {
            ensure!(!to.is_empty(), "a column needs a name");
            ensure!(!system(&to), "{to} is a system column's name");
            ensure!(m.stored(&to).is_none(), "{name} already has a column {to}");
            match to == stored {
                true => m.names.remove(&stored),
                false => m.names.insert(stored.clone(), to.clone()),
            };
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
            ensure!(role.is_empty(), "{column} is {name}'s {role} column: it stays");
            ensure!(m.live().count() > 1 + usize::from(m.stored("_deleted").is_some()), "{column} is {name}'s last column (DROP TABLE {name})");
            m.cluster.retain(|c| *c != stored);
            m.merge.remove(&stored);
            m.names.remove(&stored);
            m.dropped.push(stored.clone());
            j!({"dropped": column})
        }
        Change::Type(sql_type) => {
            let ctx = datafusion::prelude::SessionContext::new();
            ctx.sql(&format!("CREATE TABLE t (c {sql_type})")).await?;
            let new = crate::write::stored(ctx.table("t").await?.schema().field(0).data_type());
            let at = m.columns.iter().position(|(c, _)| *c == stored).expect("a live column");
            let old = crate::query::dtype(&m.columns[at].1)?;
            if old == new {
                return Ok(j!({"table": name, "unchanged": true}));
            }
            ensure!(widens(&old, &new), "{column} is {old}: a column's type can only widen (a smaller integer to a bigger one, FLOAT to DOUBLE, a DECIMAL to more digits), which every file already written still reads as");
            ensure!(partition != Some(stored.as_str()), "{column} is {name}'s partition_by column: its type stays");
            m.columns[at].1 = crate::query::type_name(&new);
            j!({"column": column, "type": m.columns[at].1})
        }
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
fn widens(old: &datafusion::arrow::datatypes::DataType, new: &datafusion::arrow::datatypes::DataType) -> bool {
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

/// `DROP VIEW` or `DROP MATERIALIZED VIEW`: a stored view, or a live one with its tables and state.
async fn drop_view(lake: &Lake, name: &str, if_exists: bool) -> Result<Value> {
    if lake.cat.get::<crate::feeds::Feed>(&crate::feeds::feed_key(name)).await?.is_some() {
        let offsets: Vec<String> = lake.cat.scan::<u64>(&crate::store::producer_key(&format!("feed:{name}:")), &crate::store::producer_key(&format!("feed:{name};"))).await?.into_iter().map(|(k, _)| k).collect();
        let gone: Vec<String> = [crate::feeds::feed_key(name), table_key(name)].into_iter().chain(offsets.iter().cloned()).collect();
        lake.cat.commit(vec![], &gone).await?;
        crate::log::forget_producers(offsets.into_iter().map(|k| k[2..].to_string())); // (its offsets: made again, it starts over)
        return Ok(j!({"view": name, "dropped": true}));
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
    for table in [name.to_string(), kept, crate::sys::deleted(name)] {
        if let Some(meta) = lake.cat.get::<TableMeta>(&table_key(&table)).await? {
            for format in &meta.publish {
                crate::delta::unpublish(lake, &table, format).await?;
            }
            gone.push(table_key(&table));
        }
    }
    lake.cat.commit(vec![], &gone).await?;
    crate::log::forget_producers(producers); // (a view made again under this name starts over)
    crate::views::forget(lake);
    Ok(j!({"view": shown, "dropped": true}))
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
    let gone: Vec<String> = [crate::views::view_key(name), format!("w/{name}")].into_iter().chain(producers.iter().map(|p| crate::store::producer_key(p))).collect();
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

/// Might `sql` name table `t`? A word match on its last part (a session registers the tables that
/// might be read: one too many costs nothing).
pub fn mentions(sql: &str, t: &str) -> bool {
    let last = split(t).1.to_lowercase();
    sql.to_lowercase().split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '-')).any(|w| w == last)
}
