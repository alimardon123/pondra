//! Protected databases (ADR-058, step 3). `ALTER DATABASE db SET (protected = true)` marks a database
//! whose project objects change only through a deploy: what a project's last finished deploy declared
//! (`deploy::declared`) is refused by name, unless a deploy is making it (`DEPLOYING`). Rows, people
//! and objects no project declared stay as they were, in the project's schema too. A branch isn't
//! protected: its `z/` keys are never copied (`branch::make`). Lifting it is an admin's statement,
//! written to `pondra.audit` (class `role`) and shown in `pondra.databases` until it is set again.
use crate::ddl::{self, Ddl};
use crate::store::{json, Lake};
use crate::write::Stmt;
use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Where the protection is kept: under `z/`, which a branch never copies.
pub const KEY: &str = "z/protect";

/// A database's protection, and its last change: when, and by whom.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Protection {
    pub protected: bool,
    pub at_ms: u64,
    pub by: String,
}

tokio::task_local! {
    /// Set by a deploy around its statements: a project's objects may change while it runs.
    pub static DEPLOYING: ();
}

/// Whether a deploy is making the project true in this task (its statements, and a Python procedure's
/// calls back while it runs: `auth::lend`). A procedure started with `pondra.start` is not: it would
/// outlive the deploy, so it has no exemption (`routines::start` refuses to start one).
pub fn deploying() -> bool { DEPLOYING.try_with(|_| ()).is_ok() }

pub async fn of(lake: &Lake) -> Result<Option<Protection>> { lake.cat.get(KEY).await }

pub async fn protected(lake: &Lake) -> Result<bool> { Ok(of(lake).await?.is_some_and(|p| p.protected)) }

/// `ALTER DATABASE db SET (protected = on)`: run on db's own leader, under its lock (`ddl::carry_out`).
pub async fn set(lake: &Lake, database: &str, on: bool, by: &str) -> Result<Value> {
    let here = ddl::lake_name(lake);
    ensure!(database == here, "this is the database {here}: ALTER DATABASE {database} SET (protected = …) is run on {database}");
    if !on && !protected(lake).await? {
        return Ok(j!({"database": database, "protected": false, "unchanged": true})); // (nothing to lift)
    }
    let now = Protection { protected: on, at_ms: crate::log::now_ms(), by: by.to_string() };
    lake.cat.commit(vec![(KEY.to_string(), json(&now))], &[]).await?;
    Ok(j!({"database": database, "protected": on}))
}

/// Who a protection change is made by, when the statement doesn't say (`pondra.databases`).
pub fn stamp(d: &mut Ddl) {
    if let Ddl::Protect { by, .. } = d {
        if by.is_empty() {
            *by = crate::auth::current().map(|p| p.name).filter(|n| !n.is_empty()).unwrap_or_else(|| "admin".into());
        }
    }
}

/// Where a statement comes in (`write::on_node_listed`): stamps a protection change with who makes
/// it, and refuses a change to a project's object in a protected database.
pub async fn door(lake: &Lake, mut stmt: Stmt) -> Result<Stmt> {
    if let Stmt::Ddl(ds) = &mut stmt {
        ds.iter_mut().for_each(stamp);
    }
    for t in touched(&stmt) {
        check(lake, t).await?;
    }
    Ok(stmt)
}

/// The same refusal for one statement sent straight to the cluster's door (`server::ddl`).
pub async fn check_ddl(lake: &Lake, d: &Ddl) -> Result<()> {
    for t in ddl_touches(d) {
        check(lake, t).await?;
    }
    Ok(())
}

/// The same refusal for `POST /tables/{name}`, which may add columns to a table that is there.
pub async fn check_table(lake: &Lake, name: &str) -> Result<()> { check(lake, Touch::Object(name.to_string())).await }

/// What a change touches, by name.
enum Touch {
    Object(String),                         // a table, view, routine, secret or attachment
    Schema(String),                         // DROP SCHEMA: the schema, and what is in it
    Roles(Vec<String>),                     // a role altered or dropped, or granted to or revoked from
    Database(Option<String>, &'static str), // a database refreshed or dropped (None: this one)
}

/// What a statement changes. Rows, and what a session keeps, change no object.
fn touched(stmt: &Stmt) -> Vec<Touch> {
    match stmt {
        Stmt::Create(c) if c.temporary => vec![], // (a session's: `temp.rs`)
        Stmt::Create(c) => vec![Touch::Object(crate::write::object(&c.name))],
        Stmt::Define(t, _) | Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) => vec![Touch::Object(t.clone())],
        Stmt::Ddl(ds) => ds.iter().flat_map(ddl_touches).collect(),
        Stmt::Insert(..) | Stmt::InsertInto(..) | Stmt::Update(..) | Stmt::Delete(..) | Stmt::Merge(_) | Stmt::Invalid(_) | Stmt::CopyTo(..) | Stmt::TempView(..) | Stmt::TempSecret(..) => vec![],
    }
}

/// What a statement the leader carries out changes, by name.
fn ddl_touches(d: &Ddl) -> Vec<Touch> {
    let object = |n: &str| vec![Touch::Object(n.to_string())];
    match d {
        Ddl::DropTable { name, .. } | Ddl::Undrop { name } | Ddl::Clone { name, .. } | Ddl::CreateView { name, .. } | Ddl::CreateExternal { name, .. }
        | Ddl::CreateMaterialized { name, .. } | Ddl::DropView { name, .. } | Ddl::Attach { name, .. } | Ddl::Detach { name, .. } | Ddl::CreateRoutine { name, .. }
        | Ddl::DropRoutine { name, .. } | Ddl::CreateSecret { name, .. } | Ddl::AttachOutside { name, .. } | Ddl::DropSecret { name, .. } | Ddl::CreateTask { name, .. }
        | Ddl::DropTask { name, .. } | Ddl::AlterTask { name, .. } | Ddl::DetachView { name } => object(name),
        Ddl::AlterColumn { table, .. } | Ddl::Constraint { table, .. } => object(table),
        Ddl::RenameTable { name, to } => vec![Touch::Object(name.clone()), Touch::Object(to.clone())],
        Ddl::Replacing { name, then } => std::iter::once(Touch::Object(name.clone())).chain(ddl_touches(then)).collect(),
        Ddl::Unless { then, .. } => ddl_touches(then),
        Ddl::Object(crate::objects::Op::OrAlterTable { name, .. }) => object(name),
        Ddl::Object(crate::objects::Op::Comment { .. }) => vec![], // (a comment changes no object's definition)
        Ddl::DropSchema { name, .. } => vec![Touch::Schema(name.clone())],
        Ddl::Refresh { database, .. } => vec![Touch::Database(database.clone(), "refreshed")],
        Ddl::DropDatabase { name, .. } => vec![Touch::Database(Some(name.clone()), "dropped")],
        Ddl::Users(c) => users(c),
        // (none of these is a project's: `deploy::head` refuses the kinds a project can't declare)
        Ddl::CreateSchema { .. } | Ddl::CreateDatabase { .. } | Ddl::Branch(_) | Ddl::Pin { .. } | Ddl::Unpin { .. } | Ddl::Deploy { .. } | Ddl::ExecuteTask { .. }
        | Ddl::RunLog | Ddl::AuditLog | Ddl::HistoryLog | Ddl::Shares(_) | Ddl::Sequence(_) | Ddl::Index(_) | Ddl::Type(_) | Ddl::Protect { .. } => vec![],
    }
}

/// A role's change: the roles it names (people's membership and tokens are the environment admin's).
fn users(c: &crate::users::Change) -> Vec<Touch> {
    use crate::users::Change;
    match c {
        Change::Alter { name, .. } | Change::Drop { name, .. } => vec![Touch::Roles(vec![name.clone()])],
        Change::Grant { to: roles, .. } | Change::Revoke { from: roles, .. } => vec![Touch::Roles(roles.clone())],
        Change::Create { .. } | Change::GrantRole { .. } | Change::RevokeRole { .. } | Change::CreateToken { .. } | Change::DropToken { .. } => vec![],
    }
}

/// Refuses a change to what a project declares in a protected database (none while a deploy runs).
async fn check(lake: &Lake, t: Touch) -> Result<()> {
    if deploying() {
        return Ok(());
    }
    match t {
        Touch::Object(name) => check_object(lake, &name).await,
        Touch::Schema(name) => check_schema(lake, &name).await,
        Touch::Roles(roles) => check_roles(lake, &roles).await,
        Touch::Database(name, what) => check_database(lake, name, what).await,
    }
}

/// The database a name is in (its name, and the lake when it isn't this one) and the name inside it.
async fn place(lake: &Lake, name: &str) -> (String, Option<Arc<Lake>>, String) {
    let here = ddl::lake_name(lake);
    match ddl::resolve(lake, name).await {
        Ok((Some(other), local)) => (name.split('.').next().unwrap_or_default().to_string(), Some(other), local),
        Ok((None, local)) => (here, None, local),
        Err(_) => (here, None, name.to_string()), // (not a name here: taken as written, and the statement fails all the same)
    }
}

async fn check_object(lake: &Lake, name: &str) -> Result<()> {
    let (db, other, local) = place(lake, name).await;
    let target = other.as_deref().unwrap_or(lake);
    if !protected(target).await? {
        return Ok(());
    }
    let owned = owner(&crate::deploy::declared(target).await?, |k, n| !matches!(k, "schema" | "role" | "grant") && n == local);
    refuse(owned, &db)
}

/// DROP SCHEMA: refused when the schema is a project's, or a project's object is in it.
async fn check_schema(lake: &Lake, schema: &str) -> Result<()> {
    if !protected(lake).await? {
        return Ok(());
    }
    let inside = format!("{schema}.");
    let owned = owner(&crate::deploy::declared(lake).await?, |k, n| (k == "schema" && n == schema) || (!matches!(k, "role" | "grant") && n.starts_with(&inside)));
    refuse(owned, &ddl::lake_name(lake))
}

async fn check_roles(lake: &Lake, roles: &[String]) -> Result<()> {
    if !protected(lake).await? {
        return Ok(());
    }
    let declared = crate::deploy::declared(lake).await?;
    for role in roles {
        refuse(owner(&declared, |k, n| makes_role(k, n, role)), &ddl::lake_name(lake))?;
    }
    Ok(())
}

/// A protected database isn't refreshed or dropped. `name` None is this database.
async fn check_database(lake: &Lake, name: Option<String>, what: &str) -> Result<()> {
    let here = ddl::lake_name(lake);
    let name = name.unwrap_or_else(|| here.clone());
    let other = if name == here { None } else { attached_as(lake, &name) };
    let db = match (name == here, &other) {
        (true, _) => lake,
        (false, Some(o)) => &**o,
        (false, None) => return Ok(()), // (not attached here: the statement says so)
    };
    if protected(db).await? {
        bail!("permission denied: database {name} is protected, so it isn't {what}: ALTER DATABASE {name} SET (protected = false) lifts it (an admin's)");
    }
    Ok(())
}

fn attached_as(lake: &Lake, name: &str) -> Option<Arc<Lake>> {
    lake.attached.read().unwrap().iter().find(|(n, _)| n == name).map(|(_, l)| l.clone())
}

/// The first declared key that `mine` says is a project's own, with its project's name.
fn owner(declared: &BTreeMap<String, String>, mine: impl Fn(&str, &str) -> bool) -> Option<(String, String)> {
    declared.iter().find(|(k, _)| {
        let (kind, name) = crate::deploy::split_key(k.as_str());
        mine(kind, name)
    }).map(|(k, p)| (k.clone(), p.clone()))
}

/// Whether a project's key makes `role` one of its roles: declared (`role r`), or granted to (`grant … TO r`).
fn makes_role(kind: &str, name: &str, role: &str) -> bool {
    (kind == "role" && name == role) || (kind == "grant" && grantees(name).iter().any(|g| g == role))
}

/// The roles a project's grant names: the text after its last TO (`TO analyst, "Ann"`), as SQL names them.
fn grantees(grant: &str) -> Vec<String> {
    let Some(at) = grant.to_ascii_uppercase().rfind(" TO ") else { return vec![] };
    grant[at + 4..].split(',').map(|r| {
        let r = r.trim();
        if r.starts_with('"') { r.trim_matches('"').to_string() } else { r.to_lowercase() }
    }).collect()
}

/// The refusal for a change to what `owned` names (its key and its project), in database `db`.
fn refuse(owned: Option<(String, String)>, db: &str) -> Result<()> {
    match owned {
        Some((key, project)) => bail!("permission denied: {key} is project {project}'s, and {db} is protected: change it in the project and deploy it (or an admin lifts the protection: ALTER DATABASE {db} SET (protected = false))"),
        None => Ok(()),
    }
}
