//! Rights: what a principal may do, decided in one place (design v2.1 §5 "Rights: one module, two parts", step 6).
//!
//! `may(who, action, object)` is the gate. A door, a statement's write, a stream of a table's rows and a lock ask it, and
//! nothing else reads a user's grants or compares a role to decide one of these. A principal (`auth::Principal`) is a
//! role and, for a user who isn't a superuser, its grants. A token, a superuser and an open node have no grants: their
//! role decides. A user's grants decide what it may do to a table, and it never defines the lake. Every refusal is a
//! `Refusal`, insufficient_privilege (42501) at every door, in the words made here.
//!
//! Step 6's later pieces fill this in behind the same call (l3/rights-prep.md): what the principal sees (its `Scope`,
//! which answers 42P01 for an object it doesn't see), the rights epoch, a grant's row predicate and a role's ceiling.

use crate::auth::{catalog_name, Principal, Role};
use crate::write::Stmt;

/// What a principal asks to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Read some of a table's columns: SELECT. (Which ones, planning asks of the grants.)
    Select,
    /// Read every column of a table: a stream of its rows (Kafka's fetch, Flight's DoGet, `/watch`, `/lookup`, MCP's
    /// `watch`, an Iceberg engine's read) and RETURNING's rows.
    SelectAll,
    Insert,
    Update,
    Delete,
    /// Take a table's rows as an UPDATE does: `SELECT … FOR UPDATE`, which needs UPDATE's privilege, as Postgres asks.
    Lock,
    /// Make or change the lake's tables, views, schemas, routines or users.
    Define,
    /// Write files outside the lake: `COPY (…) TO 'url'`.
    Export,
}

impl Action {
    /// The action a grant's privilege names (`select`, `insert`, `update`, `delete`).
    pub fn of(privilege: &str) -> Option<Action> {
        match privilege {
            "select" => Some(Action::Select),
            "insert" => Some(Action::Insert),
            "update" => Some(Action::Update),
            "delete" => Some(Action::Delete),
            _ => None,
        }
    }

    /// The privilege a user is granted for it (None: a user is never granted it: a superuser's).
    pub fn privilege(self) -> Option<&'static str> {
        match self {
            Action::Select | Action::SelectAll => Some("select"),
            Action::Insert => Some("insert"),
            Action::Update | Action::Lock => Some("update"),
            Action::Delete => Some("delete"),
            Action::Define | Action::Export => None,
        }
    }

    /// The role a principal with no grants (a token, a superuser, an open node) needs for it.
    fn role(self) -> Role {
        match self {
            Action::Select | Action::SelectAll => Role::Read,
            Action::Insert | Action::Update | Action::Delete | Action::Lock => Role::Write,
            Action::Define | Action::Export => Role::Admin,
        }
    }

    /// Does it change a table's rows?
    fn writes(self) -> bool { matches!(self, Action::Insert | Action::Update | Action::Delete) }
}

/// What an action is done to.
#[derive(Clone, Copy, Debug)]
pub enum Object<'a> {
    /// A table or a view, as SQL or the catalog names it (`public.t` is `t`).
    Table(&'a str),
    /// The lake: its definitions, and the files outside it.
    Lake,
}

/// Why `may` said no: insufficient_privilege (42501) at every door, in words that say what to grant where a grant would
/// do. (`?` makes it the coded error every door reads the SQLSTATE of.)
#[derive(Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: &'static str,
    pub text: String,
}

impl Refusal {
    fn new(text: String) -> Refusal { Refusal { code: "42501", text } }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.text) }
}

impl From<Refusal> for anyhow::Error {
    fn from(r: Refusal) -> anyhow::Error { crate::codes::coded(r.code, r.text) }
}

/// May `who` do `action` to `object`?
pub fn may(who: &Principal, action: Action, object: Object) -> Result<(), Refusal> {
    let superuser = who.role == Role::Admin && who.access.is_none();
    // (a role first: a token's, and a user, who never defines the lake or writes outside it)
    if who.access.as_ref().map_or(who.role < action.role(), |_| action.privilege().is_none()) {
        return Err(Refusal::new(format!("this {} may not {}", if who.access.is_some() { "user" } else { "token" }, match action {
            Action::Select | Action::SelectAll => "read",
            Action::Insert | Action::Update | Action::Delete => "write",
            Action::Lock => "write: FOR UPDATE takes rows as an UPDATE does",
            Action::Define => "change the lake's tables, views, schemas, routines or users (an admin token, or a superuser, does)",
            Action::Export => "write files outside the lake",
        })));
    }
    let Object::Table(name) = object else { return Ok(()) };
    let table = catalog_name(name);
    // (the lake's records and a table's old versions: Pondra writes them, and only a superuser streams their rows; SQL
    // reads the records through `pondra.audit` and `pondra.history`, which show a user its own statements only)
    if crate::sys::hidden(&table) && (action.writes() || action == Action::Define) {
        return Err(Refusal::new(format!("{table} is written by Pondra alone")));
    }
    if crate::sys::hidden(&table) && action == Action::SelectAll && !superuser {
        return Err(Refusal::new(format!("permission denied: {table} is Pondra's own, and its rows stream to a superuser only")));
    }
    let (Some(a), Some(privilege)) = (&who.access, action.privilege()) else { return Ok(()) };
    let granted = match action {
        Action::SelectAll => a.columns(privilege, &table) == Some(None),
        _ => a.may(privilege, &table),
    };
    if granted {
        return Ok(());
    }
    let p = privilege.to_uppercase();
    let what = if action == Action::SelectAll { format!("{p} on every column of") } else { format!("{p} on") };
    Err(Refusal::new(format!("permission denied: {what} {table} (GRANT {p} ON {table} TO {})", who.name)))
}

/// The same, for the request being served. None set is the node's own work: it may do anything an admin token may, and
/// a door still writes no row of Pondra's own tables for it.
pub fn may_here(action: Action, object: Object) -> anyhow::Result<()> { Ok(may(&served(), action, object)?) }

/// The principal of the request being served (none set: the node's own, an admin token's rights).
fn served() -> Principal { crate::auth::current().unwrap_or_else(|| Principal::token(Role::Admin)) }

/// May the request being served, with `role` (its door's: its principal's), run this write? Every action the write asks
/// of its table, as `asks` lists them.
pub fn may_run(role: Role, stmt: &Stmt) -> anyhow::Result<()> {
    if crate::temp::own(stmt) {
        return Ok(()); // (the session's own tables and views: any role may keep them)
    }
    let who = match crate::auth::current() {
        Some(p) => Principal { role, ..p },
        None => Principal::of(role),
    };
    // (CLONE is enough to make a branch, and its owner drops it: `branch::door` checks)
    if who.access.is_some() && matches!(stmt, Stmt::Ddl(ds) if ds.len() == 1 && matches!(ds[0], crate::ddl::Ddl::CreateDatabase { clone: Some(_), .. } | crate::ddl::Ddl::DropDatabase { .. })) {
        return Ok(());
    }
    let (table, actions) = asks(stmt);
    let object = if table.is_empty() { Object::Lake } else { Object::Table(&table) };
    for action in actions {
        may(&who, action, object)?;
    }
    Ok(())
}

/// The table a write changes (as the catalog names it; empty: the lake's) and what it asks to do to it.
pub fn asks(stmt: &Stmt) -> (String, Vec<Action>) {
    match stmt {
        Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) | Stmt::CopyInto(t, ..) => (catalog_name(t), vec![Action::Insert]), // (COPY INTO's rows are an INSERT's)
        Stmt::Update(t, ..) => (catalog_name(t), vec![Action::Update]),
        Stmt::Delete(t, _) => (catalog_name(t), vec![Action::Delete]),
        Stmt::Merge(m) => (catalog_name(&m.target), m.privileges().into_iter().filter_map(Action::of).collect()),
        // (a `$` table is Pondra's: a superuser's CREATE or definition of one is refused as a row write to it is. A replace's
        // staged table is defined by the leader's own steps, which no door checks: `write::create_as`)
        Stmt::Create(c) => (catalog_name(&crate::text::object(&c.name)), vec![Action::Define]),
        Stmt::Define(t, _) | Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) => (catalog_name(t), vec![Action::Define]),
        // (an extension's statement: the right its entry names, `statements::role`, an admin's for each today)
        Stmt::Ddl(ds) if ds.iter().any(|d| matches!(d, crate::ddl::Ddl::Entry { .. })) && crate::statements::role(ds) == Role::Admin => (String::new(), vec![Action::Define]),
        Stmt::Ddl(_) => (String::new(), vec![Action::Define]),
        Stmt::CopyTo(..) => (String::new(), vec![Action::Export]),
        // (a statement written wrong, or a temporary object of another session's: its own answer says why)
        _ => (String::new(), vec![]),
    }
}

/// What rows written to a table ask beyond INSERT, as SQL's own writes do (S11): rows that replace their key's
/// (`replaces`: ON CONFLICT DO UPDATE asks UPDATE) and rows taken out (`deletes`: DELETE). A door that takes rows or
/// another engine's commits (`/append`, Flight's DoPut, Kafka's produce, COPY FROM, an Iceberg engine's commit) asks them
/// once it knows, so a user granted INSERT alone neither overwrites nor deletes through a back door.
pub fn changing(replaces: bool, deletes: bool) -> impl Iterator<Item = Action> {
    [(replaces, Action::Update), (deletes, Action::Delete)].into_iter().filter_map(|(asked, action)| asked.then_some(action))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::users::{Access, Grant, On};
    use std::sync::Arc;

    fn user(grants: &[(&str, On, &[&str])]) -> Principal {
        let grants = grants.iter().map(|(p, on, cols)| Grant { privilege: p.to_string(), on: on.clone(), columns: cols.iter().map(|c| c.to_string()).collect() }).collect();
        Principal { name: "ann".into(), access: Some(Arc::new(Access::granted(grants))), ..Principal::of(Role::Read) }
    }

    fn said(r: Result<(), Refusal>) -> String { r.err().map(|r| r.text).unwrap_or_else(|| "ok".into()) }

    /// Every action, by a token of each role, a superuser and a user with grants: what `may` answers, in its words.
    #[test]
    fn may_decides_from_the_role_and_the_grants() {
        let t = Object::Table("public.t");
        let reader = Principal::token(Role::Read);
        let writer = Principal::token(Role::Write);
        let admin = Principal::token(Role::Admin);
        assert_eq!(said(may(&reader, Action::Select, t)), "ok");
        assert_eq!(said(may(&reader, Action::SelectAll, t)), "ok");
        assert_eq!(said(may(&reader, Action::Insert, t)), "this token may not write");
        assert_eq!(said(may(&reader, Action::Lock, t)), "this token may not write: FOR UPDATE takes rows as an UPDATE does");
        assert_eq!(said(may(&Principal::token(Role::None), Action::Select, t)), "this token may not read");
        assert_eq!(said(may(&writer, Action::Delete, t)), "ok");
        assert!(said(may(&writer, Action::Define, Object::Lake)).starts_with("this token may not change the lake's tables"));
        assert_eq!(said(may(&writer, Action::Export, Object::Lake)), "this token may not write files outside the lake");
        assert_eq!(said(may(&admin, Action::Define, t)), "ok");
        assert_eq!(said(may(&admin, Action::Export, Object::Lake)), "ok");

        let ann = user(&[("select", On::Table("t".into()), &["a"]), ("insert", On::Table("t".into()), &[]), ("select", On::Schema("s".into()), &[])]);
        assert_eq!(said(may(&ann, Action::Select, t)), "ok"); // (some columns: planning picks them)
        assert_eq!(said(may(&ann, Action::SelectAll, t)), "permission denied: SELECT on every column of t (GRANT SELECT ON t TO ann)");
        assert_eq!(said(may(&ann, Action::SelectAll, Object::Table("s.u"))), "ok");
        assert_eq!(said(may(&ann, Action::Insert, t)), "ok");
        assert_eq!(said(may(&ann, Action::Update, t)), "permission denied: UPDATE on t (GRANT UPDATE ON t TO ann)");
        assert_eq!(said(may(&ann, Action::Lock, t)), "permission denied: UPDATE on t (GRANT UPDATE ON t TO ann)");
        assert_eq!(said(may(&ann, Action::Delete, Object::Table("u"))), "permission denied: DELETE on u (GRANT DELETE ON u TO ann)");
        assert!(said(may(&ann, Action::Define, t)).starts_with("this user may not change the lake's tables"));
        assert_eq!(said(may(&ann, Action::Export, Object::Lake)), "this user may not write files outside the lake");
        assert_eq!(may(&ann, Action::Update, t).unwrap_err().code, "42501");
    }

    /// The lake's records and a table's old versions: nobody writes them, and only a superuser streams their rows.
    #[test]
    fn hidden_tables_are_pondras() {
        let audit = Object::Table("pondra$audit");
        let all = user(&[("select", On::Lake, &[]), ("insert", On::Lake, &[])]);
        assert_eq!(said(may(&Principal::token(Role::Admin), Action::Insert, audit)), "pondra$audit is written by Pondra alone");
        assert_eq!(said(may(&Principal::token(Role::Admin), Action::Define, audit)), "pondra$audit is written by Pondra alone");
        assert_eq!(said(may(&all, Action::Insert, audit)), "pondra$audit is written by Pondra alone");
        assert_eq!(said(may(&Principal::token(Role::Admin), Action::SelectAll, audit)), "ok");
        assert_eq!(said(may(&Principal::token(Role::Write), Action::SelectAll, audit)), "permission denied: pondra$audit is Pondra's own, and its rows stream to a superuser only");
        assert_eq!(said(may(&all, Action::SelectAll, audit)), "permission denied: pondra$audit is Pondra's own, and its rows stream to a superuser only");
    }

    /// A write asks of its table what SQL's own would, and rows written ask UPDATE and DELETE as they need them.
    #[test]
    fn writes_ask_their_privileges() {
        assert_eq!(changing(false, false).collect::<Vec<_>>(), vec![]);
        assert_eq!(changing(true, true).collect::<Vec<_>>(), vec![Action::Update, Action::Delete]);
        assert_eq!(changing(false, true).collect::<Vec<_>>(), vec![Action::Delete]);
        assert_eq!(asks(&Stmt::Delete("public.t".into(), None)), ("t".to_string(), vec![Action::Delete]));
        assert_eq!(asks(&Stmt::Ddl(vec![])), (String::new(), vec![Action::Define]));
    }
}
