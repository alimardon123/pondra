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
    /// Write the lake where no table is named: CHECKPOINT, the workspace's files (`pondra.git_*`).
    Write,
    /// Run code on the node: DO, a Python file or notebook (`pondra.run`).
    RunCode,
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
            Action::Define | Action::Export | Action::Write | Action::RunCode => None,
        }
    }

    /// What a statement that needs `role` asks of the lake: an extension's, by its entry (`statements::role`).
    fn of_role(role: Role) -> Option<Action> {
        match role {
            Role::Admin => Some(Action::Define),
            Role::Write => Some(Action::Write),
            Role::Read => Some(Action::Select),
            Role::None => None,
        }
    }

    /// The role it needs: of a principal with no grants (a token, a superuser, an open node), and of a user for what no
    /// grant names (a user's role is a writer's when it is granted a write, and never an admin's).
    fn role(self) -> Role {
        match self {
            Action::Select | Action::SelectAll => Role::Read,
            Action::Insert | Action::Update | Action::Delete | Action::Lock | Action::Write => Role::Write,
            Action::Define | Action::Export | Action::RunCode => Role::Admin,
        }
    }

    /// What it is, in a refusal's words ("this token may not …").
    fn words(self) -> &'static str {
        match self {
            Action::Select | Action::SelectAll => "read",
            Action::Insert | Action::Update | Action::Delete | Action::Write => "write",
            Action::Lock => "take rows FOR UPDATE, as an UPDATE does",
            Action::Define => "change the lake's tables, views, schemas, routines or users",
            Action::Export => "write files outside the lake",
            Action::RunCode => "run code on the node",
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

    /// A table the principal doesn't know is there (`Scope::knows`), answered as one that isn't: 42P01, in the words a
    /// write to a table that isn't there has. The audit log still writes it as refused (`told_unseen`).
    fn unseen(table: &str) -> Refusal {
        told_unseen();
        Refusal { code: "42P01", text: format!("no table {table}") }
    }
}

tokio::task_local! {
    /// Around a statement the audit log writes (`noting`): set when it was told that a table it holds no privilege on
    /// isn't there. Its asker can't tell that from a table that isn't, and the audit log writes it as the refusal it is.
    static UNSEEN: std::sync::atomic::AtomicBool;
}

/// `f`, and whether it was told that a table it holds no privilege on isn't there (`Refusal::unseen`, `query::guarded`).
pub async fn noting<F: std::future::Future>(f: F) -> (F::Output, bool) {
    UNSEEN.scope(Default::default(), async { (f.await, UNSEEN.with(|u| u.load(std::sync::atomic::Ordering::Relaxed))) }).await
}

/// The request being served was told that a table it holds no privilege on isn't there (`noting`).
pub fn told_unseen() { let _ = UNSEEN.try_with(|u| u.store(true, std::sync::atomic::Ordering::Relaxed)); }

/// A statement's planning error as the request being served is told it. A column that isn't there is said without the
/// columns that are (DataFusion lists them, or the nearest one, "Did you mean …"), unless the scope is whole: they may
/// be columns it may not read, which no answer names (`query::Guarded::shown`).
pub fn said(e: datafusion::error::DataFusionError) -> datafusion::error::DataFusionError {
    use datafusion::{common::SchemaError, error::DataFusionError};
    if scope().whole() {
        return e;
    }
    let missing = match e.find_root() {
        DataFusionError::SchemaError(s, _) => match s.as_ref() {
            SchemaError::FieldNotFound { field, valid_fields } if !valid_fields.is_empty() => Some(field.clone()),
            _ => None,
        },
        _ => None,
    };
    match missing {
        Some(field) => DataFusionError::SchemaError(Box::new(SchemaError::FieldNotFound { field, valid_fields: vec![] }), Box::new(None)),
        None => e,
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.text) }
}

impl From<Refusal> for anyhow::Error {
    fn from(r: Refusal) -> anyhow::Error { crate::codes::coded(r.code, r.text) }
}

/// May `who` do `action` to `object`?
pub fn may(who: &Principal, action: Action, object: Object) -> Result<(), Refusal> { may_in(who, action, object, false) }

/// `may`, and `sql`: asked for SQL's own write (`may_run`), which reads the rows it changes through the writer's filters on
/// them (`change.rs`). Every other door's UPDATE or DELETE replaces or deletes rows by their key, unread (`writer::may`).
fn may_in(who: &Principal, action: Action, object: Object, sql: bool) -> Result<(), Refusal> {
    let superuser = who.superuser();
    // (a role first: a token's, a user's for what no grant names, and whoever isn't signed in, which sees nothing)
    let need = action.role();
    let token = who.scope.whole() || who.role == Role::None;
    if (token || action.privilege().is_none()) && who.role < need {
        let needs = match need {
            Role::Admin => "an admin token, or a superuser",
            Role::Write => "a write token, or a user granted a write",
            _ => "a token, or a user's sign-in",
        };
        return Err(Refusal::new(format!("this {} may not {}: it needs {needs}", if token { "token" } else { "user" }, action.words())));
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
    let (Some(a), Some(privilege)) = (&who.scope.access, action.privilege()) else { return Ok(()) };
    let granted = match action {
        Action::SelectAll => a.columns(privilege, &table) == Some(None),
        _ => a.may(privilege, &table),
    };
    // (every column and every row: a table some of whose rows its grants hide is read with SELECT, which filters them)
    if granted && action == Action::SelectAll && a.rows(privilege, &table, false).is_some() {
        return Err(Refusal::new(format!("permission denied: {table} shows {} some of its rows only (a filter on its rows in a grant): read it with SELECT", who.name)));
    }
    // (a load's rows replace their keys' rows or delete them, unread: a writer some of whose rows its grants hide changes them
    // with SQL, which reads them through the filter, so it never replaces or deletes a row it can't read)
    if granted && !sql && matches!(action, Action::Update | Action::Delete) && [privilege, "select"].iter().any(|p| a.rows(p, &table, false).is_some()) {
        let held = match a.rows("select", &table, false) {
            Some(_) => format!("shows {} some of its rows only", who.name),
            None => format!("lets {} {privilege} some of its rows only", who.name),
        };
        return Err(Refusal::new(format!("permission denied: {table} {held} (a filter on its rows in a grant): change them with SQL's UPDATE, DELETE, MERGE or INSERT … ON CONFLICT")));
    }
    if granted {
        return Ok(());
    }
    if !a.knows(&table) {
        return Err(Refusal::unseen(&table)); // (never "permission denied": that would say it is there)
    }
    let p = privilege.to_uppercase();
    let what = if action == Action::SelectAll { format!("{p} on every column of") } else { format!("{p} on") };
    Err(Refusal::new(format!("permission denied: {what} {table} (GRANT {p} ON {table} TO {})", who.name)))
}

/// The same, for the request being served. The node's own work (`auth::as_node`) may do anything an admin token may,
/// and a door still writes no row of Pondra's own tables for it. Work with neither is refused (`nobody`).
pub fn may_here(action: Action, object: Object) -> anyhow::Result<()> { Ok(may(&served(action)?, action, object)?) }

/// The principal of the request being served: the node's own work's is an admin token.
fn served(action: Action) -> Result<Principal, Refusal> {
    match crate::auth::current() {
        Some(p) => Ok(p),
        None if crate::auth::node_work() => Ok(Principal::node()),
        None => Err(nobody(action)),
    }
}

/// Work that no request is served by and that isn't the node's own: work a request started, spawned without its caller
/// (`auth::carried`). It gets no rights, rather than an admin's.
fn nobody(action: Action) -> Refusal {
    Refusal::new(format!("nobody asked for this, so it may not {}: work a request starts runs as its caller (auth::carried)", action.words()))
}

/// The same, for the request being served with `role`: its door's, a statement's `Who` (step 8's session carries one
/// principal, and this goes).
pub fn may_as(role: Role, action: Action, object: Object) -> anyhow::Result<()> { Ok(may(&as_role(role, action)?, action, object)?) }

/// The principal of the request being served, with `role` (the node's own work: a token of it). `action` names a refusal.
fn as_role(role: Role, action: Action) -> Result<Principal, Refusal> {
    match crate::auth::current() {
        Some(p) => Ok(Principal { role, ..p }),
        None if crate::auth::node_work() => Ok(Principal::of(role)),
        None => Err(nobody(action)),
    }
}

/// May the request being served, with `role` (`may_as`), run this write? Every action the write asks of its table, as
/// `asks` lists them.
pub fn may_run(role: Role, stmt: &Stmt) -> anyhow::Result<()> {
    if crate::temp::own(stmt) {
        return Ok(()); // (the session's own tables and views: any role may keep them)
    }
    let (table, actions) = asks(stmt);
    let Some(&first) = actions.first() else { return Ok(()) };
    let who = as_role(role, first)?;
    // (CLONE is enough to make a branch, and its owner drops it: `branch::door` checks)
    if !who.scope.whole() && matches!(stmt, Stmt::Ddl(ds) if ds.len() == 1 && matches!(ds[0], crate::ddl::Ddl::CreateDatabase { clone: Some(_), .. } | crate::ddl::Ddl::DropDatabase { .. })) {
        return Ok(());
    }
    let object = if table.is_empty() { Object::Lake } else { Object::Table(&table) };
    for action in actions {
        may_in(&who, action, object, true)?;
    }
    Ok(())
}

/// What the request being served may see and read (design v2.1 §5, step 6 piece 2): everything (`whole`: a token, a
/// superuser, an open node, the node's own work), what a user's grants name, or nothing (whoever isn't signed in, and work
/// no one asked for). Listings ask `sees`, planning asks `columns`, the key lookup asks `reads`, the results cache keeps an
/// answer under `key`, and nothing outside this module reads a user's grants. There is no default: each scope is made as
/// one of these, by name. It also says who it is for, as far as planning asks (`superuser`, `asker`: M8 piece 3d-3b), which
/// only a principal tells it (`Principal::asking`).
#[derive(Clone)]
pub struct Scope {
    access: Option<std::sync::Arc<crate::users::Access>>,
    asker: Option<std::sync::Arc<Asker>>, // (None: made without a principal, which names no one and is no superuser's)
    key: Option<std::sync::Arc<str>>, // (`key` when its grants filter rows: theirs with its reader filled in; None: `Access::key`)
    writing: Option<std::sync::Arc<(String, String)>>, // (a write's table and its privilege's filter on its rows, ANDed to SELECT's: `writing`)
}

/// Who a scope is for, as planning asks it (`pondra.history`'s rows, `pondra.audit`): its principal's name, whether that
/// principal is an admin, and whether it runs the nodes. Set only from a principal (`Scope::asked_by`), never made of loose
/// values.
struct Asker {
    name: String,
    admin: bool,    // (its role is Admin: with a whole scope, a superuser's, `Scope::superuser`)
    operator: bool, // (whoever runs the nodes: `Principal::operator`, `Scope::operator`)
}

/// The scope of the request being served: its principal's (`Principal::scope`), or the node's own work's (`auth::as_node`:
/// `Principal::node`'s, whole). Work with neither sees nothing (M8): work a request started and lost its caller on the way
/// (spawned without `auth::carried`) reads no table and lists none, as `may_here` refuses it every right.
pub fn scope() -> Scope {
    match crate::auth::current() {
        Some(p) => p.scope,
        None if crate::auth::node_work() => crate::auth::Principal::node().scope,
        None => Scope::nothing(),
    }
}

impl Scope {
    /// Everything, this lake's and the lakes attached to it: a token's, a superuser's, an open node's, and the node's own
    /// work's (`auth::Principal::node`). `tools/scope_check.py` counts where one is made.
    pub fn everything() -> Scope { Scope { access: None, asker: None, key: None, writing: None } }

    /// Nothing: no table, column, name or code. Whoever isn't signed in (`Principal::of(Role::None)`), and work with no
    /// caller that isn't the node's own (`scope`).
    pub fn nothing() -> Scope { Scope { access: Some(std::sync::Arc::new(Default::default())), asker: None, key: None, writing: None } }

    /// What a user who isn't a superuser may see: what `access` grants it (`users::principal_with` makes it once, as the
    /// principal is made).
    pub fn granted(access: std::sync::Arc<crate::users::Access>) -> Scope { Scope { access: Some(access), asker: None, key: None, writing: None } }

    /// The scope a planning session was made for (`query::session_for`): the session carries it to everything planned in it
    /// (a `SessionConfig` extension), so no planning reads the task's caller. A session made without one sees nothing: it
    /// never falls back to whole (M8 piece 3).
    pub fn of_session(config: &datafusion::prelude::SessionConfig) -> Scope {
        config.get_extension::<Scope>().map_or_else(Scope::nothing, |s| (*s).clone())
    }

    /// Give `ctx` this scope, for everything planned in it to read (`of_session`).
    pub fn onto(&self, ctx: &datafusion::prelude::SessionContext) {
        ctx.state_ref().write().config_mut().set_extension(std::sync::Arc::new(self.clone()));
    }

    /// A user's own quota (`users::quota`); None: whole, which has none.
    pub fn limits(&self) -> Option<crate::users::Limits> { self.access.as_ref().map(|a| a.limits) }

    /// Everything: no grants narrow it.
    pub fn whole(&self) -> bool { self.access.is_none() }

    /// The same scope, for `p` (`Principal::asking`): its name, whether it is an admin, and whether it runs the nodes, for
    /// planning to ask (`superuser`, `asker`, `operator`). The only way a scope learns who it is for.
    pub(crate) fn asked_by(&self, p: &Principal) -> Scope {
        let asker = Asker { name: p.name.clone(), admin: p.role == Role::Admin, operator: p.operator };
        // (a filter that names its reader answers per reader: its filled-in text goes in the key, step 6 piece 5)
        let key = self.access.as_ref().filter(|a| a.filters().next().is_some()).map(|a| {
            let mut filled: Vec<String> = a.filters().map(|f| crate::row_filter::filled(f, Some(&asker.name))).collect();
            filled.sort();
            filled.dedup();
            crate::users::sha256(&serde_json::to_string(&(a.key(), filled)).unwrap_or_default())[..32].into()
        });
        Scope { access: self.access.clone(), asker: Some(std::sync::Arc::new(asker)), key, writing: None }
    }

    /// Is it a superuser's (`pondra.audit`, every row of `pondra.history`)? Whole, and its principal an admin. Never whole
    /// alone: a read token's is whole too, and a scope made without a principal is no one's.
    pub fn superuser(&self) -> bool { self.whole() && self.asker.as_ref().is_some_and(|a| a.admin) }

    /// The name of whom it is for (its principal's: "" for nobody's), or None for a scope made without a principal.
    pub fn asker(&self) -> Option<&str> { self.asker.as_ref().map(|a| a.name.as_str()) }

    /// Is it for whoever runs the nodes (one of their tokens, their key, the program that started this one:
    /// `Principal::operator`)? A scope made without a principal is not.
    pub fn operator(&self) -> bool { self.asker.as_ref().is_some_and(|a| a.operator) }

    /// The folder name its principal stages a joined page's rows under (`auth::slug`, as `auth::page_creates` checks): none
    /// for whoever runs the nodes, for a nameless one, and for a scope made without a principal.
    pub fn slug(&self) -> Option<String> {
        self.asker.as_ref().filter(|a| !a.operator && !a.name.is_empty()).map(|a| crate::text::hex(a.name.as_bytes()))
    }

    /// What an answer kept for it is kept under, beside its text (`App::remembers`, the Postgres port's too): what it
    /// holds, its grants and its names, never who holds it. So a GRANT or a REVOKE makes another key, and no answer made
    /// under other grants is ever given; "" when whole, as every token's and superuser's answers are kept today. Whom it is
    /// for isn't in it: what answers by that (`pondra.history`, `pondra.audit`) is never kept (`App::remembers`).
    /// Where its grants filter rows, their filters filled in for its reader are in it too (step 6 piece 5), so a filter on
    /// `current_user` keeps two readers' answers apart whatever else the key holds.
    pub fn key(&self) -> &str { self.key.as_deref().unwrap_or_else(|| self.access.as_ref().map_or("", |a| a.key())) }

    /// The rows of `table` (as the catalog names it) it may use for `privilege` (as `Action::privilege` names it), as one
    /// SQL condition over the table's columns with its reader filled in (`row_filter::filled`): None, every row (whole,
    /// or a grant with no filter). Its grants' filters join with OR. `query::Guarded` plans it (step 6 piece 5b).
    pub fn rows(&self, privilege: &str, table: &str) -> Option<String> { self.rows_of(privilege, table, false) }

    /// The same, for a table of another lake attached here (`l.t`, `l.s.t`): only grants naming it count (`named`).
    pub fn rows_named(&self, privilege: &str, table: &str) -> Option<String> { self.rows_of(privilege, table, true) }

    fn rows_of(&self, privilege: &str, table: &str, named: bool) -> Option<String> {
        let granted = self.granted_rows(privilege, table, named);
        match self.writing.as_deref() {
            Some((t, also)) if privilege == "select" && !named && t == table => Some(match granted {
                Some(read) => format!("({read}) AND ({also})"),
                None => also.clone(),
            }),
            _ => granted,
        }
    }

    /// The scope a write reads the rows of `table` it changes with (`change::rows_of`): its SELECT filter on them and
    /// `privilege`'s (UPDATE's or DELETE's, step 6 piece 5c) together, so it changes only rows both hold, and its own
    /// conditions run on those only (`query::Guarded`, its barrier). Every read of `table` it plans takes both, a
    /// subquery's too. Itself when `privilege` has no filter on `table`. Its key is its own: no answer is shared with a read's.
    pub fn writing(&self, privilege: &str, table: &str) -> Scope {
        let Some(also) = self.rows(privilege, table) else { return self.clone() };
        let key = crate::users::sha256(&serde_json::to_string(&(self.key(), privilege, table, &also)).unwrap_or_default())[..32].into();
        Scope { key: Some(key), writing: Some(std::sync::Arc::new((table.to_string(), also))), ..self.clone() }
    }

    /// Does a grant filter the rows of `table` it may use, for any privilege? Then a write of it is planned, so each filter
    /// reads the rows it changes: never a key's row read without planning (`txn::point_change`, `upsert_point`).
    pub fn filtered(&self, table: &str) -> bool { ["select", "insert", "update", "delete"].iter().any(|p| self.rows(p, table).is_some()) }

    fn granted_rows(&self, privilege: &str, table: &str, named: bool) -> Option<String> {
        let filters = self.access.as_ref()?.rows(privilege, table, named)?;
        let reader = self.asker.as_ref().map(|a| a.name.as_str());
        let filled: Vec<String> = filters.iter().map(|f| crate::row_filter::filled(f, reader)).collect();
        Some(match filled.as_slice() {
            [one] => one.clone(),
            many => many.iter().map(|f| format!("({f})")).collect::<Vec<_>>().join(" OR "),
        })
    }

    /// May it read these columns of `table` (as the catalog names it)? A key lookup asks before it answers without
    /// planning (`serve::point`), as `query::Guarded` asks of a plan's scan: every column it returns and its key's.
    /// A table some of whose rows its grants hide (`rows`) is read only as planned, through `query::Guarded`, never by its key.
    pub fn reads<'a>(&self, table: &str, mut columns: impl Iterator<Item = &'a str>) -> bool {
        if self.rows("select", table).is_some() {
            return false;
        }
        match self.columns(table) {
            None => false,
            Some(None) => true,
            Some(Some(granted)) => columns.all(|c| granted.contains(c)),
        }
    }

    /// Does it see `object`? A table or view (as the catalog names it, `l.t` for another lake's) when it may read any of
    /// its columns (SELECT, S27); the lake's own records (its users, identity providers, shares) when it is whole.
    pub fn sees(&self, object: Object) -> bool {
        match (&self.access, object) {
            (None, _) => true,
            (Some(a), Object::Table(t)) => a.may("select", t),
            (Some(_), Object::Lake) => false,
        }
    }

    /// The same, for a table or view of another lake attached here (`l.t`, `l.s.t`): only grants naming it count, as its
    /// reads ask (`named`). ON ALL TABLES and ON SCHEMA are this lake's, so they show none of another's.
    pub fn sees_named(&self, table: &str) -> bool { self.access.as_ref().is_none_or(|a| a.named("select", table).is_some()) }

    /// Does it know `table` (as the catalog names it) is there? Whole, or holding any privilege on it (SELECT, INSERT,
    /// UPDATE or DELETE, on it, its schema or the lake). A table it doesn't know is answered as one that isn't there,
    /// 42P01, at every door: planning finds no such table, and `may` says "no table", so no answer tells it the table
    /// exists, nor its columns. One it knows but may not read or write is refused, 42501.
    pub fn knows(&self, table: &str) -> bool { self.access.as_ref().is_none_or(|a| a.knows(table)) }

    /// The same, for a table of another lake attached here (`l.t`, `l.s.t`): only grants naming it count (`named`).
    pub fn knows_named(&self, table: &str) -> bool { self.access.as_ref().is_none_or(|a| a.knows_named(table)) }

    /// The columns of `table` (as the catalog names it) it reads: None, none; Some(None), every column; Some(these).
    pub fn columns(&self, table: &str) -> Option<Option<std::collections::HashSet<String>>> { self.access.as_ref().map_or(Some(None), |a| a.columns("select", table)) }

    /// The same, for a table of another lake attached here (`l.t`, `l.s.t`): only grants naming it count; ON ALL TABLES
    /// and ON SCHEMA are this lake's. `privilege` as `Action::privilege` names it.
    pub fn named(&self, privilege: &str, table: &str) -> Option<Option<std::collections::HashSet<String>>> { self.access.as_ref().map_or(Some(None), |a| a.named(privilege, table)) }

    /// May it use the secret `name` (GRANT USAGE ON SECRET)?
    pub fn secret(&self, name: &str) -> bool { self.access.as_ref().is_none_or(|a| a.secret(name)) }

    /// May it use the object `name` of an extension's `kind` for `privilege` (`READ` on a connection)? Whole: always.
    pub fn may_use(&self, kind: &str, name: &str, privilege: &str) -> bool { self.access.as_ref().is_none_or(|a| a.may_use(kind, name, privilege)) }

    /// The schemas a user granted CLONE may branch (ADR-058): None, no CLONE grant (and a whole scope, which needs none);
    /// Some(none), every schema (CLONE ON DATABASE); Some(these), CLONE ON SCHEMA.
    pub fn clones(&self) -> Option<Vec<String>> { self.access.as_ref().and_then(|a| a.clones()) }

    /// May it branch `db`, a database attached here, through its attachment (GRANT CLONE ON DATABASE db)?
    pub fn clones_of(&self, db: &str) -> bool { self.access.as_ref().is_none_or(|a| a.clones_of(db)) }

    /// Is it a user granted APPLY on this database (`apply::applier`)? A whole scope applies by its role instead: false.
    pub fn applies(&self) -> bool { self.access.as_ref().is_some_and(|a| a.applies()) }

    /// Is it shown routines' code, tasks' statements and views' queries (S27): a token, a superuser, an open node,
    /// and a user who could hold them anyway (APPLY deploys them, CLONE ON DATABASE copies them into a branch it
    /// owns)? A user granted some tables is shown their names and parameters only.
    pub fn shows_code(&self) -> bool { self.access.as_ref().is_none_or(|a| a.applies() || a.clones().is_some_and(|s| s.is_empty())) }

    /// The names whose grants it has: a user's own, its roles' and public's. None: whole.
    pub fn names(&self) -> Option<&std::collections::HashSet<String>> { self.access.as_ref().map(|a| &a.names) }
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
        // (UNDROP of a kind one of them drops: that DROP's right)
        Stmt::Ddl(ds) if ds.iter().any(|d| matches!(d, crate::ddl::Ddl::Entry { .. }) || crate::statements::undrops_entry(d)) => (String::new(), Action::of_role(crate::statements::role(ds)).into_iter().collect()),
        Stmt::Ddl(_) => (String::new(), vec![Action::Define]),
        Stmt::CopyTo(..) => (String::new(), vec![Action::Export]),
        // (as DELETE /files took it: a writer's, and a `.py`, code a node runs, an admin's)
        Stmt::UndropFile(p) => (String::new(), vec![if crate::auth::py_name(p) { Action::RunCode } else { Action::Write }]),
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
        let grants = grants.iter().map(|(p, on, cols)| Grant { privilege: p.to_string(), on: on.clone(), columns: cols.iter().map(|c| c.to_string()).collect(), rows: None }).collect();
        Principal { name: "ann".into(), scope: Scope::granted(Arc::new(Access::granted(grants))), ..Principal::of(Role::Read) }
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
        assert_eq!(said(may(&reader, Action::Insert, t)), "this token may not write: it needs a write token, or a user granted a write");
        assert_eq!(said(may(&reader, Action::Write, Object::Lake)), "this token may not write: it needs a write token, or a user granted a write");
        assert_eq!(said(may(&reader, Action::Lock, t)), "this token may not take rows FOR UPDATE, as an UPDATE does: it needs a write token, or a user granted a write");
        assert_eq!(said(may(&Principal::token(Role::None), Action::Select, t)), "this token may not read: it needs a token, or a user's sign-in");
        assert_eq!(said(may(&writer, Action::Delete, t)), "ok");
        assert_eq!(said(may(&writer, Action::Write, Object::Lake)), "ok");
        assert_eq!(said(may(&writer, Action::Define, Object::Lake)), "this token may not change the lake's tables, views, schemas, routines or users: it needs an admin token, or a superuser");
        assert_eq!(said(may(&writer, Action::Export, Object::Lake)), "this token may not write files outside the lake: it needs an admin token, or a superuser");
        assert_eq!(said(may(&writer, Action::RunCode, Object::Lake)), "this token may not run code on the node: it needs an admin token, or a superuser");
        assert_eq!(said(may(&admin, Action::Define, t)), "ok");
        assert_eq!(said(may(&admin, Action::Export, Object::Lake)), "ok");
        assert_eq!(said(may(&admin, Action::RunCode, Object::Lake)), "ok");

        let ann = user(&[("select", On::Table("t".into()), &["a"]), ("insert", On::Table("t".into()), &[]), ("select", On::Schema("s".into()), &[])]);
        assert_eq!(said(may(&ann, Action::Select, t)), "ok"); // (some columns: planning picks them)
        assert_eq!(said(may(&ann, Action::SelectAll, t)), "permission denied: SELECT on every column of t (GRANT SELECT ON t TO ann)");
        assert_eq!(said(may(&ann, Action::SelectAll, Object::Table("s.u"))), "ok");
        assert_eq!(said(may(&ann, Action::Insert, t)), "ok");
        assert_eq!(said(may(&ann, Action::Update, t)), "permission denied: UPDATE on t (GRANT UPDATE ON t TO ann)");
        assert_eq!(said(may(&ann, Action::Lock, t)), "permission denied: UPDATE on t (GRANT UPDATE ON t TO ann)");
        assert_eq!(said(may(&ann, Action::Delete, Object::Table("s.u"))), "permission denied: DELETE on s.u (GRANT DELETE ON s.u TO ann)"); // (it knows s.u: SELECT on s)
        assert_eq!(said(may(&ann, Action::Delete, Object::Table("u"))), "no table u"); // (no privilege on u: as if it weren't there)
        assert_eq!((may(&ann, Action::SelectAll, Object::Table("u")).unwrap_err().code, may(&ann, Action::Insert, Object::Table("u")).unwrap_err().code), ("42P01", "42P01"));
        assert!(said(may(&ann, Action::Define, t)).starts_with("this user may not change the lake's tables"));
        assert_eq!(said(may(&ann, Action::Export, Object::Lake)), "this user may not write files outside the lake: it needs an admin token, or a superuser");
        assert_eq!(said(may(&ann, Action::Write, Object::Lake)), "this user may not write: it needs a write token, or a user granted a write"); // (a reader's role)
        let bob = Principal { role: Role::Write, ..ann.clone() };
        assert_eq!(said(may(&bob, Action::Write, Object::Lake)), "ok"); // (a user granted a write: CHECKPOINT, the workspace's files)
        assert_eq!(said(may(&bob, Action::RunCode, Object::Lake)), "this user may not run code on the node: it needs an admin token, or a superuser");
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

    /// A column that isn't there is said without the columns that are, to a user with grants: they may be ones it may not
    /// read (`super::said`). A token and a superuser are told them, and the nearest.
    #[tokio::test]
    async fn a_missing_column_names_no_other_to_a_user_with_grants() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::{field_not_found, DFSchema};
        let schema = DFSchema::try_from_qualified_schema("t", &Schema::new(vec![Field::new("id", DataType::Int64, true), Field::new("card", DataType::Utf8, true)])).unwrap();
        let schema = &schema;
        let told = move |p: Principal| crate::auth::WHO.scope(p, async move { super::said(field_not_found(Some("t"), "car", schema)).to_string() });
        let ann = user(&[("select", On::Table("t".into()), &["id"])]);
        assert_eq!(told(ann).await, "Schema error: No field named t.car.", "the column as the statement named it, and no other");
        let admin = told(Principal::token(Role::Admin)).await;
        assert!(admin.contains("Did you mean 't.card'?") && admin.contains("Valid fields are t.id, t.card."), "{admin}");
        assert_eq!(crate::codes::of(&anyhow::Error::new(super::said(field_not_found(Some("t"), "car", schema)))), "42703"); // (no caller: whole)
    }

    /// Work with no caller is refused, not given an admin's rights: work a request spawns without `auth::carried` loses
    /// its caller. Only the node's own work (`auth::as_node`) is decided as an admin token's, and what it spawns with
    /// `carried` stays so.
    #[tokio::test]
    async fn work_with_no_caller_is_refused() {
        use crate::auth::{as_node, carried, WHO};
        let t = Object::Table("t");
        let told = |r: anyhow::Result<()>| r.err().map(|e| e.to_string()).unwrap_or_else(|| "ok".into());
        let nobody = "nobody asked for this, so it may not write: work a request starts runs as its caller (auth::carried)";
        assert_eq!(told(may_here(Action::Insert, t)), nobody);
        assert_eq!(told(may_as(Role::Admin, Action::Insert, t)), nobody);
        assert_eq!(told(may_run(Role::Admin, &Stmt::Delete("t".into(), None))), nobody);
        assert_eq!(crate::codes::of(&may_here(Action::Insert, t).unwrap_err()), "42501");
        let (lost, kept) = WHO
            .scope(Principal::token(Role::Admin), async {
                let lost = tokio::spawn(async move { told(may_here(Action::Insert, t)) }).await.unwrap();
                (lost, tokio::spawn(carried(async move { told(may_here(Action::Insert, t)) })).await.unwrap())
            })
            .await;
        assert_eq!((lost.as_str(), kept.as_str()), (nobody, "ok"), "spawned without its caller: refused, not an admin's");
        let node = as_node(async {
            let audit = told(may_here(Action::Insert, Object::Table("pondra$audit")));
            (told(may_here(Action::Insert, t)), audit, tokio::spawn(carried(async move { told(may_as(Role::Write, Action::Insert, t)) })).await.unwrap())
        })
        .await;
        assert_eq!(node, ("ok".to_string(), "pondra$audit is written by Pondra alone".to_string(), "ok".to_string()));
    }

    /// What work with no caller sees: nothing (11-08 audit M8), as it may do nothing. No table, column or name, no code,
    /// and no answer kept under a whole scope's key. The node's own work (`as_node`) sees everything, as
    /// `Principal::node`; so does a token, and whoever isn't signed in sees nothing.
    #[tokio::test]
    async fn work_with_no_caller_sees_nothing() {
        use crate::auth::{as_node, WHO};
        let sees = |s: &Scope| (s.whole(), s.sees(Object::Table("t")), s.columns("t").is_some(), s.reads("t", ["a"].into_iter()), s.shows_code(), s.key().is_empty());
        let nothing = (false, false, false, false, false, false);
        let everything = (true, true, true, true, true, true);
        assert_eq!(sees(&scope()), nothing, "no caller, not the node's own work");
        assert_eq!(sees(&tokio::spawn(async { scope() }).await.unwrap()), nothing);
        assert_eq!(sees(&as_node(async { scope() }).await), everything);
        assert_eq!(sees(&Principal::node().scope), everything);
        assert_eq!(sees(&WHO.scope(Principal::token(Role::Read), async { scope() }).await), everything);
        assert_eq!(sees(&Principal::of(Role::None).scope), nothing, "nobody signed in");
        assert!(Principal::of(Role::Read).scope.whole() && Principal::of(Role::Admin).scope.whole());
        let ann = Principal { name: "ann".into(), scope: Scope::granted(Arc::new(Access::granted(vec![Grant { privilege: "select".into(), on: On::Table("t".into()), columns: vec![], rows: None }]))), ..Principal::of(Role::Read) };
        let node = crate::node_state::NodeState::unhosted();
        let (here, spawned, carried) = WHO
            .scope(ann, async { (scope(), node.tasks.aside(crate::auth::Door, async { scope() }).await.unwrap(), node.tasks.aside(crate::auth::caller(), async { scope() }).await.unwrap()) })
            .await;
        assert_eq!((sees(&here).1, sees(&spawned).1), (true, false), "a task spawned without its caller sees nothing of its caller's");
        assert_eq!((sees(&carried).1, sees(&carried).0), (true, false), "a task spawned as its caller sees what its caller does, no more");
        let ours = node.tasks.spawn_node(crate::spawns::Vital::Own, async { scope() }).await.unwrap();
        assert_eq!(sees(&ours), everything, "the node's own task sees everything");
    }

    /// An answer is kept under what its scope holds (`Scope::key`): the same grants and names in any order are one key;
    /// another column, table, privilege or name is another, and so is a REVOKE; a whole scope's is "" and no granted one's
    /// is. A key lookup reads only what it may (`Scope::reads`).
    #[test]
    fn a_scope_is_kept_by_what_it_holds() {
        let scope = |grants: &[(&str, On, &[&str])], names: &[&str]| {
            let mut a = Access::granted(grants.iter().map(|(p, on, cols)| Grant { privilege: p.to_string(), on: on.clone(), columns: cols.iter().map(|c| c.to_string()).collect(), rows: None }).collect());
            a.names = names.iter().map(|n| n.to_string()).collect();
            Scope::granted(Arc::new(a))
        };
        let t = || On::Table("t".into());
        let ann = scope(&[("select", t(), &["a", "b"]), ("select", On::Table("u".into()), &[])], &["ann", "analyst", "public"]);
        let same = scope(&[("select", On::Table("u".into()), &[]), ("select", t(), &["a", "b"]), ("select", t(), &["a", "b"])], &["public", "analyst", "ann"]);
        assert_eq!(ann.key(), same.key(), "the same grants and names, in another order and one twice: one key");
        assert_eq!(ann.key().len(), 32);
        let others = [
            scope(&[("select", t(), &["a"]), ("select", On::Table("u".into()), &[])], &["ann", "analyst", "public"]), // (a column fewer)
            scope(&[("select", t(), &["a", "b"])], &["ann", "analyst", "public"]),                                      // (a REVOKE of u)
            scope(&[("select", t(), &["a", "b"]), ("insert", On::Table("u".into()), &[])], &["ann", "analyst", "public"]), // (another privilege)
            scope(&[("select", t(), &["a", "b"]), ("select", On::Table("u".into()), &[])], &["bob", "analyst", "public"]), // (another name)
            scope(&[], &[]),
        ];
        for (i, o) in others.iter().enumerate() {
            assert_ne!(o.key(), ann.key(), "{i}");
            assert!(others[..i].iter().all(|p| p.key() != o.key()), "{i}");
        }
        assert_eq!(Scope::everything().key(), "");
        assert!(others.iter().chain([&ann]).all(|s| !s.key().is_empty()), "a granted scope never shares a whole one's answers");
        let read = |s: &Scope, table: &str, cols: &[&str]| s.reads(table, cols.iter().copied());
        assert!(read(&ann, "t", &["a", "b"]) && read(&ann, "u", &["x", "y"]) && read(&Scope::everything(), "v", &["z"]));
        assert!(!read(&ann, "t", &["a", "c"]) && !read(&ann, "v", &[]), "a column not granted, or a table: SQL answers it, refusing");
    }

    /// A grant's filter on rows (step 6 piece 5): its grants' filters join with OR, one with none shows every row, and each
    /// is filled in for its reader. A table some of whose rows are hidden is never read whole (`SelectAll`) nor by its key
    /// (`reads`), and two readers of one filter on `current_user` keep their answers apart, with the same names and grants.
    #[test]
    fn a_filter_on_rows_is_its_readers() {
        let grant = |p: &str, on: On, rows: Option<&str>| Grant { privilege: p.into(), on, columns: vec![], rows: rows.map(String::from) };
        let t = || On::Table("t".into());
        let access = |grants: Vec<Grant>| {
            let mut a = Access::granted(grants);
            a.names = ["ann", "public"].iter().map(|n| n.to_string()).collect();
            Arc::new(a)
        };
        let as_ = |name: &str, a: &Arc<Access>| Principal { name: name.into(), scope: Scope::granted(a.clone()), ..Principal::of(Role::Read) }.asking();
        let mine = access(vec![grant("select", t(), Some("owner = current_user")), grant("select", On::Schema("public".into()), Some("dept = 'x'")), grant("insert", t(), None)]);
        let (ann, bob) = (as_("ann", &mine), as_("bob", &mine));
        assert_eq!(ann.scope.rows("select", "t").as_deref(), Some("(dept = 'x') OR (owner = 'ann')"));
        assert_eq!(bob.scope.rows("select", "t").as_deref(), Some("(dept = 'x') OR (owner = 'bob')"));
        assert_eq!(ann.scope.rows("select", "u").as_deref(), Some("dept = 'x'"), "the schema's grant, for every table of it");
        assert_eq!(ann.scope.rows("insert", "t"), None, "another privilege's grant, with no filter: every row");
        assert_eq!(Scope::granted(mine.clone()).rows("select", "t").as_deref(), Some("(dept = 'x') OR (owner = NULL)"), "no reader: no one's rows");
        assert_eq!(Principal::node().scope.rows("select", "t"), None);
        let every = access(vec![grant("select", t(), Some("owner = current_user")), grant("select", On::Lake, None)]);
        assert_eq!(as_("ann", &every).scope.rows("select", "t"), None, "one grant with no filter: every row");
        let named = access(vec![grant("select", On::Table("l.t".into()), Some("n > 1")), grant("select", On::Lake, Some("n > 2"))]);
        assert_eq!(as_("ann", &named).scope.rows_named("select", "l.t").as_deref(), Some("n > 1"), "another lake's table: only grants naming it");

        assert_ne!(ann.scope.key(), bob.scope.key(), "one filter on current_user, the same grants and names: two keys");
        assert_eq!(ann.scope.key(), as_("ann", &mine).scope.key());
        let plain = access(vec![grant("select", t(), None)]);
        assert_eq!(as_("ann", &plain).scope.key(), as_("bob", &plain).scope.key(), "no filter: the key is what the scope holds, as before");
        assert_eq!(as_("ann", &plain).scope.key(), plain.key());

        assert_eq!(said(may(&ann, Action::Select, Object::Table("t"))), "ok");
        assert_eq!(said(may(&ann, Action::SelectAll, Object::Table("t"))), "permission denied: t shows ann some of its rows only (a filter on its rows in a grant): read it with SELECT");
        assert_eq!(said(may(&as_("ann", &every), Action::SelectAll, Object::Table("t"))), "ok");
        let writer = as_("ann", &access(vec![grant("select", t(), Some("owner = current_user")), grant("insert", t(), None), grant("update", t(), None), grant("delete", t(), None)]));
        for action in [Action::Update, Action::Delete] {
            assert_eq!(said(may(&writer, action, Object::Table("t"))), "permission denied: t shows ann some of its rows only (a filter on its rows in a grant): change them with SQL's UPDATE, DELETE, MERGE or INSERT … ON CONFLICT", "a load's rows replace or delete rows by their key, unread");
            assert_eq!(said(may_in(&writer, action, Object::Table("t"), true)), "ok", "SQL's change reads them through the filter");
        }
        assert_eq!(said(may(&writer, Action::Insert, Object::Table("t"))), "ok", "a load's new rows change no row");

        // (UPDATE's and DELETE's filters, piece 5c-1: a write reads its rows with SELECT's and its own together)
        let editor = as_("ann", &access(vec![grant("select", t(), Some("dept = 'x'")), grant("update", t(), Some("owner = current_user")), grant("delete", t(), None)]));
        let writing = editor.scope.writing("update", "t");
        assert_eq!(writing.rows("select", "t").as_deref(), Some("(dept = 'x') AND (owner = 'ann')"));
        assert_eq!(writing.rows("select", "u"), None, "another table's reads are as before");
        assert_eq!(writing.rows("update", "t").as_deref(), Some("owner = 'ann'"));
        assert_ne!(writing.key(), editor.scope.key(), "no answer is shared with a read's");
        assert_eq!(editor.scope.writing("delete", "t").key(), editor.scope.key(), "no filter on DELETE's rows: the scope itself");
        let updater = as_("ann", &access(vec![grant("select", t(), None), grant("update", t(), Some("owner = current_user"))]));
        assert_eq!(updater.scope.writing("update", "t").rows("select", "t").as_deref(), Some("owner = 'ann'"));
        assert!(editor.scope.filtered("t") && updater.scope.filtered("t") && !updater.scope.filtered("u") && !Principal::node().scope.filtered("t"));
        assert_eq!(said(may(&updater, Action::Update, Object::Table("t"))), "permission denied: t lets ann update some of its rows only (a filter on its rows in a grant): change them with SQL's UPDATE, DELETE, MERGE or INSERT … ON CONFLICT");
        assert_eq!(said(may_in(&updater, Action::Update, Object::Table("t"), true)), "ok");
        assert!(!ann.scope.reads("t", ["owner"].into_iter()), "a key lookup: SQL answers it, through its filter");
        assert!(as_("ann", &plain).scope.reads("t", ["owner"].into_iter()));
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
