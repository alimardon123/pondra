//! Who is asking, and may they. The three tokens (`--read-token`, `--write-token`,
//! `--admin-token`; give every node the same): reading needs any of them; writing rows a write or
//! admin token; changing tables, views and tasks, and the nodes' own calls to each other, the admin
//! token. Over HTTP a token goes in `Authorization: Bearer …`; over Postgres the user name picks the
//! role (`reader`, `writer`, `admin`) and the password is its token. Users and roles
//! (`users.rs`) sign in with a password, a token or a session, and may do what they are granted.
//! With no tokens set and no user who signs in, everyone may do everything (a lake behind your own
//! network, as before).
//!
//! Every door (HTTP, Postgres, Kafka, Flight) works out a `Principal` and runs the request inside
//! `WHO.scope(…)`, so one check (`check`, `allows`, the query's tables in `query.rs`) sees it.
use crate::write::Stmt;
use anyhow::{bail, Result};
use std::sync::Arc;

/// Who a request is: a name, the most it may do (`role`), and, for a user who isn't a superuser,
/// what it is granted (`access`; None: everything its role allows).
#[derive(Clone)]
pub struct Principal {
    pub name: String,
    pub role: Role,
    pub access: Option<Arc<crate::users::Access>>,
    pub door: &'static str,                  // (which door it came in by, and from where: the audit log's)
    pub from: Option<std::net::SocketAddr>,
}

impl Principal {
    /// A token's (or nobody's): its role over everything.
    pub fn of(role: Role) -> Principal {
        let name = match role {
            Role::Admin => "admin",
            Role::Write => "writer",
            Role::Read => "reader",
            Role::None => "",
        };
        Principal { name: name.into(), role, access: None, door: "node", from: None }
    }

    /// The same, as come in by `door` from `from`.
    pub fn at(mut self, door: &'static str, from: Option<std::net::SocketAddr>) -> Principal {
        (self.door, self.from) = (door, from);
        self
    }

    /// May it use `privilege` on `table` (as SQL names it)? (Its role decides what kind of thing.)
    pub fn may(&self, privilege: &str, table: &str) -> bool { self.access.as_ref().is_none_or(|a| a.may(privilege, &catalog_name(table))) }

    /// May it read every column of `table`? (What a stream of its rows needs: Kafka, Flight, lookups.)
    pub fn may_read_all(&self, table: &str) -> bool { self.access.as_ref().is_none_or(|a| a.columns("select", &catalog_name(table)) == Some(None)) }
}

tokio::task_local! {
    /// The principal of the request being served.
    pub static WHO: Principal;
}

/// The principal of the request being served (None: no door set one: this process's own work).
pub fn current() -> Option<Principal> { WHO.try_with(|p| p.clone()).ok() }

/// May the request being served read every column of `table`? Refused with why, if not.
pub fn check_all(table: &str) -> Result<()> {
    match current() {
        Some(p) if !p.may_read_all(table) => bail!("permission denied: SELECT on every column of {table}, which a stream of its rows needs (GRANT SELECT ON {table} TO {})", p.name),
        _ => Ok(()),
    }
}

/// A user's grants, if the request being served is a user's who isn't a superuser.
pub fn limited() -> Option<Arc<crate::users::Access>> { current().and_then(|p| p.access) }

/// May the request being served use `privilege` on `table` (as the catalog names it)?
pub fn check(privilege: &str, table: &str) -> Result<()> {
    match limited() {
        Some(a) if !a.may(privilege, table) => bail!("permission denied: {} on {table} (GRANT {} ON {table} TO {})", privilege.to_uppercase(), privilege.to_uppercase(), current().map(|p| p.name).unwrap_or_default()),
        _ => Ok(()),
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Debug)]
pub enum Role {
    None,
    Read,
    Write,
    Admin,
}

#[derive(Clone, Default)]
pub struct Auth {
    read: Option<String>,
    write: Option<String>,
    admin: Option<String>,
}

impl Auth {
    pub fn new(read: Option<String>, write: Option<String>, admin: Option<String>) -> Auth { Auth { read, write, admin } }

    pub fn on(&self) -> bool { self.read.is_some() || self.write.is_some() || self.admin.is_some() }

    /// The role a bearer token grants.
    pub fn role(&self, token: Option<&str>) -> Role {
        if let Some((role, _)) = lent(token) {
            return role; // (a Python procedure's calls back: its caller's)
        }
        match self.on() {
            false => Role::Admin,
            true => token.map_or(Role::None, |t| self.token_role(t)),
        }
    }

    /// The role one of the three tokens grants (compared in constant time), or None.
    pub fn token_role(&self, token: &str) -> Role {
        let is = |t: &Option<String>| t.as_ref().is_some_and(|t| aws_lc_rs::constant_time::verify_slices_are_equal(t.as_bytes(), token.as_bytes()).is_ok());
        match () {
            _ if is(&self.admin) => Role::Admin,
            _ if is(&self.write) => Role::Write,
            _ if is(&self.read) => Role::Read,
            _ => Role::None,
        }
    }

    /// A lent token's principal: its procedure's caller's.
    pub fn lent_principal(&self, token: &str) -> Option<Principal> { LENT.lock().unwrap().get(token).map(|l| l.who.clone()) }

    /// The token a Postgres user name stands for (the password it must send).
    pub fn token_for(&self, user: &str) -> Option<String> {
        match user {
            "admin" => self.admin.clone(),
            "writer" => self.write.clone(),
            _ => self.read.clone(),
        }
    }

    /// The role of a Postgres user who got past the password check.
    pub fn role_of_user(&self, user: &str) -> Role { self.role(self.token_for(user).as_deref()) }

    /// What an HTTP route needs (writes in `POST /sql` are checked by `allows`).
    pub fn needed(path: &str, method: &str) -> Role {
        let first = path.trim_start_matches('/').split('/').next().unwrap_or_default();
        match first {
            "files" if method == "GET" => Role::Read, // (objects next to the tables: files.rs)
            "files" => Role::Write,
            "" | "console" => Role::None, // (the console's page and files: they hold no data, and ask for a token)
            "stats" | "healthz" | "ready" => Role::None, // (health checks: load balancers and the tests poll them)
            "login" | "whoami" => Role::None, // (signing in; and who one is)
            "secrets" => Role::None, // (a procedure's lent token only: `server::secret`)
            "v1" if method == "POST" || method == "DELETE" => Role::Write, // (another engine's append: ADR-028; its tables made, dropped and renamed: ADR-029, as the SQL's rights say)
            "sql" | "lookup" | "watch" | "live" | "sessions" | "mcp" | "v1" | "metrics" | "routines" | "objects" | "kinds" | "plan" | "deploy" | "test" | "export" => Role::Read, // (MCP writes are checked by `allows`; v1: the Iceberg REST catalog; a deploy, not its plan, needs an admin: `deploy::ask`)
            "append" | "insert" => Role::Write,
            "cluster" if path.starts_with("/cluster/files") || path.starts_with("/cluster/commit") => Role::Write, // (writers on other machines)
            "cluster" if path.starts_with("/cluster/leader") => Role::None,
            _ => Role::Admin,
        }
    }

    /// May this role run this write? (And, for a user's request, is it granted: `check`.)
    pub fn allows(&self, role: Role, stmt: &Stmt) -> Result<()> {
        if crate::temp::own(stmt) {
            return Ok(()); // (the session's own tables and views: any role may keep them)
        }
        let need = if matches!(stmt, Stmt::Create(_) | Stmt::Define(..) | Stmt::AddColumn(..) | Stmt::SetOptions(..) | Stmt::Ddl(_) | Stmt::CopyTo(..)) { Role::Admin } else { Role::Write };
        if role < need && !(need == Role::Write && limited().is_some()) { // (a user's writes: its grants, below)
            bail!("this {} may not {}", if limited().is_some() { "user" } else { "token" }, match stmt {
                Stmt::CopyTo(..) => "write files outside the lake",
                _ if need == Role::Admin => "change the lake's tables, views, schemas, routines or users (an admin token, or a superuser, does)",
                _ => "write",
            });
        }
        match stmt {
            Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) => check("insert", &catalog_name(t)),
            Stmt::Update(t, ..) => check("update", &catalog_name(t)),
            Stmt::Delete(t, _) => check("delete", &catalog_name(t)),
            Stmt::Merge(m) => {
                let t = catalog_name(&m.target);
                for p in m.privileges() {
                    check(p, &t)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// A token lent to a Python procedure for its calls back to the node (`routines::python`): the
/// rights of whoever called it (and whether it may read files on this machine), until it ends;
/// and the secrets it has read (`pondra.secret`), kept out of what it says.
pub struct Lease(pub String);

struct Lent {
    role: Role,
    who: Principal, // (its caller: whose rights it has)
    files: bool,
    secrets: Vec<String>,
    session: Option<String>, // (its caller's temporary tables are its own too: `temp.rs`)
    vars: crate::vars::Lent, // (its run's variables and given values: `db.vars` is `$name`)
}

/// A table as the catalog names it (`t` for `public.t`, `s.t`; another lake's `l.s.t` stays).
pub fn catalog_name(t: &str) -> String {
    match t.split('.').collect::<Vec<_>>()[..] {
        [s, t] if s == crate::ddl::PUBLIC => t.to_string(),
        _ => t.to_string(),
    }
}

static LENT: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Lent>>> = std::sync::LazyLock::new(Default::default);

pub fn lend(role: Role, files: bool) -> Lease {
    let token = format!("lease-{}", uuid::Uuid::new_v4().simple());
    let who = current().filter(|p| p.role >= role).unwrap_or_else(|| Principal::of(role));
    LENT.lock().unwrap().insert(token.clone(), Lent { role, who, files, secrets: vec![], session: crate::temp::current(), vars: crate::vars::lend() });
    Lease(token)
}

/// What a lent token allows, while its procedure runs.
pub fn lent(token: Option<&str>) -> Option<(Role, bool)> { LENT.lock().unwrap().get(token?).map(|l| (l.role, l.files)) }

/// The session of the caller a lent token's procedure runs for.
pub fn lent_session(token: Option<&str>) -> Option<String> { LENT.lock().unwrap().get(token?).and_then(|l| l.session.clone()) }

/// The variables of the run a lent token's code runs in (`vars::within`).
pub fn lent_vars(token: Option<&str>) -> Option<crate::vars::Lent> { LENT.lock().unwrap().get(token?).map(|l| l.vars.clone()) }

/// A secret's values, read by a lent token's procedure: kept, to be blanked out of its notices,
/// its error and the run log. False: not a lent token (only a procedure's code reads a secret).
pub fn revealed(token: Option<&str>, values: impl IntoIterator<Item = String>) -> bool {
    let mut all = LENT.lock().unwrap();
    let Some(l) = token.and_then(|t| all.get_mut(t)) else { return false };
    l.secrets.extend(values.into_iter().filter(|v| v.len() >= 4)); // (a port number or `true` isn't worth hiding, and would blank out too much)
    true
}

impl Lease {
    /// `text` with every secret this procedure read replaced by `***`.
    pub fn redact(&self, text: &str) -> String {
        let all = LENT.lock().unwrap();
        let Some(l) = all.get(&self.0) else { return text.to_string() };
        let mut out = text.replace(&self.0, "***"); // (its token too)
        for s in &l.secrets {
            out = out.replace(s.as_str(), "***");
        }
        out
    }
}

impl Drop for Lease {
    fn drop(&mut self) { LENT.lock().unwrap().remove(&self.0); }
}
