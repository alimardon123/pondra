//! Access tokens (`--read-token`, `--write-token`, `--admin-token`; give every node the same).
//! Reading needs any of them; writing rows a write or admin token; changing tables, views and
//! tasks, and the nodes' own calls to each other, the admin token. Over HTTP a token goes in
//! `Authorization: Bearer …`; over Postgres the user name picks the role (`reader`, `writer`,
//! `admin`) and the password is its token. With no tokens set, everyone may do everything (a lake
//! behind your own network, as before).
use crate::write::Stmt;
use anyhow::{bail, Result};

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
        let is = |t: &Option<String>| t.is_some() && t.as_deref() == token;
        match () {
            _ if !self.on() || is(&self.admin) => Role::Admin,
            _ if is(&self.write) => Role::Write,
            _ if is(&self.read) => Role::Read,
            _ => Role::None,
        }
    }

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
            "" => Role::None,      // (the console's page: it holds no data, and asks for a token)
            "stats" => Role::None, // (a health check: load balancers and the tests poll it)
            "secrets" => Role::None, // (a procedure's lent token only: `server::secret`)
            "v1" if method == "POST" => Role::Write, // (another engine's append: ADR-028)
            "sql" | "lookup" | "watch" | "live" | "sessions" | "mcp" | "v1" | "metrics" | "routines" | "objects" => Role::Read, // (MCP writes are checked by `allows`; v1: the Iceberg REST catalog)
            "append" | "insert" => Role::Write,
            "cluster" if path.starts_with("/cluster/files") || path.starts_with("/cluster/commit") => Role::Write, // (writers on other machines)
            "cluster" if path.starts_with("/cluster/leader") => Role::None,
            _ => Role::Admin,
        }
    }

    /// May this role run this write?
    pub fn allows(&self, role: Role, stmt: &Stmt) -> Result<()> {
        if crate::temp::own(stmt) {
            return Ok(()); // (the session's own tables and views: any role may keep them)
        }
        let need = if matches!(stmt, Stmt::Create(_) | Stmt::Define(..) | Stmt::AddColumn(..) | Stmt::SetOptions(..) | Stmt::Ddl(_) | Stmt::CopyTo(..)) { Role::Admin } else { Role::Write };
        if role < need {
            bail!("this token may not {}", match stmt {
                Stmt::CopyTo(..) => "write files outside the lake",
                _ if need == Role::Admin => "change the lake's tables, views, schemas or routines (an admin token does)",
                _ => "write",
            });
        }
        Ok(())
    }
}

/// A token lent to a Python procedure for its calls back to the node (`routines::python`): the
/// rights of whoever called it (and whether it may read files on this machine), until it ends;
/// and the secrets it has read (`pondra.secret`), kept out of what it says.
pub struct Lease(pub String);

struct Lent {
    role: Role,
    files: bool,
    secrets: Vec<String>,
    session: Option<String>, // (its caller's temporary tables are its own too: `temp.rs`)
}

static LENT: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Lent>>> = std::sync::LazyLock::new(Default::default);

pub fn lend(role: Role, files: bool) -> Lease {
    let token = format!("lease-{}", uuid::Uuid::new_v4().simple());
    LENT.lock().unwrap().insert(token.clone(), Lent { role, files, secrets: vec![], session: crate::temp::current() });
    Lease(token)
}

/// What a lent token allows, while its procedure runs.
pub fn lent(token: Option<&str>) -> Option<(Role, bool)> { LENT.lock().unwrap().get(token?).map(|l| (l.role, l.files)) }

/// The session of the caller a lent token's procedure runs for.
pub fn lent_session(token: Option<&str>) -> Option<String> { LENT.lock().unwrap().get(token?).and_then(|l| l.session.clone()) }

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
