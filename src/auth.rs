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
    pub fn needed(path: &str) -> Role {
        let first = path.trim_start_matches('/').split('/').next().unwrap_or_default();
        match first {
            "stats" => Role::None, // (a health check: load balancers and the tests poll it)
            "sql" | "lookup" | "watch" | "mcp" => Role::Read, // (MCP writes are checked by `allows`)
            "append" | "insert" => Role::Write,
            "cluster" if path.starts_with("/cluster/files") || path.starts_with("/cluster/commit") => Role::Write, // (writers on other machines)
            "cluster" if path.starts_with("/cluster/leader") => Role::None,
            _ => Role::Admin,
        }
    }

    /// May this role run this write?
    pub fn allows(&self, role: Role, stmt: &Stmt) -> Result<()> {
        let need = if matches!(stmt, Stmt::Create(_)) { Role::Admin } else { Role::Write };
        if role < need {
            bail!("this token may not {}", if need == Role::Admin { "create tables" } else { "write" });
        }
        Ok(())
    }
}
