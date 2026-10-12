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
use anyhow::{bail, ensure, Result};
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
    pub operator: bool, // (whoever runs the nodes: one of their tokens, their key, the program that started this one; `users::across`)
}

impl Principal {
    /// Its role over everything, as anyone on a node nothing locks (or nobody).
    pub fn of(role: Role) -> Principal {
        let name = match role {
            Role::Admin => "admin",
            Role::Write => "writer",
            Role::Read => "reader",
            Role::None => "",
        };
        Principal { name: name.into(), role, access: None, door: "node", from: None, operator: false }
    }

    /// A token's (one of the nodes', or their key), or the program's that started the node: its
    /// role over everything, this lake's and the lakes attached to it.
    pub fn token(role: Role) -> Principal { Principal { operator: true, ..Principal::of(role) } }

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

/// Who the request being served is, as one string: an answer kept for it is read back only by the
/// same (`pages.rs`). No principal is this process's own work, which keeps and reads its own.
pub fn holder() -> String { current().map_or_else(|| "node".to_string(), |p| format!("{}|{:?}|{}", p.name, p.role, p.operator)) }

/// `f` as the request that makes it, for work it starts and doesn't wait for (`pondra.start`): the
/// same user (its grants) and session. A spawned task keeps no task-local, and with no principal a
/// started procedure ran with its role's every right, past the user's grants.
pub fn carried<F: std::future::Future>(f: F) -> impl std::future::Future<Output = F::Output> {
    let (who, session) = (current(), crate::temp::current());
    async move {
        let f = crate::temp::SESSION.scope(session, f);
        match who {
            Some(p) => WHO.scope(p, f).await,
            None => f.await,
        }
    }
}

/// May the request being served read every column of `table`? Refused with why, if not.
pub fn check_all(table: &str) -> Result<()> {
    match current() {
        Some(p) if !p.may_read_all(table) => bail!("permission denied: SELECT on every column of {table}, which a stream of its rows needs (GRANT SELECT ON {table} TO {})", p.name),
        _ => Ok(()),
    }
}

/// A user's grants, if the request being served is a user's who isn't a superuser.
pub fn limited() -> Option<Arc<crate::users::Access>> { current().and_then(|p| p.access) }

/// Is the request being served from whoever runs the nodes (`Principal::token`)? (None: work no
/// door started, a task's included, is not: whoever made the task may be anyone.)
pub fn operator() -> bool { current().is_some_and(|p| p.operator) }

/// Does a node in this process take tokens? (Then nothing it serves is open, attached lakes included.)
pub fn tokens_on() -> bool { TOKENS.load(std::sync::atomic::Ordering::Relaxed) }

static TOKENS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The header that carries, with a change a node sends its leader, the user it is for: the leader
/// runs it as that user, so its subqueries read with the user's grants, as where it came in. Only the
/// cluster's own calls may say it (`Principal::operator`), and it only narrows what they may do.
pub const AS_USER: &str = "x-pondra-as";

/// The user the request being served is for, when its work goes on elsewhere (None: not a user's).
pub fn as_user() -> Option<String> { current().filter(|p| p.access.is_some()).map(|p| p.name) }

/// Whether a request a node with no sign-in would answer as anyone may be answered. Programs send no
/// `Origin` or `Sec-Fetch-Site`, and the node's own console sends its own origin; a browser sends
/// what the page it runs is. So a page from anywhere else can't use a node on the user's machine
/// through the user's browser (CSRF), nor can a page that pointed its own name at this machine (DNS
/// rebinding: asked from this machine by a name that isn't this machine's). A link followed from
/// elsewhere still opens the console: a page is shown in the user's tab, never to the page that
/// linked it, and no GET changes anything. `own` is the node's address (`--advertise`). Some(why):
/// refuse.
pub fn cross_site(h: &axum::http::HeaderMap, method: &axum::http::Method, from: Option<std::net::SocketAddr>, own: &str) -> Option<String> {
    let header = |k: &str| h.get(k).and_then(|v| v.to_str().ok()).map(str::to_ascii_lowercase);
    let host = header("host").unwrap_or_default();
    let away = "a page from another site may not use this node through your browser: open the node's own console, or give the node a token (--admin-token)";
    let navigated = (method == axum::http::Method::GET || method == axum::http::Method::HEAD) && header("sec-fetch-mode").is_some_and(|m| m == "navigate");
    if !navigated && header("sec-fetch-site").is_some_and(|s| s != "same-origin" && s != "none") {
        return Some(away.into());
    }
    // (an origin's default port is left out, a Host's may not be)
    let bare = |a: &str| a.trim_end_matches(":80").trim_end_matches(":443").to_string();
    if let Some(origin) = header("origin").filter(|_| !navigated) {
        if origin.split_once("://").is_none_or(|(_, o)| bare(o) != bare(&host)) {
            return Some(away.into());
        }
    }
    let name = host.rsplit_once(':').filter(|(_, p)| p.parse::<u16>().is_ok()).map_or(host.as_str(), |(n, _)| n);
    let name = name.trim_start_matches('[').trim_end_matches(']');
    let ours = name.is_empty()
        || name.parse::<std::net::IpAddr>().is_ok()
        || name == "localhost"
        || name.ends_with(".localhost")
        || own.rsplit_once(':').is_some_and(|(o, _)| o.eq_ignore_ascii_case(name))
        || MACHINE.iter().any(|m| m.eq_ignore_ascii_case(name) || m.split('.').next().is_some_and(|short| short.eq_ignore_ascii_case(name)));
    if from.is_some_and(|f| f.ip().is_loopback()) && !ours {
        return Some(format!("asked from this machine as {name}, which isn't this machine's name: a page may have pointed its name here (DNS rebinding). Use http://127.0.0.1 or http://localhost, or give the node a token (--admin-token)"));
    }
    None
}

/// This machine's name, as the OS says it.
static MACHINE: std::sync::LazyLock<Option<String>> = std::sync::LazyLock::new(sysinfo::System::host_name);

/// May the request being served add rows to `table` (as the catalog names it) through a door that
/// takes rows, not SQL (`/append`, `/insert`)? Its INSERT grant, as SQL's `INSERT` asks; and never a
/// system table (`$` in its name: `pondra$audit`, `t$deleted`), which only Pondra writes.
pub fn check_rows(table: &str) -> Result<()> {
    ensure!(!table.contains('$'), "{table} is written by Pondra alone");
    check("insert", table)
}

/// Is the request being served at least `role`'s? (Making a table through `/insert`: an admin's, as
/// `CREATE TABLE` is.)
pub fn at_least(role: Role, what: &str) -> Result<()> {
    ensure!(current().is_none_or(|p| p.role >= role), "{what} needs an admin");
    Ok(())
}

/// The lake's own records: what was done and by whom, which no writer but the nodes may add to.
const RECORDS: [&str; 3] = ["pondra$audit", "pondra$history", "pondra$runs"];

/// What the nodes' own write routes (`/cluster/commit`, `/cluster/files`) take from a caller that
/// isn't an admin (a `pondra sql` writer on another machine, with a write token): rows into tables
/// that are there, and no more. A user writes with SQL, where its grants are checked. A write token
/// never moves a sequence or a branch's row ids, and never names an object the lake would one day
/// delete as its own: a segment outside `log/`, a file outside its table's folder; nor adds to the
/// lake's own records.
pub fn flush_from_writer(f: &crate::log::Flush) -> Result<()> {
    if !writer()? {
        return Ok(());
    }
    ensure!(f.sequence.is_none() && f.blocks == 0, "moving a sequence or a branch's row ids needs an admin");
    if let Some(p) = f.parts.iter().find(|p| RECORDS.contains(&p.table.as_str())) {
        bail!("{} is written by Pondra alone", p.table);
    }
    let seg = f.path.strip_prefix("log/").and_then(|n| n.strip_suffix(".seg"));
    ensure!(f.path.is_empty() || seg.is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')), "a flush's rows are in a segment of the log (log/<id>.seg), not {}", f.path);
    Ok(())
}

/// The same for a bulk INSERT's files: into a table that is there (`folder`: its folder under
/// `data/`), each in that folder.
pub fn files_from_writer(folder: Option<&str>, files: &[String]) -> Result<()> {
    if !writer()? {
        return Ok(());
    }
    let Some(folder) = folder else { bail!("making a table needs an admin") };
    let home = format!("data/{folder}/");
    ensure!(files.iter().all(|f| f.starts_with(&home) && !f.split('/').any(|part| part == ".." || part == ".")), "a table's files are in its folder ({home})");
    Ok(())
}

/// Is the request being served a writer's that isn't an admin (refused if it is a user's)?
fn writer() -> Result<bool> {
    let Some(p) = current().filter(|p| p.role < Role::Admin) else { return Ok(false) };
    ensure!(p.access.is_none(), "a user's rows go in through SQL (POST /sql, the Postgres port), where its grants are checked: this route is the nodes' own");
    Ok(true)
}

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
    /// Does the node listen beyond this machine (`--addr`, `--pg`, `--kafka` or `--flight`)? Then it is
    /// never open, tokens or not (`App::open`).
    network: bool,
    /// The key of the program that started the node for itself (the shell, `local()`, the project commands:
    /// `PONDRA_OWNER_KEY`), if one did. Then it is never open either: only that program (its key), a token
    /// or a user signs in, and the key is the admin's password at the doors that take one (Postgres's
    /// `admin`, while no admin token is set).
    owner: Option<String>,
}

impl Auth {
    pub fn new(read: Option<String>, write: Option<String>, admin: Option<String>) -> Auth {
        let auth = Auth { read, write, admin, network: false, owner: None };
        if auth.on() {
            TOKENS.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        auth
    }

    pub fn on(&self) -> bool { self.read.is_some() || self.write.is_some() || self.admin.is_some() }

    /// This node listens beyond this machine (`network`).
    pub fn on_network(self, network: bool) -> Auth { Auth { network, ..self } }

    pub fn network(&self) -> bool { self.network }

    /// Is an admin token set? Then the nodes call each other with it, and the nodes' key counts for nothing.
    pub fn admin_set(&self) -> bool { self.admin.is_some() }

    /// This node was started by a program for itself, which signs its requests with this key (`owner`).
    pub fn owned_by(self, owner: Option<String>) -> Auth { Auth { owner, ..self } }

    pub fn owned(&self) -> bool { self.owner.is_some() }

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

    /// The role one of the three tokens (or the program's key: an admin's) grants, compared in constant time, or None.
    pub fn token_role(&self, token: &str) -> Role {
        let is = |t: &Option<String>| t.as_ref().is_some_and(|t| aws_lc_rs::constant_time::verify_slices_are_equal(t.as_bytes(), token.as_bytes()).is_ok());
        match () {
            _ if is(&self.admin) || is(&self.owner) => Role::Admin,
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
            "admin" => self.admin.clone().or_else(|| self.owner.clone()),
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
            "files" if path.to_ascii_lowercase().ends_with(".py") => Role::Admin, // (code a node runs: an admin stores it, invariant 85)
            "files" => Role::Write,
            "" | "console" => Role::None, // (the console's page and files: they hold no data, and ask for a token)
            "stats" | "healthz" | "ready" => Role::None, // (health checks: load balancers and the tests poll them)
            "login" | "whoami" => Role::None, // (signing in; and who one is)
            "secrets" => Role::None, // (a procedure's lent token only: `server::secret`)
            "v1" if method == "POST" || method == "DELETE" => Role::Write, // (another engine's append: ADR-028; its tables made, dropped and renamed: ADR-029, as the SQL's rights say)
            "sql" | "lookup" | "watch" | "live" | "sessions" | "mcp" | "v1" | "metrics" | "routines" | "objects" | "kinds" | "plan" | "apply" | "test" | "status" | "capture" | "export" => Role::Read, // (MCP writes are checked by `allows`; v1: the Iceberg REST catalog; an apply, not its plan, needs an admin: `apply::ask`)
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
        // (CLONE is enough to make a branch, and its owner drops it: `branch::door` checks)
        if limited().is_some() && matches!(stmt, Stmt::Ddl(ds) if ds.len() == 1 && matches!(ds[0], crate::ddl::Ddl::CreateDatabase { clone: Some(_), .. } | crate::ddl::Ddl::DropDatabase { .. })) {
            return Ok(());
        }
        let need = if matches!(stmt, Stmt::Create(_) | Stmt::Define(..) | Stmt::AddColumn(..) | Stmt::SetOptions(..) | Stmt::Ddl(_) | Stmt::CopyTo(..)) { Role::Admin } else { Role::Write };
        if role < need && !(need == Role::Write && limited().is_some()) { // (a user's writes: its grants, below)
            bail!("this {} may not {}", if limited().is_some() { "user" } else { "token" }, match stmt {
                Stmt::CopyTo(..) => "write files outside the lake",
                _ if need == Role::Admin => "change the lake's tables, views, schemas, routines or users (an admin token, or a superuser, does)",
                _ => "write",
            });
        }
        let (t, privileges) = needs(stmt);
        privileges.iter().try_for_each(|p| check(p, &t))
    }
}

/// The table a write changes (as the catalog names it) and the privileges it needs on it: none for
/// anything else.
pub fn needs(stmt: &Stmt) -> (String, Vec<&'static str>) {
    match stmt {
        Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) | Stmt::CopyInto(t, ..) => (catalog_name(t), vec!["insert"]), // (COPY INTO's rows are an INSERT's)
        Stmt::Update(t, ..) => (catalog_name(t), vec!["update"]),
        Stmt::Delete(t, _) => (catalog_name(t), vec!["delete"]),
        Stmt::Merge(m) => (catalog_name(&m.target), m.privileges()),
        _ => (String::new(), vec![]),
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
    applying: bool,          // (called by an apply's statement: `protect::applying`)
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
    let applying = crate::protect::applying(); // (an apply's migration calling it: its calls back may change a project's objects, until the call ends)
    LENT.lock().unwrap().insert(token.clone(), Lent { role, who, files, secrets: vec![], session: crate::temp::current(), vars: crate::vars::lend(), applying });
    Lease(token)
}

/// What a lent token allows, while its procedure runs.
pub fn lent(token: Option<&str>) -> Option<(Role, bool)> { LENT.lock().unwrap().get(token?).map(|l| (l.role, l.files)) }

/// Whether a lent token's procedure runs inside an apply (`protect::applying`): its requests are the apply's.
pub fn lent_applying(token: Option<&str>) -> bool { token.is_some_and(|t| LENT.lock().unwrap().get(t).is_some_and(|l| l.applying)) }

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

/// Failed sign-ins per address and user name (S30, invariant 381): ten in a minute, and that pair is
/// refused at once (the right password too) until its minute has fewer. Per pair, not per address: behind
/// one proxy or load balancer, ten wrong passwords from anyone would lock everyone out. A bearer token
/// isn't counted: it is random, so guessing one isn't the risk.
const TRIES: usize = 10;
const MINUTE: std::time::Duration = std::time::Duration::from_secs(60);
const PAIRS: usize = 10_000; // (the pairs held at once: when full, the one whose newest failure is oldest goes)

#[derive(Default)]
pub struct Guesses {
    failures: std::sync::Mutex<std::collections::HashMap<(std::net::IpAddr, String), std::collections::VecDeque<std::time::Instant>>>,
}

static GUESSED: std::sync::LazyLock<Guesses> = std::sync::LazyLock::new(Default::default);

/// Seconds to wait before this address may try this user name again, when it has had ten failures in
/// the last minute (None: it may try). A door that can't tell the address counts under 0.0.0.0.
pub fn guessing(addr: Option<std::net::IpAddr>, user: &str) -> Option<u64> { GUESSED.guessing_at(addr, user, std::time::Instant::now()) }

/// A failed sign-in of this address and user name, now.
pub fn guessed(addr: Option<std::net::IpAddr>, user: &str) { GUESSED.guessed_at(addr, user, std::time::Instant::now()) }

impl Guesses {
    fn key(addr: Option<std::net::IpAddr>, user: &str) -> (std::net::IpAddr, String) {
        (addr.unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)), user.to_string())
    }

    fn guessing_at(&self, addr: Option<std::net::IpAddr>, user: &str, now: std::time::Instant) -> Option<u64> {
        let key = Self::key(addr, user);
        let mut all = self.failures.lock().unwrap();
        let times = all.get_mut(&key)?;
        while times.front().is_some_and(|t| now.saturating_duration_since(*t) >= MINUTE) {
            times.pop_front();
        }
        let n = times.len();
        if n < TRIES {
            if n == 0 {
                all.remove(&key);
            }
            return None;
        }
        // (the pair has fewer than ten again once its (n - 9)th failure, the one `TRIES` back from the
        // newest, is a minute old)
        let until = times[n - TRIES];
        let left = MINUTE.saturating_sub(now.saturating_duration_since(until));
        Some(left.as_secs_f64().ceil().max(1.0) as u64)
    }

    fn guessed_at(&self, addr: Option<std::net::IpAddr>, user: &str, now: std::time::Instant) {
        let key = Self::key(addr, user);
        let mut all = self.failures.lock().unwrap();
        if !all.contains_key(&key) && all.len() >= PAIRS {
            // (full: the pair whose newest failure is oldest goes; a scan, and only when full)
            let oldest = all.iter().min_by_key(|(_, times)| times.back().copied()).map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                all.remove(&k);
            }
        }
        let times = all.entry(key).or_default();
        while times.front().is_some_and(|t| now.saturating_duration_since(*t) >= MINUTE) {
            times.pop_front();
        }
        times.push_back(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::{Duration, Instant};

    fn ip(last: u8) -> Option<IpAddr> { Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))) }

    #[test]
    fn ten_failures_refuse_the_pair_for_a_minute() {
        let (g, t) = (Guesses::default(), Instant::now());
        for _ in 0..9 {
            g.guessed_at(ip(1), "al", t);
        }
        assert_eq!(g.guessing_at(ip(1), "al", t), None, "nine failures: it may still try");
        g.guessed_at(ip(1), "al", t);
        let wait = g.guessing_at(ip(1), "al", t).expect("ten failures: refused");
        assert!((1..=60).contains(&wait), "the wait is at most a minute, was {wait}");
    }

    #[test]
    fn another_user_or_address_is_free() {
        let (g, t) = (Guesses::default(), Instant::now());
        for _ in 0..10 {
            g.guessed_at(ip(1), "al", t);
        }
        assert_eq!(g.guessing_at(ip(1), "bo", t), None, "another user from the same address");
        assert_eq!(g.guessing_at(ip(2), "al", t), None, "the same user from another address");
    }

    #[test]
    fn old_failures_expire() {
        let (g, t) = (Guesses::default(), Instant::now());
        for _ in 0..10 {
            g.guessed_at(ip(1), "al", t);
        }
        assert!(g.guessing_at(ip(1), "al", t + Duration::from_secs(59)).is_some(), "a minute not yet over");
        assert_eq!(g.guessing_at(ip(1), "al", t + Duration::from_secs(61)), None, "a minute over: free again");
    }

    #[test]
    fn a_door_that_cannot_tell_the_address_counts_under_zero() {
        let (g, t) = (Guesses::default(), Instant::now());
        for _ in 0..10 {
            g.guessed_at(None, "al", t);
        }
        assert!(g.guessing_at(None, "al", t).is_some());
        assert!(g.guessing_at(Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)), "al", t).is_some());
    }

    #[test]
    fn the_pairs_held_are_at_most_ten_thousand() {
        let (g, t) = (Guesses::default(), Instant::now());
        for i in 0..PAIRS + 5 {
            g.guessed_at(ip(1), &format!("u{i}"), t + Duration::from_millis(i as u64));
        }
        let all = g.failures.lock().unwrap();
        assert_eq!(all.len(), PAIRS);
        assert!(!all.contains_key(&Guesses::key(ip(1), "u0")), "the oldest pair went");
    }
}
