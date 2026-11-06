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

    /// May it read every column of `table`? (What a stream of its rows needs: Kafka, Flight, lookups.) A
    /// hidden table (the lake's records, a table's old versions) only a superuser streams: SQL reads the
    /// records through `pondra.audit` and `pondra.history`, which show a user its own statements only.
    pub fn may_read_all(&self, table: &str) -> bool {
        if crate::sys::hidden(table) {
            return self.role == Role::Admin && self.access.is_none();
        }
        self.access.as_ref().is_none_or(|a| a.columns("select", &catalog_name(table)) == Some(None))
    }
}

tokio::task_local! {
    /// The principal of the request being served.
    pub static WHO: Principal;
}

/// The principal of the request being served (None: no door set one: this process's own work).
pub fn current() -> Option<Principal> { WHO.try_with(|p| p.clone()).ok() }

/// Who is asking, as `current_user` says it: the principal's name, or `pondra` where no one signed in (an open node, the
/// process's own work), as the Postgres port says it.
pub fn name() -> String { current().map(|p| p.name).filter(|n| !n.is_empty()).unwrap_or_else(|| "pondra".into()) }

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
        Some(p) if !p.may_read_all(table) && crate::sys::hidden(table) => bail!("permission denied: {table} is Pondra's own, and its rows stream to a superuser only"),
        Some(p) if !p.may_read_all(table) => bail!("permission denied: SELECT on every column of {table}, which a stream of its rows needs (GRANT SELECT ON {table} TO {})", p.name),
        _ => Ok(()),
    }
}

/// A user's grants, if the request being served is a user's who isn't a superuser.
pub fn limited() -> Option<Arc<crate::users::Access>> { current().and_then(|p| p.access) }

/// Whether the request being served is shown routines' code, tasks' statements and views' queries (S27): a token, a
/// superuser, an open node, and a user who could hold them anyway (APPLY deploys them, CLONE ON DATABASE copies them
/// into a branch it owns). A user granted some tables is shown their names and parameters only.
pub fn shows_code() -> bool { limited().is_none_or(|a| a.applies() || a.clones().is_some_and(|s| s.is_empty())) }

/// Is the request being served from whoever runs the nodes (`Principal::token`)? (None: work no
/// door started, a task's included, is not: whoever made the task may be anyone.)
pub fn operator() -> bool { current().is_some_and(|p| p.operator) }

/// Does a node in this process take tokens? (Then nothing it serves is open, attached lakes included.)
pub fn tokens_on() -> bool { TOKENS.load(std::sync::atomic::Ordering::Relaxed) }

crate::per_node!(static TOKENS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false));

/// The header that carries, with a change a node sends its leader, the user it is for: the leader
/// runs it as that user, so its subqueries read with the user's grants, as where it came in. Only the
/// cluster's own calls may say it (the nodes' key or the admin token, to `/cluster/*`), and it only narrows what they may do.
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
#[cfg(feature = "doors")]
pub fn cross_site(h: &axum::http::HeaderMap, method: &axum::http::Method, from: Option<std::net::SocketAddr>, own: &str) -> Option<String> {
    let header = |k: &str| h.get(k).and_then(|v| v.to_str().ok()).map(str::to_ascii_lowercase);
    let host = header("host").unwrap_or_default();
    let away = "a page from another site may not use this node through your browser: open the node's own console, or give the node a token (--admin-token)";
    // (a page from an origin the operator named, `origins.rs`, is from another site by that choice: the two checks
    // that refuse other sites skip it, and the DNS check below still runs. `listed` reads only the names, never `*`)
    if header("origin").is_some_and(|o| crate::origins::listed(&o)) {
        return rebinding(&host, from, own);
    }
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
    rebinding(&host, from, own)
}

/// A request from this machine by a name that isn't this machine's: a page may have pointed its name here (DNS
/// rebinding). An address, `localhost` or this machine's own name is fine.
#[cfg(feature = "doors")]
fn rebinding(host: &str, from: Option<std::net::SocketAddr>, own: &str) -> Option<String> {
    let name = host.rsplit_once(':').filter(|(_, p)| p.parse::<u16>().is_ok()).map_or(host, |(n, _)| n);
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

/// This machine's name, as the OS says it (a page has none: its host is the browser's).
#[cfg(not(target_arch = "wasm32"))]
static MACHINE: std::sync::LazyLock<Option<String>> = std::sync::LazyLock::new(sysinfo::System::host_name);
#[cfg(target_arch = "wasm32")]
static MACHINE: std::sync::LazyLock<Option<String>> = std::sync::LazyLock::new(|| None);

/// May the request being served add rows to `table` (as the catalog names it) through a door that
/// takes rows, not SQL (`/append`, `/insert`, Flight's DoPut and ingest, Kafka's produce)? Its INSERT
/// grant, as SQL's `INSERT` asks; and never a system table (`$` in its name: `pondra$audit`,
/// `t$deleted`), which only Pondra writes.
pub fn check_rows(table: &str) -> Result<()> {
    ensure!(!table.contains('$'), "{table} is written by Pondra alone");
    check("insert", table)
}

/// What a write that changes rows already there asks of a user's grants beyond INSERT, as SQL's own
/// do (S11): rows that replace their key's (`replaces`: ON CONFLICT DO UPDATE asks UPDATE) and rows
/// taken out (`deletes`: DELETE). A door that takes rows or another engine's commits asks it once it
/// knows, so a user granted INSERT alone neither overwrites nor deletes through a back door.
pub fn check_changes(table: &str, replaces: bool, deletes: bool) -> Result<()> {
    if replaces {
        check("update", table)?;
    }
    if deletes {
        check("delete", table)?;
    }
    Ok(())
}

/// A row door's rows into a table (`/append`, Flight, Kafka, Postgres's COPY): a keyed table's
/// replace the rows of their keys, and its rows marked `_deleted` (a tombstone, Debezium's delete)
/// delete them.
pub fn check_load(meta: &crate::store::TableMeta, table: &str, rows: &datafusion::arrow::array::RecordBatch) -> Result<()> {
    if limited().is_none() {
        return Ok(()); // (a token's role, or the node's own work: no grants to ask)
    }
    let marked = rows.column_by_name("_deleted").and_then(|c| c.as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>()).is_some_and(|d| d.true_count() > 0);
    check_changes(table, !meta.key.is_empty(), marked)
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
    // (a reserve past one is a branch's REFRESH skipping the numbers its base took: the leader's own, never a writer's)
    ensure!(f.sequence.is_none() && f.blocks == 0 && f.reserve <= 1, "moving a sequence or a branch's row ids or commit numbers needs an admin");
    if let Some(p) = f.parts.iter().find(|p| RECORDS.contains(&p.table.as_str())) {
        bail!("{} is written by Pondra alone", p.table);
    }
    let seg = f.path.strip_prefix("log/").and_then(|n| n.strip_suffix(".seg"));
    ensure!(f.path.is_empty() || seg.is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')), "a flush's rows are in a segment of the log (log/<id>.seg), not {}", f.path);
    Ok(())
}

/// The same for a bulk INSERT's files: into a table that is there (`folder`: its folder under `data/`), each in that folder,
/// and never the lake's own records.
pub fn files_from_writer(table: &str, folder: Option<&str>, files: &[String]) -> Result<()> {
    if !writer()? {
        return Ok(());
    }
    ensure!(!RECORDS.contains(&table), "{table} is written by Pondra alone"); // (as a flush's rows are)
    let Some(folder) = folder else { bail!("making a table needs an admin") };
    let home = format!("data/{folder}/");
    ensure!(files.iter().all(|f| f.starts_with(&home) && !f.split('/').any(|part| part == ".." || part == ".")), "a table's files are in its folder ({home})");
    Ok(())
}

/// A caller's slug: the one folder of its own a joined page stages its rows in, `stage/<slug>/` (`page_creates`). The user
/// name in hex, one-to-one and safe in any key. None for a principal with no name of its own (a token, an open node).
pub fn slug(p: &Principal) -> Option<String> { (!p.operator && !p.name.is_empty()).then(|| crate::text::hex(p.name.as_bytes())) }

/// An object a joined page may make in the lake (through its node's `/store`, once: `served::lake_files_only`): a file of its
/// own staged rows, `stage/<slug>/<job>/<n>.parquet`, where `<slug>` is the caller's own (`slug`: another's is refused, and a
/// caller with none stages nothing), `<job>` is `[A-Za-z0-9-]{1,64}` and `<n>` is at most six digits. The node's INSERT reads
/// those files and then deletes them (`server::insert`).
pub fn page_creates(key: &str) -> bool {
    let Some(me) = current().and_then(|p| slug(&p)) else { return false };
    let mut parts = key.split('/');
    let (Some("stage"), Some(who), Some(job), Some(file), None) = (parts.next(), parts.next(), parts.next(), parts.next(), parts.next()) else { return false };
    let n = file.strip_suffix(".parquet").unwrap_or_default();
    who == me && staged_job(job) && (1..=6).contains(&n.len()) && n.bytes().all(|b| b.is_ascii_digit())
}

/// A job name a staged INSERT takes: letters, digits and hyphens, 64 at most (it is a folder's name in `stage/`).
pub fn staged_job(job: &str) -> bool { !job.is_empty() && job.len() <= 64 && job.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') }

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

/// A locking read (`SELECT … FOR UPDATE`) takes the rows as an UPDATE would, so it needs what an UPDATE needs: a write
/// token, or a user granted UPDATE on the table (Postgres asks the UPDATE privilege of FOR UPDATE, 42501 without it).
pub fn may_lock(table: &str) -> Result<()> {
    match current() {
        Some(p) if p.access.is_none() && p.role < Role::Write => bail!("permission denied: FOR UPDATE on {table} needs UPDATE, and this token may only read"),
        _ => check("update", &catalog_name(table)),
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

    /// The three tokens as the environment gives them (`PONDRA_READ_TOKEN`, `PONDRA_WRITE_TOKEN`, `PONDRA_ADMIN_TOKEN`): what
    /// a library's node and a guest take.
    pub fn from_env() -> Auth {
        Auth::new(crate::env::get("PONDRA_READ_TOKEN"), crate::env::get("PONDRA_WRITE_TOKEN"), crate::env::get("PONDRA_ADMIN_TOKEN"))
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

    /// The key of the program that started this node (`owned_by`), if one did: what the node's own calls are signed with.
    pub fn owner_key(&self) -> Option<&str> { self.owner.as_deref() }

    /// Is `key` the key of the program that started this node (`owned_by`)? Compared in constant time.
    pub fn is_owner(&self, key: &str) -> bool {
        self.owner.as_ref().is_some_and(|k| crate::crypto::same(k.as_bytes(), key.as_bytes()))
    }

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
        let is = |t: &Option<String>| t.as_ref().is_some_and(|t| crate::crypto::same(t.as_bytes(), token.as_bytes()));
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
            "files" if runs_as_code(path) => Role::Admin, // (code a node runs: an admin stores it, invariant 85)
            "files" => Role::Write,
            "" | "console" => Role::None, // (the console's page and files: they hold no data, and ask for a token)
            "stats" | "healthz" | "ready" => Role::None, // (health checks: load balancers and the tests poll them)
            "login" | "whoami" | "sso" | ".well-known" => Role::None, // (signing in, and where to: ADR-074; and who one is)
            "secrets" => Role::None, // (a procedure's lent token only: `server::secret`)
            "v1" if method == "POST" || method == "DELETE" => Role::Write, // (another engine's append: ADR-028; its tables made, dropped and renamed: ADR-029, as the SQL's rights say)
            "sql" | "lookup" | "watch" | "live" | "sessions" | "mcp" | "v1" | "metrics" | "routines" | "objects" | "kinds" | "plan" | "apply" | "test" | "status" | "capture" | "export" => Role::Read, // (MCP writes are checked by `allows`; v1: the Iceberg REST catalog; an apply, not its plan, needs an admin: `apply::ask`)
            "append" | "insert" => Role::Write,
            "cluster" if path.starts_with("/cluster/files") || path.starts_with("/cluster/commit") => Role::Write, // (writers on other machines)
            "cluster" if path.starts_with("/cluster/leader") => Role::None,
            _ => Role::Admin,
        }
    }

    /// The nodes' own routes (`/cluster/*`) a user's sign-in may reach, a superuser's too (`server::guard`): a branch's pin
    /// (`ddl` lets nothing else of a user's through), the leader's probe, where the node's Kafka door is (the console
    /// asks), and what a page that joined the cluster follows: the commit stream and the catalog, which answer a user only
    /// the keys `objects::readable` allows (`server::feed`, `server::kv`), and where a superuser's joined page sends its own
    /// statements' lines (`server::records`, `page_records`). The others carry the lake's keys or write what only the nodes write.
    pub fn users_reach(path: &str) -> bool { matches!(path, "/cluster/ddl" | "/cluster/leader" | "/cluster/kafka" | "/cluster/log" | "/cluster/kv" | "/cluster/records") }

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
            let what = match stmt {
                Stmt::CopyTo(..) => "write files outside the lake",
                _ if need == Role::Admin => "change the lake's tables, views, schemas, routines or users (an admin token, or a superuser, does)",
                _ => "write",
            };
            return Err(crate::codes::coded("42501", format!("this {} may not {what}", if limited().is_some() { "user" } else { "token" }))); // (insufficient_privilege, by code: no door reads the words)
        }
        let (t, privileges) = needs(stmt);
        ensure!(!crate::sys::hidden(&t), "{t} is written by Pondra alone"); // (the lake's records, a table's old versions: superusers too)
        privileges.iter().try_for_each(|p| check(p, &t))
    }
}

/// Does a request's path name code a node runs? Decided on the name the door stores it under, so the path is
/// percent-decoded as the door decodes it (`/files/a/job.p%79` is `job.py`).
fn runs_as_code(path: &str) -> bool { py_name(&percent_encoding::percent_decode_str(path).decode_utf8_lossy()) }

/// The `.py` test on a name (`files::put` asks it of the name the store keeps): any case, and no trailing `/`, dot or
/// space, which the store drops and Windows drops.
pub(crate) fn py_name(name: &str) -> bool { name.trim_end_matches(['/', '.', ' ']).to_ascii_lowercase().ends_with(".py") }

/// The table a write changes (as the catalog names it) and the privileges it needs on it: none for
/// anything else.
pub fn needs(stmt: &Stmt) -> (String, Vec<&'static str>) {
    match stmt {
        Stmt::Insert(t, _) | Stmt::InsertInto(t, ..) | Stmt::CopyInto(t, ..) => (catalog_name(t), vec!["insert"]), // (COPY INTO's rows are an INSERT's)
        Stmt::Update(t, ..) => (catalog_name(t), vec!["update"]),
        Stmt::Delete(t, _) => (catalog_name(t), vec!["delete"]),
        Stmt::Merge(m) => (catalog_name(&m.target), m.privileges()),
        // (a `$` table is Pondra's: a user's CREATE or definition of one is refused as a row write to it is. A replace's staged
        // table is defined by the leader's own steps, which no door checks: `write::create_as`)
        Stmt::Create(c) => (catalog_name(&crate::text::object(&c.name)), vec![]),
        Stmt::Define(t, _) | Stmt::AddColumn(t, ..) | Stmt::SetOptions(t, _) => (catalog_name(t), vec![]),
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
        [s, t] if s == crate::text::PUBLIC => t.to_string(),
        _ => t.to_string(),
    }
}

crate::per_node!(static LENT: std::sync::Mutex<std::collections::HashMap<String, Lent>> = Default::default());

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
    failures: std::sync::Mutex<std::collections::HashMap<(std::net::IpAddr, String), std::collections::VecDeque<datafusion::common::instant::Instant>>>,
}

// (a node's own: two nodes in one process count apart, ADR-061 §3)
crate::per_node!(static GUESSED: Guesses = Guesses::default(); clear = |g| {
    let taken = std::mem::take(&mut *g.failures.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
    drop(taken);
});

/// Seconds to wait before this address may try this user name again, when it has had ten failures in
/// the last minute (None: it may try). A door that can't tell the address counts under 0.0.0.0. Each node
/// counts its own, so a cluster of N nodes allows N times the guesses until 1.1 (redesign step 11's shared state).
pub fn guessing(addr: Option<std::net::IpAddr>, user: &str) -> Option<u64> { GUESSED.guessing_at(addr, user, datafusion::common::instant::Instant::now()) }

/// A failed sign-in of this address and user name, now.
pub fn guessed(addr: Option<std::net::IpAddr>, user: &str) { GUESSED.guessed_at(addr, user, datafusion::common::instant::Instant::now()) }

/// What a sign-in refused for its failures is told, with how long to wait (`guessing`), at every door.
pub fn guessing_says(user: &str, wait: u64) -> String { format!("too many failed sign-ins for {user} from this address: wait {wait} s") }

impl Guesses {
    fn key(addr: Option<std::net::IpAddr>, user: &str) -> (std::net::IpAddr, String) {
        (addr.unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)), user.to_string())
    }

    fn guessing_at(&self, addr: Option<std::net::IpAddr>, user: &str, now: datafusion::common::instant::Instant) -> Option<u64> {
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

    fn guessed_at(&self, addr: Option<std::net::IpAddr>, user: &str, now: datafusion::common::instant::Instant) {
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

/// A credential as a client sent it, read alike at every door (redesign step 7, piece 1): an `Authorization` header (HTTP,
/// MCP, Iceberg REST, Delta Sharing, Flight), Kafka's SASL PLAIN, Postgres's password message. It decides no rights: a
/// door asks `users` and `Auth` whom it signs in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Credential {
    /// A bearer token: a node's token, a user's (`pt_`), a session (`ps_`), a provider's JWT, a key, a lent token.
    Token(String),
    /// A name and a password (which may itself be a token, under any name).
    Password { user: String, password: String },
    /// A Bearer or Basic header that can't be read, and why: refused at every door with `Credential::MALFORMED`, never
    /// taken for no credential.
    Malformed(&'static str),
    /// None sent, or a scheme Pondra doesn't read.
    None,
}

impl Credential {
    /// What every door answers a credential it can't read, before why.
    pub const MALFORMED: &'static str = "the credential can't be read";

    /// An `Authorization` header's value: `Bearer <token>` or `Basic <base64 of name:password>`. The scheme in any case
    /// (RFC 9110 §11.1), the spaces around its value taken off, Basic's base64 with its padding or without (as ADBC
    /// sends it), the name ended at the first `:`.
    pub fn of_header(value: Option<&str>) -> Credential {
        let Some((scheme, rest)) = value.map(str::trim).and_then(|v| v.split_once(' ')) else {
            return match value.map(str::trim) {
                Some(v) if v.eq_ignore_ascii_case("bearer") => Credential::Malformed("a Bearer header with no token"),
                Some(v) if v.eq_ignore_ascii_case("basic") => Credential::Malformed("a Basic header with no name and password"),
                _ => Credential::None,
            };
        };
        let rest = rest.trim();
        if scheme.eq_ignore_ascii_case("bearer") {
            return match rest {
                "" => Credential::Malformed("a Bearer header with no token"),
                t => Credential::Token(t.to_string()),
            };
        }
        if !scheme.eq_ignore_ascii_case("basic") {
            return Credential::None;
        }
        use base64::Engine;
        let lenient = base64::engine::GeneralPurpose::new(&base64::alphabet::STANDARD, base64::engine::GeneralPurposeConfig::new().with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent));
        let Ok(bytes) = lenient.decode(rest) else { return Credential::Malformed("a Basic header's value isn't base64") };
        let Ok(text) = String::from_utf8(bytes) else { return Credential::Malformed("a Basic header's name and password aren't UTF-8") };
        match text.split_once(':') {
            Some((user, password)) => Credential::Password { user: user.to_string(), password: password.to_string() },
            None => Credential::Malformed("a Basic header with no ':' between the name and the password"),
        }
    }

    /// A request's `Authorization` header (`of_header`); one that isn't text can't be read.
    pub fn of_headers(headers: &http::HeaderMap) -> Credential {
        match headers.get(http::header::AUTHORIZATION).map(|v| v.to_str()) {
            None => Credential::None,
            Some(Ok(v)) => Credential::of_header(Some(v)),
            Some(Err(_)) => Credential::Malformed("an Authorization header that isn't text"),
        }
    }

    /// Kafka's SASL PLAIN message (RFC 4616): `authzid NUL name NUL password`.
    pub fn of_sasl_plain(message: &[u8]) -> Credential {
        let parts: Vec<&[u8]> = message.split(|b| *b == 0).collect();
        let [_, user, password] = parts[..] else { return Credential::Malformed("a SASL PLAIN message that isn't authzid, name and password") };
        match (std::str::from_utf8(user), std::str::from_utf8(password)) {
            (Ok(user), Ok(password)) => Credential::Password { user: user.to_string(), password: password.to_string() },
            _ => Credential::Malformed("a SASL PLAIN name and password that aren't UTF-8"),
        }
    }

    /// The token, if it is one.
    pub fn token(&self) -> Option<&str> {
        match self {
            Credential::Token(t) => Some(t),
            _ => None,
        }
    }

    /// The name a password came with (who tried, for the audit and the guess count).
    pub fn user(&self) -> Option<&str> {
        match self {
            Credential::Password { user, .. } => Some(user),
            _ => None,
        }
    }

    /// The secret itself: a token, or a password (which may be a token).
    pub fn secret(&self) -> Option<&str> {
        match self {
            Credential::Token(t) => Some(t),
            Credential::Password { password, .. } => Some(password),
            _ => None,
        }
    }

    /// What a door answers it when it can't be read (`MALFORMED`, and why).
    pub fn malformed(&self) -> Option<String> {
        match self {
            Credential::Malformed(why) => Some(format!("{}: {why}", Credential::MALFORMED)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::{Duration, Instant};

    #[test]
    fn a_credential_is_read_alike_at_every_door() {
        use Credential::*;
        let pw = |u: &str, p: &str| Password { user: u.into(), password: p.into() };
        let b64 = |s: &str| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(s)
        };
        let cases: Vec<(String, Credential)> = vec![
            ("Bearer pt_abc".into(), Token("pt_abc".into())),
            ("bearer pt_abc".into(), Token("pt_abc".into())), // (the scheme in any case)
            ("BEARER   pt_abc  ".into(), Token("pt_abc".into())),
            (format!("Basic {}", b64("ann:secret")), pw("ann", "secret")),
            (format!("basic {}", b64("ann:secret").trim_end_matches('=')), pw("ann", "secret")), // (no padding: ADBC)
            (format!("Basic {}", b64("ann:a:b")), pw("ann", "a:b")), // (the name ends at the first ':')
            (format!("Basic {}", b64(":tok")), pw("", "tok")),
            ("Bearer".into(), Malformed("a Bearer header with no token")),
            ("Bearer   ".into(), Malformed("a Bearer header with no token")),
            ("Basic".into(), Malformed("a Basic header with no name and password")),
            ("Basic %%%".into(), Malformed("a Basic header's value isn't base64")),
            (format!("Basic {}", b64("ann")), Malformed("a Basic header with no ':' between the name and the password")),
            ("Digest x=1".into(), None),
            ("Token abc".into(), None),
            ("".into(), None),
        ];
        for (header, want) in cases {
            assert_eq!(Credential::of_header(Some(&header)), want, "{header:?}");
        }
        assert_eq!(Credential::of_header(Option::None), None);
        // (one name and password, as each door's wire carries it, is one credential)
        let http = Credential::of_header(Some(&format!("Basic {}", b64("ann:pässword")))); // (HTTP and Flight Basic)
        let kafka = Credential::of_sasl_plain(b"\0ann\0p\xc3\xa4ssword");
        let pg = pw("ann", "pässword"); // (Postgres's password message is the password; the name is the startup's)
        assert_eq!((&http, &kafka), (&pg, &pg));
        assert_eq!(Credential::of_sasl_plain(b"ann\0secret"), Malformed("a SASL PLAIN message that isn't authzid, name and password"));
        assert_eq!(Credential::of_sasl_plain(b"\0ann\0\xff"), Malformed("a SASL PLAIN name and password that aren't UTF-8"));
        assert_eq!(Malformed("x").malformed().as_deref(), Some("the credential can't be read: x"));
    }

    fn ip(last: u8) -> Option<IpAddr> { Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))) }

    /// A write token's flush and files are checked (`writer`): a segment of the log only, a table's files in its folder, the
    /// lake's own records never. With no sign-in (the nodes' own), neither is checked, as before.
    #[test]
    fn a_writers_flush_and_files_are_checked_and_the_nodes_own_are_not() {
        use crate::log::{Flush, Part};
        let flush = |path: &str, table: Option<&str>| Flush {
            path: path.into(),
            parts: table.map(|t| Part { table: t.into(), off: 0, len: 0, rows: 0, src: None, placed: None }).into_iter().collect(),
            ..Default::default()
        };
        let mine = ["data/orders/a.parquet".to_string()];
        let other = ["data/hr/a.parquet".to_string()];
        futures::executor::block_on(WHO.scope(Principal::of(Role::Write), async {
            assert!(flush_from_writer(&flush("log/a1.seg", None)).is_ok());
            assert!(flush_from_writer(&flush("data/t/x.seg", None)).is_err(), "a segment outside the log");
            assert!(flush_from_writer(&flush("", Some("pondra$audit"))).is_err(), "the lake's records");
            assert!(flush_from_writer(&Flush { reserve: 1, ..Default::default() }).is_ok(), "a bulk INSERT's stamp takes one number");
            assert!(flush_from_writer(&Flush { reserve: 2, ..Default::default() }).is_err(), "more than one is the leader's own");
            assert!(files_from_writer("orders", Some("orders"), &mine).is_ok());
            assert!(files_from_writer("orders", Some("orders"), &other).is_err(), "a file outside the table's folder");
            assert!(files_from_writer("orders", None, &mine).is_err(), "a table that isn't there");
            assert!(files_from_writer("pondra$audit", Some("pondra$audit"), &["data/pondra$audit/a.parquet".to_string()]).is_err(), "the lake's records");
        }));
        assert!(flush_from_writer(&flush("data/t/x.seg", None)).is_ok(), "the nodes' own, with no sign-in: unchanged");
        assert!(files_from_writer("orders", Some("orders"), &other).is_ok(), "the nodes' own, unchanged");
    }

    /// What a joined page stages (`page_creates`): Parquet in its own slug's folder, `stage/<slug>/<job>/<n>.parquet`, for the
    /// user the request is for. Another's slug, a job name outside `[A-Za-z0-9-]{1,64}`, a file number past six digits, any
    /// other key, a token's and a caller with no name are all refused.
    #[test]
    fn a_page_stages_only_its_own_files() {
        let user = |name: &str| Principal { name: name.into(), role: Role::Admin, access: None, door: "http", from: None, operator: false };
        assert_eq!(slug(&user("ali")).as_deref(), Some("616c69"), "a user's slug is its name in hex");
        assert!(slug(&Principal::token(Role::Admin)).is_none(), "a token has no slug");
        let long = format!("stage/616c69/{}/0.parquet", "j".repeat(65));
        futures::executor::block_on(WHO.scope(user("ali"), async {
            for key in ["stage/616c69/j1/0.parquet", "stage/616c69/auto-3f2a/123456.parquet", "stage/616c69/a-b-C9/7.parquet"] {
                assert!(page_creates(key), "{key}");
            }
            for key in ["stage/626f62/j1/0.parquet", "stage/616c69/j_1/0.parquet", "stage/616c69/j.1/0.parquet", "stage/616c69//0.parquet",
                        "stage/616c69/j1/1234567.parquet", "stage/616c69/j1/.parquet", "stage/616c69/j1/a.csv", "stage/616c69/j1/0.parquet/x",
                        "stage/616c69/j1", "stage/616c69/j1/0.parquet/", "data/orders/3f2a.parquet", "log/0a1b.seg", "stage/616c69/j1/../0.parquet", long.as_str()] {
                assert!(!page_creates(key), "{key}");
            }
        }));
        assert!(!futures::executor::block_on(WHO.scope(user(""), async { page_creates("stage/616c69/j1/0.parquet") })), "a caller with no name stages nothing");
        assert!(!page_creates("stage/616c69/j1/0.parquet"), "no sign-in: nothing is staged");
    }

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

    /// A `.py` file is code a node runs whatever its case, its percent-encoding, or the trailing `/` the store drops
    /// (the name it is stored under decides: `runs_as_code`).
    #[test]
    fn a_py_file_is_code_by_the_name_it_is_stored_under() {
        for path in ["/files/a/job.py", "/files/a/job.p%79", "/files/a/job.PY/", "/files/a/job.py%20"] {
            assert!(runs_as_code(path), "{path}");
        }
        assert!(!runs_as_code("/files/a/job.sql"));
    }
}
