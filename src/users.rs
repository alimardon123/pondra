//! Users, roles and what each may do (ADR-035 §2): `CREATE USER`, `CREATE ROLE`, `GRANT`,
//! `REVOKE`, `CREATE TOKEN`, kept in the catalog under `u/<name>` (a role is a user who can't sign
//! in). A password is kept only as a SCRAM-SHA-256 verifier, as Postgres keeps it (salt, stored key,
//! server key): any node checks a password by hashing what it is given, and Postgres clients sign
//! in with SCRAM, so no door ever needs the password itself. A token is kept as its SHA-256. A
//! signed-in session is a short-lived token signed with the lake's key, which every node checks.
//!
//! What a user may do is its grants, its roles' and `public`'s (every user's): SELECT, INSERT,
//! UPDATE, DELETE on a table (SELECT on some of its columns), on every table of a schema or of
//! the lake; USAGE on a secret. A superuser may do everything, as the admin token does. The three
//! tokens stay, as the first admin and for scripts (`auth.rs`).
use crate::auth::{Principal, Role};
use crate::store::{json, Lake};
use anyhow::{anyhow, bail, ensure, Context, Result};
use aws_lc_rs::{digest, hmac};
use base64::{engine::general_purpose::STANDARD as B64, engine::general_purpose::URL_SAFE_NO_PAD as B64U, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json as j, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

pub fn user_key(name: &str) -> String { format!("u/{name}") }
const KEYS: &str = "z/auth"; // (the lake's own keys: sessions' signing key, and the nodes' key; `k/` is streaming tasks')
/// The names the three tokens sign in as over Postgres and Kafka: no user may take them.
pub const BUILT_IN: [&str; 3] = ["admin", "writer", "reader"];

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct User {
    #[serde(default)]
    pub login: bool, // a user (true) or a role
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verifier: Option<Verifier>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tokens: Vec<Token>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<String>, // what it is a member of
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub superuser: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default, flatten)]
    pub limits: Limits,
}

/// A user's quota, on each node: at most `max_queries` of its statements at once (the rest wait
/// their turn, 30 s at most), each at most `timeout_secs` (`MAX_QUERIES 4`,
/// `STATEMENT_TIMEOUT '5 minutes'`; 0 is none). Without its own: `PONDRA_USER_QUERIES`,
/// `PONDRA_USER_TIMEOUT` (seconds). A superuser has none.
#[derive(Serialize, Deserialize, Clone, Copy, Default, PartialEq, Debug)]
pub struct Limits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queries: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

/// A password as SCRAM-SHA-256 keeps it (RFC 5802, 7677): what checks it, never what makes it.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Verifier {
    pub salt: String, // (base64, as SCRAM sends them)
    pub iterations: u32,
    pub stored_key: String,
    pub server_key: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Token {
    pub name: String,
    pub hash: String, // (SHA-256, hex)
    pub created_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Grant {
    pub privilege: String, // select, insert, update, delete, usage, clone, deploy
    pub on: On,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>, // (SELECT on these only)
}

impl Grant {
    /// CLONE and DEPLOY are the database's own (`ON DATABASE`, ADR-058), not every table's.
    pub fn of_database(&self) -> bool { matches!(self.privilege.as_str(), "clone" | "deploy") }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum On {
    Table(String),  // as the catalog names it: `t`, `s.t`
    Schema(String), // every table of it, now and later
    Lake,           // every table of the lake
    Secret(String),
}

/// What a statement about users changes: the leader carries it out (`ddl::Ddl::Users`). A password
/// is turned into its verifier where the statement arrives: it goes no further.
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "do", rename_all = "snake_case")]
pub enum Change {
    Create { name: String, login: bool, if_not_exists: bool, verifier: Option<Verifier>, superuser: bool, #[serde(default)] limits: Limits },
    Alter { name: String, verifier: Option<Verifier>, superuser: Option<bool>, #[serde(default)] limits: Limits },
    Drop { name: String, if_exists: bool },
    Grant { privileges: Vec<String>, on: On, columns: Vec<String>, to: Vec<String>, #[serde(default, skip_serializing_if = "Option::is_none")] database: Option<String> },
    Revoke { privileges: Vec<String>, on: On, columns: Vec<String>, from: Vec<String>, #[serde(default, skip_serializing_if = "Option::is_none")] database: Option<String> },
    GrantRole { roles: Vec<String>, to: Vec<String> },
    RevokeRole { roles: Vec<String>, from: Vec<String> },
    CreateToken { name: String, user: String, expires_secs: Option<u64> },
    DropToken { name: String, user: String },
}

const PRIVILEGES: [&str; 7] = ["select", "insert", "update", "delete", "usage", "clone", "deploy"]; // (CLONE: a database's or a schema's, ADR-058; DEPLOY: a database's)

// ---------------------------------------------------------------- statements

/// `CREATE USER|ROLE`, `ALTER USER|ROLE`, `DROP USER|ROLE`, `GRANT`, `REVOKE`, `CREATE TOKEN`,
/// `DROP TOKEN`, as a write the leader carries out; None for anything else.
pub fn statement(sql: &str) -> Option<crate::write::Stmt> {
    use crate::write::Stmt;
    let words = Words::of(sql)?;
    let first = words.peek_word()?;
    if !matches!(first.as_str(), "create" | "alter" | "drop" | "grant" | "revoke") {
        return None;
    }
    let second = words.0.iter().rev().nth(1).map(|t| t.word()).unwrap_or_default(); // (the words are kept last first)
    let ours = match first.as_str() {
        "grant" | "revoke" => true,
        _ => matches!(second.as_str(), "user" | "role" | "token"),
    };
    if !ours {
        return None;
    }
    Some(match parse(words) {
        Ok(change) => Stmt::Ddl(vec![crate::ddl::Ddl::Users(change)]),
        Err(e) => Stmt::Invalid(e.to_string()),
    })
}

/// SQL's tokens, without spaces and comments (so quoting is SQL's, not a pattern's); `shares.rs`
/// reads its statements with them too.
pub(crate) struct Words(pub(crate) Vec<W>);

#[derive(Clone, Debug)]
pub(crate) enum W {
    Word(String, bool), // (its text as SQL resolves it, and whether it was quoted)
    Str(String),
    Num(String),
    Sym(char),
}

impl W {
    pub(crate) fn word(&self) -> String {
        match self {
            W::Word(w, false) => w.clone(),
            _ => String::new(),
        }
    }
}

impl Words {
    pub(crate) fn of(sql: &str) -> Option<Words> {
        use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};
        let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?;
        let mut out = vec![];
        for t in tokens {
            out.push(match t {
                Token::Whitespace(_) => continue,
                Token::Word(w) if w.quote_style.is_some() => W::Word(w.value, true),
                Token::Word(w) => W::Word(w.value.to_lowercase(), false),
                Token::SingleQuotedString(s) => W::Str(s),
                Token::Number(n, _) => W::Num(n),
                Token::Comma => W::Sym(','),
                Token::LParen => W::Sym('('),
                Token::RParen => W::Sym(')'),
                Token::Period => W::Sym('.'),
                Token::Eq => W::Sym('='),
                Token::SemiColon => continue,
                _ => W::Sym('?'),
            });
        }
        out.reverse(); // (taken from the end: the next is the last)
        Some(Words(out))
    }
    pub(crate) fn peek_word(&self) -> Option<String> { self.0.last().map(|w| w.word()) }
    pub(crate) fn next(&mut self) -> Option<W> { self.0.pop() }
    /// The next word, if it is `w` (taken).
    pub(crate) fn is(&mut self, w: &str) -> bool {
        let yes = self.peek_word().as_deref() == Some(w);
        if yes {
            self.0.pop();
        }
        yes
    }
    pub(crate) fn expect(&mut self, w: &str) -> Result<()> { if self.is(w) { Ok(()) } else { bail!("expected {} {}", w.to_uppercase(), self.near()) } }
    pub(crate) fn sym(&mut self, c: char) -> bool {
        let yes = matches!(self.0.last(), Some(W::Sym(s)) if *s == c);
        if yes {
            self.0.pop();
        }
        yes
    }
    pub(crate) fn near(&self) -> String {
        match self.0.last() {
            None => "at the end".into(),
            Some(W::Word(w, _)) | Some(W::Str(w)) | Some(W::Num(w)) => format!("near {w}"),
            Some(W::Sym(c)) => format!("near {c}"),
        }
    }
    /// A name: `a`, `"A"`, or dotted (`s.t`, `lake.s.t`).
    pub(crate) fn name(&mut self) -> Result<String> {
        let mut parts = vec![];
        loop {
            match self.next() {
                Some(W::Word(w, _)) => parts.push(w),
                _ => bail!("expected a name {}", self.near()),
            }
            if !self.sym('.') {
                return Ok(parts.join("."));
            }
        }
    }
    pub(crate) fn names(&mut self) -> Result<Vec<String>> {
        let mut all = vec![self.name()?];
        while self.sym(',') {
            all.push(self.name()?);
        }
        Ok(all)
    }
    pub(crate) fn string(&mut self) -> Result<String> {
        match self.next() {
            Some(W::Str(s)) => Ok(s),
            _ => bail!("expected a quoted string {}", self.near()),
        }
    }
    pub(crate) fn done(&self) -> Result<()> { if self.0.is_empty() { Ok(()) } else { bail!("unexpected words {}", self.near()) } }
}

fn parse(mut w: Words) -> Result<Change> {
    let verb = w.next().map(|x| x.word()).unwrap_or_default();
    if verb != "grant" && verb != "revoke" && w.is("token") {
        return token(&mut w, verb == "create");
    }
    match verb.as_str() {
        "create" | "alter" => {
            let login = match w.next().map(|x| x.word()).as_deref() {
                Some("user") => true,
                _ => false, // (role: checked by `statement`)
            };
            let if_not_exists = verb == "create" && w.is("if") && { w.expect("not")?; w.expect("exists")?; true };
            let name = plain(&w.name()?)?;
            w.is("with");
            let (mut verifier, mut superuser, mut login2, mut limits) = (None, None, None, Limits::default());
            while !w.0.is_empty() {
                if w.is("password") {
                    let p = w.string()?;
                    ensure!(p.chars().count() >= 8, "a password has at least 8 characters");
                    verifier = Some(Verifier::of(&p));
                } else if w.is("superuser") {
                    superuser = Some(true);
                } else if w.is("nosuperuser") {
                    superuser = Some(false);
                } else if w.is("login") {
                    login2 = Some(true);
                } else if w.is("nologin") {
                    login2 = Some(false);
                } else if w.is("max_queries") {
                    w.sym('=');
                    limits.max_queries = Some(match w.next() {
                        Some(W::Num(n)) => n.parse().map_err(|_| anyhow!("MAX_QUERIES is a whole number: 4 (0: no limit)"))?,
                        _ => bail!("MAX_QUERIES is a whole number: 4 (0: no limit)"),
                    });
                } else if w.is("statement_timeout") {
                    w.sym('=');
                    limits.timeout_secs = Some(match w.next() {
                        Some(W::Num(n)) if n == "0" => 0,
                        Some(W::Str(s)) => match crate::runs::every(&s) {
                            Ok(crate::runs::Every::Seconds(s)) => s,
                            _ => bail!("STATEMENT_TIMEOUT '5 minutes' (or seconds, hours; 0: none)"),
                        },
                        _ => bail!("STATEMENT_TIMEOUT '5 minutes' (or seconds, hours; 0: none)"),
                    });
                } else {
                    bail!("expected PASSWORD '…', SUPERUSER, NOSUPERUSER, LOGIN, NOLOGIN, MAX_QUERIES n or STATEMENT_TIMEOUT '…' {}", w.near());
                }
            }
            Ok(match verb.as_str() {
                "create" => Change::Create { name, login: login2.unwrap_or(login), if_not_exists, verifier, superuser: superuser.unwrap_or(false), limits },
                _ => {
                    ensure!(verifier.is_some() || superuser.is_some() || limits != Limits::default(), "ALTER USER {name} PASSWORD '…' | SUPERUSER | NOSUPERUSER | MAX_QUERIES n | STATEMENT_TIMEOUT '…'");
                    Change::Alter { name, verifier, superuser, limits }
                }
            })
        }
        "drop" => {
            w.next(); // (USER or ROLE)
            let if_exists = w.is("if") && { w.expect("exists")?; true };
            let name = plain(&w.name()?)?;
            w.done()?;
            Ok(Change::Drop { name, if_exists })
        }
        "grant" | "revoke" => grant(&mut w, verb == "grant"),
        _ => bail!("not a statement about users"),
    }
}

/// `CREATE TOKEN name FOR [USER] u [EXPIRES IN '30 days']`, `DROP TOKEN name FOR [USER] u`.
fn token(w: &mut Words, create: bool) -> Result<Change> {
    let name = plain(&w.name()?)?;
    w.expect("for")?;
    w.is("user");
    let user = plain(&w.name()?)?;
    if !create {
        w.done()?;
        return Ok(Change::DropToken { name, user });
    }
    let expires_secs = if w.is("expires") {
        w.expect("in")?;
        match crate::runs::every(&w.string()?) {
            Ok(crate::runs::Every::Seconds(s)) => Some(s),
            _ => bail!("EXPIRES IN '30 days' (or hours, minutes)"),
        }
    } else {
        None
    };
    w.done()?;
    Ok(Change::CreateToken { name, user, expires_secs })
}

/// `GRANT privileges [(columns)] ON [TABLE] t | SCHEMA s | ALL TABLES IN SCHEMA s | ALL TABLES |
/// SECRET s TO grantees`, `GRANT roles TO users`, and REVOKE's of each, `FROM`.
fn grant(w: &mut Words, grant: bool) -> Result<Change> {
    let mut privileges = vec![];
    let mut columns = vec![];
    let mut database = None; // (ON DATABASE: CLONE's alone, and the statement's own database)
    // (a list of privileges, or of roles: told apart by ON)
    let mut list = vec![];
    loop {
        let x = w.name()?;
        if w.sym('(') {
            columns = w.names()?;
            ensure!(w.sym(')'), "expected ) after the columns");
        }
        list.push(x);
        if w.is("privileges") {
            // (ALL PRIVILEGES)
        }
        if !w.sym(',') {
            break;
        }
    }
    let to = if grant { "to" } else { "from" };
    if !w.is("on") {
        w.expect(to)?;
        let users = w.names()?.iter().map(|n| plain(n)).collect::<Result<Vec<_>>>()?;
        w.done()?;
        let roles = list.iter().map(|n| plain(n)).collect::<Result<Vec<_>>>()?;
        return Ok(if grant { Change::GrantRole { roles, to: users } } else { Change::RevokeRole { roles, from: users } });
    }
    for p in list {
        match p.as_str() {
            "all" => privileges.extend(["select", "insert", "update", "delete"].map(String::from)),
            p if PRIVILEGES.contains(&p) => privileges.push(p.to_string()),
            p => bail!("{p}: the privileges are SELECT, INSERT, UPDATE, DELETE, ALL (on tables), USAGE (on secrets), CLONE (on a database or a schema) and DEPLOY (on a database)"),
        }
    }
    let on = if w.is("schema") {
        On::Schema(w.name()?)
    } else if w.is("secret") {
        On::Secret(w.name()?)
    } else if w.is("database") {
        database = Some(w.name()?.trim_matches('"').to_lowercase()); // (checked to be this one where it runs: `apply`)
        On::Lake
    } else if w.is("all") {
        w.expect("tables")?;
        if w.is("in") {
            w.expect("schema")?;
            On::Schema(w.name()?)
        } else {
            On::Lake
        }
    } else {
        w.is("table");
        On::Table(w.name()?)
    };
    ensure!(columns.is_empty() || privileges == ["select"] && matches!(on, On::Table(_)), "columns are named for SELECT on a table: GRANT SELECT (a, b) ON t TO r");
    let usage = matches!(on, On::Secret(_));
    ensure!(privileges.iter().all(|p| (p == "usage") == usage), "{}", if usage { "a secret's privilege is USAGE" } else { "USAGE is a secret's" });
    ensure!(!privileges.iter().any(|p| p == "clone") || database.is_some() || matches!(on, On::Schema(_)), "CLONE is a database's or a schema's: GRANT CLONE ON DATABASE prod | SCHEMA sales TO r");
    ensure!(!privileges.iter().any(|p| p == "deploy") || database.is_some(), "DEPLOY is a database's: GRANT DEPLOY ON DATABASE prod TO ci");
    ensure!(database.is_none() || privileges.iter().all(|p| p == "clone" || p == "deploy"), "ON DATABASE is for CLONE and DEPLOY alone: a database's tables are ON ALL TABLES");
    w.expect(to)?;
    let who = w.names()?.iter().map(|n| plain(n)).collect::<Result<Vec<_>>>()?;
    w.done()?;
    Ok(if grant { Change::Grant { privileges, on, columns, to: who, database } } else { Change::Revoke { privileges, on, columns, from: who, database } })
}

/// `ON DATABASE d` names the database the statement runs on: another's grants are made there.
fn this_database(lake: &Lake, named: Option<&str>) -> Result<()> {
    let here = crate::ddl::lake_name(lake);
    match named {
        Some(d) if d != here => bail!("this is the database {here}: GRANT … ON DATABASE {d} is run on {d}"),
        _ => Ok(()),
    }
}

/// A user's or role's name: a plain one, not the tokens' (`admin`, `writer`, `reader`).
fn plain(name: &str) -> Result<String> {
    ensure!(!name.is_empty() && name.len() <= 63 && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !name.starts_with(|c: char| c.is_ascii_digit()),
        "{name}: a user's or role's name is letters, digits and _, at most 63");
    ensure!(!BUILT_IN.contains(&name), "{name} is the {name} token's name (`pondra serve --{}-token`): pick another", if name == "reader" { "read" } else if name == "writer" { "write" } else { "admin" });
    Ok(name.to_string())
}

// ---------------------------------------------------------------- the leader carries them out

/// Leader: carry out a change (under the lake's lock).
pub async fn apply(lake: &Lake, c: Change) -> Result<Value> {
    let get = |n: &str| { let k = user_key(n); async move { lake.cat.get::<User>(&k).await } };
    let put = |n: &str, u: &User| (user_key(n), json(u));
    let must = |n: &str, u: Option<User>| u.ok_or_else(|| anyhow!("no user or role {n} (CREATE USER {n}, CREATE ROLE {n})"));
    let out = match c {
        Change::Create { name, login, if_not_exists, verifier, superuser, limits } => {
            if get(&name).await?.is_some() {
                ensure!(if_not_exists, "{} {name} already exists", if login { "user" } else { "role" });
                return Ok(j!({"user": name, "unchanged": true}));
            }
            ensure!(name != "public", "public is every user: GRANT … TO public");
            let u = User { login, verifier, superuser, created_ms: crate::log::now_ms(), limits, ..Default::default() };
            lake.cat.commit(vec![put(&name, &u)], &[]).await?;
            j!({ if login { "user" } else { "role" }: name })
        }
        Change::Alter { name, verifier, superuser, limits } => {
            let mut u = must(&name, get(&name).await?)?;
            if verifier.is_some() {
                u.verifier = verifier;
            }
            if let Some(n) = limits.max_queries {
                u.limits.max_queries = Some(n).filter(|n| *n > 0);
            }
            if let Some(s) = limits.timeout_secs {
                u.limits.timeout_secs = Some(s).filter(|s| *s > 0);
            }
            if let Some(s) = superuser {
                u.superuser = s;
            }
            lake.cat.commit(vec![put(&name, &u)], &[]).await?;
            j!({"user": name, "altered": true})
        }
        Change::Drop { name, if_exists } => {
            if get(&name).await?.is_none() {
                ensure!(if_exists, "no user or role {name}");
                return Ok(j!({"user": name, "dropped": false}));
            }
            // (out of every role it was in, and every member of it out of it)
            let mut puts = vec![];
            for (k, mut u) in lake.cat.scan::<User>("u/", "u0").await? {
                if u.roles.iter().any(|r| *r == name) {
                    u.roles.retain(|r| *r != name);
                    puts.push((k, json(&u)));
                }
            }
            lake.cat.commit(puts, &[user_key(&name)]).await?;
            j!({"user": name, "dropped": true})
        }
        Change::Grant { privileges, on, columns, to, database } => {
            this_database(lake, database.as_deref())?;
            let on = resolved(lake, on).await?;
            let mut puts = vec![];
            for n in &to {
                let mut u = grantee(lake, n).await?;
                for p in &privileges {
                    let g = Grant { privilege: p.clone(), on: on.clone(), columns: if p == "select" { columns.clone() } else { vec![] } };
                    u.grants.retain(|x| !(x.privilege == g.privilege && x.on == g.on)); // (the newest grant of it stands: columns replaced)
                    u.grants.push(g);
                }
                puts.push(put(n, &u));
            }
            lake.cat.commit(puts, &[]).await?;
            j!({"granted": privileges, "to": to})
        }
        Change::Revoke { privileges, on, columns, from, database } => {
            this_database(lake, database.as_deref())?;
            let on = resolved(lake, on).await?;
            let mut puts = vec![];
            for n in &from {
                let mut u = grantee(lake, n).await?;
                for p in &privileges {
                    // (some columns: out of a grant of some columns; none named: the grant goes)
                    u.grants.retain_mut(|x| match x.privilege == *p && x.on == on {
                        false => true,
                        true if columns.is_empty() || x.columns.is_empty() => !columns.is_empty(),
                        true => {
                            x.columns.retain(|c| !columns.contains(c));
                            !x.columns.is_empty()
                        }
                    });
                }
                puts.push(put(n, &u));
            }
            lake.cat.commit(puts, &[]).await?;
            j!({"revoked": privileges, "from": from})
        }
        Change::GrantRole { roles, to } => {
            for r in &roles {
                let role = must(r, get(r).await?)?;
                ensure!(!role.login, "{r} is a user: a user is granted roles, not other users");
            }
            let mut puts = vec![];
            for n in &to {
                let mut u = must(n, get(n).await?)?;
                for r in &roles {
                    ensure!(r != n && !member(lake, r, n, 0).await?, "{r} would then be a member of itself");
                    if !u.roles.contains(r) {
                        u.roles.push(r.clone());
                    }
                }
                puts.push(put(n, &u));
            }
            lake.cat.commit(puts, &[]).await?;
            j!({"granted": roles, "to": to})
        }
        Change::RevokeRole { roles, from } => {
            let mut puts = vec![];
            for n in &from {
                let mut u = must(n, get(n).await?)?;
                u.roles.retain(|r| !roles.contains(r));
                puts.push(put(n, &u));
            }
            lake.cat.commit(puts, &[]).await?;
            j!({"revoked": roles, "from": from})
        }
        Change::CreateToken { name, user, expires_secs } => {
            let mut u = must(&user, get(&user).await?)?;
            ensure!(u.login, "{user} is a role: a token is a user's");
            ensure!(!u.tokens.iter().any(|t| t.name == name), "{user} already has a token {name} (DROP TOKEN {name} FOR {user})");
            let mut secret = [0u8; 32];
            aws_lc_rs::rand::fill(&mut secret).map_err(|_| anyhow!("no randomness"))?;
            let token = format!("pt_{}_{}", B64U.encode(&user), B64U.encode(secret));
            let now = crate::log::now_ms();
            u.tokens.push(Token { name: name.clone(), hash: sha256(&token), created_ms: now, expires_ms: expires_secs.map(|s| now + s * 1000) });
            lake.cat.commit(vec![put(&user, &u)], &[]).await?;
            // (shown this once: only its hash is kept)
            j!({"token": token, "name": name, "user": user, "expires": expires_secs.map(|s| now + s * 1000)})
        }
        Change::DropToken { name, user } => {
            let mut u = must(&user, get(&user).await?)?;
            let before = u.tokens.len();
            u.tokens.retain(|t| t.name != name);
            ensure!(u.tokens.len() < before, "{user} has no token {name}");
            lake.cat.commit(vec![put(&user, &u)], &[]).await?;
            j!({"token": name, "user": user, "dropped": true})
        }
    };
    forget();
    Ok(out)
}

/// Whom a grant goes to: a user, a role, or `public` (every user: made when first granted to).
async fn grantee(lake: &Lake, name: &str) -> Result<User> {
    match lake.cat.get::<User>(&user_key(name)).await? {
        Some(u) => Ok(u),
        None if name == "public" => Ok(User::default()),
        None => bail!("no user or role {name} (CREATE ROLE {name})"),
    }
}

/// Is `who` a member of `role`, through any of its roles?
async fn member(lake: &Lake, who: &str, role: &str, depth: usize) -> Result<bool> {
    if depth > 16 {
        return Ok(true); // (a cycle: refused)
    }
    let Some(u) = lake.cat.get::<User>(&user_key(who)).await? else { return Ok(false) };
    for r in &u.roles {
        if r == role || Box::pin(member(lake, r, role, depth + 1)).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Is there a table or view `n` in `lake`?
async fn there(lake: &Lake, n: &str) -> Result<bool> {
    Ok(lake.cat.get::<Value>(&crate::store::table_key(n)).await?.is_some() || lake.cat.get::<Value>(&crate::ddl::query_key(n)).await?.is_some())
}

/// What a grant is on, named as the catalog names it, and there.
async fn resolved(lake: &Lake, on: On) -> Result<On> {
    let here = crate::ddl::lake_name(lake);
    let strip = |n: &str| {
        let parts: Vec<&str> = n.split('.').collect();
        let parts = if parts.len() == 3 && parts[0] == here { &parts[1..] } else { &parts[..] };
        match parts {
            [s, t] if *s == crate::ddl::PUBLIC => t.to_string(),
            p => p.join("."),
        }
    };
    Ok(match on {
        On::Table(n) => {
            let n = strip(&n);
            if let Ok((Some(other), local)) = crate::ddl::resolve(lake, &n).await {
                // (another database's table, `l.t`: what its own sign-in lets through, this grant narrows: `across`)
                ensure!(there(&other, &local).await?, "no table or view {n}");
                return Ok(On::Table(format!("{}.{local}", n.split('.').next().unwrap_or_default())));
            }
            ensure!(there(lake, &n).await?, "no table or view {n}");
            On::Table(n)
        }
        On::Schema(s) => {
            ensure!(crate::ddl::schemas(lake).await?.contains(&s), "no schema {s}");
            On::Schema(s)
        }
        On::Secret(s) => {
            ensure!(lake.cat.get::<Value>(&format!("e/{s}")).await?.is_some(), "no secret {s}"); // (`ext::secret_key`)
            On::Secret(s)
        }
        On::Lake => On::Lake,
    })
}

// ---------------------------------------------------------------- passwords, tokens, sessions

pub fn sha256(s: &str) -> String { digest::digest(&digest::SHA256, s.as_bytes()).as_ref().iter().map(|b| format!("{b:02x}")).collect() }

fn hmac256(key: &[u8], data: &[u8]) -> Vec<u8> { hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data).as_ref().to_vec() }

/// SCRAM's iterations (RFC 7677's 4,096 at least; Postgres's default is 4,096): a login costs this
/// many HMACs, about 10 ms; a password checked is remembered a minute (`VERIFIED`).
const ITERATIONS: u32 = 10_000;

impl Verifier {
    pub fn of(password: &str) -> Verifier {
        let mut salt = [0u8; 16];
        aws_lc_rs::rand::fill(&mut salt).expect("randomness");
        Verifier::with(password, &salt, ITERATIONS)
    }
    pub fn with(password: &str, salt: &[u8], iterations: u32) -> Verifier {
        let salted = salted(password, salt, iterations);
        let client_key = hmac256(&salted, b"Client Key");
        Verifier {
            salt: B64.encode(salt),
            iterations,
            stored_key: B64.encode(digest::digest(&digest::SHA256, &client_key)),
            server_key: B64.encode(hmac256(&salted, b"Server Key")),
        }
    }
    /// Is this the password? (Computed as a client would, compared in constant time.)
    pub fn matches(&self, password: &str) -> bool {
        let Ok(salt) = B64.decode(&self.salt) else { return false };
        let other = Verifier::with(password, &salt, self.iterations);
        aws_lc_rs::constant_time::verify_slices_are_equal(other.stored_key.as_bytes(), self.stored_key.as_bytes()).is_ok()
    }
}

/// SCRAM's Hi(): PBKDF2-HMAC-SHA-256 of the password (SASLprep'd as ASCII-printable is: as given).
fn salted(password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    aws_lc_rs::pbkdf2::derive(aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA256, std::num::NonZeroU32::new(iterations.max(1)).expect("non-zero"), salt, password.as_bytes(), &mut out);
    out
}

/// A Postgres client's SCRAM-SHA-256 exchange, on the server's side: the server's first message
/// for the client's, then the client's proof checked and the server's signature for it.
pub struct Scram {
    pub verifier: Verifier,
    nonce: String,
    first_bare: String,
    server_first: String,
}

impl Scram {
    /// The client's first message (`n,,n=user,r=nonce`): the server's (`r=…,s=salt,i=n`).
    pub fn first(verifier: Verifier, client_first: &str) -> Result<(Scram, String)> {
        let bare = client_first.strip_prefix("n,,").or_else(|| client_first.strip_prefix("y,,")).context("SCRAM without channel binding (n,,)")?;
        let cnonce = bare.split(',').find_map(|p| p.strip_prefix("r=")).context("the client's nonce")?;
        let mut more = [0u8; 18];
        aws_lc_rs::rand::fill(&mut more).map_err(|_| anyhow!("no randomness"))?;
        let nonce = format!("{cnonce}{}", B64.encode(more));
        let server_first = format!("r={nonce},s={},i={}", verifier.salt, verifier.iterations);
        Ok((Scram { verifier, nonce, first_bare: bare.to_string(), server_first: server_first.clone() }, server_first))
    }
    /// The client's final message (`c=…,r=…,p=proof`): the server's (`v=signature`), if the proof
    /// shows the client knows the password.
    pub fn last(&self, client_final: &str) -> Result<String> {
        let (without, proof) = client_final.rsplit_once(",p=").context("the client's proof")?;
        ensure!(without.split(',').any(|p| p.strip_prefix("r=") == Some(&self.nonce)), "the nonce changed");
        let auth = format!("{},{},{}", self.first_bare, self.server_first, without);
        let stored = B64.decode(&self.verifier.stored_key)?;
        let signature = hmac256(&stored, auth.as_bytes());
        let proof = B64.decode(proof)?;
        ensure!(proof.len() == signature.len(), "a proof of the wrong length");
        let client_key: Vec<u8> = proof.iter().zip(&signature).map(|(a, b)| a ^ b).collect();
        let check = digest::digest(&digest::SHA256, &client_key);
        ensure!(aws_lc_rs::constant_time::verify_slices_are_equal(check.as_ref(), &stored).is_ok(), "password authentication failed");
        Ok(format!("v={}", B64.encode(hmac256(&B64.decode(&self.verifier.server_key)?, auth.as_bytes()))))
    }
}

/// The lake's own keys, made the first time they're wanted: the one sessions are signed with, and
/// the one nodes call each other with when no admin token is set. Kept in the catalog, sealed by the
/// master key when every node shares one (`Kept`): a key that only reads the bucket, as a branch on
/// another server holds (ADR-058), must not sign anyone in.
#[derive(Serialize, Deserialize, Clone)]
struct Keys {
    session: String,
    node: String,
}

/// `z/auth` as kept: sealed (`ext::seal`, at format 3), or in the clear (a lake whose nodes share no
/// master key, or one made before; its leader seals it once every node can read it sealed: `seal_keys`).
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Kept {
    Sealed { sealed: String, key: String },
    Clear(Keys),
}

/// The format the lake's keys are sealed at: a release before it would find none.
const SEALED: u32 = 3;

fn sealed(k: &Keys) -> Result<Kept> {
    let (sealed, key) = crate::ext::seal(&serde_json::to_vec(k)?)?;
    Ok(Kept::Sealed { sealed, key })
}

static KEYS_HERE: LazyLock<Mutex<HashMap<String, Keys>>> = LazyLock::new(Default::default);

async fn keys(lake: &Lake) -> Result<Keys> {
    if let Some(k) = KEYS_HERE.lock().unwrap().get(&lake.url) {
        return Ok(k.clone());
    }
    static MAKING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(()); // (made once: a request may ask while the leader makes them)
    let _one = MAKING.lock().await;
    if let Some(k) = KEYS_HERE.lock().unwrap().get(&lake.url) {
        return Ok(k.clone());
    }
    let found = match lake.cat.get::<Kept>(KEYS).await? {
        // (a follower whose mirror was seeded before the leader's keys were flushed: its commit
        // stream won't open without them where users sign in, so they come from its own view)
        None if !lake.cat.is_writer() => lake.cat.from_view(KEYS).await?.map(|v| serde_json::from_slice(&v)).transpose()?,
        found => found,
    };
    let k = match found {
        Some(Kept::Clear(k)) => k,
        Some(Kept::Sealed { sealed, key }) => {
            let plain = crate::ext::unseal(&sealed, &key).map_err(|e| anyhow!("the lake's own keys are sealed by a master key this node doesn't have ({e:#}): every node needs the same PONDRA_SECRET_KEY (or PONDRA_KMS_COMMAND)"))?;
            serde_json::from_slice(&plain)?
        }
        None => {
            let mut a = [0u8; 32];
            let mut b = [0u8; 32];
            aws_lc_rs::rand::fill(&mut a).map_err(|_| anyhow!("no randomness"))?;
            aws_lc_rs::rand::fill(&mut b).map_err(|_| anyhow!("no randomness"))?;
            let k = Keys { session: B64.encode(a), node: format!("pn_{}", B64U.encode(b)) };
            ensure!(lake.cat.is_writer(), "the lake's keys aren't made yet: its leader makes them when it starts");
            // (sealed from the first write when no release older than format 3 can read the lake; otherwise
            // in the clear, and `seal_keys` seals them once every node can read them sealed)
            let fresh = crate::format::made() || crate::format::of(&lake.cat).await?.format >= SEALED;
            let kept = match crate::ext::shared_master() && fresh {
                true => {
                    crate::format::require(lake, SEALED, "the lake's own keys, sealed").await?;
                    sealed(&k)?
                }
                false => Kept::Clear(k.clone()),
            };
            lake.cat.commit(vec![(KEYS.to_string(), json(&kept))], &[]).await?;
            k
        }
    };
    KEYS_HERE.lock().unwrap().insert(lake.url.clone(), k.clone());
    Ok(k)
}

/// Leader: make the lake's keys if there are none yet (when it starts).
pub async fn make_keys(lake: &Lake) -> Result<()> { keys(lake).await.map(|_| ()) }

/// Leader: the lake's own keys sealed by the master key once every node can read them sealed
/// (format 3), when every node shares one (`ext::shared_master`); and wrapped again after the
/// master key changed (`PONDRA_SECRET_KEY_PREVIOUS`), as `ext::rewrap` does secrets'.
pub async fn seal_keys(lake: &Lake) -> Result<()> {
    keys(lake).await?; // (made, if they aren't yet)
    match lake.cat.get::<Kept>(KEYS).await? {
        Some(Kept::Clear(k)) if crate::ext::shared_master() => {
            crate::format::require(lake, SEALED, "the lake's own keys, sealed").await?;
            lake.cat.commit(vec![(KEYS.to_string(), json(&sealed(&k)?))], &[]).await?;
            eprintln!("the lake's own keys are sealed by its master key now");
        }
        Some(Kept::Sealed { sealed, key }) => {
            if let Some(key) = crate::ext::rewrapped(&key)? {
                lake.cat.commit(vec![(KEYS.to_string(), json(&Kept::Sealed { sealed, key }))], &[]).await?;
                eprintln!("the lake's own keys rewrapped with the master key in use now");
            }
        }
        _ => {}
    }
    Ok(())
}

/// How the lake's own keys are kept: "sealed", "clear", or "none" yet (`/stats`).
pub async fn kept(lake: &Lake) -> &'static str {
    match lake.cat.get::<Kept>(KEYS).await {
        Ok(Some(Kept::Sealed { .. })) => "sealed",
        Ok(Some(Kept::Clear(_))) => "clear",
        _ => "none",
    }
}

/// The key nodes call each other with when no admin token is set (`cluster::http`).
pub async fn node_key(lake: &Lake) -> Result<String> { Ok(keys(lake).await?.node) }

/// How long a signed-in session lasts: `PONDRA_SESSION_HOURS` (12).
fn session_ms() -> u64 { std::env::var("PONDRA_SESSION_HOURS").ok().and_then(|h| h.parse::<f64>().ok()).map_or(12 * 3_600_000, |h| (h * 3_600_000.0) as u64) }

/// `data` signed with the lake's key (base64url): what `vend.rs` puts in a file's link.
pub async fn sign(lake: &Lake, data: &[u8]) -> Result<String> { Ok(B64U.encode(hmac256(&B64.decode(keys(lake).await?.session)?, data))) }

/// A session for `user`: `ps_<payload>.<signature>`, checked by any node with the lake's key.
pub async fn session(lake: &Lake, user: &str) -> Result<(String, u64)> {
    let until = crate::log::now_ms() + session_ms();
    let payload = B64U.encode(serde_json::to_vec(&j!({"u": user, "x": until}))?);
    let sig = B64U.encode(hmac256(&B64.decode(keys(lake).await?.session)?, payload.as_bytes()));
    Ok((format!("ps_{payload}.{sig}"), until))
}

async fn session_user(lake: &Lake, token: &str) -> Option<String> {
    let (payload, sig) = token.strip_prefix("ps_")?.split_once('.')?;
    let key = B64.decode(keys(lake).await.ok()?.session).ok()?;
    hmac::verify(&hmac::Key::new(hmac::HMAC_SHA256, &key), payload.as_bytes(), &B64U.decode(sig).ok()?).ok()?;
    let v: Value = serde_json::from_slice(&B64U.decode(payload).ok()?).ok()?;
    (v["x"].as_u64()? > crate::log::now_ms()).then(|| v["u"].as_str().map(String::from))?
}

/// Passwords checked lately (by their hash with the user's name): a minute, so a client that sends
/// its password with every request (HTTP Basic) pays for SCRAM's hashing once.
static VERIFIED: LazyLock<Mutex<HashMap<String, std::time::Instant>>> = LazyLock::new(Default::default);

/// Is this `user`'s password? (Its verifier checked, or remembered as checked.)
pub async fn password_ok(lake: &Lake, user: &str, password: &str) -> bool {
    let key = sha256(&format!("{user}\0{password}"));
    if VERIFIED.lock().unwrap().get(&key).is_some_and(|t| t.elapsed().as_secs() < 60) {
        return true;
    }
    let Ok(Some(u)) = lake.cat.get::<User>(&user_key(user)).await else { return false };
    let ok = u.login && u.verifier.as_ref().is_some_and(|v| v.matches(password));
    if ok {
        let mut all = VERIFIED.lock().unwrap();
        if all.len() > 10_000 {
            all.clear();
        }
        all.insert(key, std::time::Instant::now());
    }
    ok
}

/// A user's verifier, for a Postgres client's SCRAM exchange.
pub async fn verifier(lake: &Lake, user: &str) -> Option<Verifier> { lake.cat.get::<User>(&user_key(user)).await.ok()?.filter(|u| u.login)?.verifier }

/// A user's token (`pt_…`): whose, if it is theirs and hasn't expired.
async fn token_user(lake: &Lake, token: &str) -> Option<String> {
    let user = String::from_utf8(B64U.decode(token.strip_prefix("pt_")?.split_once('_')?.0).ok()?).ok()?;
    let u = lake.cat.get::<User>(&user_key(&user)).await.ok()??;
    let hash = sha256(token);
    let now = crate::log::now_ms();
    (u.login && u.tokens.iter().any(|t| aws_lc_rs::constant_time::verify_slices_are_equal(t.hash.as_bytes(), hash.as_bytes()).is_ok() && t.expires_ms.is_none_or(|x| x > now))).then_some(user)
}

// ---------------------------------------------------------------- who is asking, and what they may do

/// What a user may do: every grant of its own, its roles' (and theirs') and public's.
#[derive(Default, Debug)]
pub struct Access {
    grants: Vec<Grant>,
    names: HashSet<String>, // (itself, its roles, public: whose grants they are)
    pub limits: Limits,     // (its own)
}

impl Access {
    /// The columns of `table` (as the catalog names it) it may use for `privilege`: None, none;
    /// Some(None), every column; Some(Some(these)).
    pub fn columns(&self, privilege: &str, table: &str) -> Option<Option<HashSet<String>>> {
        let schema = crate::ddl::split(table).0;
        let mut some: Option<HashSet<String>> = None;
        for g in self.grants.iter().filter(|g| g.privilege == privilege) {
            match &g.on {
                On::Lake => return Some(None),
                On::Schema(s) if s == schema => return Some(None),
                On::Table(t) if t == table && g.columns.is_empty() => return Some(None),
                On::Table(t) if t == table => some.get_or_insert_with(HashSet::new).extend(g.columns.iter().cloned()),
                _ => {}
            }
        }
        some.map(Some)
    }
    pub fn may(&self, privilege: &str, table: &str) -> bool { self.columns(privilege, table).is_some() }
    /// The same, for a table of another lake attached here (`l.t`, `l.s.t`): only grants naming
    /// it count; ON ALL TABLES and ON SCHEMA are this lake's.
    pub fn named(&self, privilege: &str, table: &str) -> Option<Option<HashSet<String>>> {
        let mut some: Option<HashSet<String>> = None;
        for g in self.grants.iter().filter(|g| g.privilege == privilege && g.on == On::Table(table.to_string())) {
            if g.columns.is_empty() {
                return Some(None);
            }
            some.get_or_insert_with(HashSet::new).extend(g.columns.iter().cloned());
        }
        some.map(Some)
    }
    pub fn secret(&self, name: &str) -> bool { self.grants.iter().any(|g| g.privilege == "usage" && g.on == On::Secret(name.to_string())) }
    /// The schemas this user may clone a branch of, or pin one for (ADR-058): None, no CLONE grant;
    /// Some(none), every schema (CLONE ON DATABASE); Some(these), CLONE ON SCHEMA.
    pub fn clones(&self) -> Option<Vec<String>> {
        let mut some: Option<Vec<String>> = None;
        for g in self.grants.iter().filter(|g| g.privilege == "clone") {
            match &g.on {
                On::Lake => return Some(vec![]),
                On::Schema(s) => some.get_or_insert_with(Vec::new).push(s.clone()),
                _ => {}
            }
        }
        some
    }
    /// Whether this user may deploy this database (GRANT DEPLOY ON DATABASE, ADR-058): `deploy::ask`.
    pub fn deploys(&self) -> bool { self.grants.iter().any(|g| g.privilege == "deploy" && g.on == On::Lake) }
    fn writes(&self) -> bool { self.grants.iter().any(|g| ["insert", "update", "delete"].contains(&g.privilege.as_str())) }
}

/// Users' access, by name, as of a catalog version (grants change seldom: a check is a lookup).
static ACCESS: LazyLock<Mutex<HashMap<String, (Option<u64>, std::time::Instant, Arc<Access>, bool)>>> = LazyLock::new(Default::default);

fn forget() {
    ACCESS.lock().unwrap().clear();
    ANY.lock().unwrap().clear();
}

/// The principal `user` signs in as: a superuser's rights are the admin token's; anyone else's
/// are its grants.
pub async fn principal(lake: &Lake, user: &str) -> Result<Principal> {
    let version = lake.cat.version();
    let cached = ACCESS.lock().unwrap().get(user).filter(|c| fresh(lake, version, c.0, c.1)).map(|c| (c.2.clone(), c.3));
    let (access, superuser) = match cached {
        Some(c) => c,
        None => {
            let me = lake.cat.get::<User>(&user_key(user)).await?.ok_or_else(|| anyhow!("no user {user}"))?;
            let mut grants = vec![];
            let mut seen = HashSet::new();
            let mut todo = vec![("public".to_string(), None), (user.to_string(), Some(me.clone()))];
            let mut superuser = me.superuser;
            while let Some((n, u)) = todo.pop() {
                if !seen.insert(n.clone()) || seen.len() > 64 {
                    continue;
                }
                let Some(u) = (match u {
                    Some(u) => Some(u),
                    None => lake.cat.get::<User>(&user_key(&n)).await?,
                }) else { continue };
                superuser |= u.superuser && n != "public";
                grants.extend(u.grants.iter().cloned());
                todo.extend(u.roles.iter().map(|r| (r.clone(), None)));
            }
            let access = Arc::new(Access { grants, names: seen, limits: me.limits });
            ACCESS.lock().unwrap().insert(user.to_string(), (version, std::time::Instant::now(), access.clone(), superuser));
            (access, superuser)
        }
    };
    let role = if superuser { Role::Admin } else if access.writes() { Role::Write } else { Role::Read };
    Ok(Principal { name: user.to_string(), role, access: (!superuser).then_some(access), door: "node", from: None, operator: false })
}

/// Seconds as said: `90 seconds`, `5 minutes`, `1 hour`.
fn span(s: u64) -> String {
    let (n, unit) = match s {
        _ if s >= 3600 && s % 3600 == 0 => (s / 3600, "hour"),
        _ if s >= 60 && s % 60 == 0 => (s / 60, "minute"),
        _ => (s, "second"),
    };
    format!("{n} {unit}{}", if n == 1 { "" } else { "s" })
}

/// The quota of the statement being run (its user's `Limits`): a turn among its statements on this
/// node, and how long it may take. Tokens and superusers have none.
pub struct Quota {
    turns: Option<(Arc<tokio::sync::Semaphore>, u32)>,
    pub timeout: Option<std::time::Duration>,
    pub user: String,
}

pub fn quota() -> Quota {
    let p = crate::auth::current();
    let Some((user, a)) = p.and_then(|p| Some((p.name, p.access?))) else { return Quota { turns: None, timeout: None, user: String::new() } };
    let env = |v: &str| std::env::var(v).ok().and_then(|n| n.parse::<u64>().ok());
    let most = a.limits.max_queries.map(u64::from).or_else(|| env("PONDRA_USER_QUERIES")).filter(|n| *n > 0).map(|n| n.min(10_000) as u32);
    let timeout = a.limits.timeout_secs.or_else(|| env("PONDRA_USER_TIMEOUT")).filter(|s| *s > 0).map(std::time::Duration::from_secs);
    static TURNS: LazyLock<Mutex<HashMap<String, (u32, Arc<tokio::sync::Semaphore>)>>> = LazyLock::new(Default::default);
    let turns = most.map(|n| {
        let mut all = TURNS.lock().unwrap();
        let e = all.entry(user.clone()).or_insert_with(|| (n, Arc::new(tokio::sync::Semaphore::new(n as usize))));
        if e.0 != n {
            *e = (n, Arc::new(tokio::sync::Semaphore::new(n as usize))); // (changed: the new number from now on)
        }
        (e.1.clone(), n)
    });
    Quota { turns, timeout, user }
}

impl Quota {
    /// Its turn: at once if fewer than its number run, else when one ends (30 s at most).
    pub async fn turn(&self) -> std::result::Result<Option<tokio::sync::OwnedSemaphorePermit>, String> {
        let Some((s, n)) = &self.turns else { return Ok(None) };
        match tokio::time::timeout(std::time::Duration::from_secs(30), s.clone().acquire_owned()).await {
            Ok(Ok(p)) => Ok(Some(p)),
            _ => Err(format!("quota: {} runs at most {n} statements at once (MAX_QUERIES); this one waited 30 s for its turn", self.user)),
        }
    }
}

/// Is what was worked out at catalog version `then` (at `at`) still so? On the leader, until the
/// catalog changes; elsewhere (a follower's version may not move with every commit it is sent), a
/// second at most.
fn fresh(lake: &Lake, now: Option<u64>, then: Option<u64>, at: std::time::Instant) -> bool {
    (lake.cat.is_writer() && now.is_some() && now == then) || at.elapsed() < std::time::Duration::from_secs(1)
}

/// May the request being served read or write the tables of `other`, a lake attached here as `ns`?
/// Refused, saying why, when that lake has a sign-in of its own (a user who signs in, or the node's
/// tokens) and the request isn't from whoever runs the nodes (`auth::operator`): a user signed in
/// here is nobody there, and a database nothing locks opens nothing of another (the lake server's
/// databases attach each other). A user granted some tables needs a grant naming the other lake's
/// table besides (`Access::named`).
pub async fn across(other: &Lake, ns: &str) -> Result<()> {
    if let Some(r) = crate::store::reach_of(&other.url).filter(|r| r.read_only) {
        return read_only(&r, ns);
    }
    if crate::auth::operator() || !(crate::auth::tokens_on() || any(other).await) {
        return Ok(());
    }
    bail!("permission denied: {ns} signs in on its own: its tables are read and written through it (the database {ns}), or with one of the nodes' tokens")
}

/// A lake on another server, attached READ_ONLY (ADR-058): its users sign in there, and its bucket
/// key reads all of it whoever asks, so here it is read by whoever may use that key's secret (a
/// user granted USAGE on it), and by tokens, as files under a secret's scope are.
fn read_only(r: &crate::store::Reach, ns: &str) -> Result<()> {
    let Some(a) = crate::auth::limited() else { return Ok(()) };
    match &r.secret {
        Some(s) if a.secret(s) => Ok(()),
        Some(s) => bail!("permission denied: {ns} is read with the secret {s}: GRANT USAGE ON SECRET {s} TO …"),
        None => bail!("permission denied: {ns} is read with this server's own key: a token's, or an admin's"),
    }
}

/// The same for a write to `table` of that lake (its name there), and, for a user granted some
/// tables, a grant naming it for each privilege the write needs.
pub async fn across_write(other: &Lake, ns: &str, table: &str, stmt: &crate::write::Stmt) -> Result<()> {
    across(other, ns).await?;
    let Some(a) = crate::auth::limited() else { return Ok(()) };
    let (table, privileges) = (format!("{ns}.{table}"), crate::auth::needs(stmt).1);
    match privileges.iter().find(|p| a.named(p, &table).is_none()) {
        Some(p) => bail!("permission denied: {} on {table}, another database's table: GRANT {} ON {table} TO … (ON ALL TABLES and ON SCHEMA are this database's)", p.to_uppercase(), p.to_uppercase()),
        None => Ok(()),
    }
}

static ANY: LazyLock<Mutex<HashMap<String, (Option<u64>, std::time::Instant, bool)>>> = LazyLock::new(Default::default);

/// Does the lake have a user who signs in (a password or a token)? Then a request with nothing
/// signed is refused, as with a token set.
pub async fn any(lake: &Lake) -> bool {
    let version = lake.cat.version();
    if let Some(c) = ANY.lock().unwrap().get(&lake.url).filter(|c| fresh(lake, version, c.0, c.1)) {
        return c.2;
    }
    let yes = lake.cat.scan::<User>("u/", "u0").await.map(|all| all.iter().any(|(_, u)| u.login && (u.verifier.is_some() || !u.tokens.is_empty()))).unwrap_or(false);
    ANY.lock().unwrap().insert(lake.url.clone(), (version, std::time::Instant::now(), yes));
    yes
}

/// Who an HTTP request's `Authorization` is: a token's role, a user's token or session, or a user's
/// name and password (Basic). None: nobody (refused where a sign-in is needed).
pub async fn who(lake: &Lake, auth: &crate::auth::Auth, header: Option<&str>) -> Option<Principal> {
    let header = header?;
    if let Some(token) = header.strip_prefix("Bearer ") {
        if let Some(p) = auth.lent_principal(token) {
            return Some(p);
        }
        let role = auth.token_role(token);
        if role > Role::None {
            return Some(Principal::token(role));
        }
        if token.starts_with("pn_") && keys(lake).await.is_ok_and(|k| k.node == token) {
            return Some(Principal::token(Role::Admin));
        }
        let user = if token.starts_with("ps_") { session_user(lake, token).await? } else { token_user(lake, token).await? };
        return principal(lake, &user).await.ok();
    }
    let basic = String::from_utf8(B64.decode(header.strip_prefix("Basic ")?).ok()?).ok()?;
    let (user, password) = basic.split_once(':')?;
    sign_in(lake, auth, user, password).await
}

/// A name and a password (or a token as the password): the Postgres, Kafka and Flight doors' and
/// HTTP Basic's. The tokens' names (`admin`, `writer`, `reader`) take their tokens.
pub async fn sign_in(lake: &Lake, auth: &crate::auth::Auth, user: &str, password: &str) -> Option<Principal> {
    if BUILT_IN.contains(&user) {
        let role = auth.token_role(password);
        return (role > Role::None && auth.token_for(user).as_deref() == Some(password)).then(|| Principal::token(role));
    }
    let ok = match password.starts_with("pt_") {
        true => token_user(lake, password).await.as_deref() == Some(user),
        false => password_ok(lake, user, password).await,
    };
    match ok {
        true => principal(lake, user).await.ok(),
        false => None,
    }
}

/// `SHOW USERS`, `SHOW GRANTS [TO name]`: what `pondra.users` and `pondra.grants` hold.
pub async fn tables(lake: &Lake) -> Result<Vec<(&'static str, Arc<dyn datafusion::catalog::TableProvider>)>> {
    use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray};
    use datafusion::datasource::MemTable;
    let mut all = lake.cat.scan::<User>("u/", "u0").await?;
    if let Some(a) = crate::auth::limited() {
        all.retain(|(k, _)| a.names.contains(&k[2..])); // (a user's: itself and its roles)
    }
    let s = |f: &dyn Fn(&String, &User) -> Option<String>| Arc::new(all.iter().map(|(k, u)| f(&k[2..].to_string(), u)).collect::<StringArray>()) as ArrayRef;
    let users = RecordBatch::try_from_iter(vec![
        ("name", s(&|n, _| Some(n.clone()))),
        ("kind", s(&|n, u| Some(if n == "public" { "every user" } else if u.login { "user" } else { "role" }.into()))),
        ("superuser", Arc::new(all.iter().map(|(_, u)| Some(u.superuser)).collect::<BooleanArray>()) as ArrayRef),
        ("password", Arc::new(all.iter().map(|(_, u)| Some(u.verifier.is_some())).collect::<BooleanArray>()) as ArrayRef),
        ("tokens", s(&|_, u| Some(u.tokens.iter().map(|t| t.name.clone()).collect::<Vec<_>>().join(", ")).filter(|t| !t.is_empty()))),
        ("member_of", s(&|_, u| Some(u.roles.join(", ")).filter(|r| !r.is_empty()))),
        ("max_queries", Arc::new(all.iter().map(|(_, u)| u.limits.max_queries.map(|n| n as i64)).collect::<datafusion::arrow::array::Int64Array>()) as ArrayRef),
        ("statement_timeout", s(&|_, u| u.limits.timeout_secs.map(span))),
        ("created", Arc::new(all.iter().map(|(_, u)| (u.created_ms > 0).then_some(u.created_ms as i64 * 1000)).collect::<TimestampMicrosecondArray>().with_timezone("UTC")) as ArrayRef),
    ])?;
    let rows: Vec<(String, &Grant)> = all.iter().flat_map(|(k, u)| u.grants.iter().map(move |g| (k[2..].to_string(), g))).collect();
    let here = crate::ddl::lake_name(lake); // (CLONE ON DATABASE is this database, not every table)
    let g = |f: &dyn Fn(&String, &Grant) -> Option<String>| Arc::new(rows.iter().map(|(n, g)| f(n, g)).collect::<StringArray>()) as ArrayRef;
    let grants = RecordBatch::try_from_iter(vec![
        ("grantee", g(&|n, _| Some(n.clone()))),
        ("privilege", g(&|_, g| Some(g.privilege.to_uppercase()))),
        ("on_kind", g(&|_, g| Some(match &g.on { On::Table(_) => "table", On::Schema(_) => "schema", On::Lake if g.of_database() => "database", On::Lake => "lake", On::Secret(_) => "secret" }.into()))),
        ("on_name", g(&|_, g| match &g.on { On::Table(n) | On::Schema(n) | On::Secret(n) => Some(n.clone()), On::Lake if g.of_database() => Some(here.clone()), On::Lake => None })),
        ("columns", g(&|_, g| Some(g.columns.join(", ")).filter(|c| !c.is_empty()))),
    ])?;
    let mem = |b: RecordBatch| -> Result<Arc<dyn datafusion::catalog::TableProvider>> { Ok(Arc::new(MemTable::try_new(b.schema(), vec![vec![b]])?)) };
    let mut out = vec![("users", mem(users)?), ("grants", mem(grants)?)];
    out.extend(crate::shares::tables(lake).await?); // (`pondra.shares`, `pondra.recipients`)
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scram_round_trip() {
        // RFC 7677's example: user, pencil, its nonces and salt.
        let v = Verifier::with("pencil", &B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap(), 4096);
        let (s, first) = Scram::first(v.clone(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO").unwrap();
        assert!(first.starts_with("r=rOprNGfwEbeRWgbNEkqO") && first.ends_with(",s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"));
        // (a client's proof, computed as a client would, for the nonce this server picked)
        let nonce = first.split(',').next().unwrap().strip_prefix("r=").unwrap();
        let without = format!("c=biws,r={nonce}");
        let auth = format!("n=user,r=rOprNGfwEbeRWgbNEkqO,{first},{without}");
        let salted = salted("pencil", &B64.decode(&v.salt).unwrap(), 4096);
        let client_key = hmac256(&salted, b"Client Key");
        let stored = digest::digest(&digest::SHA256, &client_key);
        let sig = hmac256(stored.as_ref(), auth.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(&sig).map(|(a, b)| a ^ b).collect();
        let last = s.last(&format!("{without},p={}", B64.encode(proof))).unwrap();
        assert_eq!(last, format!("v={}", B64.encode(hmac256(&hmac256(&salted, b"Server Key"), auth.as_bytes()))));
        assert!(s.last(&format!("{without},p={}", B64.encode([0u8; 32]))).is_err());
        assert!(v.matches("pencil") && !v.matches("pencil2"));
    }

    #[test]
    fn statements() {
        let p = |s: &str| match statement(s) {
            Some(crate::write::Stmt::Ddl(d)) => match &d[0] {
                crate::ddl::Ddl::Users(c) => serde_json::to_value(c).unwrap(),
                _ => panic!(),
            },
            Some(crate::write::Stmt::Invalid(e)) => j!({"error": e}),
            _ => j!(null),
        };
        assert_eq!(p("CREATE USER ann PASSWORD 'longenough'")["do"], "create");
        assert_eq!(p("create role analyst")["login"], false);
        assert!(p("CREATE USER ann PASSWORD 'short'")["error"].as_str().unwrap().contains("8 characters"));
        assert!(p("CREATE USER admin")["error"].as_str().unwrap().contains("token"));
        let g = p("GRANT SELECT (id, name) ON TABLE sales.orders TO analyst, bob");
        assert_eq!((g["on"]["name"].as_str(), g["columns"].as_array().unwrap().len(), g["to"].as_array().unwrap().len()), (Some("sales.orders"), 2, 2));
        assert_eq!(p("GRANT ALL ON ALL TABLES IN SCHEMA sales TO analyst")["privileges"].as_array().unwrap().len(), 4);
        assert_eq!(p("GRANT USAGE ON SECRET s3_sales TO analyst")["on"]["kind"], "secret");
        assert!(p("GRANT USAGE ON TABLE t TO analyst")["error"].is_string());
        assert_eq!(p("GRANT analyst TO ann")["do"], "grant_role");
        assert_eq!(p("REVOKE analyst FROM ann")["do"], "revoke_role");
        assert_eq!(p("REVOKE INSERT ON t FROM analyst")["do"], "revoke");
        assert_eq!(p("CREATE TOKEN ci FOR USER ann EXPIRES IN '30 days'")["expires_secs"], 30 * 86400);
        assert_eq!(p("DROP TOKEN ci FOR ann")["do"], "drop_token");
        assert_eq!(p("DROP USER IF EXISTS ann")["if_exists"], true);
        assert_eq!(p("CREATE TABLE t (a INT)"), j!(null));
        assert_eq!(p("CREATE SECRET s (TYPE s3)"), j!(null));
    }
}
