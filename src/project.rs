//! A project on the command line (ADR-047 §4, §6): `pondra init`, `login`, `branch`, `plan`,
//! `switch`, `apply`, `test`, `export`, `diff` and `ci init`. A branch is a Git branch and a database of the same
//! name, cloned from prod with no file copied. The folder is read here and sent to the environment's
//! node (`POST /apply`), which plans and applies it (`apply.rs`). An environment is a database, found by
//! `--url` or `--lake`, else by pondra.toml: `[env.prod] url = …` (or `lake = …`), or the server the
//! project (or the environment) names and the database's name (`/db/prod`, `pondra serve --lakes`).
//! A token comes from `--token`, `PONDRA_TOKEN`, or `pondra login`'s (`~/.pondra/login.json`).

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(clap::Subcommand)]
pub enum Command {
    /// Start a project in DIR (default: this folder): pondra.toml, objects/, migrations/ and tests/.
    /// `pondra export` writes one from a database instead (pondra init shop).
    Init {
        dir: Option<String>,
        /// Its name (default: the folder's).
        #[arg(long)]
        name: Option<String>,
    },
    /// Keep a token for each server, for every command after. With no URL: the servers pondra.toml
    /// names (pondra login https://pondra.acme.com).
    Login {
        url: Option<String>,
        /// The project whose pondra.toml names the servers.
        #[arg(long, default_value = ".")]
        project: String,
        /// The token (asked for when neither this nor --user is given).
        #[arg(long)]
        token: Option<String>,
        /// Sign in as a user instead: its password is asked for (or PONDRA_PASSWORD).
        #[arg(long)]
        user: Option<String>,
    },
    /// Start a branch: a Git branch and a database of the same name, prod's data with no file
    /// copied (pondra branch add-discounts). With no name, the branches; -d NAME drops one.
    Branch {
        /// Its name: a Git branch and a database of the same name.
        name: Option<String>,
        /// The database it starts as (default: [env.dev]'s clone, else prod).
        #[arg(long)]
        from: Option<String>,
        /// Drop its database, and its Git branch when git lets it go.
        #[arg(short = 'd', long = "delete", value_name = "NAME", conflicts_with_all = ["name", "from"])]
        delete: Option<String>,
        #[command(flatten)]
        at: Where,
    },
    /// Go to a branch that exists, as `git switch` does, and make its database if it isn't there (pondra switch add-discounts).
    Switch {
        /// The branch to go to.
        name: String,
        #[command(flatten)]
        at: Where,
    },
    /// What an apply would change in an environment, and why anything is refused (pondra plan test).
    Plan {
        /// The environment or branch (default: the git branch's).
        #[arg(value_name = "ENV", conflicts_with = "env")]
        target: Option<String>,
        #[command(flatten)]
        at: Where,
        /// Drop the objects the project no longer declares.
        #[arg(long)]
        prune: bool,
    },
    /// Make the project true in an environment: its plan, each step, then its tests (pondra apply test).
    Apply {
        /// The environment or branch (default: the git branch's).
        #[arg(value_name = "ENV", conflicts_with = "env")]
        target: Option<String>,
        #[command(flatten)]
        at: Where,
        /// Apply again on every save of the project's files (Ctrl-C stops)
        #[arg(long)]
        watch: bool,
        /// Make the database again from the one it is cloned from first (a branch, or [env.test])
        #[arg(long)]
        fresh: bool,
        /// Skip tests/ (they run after every apply otherwise)
        #[arg(long)]
        no_test: bool,
        /// Drop the objects the project no longer declares.
        #[arg(long)]
        prune: bool,
        /// Stop after this many applies (for the tests; with --watch).
        #[arg(long, hide = true)]
        applies: Option<usize>,
    },
    /// Run the project's tests (tests/*.sql: any row back is a failure) in an environment (pondra test test).
    Test {
        /// The environment or branch (default: the git branch's).
        #[arg(value_name = "ENV", conflicts_with = "env")]
        target: Option<String>,
        #[command(flatten)]
        at: Where,
    },
    /// An environment's objects as a project's files, in DIR (default: this folder) (pondra export copy --env prod).
    Export {
        dir: Option<String>,
        #[command(flatten)]
        at: Where,
    },
    /// What an environment changes against another: the objects, and the rows of each table both have
    /// (`pondra.diff`). Written as Markdown, for a pull request's summary (pondra diff pr-12).
    Diff {
        /// The environment or branch (default: the git branch's).
        #[arg(value_name = "ENV", conflicts_with = "env")]
        target: Option<String>,
        #[command(flatten)]
        at: Where,
        /// The database it is compared with (default: its clone, else prod).
        #[arg(long)]
        against: Option<String>,
    },
    /// Continuous integration: `pondra ci init` writes a GitHub Actions workflow for the project (pondra ci init).
    Ci {
        #[command(subcommand)]
        cmd: Ci,
    },
}

#[derive(clap::Subcommand)]
pub enum Ci {
    /// Write .github/workflows/pondra.yml for the project (--force writes it again) (pondra ci init).
    Init {
        #[arg(long, default_value = ".")]
        project: String,
        #[arg(long)]
        force: bool,
    },
}

#[derive(clap::Args, Clone)]
pub struct Where {
    /// The environment or branch (default: PONDRA_ENV, else the git branch's).
    #[arg(long, short)]
    env: Option<String>,
    /// The project's folder.
    #[arg(long, default_value = ".")]
    project: String,
    /// A node to use instead (http://host:8080).
    #[arg(long)]
    url: Option<String>,
    /// A lake to start a node for instead (a folder or s3://bucket/prefix).
    #[arg(long, conflicts_with = "url")]
    lake: Option<String>,
    /// A token for it (also PONDRA_TOKEN).
    #[arg(long)]
    token: Option<String>,
}

impl Where {
    /// The same place, for the environment or branch a command's positional names.
    fn with(self, target: Option<String>) -> Where {
        match target {
            Some(env) => Where { env: Some(env), ..self },
            None => self,
        }
    }
}

#[derive(Deserialize, Default)]
struct Toml {
    #[serde(default)]
    project: Section,
    #[serde(default)]
    env: BTreeMap<String, Env>,
    #[serde(default)]
    secrets: BTreeMap<String, String>, // a `$name` → `env:VARIABLE`, read on the applying machine
}

#[derive(Deserialize, Default)]
struct Section {
    #[serde(default)]
    server: Option<String>, // the server the project's databases are on, unless an environment says
}

#[derive(Deserialize, Default, Clone)]
struct Env {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    lake: Option<String>,
    #[serde(default)]
    server: Option<String>, // the server this environment's database is on (ADR-058)
    #[serde(default, alias = "base")]
    clone: Option<String>, // made from this database when it isn't there (and by --fresh)
    #[serde(default)]
    protected: bool, // its objects change only by an apply: `pondra apply --watch` never applies here
}

fn read_toml(dir: &Path) -> Result<Toml> {
    match std::fs::read_to_string(dir.join("pondra.toml")) {
        Ok(t) => toml::from_str(&t).context("pondra.toml"),
        Err(_) => Ok(Toml::default()),
    }
}

/// A database's name as SQL takes it: `pr-123` and `feature/x` become `pr_123` and `feature_x`.
fn database(name: &str) -> String {
    let n: String = name.to_lowercase().chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    if n.starts_with(|c: char| c.is_ascii_digit()) { format!("_{n}") } else { n }
}

fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").arg("-C").arg(dir).args(args).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string()).filter(|s| !s.is_empty())
}

/// Runs git in `dir`, and bails with git's own words when it refuses.
fn git_do(dir: &Path, args: &[&str]) -> Result<()> {
    let out = std::process::Command::new("git").arg("-C").arg(dir).args(args).output().context("git isn't installed")?;
    ensure!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr).trim());
    Ok(())
}

/// Whether git in `dir` has a branch of this name.
fn git_has_branch(dir: &Path, name: &str) -> bool {
    let reference = format!("refs/heads/{name}");
    git(dir, &["rev-parse", "--verify", "--quiet", reference.as_str()]).is_some()
}

/// The commit an apply is of: git's short id, `-dirty` when files changed since.
fn commit(dir: &Path) -> Option<String> {
    let id = git(dir, &["rev-parse", "--short", "HEAD"])?;
    Some(if git(dir, &["status", "--porcelain"]).is_some() { format!("{id}-dirty") } else { id })
}

/// The first line of an error, as the user reads it.
fn first_line(e: &anyhow::Error) -> String {
    format!("{e:#}").lines().next().unwrap_or_default().to_string()
}

// ---------------------------------------------------------------- environments and branches

/// Is `env` a branch: a name pondra.toml doesn't declare, or `dev` (whose section is every branch's).
fn is_branch(toml: &Toml, env: &str) -> bool {
    env == "dev" || !toml.env.contains_key(env)
}

/// The pondra.toml section that describes an environment's database: its own, or [env.dev] for a branch.
fn section_of<'a>(toml: &'a Toml, env: &str) -> Option<&'a Env> {
    toml.env.get(if is_branch(toml, env) { "dev" } else { env })
}

/// The database `env` is made from when it isn't there: its `clone`; a branch's is [env.dev]'s clone,
/// else prod. None for a database of its own (prod), which an apply makes empty on its server.
fn clone_of(toml: &Toml, env: &str) -> Option<String> {
    let src = match is_branch(toml, env) {
        true => toml.env.get("dev").and_then(|e| e.clone.clone()).unwrap_or_else(|| "prod".into()),
        false => toml.env.get(env)?.clone.clone()?,
    };
    // (a database is not a clone of itself: an undeclared prod is its own database)
    (database(env) != database(&src)).then_some(src)
}

/// The server `env`'s database is on, without a trailing slash: its own `server` (a branch's is
/// [env.dev]'s), else the project's.
fn server_of(toml: &Toml, env: &str) -> Option<String> {
    section_of(toml, env).and_then(|e| e.server.clone()).or_else(|| toml.project.server.clone()).map(|s| s.trim_end_matches('/').to_string())
}

/// Is `db` the database of an environment pondra.toml declares? dev is every branch's section, not one.
fn declared(toml: &Toml, db: &str) -> bool {
    toml.env.keys().any(|e| e.as_str() != "dev" && database(e) == db)
}

/// The database a branch named `name` is. Refused when the name is no branch's: git's main lines, prod,
/// and the environments pondra.toml declares.
fn branch_db(toml: &Toml, name: &str) -> Result<String> {
    let db = database(name);
    ensure!(!matches!(db.as_str(), "main" | "master"), "{name} is git's main line, not a branch: git switch -c a-branch, then pondra branch a-branch");
    ensure!(db != "prod" && db != LOCAL && !declared(toml, &db), "{name} is an environment in pondra.toml, not a branch: pondra apply {name}");
    Ok(db)
}

/// Environments a save never applies to: prod (and git's main lines), and any pondra.toml protects.
fn unwatched(toml: &Toml, env: &str) -> bool {
    matches!(env, "prod" | "main" | "master") || toml.env.get(env).is_some_and(|e| e.protected)
}

/// The environment meant: `--env` or the positional, `PONDRA_ENV`, else the git branch's database. With a
/// `--url` or `--lake` it is pondra.toml's first environment (or dev).
fn env_of(at: &Where, toml: &Toml, dir: &Path) -> Result<String> {
    if let Some(e) = at.env.clone().or_else(|| std::env::var("PONDRA_ENV").ok()) {
        return Ok(e);
    }
    if at.url.is_some() || at.lake.is_some() {
        return Ok(toml.env.keys().next().cloned().unwrap_or_else(|| "dev".into()));
    }
    match git(dir, &["rev-parse", "--abbrev-ref", "HEAD"]).as_deref() {
        Some(b @ ("main" | "master")) => bail!("on {b}: name the environment (pondra apply test, pondra apply prod), or start a branch: pondra branch NAME"),
        Some(b) if b != "HEAD" => Ok(database(b)),
        _ => only_env(toml),
    }
}

/// pondra.toml's one environment, when it declares exactly one.
fn only_env(toml: &Toml) -> Result<String> {
    match &toml.env.keys().collect::<Vec<_>>()[..] {
        [one] => Ok(one.to_string()),
        _ => bail!("which environment? pondra apply prod (or PONDRA_ENV)"),
    }
}

// ---------------------------------------------------------------- nodes

/// A node to talk to, and the node this command started for a lake, if it did.
struct Node {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
    owner: String,
    child: Option<std::process::Child>,
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(c) = &mut self.child {
            crate::shell::stop(c);
        }
    }
}

impl Node {
    async fn send(&self, r: reqwest::RequestBuilder) -> Result<Value> {
        let r = match &self.token {
            Some(t) => r.bearer_auth(t),
            None => r,
        };
        let r = r.header("x-pondra-owner", &self.owner).send().await.with_context(|| format!("{} doesn't answer", self.base))?;
        let ok = r.status().is_success();
        let text = r.text().await?;
        ensure!(ok, "{}", text.trim());
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    async fn post(&self, rel: &str, body: Value) -> Result<Value> { self.send(self.http.post(format!("{}{rel}", self.base)).json(&body)).await }

    async fn get(&self, rel: &str) -> Result<Value> { self.send(self.http.get(format!("{}{rel}", self.base))).await }

    async fn sql(&self, sql: &str) -> Result<Value> { self.post("/sql", json!({"sql": sql})).await }
}

/// This machine's own database: the lake in the project's `lake/` folder, which `pondra` opens as its shell
/// there, unless `[env.local]` gives a lake, url or server. No server: `pondra apply local --watch` on a laptop.
const LOCAL: &str = "local";

/// The lake an environment's node is started for: `--lake`, or pondra.toml's `lake` unless a `--url` is given
/// (for `local`, the project's `lake/`).
fn lake_of(at: &Where, toml: &Toml, env: &str) -> Option<String> {
    let local = || (env == LOCAL && toml.env.get(LOCAL).is_none_or(|e| e.url.is_none() && e.server.is_none())).then(|| Path::new(&at.project).join("lake").to_string_lossy().into_owned());
    at.lake.clone().or_else(|| toml.env.get(env).and_then(|e| e.lake.clone()).or_else(local).filter(|_| at.url.is_none()))
}

/// Where an environment's node is, started or not: its url, else its database on its server. None for a
/// lake (its node is started for the command) and for an environment given nowhere.
fn address(at: &Where, toml: &Toml, env: &str) -> Option<String> {
    if lake_of(at, toml, env).is_some() {
        return None;
    }
    match at.url.clone().or_else(|| toml.env.get(env).and_then(|e| e.url.clone())) {
        Some(u) => Some(u.trim_end_matches('/').to_string()),
        None => server_of(toml, env).map(|s| format!("{s}/db/{}", database(env))),
    }
}

/// Whether pondra.toml gives an environment an address or a lake (its own `url` or `lake`): its database is
/// not on the project's server, and pondra makes nothing there.
fn addressed(toml: &Toml, env: &str) -> bool {
    toml.env.get(env).is_some_and(|e| e.url.is_some() || e.lake.is_some())
}

/// Whether an environment is given by an address or a lake (`--url`, `--lake`, or pondra.toml's own `url` or
/// `lake`). pondra makes no database there.
fn given(at: &Where, toml: &Toml, env: &str) -> bool {
    at.url.is_some() || lake_of(at, toml, env).is_some() || addressed(toml, env)
}

/// The token for a node at `base`: `--token`, `PONDRA_TOKEN`, else the one `pondra login` kept for that server.
fn token_for(at: &Where, base: &str) -> Option<String> {
    at.token.clone().or_else(|| std::env::var("PONDRA_TOKEN").ok()).or_else(|| saved(base))
}

/// A node at `base`, as it is. Nothing is started.
fn node_at(at: &Where, base: String) -> Node {
    let token = token_for(at, &base);
    Node { http: reqwest::Client::new(), base, token, owner: String::new(), child: None }
}

/// The node an environment is served by. A lake's node is started for the command and stopped when it ends.
async fn node(at: &Where, toml: &Toml, env: &str) -> Result<Node> {
    if let Some(lake) = lake_of(at, toml, env) {
        let (mut child, base, owner, log) = crate::shell::start(&lake)?;
        let http = crate::shell::up(&base, &mut child, &log).await?;
        return Ok(Node { http, base, token: at.token.clone().or_else(|| std::env::var("PONDRA_TOKEN").ok()), owner, child: Some(child) });
    }
    match address(at, toml, env) {
        Some(base) => Ok(node_at(at, base)),
        None => bail!("where is {env}? [env.{env}] url = \"https://…\" or [project] server = \"https://…\" in pondra.toml, or --url"),
    }
}

// ---------------------------------------------------------------- making databases

/// The node that makes `env`'s database as a clone of `src`. On src's own server it is src's node; when
/// env is on another server, it is that server's root, where src is attached (ADR-058). An address the
/// user gave is where they said.
async fn maker(at: &Where, toml: &Toml, env: &str, src: &str) -> Result<Node> {
    let typed = at.url.is_some() || at.lake.is_some();
    match (server_of(toml, env), server_of(toml, src)) {
        (Some(s), other) if !typed && other.as_deref() != Some(s.as_str()) => Ok(node_at(at, s)),
        _ => node(at, toml, src).await,
    }
}

/// `CREATE DATABASE IF NOT EXISTS env CLONE src`, sent to the node that makes it (`fresh`: dropped first).
/// The answer has an `attached` key when the database was there already.
async fn clone_db(at: &Where, toml: &Toml, env: &str, src: &str, fresh: bool) -> Result<Value> {
    let m = maker(at, toml, env, src).await?;
    let (db, from) = (database(env), database(src));
    if fresh {
        m.sql(&format!("DROP DATABASE IF EXISTS {db}")).await?;
    }
    m.sql(&format!("CREATE DATABASE IF NOT EXISTS {db} CLONE {from}")).await
}

/// Makes `env`'s database when it is a clone and isn't there (again first, with `fresh`). Returns the line
/// to print when it made one. A database of its own is made on its server when a command first asks for it
/// (`ask_node`).
async fn made(at: &Where, toml: &Toml, env: &str, fresh: bool) -> Result<Option<String>> {
    if given(at, toml, env) {
        ensure!(!fresh, "--fresh needs a server: {env} is given by url or lake");
        return Ok(None);
    }
    let Some(src) = clone_of(toml, env) else {
        ensure!(!fresh, "{env} isn't a clone: --fresh makes clones again ([env.{env}] clone = \"prod\")");
        return Ok(None);
    };
    let answer = clone_db(at, toml, env, &src, fresh).await?;
    if answer.get("attached").is_some() && !fresh {
        return Ok(None);
    }
    Ok(Some(format!("{env}: made from {src}, no file copied\n")))
}

/// The server a database of its own is made on, when a command finds it missing. None for a url, a lake or a clone.
fn own_server(at: &Where, toml: &Toml, env: &str) -> Option<String> {
    if given(at, toml, env) || clone_of(toml, env).is_some() {
        return None;
    }
    server_of(toml, env)
}

const MISSING: &str = "does not exist (CREATE DATABASE";

/// Asks an environment's node `rel` (a plan, an apply, the tests). A database of its own that isn't there yet
/// is made on its server, and the request asked again; the line returned says so.
async fn ask_node(at: &Where, toml: &Toml, env: &str, node: &Node, rel: &str, body: Value) -> Result<(Value, String)> {
    match node.post(rel, body.clone()).await {
        Err(e) if format!("{e:#}").contains(MISSING) => {
            let Some(server) = own_server(at, toml, env) else { return Err(e) };
            node_at(at, server.clone()).post("/databases", json!({"name": database(env)})).await?;
            let line = format!("{env}: a new database on {server}\n");
            Ok((node.post(rel, body).await?, line))
        }
        r => Ok((r?, String::new())),
    }
}

/// Whether a database is protected (its `pondra.databases` row says so). A database not there yet isn't.
async fn is_protected_db(node: &Node, env: &str) -> Result<bool> {
    match node.sql(&format!("SELECT protected FROM pondra.databases WHERE name = '{}'", database(env))).await {
        Ok(rows) => Ok(rows[0]["protected"] == true),
        Err(e) if format!("{e:#}").contains("does not exist") => Ok(false),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------- login

fn home() -> Result<PathBuf> {
    if let Some(h) = std::env::var_os("PONDRA_HOME") {
        return Ok(PathBuf::from(h));
    }
    let h = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).context("no home folder: set PONDRA_HOME")?;
    Ok(PathBuf::from(h).join(".pondra"))
}

fn logins() -> BTreeMap<String, String> {
    home().ok().and_then(|h| std::fs::read(h.join("login.json")).ok()).and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default()
}

/// The token kept for the server that `url` is on (the longest address it starts with).
fn saved(url: &str) -> Option<String> {
    logins().into_iter().filter(|(s, _)| url == s || url.starts_with(&format!("{s}/"))).max_by_key(|(s, _)| s.len()).map(|(_, t)| t)
}

fn ask(prompt: &str, hidden: bool) -> Result<String> {
    use std::io::{IsTerminal, Write};
    eprint!("{prompt}");
    std::io::stderr().flush()?;
    let quiet = hidden && cfg!(unix) && std::io::stdin().is_terminal();
    if quiet {
        let _ = std::process::Command::new("stty").arg("-echo").stdin(std::process::Stdio::inherit()).status();
    }
    let mut line = String::new();
    let read = std::io::stdin().read_line(&mut line);
    if quiet {
        let _ = std::process::Command::new("stty").arg("echo").stdin(std::process::Stdio::inherit()).status();
        eprintln!();
    }
    read?;
    Ok(line.trim().to_string())
}

/// The servers pondra.toml names, each once, in the order `pondra login` signs in to them: the project's
/// server, each environment's server, then each environment's url.
fn servers(toml: &Toml) -> Vec<String> {
    let names = toml.project.server.iter().chain(toml.env.values().filter_map(|e| e.server.as_ref())).chain(toml.env.values().filter_map(|e| e.url.as_ref()));
    let mut out: Vec<String> = vec![];
    for name in names {
        let base = name.trim_end_matches('/').to_string();
        if !out.contains(&base) {
            out.push(base);
        }
    }
    out
}

/// The databases a server's sign-in may answer at, when its root can't: the environments on it that are
/// databases of their own first, then the clones; prod when pondra.toml names none there. An environment
/// given by its own url or lake isn't on the server, so it is left out.
fn sign_in_dbs(toml: &Toml, base: &str) -> Vec<String> {
    let on_server = |e: &&String| e.as_str() != "dev" && !addressed(toml, e) && server_of(toml, e).as_deref() == Some(base);
    let mut envs: Vec<&String> = toml.env.keys().filter(on_server).collect();
    envs.sort_by_key(|e| clone_of(toml, e).is_some());
    let mut dbs: Vec<String> = envs.into_iter().map(|e| database(e)).collect();
    if dbs.is_empty() {
        dbs.push("prod".into());
    }
    dbs
}

/// `pondra login [URL]`: a token kept for one server, or for each server pondra.toml names.
async fn login(url: Option<String>, project: &str, token: Option<String>, user: Option<String>) -> Result<String> {
    let toml = read_toml(Path::new(project))?;
    let bases = match url {
        Some(u) => vec![u.trim_end_matches('/').to_string()],
        None => {
            let names = servers(&toml);
            ensure!(!names.is_empty(), "which server? pondra login https://pondra.acme.com");
            names
        }
    };
    let mut said = vec![];
    for base in bases {
        let dbs = sign_in_dbs(&toml, &base);
        said.push(sign_in(&base, &dbs, token.clone(), user.clone()).await.with_context(|| format!("signing in to {base}"))?);
    }
    Ok(said.join("\n"))
}

/// Signs in to the server at `base` and keeps its token under `base`, so every database there finds it. A
/// server with several databases and no default answers 404 at its root: the sign-in then goes to the first of
/// `dbs` that answers.
async fn sign_in(base: &str, dbs: &[String], token: Option<String>, user: Option<String>) -> Result<String> {
    let http = reqwest::Client::new();
    let api = answering(&http, base, dbs).await?;
    let token = match (token, user) {
        (Some(t), _) => t,
        (None, Some(u)) => password_token(&http, &api, base, &u).await?,
        (None, None) => ask(&format!("token for {base}: "), true)?,
    };
    let r = http.get(format!("{api}/whoami")).bearer_auth(&token).send().await.with_context(|| format!("{base} doesn't answer"))?;
    ensure!(r.status().is_success(), "{}", r.text().await?.trim());
    let me: Value = r.json().await?;
    ensure!(me["role"] != "none", "{base} doesn't take that token");
    keep_login(base, token)?;
    let who = me["user"].as_str().filter(|u| !u.is_empty()).map(|u| format!("{u}, ")).unwrap_or_default();
    Ok(format!("signed in to {base} ({who}{})", me["role"].as_str().unwrap_or_default()))
}

/// The token a user's password gets at `api`: asked for (or PONDRA_PASSWORD), then `POST /login`.
async fn password_token(http: &reqwest::Client, api: &str, base: &str, user: &str) -> Result<String> {
    let password = match std::env::var("PONDRA_PASSWORD") {
        Ok(p) => p,
        Err(_) => ask(&format!("{user}'s password for {base}: "), true)?,
    };
    let r = http.post(format!("{api}/login")).json(&json!({"user": user, "password": password})).send().await?;
    ensure!(r.status().is_success(), "{}", r.text().await?.trim());
    Ok(r.json::<Value>().await?["token"].as_str().context("no token")?.to_string())
}

/// The address sign-in answers at: the server's root, or, when the root is a 404, the first of `dbs` that answers.
async fn answering(http: &reqwest::Client, base: &str, dbs: &[String]) -> Result<String> {
    let root = http.get(format!("{base}/whoami")).send().await.with_context(|| format!("{base} doesn't answer"))?;
    if root.status() != reqwest::StatusCode::NOT_FOUND {
        return Ok(base.to_string());
    }
    for d in dbs {
        let db = format!("{base}/db/{d}");
        if answers(http, &db).await {
            return Ok(db);
        }
    }
    bail!("{base} has no default database, and none of {} answers there", dbs.join(", "))
}

/// Whether a database's `/whoami` is answered (anything but a 404).
async fn answers(http: &reqwest::Client, at: &str) -> bool {
    http.get(format!("{at}/whoami")).send().await.is_ok_and(|r| r.status() != reqwest::StatusCode::NOT_FOUND)
}

/// Writes the token for `base` into the logins file (owner-only on unix).
fn keep_login(base: &str, token: String) -> Result<()> {
    let dir = home()?;
    std::fs::create_dir_all(&dir)?;
    let mut all = logins();
    all.insert(base.to_string(), token);
    let at = dir.join("login.json");
    std::fs::write(&at, serde_json::to_vec_pretty(&all)?)?;
    #[cfg(unix)]
    std::fs::set_permissions(&at, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    Ok(())
}

// ---------------------------------------------------------------- the project's files

/// The files an apply reads: pondra.toml, objects/**/*.sql, migrations/*.sql, tests/*.sql.
fn files(dir: &Path) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let toml = dir.join("pondra.toml");
    ensure!(toml.exists(), "{} holds no pondra.toml: pondra init starts a project, pondra export writes one from a database", dir.display());
    out.insert("pondra.toml".into(), std::fs::read_to_string(&toml)?);
    fn walk(root: &Path, at: &Path, deep: bool, out: &mut BTreeMap<String, String>) -> Result<()> {
        let Ok(entries) = std::fs::read_dir(at) else { return Ok(()) };
        for e in entries {
            let p = e?.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if name.starts_with('.') {
                continue;
            }
            if p.is_dir() {
                if deep {
                    walk(root, &p, deep, out)?;
                }
            } else if name.ends_with(".sql") {
                let rel = p.strip_prefix(root)?.components().map(|c| c.as_os_str().to_string_lossy().to_string()).collect::<Vec<_>>().join("/");
                out.insert(rel, std::fs::read_to_string(&p).with_context(|| p.display().to_string())?);
            }
        }
        Ok(())
    }
    walk(dir, &dir.join("objects"), true, &mut out)?;
    walk(dir, &dir.join("migrations"), false, &mut out)?;
    walk(dir, &dir.join("tests"), false, &mut out)?;
    Ok(out)
}

/// `[secrets]`' values, from this machine's environment: bound into statements, never kept.
fn secrets(toml: &Toml) -> Result<BTreeMap<String, String>> {
    toml.secrets.iter().map(|(name, from)| {
        let var = from.strip_prefix("env:").with_context(|| format!("[secrets] {name} = \"env:VARIABLE\": its value comes from this machine's environment"))?;
        Ok((name.clone(), std::env::var(var).with_context(|| format!("secret {name}: set {var}"))?))
    }).collect()
}

/// pondra.toml's environment whose values a database takes: its own, else `[env.dev]`'s (a branch).
fn section(toml: &Toml, env: &str) -> Option<String> {
    if toml.env.contains_key(env) { Some(env.to_string()) } else { toml.env.contains_key("dev").then(|| "dev".to_string()) }
}

fn steps_text(steps: &[Value]) -> String {
    let wide = steps.iter().map(|s| s["name"].as_str().unwrap_or_default().chars().count()).max().unwrap_or(0).clamp(8, 40);
    steps.iter().map(|s| format!("  {} {:<10} {:<wide$}  {}\n", s["mark"].as_str().unwrap_or(" "), s["kind"].as_str().unwrap_or_default(), s["name"].as_str().unwrap_or_default(), s["what"].as_str().unwrap_or_default())).collect()
}

fn tests_text(tests: &[Value]) -> (String, bool) {
    let failed: Vec<&Value> = tests.iter().filter(|t| t["ok"] != true).collect();
    let mut out = match (tests.len(), failed.len()) {
        (0, _) => "  ✓ tests      none in tests/\n".to_string(),
        (n, 0) => format!("  ✓ tests      {n} passed\n"),
        (n, f) => format!("  ✗ tests      {f} of {n} failed\n"),
    };
    for t in &failed {
        let why = match (&t["error"], &t["rows"]) {
            (Value::String(e), _) => e.clone(),
            (_, n) => format!("{n} rows back: {}", t["first"]),
        };
        out.push_str(&format!("      {}: {why}\n", t["test"].as_str().unwrap_or_default()));
    }
    (out, failed.is_empty())
}

// ---------------------------------------------------------------- the commands

pub async fn command(cmd: Command) -> Result<()> {
    let (said, ok) = run(cmd).await?;
    print!("{said}");
    if !ok {
        std::process::exit(1);
    }
    Ok(())
}

async fn run(cmd: Command) -> Result<(String, bool)> {
    match cmd {
        Command::Init { dir, name } => init(dir.as_deref().unwrap_or("."), name).map(|s| (s, true)),
        Command::Login { url, project, token, user } => login(url, &project, token, user).await.map(|s| (s + "\n", true)),
        Command::Branch { name, from, delete, at } => branch_cmd(&at, name, from, delete).await,
        Command::Plan { target, at, prune } => apply_env(&at.with(target), Run { apply: false, test: false, prune, fresh: false }).await,
        Command::Switch { name, at } => switch_cmd(&at, &name).await,
        Command::Apply { target, at, watch, fresh, no_test, prune, applies } => {
            let at = at.with(target);
            let run = Run { apply: true, test: !no_test, prune, fresh };
            match watch {
                true => watch_env(&at, run, applies).await.map(|()| (String::new(), true)),
                false => apply_env(&at, run).await,
            }
        }
        Command::Test { target, at } => tests(&at.with(target)).await,
        Command::Export { dir, at } => export(dir.as_deref().unwrap_or("."), &at).await.map(|s| (s, true)),
        Command::Diff { target, at, against } => diff(&at.with(target), against).await,
        Command::Ci { cmd: Ci::Init { project, force } } => ci_init(&project, force).map(|s| (s, true)),
    }
}

fn init(dir: &str, name: Option<String>) -> Result<String> {
    let dir = Path::new(dir);
    ensure!(!dir.join("pondra.toml").exists(), "{} is a project already (pondra.toml)", dir.display());
    std::fs::create_dir_all(dir)?;
    let name = name.unwrap_or_else(|| std::fs::canonicalize(dir).ok().and_then(|d| d.file_name().map(|n| n.to_string_lossy().to_string())).unwrap_or_else(|| "project".into()));
    let toml = format!(
        "[project]\nname = \"{}\"\n# server = \"https://pondra.example.com\"   # where its databases are (pondra serve --lakes)\n\n[env.prod]\n# url = \"https://prod.example.com\"     # a database served on its own\n# values = {{ min_order = 10 }}          # $name values its statements are given\n# protected = true                     # its objects change only by an apply (GRANT APPLY ON DATABASE prod TO ci)\n\n[env.dev]                             # what a developer's branch takes\n# clone = \"prod\"                      # the database a branch starts as (the default)\n\n# [env.local]                          # pondra apply local: this machine's own database, in lake/ (or lake = \"…\")\n\n# [secrets]\n# crm_token = \"env:CRM_TOKEN\"          # a $name's value from the applying machine's environment\n",
        database(&name)
    );
    std::fs::write(dir.join("pondra.toml"), toml)?;
    for (d, keep) in [("objects", "CREATE TABLE, VIEW, MATERIALIZED VIEW, FUNCTION, PROCEDURE, TASK, ROLE, GRANT: what every object is, in any layout.\n"), ("migrations", "One-off steps (a rename, a backfill), each run once in each database, in name order.\n"), ("tests", "Queries: any row back is a failure.\n")] {
        std::fs::create_dir_all(dir.join(d))?;
        let readme = dir.join(d).join("README.md");
        if !readme.exists() {
            std::fs::write(readme, keep)?;
        }
    }
    // (`pondra apply local` makes this machine's database in lake/: rows, never the project's files)
    let ignore = dir.join(".gitignore");
    let had = std::fs::read_to_string(&ignore).unwrap_or_default();
    if !had.lines().any(|l| matches!(l.trim(), "lake/" | "/lake/" | "lake" | "/lake")) {
        let gap = if had.is_empty() || had.ends_with('\n') { "" } else { "\n" };
        std::fs::write(&ignore, format!("{had}{gap}/lake/\n"))?;
    }
    Ok(format!("a project in {}: pondra.toml, objects/, migrations/, tests/, .gitignore\n", dir.display()))
}

// ---------------------------------------------------------------- branches

/// `pondra branch`: a branch started (NAME), dropped (-d NAME), or listed (no name).
async fn branch_cmd(at: &Where, name: Option<String>, from: Option<String>, delete: Option<String>) -> Result<(String, bool)> {
    let said = match (delete, name) {
        (Some(d), _) => drop_branch(at, &d).await?,
        (None, Some(n)) => start_branch(at, &n, from).await?,
        (None, None) => {
            ensure!(from.is_none(), "--from goes with a name: pondra branch NAME --from DB");
            list_branches(at).await?
        }
    };
    Ok((said, true))
}

/// `pondra branch NAME [--from DB]`: a database named NAME, cloned from DB (default: the environment's clone,
/// else prod) with no file copied, and the Git branch of that name switched to.
async fn start_branch(at: &Where, name: &str, from: Option<String>) -> Result<String> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let db = branch_db(&toml, name)?;
    let src = from.or_else(|| clone_of(&toml, &db)).unwrap_or_else(|| "prod".into());
    ensure!(db != database(&src), "{db} is the database it would start as");
    let answer = clone_db(at, &toml, &db, &src, false).await?;
    let in_git = git(dir, &["rev-parse", "--git-dir"]).is_some();
    if in_git {
        let switched = match git_has_branch(dir, name) {
            true => git_do(dir, &["switch", name]),
            false => git_do(dir, &["switch", "-c", name]),
        };
        switched.with_context(|| format!("{db} is made, but git couldn't switch"))?;
    }
    let mut out = match answer.get("attached") {
        Some(_) => format!("{name}: its database was there already"),
        None => format!("{name}: a branch with {src}'s data as it is now, no file copied"),
    };
    if !in_git {
        out.push_str(" (a database only: no git repository here)");
    }
    out.push_str("\n  next: pondra apply (or pondra apply --watch to apply on every save)\n");
    if let Some(base) = address(at, &toml, &db) {
        out.push_str(&format!("  console: {base}/\n"));
    }
    Ok(out)
}

/// `pondra switch NAME`: goes to a branch that exists, as `git switch` does, and makes its database if it
/// isn't there, as `pondra apply` would, so a branch is never checked out without its database. The
/// checked-out Git branch is what picks the database, so nothing is kept here.
///
/// ```text
/// pondra switch add-discounts
/// ```
async fn switch_cmd(at: &Where, name: &str) -> Result<(String, bool)> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    // (main and master are git's lines, not branches: they have no database, and `apply` names an environment)
    let db = match matches!(name, "main" | "master") {
        true => None,
        false => Some(branch_db(&toml, name)?),
    };
    ensure!(git_has_branch(dir, name), "no branch {name}: pondra branch {name} starts one");
    git_do(dir, &["switch", name])?;
    let Some(db) = db else {
        return Ok((format!("{name}: apply to an environment with pondra apply test or pondra apply prod\n"), true));
    };
    let new_db = made(at, &toml, &db, false).await?.is_some();
    let mut out = match new_db {
        true => format!("{name}: switched to it, its database made from {} with no file copied\n", clone_of(&toml, &db).unwrap_or_default()),
        false => format!("{name}: switched to it, its database was there already\n"),
    };
    out.push_str("  next: pondra apply (or pondra apply --watch to apply on every save)\n");
    if let Some(base) = address(at, &toml, &db) {
        out.push_str(&format!("  console: {base}/\n"));
    }
    Ok((out, true))
}

/// `pondra branch -d NAME`: its database dropped, and its Git branch with it when git lets it go.
async fn drop_branch(at: &Where, name: &str) -> Result<String> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let db = branch_db(&toml, name)?;
    let src = clone_of(&toml, &db).unwrap_or_else(|| "prod".into());
    maker(at, &toml, &db, &src).await?.sql(&format!("DROP DATABASE IF EXISTS {db}")).await?;
    let mut out = format!("{name}: its database dropped");
    if git(dir, &["rev-parse", "--git-dir"]).is_some() && git_has_branch(dir, name) {
        match git_do(dir, &["branch", "-d", name]) {
            Ok(()) => out.push_str(", and its git branch"),
            Err(e) => out.push_str(&format!("\n  the git branch stays: {}", first_line(&e))),
        }
    }
    Ok(out + "\n")
}

/// `pondra branch` with no name: the branch databases on the server branches are made on, the current Git
/// branch's marked with `*`.
async fn list_branches(at: &Where) -> Result<String> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let src = clone_of(&toml, "dev").unwrap_or_else(|| "prod".into());
    let rows = maker(at, &toml, "dev", &src).await?.sql("SELECT * FROM pondra.databases WHERE base IS NOT NULL ORDER BY name").await?;
    let now = git(dir, &["rev-parse", "--abbrev-ref", "HEAD"]).map(|b| database(&b));
    let shown: Vec<&Value> = rows.as_array().map(|a| a.iter().filter(|r| !declared(&toml, r["name"].as_str().unwrap_or_default())).collect()).unwrap_or_default();
    if shown.is_empty() {
        return Ok("no branches yet: pondra branch NAME starts one\n".into());
    }
    let width = shown.iter().map(|r| r["name"].as_str().unwrap_or_default().chars().count()).max().unwrap_or(0);
    let mut out = String::new();
    for r in shown {
        let name = r["name"].as_str().unwrap_or_default();
        let mark = if now.as_deref() == Some(name) { "*" } else { " " };
        let when: String = r["branched_at"].as_str().unwrap_or_default().replace('T', " ").chars().take(16).collect();
        let owner = r["owner"].as_str().map(|o| format!("  {o}")).unwrap_or_default();
        out.push_str(&format!("{mark} {name:<width$}  from {}  {when}{owner}\n", r["base"].as_str().unwrap_or_default()));
    }
    Ok(out)
}

// ---------------------------------------------------------------- continuous integration

/// The workflow `pondra ci init` writes, in three parts: the pull requests and the merge's test job
/// (in `workflow`), then prod, which waits for test when there is one. `@VERSION@` is this pondra.
const WORKFLOW_TOP: &str = r##"# Pondra (pondra ci init): each pull request gets a branch of prod with its code applied and
# tested, and the diff on the pull request; a merge to main applies to test; an approval in
# GitHub's "prod" environment applies the same commit to prod.
name: pondra
on:
  pull_request:
    types: [opened, synchronize, reopened, closed]
  push:
    branches: [main]
concurrency: pondra-${{ github.event.pull_request.number || github.ref }}
jobs:
  pull-request:
    if: github.event_name == 'pull_request' && github.event.action != 'closed'
    runs-on: ubuntu-latest
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_DEV_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra apply pr-${{ github.event.number }} --fresh
      - run: pondra diff pr-${{ github.event.number }} >> "$GITHUB_STEP_SUMMARY"
  closed:
    if: github.event_name == 'pull_request' && github.event.action == 'closed'
    runs-on: ubuntu-latest
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_DEV_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra branch -d pr-${{ github.event.number }}
"##;

const WORKFLOW_TEST: &str = r##"  test:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_TEST_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra apply test
"##;

const WORKFLOW_PROD: &str = r##"    runs-on: ubuntu-latest
    environment: prod
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_PROD_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra apply prod
"##;

/// The whole workflow for this pondra; the test job and prod's `needs` only when pondra.toml has [env.test].
fn workflow(version: &str, test: bool) -> String {
    let mut y = String::from(WORKFLOW_TOP);
    if test {
        y.push_str(WORKFLOW_TEST);
    }
    y.push_str("  prod:\n    if: github.event_name == 'push'\n");
    if test {
        y.push_str("    needs: test\n");
    }
    y.push_str(WORKFLOW_PROD);
    y.replace("@VERSION@", version)
}

/// `pondra ci init`: the project's GitHub Actions workflow, and what is left for a person to set up.
fn ci_init(dir: &str, force: bool) -> Result<String> {
    let project = Path::new(dir);
    ensure!(project.join("pondra.toml").exists(), "no pondra.toml here: pondra init first");
    let path = project.join(".github/workflows/pondra.yml");
    ensure!(force || !path.exists(), "{} is there: --force writes it again", path.display());
    let toml = read_toml(project)?;
    std::fs::create_dir_all(path.parent().unwrap_or(project))?;
    std::fs::write(&path, workflow(env!("CARGO_PKG_VERSION"), toml.env.contains_key("test")))?;
    let mut out = String::from("wrote .github/workflows/pondra.yml. Then:\n");
    out.push_str("  1. on prod, a user for CI that may apply and nothing more:\n");
    out.push_str("       CREATE USER ci_prod; GRANT APPLY ON DATABASE prod TO ci_prod; CREATE TOKEN github FOR USER ci_prod;\n");
    out.push_str("  2. on the server the branches are made on (dev), a token that may make databases\n");
    out.push_str("  3. in GitHub: the secrets PONDRA_DEV_TOKEN, PONDRA_PROD_TOKEN and, with [env.test], PONDRA_TEST_TOKEN\n");
    out.push_str("     (the same token for each if one server holds everything), and an environment named prod with\n");
    out.push_str("     required reviewers (Settings → Environments)\n");
    if !toml.env.get("prod").is_some_and(|e| e.protected) {
        out.push_str("  4. in pondra.toml: protected = true under [env.prod], so only CI's applies change prod\n");
    }
    Ok(out)
}

// ---------------------------------------------------------------- plans, applies and tests

/// What a run does to an environment. `apply` false is a plan only; `test` runs tests/ after; `fresh` makes a
/// clone's database again first; `prune` drops the objects the project no longer declares.
#[derive(Clone, Copy)]
struct Run {
    apply: bool,
    test: bool,
    prune: bool,
    fresh: bool,
}

/// Makes the project true in an environment, or shows what it would change: the plan, then each step, then the
/// tests. Prints the line for a database it made, and the console's address on success.
async fn apply_env(at: &Where, run: Run) -> Result<(String, bool)> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let env = env_of(at, &toml, dir)?;
    let head = made(at, &toml, &env, run.fresh).await?.unwrap_or_default();
    let node = node(at, &toml, &env).await?;
    let commit = commit(dir);
    let mut ask = json!({"files": files(dir)?, "env": section(&toml, &env), "commit": commit, "secrets": secrets(&toml)?, "prune": run.prune});
    let (plan, line) = ask_node(at, &toml, &env, &node, "/plan", ask.clone()).await?;
    let steps = plan["plan"]["steps"].as_array().cloned().unwrap_or_default();
    let refused: Vec<String> = serde_json::from_value(plan["plan"]["refused"].clone()).unwrap_or_default();
    let after = plan["plan"]["after"].as_u64().unwrap_or(0);
    let git = commit.map(|c| format!(" · git {c}")).unwrap_or_default();
    let mut out = format!("{head}{line}{env}: {}{git}\n", if after == 0 { "first apply".to_string() } else { format!("after apply {after}") });
    out.push_str(&if steps.is_empty() { "  nothing to change\n".to_string() } else { steps_text(&steps) });
    for r in &refused {
        out.push_str(&format!("  ✗ refused    {r}\n"));
    }
    if !refused.is_empty() || !run.apply {
        return Ok((out, refused.is_empty()));
    }
    ask["test"] = json!(run.test);
    ask["plan"] = plan["plan"]["id"].clone();
    let done = node.post("/apply", ask).await?;
    let ran = done["steps"].as_array().cloned().unwrap_or_default();
    let mut out = format!("{head}{line}{env}: apply {}{git}\n", done["apply"]);
    out.push_str(&if ran.is_empty() { "  nothing to change\n".to_string() } else { steps_text(&ran) });
    let mut ok = done["ok"] == true;
    if run.test {
        let (t, passed) = tests_text(done["tests"].as_array().map(Vec::as_slice).unwrap_or_default());
        out.push_str(&t);
        ok &= passed;
    }
    if ok && node.child.is_none() {
        out.push_str(&format!("  console: {}/\n", node.base));
    }
    Ok((out, ok))
}

/// `pondra test`: the project's tests in an environment.
async fn tests(at: &Where) -> Result<(String, bool)> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let env = env_of(at, &toml, dir)?;
    let head = made(at, &toml, &env, false).await?.unwrap_or_default();
    let node = node(at, &toml, &env).await?;
    let body = json!({"files": files(dir)?, "env": section(&toml, &env), "secrets": secrets(&toml)?});
    let (said, line) = ask_node(at, &toml, &env, &node, "/test", body).await?;
    let (t, ok) = tests_text(said["tests"].as_array().map(Vec::as_slice).unwrap_or_default());
    Ok((format!("{head}{line}{env}:\n{t}"), ok))
}

/// `pondra apply --watch`: the environment applied and tested now, then again after each save of the
/// project's files. An apply that fails is printed and the watch goes on; Ctrl-C ends it.
async fn watch_env(at: &Where, run: Run, applies: Option<usize>) -> Result<()> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let env = env_of(at, &toml, dir)?;
    let node = node(at, &toml, &env).await?;
    let protected = unwatched(&toml, &env) || is_protected_db(&node, &env).await?;
    ensure!(!protected, "pondra apply --watch applies on every save: not to {env}. git switch -c a-branch first");
    say(&format!("{env}: watching the project's files: each save is applied and tested (Ctrl-C stops)\n"));
    // (a lake's node is started once and kept while it watches: each apply goes to it by its address, and its
    // console stays up at the address printed)
    let target = match node.child {
        Some(_) => Where { env: Some(env.clone()), url: Some(node.base.clone()), lake: None, ..at.clone() },
        None => Where { env: Some(env.clone()), ..at.clone() },
    };
    let mut last = files(dir)?; // (what the last apply was of)
    let mut count = 0;
    loop {
        // (--fresh applies to the first apply only: a save never makes the database again)
        apply_say(&target, Run { fresh: run.fresh && count == 0, ..run }).await;
        count += 1;
        if applies.is_some_and(|n| count >= n) || !settled(dir, &mut last).await {
            return Ok(());
        }
    }
}

/// One apply_env with its tests, printed. A failure is printed too, and the watch goes on.
async fn apply_say(at: &Where, run: Run) {
    match apply_env(at, run).await {
        Ok((text, _)) => say(&text),
        Err(e) => say(&format!("{e:#}\n")),
    }
}

/// Waits until the files differ from `last` and have stopped changing for 300 ms, then makes `last`
/// what they are; false when Ctrl-C comes first. A save of several files is one change, one apply.
async fn settled(dir: &Path, last: &mut BTreeMap<String, String>) -> bool {
    loop {
        if !pause(500).await {
            return false;
        }
        let Ok(mut now) = files(dir) else { continue };
        if now == *last {
            continue;
        }
        loop {
            if !pause(300).await {
                return false;
            }
            match files(dir) {
                Ok(next) if next == now => break,
                Ok(next) => now = next,
                Err(_) => {}
            }
        }
        say(&format!("{} changed: {}\n", clock(), changed(last, &now)));
        *last = now;
        return true;
    }
}

/// Sleeps `ms`: false if Ctrl-C comes first.
async fn pause(ms: u64) -> bool {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => false,
        _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => true,
    }
}

/// The names of the files that differ between two snapshots, sorted.
fn changed(a: &BTreeMap<String, String>, b: &BTreeMap<String, String>) -> String {
    let mut names: Vec<&String> = a.keys().chain(b.keys()).filter(|k| a.get(*k) != b.get(*k)).collect();
    names.sort();
    names.dedup();
    names.iter().map(|n| n.as_str()).collect::<Vec<_>>().join(", ")
}

/// The time of day in UTC as HH:MM:SS (chrono is built without a clock, so it is worked out here).
fn clock() -> String {
    let s = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs() % 86_400);
    format!("{:02}:{:02}:{:02}", s / 3600, s % 3600 / 60, s % 60)
}

/// Prints now, not when the buffer fills: a watch's lines show as they happen.
fn say(text: &str) {
    use std::io::Write;
    print!("{text}");
    let _ = std::io::stdout().flush();
}

// ---------------------------------------------------------------- export and diff

async fn export(dir: &str, at: &Where) -> Result<String> {
    let project = Path::new(&at.project);
    let toml = read_toml(project)?;
    let env = env_of(at, &toml, project)?;
    let node = node(at, &toml, &env).await?;
    let files: BTreeMap<String, String> = serde_json::from_value(node.get("/export").await?)?;
    let dir = Path::new(dir);
    let mut written = 0;
    for (rel, text) in &files {
        let path = dir.join(rel);
        if rel == "pondra.toml" && path.exists() {
            continue; // (a project's own is kept)
        }
        std::fs::create_dir_all(path.parent().unwrap_or(dir))?;
        std::fs::write(&path, text)?;
        written += 1;
    }
    Ok(format!("{env}: {written} files in {}\n", dir.display()))
}

/// A table's name from its file's place: `objects/sales/orders.sql` is `sales.orders`.
fn table_of(path: &str) -> Option<String> {
    let rel = path.strip_prefix("objects/")?.strip_suffix(".sql")?;
    Some(rel.replace('/', "."))
}

/// Is this object file a table's definition.
fn is_table(text: &str) -> bool {
    text.trim_start().starts_with("CREATE TABLE")
}

/// A node's `/export`: each object file's path and text.
async fn exported(node: &Node) -> Result<BTreeMap<String, String>> {
    Ok(serde_json::from_value(node.get("/export").await?)?)
}

/// The objects that differ between two exports (`objects/` files only): `+` new, `-` gone, `~` changed.
fn objects_diff(a: &BTreeMap<String, String>, b: &BTreeMap<String, String>) -> String {
    let paths: std::collections::BTreeSet<&String> = a.keys().chain(b.keys()).filter(|p| p.starts_with("objects/")).collect();
    let lines: Vec<String> = paths.into_iter().filter_map(|p| match (a.get(p), b.get(p)) {
        (None, Some(_)) => Some(format!("- `+` {p}\n")),
        (Some(_), None) => Some(format!("- `-` {p}\n")),
        (Some(x), Some(y)) if x != y => Some(format!("- `~` {p}\n")),
        _ => None,
    }).collect();
    if lines.is_empty() { "Objects: the same.\n\n".to_string() } else { format!("Objects:\n\n{}\n", lines.concat()) }
}

/// The rows each table has changed against `against`, as Markdown rows: inserted, changed, deleted (`pondra.diff`).
async fn row_diffs(mine: &Node, against: &str, tables: &[String]) -> Vec<String> {
    let mut rows = vec![];
    for t in tables {
        let sql = format!("SELECT _change_type AS c, count(*) AS n FROM pondra.diff('{}.{t}', '{t}') GROUP BY 1", database(against));
        match mine.sql(&sql).await {
            Ok(Value::Array(found)) if !found.is_empty() => {
                let n = |c: &str| found.iter().filter(|r| r["c"] == c).map(|r| r["n"].as_u64().unwrap_or(0)).sum::<u64>();
                rows.push(format!("| {t} | {} | {} | {} |\n", n("insert"), n("update_postimage"), n("delete")));
            }
            Ok(_) => {}
            Err(e) => rows.push(format!("| {t} | ({}) | | |\n", first_line(&e))),
        }
    }
    rows
}

/// `pondra diff`: what an environment changes against another. When the other can't be read from here, the
/// objects are left out and the rows still come.
async fn diff(at: &Where, against: Option<String>) -> Result<(String, bool)> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let env = env_of(at, &toml, dir)?;
    let head = made(at, &toml, &env, false).await?.unwrap_or_default();
    let against = against.unwrap_or_else(|| clone_of(&toml, &env).unwrap_or_else(|| "prod".into()));
    let mine = node(at, &toml, &env).await?;
    let base = node(&Where { env: Some(against.clone()), url: None, lake: None, ..at.clone() }, &toml, &against).await?;
    let b = exported(&mine).await?;
    let theirs = exported(&base).await;
    let mut out = format!("{head}### {} against {}\n\n", database(&env), database(&against));
    match &theirs {
        Ok(a) => out.push_str(&objects_diff(a, &b)),
        Err(e) => out.push_str(&format!("Objects: {against} can't be read from here ({}).\n\n", first_line(e))),
    }
    // (a table both sides define; when the other can't be read, every table here)
    let tables: Vec<String> = b.iter().filter(|(p, t)| is_table(t) && theirs.as_ref().ok().is_none_or(|a| a.get(*p).is_some_and(|x| is_table(x)))).filter_map(|(p, _)| table_of(p)).collect();
    let rows = row_diffs(&mine, &against, &tables).await;
    out.push_str(&match rows.is_empty() {
        true => "Rows: the same in every table both have.\n".to_string(),
        false => format!("Rows:\n\n| table | inserted | changed | deleted |\n|---|---:|---:|---:|\n{}", rows.concat()),
    });
    Ok((out, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Toml {
        toml::from_str(text).unwrap()
    }

    #[test]
    fn names() {
        assert_eq!(database("pr-123"), "pr_123");
        assert_eq!(database("Feature/Orders-By-Channel"), "feature_orders_by_channel");
        assert_eq!(database("123"), "_123");
        assert_eq!(table_of("objects/sales/orders.sql").as_deref(), Some("sales.orders"));
        assert_eq!(table_of("objects/k.sql").as_deref(), Some("k"));
    }

    #[test]
    fn local_is_the_projects_lake() {
        let at = |url: Option<&str>| Where { env: None, project: "proj".into(), url: url.map(String::from), lake: None, token: None };
        let t = parse("[project]\nserver = \"https://pondra.acme.com\"\n");
        assert_eq!(lake_of(&at(None), &t, LOCAL), Some(Path::new("proj").join("lake").to_string_lossy().into_owned()));
        assert!(given(&at(None), &t, LOCAL)); // (nothing made on the server: no clone of prod)
        assert_eq!(lake_of(&at(Some("http://127.0.0.1:9")), &t, LOCAL), None);
        assert_eq!(lake_of(&at(None), &t, "add-discounts"), None);
        let declared = parse("[env.local]\nurl = \"http://127.0.0.1:8080\"\n");
        assert_eq!(lake_of(&at(None), &declared, LOCAL), None); // (pondra.toml says where it is)
        let values = parse("[env.local]\nvalues = { min_order = 0 }\n");
        assert!(lake_of(&at(None), &values, LOCAL).is_some()); // (values alone: still lake/)
        assert!(branch_db(&t, "local").is_err());
    }

    #[test]
    fn one_server_for_all_environments() {
        // (Layout A: the project names one server; prod is protected; test is a clone of prod)
        let t = parse("[project]\nserver = \"https://pondra.acme.com/\"\n\n[env.prod]\nprotected = true\n\n[env.test]\nclone = \"prod\"\n");
        assert!(!is_branch(&t, "prod") && !is_branch(&t, "test"));
        assert!(is_branch(&t, "add-discounts") && is_branch(&t, "dev"));
        assert_eq!(clone_of(&t, "test").as_deref(), Some("prod"));
        assert_eq!(clone_of(&t, "prod"), None);
        assert_eq!(clone_of(&t, "add-discounts").as_deref(), Some("prod"));
        assert_eq!(server_of(&t, "test").as_deref(), Some("https://pondra.acme.com"));
        assert_eq!(server_of(&t, "add-discounts").as_deref(), Some("https://pondra.acme.com"));
    }

    #[test]
    fn a_server_for_each_environment() {
        // (Layout B: each environment on its own server; a branch's server is dev's; `base` reads as clone)
        let t = parse("[env.prod]\nserver = \"https://prod.acme.com\"\n\n[env.test]\nserver = \"https://test.acme.com\"\nclone = \"prod\"\n\n[env.dev]\nserver = \"https://dev.acme.com\"\nbase = \"prod\"\n");
        assert_eq!(server_of(&t, "prod").as_deref(), Some("https://prod.acme.com"));
        assert_eq!(server_of(&t, "test").as_deref(), Some("https://test.acme.com"));
        assert_eq!(server_of(&t, "add-discounts").as_deref(), Some("https://dev.acme.com"));
        assert_eq!(clone_of(&t, "dev").as_deref(), Some("prod"));
        assert_eq!(clone_of(&t, "add-discounts").as_deref(), Some("prod"));
    }

    #[test]
    fn an_undeclared_prod_is_not_a_clone_of_itself() {
        let t = parse("[project]\nserver = \"https://x\"\n");
        assert_eq!(clone_of(&t, "prod"), None);
        assert!(is_branch(&t, "prod"));
        assert_eq!(clone_of(&t, "pr-1").as_deref(), Some("prod"));
    }

    #[test]
    fn branch_names_refuse_environments() {
        let t = parse("[env.prod]\n\n[env.test]\nclone = \"prod\"\n");
        assert!(branch_db(&t, "add-discounts").is_ok());
        assert!(branch_db(&t, "dev").is_ok());
        assert!(branch_db(&t, "test").is_err());
        assert!(branch_db(&t, "prod").is_err());
        assert!(branch_db(&t, "main").is_err());
    }

    #[test]
    fn servers_and_sign_in_databases() {
        let t = parse("[project]\nserver = \"https://a/\"\n\n[env.prod]\nurl = \"https://b\"\n\n[env.test]\nclone = \"prod\"\n\n[env.dev]\nclone = \"prod\"\n");
        assert_eq!(servers(&t), vec!["https://a", "https://b"]);
        // (prod is given by its own url, so it is not on a's server: only test is)
        assert_eq!(sign_in_dbs(&t, "https://a"), vec!["test"]);
        assert_eq!(sign_in_dbs(&t, "https://zz"), vec!["prod"]);
    }
}
