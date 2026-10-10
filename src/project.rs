//! A project on the command line (ADR-047 §4, §6): `pondra init`, `login`, `branch`, `plan`,
//! `deploy`, `test`, `export`, `diff`, `dev` and `ci init`. The folder is read here and sent to the environment's node
//! (`POST /deploy`), which plans and deploys it (`deploy.rs`). An environment is a database, found by
//! `--url` or `--lake`, else by pondra.toml: `[env.prod] url = …` (or `lake = …`), or the project's
//! `server` and the database's name (`/db/prod`, `pondra serve --lakes`). A token comes from
//! `--token`, `PONDRA_TOKEN`, or `pondra login`'s (`~/.pondra/login.json`).

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(clap::Subcommand)]
pub enum Command {
    /// Start a project in DIR (default: this folder): pondra.toml, objects/, migrations/ and tests/.
    /// `pondra export` writes one from a database instead.
    Init {
        dir: Option<String>,
        /// Its name (default: the folder's).
        #[arg(long)]
        name: Option<String>,
    },
    /// Keep a token for a server, for every command after: `pondra login https://pondra.acme.com`.
    Login {
        url: String,
        /// The token (asked for when neither this nor --user is given).
        #[arg(long)]
        token: Option<String>,
        /// Sign in as a user instead: its password is asked for (or PONDRA_PASSWORD).
        #[arg(long)]
        user: Option<String>,
    },
    /// A database that starts as another is now, copying no file: `pondra branch` makes one named
    /// after the git branch, from prod. `--drop` lets it go.
    Branch {
        /// Its name (default: the git branch's, letters, digits and _).
        name: Option<String>,
        /// The database it starts as.
        #[arg(long, default_value = "prod")]
        from: String,
        /// Made again if it is there.
        #[arg(long)]
        replace: bool,
        /// Drop it instead.
        #[arg(long)]
        drop: bool,
        /// Leave it as it is if it is there: for scripts and the git hook.
        #[arg(long, conflicts_with_all = ["replace", "drop"])]
        if_missing: bool,
        /// Install a git hook that makes the branch's database after each `git switch`.
        #[arg(long)]
        hook: bool,
        #[command(flatten)]
        at: Where,
    },
    /// What a deploy would change in an environment, and why anything is refused.
    Plan {
        #[command(flatten)]
        at: Where,
        /// Drop the objects the project no longer declares.
        #[arg(long)]
        prune: bool,
    },
    /// Make the project true in an environment: its plan, each step, then the tests (--test).
    Deploy {
        #[command(flatten)]
        at: Where,
        /// Run tests/ after it.
        #[arg(long)]
        test: bool,
        /// Drop the objects the project no longer declares.
        #[arg(long)]
        prune: bool,
    },
    /// Run the project's tests (tests/*.sql: any row back is a failure) in an environment.
    Test {
        #[command(flatten)]
        at: Where,
    },
    /// An environment's objects as a project's files, in DIR (default: this folder).
    Export {
        dir: Option<String>,
        #[command(flatten)]
        at: Where,
    },
    /// What an environment changes against another: the objects, and the rows of each table both
    /// have (`pondra.diff`). Written as Markdown, for a pull request's summary.
    Diff {
        #[command(flatten)]
        at: Where,
        /// The database it is compared with.
        #[arg(long, default_value = "prod")]
        against: String,
    },
    /// Continuous integration: `pondra ci init` writes a GitHub Actions workflow for the project.
    Ci {
        #[command(subcommand)]
        cmd: Ci,
    },
    /// Work on the git branch's database: make it if it isn't there (from --from), then deploy the
    /// project and run its tests each time a file of it is saved. Ctrl-C stops.
    Dev {
        #[command(flatten)]
        at: Where,
        /// The database a new branch starts as.
        #[arg(long, default_value = "prod")]
        from: String,
        /// Stop after this many deploys (the first included): for the tests.
        #[arg(long, hide = true)]
        deploys: Option<usize>,
    },
}

#[derive(clap::Subcommand)]
pub enum Ci {
    /// Write .github/workflows/pondra.yml for the project (--force writes it again).
    Init {
        #[arg(long, default_value = ".")]
        project: String,
        #[arg(long)]
        force: bool,
    },
}

#[derive(clap::Args, Clone)]
pub struct Where {
    /// The environment: its name in pondra.toml, or a database on the project's server (default:
    /// PONDRA_ENV, else the git branch's database, else pondra.toml's only environment).
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

#[derive(Deserialize, Default)]
struct Toml {
    #[serde(default)]
    project: Section,
    #[serde(default)]
    env: BTreeMap<String, Env>,
    #[serde(default)]
    secrets: BTreeMap<String, String>, // a `$name` → `env:VARIABLE`, read on the deploying machine
}

#[derive(Deserialize, Default)]
struct Section {
    #[serde(default)]
    server: Option<String>,
}

#[derive(Deserialize, Default, Clone)]
struct Env {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    lake: Option<String>,
    #[serde(default)]
    clone: Option<String>, // made again as a branch of this database before each deploy
    #[serde(default)]
    base: Option<String>, // branches of this database are made on this environment's server (ADR-058)
    #[serde(default)]
    protected: bool, // (its objects change only by a deploy: `pondra dev` never deploys here)
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

/// The commit a deploy is of: git's short id, `-dirty` when files changed since.
fn commit(dir: &Path) -> Option<String> {
    let id = git(dir, &["rev-parse", "--short", "HEAD"])?;
    Some(if git(dir, &["status", "--porcelain"]).is_some() { format!("{id}-dirty") } else { id })
}

/// The environment meant: `--env`, `PONDRA_ENV`, the git branch's database, or the only one.
fn env_of(at: &Where, toml: &Toml, dir: &Path) -> Result<String> {
    if let Some(e) = at.env.clone().or_else(|| std::env::var("PONDRA_ENV").ok()) {
        return Ok(e);
    }
    if at.url.is_some() || at.lake.is_some() {
        return Ok(toml.env.keys().next().cloned().unwrap_or_else(|| "dev".into()));
    }
    if let Some(b) = git(dir, &["rev-parse", "--abbrev-ref", "HEAD"]).filter(|b| !["main", "master", "HEAD"].contains(&b.as_str())) {
        return Ok(database(&b));
    }
    match &toml.env.keys().collect::<Vec<_>>()[..] {
        [one] => Ok(one.to_string()),
        _ => bail!("which environment? --env prod (or PONDRA_ENV)"),
    }
}

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

/// The node an environment is served by.
async fn node(at: &Where, toml: &Toml, env: &str) -> Result<Node> {
    let section = toml.env.get(env).cloned().unwrap_or_default();
    let lake = at.lake.clone().or_else(|| section.lake.clone().filter(|_| at.url.is_none()));
    if let Some(lake) = lake {
        let (mut child, base, owner, log) = crate::shell::start(&lake)?;
        let http = crate::shell::up(&base, &mut child, &log).await?;
        return Ok(Node { http, base, token: at.token.clone().or_else(|| std::env::var("PONDRA_TOKEN").ok()), owner, child: Some(child) });
    }
    let base = match (&at.url, &section.url, &toml.project.server) {
        (Some(u), ..) | (None, Some(u), _) => u.trim_end_matches('/').to_string(),
        (None, None, Some(s)) => format!("{}/db/{}", s.trim_end_matches('/'), database(env)),
        _ => bail!("where is {env}? [env.{env}] url = \"https://…\" or [project] server = \"https://…\" in pondra.toml, or --url"),
    };
    let token = at.token.clone().or_else(|| std::env::var("PONDRA_TOKEN").ok()).or_else(|| saved(&base));
    Ok(Node { http: reqwest::Client::new(), base, token, owner: String::new(), child: None })
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

async fn login(url: &str, token: Option<String>, user: Option<String>) -> Result<String> {
    let base = url.trim_end_matches('/').to_string();
    let http = reqwest::Client::new();
    let token = match (token, user) {
        (Some(t), _) => t,
        (None, Some(u)) => {
            let password = match std::env::var("PONDRA_PASSWORD") {
                Ok(p) => p,
                Err(_) => ask(&format!("{u}'s password: "), true)?,
            };
            let r = http.post(format!("{base}/login")).json(&json!({"user": u, "password": password})).send().await?;
            ensure!(r.status().is_success(), "{}", r.text().await?.trim());
            r.json::<Value>().await?["token"].as_str().context("no token")?.to_string()
        }
        (None, None) => ask("token: ", true)?,
    };
    let r = http.get(format!("{base}/whoami")).bearer_auth(&token).send().await.with_context(|| format!("{base} doesn't answer"))?;
    ensure!(r.status().is_success(), "{}", r.text().await?.trim());
    let me: Value = r.json().await?;
    ensure!(me["role"] != "none", "{base} doesn't take that token");
    let dir = home()?;
    std::fs::create_dir_all(&dir)?;
    let mut all = logins();
    all.insert(base.clone(), token);
    let at = dir.join("login.json");
    std::fs::write(&at, serde_json::to_vec_pretty(&all)?)?;
    #[cfg(unix)]
    std::fs::set_permissions(&at, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    let who = me["user"].as_str().filter(|u| !u.is_empty()).map(|u| format!("{u}, ")).unwrap_or_default();
    Ok(format!("signed in to {base} ({who}{})", me["role"].as_str().unwrap_or_default()))
}

// ---------------------------------------------------------------- the project's files

/// The files a deploy reads: pondra.toml, objects/**/*.sql, migrations/*.sql, tests/*.sql.
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
        Command::Login { url, token, user } => login(&url, token, user).await.map(|s| (s + "\n", true)),
        Command::Branch { hook: true, from, at, .. } => git_hook(&at, &from).map(|s| (s, true)),
        Command::Branch { name, from, replace, drop, if_missing, at, .. } => branch(name, &from, replace, drop, if_missing, &at).await.map(|s| (s, true)),
        Command::Plan { at, prune } => deploy(&at, false, false, prune).await,
        Command::Deploy { at, test, prune } => deploy(&at, true, test, prune).await,
        Command::Test { at } => tests(&at).await,
        Command::Export { dir, at } => export(dir.as_deref().unwrap_or("."), &at).await.map(|s| (s, true)),
        Command::Diff { at, against } => diff(&at, &against).await,
        Command::Ci { cmd: Ci::Init { project, force } } => ci_init(&project, force).map(|s| (s, true)),
        Command::Dev { at, from, deploys } => dev(&at, &from, deploys).await.map(|()| (String::new(), true)),
    }
}

fn init(dir: &str, name: Option<String>) -> Result<String> {
    let dir = Path::new(dir);
    ensure!(!dir.join("pondra.toml").exists(), "{} is a project already (pondra.toml)", dir.display());
    std::fs::create_dir_all(dir)?;
    let name = name.unwrap_or_else(|| std::fs::canonicalize(dir).ok().and_then(|d| d.file_name().map(|n| n.to_string_lossy().to_string())).unwrap_or_else(|| "project".into()));
    let toml = format!(
        "[project]\nname = \"{}\"\n# server = \"https://pondra.example.com\"   # where its databases are (pondra serve --lakes)\n\n[env.prod]\n# url = \"https://prod.example.com\"     # a database served on its own\n# values = {{ min_order = 10 }}          # $name values its statements are given\n# protected = true                     # its objects change only by a deploy (GRANT DEPLOY ON DATABASE prod TO ci)\n\n[env.dev]                             # what a developer's branch takes\n# base = \"prod\"                       # branches made on this server, of prod attached there (ADR-058)\n\n# [secrets]\n# crm_token = \"env:CRM_TOKEN\"          # a $name's value from the deploying machine's environment\n",
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
    Ok(format!("a project in {}: pondra.toml, objects/, migrations/, tests/\n", dir.display()))
}

async fn branch(name: Option<String>, from: &str, replace: bool, drop: bool, if_missing: bool, at: &Where) -> Result<String> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let name = match name.or_else(|| at.env.clone()) {
        Some(n) => database(&n),
        None => database(&git(dir, &["rev-parse", "--abbrev-ref", "HEAD"]).filter(|b| b != "HEAD").context("a branch's name: pondra branch NAME (or run it in a git branch)")?),
    };
    ensure!(name != database(from), "{name} is the database it would start as");
    // (a branch is made where its base's environment is: the server that has prod attached, ADR-058)
    let maker = toml.env.iter().find(|(_, e)| e.base.as_deref() == Some(from)).map(|(n, _)| n.clone()).unwrap_or_else(|| from.to_string());
    let base = node(&Where { env: Some(maker.clone()), ..at.clone() }, &toml, &maker).await?;
    if drop || replace {
        base.sql(&format!("DROP DATABASE IF EXISTS {name}")).await?;
        if drop {
            return Ok(format!("{name} dropped\n"));
        }
    }
    let ine = if if_missing { "IF NOT EXISTS " } else { "" };
    let made = base.sql(&format!("CREATE DATABASE {ine}{name} CLONE {}", database(from))).await?;
    // (an IF NOT EXISTS that found it answers the attachment it already had, not a new clone)
    if if_missing && made.get("attached").is_some() {
        return Ok(format!("{name}: there already\n"));
    }
    Ok(format!("{name}: {from} as it is now, no file copied (pondra deploy --env {name})\n"))
}

/// `pondra branch --hook`: a git hook that makes the branch's database each time a `git switch` lands
/// on a branch (`--if-missing`). A hook of pondra's own is rewritten; any other is refused by name.
fn git_hook(at: &Where, from: &str) -> Result<String> {
    const MARK: &str = "# pondra branch --hook";
    let dir = Path::new(&at.project);
    let hooks = git(dir, &["rev-parse", "--git-path", "hooks"]).context("a git repository: git init, then pondra branch --hook")?;
    let path = dir.join(hooks).join("post-checkout");
    if path.exists() {
        let old = std::fs::read(&path)?;
        ensure!(String::from_utf8_lossy(&old).contains(MARK), "{} is a post-checkout hook of its own: move it aside, then pondra branch --hook", path.display());
    }
    // (the words are quoted, so a path with a space or a `$` stays one word in the shell)
    let project = sh_word(&std::path::absolute(dir)?.display().to_string());
    let script = format!(
        "#!/bin/sh\n{MARK}: a database for each git branch, made when you switch to it\n\
         [ \"$3\" = 1 ] || exit 0\n\
         b=$(git rev-parse --abbrev-ref HEAD)\n\
         case \"$b\" in main|master|HEAD) exit 0 ;; esac\n\
         pondra branch --if-missing --from {} --project {project} || echo \"pondra: no database for $b yet (pondra branch)\"\n",
        sh_word(from)
    );
    std::fs::write(&path, script)?;
    #[cfg(unix)]
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))?;
    Ok(format!("a git hook in {}: each git switch to a branch makes its database (from {from})\n", path.display()))
}

/// A word for a POSIX shell: in single quotes, with each quote inside it written `'\''`.
fn sh_word(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

async fn deploy(at: &Where, apply: bool, test: bool, prune: bool) -> Result<(String, bool)> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let env = env_of(at, &toml, dir)?;
    let files = files(dir)?;
    if let (true, Some(from)) = (apply, toml.env.get(&env).and_then(|e| e.clone.clone())) {
        branch(Some(env.clone()), &from, true, false, false, at).await?; // (made again from its base before each deploy)
    }
    let node = node(at, &toml, &env).await?;
    let commit = commit(dir);
    let mut ask = json!({"files": files, "env": section(&toml, &env), "commit": commit, "secrets": secrets(&toml)?, "prune": prune});
    let plan = node.post("/plan", ask.clone()).await?;
    let steps = plan["plan"]["steps"].as_array().cloned().unwrap_or_default();
    let refused: Vec<String> = serde_json::from_value(plan["plan"]["refused"].clone()).unwrap_or_default();
    let after = plan["plan"]["after"].as_u64().unwrap_or(0);
    let git = commit.map(|c| format!(" · git {c}")).unwrap_or_default();
    let mut out = format!("{env}: {}{git}\n", if after == 0 { "first deploy".to_string() } else { format!("after deploy {after}") });
    out.push_str(&if steps.is_empty() { "  nothing to change\n".to_string() } else { steps_text(&steps) });
    for r in &refused {
        out.push_str(&format!("  ✗ refused    {r}\n"));
    }
    if !refused.is_empty() || !apply {
        return Ok((out, refused.is_empty()));
    }
    ask["test"] = json!(test);
    ask["plan"] = plan["plan"]["id"].clone();
    let done = node.post("/deploy", ask).await?;
    let ran = done["steps"].as_array().cloned().unwrap_or_default();
    out = format!("{env}: deploy {}{git}\n", done["deploy"]);
    out.push_str(&if ran.is_empty() { "  nothing to change\n".to_string() } else { steps_text(&ran) });
    let mut ok = done["ok"] == true;
    if test {
        let (t, passed) = tests_text(done["tests"].as_array().map(Vec::as_slice).unwrap_or_default());
        out.push_str(&t);
        ok &= passed;
    }
    Ok((out, ok))
}

async fn tests(at: &Where) -> Result<(String, bool)> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let env = env_of(at, &toml, dir)?;
    let node = node(at, &toml, &env).await?;
    let said = node.post("/test", json!({"files": files(dir)?, "env": section(&toml, &env), "secrets": secrets(&toml)?})).await?;
    let (t, ok) = tests_text(said["tests"].as_array().map(Vec::as_slice).unwrap_or_default());
    Ok((format!("{env}:\n{t}"), ok))
}

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

async fn diff(at: &Where, against: &str) -> Result<(String, bool)> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let env = env_of(at, &toml, dir)?;
    let mine = node(at, &toml, &env).await?;
    let base = node(&Where { env: Some(against.to_string()), url: None, lake: None, ..at.clone() }, &toml, against).await?;
    let (a, b): (BTreeMap<String, String>, BTreeMap<String, String>) = (serde_json::from_value(base.get("/export").await?)?, serde_json::from_value(mine.get("/export").await?)?);
    let mut out = format!("### {} against {}\n\n", database(&env), database(against));
    let objects: Vec<String> = a.keys().chain(b.keys()).filter(|p| p.starts_with("objects/")).collect::<std::collections::BTreeSet<_>>().into_iter().filter_map(|p| match (a.get(p), b.get(p)) {
        (None, Some(_)) => Some(format!("- `+` {p}\n")),
        (Some(_), None) => Some(format!("- `-` {p}\n")),
        (Some(x), Some(y)) if x != y => Some(format!("- `~` {p}\n")),
        _ => None,
    }).collect();
    out.push_str(&if objects.is_empty() { "Objects: the same.\n\n".to_string() } else { format!("Objects:\n\n{}\n", objects.concat()) });
    let tables: Vec<String> = b.iter().filter(|(p, t)| t.trim_start().starts_with("CREATE TABLE") && a.get(*p).is_some_and(|x| x.trim_start().starts_with("CREATE TABLE"))).filter_map(|(p, _)| table_of(p)).collect();
    let mut rows = vec![];
    // (asked of this environment's node, which reads its own rows as they are now: what was just
    // written to a branch is in its diff; the base is read as its neighbour, `--attach-found`)
    for t in &tables {
        let sql = format!("SELECT _change_type AS c, count(*) AS n FROM pondra.diff('{}.{t}', '{t}') GROUP BY 1", database(against));
        match mine.sql(&sql).await {
            Ok(Value::Array(found)) if !found.is_empty() => {
                let n = |c: &str| found.iter().filter(|r| r["c"] == c).map(|r| r["n"].as_u64().unwrap_or(0)).sum::<u64>();
                rows.push(format!("| {t} | {} | {} | {} |\n", n("insert"), n("update_postimage"), n("delete")));
            }
            Ok(_) => {}
            Err(e) => rows.push(format!("| {t} | ({}) | | |\n", format!("{e:#}").lines().next().unwrap_or_default())),
        }
    }
    out.push_str(&match rows.is_empty() {
        true => "Rows: the same in every table both have.\n".to_string(),
        false => format!("Rows:\n\n| table | inserted | changed | deleted |\n|---|---:|---:|---:|\n{}", rows.concat()),
    });
    Ok((out, true))
}

// ---------------------------------------------------------------- the developer's day

/// The workflow `pondra ci init` writes, in three parts: the pull requests and the merge's test job
/// (in `workflow`), then prod, which waits for test when there is one. `@VERSION@` is this pondra.
const WORKFLOW_TOP: &str = r##"# Pondra (pondra ci init): each pull request gets a branch of prod with its code deployed and
# tested, and the diff on the pull request; a merge to main deploys to test; an approval in
# GitHub's "prod" environment deploys the same commit to prod.
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
      - run: pondra branch pr-${{ github.event.number }} --from prod --replace
      - run: pondra deploy --env pr-${{ github.event.number }} --test
      - run: pondra diff --env pr-${{ github.event.number }} >> "$GITHUB_STEP_SUMMARY"
  closed:
    if: github.event_name == 'pull_request' && github.event.action == 'closed'
    runs-on: ubuntu-latest
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_DEV_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra branch pr-${{ github.event.number }} --drop
"##;

const WORKFLOW_TEST: &str = r##"  test:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_DEV_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra deploy --env test --test
"##;

const WORKFLOW_PROD: &str = r##"    runs-on: ubuntu-latest
    environment: prod
    env:
      PONDRA_TOKEN: ${{ secrets.PONDRA_PROD_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - run: pip install pondra==@VERSION@
      - run: pondra deploy --env prod --test
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
    out.push_str("  1. on prod, a user for CI that may deploy and nothing more:\n");
    out.push_str("       CREATE USER ci; GRANT DEPLOY ON DATABASE prod TO ci; CREATE TOKEN github FOR USER ci;\n");
    out.push_str("  2. on the server the branches are made on (dev), a token that may make databases\n");
    out.push_str("  3. in GitHub: the secrets PONDRA_PROD_TOKEN and PONDRA_DEV_TOKEN (the same token if one server\n");
    out.push_str("     holds everything), and an environment named prod with required reviewers\n");
    out.push_str("     (Settings → Environments)\n");
    if !toml.env.get("prod").is_some_and(|e| e.protected) {
        out.push_str("  4. in pondra.toml: protected = true under [env.prod], so only CI's deploys change prod\n");
    }
    Ok(out)
}

fn dev_refused(env: &str) -> Result<()> {
    bail!("pondra dev deploys on every save: not to {env}. git switch -c a-branch first")
}

/// `pondra dev`: the git branch's database (made from `from` when pondra.toml doesn't declare it), deployed
/// and tested now, and again after each save of the project's files. A deploy that fails is printed and
/// the watch goes on; Ctrl-C ends it.
async fn dev(at: &Where, from: &str, deploys: Option<usize>) -> Result<()> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    // (on main, env_of would ask which environment: the refusal names main, and says what to do)
    let env = match git(dir, &["rev-parse", "--abbrev-ref", "HEAD"]) {
        Some(b) if at.env.is_none() && (b == "main" || b == "master") => b,
        _ => env_of(at, &toml, dir)?,
    };
    if matches!(env.as_str(), "prod" | "main" | "master") || toml.env.get(&env).is_some_and(|e| e.protected) {
        return dev_refused(&env);
    }
    let lake = at.lake.is_some() || (at.url.is_none() && toml.env.get(&env).is_some_and(|e| e.lake.is_some()));
    ensure!(!lake, "pondra dev works with a server (--url, or [project] server): a lake's node stops after each command");
    if !toml.env.contains_key(&env) {
        say(&branch(Some(env.clone()), from, false, false, true, at).await?);
    }
    let node = node(at, &toml, &env).await?;
    let db = node.sql(&format!("SELECT protected FROM pondra.databases WHERE name = '{}'", database(&env))).await?;
    if db[0]["protected"] == true {
        return dev_refused(&env);
    }
    say(&format!("{env}: console at {}/\nwatching the project's files: each save is deployed and tested (Ctrl-C stops)\n", node.base));
    let target = Where { env: Some(env.clone()), ..at.clone() };
    let mut last = files(dir)?; // (what the last deploy was of)
    let mut count = 0;
    loop {
        deploy_say(&target).await;
        count += 1;
        if deploys.is_some_and(|n| count >= n) || !settled(dir, &mut last).await {
            return Ok(());
        }
    }
}

/// One deploy with its tests, printed. A failure is printed too, and the watch goes on.
async fn deploy_say(at: &Where) {
    match deploy(at, true, true, false).await {
        Ok((text, _)) => say(&text),
        Err(e) => say(&format!("{e:#}\n")),
    }
}

/// Waits until the files differ from `last` and have stopped changing for 300 ms, then makes `last`
/// what they are; false when Ctrl-C comes first. A save of several files is one change, one deploy.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(database("pr-123"), "pr_123");
        assert_eq!(database("Feature/Orders-By-Channel"), "feature_orders_by_channel");
        assert_eq!(database("123"), "_123");
        assert_eq!(table_of("objects/sales/orders.sql").as_deref(), Some("sales.orders"));
        assert_eq!(table_of("objects/k.sql").as_deref(), Some("k"));
    }
}
