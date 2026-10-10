//! A project on the command line (ADR-047 §4, §6): `pondra init`, `login`, `branch`, `plan`,
//! `deploy`, `test`, `export` and `diff`. The folder is read here and sent to the environment's node
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
        Command::Branch { name, from, replace, drop, at } => branch(name, &from, replace, drop, &at).await.map(|s| (s, true)),
        Command::Plan { at, prune } => deploy(&at, false, false, prune).await,
        Command::Deploy { at, test, prune } => deploy(&at, true, test, prune).await,
        Command::Test { at } => tests(&at).await,
        Command::Export { dir, at } => export(dir.as_deref().unwrap_or("."), &at).await.map(|s| (s, true)),
        Command::Diff { at, against } => diff(&at, &against).await,
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

async fn branch(name: Option<String>, from: &str, replace: bool, drop: bool, at: &Where) -> Result<String> {
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
    base.sql(&format!("CREATE DATABASE {name} CLONE {}", database(from))).await?;
    Ok(format!("{name}: {from} as it is now, no file copied (pondra deploy --env {name})\n"))
}

async fn deploy(at: &Where, apply: bool, test: bool, prune: bool) -> Result<(String, bool)> {
    let dir = Path::new(&at.project);
    let toml = read_toml(dir)?;
    let env = env_of(at, &toml, dir)?;
    let files = files(dir)?;
    if let (true, Some(from)) = (apply, toml.env.get(&env).and_then(|e| e.clone.clone())) {
        branch(Some(env.clone()), &from, true, false, at).await?; // (made again from its base before each deploy)
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
