//! Git folders (ADR-060 §8): a folder of the workspace (`files/<folder>/`) paired with a branch of a
//! GitHub repository, so the console can commit to it and open pull requests. The node talks to the
//! host's REST API itself: no `git` program is needed, so it runs in the distroless image and as a
//! Windows service. The folder's record (`.pondra-git.json`) names the repository, the branch, the
//! commit the folder matches and each file's git blob id as of that commit. A blob id is git's own
//! (SHA-1 of `blob <size>\0` and the bytes), so local changes come from the folder's files and the
//! record alone, with no network.

use crate::auth::Role;
use crate::ext;
use crate::files::{under_files, APPLIES, VERSIONS};
use crate::routines::{Outcome, Who};
use crate::server::App;
use crate::store::{Changed, Lake};
use anyhow::{anyhow, bail, ensure, Context, Result};
use aws_lc_rs::digest;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::sql::sqlparser::ast::FunctionArg;
use futures::TryStreamExt;
use object_store::path::Path;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock};

const RECORD: &str = ".pondra-git.json";

/// A Git folder's record: what it is paired with, and each file's blob id as of its commit.
#[derive(Serialize, Deserialize, Clone, Default)]
struct Record {
    repository: String,
    branch: String,
    commit: String,
    files: BTreeMap<String, String>,
}

// ---------------------------------------------------------------- the host

/// What the procedures ask of a host. Each call is one or two REST requests (`GitHub`).
#[async_trait::async_trait]
pub trait Host: Send + Sync {
    async fn default_branch(&self) -> Result<String>;
    /// The commit a branch points at; None when the repository has no such branch.
    async fn head(&self, branch: &str) -> Result<Option<String>>;
    /// Every file of a commit: path → blob id.
    async fn files(&self, commit: &str) -> Result<BTreeMap<String, String>>;
    async fn blob(&self, id: &str) -> Result<Vec<u8>>;
    /// A commit on top of `parent` that writes or (None) deletes each path, moved onto `branch`.
    /// None when the branch moved on since `parent` (GitHub's refusal to fast-forward).
    async fn commit_on(&self, branch: &str, parent: &str, message: &str, changes: &[(String, Option<Vec<u8>>)]) -> Result<Option<String>>;
    async fn make_branch(&self, name: &str, commit: &str) -> Result<()>;
    /// Opens a pull request; its number and its web address.
    async fn pull_request(&self, title: &str, head: &str, base: &str, body: &str) -> Result<(u64, String)>;
}

/// The host a provider's repositories are reached through: one arm per provider.
pub fn host(provider: &str, repository: &str, token: &str) -> Result<Box<dyn Host>> {
    match provider {
        "github" => Ok(Box::new(GitHub::new(repository, token)?)),
        "gitlab" => bail!("GitLab isn't spoken to yet: PROVIDER 'github'"),
        other => bail!("{other} isn't spoken to yet: PROVIDER 'github'"),
    }
}

/// GitHub's REST API for one repository, with a token of its own.
pub struct GitHub {
    api: String,
    repo: String,
    token: String,
}

impl GitHub {
    fn new(repository: &str, token: &str) -> Result<Self> {
        let (api, repo) = api_of(repository)?;
        Ok(Self { api, repo, token: token.to_string() })
    }

    /// One request under the repository's path: GitHub's status and its JSON answer.
    async fn send(&self, method: Method, path: &str, body: Option<Value>) -> Result<(StatusCode, Value)> {
        let url = format!("{}/repos/{}{path}", self.api, self.repo);
        let mut req = ext::web().request(method, url.as_str()).bearer_auth(&self.token).header("accept", "application/vnd.github+json").header("user-agent", "pondra").header("x-github-api-version", "2022-11-28");
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.with_context(|| format!("reaching GitHub at {}", self.api))?;
        let status = resp.status();
        let text = resp.text().await?;
        Ok((status, serde_json::from_str(&text).unwrap_or(Value::Null)))
    }

    /// A request that must succeed.
    async fn ok(&self, what: &str, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let (status, v) = self.send(method, path, body).await?;
        if !status.is_success() {
            return Err(refused(status, &v, what));
        }
        Ok(v)
    }

    async fn tree_of(&self, commit: &str, what: &str) -> Result<String> {
        let c = self.ok(what, Method::GET, &format!("/git/commits/{commit}"), None).await?;
        Ok(c["tree"]["sha"].as_str().context("GitHub: a commit without its tree")?.to_string())
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<String> {
        let body = json!({"content": B64.encode(bytes), "encoding": "base64"});
        let v = self.ok(&format!("writing a blob to {}", self.repo), Method::POST, "/git/blobs", Some(body)).await?;
        Ok(v["sha"].as_str().context("GitHub: a blob without its id")?.to_string())
    }
}

/// GitHub's refusal in its own words: `GitHub: 401 Bad credentials (reading main of acme/sales)`.
fn refused(status: StatusCode, v: &Value, what: &str) -> anyhow::Error {
    anyhow!("GitHub: {} {} ({what})", status.as_u16(), v["message"].as_str().or(status.canonical_reason()).unwrap_or("no answer"))
}

#[async_trait::async_trait]
impl Host for GitHub {
    async fn default_branch(&self) -> Result<String> {
        let v = self.ok(&format!("reading {}", self.repo), Method::GET, "", None).await?;
        Ok(v["default_branch"].as_str().context("GitHub: a repository without a default branch")?.to_string())
    }

    async fn head(&self, branch: &str) -> Result<Option<String>> {
        let (status, v) = self.send(Method::GET, &format!("/git/ref/heads/{branch}"), None).await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(refused(status, &v, &format!("reading {branch} of {}", self.repo)));
        }
        Ok(v["object"]["sha"].as_str().map(String::from))
    }

    async fn files(&self, commit: &str) -> Result<BTreeMap<String, String>> {
        let what = format!("reading {commit} of {}", self.repo);
        let tree = self.tree_of(commit, &what).await?;
        let t = self.ok(&what, Method::GET, &format!("/git/trees/{tree}?recursive=1"), None).await?;
        ensure!(t["truncated"] != true, "GitHub: {} is too big to list in one answer", self.repo);
        Ok(t["tree"].as_array().into_iter().flatten().filter(|e| e["type"] == "blob").filter_map(|e| Some((e["path"].as_str()?.to_string(), e["sha"].as_str()?.to_string()))).collect())
    }

    async fn blob(&self, id: &str) -> Result<Vec<u8>> {
        let v = self.ok(&format!("reading a blob of {}", self.repo), Method::GET, &format!("/git/blobs/{id}"), None).await?;
        let text: String = v["content"].as_str().context("GitHub: a blob without content")?.split_whitespace().collect(); // (GitHub wraps its base64)
        Ok(B64.decode(text)?)
    }

    async fn commit_on(&self, branch: &str, parent: &str, message: &str, changes: &[(String, Option<Vec<u8>>)]) -> Result<Option<String>> {
        let what = format!("committing to {branch} of {}", self.repo);
        let mut tree = vec![];
        for (path, bytes) in changes {
            let sha = match bytes {
                Some(b) => Some(self.put_blob(b).await?),
                None => None, // (a deleted path: `sha` null removes it from the tree)
            };
            tree.push(json!({"path": path, "mode": "100644", "type": "blob", "sha": sha}));
        }
        let base = self.tree_of(parent, &what).await?;
        let t = self.ok(&what, Method::POST, "/git/trees", Some(json!({"base_tree": base, "tree": tree}))).await?;
        let c = self.ok(&what, Method::POST, "/git/commits", Some(json!({"message": message, "tree": t["sha"], "parents": [parent]}))).await?;
        let commit = c["sha"].as_str().context("GitHub: a commit without its id")?.to_string();
        let (status, v) = self.send(Method::PATCH, &format!("/git/refs/heads/{branch}"), Some(json!({"sha": commit, "force": false}))).await?;
        match status.as_u16() {
            200..=299 => Ok(Some(commit)),
            422 => Ok(None), // (not a fast-forward: the branch moved on)
            _ => Err(refused(status, &v, &what)),
        }
    }

    async fn make_branch(&self, name: &str, commit: &str) -> Result<()> {
        let body = json!({"ref": format!("refs/heads/{name}"), "sha": commit});
        self.ok(&format!("making {name} in {}", self.repo), Method::POST, "/git/refs", Some(body)).await.map(drop)
    }

    async fn pull_request(&self, title: &str, head: &str, base: &str, body: &str) -> Result<(u64, String)> {
        let b = json!({"title": title, "head": head, "base": base, "body": body});
        let v = self.ok(&format!("opening a pull request in {}", self.repo), Method::POST, "/pulls", Some(b)).await?;
        Ok((v["number"].as_u64().context("GitHub: a pull request without its number")?, v["html_url"].as_str().unwrap_or_default().to_string()))
    }
}

/// A repository URL as it is kept: no trailing `/`, no `.git`.
fn normal(url: &str) -> String {
    let u = url.trim().trim_end_matches('/');
    u.strip_suffix(".git").unwrap_or(u).trim_end_matches('/').to_string()
}

/// A repository URL as its API's base and `owner/repo`: github.com's API is at api.github.com,
/// another host's (GitHub Enterprise) at its `/api/v3`. A token goes only over https, or to this machine.
/// The URL is checked as written (no `..`, no user part), since a secret's SCOPE is matched against it.
fn api_of(url: &str) -> Result<(String, String)> {
    let url = normal(url);
    let usage = "a repository's URL: https://github.com/owner/repo";
    let (scheme, rest) = url.split_once("://").context(usage)?;
    let (site, repo) = rest.split_once('/').context(usage)?;
    let parts: Vec<&str> = repo.split('/').collect();
    let named = |p: &&str| clean(p) && p.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c));
    ensure!(parts.len() == 2 && parts.iter().all(named) && site.chars().all(|c| c.is_ascii_alphanumeric() || ".-:[]".contains(c)), "{url}: {usage}");
    let host = site.rsplit_once(':').filter(|(_, port)| port.chars().all(|c| c.is_ascii_digit())).map_or(site, |(h, _)| h);
    ensure!(scheme == "https" || (scheme == "http" && matches!(host, "127.0.0.1" | "localhost" | "[::1]")), "a git token goes over https only: {url}");
    let api = match site {
        "github.com" => "https://api.github.com".to_string(),
        _ => format!("{scheme}://{site}/api/v3"),
    };
    Ok((api, repo.to_string()))
}

/// A relative path with no empty, `.` or `..` part.
fn clean(path: &str) -> bool { path.split('/').all(|p| !p.is_empty() && p != "." && p != "..") }

// ---------------------------------------------------------------- the secret

/// A repository's git secret: the one named, else the one whose SCOPE is the longest covering the
/// repository; either must be one the caller may use (USAGE) and must cover it, so a token goes only
/// where an admin said it may. Returns the secret's name, its token and its provider.
pub async fn git_token(lake: &Lake, repository: &str, name: Option<&str>) -> Result<(String, String, String)> {
    let all = ext::list(lake).await?;
    let mut covering: Vec<(&String, usize)> = all.iter().filter(|(n, s)| s.kind == "git" && ext::usable(n, s)).filter_map(|(n, s)| {
        s.scope.as_deref().filter(|scope| within(scope, repository)).map(|scope| (n, scope.trim_end_matches('/').len()))
    }).collect();
    covering.sort_by_key(|(_, len)| std::cmp::Reverse(*len));
    let name = match name {
        Some(n) => {
            ensure!(covering.iter().any(|(m, _)| *m == n), "git secret {n} doesn't cover {repository} for you: its SCOPE must, and you need USAGE on it");
            n.to_string()
        }
        None => {
            let Some(&(best, len)) = covering.first() else {
                bail!("no git secret covers {repository}: an admin makes one (CREATE SECRET name (TYPE git, TOKEN '…', SCOPE '{repository}')) and grants USAGE on it");
            };
            let tied: Vec<&String> = covering.iter().filter(|(_, l)| *l == len).map(|(n, _)| *n).collect();
            ensure!(tied.len() == 1, "{} and {} cover {repository} alike: say which: secret => '…'", tied[0], tied[1]);
            best.clone()
        }
    };
    let values = ext::reveal(lake, &name).await?;
    let token = values.get("token").cloned().with_context(|| format!("git secret {name} has no TOKEN"))?;
    let provider = values.get("provider").cloned().unwrap_or_else(|| "github".into());
    Ok((name, token, provider))
}

/// Whether a scope covers a URL: the same, or below it at a `/` (`…/acme` covers `…/acme/sales`, not `…/acme-x`).
fn within(scope: &str, url: &str) -> bool {
    let scope = scope.trim_end_matches('/');
    url == scope || url.starts_with(&format!("{scope}/"))
}

async fn host_for(lake: &Lake, repository: &str, secret: Option<&str>) -> Result<Box<dyn Host>> {
    let (_, token, provider) = git_token(lake, repository, secret).await?;
    host(&provider, repository, &token)
}

// ---------------------------------------------------------------- the folder

/// git's id of a blob: SHA-1 of `blob <size>\0` and the bytes (what `git hash-object` prints).
pub fn blob_id(bytes: &[u8]) -> String {
    let mut ctx = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
    ctx.update(format!("blob {}\0", bytes.len()).as_bytes());
    ctx.update(bytes);
    ctx.finish().as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// `'sales'` or `'files/sales'`: the folder of the workspace it names. The workspace itself, and the
/// folders the platform keeps (versions, deploys), are refused.
pub fn folder_of(name: &str) -> Result<String> {
    let raw = name.trim().trim_matches('/');
    ensure!(!raw.is_empty() && raw != "files" && clean(raw), "{name}: a folder of the workspace ('sales'), not the workspace itself");
    let folder = under_files(raw);
    ensure!(![VERSIONS, APPLIES].iter().any(|d| format!("{folder}/").starts_with(d)), "{name}: the workspace keeps its own versions and deploys there");
    Ok(folder)
}

fn short(folder: &str) -> &str { folder.strip_prefix("files/").unwrap_or(folder) }

fn missing(e: &anyhow::Error) -> bool { matches!(e.downcast_ref::<object_store::Error>(), Some(object_store::Error::NotFound { .. })) }

/// The folder's files (paths relative to it, `/`), the record left out.
async fn listed(lake: &Lake, folder: &str) -> Result<Vec<String>> {
    let prefix = format!("{folder}/");
    let all: Vec<object_store::ObjectMeta> = lake.store.list(Some(&Path::from(prefix.as_str()))).try_collect().await?;
    Ok(all.into_iter().filter_map(|o| o.location.to_string().strip_prefix(prefix.as_str()).map(String::from)).filter(|p| p != RECORD).collect())
}

/// Each of the folder's files' blob ids, as they are now.
async fn ids_of(lake: &Lake, folder: &str) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for path in listed(lake, folder).await? {
        let (bytes, _) = lake.file(&format!("{folder}/{path}")).await?;
        out.insert(path, blob_id(&bytes));
    }
    Ok(out)
}

/// The record and the version it was read at (None: not a Git folder).
async fn record_of(lake: &Lake, folder: &str) -> Result<Option<(Record, String)>> {
    match lake.file(&format!("{folder}/{RECORD}")).await {
        Ok((bytes, version)) => Ok(Some((serde_json::from_slice(&bytes).with_context(|| format!("{folder}/{RECORD} doesn't read"))?, version))),
        Err(e) if missing(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

async fn need_record(lake: &Lake, folder: &str) -> Result<(Record, String)> {
    record_of(lake, folder).await?.with_context(|| format!("{folder} is not a Git folder: CALL git_checkout('{}', repository => …, branch => …) first", short(folder)))
}

/// Which paths differ from the record (`added`, `modified`, `deleted`), by path.
fn changes(recorded: &BTreeMap<String, String>, here: &BTreeMap<String, String>) -> Vec<(String, &'static str)> {
    let mut out: Vec<(String, &'static str)> = here.iter().filter_map(|(p, id)| match recorded.get(p) {
        None => Some((p.clone(), "added")),
        Some(old) => (old != id).then(|| (p.clone(), "modified")),
    }).collect();
    out.extend(recorded.keys().filter(|p| !here.contains_key(*p)).map(|p| (p.clone(), "deleted")));
    out.sort();
    out
}

/// `pondra.git_status`'s and `CALL git_status`'s rows: path, change, branch, commit.
async fn status(lake: &Lake, folder: &str) -> Result<Vec<[String; 4]>> {
    let (record, _) = need_record(lake, folder).await?;
    let here = ids_of(lake, folder).await?;
    Ok(changes(&record.files, &here).into_iter().map(|(p, c)| [p, c.to_string(), record.branch.clone(), record.commit.clone()]).collect())
}

/// The rows as a batch of four string columns (empty, with its columns, when there are none).
fn batch(rows: &[[String; 4]]) -> Result<RecordBatch> {
    let col = |i: usize| Arc::new(rows.iter().map(|r| Some(r[i].as_str())).collect::<StringArray>()) as ArrayRef;
    Ok(RecordBatch::try_from_iter([("path", col(0)), ("change", col(1)), ("branch", col(2)), ("commit", col(3))])?)
}

/// `pondra.git_status('sales')` in a query: the folder's rows, put in its place where SQL comes in
/// (as `vars::parameters_in` puts a file's parameters).
pub async fn status_in(lake: &Lake, sql: &str) -> Result<String> {
    static CALL: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\bpondra\.git_status\s*\(\s*'((?:[^']|'')*)'\s*\)").expect("a regex"));
    if !sql.as_bytes().windows(10).any(|w| w.eq_ignore_ascii_case(b"git_status")) || !CALL.is_match(sql) {
        return Ok(sql.to_string());
    }
    let mut out = String::new();
    let mut last = 0;
    for c in CALL.captures_iter(sql) {
        let m = c.get(0).expect("a match");
        let rows = status(lake, &folder_of(&c[1].replace("''", "'"))?).await?;
        out.push_str(&sql[last..m.start()]);
        out.push_str(&values(&rows));
        last = m.end();
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// The rows as a subquery of VALUES; with none, its columns and no rows.
fn values(rows: &[[String; 4]]) -> String {
    let lit = |s: &str| format!("CAST('{}' AS VARCHAR)", s.replace('\'', "''"));
    let none = "CAST(NULL AS VARCHAR)";
    let body = match rows.is_empty() {
        true => format!("({none}, {none}, {none}, {none})"),
        false => rows.iter().map(|r| format!("({})", r.iter().map(|s| lit(s)).collect::<Vec<_>>().join(", "))).collect::<Vec<_>>().join(", "),
    };
    let only = if rows.is_empty() { " WHERE false" } else { "" };
    format!("(SELECT * FROM (VALUES {body}) AS git_status(path, change, branch, commit){only})")
}

// ---------------------------------------------------------------- writing the folder

/// Keep a save as a version (`files::keep`): one that can't be kept is said, not failed.
async fn kept(lake: &Lake, path: &str, bytes: &[u8]) {
    if let Err(e) = crate::files::keep(lake, path, bytes).await {
        eprintln!("{path}: saved, but not kept as a version: {e:#}");
    }
}

/// Save a workspace file as `PUT /files` does: over its version when it exists, kept as a version.
async fn save(lake: &Lake, path: &str, bytes: Vec<u8>) -> Result<()> {
    match lake.version(path).await {
        Ok(v) => lake.replace(path, bytes.clone(), &v).await.map(drop)?,
        Err(e) if missing(&e) => lake.put(path, bytes.clone()).await?,
        Err(e) => return Err(e),
    }
    kept(lake, path, &bytes).await;
    Ok(())
}

/// Save the record over the version it was read at (`read_at`), so two commits of one folder at
/// once can't both record. The first record of a folder is made new (`None`).
async fn save_record(lake: &Lake, folder: &str, record: &Record, read_at: Option<&str>) -> Result<()> {
    let path = format!("{folder}/{RECORD}");
    let bytes = serde_json::to_vec_pretty(record)?;
    let written = match read_at {
        Some(v) => lake.replace(&path, bytes.clone(), v).await.map(drop),
        None => lake.put(&path, bytes.clone()).await,
    };
    if let Err(e) = &written {
        ensure!(!e.is::<Changed>(), "{folder} changed while it was committed: try again");
    }
    written?;
    kept(lake, &path, &bytes).await;
    Ok(())
}

/// Make the folder's files match `to`, given it matched `from`: the files that differ are written,
/// those `to` lacks are removed. A repository's own record is refused, never written over.
async fn move_files(lake: &Lake, host: &dyn Host, folder: &str, from: &BTreeMap<String, String>, to: &BTreeMap<String, String>) -> Result<()> {
    ensure!(!to.contains_key(RECORD), "the repository has a file named {RECORD}: the folder's own record");
    if let Some(p) = to.keys().find(|p| !clean(p)) {
        bail!("the repository names a file outside its folder: {p}");
    }
    for (path, id) in to.iter().filter(|(p, id)| from.get(*p) != Some(*id)) {
        save(lake, &format!("{folder}/{path}"), host.blob(id.as_str()).await?).await?;
    }
    for path in from.keys().filter(|p| !to.contains_key(*p)) {
        match lake.remove_file(&format!("{folder}/{path}")).await {
            Err(e) if !missing(&e) => return Err(e),
            _ => {} // (already gone: deleted here too)
        }
    }
    Ok(())
}

/// Up to five paths, by name: `a.sql, b.sql and 3 more`.
fn names(changed: &[(String, &str)]) -> String {
    let mut s = changed.iter().take(5).map(|(p, _)| p.as_str()).collect::<Vec<_>>().join(", ");
    if changed.len() > 5 {
        s += &format!(" and {} more", changed.len() - 5);
    }
    s
}

fn moved_on(record: &Record, folder: &str) -> String {
    format!("{} moved on since {} (the folder's commit): git_pull('{}') first", record.branch, record.commit.get(..7).unwrap_or(record.commit.as_str()), short(folder))
}

/// A branch's name as git allows it and a URL names it: parts of letters, digits and `- _ .`, none
/// starting with `.`, joined by `/`.
fn branch_ok(b: &str) -> bool { !b.contains("..") && b.split('/').all(|p| !p.is_empty() && !p.starts_with('.') && p.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))) }

/// A procedure's text argument, if it was given.
fn text(v: &Value, key: &str) -> Option<String> { v[key].as_str().map(String::from) }

/// `paths => ['a.sql']` (a list, or one path as text).
fn paths_of(v: &Value) -> Option<Vec<String>> {
    match &v["paths"] {
        Value::Array(a) => Some(a.iter().filter_map(|p| p.as_str().map(String::from)).collect()),
        Value::String(s) => Some(vec![s.clone()]),
        _ => None,
    }
}

// ---------------------------------------------------------------- the procedures

/// `CALL git_checkout('sales', …)` and the other four (with `pondra.` in front, any case).
pub fn is_own(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(lower.strip_prefix("pondra.").unwrap_or(lower.as_str()), "git_checkout" | "git_status" | "git_commit" | "git_pull" | "git_pull_request")
}

/// The named arguments each procedure takes (`secret` is every one's).
fn named_by(verb: &str) -> &'static [&'static str] {
    match verb {
        "git_checkout" => &["repository", "branch", "secret"],
        "git_commit" => &["message", "paths", "secret"],
        "git_pull_request" => &["title", "base", "body", "secret"],
        _ => &["secret"],
    }
}

/// Runs one of the five as its caller: the folder by position, the rest by name (`git_commit`'s
/// message may come second, as in `git_commit('sales', 'Discounts by channel')`).
pub async fn call(app: &App, name: &str, args: &[FunctionArg], who: Who) -> Result<Outcome> {
    let lower = name.to_ascii_lowercase();
    let verb = lower.rsplit('.').next().unwrap_or_default();
    let a = crate::routines::args(name, args)?;
    let given = &a.given;
    let message_second = verb == "git_commit";
    ensure!(given.len() == 1 || (message_second && given.len() == 2), "{name}('sales'{}): the folder by position, the rest by name", if message_second { ", 'message'" } else { "" });
    if let Some((n, _)) = a.named.iter().find(|(n, _)| !named_by(verb).contains(&n.as_str())) {
        bail!("{name} has no {n}: by name it takes {}", named_by(verb).join(", "));
    }
    let mut select = vec![format!("CAST(({}) AS VARCHAR) AS folder", given[0])];
    if given.len() == 2 {
        select.push(format!("CAST(({}) AS VARCHAR) AS message", given[1]));
    }
    select.extend(a.named.iter().map(|(n, e)| format!("({e}) AS {n}")));
    let rows = app.query(&format!("SELECT {}", select.join(", ")), None).await?;
    let v = crate::apply::rows_json(&rows, 1)?.into_iter().next().unwrap_or_default();
    let folder = folder_of(v["folder"].as_str().unwrap_or_default())?;
    if verb != "git_status" {
        ensure!(who.role >= Role::Write, "{name} writes the workspace's files: it needs a write token");
    }
    let (lake, secret) = (&app.lake, text(&v, "secret"));
    Ok(match verb {
        "git_status" => Outcome::Rows(vec![batch(&status(lake, &folder).await?)?]),
        "git_checkout" => Outcome::Done(checkout(lake, &folder, &v, secret).await?),
        "git_commit" => Outcome::Done(commit(lake, &folder, &v, secret).await?),
        "git_pull" => Outcome::Done(pull(lake, &folder, secret).await?),
        _ => Outcome::Done(pull_request(lake, &folder, &v, secret).await?),
    })
}

/// `git_checkout`: the first checkout of a folder, or (a Git folder) its switch to another branch.
async fn checkout(lake: &Lake, folder: &str, v: &Value, secret: Option<String>) -> Result<Value> {
    let repository = text(v, "repository").map(|r| normal(&r));
    let branch = text(v, "branch");
    if let Some(b) = &branch {
        ensure!(branch_ok(b), "{b}: a branch's name is letters, digits and - _ . /");
    }
    match record_of(lake, folder).await? {
        None => first(lake, folder, repository, branch, secret).await,
        Some((record, read_at)) => switch(lake, folder, record, &read_at, repository, branch, secret).await,
    }
}

/// The first checkout: the branch's files, then the record (written last).
async fn first(lake: &Lake, folder: &str, repository: Option<String>, branch: Option<String>, secret: Option<String>) -> Result<Value> {
    let repository = repository.context("the first checkout names its repository: git_checkout('sales', repository => 'https://github.com/acme/sales')")?;
    ensure!(listed(lake, folder).await?.is_empty(), "{folder} holds files already: check a repository out into an empty folder");
    let host = host_for(lake, &repository, secret.as_deref()).await?;
    let branch = match branch {
        Some(b) => b,
        None => host.default_branch().await?,
    };
    let commit = host.head(&branch).await?.with_context(|| format!("{repository} has no branch {branch}"))?;
    let files = host.files(&commit).await?;
    move_files(lake, host.as_ref(), folder, &BTreeMap::new(), &files).await?;
    let n = files.len();
    let record = Record { repository: repository.clone(), branch: branch.clone(), commit: commit.clone(), files };
    save_record(lake, folder, &record, None).await?;
    Ok(json!({"folder": folder, "repository": repository, "branch": branch, "commit": commit, "files": n}))
}

/// A Git folder's checkout: the same branch answers unchanged; another repository is refused; another
/// branch is checked out over the folder (refused with local changes), or made at the record's commit
/// when the repository lacks it, the local changes carried over as `git switch -c` does.
async fn switch(lake: &Lake, folder: &str, mut record: Record, read_at: &str, repository: Option<String>, branch: Option<String>, secret: Option<String>) -> Result<Value> {
    if let Some(r) = &repository {
        ensure!(*r == record.repository, "{folder} is a Git folder of {}: check {r} out into another folder", record.repository);
    }
    let branch = branch.unwrap_or_else(|| record.branch.clone());
    if branch == record.branch {
        return Ok(json!({"folder": folder, "branch": branch, "unchanged": true}));
    }
    let host = host_for(lake, &record.repository, secret.as_deref()).await?;
    let made = match host.head(&branch).await? {
        Some(commit) => {
            let local = changes(&record.files, &ids_of(lake, folder).await?);
            ensure!(local.is_empty(), "{folder} has changes not committed: {}: git_commit them or put them back first", names(&local));
            let files = host.files(&commit).await?;
            move_files(lake, host.as_ref(), folder, &record.files, &files).await?;
            record.files = files;
            record.commit = commit;
            false
        }
        None => {
            host.make_branch(&branch, &record.commit).await?;
            true
        }
    };
    record.branch = branch;
    save_record(lake, folder, &record, Some(read_at)).await?;
    Ok(json!({"folder": folder, "repository": record.repository, "branch": record.branch, "commit": record.commit, "files": record.files.len(), "made": made}))
}

/// `git_commit`: the folder's local changes (or only `paths`) on top of the record's commit, moved
/// onto the branch, fast-forward only. The record then holds the new commit and the committed paths' ids.
async fn commit(lake: &Lake, folder: &str, v: &Value, secret: Option<String>) -> Result<Value> {
    let message = text(v, "message").context("git_commit('sales', 'what changed'): the message")?;
    let (mut record, read_at) = need_record(lake, folder).await?;
    let local = changes(&record.files, &ids_of(lake, folder).await?);
    let picked = match paths_of(v) {
        None => local,
        Some(paths) => {
            if let Some(p) = paths.iter().find(|p| !local.iter().any(|(q, _)| q == *p)) {
                bail!("{p} isn't changed in {folder}");
            }
            local.into_iter().filter(|(q, _)| paths.contains(q)).collect()
        }
    };
    ensure!(!picked.is_empty(), "nothing to commit in {folder}");
    let host = host_for(lake, &record.repository, secret.as_deref()).await?;
    let head = host.head(&record.branch).await?.with_context(|| format!("{} has no branch {}", record.repository, record.branch))?;
    ensure!(head == record.commit, "{}", moved_on(&record, folder));
    let mut blobs = vec![];
    for (path, change) in picked {
        let bytes = match change {
            "deleted" => None,
            _ => Some(lake.file(&format!("{folder}/{path}")).await?.0.to_vec()),
        };
        blobs.push((path, bytes));
    }
    let Some(commit) = host.commit_on(&record.branch, &record.commit, &message, &blobs).await? else {
        bail!("{}", moved_on(&record, folder));
    };
    for (path, bytes) in &blobs {
        match bytes {
            Some(b) => record.files.insert(path.clone(), blob_id(b)),
            None => record.files.remove(path),
        };
    }
    record.commit = commit.clone();
    save_record(lake, folder, &record, Some(&read_at)).await?;
    Ok(json!({"commit": commit, "files": blobs.len()}))
}

/// `git_pull`: the branch's new commits written into the folder. A path changed both here and on the
/// branch (to different things) is refused by name, and nothing changes.
async fn pull(lake: &Lake, folder: &str, secret: Option<String>) -> Result<Value> {
    let (mut record, read_at) = need_record(lake, folder).await?;
    let host = host_for(lake, &record.repository, secret.as_deref()).await?;
    let head = host.head(&record.branch).await?.with_context(|| format!("{} has no branch {}", record.repository, record.branch))?;
    if head == record.commit {
        return Ok(json!({"pulled": 0}));
    }
    let theirs = host.files(&head).await?;
    let here = ids_of(lake, folder).await?;
    let all: BTreeSet<&String> = record.files.keys().chain(theirs.keys()).collect();
    let (mut pulled, mut clash) = (0, vec![]);
    for p in all {
        let (base, there, mine) = (record.files.get(p), theirs.get(p), here.get(p));
        if base != there {
            pulled += 1;
            if mine != base && mine != there {
                clash.push(p.clone());
            }
        }
    }
    ensure!(clash.is_empty(), "changed here and on {}: {}: commit or undo one side, then pull", record.branch, clash.join(", "));
    move_files(lake, host.as_ref(), folder, &record.files, &theirs).await?;
    record.files = theirs;
    record.commit = head.clone();
    save_record(lake, folder, &record, Some(&read_at)).await?;
    Ok(json!({"pulled": pulled, "commit": head}))
}

/// `git_pull_request`: a pull request from the record's branch into `base` (the repository's default
/// branch by default).
async fn pull_request(lake: &Lake, folder: &str, v: &Value, secret: Option<String>) -> Result<Value> {
    let title = text(v, "title").context("git_pull_request('sales', title => '…')")?;
    let (record, _) = need_record(lake, folder).await?;
    let host = host_for(lake, &record.repository, secret.as_deref()).await?;
    let base = match text(v, "base") {
        Some(b) => b,
        None => host.default_branch().await?,
    };
    let body = text(v, "body").unwrap_or_default();
    let (number, url) = host.pull_request(&title, &record.branch, &base, &body).await?;
    Ok(json!({"number": number, "url": url}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blob_has_gits_id() {
        assert_eq!(blob_id(b"hello\n"), "ce013625030ba8dba906f756967f9e9ca394464a");
    }

    #[test]
    fn a_repository_is_reached_at_its_api() {
        let api = |u: &str| api_of(u).unwrap();
        assert_eq!(api("https://github.com/acme/sales"), ("https://api.github.com".into(), "acme/sales".into()));
        assert_eq!(api("http://127.0.0.1:9990/acme/sales.git/"), ("http://127.0.0.1:9990/api/v3".into(), "acme/sales".into()));
        assert!(api_of("http://github.com/acme/sales").is_err()); // (a token over https only)
        assert!(api_of("https://github.com/acme").is_err());
        assert!(api_of("http://127.0.0.1.evil.com/acme/sales").is_err()); // (this machine by name, not by prefix)
        assert!(api_of("https://github.com/acme/..").is_err());
        assert!(api_of("https://x@github.com/acme/sales").is_err());
        assert_eq!(api("http://[::1]:9990/acme/.github").1, "acme/.github");
    }

    #[test]
    fn a_scope_covers_its_own_repositories_only() {
        assert!(within("https://github.com/acme", "https://github.com/acme/sales"));
        assert!(!within("https://github.com/acme", "https://github.com/acme-x/sales"));
    }

    #[test]
    fn local_changes_are_added_modified_and_deleted() {
        let rec = BTreeMap::from([("a.sql".to_string(), "1".to_string()), ("b.sql".into(), "2".into())]);
        let here = BTreeMap::from([("a.sql".to_string(), "9".to_string()), ("c.sql".into(), "3".into())]);
        assert_eq!(changes(&rec, &here), vec![("a.sql".to_string(), "modified"), ("b.sql".into(), "deleted"), ("c.sql".into(), "added")]);
    }

    #[test]
    fn folders_are_the_workspace_s_own() {
        assert_eq!(folder_of("sales").unwrap(), "files/sales");
        assert_eq!(folder_of("files/sales/").unwrap(), "files/sales");
        assert!(folder_of("files").is_err());
        assert!(folder_of("files/.versions/x").is_err());
        assert!(folder_of("sales/../t").is_err());
    }
}
