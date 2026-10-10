//! `pondra workspace pull | push <dir>` (ADR-035 §9): the lake's files (`files/`) kept in a folder
//! of your own, for git and your editor. Each side's changes since the last sync go to the other;
//! a file changed on both sides is a conflict, listed and never overwritten. What the folder last
//! agreed with the lake is `<dir>/.pondra/workspace.json`: each file's version there (its `etag`,
//! which `PUT … If-Match` checks, invariant 148) and its contents' checksum here. Names with a part
//! starting with `.` (`.git`, `.pondra`, the lake's `.versions`) stay on their own side.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(clap::Subcommand)]
pub enum Command {
    /// The lake's files into DIR: new and changed ones written, deleted ones removed, unless the
    /// folder changed them too.
    Pull {
        /// The folder (made if it isn't there).
        dir: String,
        #[command(flatten)]
        at: At,
    },
    /// DIR's changes into the lake: each file sent only if the lake still has the version last
    /// pulled, files deleted here deleted there.
    Push {
        /// The folder last pulled into.
        dir: String,
        #[command(flatten)]
        at: At,
    },
}

#[derive(clap::Args)]
pub struct At {
    /// The lake (a folder or s3://bucket/prefix; default ./lake), unless --url.
    lake: Option<String>,
    /// A node already running (http://host:8080).
    #[arg(long)]
    url: Option<String>,
    /// A token for that node (also PONDRA_TOKEN).
    #[arg(long)]
    token: Option<String>,
}

pub async fn command(cmd: Command) -> Result<()> {
    let (Command::Pull { dir, at } | Command::Push { dir, at }) = &cmd;
    anyhow::ensure!(at.lake.is_none() || at.url.is_none(), "sync with a lake folder or with a node (--url), not both");
    let token = at.token.clone().or_else(|| std::env::var("PONDRA_TOKEN").ok());
    let push = matches!(cmd, Command::Push { .. });
    let said = match &at.url {
        Some(url) => Node { http: reqwest::Client::new(), base: url.trim_end_matches('/').into(), token }.sync(Path::new(dir), push).await,
        None => {
            let (mut child, base, _, log) = crate::shell::start(at.lake.as_deref().unwrap_or("lake"))?;
            let r = match crate::shell::up(&base, &mut child, &log).await {
                Ok(http) => Node { http, base, token }.sync(Path::new(dir), push).await,
                Err(e) => Err(e),
            };
            crate::shell::stop(&mut child);
            r
        }
    }?;
    println!("{said}");
    Ok(())
}

/// What the folder and the lake last agreed on, for one file.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
struct Synced {
    version: String,
    sum: String,
}

struct Node {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl Node {
    fn req(&self, method: reqwest::Method, rel: &str) -> reqwest::RequestBuilder {
        let mut url = reqwest::Url::parse(&self.base).expect("a node's address");
        url.path_segments_mut().expect("an http address").push("files").extend(rel.split('/'));
        let r = self.http.request(method, url);
        match &self.token {
            Some(t) => r.bearer_auth(t),
            None => r,
        }
    }

    /// The lake's files, by their path under `files/`.
    async fn list(&self) -> Result<Vec<String>> {
        let mut r = self.http.post(format!("{}/sql", self.base)).json(&json!({"sql": "SELECT path FROM files()"}));
        if let Some(t) = &self.token {
            r = r.bearer_auth(t);
        }
        let r = r.send().await?;
        anyhow::ensure!(r.status().is_success(), "listing the lake's files: {}", r.text().await?.trim());
        let rows: Vec<Value> = r.json().await?;
        Ok(rows.iter().filter_map(|row| row["path"].as_str()?.strip_prefix("files/").map(String::from)).filter(|p| !hidden(p)).collect())
    }

    /// A file's bytes and version, or None if the lake hasn't it.
    async fn get(&self, rel: &str) -> Result<Option<(Vec<u8>, String)>> {
        let r = self.req(reqwest::Method::GET, rel).send().await?;
        if r.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        anyhow::ensure!(r.status().is_success(), "{rel}: {}", r.text().await?.trim());
        let version = r.headers().get("etag").and_then(|v| v.to_str().ok()).unwrap_or_default().trim_matches('"').to_string();
        Ok(Some((r.bytes().await?.to_vec(), version)))
    }

    /// Put `bytes` as `rel`, replacing `over` (its version) or only if it isn't there: the new
    /// version, or None when the lake's file isn't the one expected.
    async fn put(&self, rel: &str, bytes: Vec<u8>, over: Option<&str>) -> Result<Option<String>> {
        let mut r = self.req(reqwest::Method::PUT, rel).body(bytes);
        if let Some(v) = over {
            r = r.header("if-match", format!("\"{v}\""));
        }
        let r = r.send().await?;
        if matches!(r.status().as_u16(), 409 | 412) {
            return Ok(None);
        }
        anyhow::ensure!(r.status().is_success(), "{rel}: {}", r.text().await?.trim());
        let said: Value = r.json().await?;
        Ok(Some(said["version"].as_str().unwrap_or_default().to_string()))
    }

    async fn sync(&self, dir: &Path, push: bool) -> Result<String> {
        let at = dir.join(".pondra").join("workspace.json");
        let mut was: BTreeMap<String, Synced> = match std::fs::read(&at) {
            Ok(b) => serde_json::from_slice::<Value>(&b).ok().and_then(|v| serde_json::from_value(v["files"].clone()).ok()).unwrap_or_default(),
            Err(_) => BTreeMap::new(),
        };
        let here = local(dir)?;
        let (mut moved, mut gone, mut conflicts) = (0, 0, vec![]);
        if push {
            for (rel, bytes) in &here {
                let sum = sum(bytes);
                let last = was.get(rel).cloned();
                if last.as_ref().is_some_and(|s| s.sum == sum) {
                    continue; // (unchanged here)
                }
                match self.put(rel, bytes.clone(), last.as_ref().map(|s| s.version.as_str())).await? {
                    Some(version) => {
                        was.insert(rel.clone(), Synced { version, sum });
                        moved += 1;
                    }
                    // (there already, or changed there: the same bytes are no conflict)
                    None => match self.get(rel).await? {
                        Some((there, version)) if there == *bytes => drop(was.insert(rel.clone(), Synced { version, sum })),
                        _ => conflicts.push(format!("{rel}: changed in the lake too (pull, then push)")),
                    },
                }
            }
            for (rel, last) in was.clone() {
                if here.contains_key(&rel) {
                    continue;
                }
                match self.get(&rel).await? {
                    Some((_, version)) if version != last.version => conflicts.push(format!("{rel}: deleted here, changed in the lake")),
                    Some(_) => {
                        let r = self.req(reqwest::Method::DELETE, &rel).send().await?;
                        anyhow::ensure!(r.status().is_success(), "{rel}: {}", r.text().await?.trim());
                        was.remove(&rel);
                        gone += 1;
                    }
                    None => drop(was.remove(&rel)),
                }
            }
        } else {
            let there = self.list().await?;
            for rel in &there {
                let mine = here.get(rel).map(|b| sum(b));
                let last = was.get(rel);
                let Some((bytes, version)) = self.get(rel).await? else { continue };
                if last.is_some_and(|s| s.version == version) {
                    continue; // (unchanged there)
                }
                let theirs = sum(&bytes);
                if mine.as_ref() == Some(&theirs) {
                    was.insert(rel.clone(), Synced { version, sum: theirs }); // (the same on both sides)
                } else if mine.is_some() && mine.as_ref() != last.map(|s| &s.sum) {
                    conflicts.push(format!("{rel}: changed here and in the lake"));
                } else {
                    let file = path(dir, rel);
                    std::fs::create_dir_all(file.parent().unwrap_or(dir))?;
                    std::fs::write(&file, &bytes).with_context(|| file.display().to_string())?;
                    was.insert(rel.clone(), Synced { version, sum: theirs });
                    moved += 1;
                }
            }
            for (rel, last) in was.clone() {
                if there.contains(&rel) {
                    continue;
                }
                match here.get(&rel) {
                    Some(b) if sum(b) != last.sum => conflicts.push(format!("{rel}: changed here, deleted in the lake")),
                    Some(_) => {
                        std::fs::remove_file(path(dir, &rel))?;
                        was.remove(&rel);
                        gone += 1;
                    }
                    None => drop(was.remove(&rel)),
                }
            }
        }
        std::fs::create_dir_all(at.parent().unwrap_or(dir))?;
        std::fs::write(&at, serde_json::to_vec_pretty(&json!({"files": was}))?)?;
        let said = if push { format!("pushed {moved}, deleted {gone} in the lake") } else { format!("pulled {moved}, removed {gone} here") };
        if !conflicts.is_empty() {
            bail!("{said}; {} left as they are:\n  {}", conflicts.len(), conflicts.join("\n  "));
        }
        Ok(said)
    }
}

/// A name the sync leaves alone: one with a part starting with `.`.
fn hidden(rel: &str) -> bool {
    rel.split('/').any(|p| p.starts_with('.'))
}

fn path(dir: &Path, rel: &str) -> PathBuf {
    rel.split('/').fold(dir.to_path_buf(), |p, part| p.join(part))
}

fn sum(bytes: &[u8]) -> String {
    format!("{:016x}", crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, bytes))
}

/// The folder's files, by their path with `/`, and their bytes.
fn local(dir: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut out = BTreeMap::new();
    let mut todo = vec![dir.to_path_buf()];
    while let Some(d) = todo.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else { continue };
        for e in entries {
            let e = e?;
            if e.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            if e.file_type()?.is_dir() {
                todo.push(e.path());
            } else {
                let rel = e.path().strip_prefix(dir)?.components().map(|c| c.as_os_str().to_string_lossy().into_owned()).collect::<Vec<_>>().join("/");
                out.insert(rel, std::fs::read(e.path())?);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #[test]
    fn hidden_names() {
        assert!(super::hidden(".versions/a.sql/1.x") && super::hidden("etl/.git/HEAD"));
        assert!(!super::hidden("etl/orders.sql"));
    }
}
