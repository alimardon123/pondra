//! `pondra`, or `pondra <lake>`: a SQL shell on a lake (`./lake` unless another folder or
//! `s3://bucket/prefix` is named), DuckDB-style. It runs a node on the lake — this same binary,
//! in the background — so views, tasks and windows run while it is open, other nodes can join,
//! and the lake is left as any node leaves it. Each statement goes to that node. The other lakes
//! in the current folder are attached as its databases, for as long as it runs (ADR-024).
use anyhow::{bail, Result};
use std::io::{BufRead, IsTerminal, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub async fn run(dir: &str) -> Result<()> {
    let (mut node, base, key, log) = start(dir)?;
    let r = session(dir, &base, &key, &mut node, &log).await;
    stop(&mut node); // (whatever happened: the node never outlives the shell)
    if r? {
        std::process::exit(1); // (a script piped in had a statement fail: say so, as DuckDB's shell does)
    }
    Ok(())
}

/// A node on `dir`, here, for this program alone: it, its address, the key that lets this program's
/// SQL read files on this machine (`FROM 'D:\data\jan.csv'`, as DuckDB's shell does), its log.
pub(crate) fn start(dir: &str) -> Result<(Child, String, String, std::path::PathBuf)> {
    if !dir.contains("://") {
        std::fs::create_dir_all(dir)?;
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let log = std::env::temp_dir().join(format!("pondra-shell-{port}.log"));
    let key = uuid::Uuid::new_v4().to_string();
    let here = std::env::current_dir()?.to_string_lossy().to_string(); // (its lakes: this one's databases)
    let node = Command::new(std::env::current_exe()?)
        .args(["serve", "--dir", dir, "--addr", &format!("127.0.0.1:{port}"), "--stop-with-stdin", "--attach-found", &here, "--python", "auto"])
        .env("PONDRA_OWNER_KEY", &key)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log)?)
        .spawn()?;
    Ok((node, format!("http://127.0.0.1:{port}"), key, log))
}

/// Only ever this machine's own node, over plain HTTP: never through a proxy the environment names
/// (it couldn't reach the node), and no CA certificates needed (minimal images have none).
pub(crate) async fn up(base: &str, node: &mut Child, log: &Path) -> Result<reqwest::Client> {
    let (http, started) = (reqwest::Client::builder().no_proxy().tls_certs_only([]).build()?, Instant::now());
    while http.get(format!("{base}/stats")).send().await.is_err() {
        if node.try_wait()?.is_some() || started.elapsed() > Duration::from_secs(120) {
            bail!("the node didn't start: {}", std::fs::read_to_string(log).unwrap_or_default());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(http)
}

/// `pondra run`: a script (`{"sql", "params"}`) on a node started for it, or on `url`; its answer
/// (rows as a table).
pub async fn script(dir: &str, url: Option<&str>, token: Option<&str>, body: serde_json::Value) -> Result<String> {
    let send = |http: reqwest::Client, base: String, key: String| async move {
        let mut r = http.post(format!("{base}/sql?format=table")).header("x-pondra-owner", key).json(&body);
        if let Some(t) = token {
            r = r.bearer_auth(t);
        }
        let r = r.send().await?;
        let (ok, heard, text) = (r.status().is_success(), notices(r.headers()), r.text().await?);
        anyhow::ensure!(ok, "{}{}", heard, text.trim());
        Ok(format!("{heard}{}\n", text.trim_end()))
    };
    if let Some(url) = url {
        return send(reqwest::Client::new(), url.trim_end_matches('/').to_string(), String::new()).await;
    }
    let (mut node, base, key, log) = start(dir)?;
    let r = match up(&base, &mut node, &log).await {
        Ok(http) => send(http, base, key).await,
        Err(e) => Err(e),
    };
    stop(&mut node);
    r
}

/// What the procedures a statement called printed (`x-pondra-notices`), a line each, to print
/// before its answer.
fn notices(headers: &reqwest::header::HeaderMap) -> String {
    let said: Vec<String> = headers.get("x-pondra-notices").and_then(|h| serde_json::from_slice(h.as_bytes()).ok()).unwrap_or_default();
    said.iter().map(|n| format!("{n}\n")).collect()
}

/// `--name value` (or `--name=value`) pairs: numbers and true/false as such, the rest as text.
pub fn params(args: &[String]) -> Result<serde_json::Map<String, serde_json::Value>> {
    let (mut out, mut it) = (serde_json::Map::new(), args.iter());
    while let Some(a) = it.next() {
        let Some(name) = a.strip_prefix("--") else { bail!("{a}: parameters are --name value") };
        let (name, value) = match name.split_once('=') {
            Some((n, v)) => (n.to_string(), v.to_string()),
            None => (name.to_string(), it.next().ok_or_else(|| anyhow::anyhow!("--{name} needs a value"))?.clone()),
        };
        let v = serde_json::from_str::<serde_json::Value>(&value).ok().filter(|v| v.is_number() || v.is_boolean()).unwrap_or(serde_json::Value::String(value));
        out.insert(name.replace('-', "_"), v);
    }
    Ok(out)
}

const DATABASES: &str = "SELECT DISTINCT catalog_name AS database FROM information_schema.schemata ORDER BY 1";

/// This lake and the ones attached to it, by name.
async fn databases(http: &reqwest::Client, base: &str) -> Result<Vec<String>> {
    let rows: Vec<serde_json::Value> = http.post(format!("{base}/sql")).body(DATABASES).send().await?.json().await?;
    Ok(rows.iter().filter_map(|r| r["database"].as_str().map(str::to_string)).collect())
}

/// The shell's statements, until its input ends; true if one failed while reading a script (not a
/// terminal: there, an error is just the answer).
async fn session(dir: &str, base: &str, key: &str, node: &mut Child, log: &Path) -> Result<bool> {
    let http = up(base, node, log).await?;
    let tty = std::io::stdin().is_terminal();
    if tty {
        let others = databases(&http, base).await.unwrap_or_default();
        let others = if others.len() > 1 { format!(" Databases: {}.", others.join(", ")) } else { String::new() };
        eprintln!("Pondra {} on {dir}, also at {base}.{others} End each statement with ;  .tables and .databases list them, .quit leaves.", env!("CARGO_PKG_VERSION"));
        eprintln!("The console, as this shell: {base}/#key={key}"); // (its key: the page then reads this machine's files as the shell does, and only while the shell runs)
    }
    let (mut sql, mut failed) = (String::new(), false);
    loop {
        if tty {
            eprint!("{}", if sql.is_empty() { "pondra> " } else { "   ...> " });
            std::io::stderr().flush().ok();
        }
        let mut line = String::new();
        let end = std::io::stdin().lock().read_line(&mut line)? == 0;
        match (sql.is_empty(), line.trim()) {
            (true, ".quit" | ".exit" | "\\q") => break,
            (true, ".databases") => line = format!("{DATABASES};"),
            // (`pondra.tables`: a materialized view says so, which `information_schema.tables` can't)
            (true, ".tables") => line = "SELECT lake, schema, name, kind FROM pondra.tables ORDER BY 1, 2, 3;".into(),
            _ => {}
        }
        sql.push_str(&line);
        let (statements, rest) = if end { (vec![std::mem::take(&mut sql)], String::new()) } else { crate::routines::statements(&sql) }; // (the last may lack its ;)
        sql = rest;
        for statement in statements.iter().filter(|s| !s.trim().is_empty()) {
            let at = Instant::now();
            let answer = match http.post(format!("{base}/sql?format=table")).header("x-pondra-owner", key).body(statement.trim().to_string()).send().await {
                Ok(r) => Ok((r.status().is_success(), notices(r.headers()), r.text().await.unwrap_or_default())),
                Err(_) if node.try_wait()?.is_some() => bail!("the node stopped: {}", std::fs::read_to_string(log).unwrap_or_default()),
                Err(e) => Err(e),
            };
            let took = at.elapsed(); // (the answer's time, not the terminal's: printing is timed apart)
            let mut out = std::io::stdout().lock();
            match answer {
                Ok((true, heard, body)) => writeln!(out, "{heard}{}", body.trim_end())?, // (one write: consoles are slow per call)
                Ok((false, heard, body)) => {
                    write!(out, "{heard}")?;
                    eprintln!("Error: {}", body.trim());
                    failed = true;
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    failed = true;
                }
            }
            out.flush()?;
            if tty {
                eprintln!("({:.3} s)", took.as_secs_f64());
            }
        }
        if end {
            break;
        }
    }
    Ok(failed && !tty)
}

/// Stop the node by closing its input (`--stop-with-stdin`): it hands the lake on at once, rather
/// than after the lease a killed leader leaves. (It would stop the same way if this shell died.)
pub(crate) fn stop(node: &mut Child) {
    drop(node.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(10);
    while node.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = node.kill();
    let _ = node.wait();
}

