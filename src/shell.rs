//! `pondra`, or `pondra <lake>`: a SQL shell on a lake (`./lake` unless another folder or
//! `s3://bucket/prefix` is named), DuckDB-style. It runs a node on the lake — this same binary,
//! in the background — so views, tasks and windows run while it is open, other nodes can join,
//! and the lake is left as any node leaves it. Each statement goes to that node.
use anyhow::{bail, Result};
use std::io::{BufRead, IsTerminal, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub async fn run(dir: &str) -> Result<()> {
    let (mut node, base, key, log) = start(dir)?;
    let r = session(dir, &base, &key, &mut node, &log).await;
    stop(&mut node); // (whatever happened: the node never outlives the shell)
    r
}

/// A node on `dir`, here, for this program alone: it, its address, the key that lets this program's
/// SQL read files on this machine (`FROM 'D:\data\jan.csv'`, as DuckDB's shell does), its log.
fn start(dir: &str) -> Result<(Child, String, String, std::path::PathBuf)> {
    if !dir.contains("://") {
        std::fs::create_dir_all(dir)?;
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let log = std::env::temp_dir().join(format!("pondra-shell-{port}.log"));
    let key = uuid::Uuid::new_v4().to_string();
    let node = Command::new(std::env::current_exe()?)
        .args(["serve", "--dir", dir, "--addr", &format!("127.0.0.1:{port}"), "--stop-with-stdin"])
        .env("PONDRA_OWNER_KEY", &key)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log)?)
        .spawn()?;
    Ok((node, format!("http://127.0.0.1:{port}"), key, log))
}

/// Only ever this machine's own node, over plain HTTP: never through a proxy the environment names
/// (it couldn't reach the node), and no CA certificates needed (minimal images have none).
async fn up(base: &str, node: &mut Child, log: &Path) -> Result<reqwest::Client> {
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
        let (ok, text) = (r.status().is_success(), r.text().await?);
        anyhow::ensure!(ok, "{}", text.trim());
        Ok(format!("{}\n", text.trim_end()))
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

async fn session(dir: &str, base: &str, key: &str, node: &mut Child, log: &Path) -> Result<()> {
    let http = up(base, node, log).await?;
    let tty = std::io::stdin().is_terminal();
    if tty {
        eprintln!("Pondra {} on {dir}, also at {base}. End each statement with ;  .tables and .databases list them, .quit leaves.", env!("CARGO_PKG_VERSION"));
    }
    let mut sql = String::new();
    loop {
        if tty {
            eprint!("{}", if sql.is_empty() { "pondra> " } else { "   ...> " });
            std::io::stderr().flush().ok();
        }
        let mut line = String::new();
        let end = std::io::stdin().lock().read_line(&mut line)? == 0;
        match (sql.is_empty(), line.trim()) {
            (true, ".quit" | ".exit" | "\\q") => break,
            (true, ".databases") => line = "SELECT DISTINCT catalog_name AS database FROM information_schema.schemata ORDER BY 1;".into(),
            // (an attached lake's tables are also this lake's schema of its name, so `l2.t` works: listed once)
            (true, ".tables") => line = "SELECT table_catalog AS lake, table_schema AS schema, table_name AS name, table_type AS kind FROM information_schema.tables WHERE table_schema <> 'information_schema' AND table_schema NOT IN (SELECT catalog_name FROM information_schema.schemata) ORDER BY 1, 2, 3;".into(),
            _ => {}
        }
        sql.push_str(&line);
        let (statements, rest) = if end { (vec![std::mem::take(&mut sql)], String::new()) } else { crate::routines::statements(&sql) }; // (the last may lack its ;)
        sql = rest;
        for statement in statements.iter().filter(|s| !s.trim().is_empty()) {
            let at = Instant::now();
            let answer = match http.post(format!("{base}/sql?format=table")).header("x-pondra-owner", key).body(statement.trim().to_string()).send().await {
                Ok(r) => Ok((r.status().is_success(), r.text().await.unwrap_or_default())),
                Err(_) if node.try_wait()?.is_some() => bail!("the node stopped: {}", std::fs::read_to_string(log).unwrap_or_default()),
                Err(e) => Err(e),
            };
            let took = at.elapsed(); // (the answer's time, not the terminal's: printing is timed apart)
            let mut out = std::io::stdout().lock();
            match answer {
                Ok((true, body)) => writeln!(out, "{}", body.trim_end())?, // (one write: consoles are slow per call)
                Ok((false, body)) => eprintln!("Error: {}", body.trim()),
                Err(e) => eprintln!("Error: {e}"),
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
    Ok(())
}

/// Stop the node by closing its input (`--stop-with-stdin`): it hands the lake on at once, rather
/// than after the lease a killed leader leaves. (It would stop the same way if this shell died.)
fn stop(node: &mut Child) {
    drop(node.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(10);
    while node.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = node.kill();
    let _ = node.wait();
}

