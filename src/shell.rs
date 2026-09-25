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
    if !dir.contains("://") {
        std::fs::create_dir_all(dir)?;
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let log = std::env::temp_dir().join(format!("pondra-shell-{port}.log"));
    let mut node = Command::new(std::env::current_exe()?)
        .args(["serve", "--dir", dir, "--addr", &format!("127.0.0.1:{port}"), "--stop-with-stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log)?)
        .spawn()?;
    let r = session(dir, &format!("http://127.0.0.1:{port}"), &mut node, &log).await;
    stop(&mut node); // (whatever happened: the node never outlives the shell)
    r
}

async fn session(dir: &str, base: &str, node: &mut Child, log: &Path) -> Result<()> {
    // Only ever this machine's own node, over plain HTTP: never through a proxy the environment
    // names (it couldn't reach the node), and no CA certificates needed (minimal images have none).
    let (http, started) = (reqwest::Client::builder().no_proxy().tls_certs_only([]).build()?, Instant::now());
    while http.get(format!("{base}/stats")).send().await.is_err() {
        if node.try_wait()?.is_some() || started.elapsed() > Duration::from_secs(120) {
            bail!("the node didn't start: {}", std::fs::read_to_string(log).unwrap_or_default());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
            (true, ".tables") => line = "SELECT table_catalog AS lake, table_schema AS schema, table_name AS name, table_type AS kind FROM information_schema.tables WHERE table_schema <> 'information_schema' ORDER BY 1, 2, 3;".into(),
            _ => {}
        }
        sql.push_str(&line);
        let (statements, rest) = if end { (vec![std::mem::take(&mut sql)], String::new()) } else { split(&sql) }; // (the last may lack its ;)
        sql = rest;
        for statement in statements.iter().filter(|s| !s.trim().is_empty()) {
            let at = Instant::now();
            let answer = match http.post(format!("{base}/sql?format=table")).body(statement.trim().to_string()).send().await {
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

/// The complete statements in `text` — each ends at a `;` outside strings, quoted names and
/// comments — and the rest, if it holds more than whitespace and comments.
fn split(text: &str) -> (Vec<String>, String) {
    let (b, mut done, mut start, mut inside, mut code) = (text.as_bytes(), vec![], 0, None, false);
    let mut i = 0;
    while i < b.len() {
        let next = b.get(i + 1).copied();
        match (inside, b[i]) {
            (Some(b'*'), b'*') if next == Some(b'/') => (inside, i) = (None, i + 1), // end of /* … */
            (Some(end), c) if end != b'*' && c == end => inside = None, // end of '…', "…", -- …
            (Some(_), _) => {}
            (None, b'-') if next == Some(b'-') => inside = Some(b'\n'),
            (None, b'/') if next == Some(b'*') => (inside, i) = (Some(b'*'), i + 1),
            (None, b';') => {
                if code {
                    done.push(text[start..i].to_string());
                }
                (start, code) = (i + 1, false);
            }
            (None, c) => {
                code |= !c.is_ascii_whitespace();
                if c == b'\'' || c == b'"' {
                    inside = Some(c);
                }
            }
        }
        i += 1;
    }
    (done, if code || inside.is_some() { text[start..].to_string() } else { String::new() })
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

